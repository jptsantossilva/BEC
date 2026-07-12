import os
import sys
import time
from datetime import datetime, timezone, timedelta
import subprocess
import shlex
import fcntl

import bec.utils.database as database
import bec.utils.telegram as telegram

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
MAX_PARALLEL = 10
BACKTESTING_JOBS_DIR = os.path.join("static", "backtest_results", "jobs")
MONTE_CARLO_JOBS_DIR = os.path.join("static", "backtest_results", "monte_carlo", "jobs")
RUNNER_LOCK_PATH = os.path.join(ROOT_DIR, "static", "backtest_results", "jobs_runner.lock")
_RUNNER_LOCK_FILE = None


def _exchange_log_message(message: str) -> str:
    return f"{database.exchange_log_prefix()} {message}"


def _print_log(message: str) -> None:
    print(_exchange_log_message(message))


def _acquire_runner_lock() -> bool:
    global _RUNNER_LOCK_FILE
    if _RUNNER_LOCK_FILE is not None:
        return True

    os.makedirs(os.path.dirname(RUNNER_LOCK_PATH), exist_ok=True)
    lock_file = open(RUNNER_LOCK_PATH, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        return False

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(f"{os.getpid()}\n")
    lock_file.flush()
    _RUNNER_LOCK_FILE = lock_file
    return True


def _resolve_schedule_script(script: str) -> str:
    """Resolve schedule scripts against the current package layout."""
    script = str(script or "").strip()
    if not script:
        return script

    script_path = os.path.normpath(script)
    if os.path.isabs(script_path) or os.path.exists(os.path.join(ROOT_DIR, script_path)):
        return script_path

    packaged_script_path = os.path.join("bec", script_path)
    if os.path.exists(os.path.join(ROOT_DIR, packaged_script_path)):
        return packaged_script_path

    return script_path


def _extract_main_timeframe(schedule_name: str):
    """Return timeframe for main jobs (main_1d/main_4h/main_1h), else None."""
    if not schedule_name.startswith("main_"):
        return None
    timeframe = schedule_name.replace("main_", "", 1)
    if timeframe in {"1d", "4h", "1h"}:
        return timeframe
    return None

def _parse_last_run(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None

def _current_slot(now_utc, cadence):
    if cadence == "1m":
        return now_utc.replace(second=0, microsecond=0)
    if cadence == "15m":
        minute = (now_utc.minute // 15) * 15
        return now_utc.replace(minute=minute, second=0, microsecond=0)
    if cadence == "1h":
        return now_utc.replace(minute=0, second=0, microsecond=0)
    if cadence == "4h":
        hour = (now_utc.hour // 4) * 4
        return now_utc.replace(hour=hour, minute=0, second=0, microsecond=0)
    if cadence == "1d":
        return now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    return None

def _on_schedule(now_utc, cadence):
    if cadence == "1m":
        return now_utc.second == 0
    if cadence == "15m":
        return now_utc.minute % 15 == 0 and now_utc.second == 0
    if cadence == "1h":
        return now_utc.minute == 0 and now_utc.second == 0
    if cadence == "4h":
        return now_utc.minute == 0 and now_utc.second == 0 and now_utc.hour % 4 == 0
    if cadence == "1d":
        return now_utc.minute == 0 and now_utc.second == 0 and now_utc.hour == 0
    return False

def _sleep_until_next_minute():
    now = datetime.now(timezone.utc)
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    sleep_seconds = (next_minute - now).total_seconds()
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def _run_startup_reconciliation():
    try:
        if not database.get_unsettled_order_intents():
            return {"checked": 0, "updated": 0, "unresolved": 0}
        from bec.exchanges.live_execution import reconcile_unsettled_orders

        stats = reconcile_unsettled_orders()
        _print_log(f"Startup exchange reconciliation: {stats}")
        return stats
    except Exception as exc:
        _print_log(f"Startup exchange reconciliation deferred: {exc!r}")
        return {"checked": 0, "updated": 0, "unresolved": 1}

def _backtesting_job_command(job):
    command = [
        PYTHON,
        "my_backtesting.py",
        "--exchange-id",
        str(job["exchange_id"]),
        "--exchange-code",
        job["exchange_code"],
        "--commission",
        str(job["commission_value"]),
        "--work-fingerprint",
        job["work_fingerprint"],
        "--symbol",
        job["symbol"],
        "--timeframe",
        job["timeframe"],
        "--strategy",
        job["strategy_id"],
    ]
    if job["optimize"]:
        command.append("--optimize")
    return command


def _start_next_backtesting_job():
    job = database.claim_next_backtesting_job()
    if job is None:
        return None

    os.makedirs(os.path.join(ROOT_DIR, BACKTESTING_JOBS_DIR), exist_ok=True)
    log_path = os.path.join(BACKTESTING_JOBS_DIR, f"{job['id']}.log")
    database.set_backtesting_job_log_path(job["id"], log_path)
    log_abs_path = os.path.join(ROOT_DIR, log_path)
    log_file = open(log_abs_path, "w", encoding="utf-8")
    command = _backtesting_job_command(job)
    started_at = datetime.now(timezone.utc).isoformat()
    log_file.write(
        _exchange_log_message(
            f"[{started_at}] Running backtest job {job['id']}: "
            f"{job['exchange_code']} - {job['strategy_id']} - "
            f"{job['symbol']} - {job['timeframe']} "
            f"(optimize={job['optimize']})"
        )
        + "\n"
    )
    log_file.flush()
    _print_log(
        f"[{started_at}] Running backtest job {job['id']} "
        f"({job['exchange_code']} - {job['strategy_id']} - "
        f"{job['symbol']} - {job['timeframe']})"
    )

    process = subprocess.Popen(
        command,
        cwd=ROOT_DIR,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return {"job": job, "process": process, "log_file": log_file}


def _write_backtesting_job_log(log_file, message: str):
    if log_file is None:
        return
    log_file.write(f"{_exchange_log_message(message)}\n")
    log_file.flush()


def _sync_backtesting_result_to_existing_position(job, log_file=None):
    strategy_id = str(job["strategy_id"]).strip()
    symbol = str(job["symbol"]).strip().upper()
    timeframe = str(job["timeframe"]).strip()

    try:
        df_result = database.get_backtesting_results_by_symbol_timeframe_strategy(
            symbol=symbol,
            time_frame=timeframe,
            strategy_id=strategy_id,
        )
        if df_result.empty:
            _write_backtesting_job_log(
                log_file,
                "No backtesting result found; existing position strategy params not synced.",
            )
            return False

        position_exists = database.get_all_positions_by_bot_symbol_strategy(
            bot=timeframe,
            symbol=symbol,
            strategy_id=strategy_id,
        )
        if not position_exists:
            _write_backtesting_job_log(
                log_file,
                "No existing matching position found; strategy params not synced.",
            )
            return False

        strategy_params_json = database.build_strategy_params_json_from_backtest_result(
            strategy_id,
            df_result.iloc[0],
        )
        database.set_backtesting_results_from_position_strategy(
            symbol=symbol,
            timeframe=timeframe,
            strategy_id=strategy_id,
            strategy_params_json=strategy_params_json,
        )
        _write_backtesting_job_log(
            log_file,
            "Updated existing position strategy params.",
        )
        return True
    except Exception as exc:
        _write_backtesting_job_log(
            log_file,
            f"Failed to sync existing position strategy params: {repr(exc)}",
        )
        return False


def _finish_backtesting_job(running_job):
    job = running_job["job"]
    process = running_job["process"]
    log_file = running_job["log_file"]
    return_code = process.poll()
    if return_code is None:
        return False

    finished_at = datetime.now(timezone.utc).isoformat()
    log_file.write(
        "\n"
        + _exchange_log_message(
            f"[{finished_at}] Backtest job finished with return code {return_code}."
        )
        + "\n"
    )
    log_file.flush()
    if return_code == 0:
        _sync_backtesting_result_to_existing_position(job, log_file=log_file)
    error_message = "" if return_code == 0 else "Backtest subprocess failed. Check the job log."
    database.complete_backtesting_job(job["id"], return_code, error_message)
    log_file.close()
    _print_log(
        f"[{finished_at}] Backtest job {job['id']} finished with return code {return_code}"
    )
    return True

def _monte_carlo_job_command(job):
    return [
        PYTHON,
        "monte_carlo.py",
        "--exchange-id",
        str(job["exchange_id"]),
        "--exchange-code",
        job["exchange_code"],
        "--symbol",
        job["symbol"],
        "--timeframe",
        job["timeframe"],
        "--strategy",
        job["strategy_id"],
        "--method",
        job["method"],
        "--scenarios",
        str(job["scenarios"]),
        "--seed",
        str(job["seed"]),
    ]


def _start_next_monte_carlo_job():
    job = database.claim_next_monte_carlo_job()
    if job is None:
        return None

    os.makedirs(os.path.join(ROOT_DIR, MONTE_CARLO_JOBS_DIR), exist_ok=True)
    log_path = os.path.join(MONTE_CARLO_JOBS_DIR, f"{job['id']}.log")
    database.set_monte_carlo_job_log_path(job["id"], log_path)
    log_abs_path = os.path.join(ROOT_DIR, log_path)
    log_file = open(log_abs_path, "w", encoding="utf-8")
    command = _monte_carlo_job_command(job)
    started_at = datetime.now(timezone.utc).isoformat()
    log_file.write(
        _exchange_log_message(
            f"[{started_at}] Running Monte Carlo job {job['id']}: "
            f"{job['exchange_code']} - {job['method']} - {job['strategy_id']} - "
            f"{job['symbol']} - {job['timeframe']} "
            f"(scenarios={job['scenarios']}, seed={job['seed']})"
        )
        + "\n"
    )
    log_file.flush()
    _print_log(
        f"[{started_at}] Running Monte Carlo job {job['id']} "
        f"({job['method']} - {job['strategy_id']} - {job['symbol']} - {job['timeframe']})"
    )

    process = subprocess.Popen(
        command,
        cwd=ROOT_DIR,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return {"job": job, "process": process, "log_file": log_file}

def _finish_monte_carlo_job(running_job):
    job = running_job["job"]
    process = running_job["process"]
    log_file = running_job["log_file"]
    return_code = process.poll()
    if return_code is None:
        return False

    finished_at = datetime.now(timezone.utc).isoformat()
    log_file.write(
        "\n"
        + _exchange_log_message(
            f"[{finished_at}] Monte Carlo job finished with return code {return_code}."
        )
        + "\n"
    )
    log_file.flush()
    log_file.close()
    error_message = "" if return_code == 0 else "Monte Carlo subprocess failed. Check the job log."
    database.complete_monte_carlo_job(job["id"], return_code, error_message)
    _print_log(
        f"[{finished_at}] Monte Carlo job {job['id']} finished with return code {return_code}"
    )
    return True

def run_loop():
    if not _acquire_runner_lock():
        _print_log("jobs_runner already running; exiting duplicate process.")
        return

    _print_log("jobs_runner started (UTC).")
    database.reset_running_backtesting_jobs("jobs_runner restarted before this job completed.")
    database.reset_running_monte_carlo_jobs("jobs_runner restarted before this job completed.")
    _run_startup_reconciliation()
    running = []
    running_backtesting_job = None
    running_monte_carlo_job = None
    while True:
        now_utc = datetime.now(timezone.utc)
        running = [(name, proc) for name, proc in running if proc.poll() is None]

        if running_backtesting_job is not None and _finish_backtesting_job(running_backtesting_job):
            running_backtesting_job = None
        if running_monte_carlo_job is not None and _finish_monte_carlo_job(running_monte_carlo_job):
            running_monte_carlo_job = None

        if running_backtesting_job is None:
            try:
                running_backtesting_job = _start_next_backtesting_job()
            except Exception as e:
                msg = f"[backtesting_runner] failed to start queued job: {repr(e)}"
                _print_log(msg)
                try:
                    telegram.send_telegram_message(telegram.telegram_token_errors,
                                                   telegram.EMOJI_WARNING,
                                                   msg)
                except Exception:
                    pass

        if running_monte_carlo_job is None:
            try:
                running_monte_carlo_job = _start_next_monte_carlo_job()
            except Exception as e:
                msg = f"[monte_carlo_runner] failed to start queued job: {repr(e)}"
                _print_log(msg)
                try:
                    telegram.send_telegram_message(telegram.telegram_token_errors,
                                                   telegram.EMOJI_WARNING,
                                                   msg)
                except Exception:
                    pass

        try:
            schedules = database.get_job_schedules()
        except Exception as e:
            _print_log(f"Failed to read schedules: {e}")
            time.sleep(1)
            continue

        for row in schedules.itertuples(index=False):
            name = row.name
            if not row.script:
                continue
            cadence = row.cadence

            # If timeframe trading is disabled, keep running main_* jobs only to process
            # potential exits from existing open positions. New entries remain blocked
            # in main.py when timeframe is disabled.
            if not row.enabled:
                timeframe = _extract_main_timeframe(name)
                if timeframe is None:
                    continue
                if database.get_num_open_positions_by_bot(timeframe) <= 0:
                    continue

            if not _on_schedule(now_utc, cadence):
                continue

            slot = _current_slot(now_utc, cadence)
            if slot is None:
                continue
            last_run = _parse_last_run(row.last_run)
            if last_run and last_run >= slot:
                continue

            if len(running) >= MAX_PARALLEL:
                continue

            try:
                fired_at = datetime.now(timezone.utc)
                _print_log(f"[{fired_at.isoformat()}] Running {name} ({cadence})")
                args = shlex.split(row.script_args or "")
                script = _resolve_schedule_script(row.script)
                proc = subprocess.Popen([PYTHON, script, *args], cwd=ROOT_DIR)
                running.append((name, proc))
                database.update_job_last_run(name, slot.isoformat())
            except Exception as e:
                msg = f"[signals_runner] {name} failed: {repr(e)}"
                _print_log(msg)
                try:
                    telegram.send_telegram_message(telegram.telegram_token_errors,
                                                   telegram.EMOJI_WARNING,
                                                   msg)
                except Exception:
                    pass

        time.sleep(1)

if __name__ == "__main__":
    run_loop()
