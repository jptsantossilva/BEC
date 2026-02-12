import os
import sys
import time
from datetime import datetime, timezone, timedelta
import subprocess
import shlex

import utils.database as database
import utils.telegram as telegram

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable
MAX_PARALLEL = 10

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

def run_loop():
    print("jobs_runner started (UTC).")
    running = []
    while True:
        _sleep_until_next_minute()
        now_utc = datetime.now(timezone.utc)
        running = [(name, proc) for name, proc in running if proc.poll() is None]
        try:
            schedules = database.get_job_schedules()
        except Exception as e:
            print(f"Failed to read schedules: {e}")
            time.sleep(1)
            continue

        for row in schedules.itertuples(index=False):
            name = row.name
            if not row.script:
                continue
            cadence = row.cadence
            if not row.enabled:
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
                print(f"[{fired_at.isoformat()}] Running {name} ({cadence})")
                args = shlex.split(row.script_args or "")
                proc = subprocess.Popen([PYTHON, row.script, *args], cwd=ROOT_DIR)
                running.append((name, proc))
                database.update_job_last_run(name, slot.isoformat())
            except Exception as e:
                msg = f"[signals_runner] {name} failed: {repr(e)}"
                print(msg)
                try:
                    telegram.send_telegram_message(telegram.telegram_token_errors,
                                                   telegram.EMOJI_WARNING,
                                                   msg)
                except Exception:
                    pass

        time.sleep(0.1)

if __name__ == "__main__":
    run_loop()
