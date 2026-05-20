import io

import pandas as pd

import bec.jobs_runner as jobs_runner


def test_resolve_schedule_script_keeps_existing_root_wrapper():
    assert jobs_runner._resolve_schedule_script("main.py") == "main.py"


def test_resolve_schedule_script_supports_legacy_signals_path():
    assert jobs_runner._resolve_schedule_script("signals/super_rsi.py") == "bec/signals/super_rsi.py"


def test_resolve_schedule_script_keeps_unknown_path_for_subprocess_error():
    assert jobs_runner._resolve_schedule_script("missing/script.py") == "missing/script.py"


def test_run_loop_exits_when_runner_lock_unavailable(monkeypatch, capsys):
    monkeypatch.setattr(jobs_runner, "_acquire_runner_lock", lambda: False)
    monkeypatch.setattr(
        jobs_runner.database,
        "reset_running_backtesting_jobs",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("runner should exit before resetting jobs")
        ),
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "reset_running_monte_carlo_jobs",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("runner should exit before resetting jobs")
        ),
    )

    jobs_runner.run_loop()

    assert "jobs_runner already running" in capsys.readouterr().out


def _backtesting_job():
    return {
        "id": 42,
        "strategy_id": "ema_cross",
        "symbol": "btcusdc",
        "timeframe": "4h",
    }


def test_sync_backtesting_result_updates_existing_position(monkeypatch):
    calls = []
    result = pd.DataFrame([{"Backtest_Config_JSON": "{}"}])

    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: result,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "get_all_positions_by_bot_symbol_strategy",
        lambda **kwargs: True,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "build_strategy_params_json_from_backtest_result",
        lambda strategy_id, row: '{"parameters":{"ema_fast":10}}',
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "set_backtesting_results_from_position_strategy",
        lambda **kwargs: calls.append(kwargs),
    )

    log_file = io.StringIO()
    synced = jobs_runner._sync_backtesting_result_to_existing_position(
        _backtesting_job(),
        log_file=log_file,
    )

    assert synced is True
    assert calls == [
        {
            "symbol": "BTCUSDC",
            "timeframe": "4h",
            "strategy_id": "ema_cross",
            "strategy_params_json": '{"parameters":{"ema_fast":10}}',
        }
    ]
    assert "Updated existing position strategy params." in log_file.getvalue()


def test_sync_backtesting_result_skips_when_result_missing(monkeypatch):
    calls = []
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "set_backtesting_results_from_position_strategy",
        lambda **kwargs: calls.append(kwargs),
    )

    log_file = io.StringIO()
    synced = jobs_runner._sync_backtesting_result_to_existing_position(
        _backtesting_job(),
        log_file=log_file,
    )

    assert synced is False
    assert calls == []
    assert "No backtesting result found" in log_file.getvalue()


def test_sync_backtesting_result_skips_when_position_missing(monkeypatch):
    calls = []
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame([{"Backtest_Config_JSON": "{}"}]),
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "get_all_positions_by_bot_symbol_strategy",
        lambda **kwargs: False,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "set_backtesting_results_from_position_strategy",
        lambda **kwargs: calls.append(kwargs),
    )

    log_file = io.StringIO()
    synced = jobs_runner._sync_backtesting_result_to_existing_position(
        _backtesting_job(),
        log_file=log_file,
    )

    assert synced is False
    assert calls == []
    assert "No existing matching position found" in log_file.getvalue()


def test_sync_backtesting_result_logs_errors_without_raising(monkeypatch):
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    log_file = io.StringIO()
    synced = jobs_runner._sync_backtesting_result_to_existing_position(
        _backtesting_job(),
        log_file=log_file,
    )

    assert synced is False
    assert "Failed to sync existing position strategy params" in log_file.getvalue()


class _FinishedProcess:
    def __init__(self, return_code):
        self.return_code = return_code

    def poll(self):
        return self.return_code


def test_finish_backtesting_job_syncs_successful_job(monkeypatch):
    sync_calls = []
    complete_calls = []

    monkeypatch.setattr(
        jobs_runner,
        "_sync_backtesting_result_to_existing_position",
        lambda job, log_file=None: sync_calls.append((job, log_file)) or True,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "complete_backtesting_job",
        lambda job_id, return_code, error_message: complete_calls.append(
            (job_id, return_code, error_message)
        ),
    )

    log_file = io.StringIO()
    finished = jobs_runner._finish_backtesting_job(
        {
            "job": _backtesting_job(),
            "process": _FinishedProcess(0),
            "log_file": log_file,
        }
    )

    assert finished is True
    assert len(sync_calls) == 1
    assert complete_calls == [(42, 0, "")]


def test_finish_backtesting_job_does_not_sync_failed_job(monkeypatch):
    sync_calls = []
    complete_calls = []

    monkeypatch.setattr(
        jobs_runner,
        "_sync_backtesting_result_to_existing_position",
        lambda job, log_file=None: sync_calls.append((job, log_file)) or True,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "complete_backtesting_job",
        lambda job_id, return_code, error_message: complete_calls.append(
            (job_id, return_code, error_message)
        ),
    )

    finished = jobs_runner._finish_backtesting_job(
        {
            "job": _backtesting_job(),
            "process": _FinishedProcess(1),
            "log_file": io.StringIO(),
        }
    )

    assert finished is True
    assert sync_calls == []
    assert complete_calls == [
        (42, 1, "Backtest subprocess failed. Check the job log.")
    ]
