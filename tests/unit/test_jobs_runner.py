import io
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

import bec.jobs_runner as jobs_runner
import bec.exchanges.live_execution as live_execution


def test_resolve_schedule_script_keeps_existing_root_wrapper():
    assert jobs_runner._resolve_schedule_script("main.py") == "main.py"


def test_resolve_schedule_script_supports_legacy_signals_path():
    assert jobs_runner._resolve_schedule_script("signals/super_rsi.py") == "bec/signals/super_rsi.py"


def test_resolve_schedule_script_keeps_unknown_path_for_subprocess_error():
    assert jobs_runner._resolve_schedule_script("missing/script.py") == "missing/script.py"


def _live_schedules():
    return pd.DataFrame(
        [
            {
                "name": "main_1d",
                "script": "main.py",
                "script_args": "1d",
                "cadence": "1d",
                "enabled": 1,
                "last_run": None,
            },
            {
                "name": "main_4h",
                "script": "main.py",
                "script_args": "4h",
                "cadence": "4h",
                "enabled": 1,
                "last_run": None,
            },
            {
                "name": "main_1h",
                "script": "main.py",
                "script_args": "1h",
                "cadence": "1h",
                "enabled": 1,
                "last_run": None,
            },
        ]
    )


def test_due_main_schedules_are_grouped_in_timeframe_order():
    entries = jobs_runner._collect_due_main_batch(
        _live_schedules(),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
        "kraken",
    )

    assert [timeframe for _row, timeframe, _slot in entries] == ["1h", "4h", "1d"]


def test_batch_rollout_allowlist_groups_validated_exchanges():
    for exchange_code in ("kraken", "okx", "binance"):
        entries = jobs_runner._collect_due_main_batch(
            _live_schedules(),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
            exchange_code,
        )

        assert [timeframe for _row, timeframe, _slot in entries] == [
            "1h",
            "4h",
            "1d",
        ]


def test_batch_rollout_allowlist_keeps_unknown_exchange_on_legacy_path():
    entries = jobs_runner._collect_due_main_batch(
        _live_schedules(),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
        "unknown",
    )

    assert entries == []


def test_disabled_main_schedule_is_batched_when_an_exit_is_still_open(monkeypatch):
    schedules = _live_schedules().iloc[[2]].copy()
    schedules.loc[:, "enabled"] = 0
    monkeypatch.setattr(
        jobs_runner.database,
        "get_num_open_positions_by_bot",
        lambda timeframe: 1 if timeframe == "1h" else 0,
    )

    entries = jobs_runner._collect_due_main_batch(
        schedules,
        datetime(2025, 1, 2, 1, tzinfo=timezone.utc),
        "kraken",
    )

    assert [timeframe for _row, timeframe, _slot in entries] == ["1h"]


def test_start_main_batch_updates_each_slot_after_process_start(monkeypatch):
    entries = jobs_runner._collect_due_main_batch(
        _live_schedules(),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
        "kraken",
    )
    calls = []
    process = object()
    monkeypatch.setattr(jobs_runner, "_print_log", lambda message: None)
    monkeypatch.setattr(
        jobs_runner.subprocess,
        "Popen",
        lambda command, cwd: calls.append(("popen", command, cwd)) or process,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "update_job_last_run",
        lambda name, slot: calls.append(("last_run", name, slot)),
    )

    name, returned_process = jobs_runner._start_main_batch(entries)

    assert name == "main_batch[1h,4h,1d]"
    assert returned_process is process
    assert calls[0][0] == "popen"
    assert calls[0][1] == [
        jobs_runner.PYTHON,
        "-m",
        "bec.trade_batch",
        "1h",
        "4h",
        "1d",
    ]
    assert [call[1] for call in calls[1:]] == ["main_1h", "main_4h", "main_1d"]


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


def test_startup_reconciliation_runs_only_for_unsettled_intents(monkeypatch):
    calls = []
    monkeypatch.setattr(
        jobs_runner.database, "get_unsettled_order_intents", lambda: [{"Id": 1}]
    )
    monkeypatch.setattr(
        live_execution,
        "reconcile_unsettled_orders",
        lambda: calls.append(True)
        or {"checked": 1, "updated": 1, "unresolved": 0},
    )
    monkeypatch.setattr(jobs_runner, "_print_log", lambda message: None)

    stats = jobs_runner._run_startup_reconciliation()

    assert calls == [True]
    assert stats["updated"] == 1


def test_runner_console_and_job_logs_include_exchange_identity(monkeypatch, capsys):
    monkeypatch.setattr(
        jobs_runner.database,
        "exchange_log_prefix",
        lambda: "[exchange_id=1:binance]",
    )

    jobs_runner._print_log("runner message")
    log_file = io.StringIO()
    jobs_runner._write_backtesting_job_log(log_file, "job message")

    assert capsys.readouterr().out == "[exchange_id=1:binance] runner message\n"
    assert log_file.getvalue() == "[exchange_id=1:binance] job message\n"


def _backtesting_job():
    return {
        "id": 42,
        "exchange_id": 2,
        "exchange_code": "kraken",
        "exchange_adapter_id": "kraken",
        "exchange_quote_asset": "USDC",
        "exchange_execution_environment": "production",
        "strategy_id": "ema_cross",
        "symbol": "btcusdc",
        "timeframe": "4h",
        "optimize": True,
        "commission_value": 0.0026,
        "work_fingerprint": "kraken-work",
    }


def test_backtesting_subprocess_command_carries_exchange_fee_and_fingerprint():
    command = jobs_runner._backtesting_job_command(_backtesting_job())

    assert command[command.index("--exchange-id") + 1] == "2"
    assert command[command.index("--exchange-code") + 1] == "kraken"
    assert command[command.index("--exchange-adapter-id") + 1] == "kraken"
    assert command[command.index("--exchange-quote-asset") + 1] == "USDC"
    assert command[command.index("--exchange-execution-environment") + 1] == "production"
    assert command[command.index("--commission") + 1] == "0.0026"
    assert command[command.index("--work-fingerprint") + 1] == "kraken-work"
    assert command[-1] == "--optimize"


def test_monte_carlo_subprocess_command_carries_exchange_identity():
    command = jobs_runner._monte_carlo_job_command(
        {
            "exchange_id": 2,
            "exchange_code": "kraken",
            "exchange_adapter_id": "kraken",
            "exchange_quote_asset": "USDC",
            "exchange_execution_environment": "production",
            "symbol": "BTC/EUR",
            "timeframe": "1h",
            "strategy_id": "ema_cross",
            "method": "trade_shuffle",
            "scenarios": 100,
            "seed": 42,
        }
    )

    assert command[command.index("--exchange-id") + 1] == "2"
    assert command[command.index("--exchange-code") + 1] == "kraken"
    assert command[command.index("--exchange-adapter-id") + 1] == "kraken"


def _mock_eligible_backtest_candidate(monkeypatch, *, database_action="inserted"):
    strategy = pd.DataFrame(
        [
            {
                "Id": "ema_cross",
                "Name": "EMA Cross",
                "Type": "builtin",
                "Status": "approved",
                "Main_Strategy": 1,
                "Backtest_Optimize": 1,
            }
        ]
    )
    result = pd.DataFrame(
        [
            {
                "Return_Perc": 12.5,
                "Backtest_Config_JSON": "{}",
                "Backtest_Work_Fingerprint": "kraken-work",
                "Backtest_Commission_Value": 0.0026,
                "Backtest_Adapter_Id": "kraken",
                "Backtest_Quote_Asset": "USDC",
                "Backtest_Execution_Environment": "production",
            }
        ]
    )
    calls = []
    monkeypatch.setattr(
        jobs_runner.database,
        "get_active_exchange",
        lambda required=True: {
            "id": 2,
            "code": "kraken",
            "adapter_id": "kraken",
            "quote_asset": "USDC",
            "execution_environment": "production",
        },
    )
    monkeypatch.setattr(
        jobs_runner.database, "is_trade_main_timeframe_enabled", lambda timeframe: True
    )
    monkeypatch.setattr(
        jobs_runner.config,
        "load_settings",
        lambda refresh=True: SimpleNamespace(main_strategies=["ema_cross"]),
    )
    monkeypatch.setattr(
        jobs_runner.database, "get_strategy_by_id", lambda strategy_id: strategy
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: result,
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_settings",
        lambda require_explicit_fee=True: {"Commission_Value": 0.0026},
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "build_backtesting_work_fingerprint",
        lambda *args, **kwargs: "kraken-work",
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "backtesting_result_matches_context",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        jobs_runner.database, "is_backtest_approved", lambda *args: (True, [])
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "build_strategy_params_json_from_backtest_result",
        lambda *args: '{"parameters":{"ema_fast":10}}',
    )
    monkeypatch.setattr(
        jobs_runner.database,
        "sync_top_performer_position_candidate",
        lambda **kwargs: calls.append(kwargs)
        or (database_action, f"candidate_{database_action}"),
    )
    return strategy, result, calls


@pytest.mark.parametrize("database_action", ["inserted", "updated"])
def test_sync_backtesting_result_promotes_eligible_candidate(
    monkeypatch, database_action
):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(
        monkeypatch, database_action=database_action
    )

    log_file = io.StringIO()
    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job(), log_file=log_file
    )

    assert outcome == (database_action, f"candidate_{database_action}")
    assert calls == [
        {
            "exchange_id": 2,
            "symbol": "BTCUSDC",
            "timeframe": "4h",
            "strategy_id": "ema_cross",
            "strategy_name": "EMA Cross",
            "strategy_params_json": '{"parameters":{"ema_fast":10}}',
        }
    ]
    assert f"Position candidate {database_action}" in log_file.getvalue()


@pytest.mark.parametrize(
    ("timeframe", "enabled", "reason"),
    [
        ("15m", True, "unsupported_live_timeframe"),
        ("4h", False, "live_timeframe_disabled"),
    ],
)
def test_sync_backtesting_result_skips_ineligible_timeframe(
    monkeypatch, timeframe, enabled, reason
):
    _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "is_trade_main_timeframe_enabled",
        lambda selected_timeframe: enabled,
    )
    job = {**_backtesting_job(), "timeframe": timeframe}

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(job)

    assert outcome == ("skipped", reason)


@pytest.mark.parametrize(
    ("strategy_change", "reason"),
    [
        ({"Status": "draft", "Type": "custom"}, "strategy_not_approved"),
        ({"Main_Strategy": 0}, "strategy_not_available_for_main"),
    ],
)
def test_sync_backtesting_result_skips_ineligible_strategy(
    monkeypatch, strategy_change, reason
):
    strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    for column, value in strategy_change.items():
        strategy.loc[0, column] = value

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    )

    assert outcome == ("skipped", reason)
    assert calls == []


def test_sync_backtesting_result_requires_selected_strategy(monkeypatch):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.config,
        "load_settings",
        lambda refresh=True: SimpleNamespace(main_strategies=["another_strategy"]),
    )

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    )

    assert outcome == ("skipped", "strategy_not_selected")
    assert calls == []


def test_sync_backtesting_result_requires_available_strategy(monkeypatch):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "get_strategy_by_id",
        lambda strategy_id: pd.DataFrame(),
    )

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    )

    assert outcome == ("skipped", "strategy_unavailable")
    assert calls == []


@pytest.mark.parametrize(
    ("result_change", "reason"),
    [
        ({"Return_Perc": 0}, "non_positive_return"),
        ({"Return_Perc": -1}, "non_positive_return"),
    ],
)
def test_sync_backtesting_result_requires_positive_return(
    monkeypatch, result_change, reason
):
    _strategy, result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    for column, value in result_change.items():
        result.loc[0, column] = value

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    )

    assert outcome == ("skipped", reason)
    assert calls == []


def test_sync_backtesting_result_skips_missing_or_stale_result(monkeypatch):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame(),
    )
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    ) == ("skipped", "missing_backtest_result")

    _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "build_backtesting_work_fingerprint",
        lambda *args, **kwargs: "new-work",
    )
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    ) == ("skipped", "queued_backtest_context_stale")
    assert calls == []


def test_sync_backtesting_result_skips_changed_commission(monkeypatch):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    job = {**_backtesting_job(), "commission_value": 0.004}

    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(job)

    assert outcome == ("skipped", "queued_backtest_context_stale")
    assert calls == []


def test_sync_backtesting_result_checks_result_context_and_approval(monkeypatch):
    _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "backtesting_result_matches_context",
        lambda *args, **kwargs: False,
    )
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    ) == ("skipped", "backtest_context_mismatch")

    _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "is_backtest_approved",
        lambda *args: (False, ["Min_Trades"]),
    )
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    ) == ("skipped", "backtest_rejected:Min_Trades")


def test_sync_backtesting_result_rechecks_exchange_and_top_performer(monkeypatch):
    _strategy, _result, calls = _mock_eligible_backtest_candidate(monkeypatch)
    mismatched_job = {**_backtesting_job(), "exchange_id": 99}
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        mismatched_job
    ) == ("skipped", "active_exchange_mismatch")

    _mock_eligible_backtest_candidate(monkeypatch, database_action="skipped")
    monkeypatch.setattr(
        jobs_runner.database,
        "sync_top_performer_position_candidate",
        lambda **kwargs: ("skipped", "not_current_top_performer"),
    )
    assert jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job()
    ) == ("skipped", "not_current_top_performer")
    assert calls == []


def test_sync_backtesting_result_logs_errors_without_raising(monkeypatch):
    _mock_eligible_backtest_candidate(monkeypatch)
    monkeypatch.setattr(
        jobs_runner.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    log_file = io.StringIO()
    outcome = jobs_runner._sync_backtesting_result_to_position_candidate(
        _backtesting_job(), log_file=log_file
    )

    assert outcome == ("failed", "technical_error")
    assert "Failed to sync backtesting result to position candidate" in log_file.getvalue()


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
        "_sync_backtesting_result_to_position_candidate",
        lambda job, log_file=None: sync_calls.append((job, log_file))
        or ("inserted", "candidate_created"),
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


def test_finish_backtesting_job_keeps_success_when_candidate_sync_fails(monkeypatch):
    complete_calls = []
    monkeypatch.setattr(
        jobs_runner,
        "_sync_backtesting_result_to_position_candidate",
        lambda job, log_file=None: ("failed", "technical_error"),
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
            "process": _FinishedProcess(0),
            "log_file": io.StringIO(),
        }
    )

    assert finished is True
    assert complete_calls == [(42, 0, "")]


def test_finish_backtesting_job_does_not_sync_failed_job(monkeypatch):
    sync_calls = []
    complete_calls = []

    monkeypatch.setattr(
        jobs_runner,
        "_sync_backtesting_result_to_position_candidate",
        lambda job, log_file=None: sync_calls.append((job, log_file))
        or ("inserted", "candidate_created"),
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
