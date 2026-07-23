import logging
import runpy
from pathlib import Path

import pytest

import bec.main as main


REPO_ROOT = Path(__file__).resolve().parents[2]


def _telegram_failure(calls, detail):
    def fail(_token, _emoji, message):
        calls.append(message)
        raise RuntimeError(detail)

    return fail


def test_cli_main_returns_zero_after_success(monkeypatch):
    calls = []

    monkeypatch.setattr(main, "read_arguments", lambda: "1h")
    monkeypatch.setattr(
        main,
        "apply_arguments",
        lambda timeframe: calls.append(("apply", timeframe)),
    )
    monkeypatch.setattr(
        main,
        "run",
        lambda timeframe: calls.append(("run", timeframe)),
    )
    monkeypatch.setattr(
        main,
        "_send_fatal_notification",
        lambda *_args: pytest.fail("Success must not send a fatal notification"),
    )

    assert main.cli_main() == 0
    assert calls == [("apply", "1h"), ("run", "1h")]


def test_cli_main_returns_two_and_logs_failed_validation_notification(
    monkeypatch, caplog
):
    notification_calls = []

    monkeypatch.setattr(main, "read_arguments", lambda: "invalid")
    monkeypatch.setattr(
        main,
        "run",
        lambda **_kwargs: pytest.fail("Invalid arguments must not start the bot"),
    )
    monkeypatch.setattr(
        main.telegram,
        "send_telegram_message",
        _telegram_failure(
            notification_calls,
            "failed https://api.telegram.org/botvalidation-secret/sendMessage",
        ),
    )

    with caplog.at_level(logging.ERROR):
        exit_code = main.cli_main()

    assert exit_code == 2
    assert notification_calls == [
        "[FATAL] Incorrect time frame. Use one of: 15m, 1h, 4h, 1d"
    ]
    assert "Incorrect time frame" in caplog.text
    assert (
        "Failed to send fatal Telegram notification "
        "(timeframe validation): RuntimeError"
    ) in caplog.text
    assert "validation-secret" not in caplog.text


def test_cli_main_returns_one_and_preserves_primary_runtime_log(
    monkeypatch, caplog
):
    notification_calls = []

    monkeypatch.setattr(main, "read_arguments", lambda: "1h")
    monkeypatch.setattr(main, "apply_arguments", lambda _timeframe: None)

    def fail_run(*, timeframe):
        assert timeframe == "1h"
        raise RuntimeError("primary bot failure")

    monkeypatch.setattr(main, "run", fail_run)
    monkeypatch.setattr(
        main.telegram,
        "send_telegram_message",
        _telegram_failure(
            notification_calls,
            "failed https://api.telegram.org/botruntime-secret/sendMessage",
        ),
    )

    with caplog.at_level(logging.ERROR):
        exit_code = main.cli_main()

    assert exit_code == 1
    assert notification_calls == [
        "[FATAL] Unhandled exception: primary bot failure"
    ]
    assert "Unhandled exception during bot run" in caplog.text
    assert "primary bot failure" in caplog.text
    assert (
        "Failed to send fatal Telegram notification (bot run): RuntimeError"
    ) in caplog.text
    assert "runtime-secret" not in caplog.text


def test_root_main_wrapper_delegates_to_package_cli(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "cli_main", lambda: calls.append("cli_main") or 17)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(REPO_ROOT / "main.py"), run_name="__main__")

    assert exit_info.value.code == 17
    assert calls == ["cli_main"]
