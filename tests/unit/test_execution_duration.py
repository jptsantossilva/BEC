from types import SimpleNamespace

import bec.main as main
import bec.utils.database as database
from pages import trading


def test_calc_duration_supports_two_decimal_seconds():
    assert database.calc_duration(0.428, decimal_places=2) == "0.43s"
    assert database.calc_duration(60.428, decimal_places=2) == " 1m 0.43s"


def test_shared_settings_balance_uses_active_exchange_adapter(monkeypatch):
    monkeypatch.setattr(
        trading.binance,
        "fetch_balance",
        lambda asset: SimpleNamespace(asset=asset, free=25.75),
    )

    assert trading._available_balance_for_settings("USDC") == (25.75, None)


def test_shared_settings_remain_available_when_private_balance_fails(monkeypatch):
    def unavailable(_asset):
        raise RuntimeError("private access unavailable")

    monkeypatch.setattr(trading.binance, "fetch_balance", unavailable)

    assert trading._available_balance_for_settings("USDC") == (
        None,
        "RuntimeError",
    )


def test_trade_report_includes_subsecond_duration(monkeypatch):
    settings = SimpleNamespace(run_mode="live")
    summary = main._new_trade_cycle_summary("1h", "summary")
    timer_values = iter([100.0, 100.428])
    sent_messages = []

    monkeypatch.setattr(main.config, "load_settings", lambda refresh=True: settings)
    monkeypatch.setattr(main.timeit, "default_timer", lambda: next(timer_values))
    monkeypatch.setattr(main, "trade", lambda *args, **kwargs: summary)
    monkeypatch.setattr(
        main,
        "positions_summary",
        lambda *args, **kwargs: "Positions: no open positions",
    )
    monkeypatch.setattr(
        main.telegram,
        "send_telegram_message",
        lambda *args: sent_messages.append(args[-1]),
    )

    main.run("1h")

    assert "Status: completed in 0.43s" in sent_messages[0]
