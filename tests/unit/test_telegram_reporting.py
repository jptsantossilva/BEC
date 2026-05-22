from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pandas as pd

import bec.add_symbol as add_symbol
import bec.exchanges.binance as binance
import bec.main as main
import bec.symbol_by_market_phase as market_phase
import bec.signals.bb_width as bb_width
import bec.utils.telegram as telegram
from bec.strategy_builder.templates import get_builtin_template
from bec.utils import telegram_reporting


def _market_data(close=100.0, rows=260):
    index = pd.date_range("2026-01-01", periods=rows, freq="h")
    return pd.DataFrame(
        {
            "Open": [close] * rows,
            "High": [close + 1] * rows,
            "Low": [close - 1] * rows,
            "Close": [close] * rows,
            "Volume": [1] * rows,
        },
        index=index,
    )


def _settings(routine_mode="summary"):
    return SimpleNamespace(
        telegram_routine_trade_logs=routine_mode,
        main_strategies=["market_phases"],
        max_number_of_open_positions=20,
        n_decimals=2,
        trade_against="USDC",
    )


def _strategy_params():
    return json.dumps({"parameters": {"fast": 20, "slow": 60}})


def _auto_switch_settings(trade_against="USDC", btc_strategy="wema20"):
    return SimpleNamespace(
        trade_against_switch=True,
        trade_against_switch_stablecoin="USDC",
        btc_strategy=btc_strategy,
        trade_against=trade_against,
    )


def test_main_get_data_accepts_weekly_timeframe(monkeypatch):
    calls = []

    def fake_get_ohlcv(**kwargs):
        calls.append(kwargs)
        return _market_data(rows=3)

    monkeypatch.setattr(main.binance, "get_ohlcv", fake_get_ohlcv)

    df = main.get_data("BTCUSDC", "1w")

    assert not df.empty
    assert calls[0]["interval"] == "1w"


def test_auto_switch_signal_timeframe_prefers_largest_fixed_timeframe(monkeypatch):
    definition = {
        "entry": {
            "left": {"type": "price", "field": "Close", "timeframe": "1d"},
            "right": {"type": "indicator", "name": "SMA", "timeframe": "1w"},
        },
        "exit": {},
    }
    monkeypatch.setattr(
        market_phase.database,
        "get_strategy_definition",
        lambda strategy_id: definition,
    )

    assert market_phase._infer_btc_auto_switch_signal_timeframe("weekly", "1d") == "1w"


def test_auto_switch_signal_timeframe_uses_base_for_current_operands(monkeypatch):
    definition = {
        "entry": {
            "left": {"type": "price", "field": "Close", "timeframe": "current"},
            "right": {"type": "indicator", "name": "SMA", "timeframe": "current"},
        },
        "exit": {},
    }
    monkeypatch.setattr(
        market_phase.database,
        "get_strategy_definition",
        lambda strategy_id: definition,
    )

    assert market_phase._infer_btc_auto_switch_signal_timeframe("daily", "1d") == "1d"


def test_auto_switch_buy_signal_records_then_skips_same_candle(monkeypatch):
    order_calls = []
    records = []
    updates = []
    processed = {"value": False}

    monkeypatch.setattr(
        market_phase.binance,
        "get_close_df",
        lambda *args, **kwargs: _market_data(rows=3),
    )
    monkeypatch.setattr(
        market_phase,
        "_infer_btc_auto_switch_signal_timeframe",
        lambda strategy_id, base_timeframe="1d": "1w",
    )
    monkeypatch.setattr(
        market_phase,
        "_get_btc_auto_switch_candle_id",
        lambda symbol, timeframe: "2026-05-18T00:00:00Z",
    )
    monkeypatch.setattr(
        market_phase,
        "_evaluate_btc_auto_switch_strategy",
        lambda strategy_id, symbol, timeframe: (True, False),
    )
    monkeypatch.setattr(
        market_phase.database,
        "auto_switch_signal_processed",
        lambda *args: processed["value"],
    )
    monkeypatch.setattr(
        market_phase.database,
        "record_auto_switch_signal",
        lambda *args: records.append(args),
    )
    monkeypatch.setattr(
        market_phase.database,
        "get_positions_by_bot_position",
        lambda bot, position: pd.DataFrame(),
    )
    monkeypatch.setattr(market_phase.database, "release_all_values", lambda: None)
    monkeypatch.setattr(
        market_phase.binance,
        "create_buy_order",
        lambda *args, **kwargs: order_calls.append(("buy", args, kwargs)),
    )
    monkeypatch.setattr(
        market_phase.config,
        "update_settings",
        lambda payload: updates.append(payload),
    )
    monkeypatch.setattr(
        market_phase.config,
        "load_settings",
        lambda refresh=False: _auto_switch_settings(trade_against="BTC"),
    )
    monkeypatch.setattr(
        market_phase.telegram,
        "send_telegram_message",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        market_phase.telegram,
        "send_error_event",
        lambda *args, **kwargs: None,
    )

    market_phase.trade_against_auto_switch(settings=_auto_switch_settings())
    processed["value"] = True
    market_phase.trade_against_auto_switch(settings=_auto_switch_settings())

    assert len(order_calls) == 1
    assert len(records) == 1
    assert records[0] == (
        "wema20",
        "BTCUSDC",
        "buy",
        "1w",
        "2026-05-18T00:00:00Z",
    )
    assert updates[:2] == [{"trade_against": "BTC"}, {"min_position_size": 0.0001}]


def test_auto_switch_sell_signal_uses_exit_alert(monkeypatch):
    order_calls = []
    records = []
    sent = []

    monkeypatch.setattr(
        market_phase.binance,
        "get_close_df",
        lambda *args, **kwargs: _market_data(rows=3),
    )
    monkeypatch.setattr(
        market_phase,
        "_infer_btc_auto_switch_signal_timeframe",
        lambda strategy_id, base_timeframe="1d": "1d",
    )
    monkeypatch.setattr(
        market_phase,
        "_get_btc_auto_switch_candle_id",
        lambda symbol, timeframe: "2026-05-20T00:00:00Z",
    )
    monkeypatch.setattr(
        market_phase,
        "_evaluate_btc_auto_switch_strategy",
        lambda strategy_id, symbol, timeframe: (False, True),
    )
    monkeypatch.setattr(market_phase.database, "auto_switch_signal_processed", lambda *args: False)
    monkeypatch.setattr(market_phase.database, "record_auto_switch_signal", lambda *args: records.append(args))
    monkeypatch.setattr(
        market_phase.database,
        "get_positions_by_bot_position",
        lambda bot, position: pd.DataFrame(),
    )
    monkeypatch.setattr(market_phase.database, "release_all_values", lambda: None)
    monkeypatch.setattr(
        market_phase.binance,
        "create_sell_order",
        lambda *args, **kwargs: order_calls.append(("sell", args, kwargs)),
    )
    monkeypatch.setattr(market_phase.config, "update_settings", lambda payload: None)
    monkeypatch.setattr(
        market_phase.config,
        "load_settings",
        lambda refresh=False: _auto_switch_settings(trade_against="USDC"),
    )
    monkeypatch.setattr(market_phase.telegram, "EMOJI_ENTER_TRADE", "ENTER")
    monkeypatch.setattr(market_phase.telegram, "EMOJI_EXIT_TRADE", "EXIT")
    monkeypatch.setattr(market_phase.telegram, "send_telegram_message", lambda *args, **kwargs: sent.append(args))
    monkeypatch.setattr(market_phase.telegram, "send_error_event", lambda *args, **kwargs: None)

    market_phase.trade_against_auto_switch(settings=_auto_switch_settings(trade_against="BTC"))

    assert sent[0][1] == "EXIT"
    assert len(order_calls) == 1
    assert records[0] == (
        "wema20",
        "BTCUSDC",
        "sell",
        "1d",
        "2026-05-20T00:00:00Z",
    )


def test_auto_switch_signal_persistence_keys_by_strategy_and_candle():
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    strategy_id = f"pytest_auto_switch_{suffix}"
    other_strategy_id = f"{strategy_id}_other"
    symbol = "BTCUSDC"
    signal = "buy"
    timeframe = "1w"
    candle_id = "2026-05-18T00:00:00Z"
    other_candle_id = "2026-05-25T00:00:00Z"

    connection = market_phase.database._get_conn()
    with connection:
        connection.execute(market_phase.database.sql_create_auto_switch_signals_table)
        connection.execute(
            "DELETE FROM Auto_Switch_Signals WHERE strategy_id IN (?, ?)",
            (strategy_id, other_strategy_id),
        )

    try:
        assert not market_phase.database.auto_switch_signal_processed(
            strategy_id,
            symbol,
            signal,
            timeframe,
            candle_id,
        )

        market_phase.database.record_auto_switch_signal(
            strategy_id,
            symbol,
            signal,
            timeframe,
            candle_id,
        )

        assert market_phase.database.auto_switch_signal_processed(
            strategy_id,
            symbol,
            signal,
            timeframe,
            candle_id,
        )
        assert not market_phase.database.auto_switch_signal_processed(
            other_strategy_id,
            symbol,
            signal,
            timeframe,
            candle_id,
        )
        assert not market_phase.database.auto_switch_signal_processed(
            strategy_id,
            symbol,
            signal,
            timeframe,
            other_candle_id,
        )
    finally:
        with connection:
            connection.execute(
                "DELETE FROM Auto_Switch_Signals WHERE strategy_id IN (?, ?)",
                (strategy_id, other_strategy_id),
            )


def test_trade_summary_replaces_condition_not_fulfilled_telegram_spam(monkeypatch):
    sent = []
    sell_rows = pd.DataFrame(
        [
            {
                "Id": 1,
                "Symbol": "AAAUSDC",
                "Strategy_Id": "market_phases",
                "Strategy_Name": "Market Phases",
                "Buy_Price": 90.0,
                "Highest_Price_Since_Entry": 100.0,
                "Trail_Stop_ATR": 0.0,
            }
        ]
    )
    buy_rows = pd.DataFrame(
        [
            {
                "Id": 2,
                "Symbol": "BBBUSDC",
                "Strategy_Id": "market_phases",
                "Strategy_Name": "Market Phases",
            }
        ]
    )

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(
        main.database,
        "get_positions_by_bot_position",
        lambda bot, position: sell_rows if position == 1 else buy_rows,
    )
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(main.database, "update_position_risk", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "update_position_pnl", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "get_position_executed_take_profit_levels", lambda position_id: [])
    monkeypatch.setattr(main, "get_current_pnl", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        main,
        "get_runtime_risk_settings",
        lambda *args, **kwargs: {
            "atr_trailing_enabled": False,
            "atr_period": 14,
            "atr_multiplier": 1.8,
            "atr_activation_pnl": 2.0,
            "stop_loss": 0.0,
            "take_profit_enabled": False,
            "take_profits": [],
        },
    )
    monkeypatch.setattr(main, "get_data", lambda symbol, timeframe: _market_data())
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: sent.append(args))

    main.telegram_prefix_sl = "1h "
    main.trade("1h", "prod", settings=_settings())

    messages = [args[2] for args in sent]
    assert len(messages) == 1
    assert "Cycle" in messages[0]
    assert "No action: sell 1, buy 1" in messages[0]
    assert not any("condition not fulfilled" in msg for msg in messages)


def test_trade_cycle_survives_declarative_hma_routine_logs(monkeypatch):
    sent = []
    hma_definition = get_builtin_template("hma_rsi_linreg")
    sell_rows = pd.DataFrame(
        [
            {
                "Id": 1,
                "Symbol": "AAAUSDC",
                "Strategy_Id": "hma_rsi_linreg",
                "Strategy_Name": "HMA RSI LINREG",
                "Buy_Price": 90.0,
                "Highest_Price_Since_Entry": 100.0,
                "Trail_Stop_ATR": 0.0,
            }
        ]
    )
    buy_rows = pd.DataFrame(
        [
            {
                "Id": 2,
                "Symbol": "BBBUSDC",
                "Strategy_Id": "hma_rsi_linreg",
                "Strategy_Name": "HMA RSI LINREG",
            }
        ]
    )

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(
        main.database,
        "get_positions_by_bot_position",
        lambda bot, position: sell_rows if position == 1 else buy_rows,
    )
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(main.database, "update_position_risk", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "update_position_pnl", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "get_position_executed_take_profit_levels", lambda position_id: [])
    monkeypatch.setattr(main.database, "get_strategy_definition", lambda strategy_id: hma_definition)
    monkeypatch.setattr(main, "get_current_pnl", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        main,
        "get_runtime_risk_settings",
        lambda *args, **kwargs: {
            "atr_trailing_enabled": False,
            "atr_period": 14,
            "atr_multiplier": 1.8,
            "atr_activation_pnl": 2.0,
            "stop_loss": 0.0,
            "take_profit_enabled": False,
            "take_profits": [],
        },
    )
    monkeypatch.setattr(main, "get_data", lambda symbol, timeframe: _market_data())
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: sent.append(args))

    settings = _settings(routine_mode="detailed")
    settings.main_strategies = ["hma_rsi_linreg"]
    main.telegram_prefix_sl = "1h "
    summary = main.trade("1h", "prod", settings=settings, send_summary=False)

    assert summary["sell_evaluated"] == 1
    assert summary["buy_evaluated"] == 1
    assert not any("HMAFast" in str(args) for args in sent)


def test_trade_calls_exchange_adapter_even_in_test_mode(monkeypatch):
    sell_rows = pd.DataFrame([{"Id": 1, "Symbol": "AAAUSDC", "Strategy_Id": "market_phases", "Strategy_Name": "Market Phases", "Buy_Price": 90.0}])
    buy_rows = pd.DataFrame([{"Id": 2, "Symbol": "BBBUSDC", "Strategy_Id": "market_phases", "Strategy_Name": "Market Phases", "Strategy_Params_JSON": _strategy_params()}])
    order_calls = []

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(main.database, "get_positions_by_bot_position", lambda bot, position: sell_rows if position == 1 else buy_rows)
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(main.database, "update_position_risk", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "update_position_pnl", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "get_position_executed_take_profit_levels", lambda position_id: [])
    monkeypatch.setattr(
        main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame([{"Backtest_Config_JSON": "{}"}]),
    )
    monkeypatch.setattr(main.database, "is_backtest_approved", lambda timeframe, row: (True, []))
    monkeypatch.setattr(main, "get_current_pnl", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(main, "get_runtime_risk_settings", lambda *args, **kwargs: {"atr_trailing_enabled": False, "atr_period": 14, "atr_multiplier": 1.8, "atr_activation_pnl": 2.0, "stop_loss": 0.0, "take_profit_enabled": False, "take_profits": []})
    monkeypatch.setattr(main, "get_data", lambda symbol, timeframe: _market_data())
    monkeypatch.setattr(main, "get_strategy_sell_condition", lambda *args, **kwargs: True)
    monkeypatch.setattr(main, "get_strategy_buy_condition", lambda *args, **kwargs: (True, "Entry: True"))
    monkeypatch.setattr(main.binance, "create_sell_order", lambda *args, **kwargs: order_calls.append(("sell", args, kwargs)))
    monkeypatch.setattr(main.binance, "create_buy_order", lambda *args, **kwargs: order_calls.append(("buy", args, kwargs)))
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: None)

    summary = main.trade("1h", "test", settings=_settings(), send_summary=False)

    assert summary["sell_actions"] == 1
    assert summary["buy_actions"] == 1
    assert [call[0] for call in order_calls] == ["sell", "buy"]


def test_trade_prod_mode_calls_exchange_orders(monkeypatch):
    sell_rows = pd.DataFrame([{"Id": 1, "Symbol": "AAAUSDC", "Strategy_Id": "market_phases", "Strategy_Name": "Market Phases", "Buy_Price": 90.0}])
    buy_rows = pd.DataFrame([{"Id": 2, "Symbol": "BBBUSDC", "Strategy_Id": "market_phases", "Strategy_Name": "Market Phases", "Strategy_Params_JSON": _strategy_params()}])
    order_calls = []

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(main.database, "get_positions_by_bot_position", lambda bot, position: sell_rows if position == 1 else buy_rows)
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(main.database, "update_position_risk", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "update_position_pnl", lambda **kwargs: None)
    monkeypatch.setattr(main.database, "get_position_executed_take_profit_levels", lambda position_id: [])
    monkeypatch.setattr(
        main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame([{"Backtest_Config_JSON": "{}"}]),
    )
    monkeypatch.setattr(main.database, "is_backtest_approved", lambda timeframe, row: (True, []))
    monkeypatch.setattr(main, "get_current_pnl", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(main, "get_runtime_risk_settings", lambda *args, **kwargs: {"atr_trailing_enabled": False, "atr_period": 14, "atr_multiplier": 1.8, "atr_activation_pnl": 2.0, "stop_loss": 0.0, "take_profit_enabled": False, "take_profits": []})
    monkeypatch.setattr(main, "get_data", lambda symbol, timeframe: _market_data())
    monkeypatch.setattr(main, "get_strategy_sell_condition", lambda *args, **kwargs: True)
    monkeypatch.setattr(main, "get_strategy_buy_condition", lambda *args, **kwargs: (True, "Entry: True"))
    monkeypatch.setattr(main.binance, "create_sell_order", lambda *args, **kwargs: order_calls.append(("sell", args, kwargs)))
    monkeypatch.setattr(main.binance, "create_buy_order", lambda *args, **kwargs: order_calls.append(("buy", args, kwargs)))
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: None)

    summary = main.trade("1h", "prod", settings=_settings(), send_summary=False)

    assert summary["sell_actions"] == 1
    assert summary["buy_actions"] == 1
    assert [call[0] for call in order_calls] == ["sell", "buy"]


def test_trade_skips_buy_without_strategy_params_snapshot(monkeypatch):
    buy_rows = pd.DataFrame(
        [{"Id": 2, "Symbol": "BBBUSDC", "Strategy_Id": "market_phases", "Strategy_Name": "Market Phases"}]
    )
    order_calls = []

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(
        main.database,
        "get_positions_by_bot_position",
        lambda bot, position: pd.DataFrame() if position == 1 else buy_rows,
    )
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(main.binance, "create_buy_order", lambda *args, **kwargs: order_calls.append(("buy", args, kwargs)))
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: None)

    summary = main.trade("1h", "prod", settings=_settings(), send_summary=False)

    assert summary["buy_evaluated"] == 1
    assert summary["buy_actions"] == 0
    assert summary["buy_no_action"] == 1
    assert order_calls == []


def test_trade_skips_buy_without_approved_backtest(monkeypatch):
    buy_rows = pd.DataFrame(
        [
            {
                "Id": 2,
                "Symbol": "BBBUSDC",
                "Strategy_Id": "market_phases",
                "Strategy_Name": "Market Phases",
                "Strategy_Params_JSON": _strategy_params(),
            }
        ]
    )
    order_calls = []

    monkeypatch.setattr(main.database, "delete_positions_not_top_rank", lambda: None)
    monkeypatch.setattr(
        main.database,
        "get_positions_by_bot_position",
        lambda bot, position: pd.DataFrame() if position == 1 else buy_rows,
    )
    monkeypatch.setattr(main.database, "is_trade_main_timeframe_enabled", lambda timeframe: True)
    monkeypatch.setattr(
        main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame([{"Backtest_Config_JSON": "{}"}]),
    )
    monkeypatch.setattr(main.database, "is_backtest_approved", lambda timeframe, row: (False, ["Quality_Grade_Min"]))
    monkeypatch.setattr(main.binance, "create_buy_order", lambda *args, **kwargs: order_calls.append(("buy", args, kwargs)))
    monkeypatch.setattr(main.telegram, "send_telegram_message", lambda *args, **kwargs: None)

    summary = main.trade("1h", "prod", settings=_settings(), send_summary=False)

    assert summary["buy_evaluated"] == 1
    assert summary["buy_actions"] == 0
    assert summary["buy_no_action"] == 1
    assert order_calls == []


def test_binance_order_functions_abort_in_test_mode(monkeypatch):
    get_client_calls = []
    settings = _settings()
    settings.run_mode = "test"

    monkeypatch.setattr(binance.config, "load_settings", lambda: settings)
    monkeypatch.setattr(binance, "get_client", lambda: get_client_calls.append(True))

    assert binance.create_buy_order("AAAUSDC", "1h") is None
    assert binance.create_sell_order("AAAUSDC", "1h") is None
    assert get_client_calls == []


def test_warning_messages_are_mirrored_to_errors(monkeypatch):
    posts = []

    class FakeResponse:
        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        telegram.requests,
        "post",
        lambda url, params=None, timeout=None: posts.append((url, params, timeout)) or FakeResponse(),
    )
    monkeypatch.setattr(telegram, "telegram_chat_id", "chat")
    monkeypatch.setattr(telegram, "telegram_token_errors", "errors-token")
    monkeypatch.setattr(telegram, "bot_prefix", "BEC")

    telegram.send_telegram_message("main-token", telegram.EMOJI_WARNING, "broken")

    urls = [post[0] for post in posts]
    assert any("boterrors-token/sendMessage" in url for url in urls)
    assert any("botmain-token/sendMessage" in url for url in urls)


def test_closed_position_alert_is_copied_to_closed_position_bot(monkeypatch):
    posts = []

    class FakeResponse:
        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        telegram.requests,
        "post",
        lambda url, params=None, timeout=None: posts.append((url, params, timeout)) or FakeResponse(),
    )
    monkeypatch.setattr(telegram, "telegram_chat_id", "chat")
    monkeypatch.setattr(telegram, "telegram_token_closed_position", "closed-token")
    monkeypatch.setattr(telegram, "bot_prefix", "BEC")
    monkeypatch.setattr(telegram, "trade_against", "USDC")

    telegram.send_telegram_alert(
        telegram_token="main-token",
        telegram_prefix="1h ",
        emoji=telegram.EMOJI_TRADE_WITH_PROFIT,
        date=datetime(2026, 1, 1, 12, 0, 0),
        symbol="BTCUSDC",
        timeframe="1h",
        strategy="Test Strategy",
        ordertype="SELL",
        unitValue=100.0,
        amount=1.0,
        trade_against_value=100.0,
        pnlPerc=5.0,
        pnl_trade_against=5.0,
        exit_reason="Take-Profit",
    )

    urls = [post[0] for post in posts]
    assert any("botmain-token/sendMessage" in url for url in urls)
    assert any("botclosed-token/sendMessage" in url for url in urls)


def test_trade_event_formatting_and_closed_position_copy(monkeypatch):
    sent = []
    monkeypatch.setattr(telegram, "send_telegram_message", lambda *args, **kwargs: sent.append(args))
    monkeypatch.setattr(telegram, "trade_against", "USDC")

    buy_msg = telegram.format_trade_event(
        action="BUY",
        symbol="BTCUSDC",
        timeframe="1h",
        strategy="20/60 EMA Cross",
        reason="Entry condition fulfilled",
        unit_price=100.0,
        quantity=0.5,
        notional_value=50.0,
        open_positions="3/20",
    )

    assert "1h BUY BTCUSDC" in buy_msg
    assert "Strategy: 20/60 EMA Cross" in buy_msg
    assert "Price: 100.0" in buy_msg
    assert "Qty: 0.5" in buy_msg
    assert "Open positions: 3/20" in buy_msg

    telegram.send_trade_event(
        telegram_token="main-token",
        telegram_prefix="1h ",
        emoji=telegram.EMOJI_TRADE_WITH_PROFIT,
        action="SELL",
        symbol="BTCUSDC",
        timeframe="1h",
        strategy="20/60 EMA Cross",
        reason="Take-Profit",
        unit_price=110.0,
        quantity=0.5,
        notional_value=55.0,
        pnl_perc=10.0,
        pnl_value=5.0,
    )

    assert len(sent) == 2
    assert sent[0][0] == "main-token"
    assert sent[1][0] == telegram.telegram_token_closed_position
    assert sent[0][2].startswith("1h SELL BTCUSDC")
    assert "1h 1h SELL" not in sent[0][2]
    assert "PnL%: 10.0" in sent[0][2]


def test_strategy_setup_is_preserved_in_trade_event_name():
    definition = get_builtin_template("hma_rsi_linreg")
    strategy_params_json = json.dumps(
        {
            "engine": "bec_strategy_ast_v2",
            "definition": definition,
            "parameters": {"hma_fast": 30, "hma_slow": 90},
        }
    )

    strategy_name = binance._format_strategy_name_with_setup(
        "hma_rsi_linreg",
        "HMA RSI LINREG",
        strategy_params_json,
    )

    assert strategy_name == "30/90 HMA RSI LINREG"
    assert (
        binance._format_strategy_name_with_setup(
            "hma_rsi_linreg",
            strategy_name,
            strategy_params_json,
        )
        == "30/90 HMA RSI LINREG"
    )


def test_positions_summary_is_compact_and_sorted(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "Symbol": "WORSTUSDC",
                "Bot": "1h",
                "Strategy_Name": "Worst Strategy",
                "PnL_Perc": -2.0,
                "PnL_Value": -4.0,
                "Position_Value": 200.0,
                "Duration": "2d",
            },
            {
                "Symbol": "BESTUSDC",
                "Bot": "1h",
                "Strategy_Name": "Best Strategy",
                "PnL_Perc": 5.0,
                "PnL_Value": 10.0,
                "Position_Value": 200.0,
                "Duration": "1d",
            },
        ]
    )
    monkeypatch.setattr(telegram_reporting.database, "get_unrealized_pnl_by_bot", lambda timeframe: df)

    msg = telegram_reporting.format_positions_summary("1h", settings=_settings())

    assert "Open: 2/20" in msg
    assert "uPnL: +6.00 USDC (+1.50%)" in msg
    assert "W/L: 1/1" in msg
    assert msg.index("BESTUSDC") < msg.index("WORSTUSDC")
    assert "1. BESTUSDC | PnL +5.00% | +10.00 USDC | Best Strategy | 1d" in msg


def test_positions_summary_empty_state(monkeypatch):
    monkeypatch.setattr(
        telegram_reporting.database,
        "get_unrealized_pnl_by_bot",
        lambda timeframe: pd.DataFrame(),
    )

    assert telegram_reporting.format_positions_summary("4h", settings=_settings()) == "Positions: no open positions"


def test_daily_summary_counts_orders_and_open_pnl(monkeypatch):
    now_utc = datetime(2026, 5, 17, tzinfo=timezone.utc)
    sells = pd.DataFrame(
        [
            {
                "Symbol": "BESTUSDC",
                "PnL_Perc": 4.0,
                "PnL_Value": 8.0,
                "Sell_Position_Value": 200.0,
                "Stop_Type": "tp",
                "Exit_Reason": "Take-Profit Level 1",
            },
            {
                "Symbol": "WORSTUSDC",
                "PnL_Perc": -2.0,
                "PnL_Value": -2.0,
                "Sell_Position_Value": 100.0,
                "Stop_Type": "hard_sl",
                "Exit_Reason": "Stop loss 10%",
            },
        ]
    )
    buys = pd.DataFrame([{"Symbol": "NEWUSDC"}, {"Symbol": "OTHERUSDC"}])
    open_positions = pd.DataFrame(
        [{"Symbol": "OPENUSDC", "PnL_Perc": 3.0, "PnL_Value": 6.0, "Position_Value": 200.0}]
    )

    def fake_orders(side, start_utc, end_utc):
        assert start_utc == now_utc - timedelta(days=1)
        assert end_utc == now_utc
        return buys if side == "BUY" else sells

    monkeypatch.setattr(telegram_reporting, "get_orders_by_side_date_range", fake_orders)
    monkeypatch.setattr(
        telegram_reporting.database,
        "get_unrealized_pnl_by_bot",
        lambda timeframe: open_positions if timeframe == "1h" else pd.DataFrame(),
    )

    msg = telegram_reporting.format_daily_summary(now_utc=now_utc, settings=_settings())

    assert "Period: 2026-05-16 UTC" in msg
    assert "Buys opened: 2" in msg
    assert "Sells closed: 2" in msg
    assert "Realized PnL: +6.00 (+2.00%)" in msg
    assert "Take profits / Stops / Strategy exits: 1/1/0" in msg
    assert "Open PnL: +6.00 (+3.00%)" in msg
    assert "Best closed: BESTUSDC +4.00% (+8.00)" in msg
    assert "Worst closed: WORSTUSDC -2.00% (-2.00)" in msg


def test_error_event_sends_detail_to_errors_and_short_notice_to_main(monkeypatch):
    sent = []
    monkeypatch.setattr(telegram, "send_telegram_message", lambda *args, **kwargs: sent.append(args))

    telegram.send_error_event(
        action="create sell order",
        symbol="BTCUSDC",
        timeframe="1h",
        strategy="EMA",
        reason="min notional",
        impact="Sell order was not placed.",
        next_step="Check balance.",
        exception=ValueError("too small"),
        main_token="main-token",
        main_prefix="1h ",
    )

    assert len(sent) == 2
    assert sent[0][0] == telegram.telegram_token_errors
    assert sent[0][1] == ""
    assert "Action: create sell order" in sent[0][2]
    assert "Impact: Sell order was not placed." in sent[0][2]
    assert sent[1][0] == "main-token"
    assert sent[1][1] == telegram.EMOJI_INFORMATION
    assert "See Errors channel" in sent[1][2]


def test_bb_width_sends_only_actionable_signal_to_telegram(monkeypatch):
    sent = []
    logged = []
    df = _market_data(rows=260)

    monkeypatch.setattr(bb_width, "get_data", lambda symbol, time_frame, start_date: df)
    monkeypatch.setattr(bb_width.database, "add_signal_log", lambda **kwargs: logged.append(kwargs))
    monkeypatch.setattr(
        bb_width.telegram,
        "send_telegram_message",
        lambda *args, **kwargs: sent.append(args),
    )

    bb_width._check_bb_width("BTCUSDC", "1d", "1D", 260)

    assert len(sent) == 1
    assert sent[0][0] == bb_width.telegram.telegram_token_signals
    assert "Bollinger Bands Width alert!" in sent[0][2]
    assert len(logged) == 1


def test_market_phase_report_is_compact_and_limits_top_performers():
    df_result = pd.DataFrame(
        [
            {"Symbol": "AUSDC", "Market_Phase": "bullish", "Perc_Above_DSMA200": 10.0},
            {"Symbol": "BUSDC", "Market_Phase": "accumulation", "Perc_Above_DSMA200": 8.0},
            {"Symbol": "CUSDC", "Market_Phase": "recovery", "Perc_Above_DSMA200": 6.0},
            {"Symbol": "DUSDC", "Market_Phase": "warning", "Perc_Above_DSMA200": 4.0},
            {"Symbol": "EUSDC", "Market_Phase": "distribution", "Perc_Above_DSMA200": 2.0},
            {"Symbol": "FUSDC", "Market_Phase": "bearish", "Perc_Above_DSMA200": -1.0},
        ]
    )
    df_top = pd.DataFrame(
        [
            {"Symbol": f"S{i}USDC", "Market_Phase": "bullish", "Perc_Above_DSMA200": 20 - i}
            for i in range(1, 8)
        ]
    )

    msg = telegram_reporting.format_market_phase_report(
        timeframe="1d",
        trade_against="USDC",
        duration="42s",
        symbols_scanned=6,
        df_result=df_result,
        df_top=df_top,
        backtesting_stats={
            "symbols_pending": 3,
            "strategies_tested": 2,
            "backtest_runs": 18,
            "approved_candidates": 5,
            "rejected_candidates": 13,
        },
        warnings=2,
    )

    assert "MKT Report" in msg
    assert "Symbols scanned: 6" in msg
    assert "Positive phases: 2" in msg
    assert "Warnings: 2 | See Errors channel" in msg
    assert "Runs: 18" in msg
    assert "Approved: 5" in msg
    assert "Rejected: 13" in msg
    assert "5. S5USDC" in msg
    assert "S6USDC" not in msg
    assert "TradingView list attached." not in msg


def test_trade_against_switch_event_is_actionable():
    msg = telegram_reporting.format_trade_against_switch_event(
        direction="USDC -> BTC",
        reason="BTCUSDC entered a bullish/accumulation regime.",
        actions=[
            "Sell all open positions to USDC.",
            "Convert available USDC balance to BTC.",
            "Update trade_against to BTC.",
        ],
    )

    assert "Trade Against Switch" in msg
    assert "Direction: USDC -> BTC" in msg
    assert "Reason: BTCUSDC entered a bullish/accumulation regime." in msg
    assert "- Convert available USDC balance to BTC." in msg


def test_btc_auto_switch_backtest_uses_refresh_cache(monkeypatch):
    settings = _auto_switch_settings(btc_strategy="btc_strategy")
    strategies = pd.DataFrame(
        [{"Id": "btc_strategy", "Name": "BTC Strategy", "Backtest_Optimize": False}]
    )
    existing_result = pd.DataFrame(
        [
            {
                "Backtest_Work_Fingerprint": "btc-fingerprint",
                "Backtest_Work_Executed_At": "2026-05-22T00:00:00Z",
            }
        ]
    )
    calc_calls = []

    monkeypatch.setattr(market_phase.database, "get_strategies_for_btc", lambda: strategies)
    monkeypatch.setattr(
        market_phase.database,
        "get_backtesting_settings",
        lambda: {"Candidate_Backtest_Refresh_Days": 7},
    )
    monkeypatch.setattr(
        market_phase.database,
        "build_backtesting_work_fingerprint",
        lambda *args, **kwargs: "btc-fingerprint",
    )
    monkeypatch.setattr(
        market_phase.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: existing_result,
    )
    monkeypatch.setattr(
        market_phase.add_symbol,
        "_backtest_result_current",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        market_phase.importlib,
        "import_module",
        lambda name: SimpleNamespace(btc_strategy=object()),
    )
    monkeypatch.setattr(
        market_phase,
        "calc_backtesting",
        lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True,
    )

    stats = market_phase._run_btc_auto_switch_backtest_if_needed(settings, "1d")

    assert calc_calls == []
    assert stats == {"evaluated": 1, "runs": 0, "skipped": 1, "failed": 0}


def test_btc_auto_switch_backtest_runs_when_cache_is_stale(monkeypatch):
    settings = _auto_switch_settings(btc_strategy="btc_strategy")
    strategy_impl = object()
    strategies = pd.DataFrame(
        [{"Id": "btc_strategy", "Name": "BTC Strategy", "Backtest_Optimize": True}]
    )
    calc_calls = []

    monkeypatch.setattr(market_phase.database, "get_strategies_for_btc", lambda: strategies)
    monkeypatch.setattr(
        market_phase.database,
        "get_backtesting_settings",
        lambda: {"Candidate_Backtest_Refresh_Days": 7},
    )
    monkeypatch.setattr(
        market_phase.database,
        "build_backtesting_work_fingerprint",
        lambda *args, **kwargs: "btc-fingerprint",
    )
    monkeypatch.setattr(
        market_phase.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        market_phase.add_symbol,
        "_backtest_result_current",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        market_phase.importlib,
        "import_module",
        lambda name: SimpleNamespace(btc_strategy=strategy_impl),
    )
    monkeypatch.setattr(
        market_phase,
        "calc_backtesting",
        lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True,
    )

    stats = market_phase._run_btc_auto_switch_backtest_if_needed(settings, "4h")

    assert len(calc_calls) == 1
    assert calc_calls[0][1] == {
        "symbol": "BTCUSDC",
        "time_frame": "4h",
        "strategy": strategy_impl,
        "optimize": True,
    }
    assert stats == {"evaluated": 1, "runs": 1, "skipped": 0, "failed": 0}


def test_add_symbol_returns_stats_without_telegram_progress(monkeypatch):
    settings = SimpleNamespace(main_strategies=["fake_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "fake_strategy", "Name": "Fake Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame([{"Backtest_Config_JSON": "{}"}])
    calc_calls = []

    monkeypatch.setattr(add_symbol.config, "load_settings", lambda refresh=True: settings)
    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(add_symbol.database, "is_trade_main_timeframe_enabled", lambda tf: True)
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(fake_strategy=object()),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_work_metadata", lambda **kwargs: None)
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(
        add_symbol.database,
        "is_backtest_approved",
        lambda tf, row: (tf != "4h", ["Rejected"] if tf == "4h" else []),
    )
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "get_rank_from_symbols_by_market_phase_by_symbol", lambda symbol: 1)
    monkeypatch.setattr(add_symbol.database, "set_rank_from_positions", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_results_from_position_strategy", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert len(calc_calls) == 3
    assert stats == {
        "symbols_pending": 1,
        "strategies_tested": 1,
        "backtest_runs": 3,
        "approved_candidates": 2,
        "rejected_candidates": 1,
    }


def test_add_symbol_backtests_only_selected_main_strategies(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [
            {"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False},
            {"Id": "unselected_strategy", "Name": "Unselected Strategy", "Backtest_Optimize": True},
        ]
    )
    result_row = pd.DataFrame([{"Backtest_Config_JSON": "{}"}])
    calc_calls = []
    selected_strategy = object()
    unselected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(add_symbol.database, "is_trade_main_timeframe_enabled", lambda tf: True)
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(
            selected_strategy=selected_strategy,
            unselected_strategy=unselected_strategy,
        ),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_work_metadata", lambda **kwargs: None)
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert len(calc_calls) == 3
    assert {kwargs["strategy"] for _args, kwargs in calc_calls} == {selected_strategy}
    assert all(call[0][0] == "AAAUSDC" for call in calc_calls)
    assert stats["strategies_tested"] == 1
    assert stats["backtest_runs"] == 3
    assert stats["approved_candidates"] == 3


def test_add_symbol_keeps_disabled_timeframes_with_open_positions(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame([{"Backtest_Config_JSON": "{}"}])
    calc_calls = []
    selected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(
        add_symbol.database,
        "is_trade_main_timeframe_enabled",
        lambda tf: tf in {"4h", "1h"},
    )
    monkeypatch.setattr(
        add_symbol.database,
        "get_num_open_positions_by_bot",
        lambda tf: 1 if tf == "1d" else 0,
    )
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(selected_strategy=selected_strategy),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_work_metadata", lambda **kwargs: None)
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert [args[1] for args, _kwargs in calc_calls] == ["1d", "4h", "1h"]
    assert stats["backtest_runs"] == 3


def test_add_symbol_skips_disabled_timeframes_with_no_open_positions(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame([{"Backtest_Config_JSON": "{}"}])
    calc_calls = []
    selected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(
        add_symbol.database,
        "is_trade_main_timeframe_enabled",
        lambda tf: tf in {"4h", "1h"},
    )
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(selected_strategy=selected_strategy),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_work_metadata", lambda **kwargs: None)
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert [args[1] for args, _kwargs in calc_calls] == ["4h", "1h"]
    assert stats["backtest_runs"] == 2


def test_add_symbol_skips_current_backtesting_result(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame(
        [
            {
                "Backtest_Config_JSON": "{}",
                "Backtest_Work_Fingerprint": "fingerprint",
                "Backtest_Work_Candle": "candle",
                "Backtest_Work_Executed_At": "2026-05-22T00:00:00Z",
            }
        ]
    )
    calc_calls = []
    metadata_calls = []
    insert_calls = []
    selected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(add_symbol.database, "is_trade_main_timeframe_enabled", lambda tf: tf == "1d")
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(add_symbol.database, "get_backtesting_settings", lambda: {"Candidate_Backtest_Refresh_Days": 7})
    monkeypatch.setattr(add_symbol.database, "build_backtesting_work_fingerprint", lambda *args, **kwargs: "fingerprint")
    monkeypatch.setattr(add_symbol, "_latest_backtest_candle", lambda *args, **kwargs: "candle")
    monkeypatch.setattr(
        add_symbol,
        "datetime",
        SimpleNamespace(now=lambda timezone: pd.Timestamp("2026-05-23T00:00:00Z").to_pydatetime()),
    )
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(selected_strategy=selected_strategy),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(
        add_symbol.database,
        "set_backtesting_work_metadata",
        lambda **kwargs: metadata_calls.append(kwargs),
    )
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: insert_calls.append(kwargs))
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert calc_calls == []
    assert metadata_calls == []
    assert len(insert_calls) == 1
    assert stats["backtest_runs"] == 0
    assert stats["approved_candidates"] == 1


def test_add_symbol_refreshes_expired_backtesting_result(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame(
        [
            {
                "Backtest_Config_JSON": "{}",
                "Backtest_Work_Fingerprint": "fingerprint",
                "Backtest_Work_Candle": "old-candle",
                "Backtest_Work_Executed_At": "2026-05-01T00:00:00Z",
            }
        ]
    )
    calc_calls = []
    metadata_calls = []
    selected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(add_symbol.database, "is_trade_main_timeframe_enabled", lambda tf: tf == "1d")
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(add_symbol.database, "get_backtesting_settings", lambda: {"Candidate_Backtest_Refresh_Days": 7})
    monkeypatch.setattr(add_symbol.database, "build_backtesting_work_fingerprint", lambda *args, **kwargs: "fingerprint")
    monkeypatch.setattr(add_symbol, "_latest_backtest_candle", lambda *args, **kwargs: "new-candle")
    monkeypatch.setattr(
        add_symbol,
        "datetime",
        SimpleNamespace(now=lambda timezone: pd.Timestamp("2026-05-22T00:00:00Z").to_pydatetime()),
    )
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(selected_strategy=selected_strategy),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) or True)
    monkeypatch.setattr(
        add_symbol.database,
        "set_backtesting_work_metadata",
        lambda **kwargs: metadata_calls.append(kwargs),
    )
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: None)
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert len(calc_calls) == 1
    assert metadata_calls[0]["work_candle"] == "new-candle"
    assert stats["backtest_runs"] == 1


def test_add_symbol_does_not_checkpoint_failed_backtest(monkeypatch):
    settings = SimpleNamespace(main_strategies=["selected_strategy"])
    pending = pd.DataFrame([{"Symbol": "AAAUSDC"}])
    strategies = pd.DataFrame(
        [{"Id": "selected_strategy", "Name": "Selected Strategy", "Backtest_Optimize": False}]
    )
    result_row = pd.DataFrame(
        [
            {
                "Backtest_Config_JSON": "{}",
                "Backtest_Work_Fingerprint": "old-fingerprint",
                "Backtest_Work_Candle": "old-candle",
                "Backtest_Work_Executed_At": "2026-05-01T00:00:00Z",
            }
        ]
    )
    calc_calls = []
    metadata_calls = []
    approval_calls = []
    insert_calls = []
    selected_strategy = object()

    monkeypatch.setattr(add_symbol.database, "get_symbols_to_calc_by_calc_completed", lambda completed: pending)
    monkeypatch.setattr(add_symbol.database, "get_strategies_for_main", lambda: strategies)
    monkeypatch.setattr(add_symbol.database, "is_trade_main_timeframe_enabled", lambda tf: tf == "1d")
    monkeypatch.setattr(add_symbol.database, "get_num_open_positions_by_bot", lambda tf: 0)
    monkeypatch.setattr(add_symbol.database, "get_backtesting_settings", lambda: {"Candidate_Backtest_Refresh_Days": 7})
    monkeypatch.setattr(add_symbol.database, "build_backtesting_work_fingerprint", lambda *args, **kwargs: "new-fingerprint")
    monkeypatch.setattr(add_symbol, "_latest_backtest_candle", lambda *args, **kwargs: "new-candle")
    monkeypatch.setattr(
        add_symbol.importlib,
        "import_module",
        lambda name: SimpleNamespace(selected_strategy=selected_strategy),
    )
    monkeypatch.setattr(add_symbol, "calc_backtesting", lambda *args, **kwargs: calc_calls.append((args, kwargs)) and False)
    monkeypatch.setattr(add_symbol.database, "set_backtesting_work_metadata", lambda **kwargs: metadata_calls.append(kwargs))
    monkeypatch.setattr(
        add_symbol.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: result_row,
    )
    monkeypatch.setattr(add_symbol.database, "set_backtesting_approval", lambda **kwargs: approval_calls.append(kwargs))
    monkeypatch.setattr(add_symbol.database, "is_backtest_approved", lambda tf, row: (True, []))
    monkeypatch.setattr(add_symbol.database, "build_strategy_params_json_from_backtest_result", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(add_symbol.database, "get_all_positions_by_bot_symbol_strategy", lambda **kwargs: False)
    monkeypatch.setattr(add_symbol.database, "insert_position", lambda **kwargs: insert_calls.append(kwargs))
    monkeypatch.setattr(add_symbol.database, "set_symbols_to_calc_completed", lambda symbol: None)

    stats = add_symbol.run(settings=settings)

    assert len(calc_calls) == 1
    assert metadata_calls == []
    assert insert_calls == []
    assert approval_calls[0]["trading_approved"] is False
    assert approval_calls[0]["trading_rejection_reasons"] == "Backtest_Failed"
    assert stats["backtest_runs"] == 1
    assert stats["approved_candidates"] == 0
    assert stats["rejected_candidates"] == 1


def test_market_phase_empty_data_routes_to_errors_only(monkeypatch):
    import bec.symbol_by_market_phase as market_phase

    errors = []
    main_messages = []
    warning_stats = {"warnings": 0}

    monkeypatch.setattr(market_phase.binance, "get_close_df", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr(market_phase.telegram, "send_error_event", lambda **kwargs: errors.append(kwargs))
    monkeypatch.setattr(
        market_phase.telegram,
        "send_telegram_message",
        lambda *args, **kwargs: main_messages.append(args),
    )

    result = market_phase.set_market_phases_to_symbols(["AAAUSDC"], "1d", warning_stats=warning_stats)

    assert result.empty
    assert warning_stats["warnings"] == 1
    assert errors[0]["action"] == "market phase OHLCV load"
    assert errors[0]["notify_main"] is False
    assert main_messages == []
