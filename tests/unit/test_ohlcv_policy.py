"""Shared history boundaries without exchange, order, or Telegram side effects."""

from types import SimpleNamespace

import pandas as pd
import pytest

import bec.utils.database as database
import bec.main as main
import bec.symbol_by_market_phase as market_phase
from bec.exchanges.market_data_batch import fetch_recent_ohlcv_batch
from bec.exchanges.base import OhlcvBatchPolicy
from bec.strategy_builder import engine
from bec.strategy_builder.templates import get_builtin_template


def _history(count):
    prices = pd.Series(range(1, count + 1), dtype=float).to_numpy()
    return pd.DataFrame(
        {"Open": prices, "High": prices + 1, "Low": prices - 1,
         "Close": prices, "Volume": 1.0},
        index=pd.date_range("2025-01-01", periods=count, freq="D"),
    )


def _phase_history(frame, symbol):
    return frame[["Close"]].rename(columns={"Close": "Price"}).assign(Symbol=symbol)


@pytest.mark.parametrize("count", [199, 200])
def test_shared_boundary_for_phase_and_individual_live_load(monkeypatch, count):
    frame = _history(count)
    requests = []
    errors = []

    def fetch(**kwargs):
        requests.append(kwargs["required_closed_candles"])
        return frame.copy()

    monkeypatch.setattr(main.binance, "get_recent_ohlcv", fetch)
    monkeypatch.setattr(
        market_phase.exchange_service, "get_recent_close_df",
        lambda **kwargs: _phase_history(fetch(**kwargs), kwargs["symbol"]),
    )
    monkeypatch.setattr(main.telegram, "send_error_event", lambda **kwargs: errors.append(kwargs))
    phase = market_phase.set_market_phases_to_symbols(["AAA/USDC"], "1d")
    data = main._TradeCycleMarketData({})
    loaded = data.load("AAA/USDC", "1d")
    pd.testing.assert_frame_equal(loaded, data.load("aaa/usdc", "1d"))
    data.send_failure_summary("1d")

    assert requests == [200, 200]  # The cached read makes no additional request.
    assert phase.empty == loaded.empty == (count < 200)
    assert len(errors) == (1 if count < 200 else 0)
    if errors:
        assert "199/200 candles" in errors[0]["reason"]


def test_phase_results_and_ranking_match_last_200_closed_candles(monkeypatch):
    frames = {"AAA/USDC": _history(201), "BBB/USDC": _history(201) + 50}
    monkeypatch.setattr(
        market_phase.exchange_service, "get_recent_close_df",
        lambda symbol, required_closed_candles, **kwargs: _phase_history(
            frames[symbol].tail(required_closed_candles), symbol
        ),
    )
    current = market_phase.set_market_phases_to_symbols(list(frames), "1d")
    monkeypatch.setattr(market_phase, "MARKET_PHASE_LOOKBACK_CANDLES", 201)
    previous = market_phase.set_market_phases_to_symbols(list(frames), "1d")
    pd.testing.assert_frame_equal(
        current.sort_values("Perc_Above_DSMA200", ascending=False),
        previous.sort_values("Perc_Above_DSMA200", ascending=False),
    )


@pytest.mark.parametrize("required,available", [(200, 199), (200, 200), (201, 200), (201, 201)])
def test_batch_obeys_effective_requirement_and_keeps_fetch_margin(required, available):
    calls = []

    class Adapter:
        ohlcv_batch_policy = OhlcvBatchPolicy(max_workers=1)

        def load_markets(self):
            return {}

        def fetch_ohlcv(self, symbol, timeframe, **kwargs):
            calls.append(kwargs)
            return _history(available)

    now = pd.Timestamp("2026-09-12", tz="UTC").to_pydatetime()
    result = fetch_recent_ohlcv_batch(
        {("AAA/USDC", "1d"): required}, now_utc=now,
        exchange={"code": "okx"}, adapter_factory=Adapter,
    )
    key = ("AAA/USDC", "1d")
    assert result.frames[key].empty == (available < required)
    assert (key in result.failures) == (available < required)
    assert calls[0]["drop_incomplete"] is True
    assert calls[0]["start_date"] == now - pd.Timedelta(days=required + 2)
    if result.failures:
        assert result.failures[key]["available"] == available
        assert result.failures[key]["required"] == required


def _mock_strategy(monkeypatch, definition, parameters=None):
    context = {
        "definition": definition,
        "parameters": parameters or {"sma_fast": 50, "sma_slow": 200},
        "order_first": 50, "order_second": 200, "setup": "50/200",
    }
    monkeypatch.setattr(database, "get_strategy_definition", lambda *args: definition)
    monkeypatch.setattr(main, "get_strategy_runtime_context", lambda *args, **kwargs: context)
    monkeypatch.setattr(main, "get_runtime_risk_settings", lambda *args, **kwargs: {})
    return context


@pytest.mark.parametrize("slow,timeframe,expected", [(200, "current", 200), (250, "current", 250), (200, "1d", 201)])
def test_live_requirements_preserve_strategy_warmup(monkeypatch, slow, timeframe, expected):
    definition = get_builtin_template("market_phases")
    for operand in engine.iter_operands(definition):
        if operand.get("type") in {"price", "indicator"}:
            operand["timeframe"] = timeframe
    _mock_strategy(monkeypatch, definition, {"sma_fast": 50, "sma_slow": slow})
    rows = pd.DataFrame([{"Symbol": "AAA/USDC", "Strategy_Id": "market_phases"}])
    requirements = main._live_history_requirements(pd.DataFrame(), rows, "1h", SimpleNamespace())
    assert requirements[("AAA/USDC", "1h")] == (expected if timeframe == "current" else 200)
    if timeframe == "1d":
        assert requirements[("AAA/USDC", "1d")] == expected


def test_ranked_symbol_with_200_candles_reaches_strategy_without_forcing_order(monkeypatch):
    frame = _history(200)
    monkeypatch.setattr(
        market_phase.exchange_service, "get_recent_close_df",
        lambda **kwargs: _phase_history(frame, kwargs["symbol"]),
    )
    ranked = market_phase.set_market_phases_to_symbols(["AAA/USDC"], "1d")
    assert ranked.iloc[0]["Market_Phase"] == "bullish"
    definition = get_builtin_template("market_phases")
    definition["entry"]["conditions"][0]["operator"] = "below"
    _mock_strategy(monkeypatch, definition)
    rows = pd.DataFrame([{
        "Id": 1, "Symbol": ranked.iloc[0]["Symbol"], "Strategy_Id": "market_phases",
        "Strategy_Name": "Market Phases",
        "Strategy_Params_JSON": '{"parameters": {"sma_fast": 50, "sma_slow": 200}}',
    }])
    settings = SimpleNamespace(telegram_routine_trade_logs="summary")
    requirements = main._live_history_requirements(pd.DataFrame(), rows, "1d", settings)
    cycle = main._PreparedTradeCycle(
        "1d", pd.DataFrame(), rows, requirements,
        main._new_trade_cycle_summary("1d", "summary"),
    )
    monkeypatch.setattr(main, "_has_approved_backtest", lambda *args: True)
    monkeypatch.setattr(main.binance, "get_recent_ohlcv", lambda **kwargs: frame.copy())
    monkeypatch.setattr(main.binance, "create_buy_order", lambda **kwargs: pytest.fail("Unexpected order"))
    monkeypatch.setattr(main.telegram, "send_error_event", lambda **kwargs: pytest.fail("Unexpected warning"))
    summary = main.trade("1d", "test", settings, send_summary=False, prepared_cycle=cycle)
    assert summary["buy_evaluated"] == summary["buy_no_action"] == 1
    assert summary["empty_data"] == summary["warnings"] == summary["buy_actions"] == 0
    assert "Buy condition not fulfilled | Entry: False" in summary["examples"][0]


def test_ema_uses_the_new_window_without_assuming_201_candle_equivalence():
    prices = _history(201)["Close"]
    window = prices.tail(200)
    actual = engine.ema(window, 200)
    pd.testing.assert_series_equal(actual, window.ewm(span=200, adjust=False).mean())
    assert actual.iloc[-1] != pytest.approx(engine.ema(prices, 200).iloc[-1])
