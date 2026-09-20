from types import SimpleNamespace

import pandas as pd

import bec.trade_batch as trade_batch
from bec.exchanges.market_data_batch import (
    OhlcvBatchMetrics,
    OhlcvBatchResult,
)


def _prepared(timeframe):
    return trade_batch.live_main._PreparedTradeCycle(
        timeframe=timeframe,
        df_sell=pd.DataFrame(),
        df_buy=pd.DataFrame(),
        requirements={(f"{timeframe.upper()}/USDC", timeframe): 201},
        summary=trade_batch.live_main._new_trade_cycle_summary(timeframe, "summary"),
    )


def _batch_result():
    return OhlcvBatchResult(
        frames={},
        failures={},
        metrics=OhlcvBatchMetrics(
            exchange_code="kraken",
            logical_requests=3,
            symbols=3,
            workers=3,
            elapsed_seconds=1.0,
            failures=0,
        ),
    )


def test_trade_cycle_summary_aggregates_capacity_blocks_once():
    summary = trade_batch.live_main._new_trade_cycle_summary("1h", "summary")
    summary["capacity_blocked"] = 3
    summary["capacity"] = {
        "open_positions": 24,
        "pending_buy_slots": 1,
        "max_positions": 25,
    }

    message = trade_batch.live_main.format_trade_cycle_summary(summary)

    assert message.count("Capacity blocked:") == 1
    assert "Capacity blocked: 3 | open 24 + pending 1 / max 25" in message


def test_run_batch_executes_all_sells_before_any_buy(monkeypatch):
    events = []
    monkeypatch.setattr(
        trade_batch.config,
        "load_settings",
        lambda refresh=True: SimpleNamespace(run_mode="test"),
    )
    monkeypatch.setattr(
        trade_batch.database,
        "delete_positions_not_top_rank",
        lambda: events.append(("cleanup",)),
    )
    monkeypatch.setattr(
        trade_batch.live_main,
        "prepare_trade_cycle",
        lambda timeframe, settings: _prepared(timeframe),
    )
    monkeypatch.setattr(
        trade_batch,
        "fetch_recent_ohlcv_batch",
        lambda requirements, now_utc: _batch_result(),
    )
    monkeypatch.setattr(
        trade_batch.live_main,
        "apply_arguments",
        lambda timeframe: None,
    )

    def fake_trade(timeframe, run_mode, **kwargs):
        del run_mode
        phase = "sell" if kwargs["execute_sells"] else "buy"
        events.append((phase, timeframe))
        return kwargs["prepared_cycle"].summary

    monkeypatch.setattr(trade_batch.live_main, "trade", fake_trade)
    monkeypatch.setattr(
        trade_batch.live_main,
        "send_trade_report",
        lambda timeframe, *args: events.append(("report", timeframe)),
    )

    trade_batch.run_batch(["1d", "1h", "4h"])

    assert events == [
        ("cleanup",),
        ("sell", "1h"),
        ("sell", "4h"),
        ("sell", "1d"),
        ("buy", "1h"),
        ("buy", "4h"),
        ("buy", "1d"),
        ("report", "1h"),
        ("report", "4h"),
        ("report", "1d"),
    ]


def test_market_data_only_skips_trading_writes_and_reports(monkeypatch, capsys):
    monkeypatch.setattr(
        trade_batch.config,
        "load_settings",
        lambda refresh=True: SimpleNamespace(run_mode="test"),
    )
    monkeypatch.setattr(
        trade_batch.database,
        "delete_positions_not_top_rank",
        lambda: (_ for _ in ()).throw(AssertionError("benchmark must be read-only")),
    )
    monkeypatch.setattr(
        trade_batch.live_main,
        "prepare_trade_cycle",
        lambda timeframe, settings: _prepared(timeframe),
    )
    monkeypatch.setattr(
        trade_batch,
        "fetch_recent_ohlcv_batch",
        lambda requirements, now_utc: _batch_result(),
    )
    monkeypatch.setattr(
        trade_batch.live_main,
        "trade",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("benchmark must not evaluate trades")
        ),
    )
    monkeypatch.setattr(
        trade_batch.live_main,
        "send_trade_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("benchmark must not send reports")
        ),
    )

    result = trade_batch.run_batch(["1h"], market_data_only=True)

    assert result["summaries"] == {}
    assert '"exchange_code": "kraken"' in capsys.readouterr().out


def test_batch_market_data_never_falls_back_to_an_unplanned_network_read():
    market_data = trade_batch.live_main._TradeCycleMarketData(
        {("BTC/USDC", "1h"): 201},
        allow_network=False,
    )

    frame = market_data.load("BTC/USDC", "1h")

    assert frame.empty
    assert market_data.failures[("BTC/USDC", "1h")]["reason"] == (
        "OHLCV requirement was not prefetched"
    )
