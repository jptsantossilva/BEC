import sqlite3

import pandas as pd

import bec.utils.database as database
from bec.market_indicators import supply_profit_loss as spl
from bec.market_indicators import summary


def _use_memory_db(monkeypatch):
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(database.sql_create_settings_table)
    conn.execute(database.sql_create_onchain_btc_supply_profit_loss_table)
    conn.execute(database.sql_create_onchain_signal_alerts_sent_table)
    conn.execute(database.sql_create_signals_log_table)
    monkeypatch.setattr(database._thread_local, "conn", conn, raising=False)
    return conn


def _sample_rows(values):
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-06-01", periods=len(values), freq="D"),
            "btc_price": [60000 + i * 1000 for i in range(len(values))],
            "percent_supply_in_profit": values,
            "percent_supply_in_loss": [100 - value for value in values],
            "retrieved_at": ["2026-06-10T11:00:00+00:00"] * len(values),
            "source": ["bitview"] * len(values),
        }
    )


def test_supply_profit_loss_summary_without_data_is_unavailable(monkeypatch):
    _use_memory_db(monkeypatch)

    rows = summary.summarize_btc_supply_profit_loss()

    assert len(rows) == 2
    assert {row.name for row in rows} == {
        "BTC Supply Profit/Loss - Top",
        "BTC Supply Profit/Loss - Bottom",
    }
    assert {row.signal_group for row in rows} == {"Top", "Bottom"}
    assert all(row.available is False for row in rows)
    assert all(row.status == "Unavailable" for row in rows)
    assert all(row.current == "No data" for row in rows)


def test_supply_profit_loss_summary_below_threshold(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 98.0)
    spl.save_cached_supply_profit_loss(_sample_rows([45.0]))

    row = summary.summarize_btc_supply_profit_loss()[0]

    assert row.available is True
    assert row.signal_group == "Top"
    assert row.hit is False
    assert row.status == "Neutral"
    assert row.current == "45.00%"
    assert row.reference == ">= 98.00%"
    assert row.distance_to_hit == "53.00 p.p."
    assert row.progress_pct == 0.0


def test_supply_profit_loss_summary_top_progress_starts_from_neutral_zone(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 98.0)
    spl.save_cached_supply_profit_loss(_sample_rows([50.28]))

    row = summary.summarize_btc_supply_profit_loss()[0]

    assert row.hit is False
    assert row.distance_to_hit == "47.72 p.p."
    assert row.progress_pct == 0.58


def test_supply_profit_loss_summary_below_extreme_top_threshold(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 98.0)
    spl.save_cached_supply_profit_loss(_sample_rows([92.0]))

    row = summary.summarize_btc_supply_profit_loss()[0]

    assert row.hit is False
    assert row.status == "Neutral"
    assert row.bias == "Neutral"
    assert row.distance_to_hit == "6.00 p.p."
    assert row.progress_pct == 87.5


def test_supply_profit_loss_summary_above_extreme_threshold(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 98.0)
    spl.save_cached_supply_profit_loss(_sample_rows([99.0]))

    row = summary.summarize_btc_supply_profit_loss()[0]

    assert row.hit is True
    assert row.status == "Risk"
    assert row.bias == "Risk"
    assert row.progress_pct == 100.0


def test_supply_profit_loss_summary_ignores_zero_extreme_top_setting(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 0.0)
    spl.save_cached_supply_profit_loss(_sample_rows([50.0]))

    row = summary.summarize_btc_supply_profit_loss()[0]

    assert row.hit is False
    assert row.reference == ">= 98.00%"


def test_supply_profit_loss_bottom_summary_uses_loss_above_profit(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_cross_tolerance", 0.0)
    spl.save_cached_supply_profit_loss(_sample_rows([50.5]))

    row = summary.summarize_btc_supply_profit_loss()[1]

    assert row.name == "BTC Supply Profit/Loss - Bottom"
    assert row.signal_group == "Bottom"
    assert row.hit is False
    assert row.status == "Watch"
    assert row.bias == "Neutral"
    assert row.reference == "Loss >= Profit"
    assert row.distance_to_hit == "1.00 p.p."
    assert row.progress_pct == 99.0


def test_supply_profit_loss_bottom_summary_hits_when_loss_is_above_profit(monkeypatch):
    _use_memory_db(monkeypatch)
    spl.save_cached_supply_profit_loss(_sample_rows([49.0]))

    row = summary.summarize_btc_supply_profit_loss()[1]

    assert row.hit is True
    assert row.status == "Stress"
    assert row.bias == "Bearish"
    assert row.distance_to_hit == "0.00 p.p."
    assert row.progress_pct == 100.0


def test_summary_aggregation_and_dataframe(monkeypatch):
    _use_memory_db(monkeypatch)
    database.set_setting("onchain_supply_profit_loss_extreme_top_threshold", 98.0)
    spl.save_cached_supply_profit_loss(_sample_rows([92.0]))

    rows = summary.get_market_indicator_summaries()
    metrics = summary.aggregate_summary_metrics(rows)
    top_metrics = summary.aggregate_summary_metrics(rows, signal_group="Top")
    bottom_metrics = summary.aggregate_summary_metrics(rows, signal_group="Bottom")
    df = summary.summaries_to_dataframe(rows)

    assert metrics["indicators"] == "2"
    assert metrics["active_signals"] == "0/2"
    assert metrics["average_progress"] == "51.75%"
    assert top_metrics["active_signals"] == "0/1"
    assert top_metrics["average_progress"] == "87.50%"
    assert bottom_metrics["active_signals"] == "0/1"
    assert bottom_metrics["average_progress"] == "16.00%"
    assert df.iloc[0]["name"] == "BTC Supply Profit/Loss - Top"
    assert df.iloc[0]["signal_group"] == "Top"
    assert df.iloc[0]["signal"] == "Not hit"
    assert df.iloc[0]["progress"] == 87.5
    assert df.iloc[1]["name"] == "BTC Supply Profit/Loss - Bottom"
    assert df.iloc[1]["signal_group"] == "Bottom"
    assert df.iloc[1]["historical_data"] == "/bitcoin_supply_profit_loss"
