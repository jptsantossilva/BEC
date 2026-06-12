import sqlite3
from datetime import datetime, timezone

import pandas as pd
import pytest

import bec.utils.database as database
from bec.market_indicators import supply_profit_loss as spl


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
            "date": pd.date_range("2024-01-01", periods=len(values), freq="D"),
            "btc_price": [40000 + i * 1000 for i in range(len(values))],
            "percent_supply_in_profit": values,
            "percent_supply_in_loss": [100 - value for value in values],
            "source": ["bitview"] * len(values),
        }
    )


def _payload(values, stamp="2026-06-10T11:44:14Z"):
    return {"version": 1, "index": "day1", "stamp": stamp, "data": values}


def _bitview_payloads():
    return {
        "price": _payload([100, 110, 120]),
        "supply_in_profit_share": _payload([47.6, 50.1, 51.2]),
        "supply_in_loss_share": _payload([52.4, 49.9, 48.8]),
        "supply_in_profit": _payload([9_000_000, 9_100_000, 9_200_000]),
        "supply_in_loss": _payload([10_000_000, 9_900_000, 9_800_000]),
    }


def test_dates_from_bitview_payload_prefers_last_complete_utc_day():
    dates = spl.dates_from_bitview_payload(
        _payload([1, 2, 3], stamp="2026-06-10T11:44:14Z"),
        now_utc=datetime(2026, 6, 10, 12, tzinfo=timezone.utc),
    )

    assert [str(date) for date in dates] == [
        "2026-06-07",
        "2026-06-08",
        "2026-06-09",
    ]


def test_normalize_bitview_series_maps_required_and_optional_values():
    df = spl.normalize_bitview_series(
        _bitview_payloads(),
        now_utc=datetime(2026, 6, 10, 12, tzinfo=timezone.utc),
    )

    assert len(df) == 3
    assert df.iloc[-1]["date"].strftime("%Y-%m-%d") == "2026-06-09"
    assert df.iloc[-1]["btc_price"] == 120
    assert df.iloc[-1]["percent_supply_in_profit"] == 51.2
    assert df.iloc[-1]["supply_in_profit_btc"] == 9_200_000
    assert df.iloc[-1]["source"] == "bitview"


def test_normalize_bitview_series_filters_null_and_zero_price_history():
    payloads = {
        "price": _payload([None, 0.0, 100.0, 110.0]),
        "supply_in_profit_share": _payload([40.0, 41.0, 42.0, 43.0]),
        "supply_in_loss_share": _payload([60.0, 59.0, 58.0, 57.0]),
    }

    df = spl.normalize_bitview_series(
        payloads,
        now_utc=datetime(2026, 6, 10, 12, tzinfo=timezone.utc),
    )

    assert len(df) == 2
    assert df["btc_price"].tolist() == [100.0, 110.0]
    assert df["percent_supply_in_profit"].tolist() == [42.0, 43.0]


def test_normalize_bitview_series_requires_aligned_required_lengths():
    payloads = _bitview_payloads()
    payloads["supply_in_loss_share"] = _payload([52.4, 49.9])

    with pytest.raises(spl.BitviewError):
        spl.normalize_bitview_series(payloads)


def test_optional_bitview_series_can_be_missing():
    payloads = _bitview_payloads()
    payloads.pop("supply_in_profit")
    payloads.pop("supply_in_loss")

    df = spl.normalize_bitview_series(
        payloads,
        now_utc=datetime(2026, 6, 10, 12, tzinfo=timezone.utc),
    )

    assert df["supply_in_profit_btc"].isna().all()
    assert df["supply_in_loss_btc"].isna().all()


def test_cache_upserts_by_date(monkeypatch):
    _use_memory_db(monkeypatch)
    first = spl.normalize_supply_profit_loss(_sample_rows([80]))
    second = spl.normalize_supply_profit_loss(_sample_rows([82]))

    spl.save_cached_supply_profit_loss(first)
    spl.save_cached_supply_profit_loss(second)
    cached = spl.load_cached_supply_profit_loss()

    assert len(cached) == 1
    assert cached.iloc[0]["percent_supply_in_profit"] == 82
    assert cached.iloc[0]["percent_supply_in_loss"] == 18


def test_detects_top_extreme_bottom_and_loss_cross_events():
    df = _sample_rows([96, 97, 94, 99, 4, 50.5, 49.0])

    events = spl.detect_supply_profit_loss_events(df, cross_tolerance=1.0)
    event_types = events["event_type"].tolist()

    assert event_types.count(spl.EVENT_TOP_ZONE) == 2
    assert spl.EVENT_EXTREME_TOP_ZONE in event_types
    assert spl.EVENT_BOTTOM_ZONE in event_types
    assert spl.EVENT_CROSS_50 in event_types


def test_loss_cross_event_requires_directional_cross():
    df = _sample_rows([49.5, 50.5])

    events = spl.detect_supply_profit_loss_events(df, cross_tolerance=1.0)

    assert spl.EVENT_CROSS_50 not in events["event_type"].tolist()


def test_extreme_top_alerts_as_separate_transition_inside_top_zone():
    df = _sample_rows([96, 97, 98.5])

    events = spl.detect_supply_profit_loss_events(df)
    top_events = events[events["event_type"] == spl.EVENT_TOP_ZONE]
    extreme_events = events[events["event_type"] == spl.EVENT_EXTREME_TOP_ZONE]

    assert top_events["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-01"]
    assert extreme_events["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-03"]


def test_alert_deduplication_blocks_repeated_send(monkeypatch):
    _use_memory_db(monkeypatch)
    events = spl.detect_supply_profit_loss_events(_sample_rows([96]))
    sent = []

    monkeypatch.setattr(
        spl,
        "send_supply_profit_loss_telegram_alert",
        lambda event: sent.append(event["event_type"]),
    )

    assert spl.send_new_supply_profit_loss_alerts(events) == 1
    assert spl.send_new_supply_profit_loss_alerts(events) == 0
    assert sent == [spl.EVENT_TOP_ZONE]


def test_send_alert_uses_signals_token(monkeypatch):
    _use_memory_db(monkeypatch)
    events = spl.detect_supply_profit_loss_events(_sample_rows([96]))
    sent = []

    monkeypatch.setattr(spl.telegram, "telegram_token_signals", "signals-token")
    monkeypatch.setattr(
        spl.telegram,
        "send_telegram_message",
        lambda *args, **kwargs: sent.append(args),
    )

    spl.send_supply_profit_loss_telegram_alert(events.iloc[0])

    assert sent[0][0] == "signals-token"
    assert "Macro/on-chain signal only" in sent[0][2]


def test_update_job_uses_database_alert_thresholds(monkeypatch):
    _use_memory_db(monkeypatch)
    spl.save_cached_supply_profit_loss(_sample_rows([94]))
    database.set_setting("onchain_supply_profit_loss_top_threshold", 90.0)
    sent = []

    monkeypatch.setattr(spl, "update_missing_days", lambda session=None: 0)
    monkeypatch.setattr(
        spl,
        "send_supply_profit_loss_telegram_alert",
        lambda event: sent.append(event["event_type"]),
    )

    assert spl.run_btc_supply_profit_loss_update_job() == 1
    assert sent == [spl.EVENT_TOP_ZONE]
