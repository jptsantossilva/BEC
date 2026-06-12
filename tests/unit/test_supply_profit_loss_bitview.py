import sqlite3

import pandas as pd
import pytest

import bec.utils.database as database
from bec.market_indicators import supply_profit_loss as spl


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return self.responses.pop(0)


def _use_memory_db(monkeypatch):
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(database.sql_create_onchain_btc_supply_profit_loss_table)
    conn.execute(database.sql_create_onchain_signal_alerts_sent_table)
    conn.execute(database.sql_create_signals_log_table)
    monkeypatch.setattr(database._thread_local, "conn", conn, raising=False)
    return conn


def _payload(values):
    return {"version": 1, "index": "day1", "stamp": "2026-06-10T11:44:14Z", "data": values}


def test_fetch_bitview_series_uses_expected_endpoint():
    session = _FakeSession([_FakeResponse(payload=_payload([1, 2]))])

    payload = spl.fetch_bitview_series("price", days=10, session=session)

    assert payload["data"] == [1, 2]
    assert session.calls[0][0].endswith("/series/price/day")
    assert session.calls[0][1] == {"start": "-10"}


def test_fetch_bitview_series_uses_start_zero_for_full_history():
    session = _FakeSession([_FakeResponse(payload=_payload([1, 2]))])

    spl.fetch_bitview_series("supply_in_profit_share", days=0, session=session)

    assert session.calls[0][0].endswith("/series/supply_in_profit_share/day")
    assert session.calls[0][1] == {"start": "0"}


def test_fetch_bitview_series_rejects_unsupported_series_before_request():
    session = _FakeSession([])

    with pytest.raises(ValueError, match="Unsupported Bitview series"):
        spl.fetch_bitview_series("unknown_series", days=10, session=session)

    assert session.calls == []


def test_fetch_bitview_series_raises_on_429():
    session = _FakeSession([_FakeResponse(status_code=429, payload={"error": "limit"})])

    with pytest.raises(spl.BitviewRateLimitError):
        spl.fetch_bitview_series("price", days=10, session=session)


def test_update_job_handles_429_without_corrupting_cache(monkeypatch):
    _use_memory_db(monkeypatch)
    existing = spl.normalize_supply_profit_loss(
        pd.DataFrame(
            {
                "date": ["2026-06-09"],
                "btc_price": [100],
                "percent_supply_in_profit": [50],
                "percent_supply_in_loss": [50],
                "source": ["bitview"],
            }
        )
    )
    spl.save_cached_supply_profit_loss(existing)
    session = _FakeSession([_FakeResponse(status_code=429, payload={"error": "limit"})])

    assert spl.run_btc_supply_profit_loss_update_job(session=session) == 0

    cached = spl.load_cached_supply_profit_loss()
    assert len(cached) == 1
    assert cached.iloc[0]["btc_price"] == 100


def test_update_missing_days_empty_cache_backfills_full_history(monkeypatch):
    _use_memory_db(monkeypatch)
    session = _FakeSession(
        [
            _FakeResponse(payload=_payload([100, 110])),
            _FakeResponse(payload=_payload([50, 51])),
            _FakeResponse(payload=_payload([50, 49])),
            _FakeResponse(payload=_payload([9_000_000, 9_100_000])),
            _FakeResponse(payload=_payload([9_000_000, 8_900_000])),
        ]
    )

    rows = spl.update_missing_days(session=session)

    assert rows == 2
    assert session.calls[0][1] == {"start": "0"}


def test_update_missing_days_fetches_only_gap(monkeypatch):
    _use_memory_db(monkeypatch)
    existing = spl.normalize_supply_profit_loss(
        pd.DataFrame(
            {
                "date": ["2026-06-07"],
                "btc_price": [100],
                "percent_supply_in_profit": [50],
                "percent_supply_in_loss": [50],
                "source": ["bitview"],
            }
        )
    )
    spl.save_cached_supply_profit_loss(existing)
    session = _FakeSession(
        [
            _FakeResponse(payload=_payload([110, 120])),
            _FakeResponse(payload=_payload([51, 52])),
            _FakeResponse(payload=_payload([49, 48])),
            _FakeResponse(payload=_payload([9_100_000, 9_200_000])),
            _FakeResponse(payload=_payload([8_900_000, 8_800_000])),
        ]
    )

    rows = spl.update_missing_days(
        session=session,
        now_utc=pd.Timestamp("2026-06-10T12:00:00Z").to_pydatetime(),
    )

    assert rows == 2
    assert session.calls[0][1] == {"start": "-2"}
    assert len(spl.load_cached_supply_profit_loss()) == 3
