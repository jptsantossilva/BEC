import threading
import time
from datetime import datetime, timezone

import pandas as pd

from bec.exchanges.base import OhlcvBatchPolicy
from bec.exchanges.market_data_batch import (
    fetch_recent_ohlcv_batch,
    merge_requirements,
)


class FakeBatchAdapter:
    ohlcv_batch_policy = OhlcvBatchPolicy(max_workers=3)

    def __init__(self, state):
        self.state = state

    def load_markets(self):
        self.state["market_loads"] += 1
        return {}

    def share_market_catalog_from(self, source):
        assert isinstance(source, FakeBatchAdapter)

    def fetch_ohlcv(self, symbol, timeframe, **kwargs):
        del kwargs
        with self.state["lock"]:
            self.state["active_by_symbol"][symbol] = (
                self.state["active_by_symbol"].get(symbol, 0) + 1
            )
            self.state["max_by_symbol"][symbol] = max(
                self.state["max_by_symbol"].get(symbol, 0),
                self.state["active_by_symbol"][symbol],
            )
            self.state["active_total"] += 1
            self.state["max_total"] = max(
                self.state["max_total"], self.state["active_total"]
            )
            self.state["calls"].append((symbol, timeframe))
        time.sleep(0.02)
        with self.state["lock"]:
            self.state["active_by_symbol"][symbol] -= 1
            self.state["active_total"] -= 1
        if symbol == "FAIL/USDC":
            raise TimeoutError("simulated")
        index = pd.date_range("2025-01-01", periods=300, freq="h")
        return pd.DataFrame({"Close": range(300)}, index=index)


def _state():
    return {
        "lock": threading.Lock(),
        "market_loads": 0,
        "active_by_symbol": {},
        "max_by_symbol": {},
        "active_total": 0,
        "max_total": 0,
        "calls": [],
    }


def test_merge_requirements_uses_largest_count():
    merged = merge_requirements(
        {("btc/usdc", "1h"): 201},
        {("BTC/USDC", "1h"): 250, ("ETH/USDC", "1d"): 201},
    )

    assert merged == {
        ("BTC/USDC", "1h"): 250,
        ("ETH/USDC", "1d"): 201,
    }


def test_batch_parallelizes_symbols_but_serializes_each_symbol():
    state = _state()
    result = fetch_recent_ohlcv_batch(
        {
            ("BTC/USDC", "1h"): 201,
            ("BTC/USDC", "1d"): 201,
            ("ETH/USDC", "1h"): 201,
            ("SOL/USDC", "1h"): 201,
        },
        now_utc=datetime(2025, 2, 1, tzinfo=timezone.utc),
        exchange={"code": "kraken"},
        adapter_factory=lambda: FakeBatchAdapter(state),
    )

    assert result.metrics.workers == 3
    assert result.metrics.logical_requests == 4
    assert result.metrics.failures == 0
    assert len(result.frames[("BTC/USDC", "1h")]) == 201
    assert len(result.frames[("BTC/USDC", "1d")]) == 201
    assert state["max_by_symbol"]["BTC/USDC"] == 1
    assert state["max_total"] >= 2
    assert state["market_loads"] == 1


def test_batch_failure_is_isolated_to_dependent_market():
    state = _state()
    result = fetch_recent_ohlcv_batch(
        {
            ("BTC/USDC", "1h"): 201,
            ("FAIL/USDC", "1h"): 201,
        },
        now_utc=datetime(2025, 2, 1, tzinfo=timezone.utc),
        exchange={"code": "kraken"},
        adapter_factory=lambda: FakeBatchAdapter(state),
    )

    assert not result.frames[("BTC/USDC", "1h")].empty
    assert result.frames[("FAIL/USDC", "1h")].empty
    assert result.failures[("FAIL/USDC", "1h")]["error_type"] == "TimeoutError"
