from decimal import Decimal, ROUND_DOWN

import ccxt
import pandas as pd
import pytest

from bec.exchanges.base import ExchangeAdapter, MarketInfo
from bec.exchanges.ccxt_adapter import PrivateExchangeOperationDisabled
from bec.exchanges.okx_adapter import OkxAdapter


class FakeOkxClient:
    precisionMode = ccxt.TICK_SIZE

    def __init__(self, *, collision: bool = False):
        self.has = {"fetchStatus": True}
        self.collision = collision
        self.private_calls = 0

    def load_markets(self, reload=False):
        markets = {
            "BTC/USDC": {
                "id": "BTC-USDC",
                "symbol": "BTC/USDC",
                "base": "BTC",
                "quote": "USDC",
                "spot": True,
                "contract": False,
                "type": "spot",
                "active": True,
                "precision": {"amount": 0.0001, "price": 0.1},
                "limits": {
                    "amount": {"min": 0.0002, "max": 10},
                    "cost": {"min": 5, "max": 100000},
                },
                "info": {"instId": "BTC-USDC"},
            },
            "DOGE/USDC": {
                "id": "DOGE-USDC",
                "symbol": "DOGE/USDC",
                "base": "DOGE",
                "quote": "USDC",
                "spot": True,
                "contract": False,
                "type": "spot",
                "active": True,
                "precision": {"amount": 1, "price": 0.00001},
                "limits": {"amount": {"min": 1, "max": None}, "cost": {"min": 1, "max": None}},
            },
            "ETH/USDC": {
                "id": "ETH-USDC",
                "symbol": "ETH/USDC",
                "base": "ETH",
                "quote": "USDC",
                "spot": True,
                "contract": False,
                "type": "spot",
                "active": False,
            },
            "BTC/USDC:USDC": {
                "id": "BTC-USDC-SWAP",
                "symbol": "BTC/USDC:USDC",
                "base": "BTC",
                "quote": "USDC",
                "spot": False,
                "contract": True,
                "type": "swap",
            },
        }
        if self.collision:
            markets["ETH/USDT"] = {
                "id": "BTC-USDC",
                "symbol": "ETH/USDT",
                "base": "ETH",
                "quote": "USDT",
                "spot": True,
                "contract": False,
                "type": "spot",
                "active": True,
            }
        return markets

    @staticmethod
    def _floor(value, step):
        converted = Decimal(str(value))
        increment = Decimal(str(step))
        return str((converted / increment).to_integral_value(rounding=ROUND_DOWN) * increment)

    def amount_to_precision(self, symbol, amount):
        assert symbol == "BTC/USDC"
        return self._floor(amount, "0.0001")

    def price_to_precision(self, symbol, price):
        assert symbol == "BTC/USDC"
        return self._floor(price, "0.1")

    @staticmethod
    def parse_timeframe(interval):
        assert interval == "1m"
        return 60

    def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
        assert (symbol, timeframe, params) == ("BTC/USDC", "1m", {})
        candles = [
            [1_700_000_000_000, 10, 12, 9, 11, 5],
            [1_700_000_060_000, 11, 13, 10, 12, 6],
            [1_700_000_120_000, 12, 14, 11, 13, 7],
        ]
        return [row for row in candles if since is None or row[0] >= since][:limit]

    @staticmethod
    def milliseconds():
        return 1_800_000_000_000

    @staticmethod
    def fetch_ticker(symbol):
        assert symbol == "BTC/USDC"
        return {"last": 60000, "bid": 59999, "ask": 60001, "timestamp": 1_700_000_000_000}

    @staticmethod
    def fetch_order_book(symbol, limit=None):
        assert (symbol, limit) == ("BTC/USDC", 5)
        return {"bids": [[59999, 0.2]], "asks": [[60001, 0.1]], "timestamp": 1_700_000_000_000}

    @staticmethod
    def fetch_status():
        return {"status": "ok", "updated": "operational"}

    def fetch_balance(self):
        self.private_calls += 1
        raise AssertionError("private API must not be reached")


def test_okx_public_adapter_normalizes_native_symbols_and_filters_non_spot_markets():
    adapter = OkxAdapter(client=FakeOkxClient(), adapter_id="myokx")

    assert isinstance(adapter, ExchangeAdapter)
    markets = adapter.load_markets()
    market = markets["BTC/USDC"]
    assert set(markets) == {"BTC/USDC", "DOGE/USDC"}
    assert market.exchange_symbol == "BTC-USDC"
    assert market.amount_step == Decimal("0.0001")
    assert market.price_step == Decimal("0.1")
    assert market.min_amount == Decimal("0.0002")
    assert market.max_cost == Decimal("100000")
    assert market.spot is True and market.contract is False
    assert adapter.normalize_symbol("btc/usdc") == "BTC/USDC"
    assert adapter.normalize_symbol("BTC-USDC") == "BTC/USDC"
    assert adapter.normalize_amount("BTC-USDC", Decimal("0.00129")) == Decimal("0.0012")
    assert adapter.normalize_price("BTC/USDC", Decimal("60000.29")) == Decimal("60000.2")


def test_okx_public_adapter_rejects_alias_collisions():
    with pytest.raises(ValueError, match="alias collision.*BTC-USDC"):
        OkxAdapter(client=FakeOkxClient(collision=True)).load_markets()


def test_okx_public_ohlcv_ticker_book_health_and_private_boundary():
    client = FakeOkxClient()
    adapter = OkxAdapter(client=client, adapter_id="okx")

    frame = adapter.fetch_ohlcv(
        "BTC-USDC", "1m", start_date=1_700_000_000_000,
        limit=2, include_symbol=True, keep_time_col=False, drop_incomplete=True,
    )
    ticker = adapter.fetch_ticker("BTC/USDC")
    book = adapter.fetch_order_book("BTC-USDC", limit=5)
    health = adapter.health_check()

    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame["Close"].tolist() == [11, 12, 13]
    assert frame["Symbol"].tolist() == ["BTC/USDC"] * 3
    assert ticker.last == Decimal("60000")
    assert book.bids == ((Decimal("59999"), Decimal("0.2")),)
    assert health.available is True
    for operation in (
        lambda: adapter.fetch_balance(),
        lambda: adapter.create_market_buy("BTC/USDC", quote_amount=Decimal("10")),
        lambda: adapter.create_market_sell("BTC/USDC", Decimal("0.001")),
        lambda: adapter.fetch_order("1", "BTC/USDC"),
        lambda: adapter.cancel_order("1", "BTC/USDC"),
    ):
        with pytest.raises(PrivateExchangeOperationDisabled):
            operation()
    assert client.private_calls == 0


def test_okx_ohlcv_clamps_to_listing_time_and_paginates_past_exchange_page_cap():
    class PaginatedOkxClient(FakeOkxClient):
        listing_time = 1_700_000_000_000
        duration_ms = 60_000

        def __init__(self):
            super().__init__()
            self.calls = []
            self.candles = [
                [
                    self.listing_time + index * self.duration_ms,
                    10,
                    12,
                    9,
                    11 + index,
                    5,
                ]
                for index in range(650)
            ]

        def load_markets(self, reload=False):
            markets = super().load_markets(reload=reload)
            markets["BTC/USDC"]["info"]["listTime"] = str(self.listing_time)
            return markets

        def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            self.calls.append((since, limit))
            assert limit == 300
            return [row for row in self.candles if row[0] >= since][:limit]

        def milliseconds(self):
            return self.candles[-1][0]

    client = PaginatedOkxClient()
    adapter = OkxAdapter(client=client)

    frame = adapter.fetch_ohlcv(
        "BTC/USDC",
        "1m",
        start_date=client.listing_time - 10_000_000,
        limit=720,
        keep_time_col=False,
    )

    assert len(frame) == 650
    assert frame.index.is_unique
    assert frame.index.is_monotonic_increasing
    assert client.calls == [
        (client.listing_time, 300),
        (client.listing_time + 300 * client.duration_ms, 300),
        (client.listing_time + 600 * client.duration_ms, 300),
    ]


def test_okx_public_ohlcv_retries_transient_rate_limit():
    class RateLimitedOkxClient(FakeOkxClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            self.calls += 1
            if self.calls == 1:
                raise ccxt.RateLimitExceeded("50011 rate limit reached")
            return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    client = RateLimitedOkxClient()
    sleeps = []
    adapter = OkxAdapter(
        client=client,
        sleeper=sleeps.append,
        random_uniform=lambda _start, _end: 0.0,
    )

    frame = adapter.fetch_ohlcv(
        "BTC/USDC",
        "1m",
        start_date=1_700_000_000_000,
        keep_time_col=False,
        max_retries=1,
        backoff_sec=2,
    )

    assert not frame.empty
    assert client.calls == 3  # retry, successful page, then end-of-history probe
    assert sleeps == [2.0]


def test_okx_default_client_uses_shared_public_throttle(monkeypatch):
    monkeypatch.setattr(
        OkxAdapter,
        "_create_client",
        staticmethod(lambda *args, **kwargs: FakeOkxClient()),
    )

    adapter = OkxAdapter()

    assert adapter._public_read_retry_enabled is True
    assert adapter._public_request_throttle.min_interval_seconds == 0.15
    assert adapter._public_request_throttle.state_path.name == "okx-public.lock"


def test_okx_market_info_can_be_persisted_with_native_and_canonical_symbols():
    market = MarketInfo(
        "BTC/USDC", "BTC-USDC", "BTC", "USDC", True,
        amount_step=Decimal("0.0001"), price_step=Decimal("0.1"),
        min_amount=Decimal("0.0002"), min_cost=Decimal("5"),
        market_type="spot", spot=True, contract=False,
        raw={"instId": "BTC-USDC"},
    )

    assert market.symbol == "BTC/USDC"
    assert market.exchange_symbol == "BTC-USDC"
