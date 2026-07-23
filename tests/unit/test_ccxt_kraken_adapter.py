from decimal import Decimal, ROUND_DOWN

import ccxt
import pandas as pd
import pytest

from bec.exchanges.base import ExchangeAdapter, OrderRequest, OrderStatus
from bec.exchanges.ccxt_adapter import (
    PrivateExchangeOperationDisabled,
    TransientPublicMarketDataError,
)
from bec.exchanges.kraken_adapter import KrakenAdapter
from bec.exchanges import service


class FakeKrakenClient:
    precisionMode = ccxt.TICK_SIZE

    def __init__(self):
        self.has = {
            "createMarketBuyOrderWithCost": True,
            "fetchStatus": True,
        }
        self.load_calls = []
        self.fail_status = False
        self.created = []
        self.candles = [
            [1_700_000_000_000, 10, 12, 9, 11, 5],
            [1_700_000_060_000, 11, 13, 10, 12, 6],
            [1_700_000_120_000, 12, 14, 11, 13, 7],
        ]

    def load_markets(self, reload=False):
        self.load_calls.append(bool(reload))
        return {
            "BTC/EUR": {
                "id": "XXBTZEUR",
                "symbol": "BTC/EUR",
                "base": "BTC",
                "quote": "EUR",
                "spot": True,
                "type": "spot",
                "active": True,
                "precision": {"amount": 0.0001, "price": 0.1},
                "limits": {
                    "amount": {"min": 0.0002, "max": 10},
                    "cost": {"min": 5, "max": None},
                },
                "info": {"altname": "XBTEUR", "wsname": "XBT/EUR"},
            },
            "BTC/EUR:EUR": {
                "id": "PF_XBT_EUR",
                "symbol": "BTC/EUR:EUR",
                "base": "BTC",
                "quote": "EUR",
                "spot": False,
                "type": "swap",
            },
        }

    @staticmethod
    def _floor(value, step):
        converted = Decimal(str(value))
        increment = Decimal(str(step))
        return str((converted / increment).to_integral_value(rounding=ROUND_DOWN) * increment)

    def amount_to_precision(self, symbol, amount):
        assert symbol == "BTC/EUR"
        return self._floor(amount, "0.0001")

    def price_to_precision(self, symbol, price):
        assert symbol == "BTC/EUR"
        return self._floor(price, "0.1")

    @staticmethod
    def parse_timeframe(interval):
        assert interval == "1m"
        return 60

    def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
        assert symbol == "BTC/EUR"
        assert timeframe == "1m"
        assert params == {}
        rows = [row for row in self.candles if since is None or row[0] >= since]
        return rows[:limit]

    @staticmethod
    def milliseconds():
        return 1_800_000_000_000

    def fetch_ticker(self, symbol):
        assert symbol == "BTC/EUR"
        return {
            "last": 60_000,
            "bid": 59_999,
            "ask": 60_001,
            "timestamp": 1_700_000_000_000,
        }

    def fetch_order_book(self, symbol, limit=None):
        assert symbol == "BTC/EUR"
        assert limit == 5
        return {
            "bids": [[59_999, 0.2]],
            "asks": [[60_001, 0.1]],
            "timestamp": 1_700_000_000_000,
        }

    def fetch_status(self):
        if self.fail_status:
            raise RuntimeError("offline")
        return {"status": "ok", "updated": "operational"}

    def fetch_balance(self):
        return {
            "free": {"XBT": "0.5", "USDC": "100"},
            "used": {"XBT": "0.1", "USDC": "2"},
        }

    def _order(self, side, status="closed", client_order_id="bec-test"):
        return {
            "id": "KRAKEN-1",
            "clientOrderId": client_order_id,
            "symbol": "BTC/EUR",
            "side": side,
            "status": status,
            "amount": 0.001,
            "filled": 0.001 if status == "closed" else 0.0005,
            "average": 60000,
            "cost": 60,
            "timestamp": 1_700_000_000_000,
            "trades": [
                {
                    "id": "trade-1",
                    "price": 60000,
                    "amount": 0.001,
                    "fee": {"currency": "XBT", "cost": 0.000001},
                }
            ],
        }

    def create_market_buy_order_with_cost(self, symbol, cost, params):
        self.created.append(("cost", symbol, cost, params))
        return self._order("buy", client_order_id=params.get("clientOrderId"))

    def create_order(self, symbol, order_type, side, amount, price, params):
        self.created.append((side, symbol, amount, price, params))
        return self._order(side, client_order_id=params.get("clientOrderId"))

    def fetch_order(self, order_id, symbol):
        assert (order_id, symbol) == ("KRAKEN-1", "BTC/EUR")
        return self._order("buy", status="open")

    def fetch_orders(self, symbol, params):
        return [
            self._order(
                "buy", status="open", client_order_id=params["clientOrderId"]
            )
        ]

    def cancel_order(self, order_id, symbol):
        assert (order_id, symbol) == ("KRAKEN-1", "BTC/EUR")
        return self._order("buy", status="canceled")


def test_kraken_adapter_implements_contract_and_maps_aliases_and_limits():
    adapter = KrakenAdapter(client=FakeKrakenClient())

    assert isinstance(adapter, ExchangeAdapter)
    market = adapter.load_markets()["BTC/EUR"]
    assert market.exchange_symbol == "XXBTZEUR"
    assert market.amount_step == Decimal("0.0001")
    assert market.price_step == Decimal("0.1")
    assert market.min_amount == Decimal("0.0002")
    assert market.min_cost == Decimal("5")
    assert adapter.normalize_symbol("XBT/EUR") == "BTC/EUR"
    assert adapter.normalize_symbol("XBTEUR") == "BTC/EUR"
    assert adapter.normalize_symbol("XXBTZEUR") == "BTC/EUR"
    assert adapter.normalize_amount("BTC/EUR", Decimal("0.00129")) == Decimal(
        "0.0012"
    )
    assert adapter.normalize_price("BTC/EUR", Decimal("60000.29")) == Decimal(
        "60000.2"
    )

    invalid = adapter.validate_order(
        OrderRequest(
            symbol="XBT/EUR",
            side="buy",
            amount=Decimal("0.0001"),
            price=Decimal("10000"),
        )
    )
    assert invalid.valid is False
    assert "amount is below the exchange minimum" in invalid.errors
    assert "cost is below the exchange minimum" in invalid.errors


@pytest.mark.parametrize("legacy_first", [False, True])
def test_kraken_canonical_market_wins_over_legacy_display_alias(legacy_first):
    class RepAliasClient(FakeKrakenClient):
        def load_markets(self, reload=False):
            self.load_calls.append(bool(reload))
            current = {
                "id": "XREPZEUR",
                "symbol": "REP/EUR",
                "base": "REP",
                "quote": "EUR",
                "spot": True,
                "type": "spot",
                "active": True,
                "precision": {},
                "limits": {},
                "info": {"altname": "REPEUR", "wsname": "REP/EUR"},
            }
            legacy = {
                "id": "XREPV1ZEUR",
                "symbol": "REP/EUR",
                "base": "REPV1",
                "quote": "EUR",
                "spot": True,
                "type": "spot",
                "active": True,
                "precision": {},
                "limits": {},
                "info": {"altname": "REPV1EUR", "wsname": "REP/EUR"},
            }
            rows = [("REPV1/EUR", legacy), ("REP/EUR", current)]
            if not legacy_first:
                rows.reverse()
            return dict(rows)

    adapter = KrakenAdapter(client=RepAliasClient())

    assert set(adapter.load_markets()) == {"REP/EUR", "REPV1/EUR"}
    assert adapter.normalize_symbol("REP/EUR") == "REP/EUR"
    assert adapter.normalize_symbol("XREPZEUR") == "REP/EUR"
    assert adapter.normalize_symbol("XREPV1ZEUR") == "REPV1/EUR"


def test_ccxt_market_metadata_cache_honors_ttl_and_force_refresh():
    client = FakeKrakenClient()
    now = [100.0]
    adapter = KrakenAdapter(
        client=client,
        market_cache_ttl_seconds=60,
        clock=lambda: now[0],
    )

    adapter.load_markets()
    adapter.load_markets()
    now[0] = 161.0
    adapter.load_markets()
    adapter.load_markets(force=True)

    assert client.load_calls == [False, True, True]


def test_kraken_public_ohlcv_ticker_order_book_and_health():
    adapter = KrakenAdapter(client=FakeKrakenClient())

    frame = adapter.fetch_ohlcv(
        "XBT/EUR",
        "1m",
        start_date=1_700_000_000_000,
        limit=2,
        keep_time_col=False,
        include_symbol=True,
        drop_incomplete=True,
    )
    ticker = adapter.fetch_ticker("BTC/EUR")
    book = adapter.fetch_order_book("XBTEUR", limit=5)
    health = adapter.health_check()

    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame["Close"].tolist() == [11, 12, 13]
    assert frame["Symbol"].tolist() == ["BTC/EUR"] * 3
    assert ticker.last == Decimal("60000")
    assert ticker.bid == Decimal("59999")
    assert book.bids == ((Decimal("59999"), Decimal("0.2")),)
    assert book.asks == ((Decimal("60001"), Decimal("0.1")),)
    assert health.available is True
    assert health.message == "operational"


def test_kraken_ohlcv_retries_rate_limit_per_page_without_duplicate_candles():
    class RateLimitedPageClient(FakeKrakenClient):
        def __init__(self):
            super().__init__()
            self.ohlcv_calls = []
            self.failed_second_page = False

        def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            self.ohlcv_calls.append(since)
            if (
                since == 1_700_000_120_000
                and not self.failed_second_page
            ):
                self.failed_second_page = True
                raise ccxt.RateLimitExceeded("EGeneral:Too many requests")
            return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    client = RateLimitedPageClient()
    sleeps = []
    adapter = KrakenAdapter(
        client=client,
        sleeper=sleeps.append,
        random_uniform=lambda _start, _end: 0.0,
    )

    frame = adapter.fetch_ohlcv(
        "BTC/EUR",
        "1m",
        start_date=1_700_000_000_000,
        limit=2,
        keep_time_col=False,
        max_retries=2,
        backoff_sec=2,
    )

    assert frame["Close"].tolist() == [11, 12, 13]
    assert client.ohlcv_calls == [
        1_700_000_000_000,
        1_700_000_120_000,
        1_700_000_120_000,
    ]
    assert sleeps == [2.0]


def test_kraken_ohlcv_exhaustion_raises_typed_transient_error():
    class UnavailableClient(FakeKrakenClient):
        def __init__(self):
            super().__init__()
            self.ohlcv_calls = 0

        def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            self.ohlcv_calls += 1
            raise ccxt.DDoSProtection("EGeneral:Too many requests")

    client = UnavailableClient()
    sleeps = []
    adapter = KrakenAdapter(
        client=client,
        sleeper=sleeps.append,
        random_uniform=lambda _start, _end: 0.0,
    )

    with pytest.raises(
        TransientPublicMarketDataError,
        match="failed after 3 attempts",
    ):
        adapter.fetch_ohlcv(
            "BTC/EUR",
            "1m",
            max_retries=2,
            backoff_sec=2,
            max_backoff_sec=3,
        )

    assert client.ohlcv_calls == 3
    assert sleeps == [2.0, 3.0]


def test_kraken_ohlcv_does_not_retry_non_transient_errors():
    class InvalidSymbolClient(FakeKrakenClient):
        def __init__(self):
            super().__init__()
            self.ohlcv_calls = 0

        def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            self.ohlcv_calls += 1
            raise ccxt.BadSymbol("invalid symbol")

    client = InvalidSymbolClient()
    sleeps = []
    adapter = KrakenAdapter(client=client, sleeper=sleeps.append)

    with pytest.raises(ccxt.BadSymbol, match="invalid symbol"):
        adapter.fetch_ohlcv("BTC/EUR", "1m")

    assert client.ohlcv_calls == 1
    assert sleeps == []


def test_exchange_service_close_frame_uses_active_public_adapter():
    adapter = KrakenAdapter(client=FakeKrakenClient())
    service.set_adapter(adapter)
    try:
        frame = service.get_close_df(
            "XBT/EUR",
            "1m",
            start_date=1_700_000_000_000,
            limit=2,
            include_symbol=True,
            keep_time_col=True,
        )
    finally:
        service.set_adapter(None)

    assert frame.columns.tolist() == ["Time", "Symbol", "Close"]
    assert frame["Symbol"].tolist() == ["BTC/EUR"] * 3


def test_kraken_health_reports_public_api_failure():
    client = FakeKrakenClient()
    client.fail_status = True
    health = KrakenAdapter(client=client).health_check()

    assert health.available is False
    assert "offline" in health.message


@pytest.mark.parametrize(
    "operation",
    [
        lambda adapter: adapter.fetch_balance("EUR"),
        lambda adapter: adapter.create_market_buy(
            "BTC/EUR", quote_amount=Decimal("10")
        ),
        lambda adapter: adapter.create_market_sell("BTC/EUR", Decimal("0.001")),
        lambda adapter: adapter.fetch_order("1", "BTC/EUR"),
        lambda adapter: adapter.cancel_order("1", "BTC/EUR"),
    ],
)
def test_kraken_private_operations_are_disabled(operation):
    adapter = KrakenAdapter(client=FakeKrakenClient())

    with pytest.raises(PrivateExchangeOperationDisabled):
        operation(adapter)


def test_kraken_private_balance_orders_fills_and_reconciliation():
    client = FakeKrakenClient()
    adapter = KrakenAdapter(client=client, private_enabled=True)

    balances = adapter.fetch_balance()
    buy = adapter.create_market_buy(
        "XBT/EUR", quote_amount=Decimal("60"), client_order_id="bec-buy"
    )
    sell = adapter.create_market_sell(
        "BTC/EUR", Decimal("0.00129"), client_order_id="bec-sell"
    )
    fetched = adapter.fetch_order("KRAKEN-1", "BTC/EUR")
    resolved = adapter.fetch_order_by_client_id("bec-buy", "BTC/EUR")
    canceled = adapter.cancel_order("KRAKEN-1", "BTC/EUR")

    assert balances["BTC"].free == Decimal("0.5")
    assert balances["BTC"].locked == Decimal("0.1")
    assert buy.status is OrderStatus.FILLED
    assert buy.fills[0].fee_asset == "BTC"
    assert buy.client_order_id == "bec-buy"
    assert sell.executed_quantity == Decimal("0.001")
    assert client.created[1][2] == 0.0012
    assert fetched.status is OrderStatus.PARTIALLY_FILLED
    assert resolved.status is OrderStatus.PARTIALLY_FILLED
    assert canceled.status is OrderStatus.CANCELED


def test_market_buy_fallback_applies_one_percent_base_quantity_buffer():
    client = FakeKrakenClient()
    client.has["createMarketBuyOrderWithCost"] = False
    adapter = KrakenAdapter(client=client, private_enabled=True)

    adapter.create_market_buy(
        "BTC/EUR", quote_amount=Decimal("60"), client_order_id="bec-buffer"
    )

    assert client.created[0][0] == "buy"
    assert client.created[0][2] == 0.0009
