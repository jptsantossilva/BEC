import ast
import inspect
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from bec.exchanges import registry
from bec.exchanges import service
from bec.exchanges.base import (
    ExchangeAdapter,
    ExchangeCapabilities,
    MarketInfo,
    OrderRequest,
    OrderStatus,
)
from bec.exchanges.binance_adapter import BinanceAdapter


EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSDC",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDC",
            "quoteOrderQtyMarketAllowed": True,
            "filters": [
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00010000",
                    "maxQty": "100.00000000",
                    "stepSize": "0.00010000",
                },
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "10.00000000"},
            ],
        }
    ]
}


class FakeBinanceClient:
    def __init__(self):
        self.created_order = None

    def get_exchange_info(self):
        return EXCHANGE_INFO

    def get_asset_balance(self, asset):
        return {"asset": asset, "free": "12.5", "locked": "0.25"}

    def get_account(self):
        return {"balances": [{"asset": "USDC", "free": "12.5", "locked": "0.25"}]}

    def get_symbol_ticker(self, symbol):
        assert symbol == "BTCUSDC"
        return {"symbol": symbol, "price": "60000.25"}

    def get_order_book(self, **params):
        assert params == {"symbol": "BTCUSDC", "limit": 5}
        return {"bids": [["60000", "0.2"]], "asks": [["60001", "0.1"]]}

    def create_order(self, **params):
        self.created_order = params
        return {
            "symbol": "BTCUSDC",
            "orderId": 123,
            "clientOrderId": params.get("newClientOrderId"),
            "transactTime": 1_750_000_000_000,
            "status": "FILLED",
            "side": params["side"],
            "origQty": "0.001",
            "executedQty": "0.001",
            "cummulativeQuoteQty": "60",
            "fills": [
                {
                    "price": "60000",
                    "qty": "0.001",
                    "commission": "0.000001",
                    "commissionAsset": "BTC",
                    "tradeId": 99,
                }
            ],
        }

    def get_order(self, **params):
        order_id = params.get("orderId", 123)
        return {
            "symbol": params["symbol"],
            "orderId": order_id,
            "status": "PARTIALLY_FILLED",
            "side": "BUY",
            "origQty": "0.002",
            "executedQty": "0.001",
            "cummulativeQuoteQty": "60",
        }

    def cancel_order(self, **params):
        return {
            "symbol": params["symbol"],
            "orderId": params["orderId"],
            "status": "CANCELED",
            "side": "BUY",
            "origQty": "0.002",
            "executedQty": "0.001",
            "cummulativeQuoteQty": "60",
        }

    def ping(self):
        return {}


def test_binance_adapter_implements_complete_contract():
    assert issubclass(BinanceAdapter, ExchangeAdapter)
    assert not inspect.isabstract(BinanceAdapter)


def test_registry_rejects_an_active_exchange_without_an_adapter(monkeypatch):
    from bec.utils import database

    registry.set_default_adapter(None)
    monkeypatch.setattr(
        database,
        "get_active_exchange",
        lambda required=False: {"code": "coinbase"},
    )

    with pytest.raises(RuntimeError, match="No adapter is available.*coinbase"):
        registry.get_default_adapter()

    registry.set_default_adapter(None)


def test_registry_selects_kraken_public_adapter(monkeypatch):
    from bec.exchanges.kraken_adapter import KrakenAdapter
    from bec.utils import database

    registry.set_default_adapter(None)
    monkeypatch.setattr(
        database,
        "get_active_exchange",
        lambda required=False: {"code": "kraken"},
    )

    assert isinstance(registry.get_default_adapter(), KrakenAdapter)
    registry.set_default_adapter(None)


def test_legacy_private_service_paths_route_kraken_to_gated_execution(monkeypatch):
    from bec.exchanges import live_execution

    class PublicAdapter:
        code = "kraken"
        name = "Kraken"
        capabilities = ExchangeCapabilities(uses_gated_live_execution=True)

    monkeypatch.setattr(service, "get_adapter", lambda: PublicAdapter())
    monkeypatch.setattr(
        service._legacy_binance,
        "create_buy_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Binance order path must not be called")
        ),
    )
    monkeypatch.setattr(
        live_execution,
        "create_buy_order",
        lambda *args, **kwargs: {"gated": True, "symbol": kwargs["symbol"]},
    )

    result = service.create_buy_order(symbol="XBTEUR", bot="1h")

    assert result == {"gated": True, "symbol": "XBTEUR"}


def test_binance_adapter_normalizes_markets_amounts_prices_and_limits():
    adapter = BinanceAdapter(client=FakeBinanceClient())

    market = adapter.load_markets()["BTC/USDC"]

    assert market.exchange_symbol == "BTCUSDC"
    assert market.quote_market_buy_allowed is True
    assert adapter.normalize_symbol("btcusdc") == "BTC/USDC"
    assert adapter.normalize_symbol("btc/usdc") == "BTC/USDC"
    assert adapter.normalize_amount("BTC/USDC", Decimal("0.00129")) == Decimal(
        "0.00120000"
    )
    assert adapter.normalize_price("BTC/USDC", Decimal("60000.259")) == Decimal(
        "60000.25000000"
    )

    invalid = adapter.validate_order(
        OrderRequest(
            symbol="BTC/USDC",
            side="buy",
            amount=Decimal("0.00001"),
            price=Decimal("60000"),
        )
    )
    assert invalid.valid is False
    assert "amount is below the exchange minimum" in invalid.errors
    assert "cost is below the exchange minimum" in invalid.errors

    valid = adapter.validate_order(
        OrderRequest(
            symbol="BTC/USDC",
            side="buy",
            quote_amount=Decimal("20"),
        )
    )
    assert valid.valid is True

    ambiguous = adapter.validate_order(
        OrderRequest(
            symbol="BTC/USDC",
            side="buy",
            amount=Decimal("0.001"),
            quote_amount=Decimal("20"),
        )
    )
    assert ambiguous.valid is False
    assert "amount and quote_amount are mutually exclusive" in ambiguous.errors


def test_binance_adapter_returns_canonical_market_and_account_data():
    adapter = BinanceAdapter(client=FakeBinanceClient())

    balance = adapter.fetch_balance("usdc")
    ticker = adapter.fetch_ticker("BTC/USDC")
    book = adapter.fetch_order_book("BTCUSDC", limit=5)

    assert balance.free == Decimal("12.5")
    assert balance.total == Decimal("12.75")
    assert ticker.last == Decimal("60000.25")
    assert book.bids == ((Decimal("60000"), Decimal("0.2")),)
    assert book.asks == ((Decimal("60001"), Decimal("0.1")),)


def test_exchange_service_filters_tradable_symbols_without_raw_exchange_data(monkeypatch):
    markets = {
        "BTC/USDC": MarketInfo("BTC/USDC", "BTCUSDC", "BTC", "USDC", True),
        "ETH/USDC": MarketInfo("ETH/USDC", "ETHUSDC", "ETH", "USDC", False),
        "ADAUP/USDC": MarketInfo(
            "ADAUP/USDC", "ADAUPUSDC", "ADAUP", "USDC", True
        ),
        "EUR/USDC": MarketInfo("EUR/USDC", "EURUSDC", "EUR", "USDC", True),
        "BTC/EUR": MarketInfo("BTC/EUR", "BTCEUR", "BTC", "EUR", True),
    }
    monkeypatch.setattr(service, "load_markets", lambda **kwargs: markets)
    monkeypatch.setattr(
        service,
        "get_adapter",
        lambda: SimpleNamespace(
            code="binance",
            capabilities=ExchangeCapabilities(
                uses_exchange_symbols_for_legacy_workflows=True
            ),
        ),
    )

    assert service.get_tradable_symbols(
        "USDC", excluded_base_assets={"EUR"}
    ) == ["BTCUSDC"]


def test_exchange_service_returns_normalized_symbols_for_kraken(monkeypatch):
    markets = {
        "BTC/USDC": MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True),
    }
    monkeypatch.setattr(service, "load_markets", lambda **kwargs: markets)
    monkeypatch.setattr(
        service,
        "get_adapter",
        lambda: SimpleNamespace(code="kraken", capabilities=ExchangeCapabilities()),
    )

    assert service.get_tradable_symbols("USDC") == ["BTC/USDC"]


def test_adapter_capabilities_describe_existing_exchange_routing():
    assert BinanceAdapter.capabilities.uses_native_private_workflows is True
    assert BinanceAdapter.capabilities.uses_gated_live_execution is False

    from bec.exchanges.kraken_adapter import KrakenAdapter

    assert KrakenAdapter.capabilities.uses_gated_live_execution is True
    assert KrakenAdapter.capabilities.requires_explicit_live_flags is True


def test_binance_adapter_normalizes_order_results():
    client = FakeBinanceClient()
    adapter = BinanceAdapter(client=client)

    created = adapter.create_market_buy(
        "BTC/USDC", quote_amount=Decimal("60"), client_order_id="bec-1"
    )
    fetched = adapter.fetch_order("123", "BTC/USDC")
    by_client_id = adapter.fetch_order_by_client_id("bec-1", "BTC/USDC")
    canceled = adapter.cancel_order("123", "BTC/USDC")

    assert client.created_order["quoteOrderQty"] == "60"
    assert client.created_order["newClientOrderId"] == "bec-1"
    assert created.status is OrderStatus.FILLED
    assert created.executed_quantity == Decimal("0.001")
    assert created.average_price == Decimal("6.0E+4")
    assert created.fills[0].fee_asset == "BTC"
    assert created.fills[0].trade_id == "99"
    assert fetched.status is OrderStatus.PARTIALLY_FILLED
    assert by_client_id.status is OrderStatus.PARTIALLY_FILLED
    assert canceled.status is OrderStatus.CANCELED
    assert adapter.health_check().available is True


def test_binance_client_order_id_is_validated_before_submission():
    adapter = BinanceAdapter(client=FakeBinanceClient())

    assert adapter.validate_client_order_id("bec-binance-123") == "bec-binance-123"
    with pytest.raises(ValueError, match="1-36"):
        adapter.validate_client_order_id("x" * 37)


def test_python_binance_imports_are_confined_to_native_adapter():
    repo_root = Path(__file__).resolve().parents[2]
    allowed = Path("bec/exchanges/binance_adapter.py")
    violations = []
    application_paths = list((repo_root / "bec").rglob("*.py"))
    application_paths.extend((repo_root / "pages").rglob("*.py"))
    application_paths.extend(repo_root.glob("*.py"))

    for path in application_paths:
        relative = path.relative_to(repo_root)
        if relative == allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == "binance" or alias.name.startswith("binance.") for alias in node.names):
                    violations.append(f"{relative}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "binance" or str(node.module).startswith("binance."):
                    violations.append(f"{relative}:{node.lineno}")

    assert violations == []


def test_application_code_does_not_import_legacy_binance_module():
    repo_root = Path(__file__).resolve().parents[2]
    violations = []
    application_paths = list((repo_root / "bec").rglob("*.py"))
    application_paths.extend((repo_root / "pages").rglob("*.py"))
    application_paths.extend(repo_root.glob("*.py"))

    for path in application_paths:
        relative = path.relative_to(repo_root)
        if relative.parts[:2] == ("bec", "exchanges"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == "bec.exchanges.binance" for alias in node.names):
                    violations.append(f"{relative}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "bec.exchanges.binance":
                    violations.append(f"{relative}:{node.lineno}")

    assert violations == []
