from decimal import Decimal

import ccxt
import pytest

from bec.exchanges.okx_adapter import OkxAdapter


class ProductionOkxClient:
    precisionMode = ccxt.TICK_SIZE

    def __init__(self):
        self.has = {"createMarketBuyOrderWithCost": True}
        self.calls = []

    @staticmethod
    def load_markets(reload=False):
        return {
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
                "limits": {"amount": {"min": 0.0001}, "cost": {"min": 5}},
            }
        }

    @staticmethod
    def amount_to_precision(symbol, amount):
        assert symbol == "BTC/USDC"
        return str(amount)

    @staticmethod
    def price_to_precision(symbol, price):
        assert symbol == "BTC/USDC"
        return str(price)

    @staticmethod
    def fetch_ticker(symbol):
        assert symbol == "BTC/USDC"
        return {"last": 100, "bid": 99, "ask": 101}

    @staticmethod
    def _order(side="buy", status="closed", client_order_id="BECPROD1"):
        return {
            "id": "OKX-PROD-1",
            "clientOrderId": client_order_id,
            "symbol": "BTC/USDC",
            "side": side,
            "status": status,
            "amount": 0.0012,
            "filled": 0.0012,
            "average": 100,
            "cost": 0.12,
            "trades": [
                {
                    "id": "fill-1",
                    "price": 100,
                    "amount": 0.0012,
                    "fee": {"currency": "BTC", "cost": 0.000001},
                }
            ],
        }

    def create_market_buy_order_with_cost(self, symbol, cost, params):
        self.calls.append(("buy", symbol, cost, params))
        return self._order(client_order_id=params["clOrdId"])

    def create_order(self, symbol, order_type, side, amount, price, params):
        self.calls.append(("sell", symbol, order_type, side, amount, price, params))
        return self._order(side=side, client_order_id=params["clOrdId"])

    def fetch_order_by_client_order_id(self, client_order_id, symbol, params):
        self.calls.append(("lookup", client_order_id, symbol, params))
        return self._order(client_order_id=client_order_id)

    def fetch_order(self, order_id, symbol, params):
        self.calls.append(("fetch", order_id, symbol, params))
        return self._order()

    def cancel_order(self, order_id, symbol, params):
        self.calls.append(("cancel", order_id, symbol, params))
        return self._order(status="canceled")


def _adapter(client=None):
    return OkxAdapter(
        client=client or ProductionOkxClient(),
        adapter_id="myokx",
        api_key="key",
        api_secret="secret",
        api_passphrase="passphrase",
        private_enabled=True,
    )


def test_okx_production_uses_quote_cost_buys_base_amount_sells_and_cash_mode():
    client = ProductionOkxClient()
    adapter = _adapter(client)

    buy = adapter.create_market_buy(
        "BTC/USDC", quote_amount=Decimal("10"), client_order_id="BECPROD1"
    )
    sell = adapter.create_market_sell(
        "BTC/USDC", Decimal("0.1"), client_order_id="BECPROD2"
    )

    assert buy.executed_quantity == Decimal("0.0012")
    assert sell.fills[0].fee_asset == "BTC"
    assert client.calls[0] == (
        "buy", "BTC/USDC", 10.0, {"tdMode": "cash", "clOrdId": "BECPROD1"}
    )
    assert client.calls[1][-1] == {"tdMode": "cash", "clOrdId": "BECPROD2"}


def test_okx_production_reconciliation_and_cancel_use_client_id_and_cash_mode():
    client = ProductionOkxClient()
    adapter = _adapter(client)

    found = adapter.fetch_order_by_client_id("BECPROD1", "BTC/USDC")
    canceled = adapter.cancel_order("OKX-PROD-1", "BTC/USDC")

    assert found.exchange_order_id == "OKX-PROD-1"
    assert canceled.status.value == "canceled"
    assert client.calls[0] == (
        "lookup", "BECPROD1", "BTC/USDC", {"tdMode": "cash", "clOrdId": "BECPROD1"}
    )
    assert client.calls[1][-1] == {"tdMode": "cash"}


def test_okx_production_client_id_lookup_avoids_unsupported_fetch_orders():
    class ClosedOrderClient(ProductionOkxClient):
        def fetch_order_by_client_order_id(self, client_order_id, symbol, params):
            self.calls.append(("lookup", client_order_id, symbol, params))
            return None

        def fetch_open_orders(self, symbol, params):
            self.calls.append(("open", symbol, params))
            return []

        def fetch_closed_orders(self, symbol, params):
            self.calls.append(("closed", symbol, params))
            return []

        def fetch_orders(self, symbol, params):
            raise ccxt.NotSupported("myokx fetchOrders() is not supported")

    client = ClosedOrderClient()
    adapter = _adapter(client)

    assert adapter.fetch_order_by_client_id("BECPROD1", "BTC/USDC") is None
    assert [call[0] for call in client.calls] == ["lookup", "open", "closed"]


def test_okx_production_parses_native_client_order_ids():
    adapter = _adapter()
    raw = ProductionOkxClient._order()
    raw.pop("clientOrderId")
    raw["info"] = {"clOrdId": "BECPROD1"}

    parsed = adapter._parse_order(raw, "BTC/USDC")

    assert parsed.client_order_id == "BECPROD1"


@pytest.mark.parametrize("client_order_id", ["too-short!", "x" * 33, "with-dash"])
def test_okx_production_rejects_invalid_client_order_ids(client_order_id):
    with pytest.raises(ValueError, match="1-32 ASCII letters or digits"):
        _adapter().validate_client_order_id(client_order_id)
