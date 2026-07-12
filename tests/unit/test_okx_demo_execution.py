from decimal import Decimal

import ccxt
import pytest

from bec.exchanges.ccxt_adapter import PrivateExchangeOperationDisabled
from bec.exchanges.okx_adapter import OkxAdapter


class DemoOkxClient:
    precisionMode = ccxt.TICK_SIZE

    def __init__(self):
        self.has = {"createMarketBuyOrderWithCost": True}
        self.calls = []

    def set_sandbox_mode(self, enabled):
        self.calls.append(("sandbox", enabled))

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
        return str(price)

    @staticmethod
    def fetch_ticker(symbol):
        assert symbol == "BTC/USDC"
        return {"last": 100, "bid": 99, "ask": 101}

    @staticmethod
    def _order(side="buy", status="closed", client_order_id="BECDEMO1"):
        return {
            "id": "OKX-DEMO-1",
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


def _adapter(client=None, **kwargs):
    return OkxAdapter(
        client=client or DemoOkxClient(),
        adapter_id="myokx",
        execution_environment="demo",
        execution_code="okx_demo",
        api_key="key",
        api_secret="secret",
        api_passphrase="passphrase",
        private_enabled=True,
        **kwargs,
    )


def test_okx_demo_uses_quote_cost_buys_base_amount_sells_and_cash_mode():
    client = DemoOkxClient()
    adapter = _adapter(client)

    buy = adapter.create_market_buy(
        "BTC/USDC", quote_amount=Decimal("10"), client_order_id="BECDEMO1"
    )
    sell = adapter.create_market_sell(
        "BTC/USDC", Decimal("0.1"), client_order_id="BECDEMO2"
    )

    assert buy.executed_quantity == Decimal("0.0012")
    assert sell.fills[0].fee_asset == "BTC"
    assert client.calls[0] == ("sandbox", True)
    assert client.calls[1] == (
        "buy", "BTC/USDC", 10.0, {"tdMode": "cash", "clOrdId": "BECDEMO1"}
    )
    assert client.calls[2][-1] == {"tdMode": "cash", "clOrdId": "BECDEMO2"}


def test_okx_demo_reconciliation_and_cancel_use_client_id_and_cash_mode():
    client = DemoOkxClient()
    adapter = _adapter(client)

    found = adapter.fetch_order_by_client_id("BECDEMO1", "BTC/USDC")
    canceled = adapter.cancel_order("OKX-DEMO-1", "BTC/USDC")

    assert found.exchange_order_id == "OKX-DEMO-1"
    assert canceled.status.value == "canceled"
    assert client.calls[1] == (
        "lookup", "BECDEMO1", "BTC/USDC", {"tdMode": "cash", "clOrdId": "BECDEMO1"}
    )
    assert client.calls[2][-1] == {"tdMode": "cash"}


def test_okx_demo_parses_native_client_order_ids():
    adapter = _adapter()
    raw = DemoOkxClient._order()
    raw.pop("clientOrderId")
    raw["info"] = {"clOrdId": "BECDEMO1"}

    parsed = adapter._parse_order(raw, "BTC/USDC")

    assert parsed.client_order_id == "BECDEMO1"


@pytest.mark.parametrize("client_order_id", ["too-short!", "x" * 33, "with-dash"])
def test_okx_demo_rejects_invalid_client_order_ids_before_submission(client_order_id):
    adapter = _adapter()

    with pytest.raises(ValueError, match="1-32 ASCII letters or digits"):
        adapter.validate_client_order_id(client_order_id)


def test_okx_production_adapter_cannot_submit_even_with_credentials():
    adapter = OkxAdapter(
        client=DemoOkxClient(),
        api_key="key",
        api_secret="secret",
        api_passphrase="passphrase",
        private_enabled=True,
    )

    with pytest.raises(PrivateExchangeOperationDisabled, match="mandatory demo identity"):
        adapter.create_market_buy("BTC/USDC", quote_amount=Decimal("10"))
