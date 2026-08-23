from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace

import pandas as pd
import pytest

import bec.exchanges.binance as binance
from bec.exchanges.base import (
    Balance,
    MarketInfo,
    OrderResult,
    OrderStatus,
    OrderValidation,
    SellOutcomeStatus,
    Ticker,
)


MARKET = MarketInfo(
    "PENGU/USDC",
    "PENGUUSDC",
    "PENGU",
    "USDC",
    True,
    amount_step=Decimal("0.0001"),
)


class FakeAdapter:
    def __init__(self, client=None):
        self.client = client

    def load_markets(self):
        return {MARKET.symbol: MARKET}

    def normalize_symbol(self, symbol):
        return MARKET.symbol

    def fetch_balance(self, asset):
        return self.client.balance

    def fetch_ticker(self, symbol):
        return Ticker(MARKET.symbol, Decimal("0.01012"))

    def normalize_amount(self, symbol, amount):
        return Decimal(str(amount)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

    def validate_order(self, request):
        amount = request.amount or Decimal("0")
        return OrderValidation(
            amount > 0,
            () if amount > 0 else ("amount must be greater than zero",),
            normalized_amount=amount,
        )

    def _parse_order(self, order, symbol):
        return OrderResult(
            exchange_order_id=str(order["orderId"]),
            symbol=symbol,
            exchange_symbol=order["symbol"],
            side="sell",
            status=OrderStatus(str(order["status"]).lower()),
            requested_quantity=Decimal(str(order["executedQty"])),
            executed_quantity=Decimal(str(order["executedQty"])),
            average_price=Decimal(str(order["fills"][0]["price"])),
            raw=order,
        )


class FakeClient:
    def __init__(self, balance, executed="1.0", status="FILLED"):
        self.balance = Balance("PENGU", Decimal(str(balance)))
        self.executed = str(executed)
        self.status = status
        self.created = []

    def create_order(self, **kwargs):
        self.created.append(kwargs)
        return {
            "symbol": "PENGUUSDC",
            "orderId": 44,
            "transactTime": 1_777_000_000_000,
            "status": self.status,
            "side": "SELL",
            "executedQty": self.executed,
            "fills": [{"price": "0.01", "qty": self.executed}],
        }


def _settings():
    return SimpleNamespace(
        run_mode="live",
        lock_values=False,
        main_strategy_configs=[],
    )


def _position(qty):
    return pd.DataFrame(
        [
            {
                "Id": 10,
                "Qty": float(qty),
                "Buy_Order_Id": "BUY-PENGU",
                "Buy_Price": 0.007754,
                "Duration": "2d",
                "Strategy_Params_JSON": "{}",
            }
        ]
    )


def _common(monkeypatch, client, qty):
    monkeypatch.setattr(binance.config, "load_settings", _settings)
    monkeypatch.setattr(binance, "BinanceAdapter", FakeAdapter)
    monkeypatch.setattr(binance, "get_client", lambda: client)
    monkeypatch.setattr(
        binance.database,
        "get_active_exchange",
        lambda required=True: {
            "name": "Binance",
            "partial_sell_policy": "accumulate",
        },
    )
    monkeypatch.setattr(
        binance.database, "get_position_by_id", lambda position_id: _position(qty)
    )
    monkeypatch.setattr(binance.database, "exchange_log_prefix", lambda: "[test]")
    monkeypatch.setattr(binance.telegram, "get_telegram_token", lambda: "token")
    monkeypatch.setattr(binance.telegram, "get_telegram_prefix", lambda bot: "1H ")


def test_binance_closes_existing_dust_without_submitting_an_order(monkeypatch):
    client = FakeClient("0.0001")
    _common(monkeypatch, client, "0.00007")
    notifications = []
    monkeypatch.setattr(
        binance.database,
        "close_position_without_order",
        lambda **kwargs: {"closed": True, "reason": "dust"},
    )
    monkeypatch.setattr(
        binance.telegram,
        "send_telegram_message",
        lambda token, emoji, message: notifications.append(message),
    )

    outcome = binance.create_sell_order(
        "PENGUUSDC", "1h", percentage=100, position_id=10
    )

    assert outcome.status == SellOutcomeStatus.CLOSED_WITHOUT_ORDER
    assert outcome.residual_quantity == Decimal("0.00007")
    assert client.created == []
    assert len(notifications) == 1


def test_binance_partial_amount_rounded_to_zero_keeps_the_position(monkeypatch):
    client = FakeClient("0.00015")
    _common(monkeypatch, client, "0.00015")
    monkeypatch.setattr(
        binance.database,
        "close_position_without_order",
        lambda **kwargs: pytest.fail("a partial rounded amount must not close the position"),
    )

    outcome = binance.create_sell_order(
        "PENGUUSDC", "1h", percentage=25, take_profit_num=1, position_id=10
    )

    assert outcome.status == SellOutcomeStatus.SKIPPED
    assert client.created == []


def test_binance_full_fill_closes_only_the_residual_below_the_step(monkeypatch):
    client = FakeClient("1.00007", executed="1.0")
    _common(monkeypatch, client, "1.00007")
    closed = []
    quantities = []
    trade_events = []
    monkeypatch.setattr(
        binance.database,
        "set_position_sell",
        lambda **kwargs: closed.append(kwargs),
    )
    monkeypatch.setattr(binance.database, "release_value", lambda position_id: None)
    monkeypatch.setattr(
        binance.database,
        "set_position_qty",
        lambda **kwargs: quantities.append(kwargs),
    )
    monkeypatch.setattr(binance.database, "build_strategy_params_json", lambda _id: "{}")
    monkeypatch.setattr(binance.database, "get_strategy_name", lambda _id: "")
    monkeypatch.setattr(binance.database, "add_order_sell", lambda **kwargs: (1.0, 10.0))
    monkeypatch.setattr(
        binance.telegram,
        "send_trade_event",
        lambda **kwargs: trade_events.append(kwargs),
    )

    outcome = binance.create_sell_order(
        "PENGUUSDC", "1h", percentage=100, position_id=10
    )

    assert outcome.status == SellOutcomeStatus.SUBMITTED
    assert outcome.position_closed is True
    assert outcome.closure_reason == "dust"
    assert outcome.submitted_quantity == Decimal("1.0000")
    assert outcome.residual_quantity == Decimal("0.00007")
    assert len(closed) == 1
    assert quantities == []
    assert "non-tradable residual" in trade_events[0]["reason"]


def test_binance_nonterminal_partial_fill_keeps_a_dust_sized_remainder(monkeypatch):
    client = FakeClient("1.00007", executed="0.99999", status="PARTIALLY_FILLED")
    _common(monkeypatch, client, "1.00007")
    closed = []
    quantities = []
    monkeypatch.setattr(
        binance.database,
        "set_position_sell",
        lambda **kwargs: closed.append(kwargs),
    )
    monkeypatch.setattr(
        binance.database,
        "set_position_qty",
        lambda **kwargs: quantities.append(kwargs),
    )
    monkeypatch.setattr(binance.database, "build_strategy_params_json", lambda _id: "{}")
    monkeypatch.setattr(binance.database, "get_strategy_name", lambda _id: "")
    monkeypatch.setattr(binance.database, "add_order_sell", lambda **kwargs: (1.0, 10.0))
    monkeypatch.setattr(binance.telegram, "send_trade_event", lambda **kwargs: None)

    outcome = binance.create_sell_order(
        "PENGUUSDC", "1h", percentage=100, position_id=10
    )

    assert outcome.position_closed is False
    assert outcome.residual_quantity == Decimal("0.00008")
    assert closed == []
    assert quantities[0]["qty"] == pytest.approx(0.00008)
