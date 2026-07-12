from decimal import Decimal
from types import SimpleNamespace

import pytest

from bec.exchanges import live_execution
from bec.exchanges.base import (
    ExchangeCapabilities,
    MarketInfo,
    OrderResult,
    OrderStatus,
    OrderValidation,
    Ticker,
)


def _exchange(**overrides):
    values = {
        "id": 2,
        "code": "kraken",
        "buy_enabled": False,
        "sell_enabled": False,
        "partial_sell_policy": "accumulate",
    }
    values.update(overrides)
    return values


def _settings(run_mode="live", lock_values=False):
    return SimpleNamespace(
        run_mode=run_mode,
        lock_values=lock_values,
        tradable_balance_ratio=1.0,
        stake_amount_type="10",
        max_number_of_open_positions=5,
        min_position_size=5,
    )


def _result(side="sell"):
    return OrderResult(
        exchange_order_id="K-1",
        symbol="BTC/USDC",
        exchange_symbol="XBTUSDC",
        side=side,
        status=OrderStatus.FILLED,
        requested_quantity=Decimal("0.2"),
        executed_quantity=Decimal("0.2"),
        average_price=Decimal("100"),
        client_order_id="bec-test",
        raw={"id": "K-1"},
    )


def test_live_operation_requires_run_mode_and_explicit_side_flag(monkeypatch):
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            name="Kraken",
            private_enabled=True,
            capabilities=ExchangeCapabilities(uses_gated_live_execution=True),
        ),
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=True: _exchange(),
    )
    monkeypatch.setattr(
        live_execution.config, "load_settings", lambda refresh=True: _settings()
    )

    with pytest.raises(RuntimeError, match="buy operations are disabled"):
        live_execution._live_exchange("buy")

    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=True: _exchange(buy_enabled=True),
    )
    monkeypatch.setattr(
        live_execution.config,
        "load_settings",
        lambda refresh=True: _settings(run_mode="test"),
    )
    with pytest.raises(RuntimeError, match="run_mode=test"):
        live_execution._live_exchange("buy")


def test_demo_run_mode_is_rejected_for_kraken(monkeypatch):
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            code="kraken",
            name="Kraken",
            private_enabled=True,
            capabilities=ExchangeCapabilities(uses_gated_live_execution=True),
        ),
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=True: _exchange(buy_enabled=True),
    )
    monkeypatch.setattr(
        live_execution.config,
        "load_settings",
        lambda refresh=True: _settings(run_mode="demo"),
    )

    with pytest.raises(RuntimeError, match="reserved for the active OKX Demo"):
        live_execution._live_exchange("buy")


def test_okx_demo_adapter_requires_the_active_demo_identity(monkeypatch):
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            code="okx_demo",
            name="OKX",
            private_enabled=True,
            capabilities=ExchangeCapabilities(uses_gated_live_execution=True),
        ),
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=True: _exchange(code="kraken", buy_enabled=True),
    )
    monkeypatch.setattr(
        live_execution.config,
        "load_settings",
        lambda refresh=True: _settings(run_mode="demo"),
    )

    with pytest.raises(RuntimeError, match="active okx_demo identity"):
        live_execution._live_exchange("buy")


def test_below_minimum_accumulate_policy_skips_without_creating_intent(monkeypatch):
    market = MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True)
    monkeypatch.setattr(
        live_execution, "_live_exchange", lambda side: _exchange(sell_enabled=True)
    )
    monkeypatch.setattr(
        live_execution,
        "_sell_amount",
        lambda **kwargs: (Decimal("0.0001"), market, SimpleNamespace()),
    )
    monkeypatch.setattr(
        live_execution.service,
        "fetch_ticker",
        lambda symbol: Ticker(symbol, Decimal("100"), bid=Decimal("100")),
    )
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(False, ("amount is below the exchange minimum",)),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("intent must not be created")),
    )

    result = live_execution.create_sell_order(
        symbol="BTC/USDC", bot="1h", percentage=50, position_id=10
    )

    assert result["skipped"] is True
    assert result["policy"] == "accumulate"


def test_sell_all_policy_revalidates_full_position_before_submission(monkeypatch):
    market = MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True)
    amounts = iter((Decimal("0.1"), Decimal("0.2")))
    created = []
    monkeypatch.setattr(
        live_execution,
        "_live_exchange",
        lambda side: _exchange(sell_enabled=True, partial_sell_policy="sell_all"),
    )
    monkeypatch.setattr(
        live_execution,
        "_sell_amount",
        lambda **kwargs: (next(amounts), market, SimpleNamespace(get=lambda *args: "")),
    )
    monkeypatch.setattr(
        live_execution.service,
        "fetch_ticker",
        lambda symbol: Ticker(symbol, Decimal("100"), bid=Decimal("100")),
    )
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(request.amount == Decimal("0.2"), ("below minimum",)),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: created.append(kwargs)
        or {"Id": 1, "Client_Order_Id": "bec-test", **kwargs},
    )
    monkeypatch.setattr(live_execution.database, "mark_order_intent_submitting", lambda _id: None)
    monkeypatch.setattr(live_execution.service, "create_market_sell", lambda *args, **kwargs: _result())
    monkeypatch.setattr(
        live_execution.database,
        "apply_order_result",
        lambda *args: {"delta_executed_qty": 0, "closed_position": False, "terminal": True},
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_order_intent",
        lambda intent_id: {"PnL_Value": 4.0, "PnL_Perc": 2.0},
    )
    monkeypatch.setattr(live_execution.telegram, "send_trade_event", lambda **kwargs: None)
    monkeypatch.setattr(live_execution.database, "get_num_open_positions", lambda: 1)

    live_execution.create_sell_order(
        symbol="BTC/USDC", bot="1h", percentage=50, position_id=10
    )

    assert created[0]["requested_qty"] == pytest.approx(0.2)
    assert created[0]["sell_percentage"] == 100


def test_uncertain_submission_is_marked_for_reconciliation(monkeypatch):
    market = MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True)
    unknown = []
    monkeypatch.setattr(
        live_execution, "_live_exchange", lambda side: _exchange(buy_enabled=True)
    )
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda symbol: symbol)
    monkeypatch.setattr(live_execution.service, "load_markets", lambda: {market.symbol: market})
    monkeypatch.setattr(live_execution, "_stake_amount", lambda quote: Decimal("10"))
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(True, estimated_cost=Decimal("10")),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: {"Id": 1, "Client_Order_Id": "bec-test"},
    )
    monkeypatch.setattr(live_execution.database, "mark_order_intent_submitting", lambda _id: None)
    monkeypatch.setattr(
        live_execution.service,
        "create_market_buy",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("unknown outcome")),
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_unknown",
        lambda intent_id, error: unknown.append((intent_id, error)),
    )
    monkeypatch.setattr(live_execution.telegram, "send_error_event", lambda **kwargs: None)

    result = live_execution.create_buy_order(
        symbol="BTC/USDC", bot="1h", position_id=10
    )

    assert result is None
    assert unknown and unknown[0][0] == 1


def test_known_submission_rejection_is_settled_without_reconciliation(monkeypatch):
    rejected = []
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_rejected",
        lambda intent_id, error: rejected.append((intent_id, error)),
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_unknown",
        lambda *_args: pytest.fail("known rejection must not be unknown"),
    )
    monkeypatch.setattr(live_execution.telegram, "send_error_event", lambda **kwargs: None)

    live_execution._record_submission_failure(7, "create exchange buy order", ValueError("invalid"))

    assert rejected == [(7, "ValueError('invalid')")]


def test_reconciliation_resolves_unknown_intent_by_client_id(monkeypatch):
    applied = []

    class Adapter:
        capabilities = ExchangeCapabilities(supports_reconciliation=True)

        def fetch_order_by_client_id(self, client_order_id, symbol):
            assert (client_order_id, symbol) == ("bec-test", "BTC/USDC")
            return _result(side="buy")

    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=False: _exchange(),
    )
    monkeypatch.setattr(live_execution.service, "get_adapter", lambda: Adapter())
    monkeypatch.setattr(
        live_execution.database,
        "get_unsettled_order_intents",
        lambda exchange_id: [
            {
                "Id": 1,
                "Side": "BUY",
                "Symbol": "BTC/USDC",
                "Exchange_Order_Id": "K-1",
                "Client_Order_Id": "bec-test",
            }
        ],
    )
    monkeypatch.setattr(
        live_execution.database,
        "apply_order_result",
        lambda intent_id, result: applied.append((intent_id, result.status)) or {
            "delta_executed_qty": 0,
            "closed_position": False,
            "terminal": True,
        },
    )

    stats = live_execution.reconcile_unsettled_orders()

    assert stats == {"checked": 1, "updated": 1, "unresolved": 0}
    assert applied == [(1, OrderStatus.FILLED)]
