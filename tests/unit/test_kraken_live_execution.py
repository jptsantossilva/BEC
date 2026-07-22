from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from bec.exchanges import live_execution
from bec.exchanges.base import (
    ExchangeCapabilities,
    MarketInfo,
    OrderResult,
    OrderStatus,
    OrderValidation,
    OrderBook,
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


@pytest.mark.parametrize(
    ("run_mode", "demo_record", "private_enabled", "expected"),
    [
        ("test", True, True, "run_mode=live"),
        ("live", False, True, "compatible completed OKX Demo validation"),
        ("live", True, False, "private credentials"),
    ],
)
def test_okx_production_runtime_gates_each_block_submission(
    monkeypatch, run_mode, demo_record, private_enabled, expected
):
    exchange = _exchange(
        code="okx",
        name="OKX (Production)",
        execution_environment="production",
        adapter_id="myokx",
        quote_asset="USDC",
        buy_enabled=True,
    )
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            code="okx",
            name="OKX",
            private_enabled=private_enabled,
            capabilities=ExchangeCapabilities(uses_gated_live_execution=True),
        ),
    )
    monkeypatch.setattr(live_execution.database, "get_active_exchange", lambda required=True: exchange)
    monkeypatch.setattr(
        live_execution.config,
        "load_settings",
        lambda refresh=True: _settings(run_mode=run_mode),
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_compatible_okx_demo_validation",
        lambda _exchange: {"Id": 1} if demo_record else None,
    )
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: SimpleNamespace(free=Decimal("1")),
    )

    with pytest.raises(RuntimeError, match=expected):
        live_execution._live_exchange("buy")


def test_okx_production_private_balance_preflight_is_required(monkeypatch):
    exchange = _exchange(
        code="okx",
        name="OKX (Production)",
        execution_environment="production",
        adapter_id="myokx",
        quote_asset="USDC",
        buy_enabled=True,
    )
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            code="okx",
            name="OKX",
            private_enabled=True,
            capabilities=ExchangeCapabilities(uses_gated_live_execution=True),
        ),
    )
    monkeypatch.setattr(live_execution.database, "get_active_exchange", lambda required=True: exchange)
    monkeypatch.setattr(live_execution.config, "load_settings", lambda refresh=True: _settings())
    monkeypatch.setattr(live_execution.database, "get_compatible_okx_demo_validation", lambda _exchange: {"Id": 1})
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: (_ for _ in ()).throw(RuntimeError("authentication failure")),
    )

    with pytest.raises(RuntimeError, match="private balance preflight failed"):
        live_execution._live_exchange("buy")


def test_okx_production_buy_requires_schedule_and_matching_backtest(monkeypatch):
    exchange = _exchange(code="okx", buy_enabled=True)
    monkeypatch.setattr(live_execution.database, "is_trade_main_timeframe_enabled", lambda bot: False)

    with pytest.raises(RuntimeError, match="explicitly enabled 1h main schedule"):
        live_execution._require_okx_production_strategy_gate(
            exchange, bot="1h", symbol="BTC/USDC", strategy_id="ema", side="buy"
        )

    monkeypatch.setattr(live_execution.database, "is_trade_main_timeframe_enabled", lambda bot: True)
    monkeypatch.setattr(
        live_execution.database, "is_active_backtest_approved_for_live_buy", lambda **kwargs: False
    )
    with pytest.raises(RuntimeError, match="approved backtest with matching exchange"):
        live_execution._require_okx_production_strategy_gate(
            exchange, bot="1h", symbol="BTC/USDC", strategy_id="ema", side="buy"
        )


def test_manual_okx_demo_buy_validates_before_creating_a_position_candidate(monkeypatch):
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    exchange = _exchange(
        code="okx_demo", buy_enabled=True, sizing_buffer_pct=5.0
    )
    created = []
    submitted = []
    monkeypatch.setattr(live_execution, "_live_exchange", lambda side: exchange)
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda symbol: "BTC/USDC")
    monkeypatch.setattr(live_execution.service, "load_markets", lambda: {"BTC/USDC": market})
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: SimpleNamespace(free=Decimal("100")),
    )
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(True, estimated_cost=request.quote_amount),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_okx_demo_manual_position_candidate",
        lambda symbol: created.append(symbol) or 42,
    )
    monkeypatch.setattr(
        live_execution,
        "create_buy_order",
        lambda **kwargs: submitted.append(kwargs) or {"result": "submitted"},
    )

    outcome = live_execution.create_okx_demo_manual_buy(
        symbol="BTC/USDC", quote_amount=Decimal("95")
    )

    assert outcome == {"result": "submitted"}
    assert created == ["BTC/USDC"]
    assert submitted[0]["position_id"] == 42
    assert submitted[0]["quote_amount"] == Decimal("95")


def test_manual_okx_demo_buy_rejects_excess_balance_before_creating_candidate(monkeypatch):
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    exchange = _exchange(
        code="okx_demo", buy_enabled=True, sizing_buffer_pct=5.0
    )
    monkeypatch.setattr(live_execution, "_live_exchange", lambda side: exchange)
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda symbol: "BTC/USDC")
    monkeypatch.setattr(live_execution.service, "load_markets", lambda: {"BTC/USDC": market})
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: SimpleNamespace(free=Decimal("100")),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_okx_demo_manual_position_candidate",
        lambda symbol: pytest.fail("candidate must not be created"),
    )

    with pytest.raises(RuntimeError, match="after reserve"):
        live_execution.create_okx_demo_manual_buy(
            symbol="BTC/USDC", quote_amount=Decimal("95.01")
        )


def test_manual_okx_demo_sell_uses_the_existing_guarded_sell_workflow(monkeypatch):
    exchange = _exchange(code="okx_demo", sell_enabled=True)
    submitted = []
    monkeypatch.setattr(live_execution, "_live_exchange", lambda side: exchange)
    monkeypatch.setattr(
        live_execution.database,
        "get_position_by_id",
        lambda position_id: pd.DataFrame(
            [{
                "Id": position_id,
                "Position": 1,
                "Symbol": "BTC/USDC",
                "Bot": "manual_demo",
                "Strategy_Id": "okx_demo_manual",
                "Strategy_Name": "OKX Demo Manual Validation",
            }]
        ),
    )
    monkeypatch.setattr(
        live_execution,
        "create_sell_order",
        lambda **kwargs: submitted.append(kwargs) or {"result": "submitted"},
    )

    outcome = live_execution.create_okx_demo_manual_sell(
        position_id=9, limit_price=Decimal("99")
    )

    assert outcome == {"result": "submitted"}
    assert submitted[0]["position_id"] == 9
    assert submitted[0]["percentage"] == 100
    assert submitted[0]["execution_style"] == "okx_demo_limit_ioc"
    assert submitted[0]["limit_price"] == Decimal("99")


def test_trade_notification_failure_cannot_propagate_after_persistence(monkeypatch, capsys):
    monkeypatch.setattr(
        live_execution.telegram,
        "send_trade_event",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("telegram unavailable")),
    )
    monkeypatch.setattr(live_execution.database, "exchange_log_prefix", lambda: "[okx_demo]")

    live_execution._send_trade_notification(
        bot="manual_demo",
        emoji="x",
        action="BUY",
        symbol="BTC/USDC",
        strategy="OKX Demo Manual Validation",
        unit_price=100,
        quantity=0.1,
        notional_value=10,
    )

    assert "Trade notification failed after persistence" in capsys.readouterr().out


def test_manual_demo_ioc_sell_uses_explicit_limit_order_not_market_order(monkeypatch):
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    submitted = []
    monkeypatch.setattr(
        live_execution,
        "_live_exchange",
        lambda side: _exchange(code="okx_demo", sell_enabled=True),
    )
    monkeypatch.setattr(
        live_execution,
        "_sell_amount",
        lambda **kwargs: (
            Decimal("0.1"),
            market,
            SimpleNamespace(get=lambda key, default="": ""),
        ),
    )
    monkeypatch.setattr(
        live_execution,
        "get_okx_demo_manual_sell_quote",
        lambda **kwargs: {"symbol": "BTC/USDC", "best_bid": Decimal("99"), "best_ask": Decimal("101")},
    )
    monkeypatch.setattr(live_execution.service, "normalize_price", lambda _symbol, price: price)
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(True, estimated_cost=Decimal("9.9")),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: {"Id": 1, "Client_Order_Id": "bec-test", **kwargs},
    )
    monkeypatch.setattr(live_execution.database, "mark_order_intent_submitting", lambda _id: None)
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(
            create_limit_sell_ioc=lambda *args, **kwargs: submitted.append((args, kwargs))
            or _result()
        ),
    )
    monkeypatch.setattr(
        live_execution.service,
        "create_market_sell",
        lambda *_args, **_kwargs: pytest.fail("market sell must not be used"),
    )
    monkeypatch.setattr(
        live_execution.database,
        "apply_order_result",
        lambda *_args: {"delta_executed_qty": 0, "closed_position": False, "terminal": True},
    )
    monkeypatch.setattr(live_execution.database, "get_order_intent", lambda _id: {})
    monkeypatch.setattr(live_execution.telegram, "send_trade_event", lambda **kwargs: None)
    monkeypatch.setattr(live_execution.database, "get_num_open_positions", lambda: 1)

    live_execution.create_sell_order(
        symbol="BTC/USDC",
        bot="manual_demo",
        position_id=9,
        execution_style="okx_demo_limit_ioc",
        limit_price=Decimal("99"),
    )

    assert submitted == [
        (("BTC/USDC", Decimal("0.1"), Decimal("99")), {"client_order_id": "bec-test"})
    ]


def test_manual_demo_limit_ioc_rejects_a_price_above_the_latest_bid(monkeypatch):
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    monkeypatch.setattr(
        live_execution,
        "_live_exchange",
        lambda side: _exchange(code="okx_demo", sell_enabled=True),
    )
    monkeypatch.setattr(
        live_execution,
        "_sell_amount",
        lambda **kwargs: (Decimal("0.1"), market, SimpleNamespace()),
    )
    monkeypatch.setattr(live_execution.service, "normalize_price", lambda _symbol, price: price)
    monkeypatch.setattr(
        live_execution,
        "get_okx_demo_manual_sell_quote",
        lambda **kwargs: {"symbol": "BTC/USDC", "best_bid": Decimal("99"), "best_ask": Decimal("101")},
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: pytest.fail("intent must not be created"),
    )

    with pytest.raises(RuntimeError, match="above the current best bid"):
        live_execution.create_sell_order(
            symbol="BTC/USDC",
            bot="manual_demo",
            position_id=9,
            execution_style="okx_demo_limit_ioc",
            limit_price=Decimal("100"),
        )


def test_manual_demo_sell_quote_uses_the_public_best_bid(monkeypatch):
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda symbol: "BTC/USDC")
    monkeypatch.setattr(
        live_execution.service,
        "fetch_order_book",
        lambda symbol, limit: OrderBook(
            symbol,
            bids=((Decimal("99.12"), Decimal("0.4")),),
            asks=((Decimal("99.24"), Decimal("0.3")),),
        ),
    )
    monkeypatch.setattr(live_execution.service, "normalize_price", lambda _symbol, price: price)

    quote = live_execution.get_okx_demo_manual_sell_quote(symbol="BTC/USDC")

    assert quote == {
        "symbol": "BTC/USDC",
        "best_bid": Decimal("99.12"),
        "best_ask": Decimal("99.24"),
    }


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


def test_okx_submission_reporting_withholds_exchange_request_details(monkeypatch):
    reported = []
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: SimpleNamespace(code="okx", is_known_submission_rejection=lambda exc: True),
    )
    monkeypatch.setattr(live_execution.database, "mark_order_intent_rejected", lambda *_args: None)
    monkeypatch.setattr(
        live_execution.telegram,
        "send_error_event",
        lambda **kwargs: reported.append(kwargs),
    )

    live_execution._record_submission_failure(
        7, "create exchange sell order", ValueError("request payload contains secret")
    )

    assert "secret" not in repr(reported[0]["exception"])
    assert "request details withheld" in repr(reported[0]["exception"])


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
