from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from bec.exchanges import live_execution
from bec.exchanges.base import (
    ExchangeCapabilities,
    MarketInfo,
    OrderFill,
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


def _pending_result(status=OrderStatus.PENDING, *, executed="0"):
    return OrderResult(
        exchange_order_id="K-1",
        symbol="BTC/USDC",
        exchange_symbol="XBTUSDC",
        side="buy",
        status=status,
        requested_quantity=Decimal("0.2"),
        executed_quantity=Decimal(executed),
        average_price=Decimal("100") if Decimal(executed) else None,
        client_order_id="bec-test",
        raw={"id": "K-1", "status": status.value},
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


def test_kraken_live_preflight_requires_balance_and_reconciliation_access(monkeypatch):
    reads = []
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
        lambda required=True: _exchange(
            buy_enabled=True, quote_asset="USDC", execution_environment="production"
        ),
    )
    monkeypatch.setattr(
        live_execution.config, "load_settings", lambda refresh=True: _settings()
    )
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: reads.append(("balance", asset)),
    )
    monkeypatch.setattr(
        live_execution.service,
        "check_reconciliation_access",
        lambda: reads.append(("orders", None)),
    )

    live_execution._live_exchange("buy")

    assert reads == [("balance", "USDC"), ("orders", None)]

    monkeypatch.setattr(
        live_execution.service,
        "check_reconciliation_access",
        lambda: (_ for _ in ()).throw(PermissionError("closed orders denied")),
    )
    with pytest.raises(RuntimeError, match="Query Closed Orders & Trades"):
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


def test_kraken_manual_live_test_reports_all_local_blockers(monkeypatch):
    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=False: _exchange(
            code="kraken",
            buy_enabled=True,
            quote_asset="USDC",
            execution_environment="production",
        ),
    )
    monkeypatch.setattr(
        live_execution.config, "read_setting", lambda name: "live"
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_job_schedule_enabled",
        lambda name: name == live_execution.database.RECONCILIATION_JOB
        or name == "main_1h",
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_num_open_positions",
        lambda: 1,
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_unsettled_order_intents",
        lambda exchange_id: [{"Id": 7}],
    )

    blockers = live_execution.get_kraken_manual_test_blockers()

    assert any("main_1h" in blocker for blocker in blockers)
    assert any("Close existing Kraken positions" in blocker for blocker in blockers)
    assert any("unsettled orders" in blocker for blocker in blockers)


def test_kraken_manual_live_test_submits_fixed_buy_and_disarms(monkeypatch):
    market = MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True)
    submitted = []
    settings_updates = []
    exchange = _exchange(
        code="kraken",
        buy_enabled=True,
        quote_asset="USDC",
        execution_environment="production",
    )
    monkeypatch.setattr(
        live_execution, "get_kraken_manual_test_blockers", lambda: ()
    )
    monkeypatch.setattr(live_execution, "_live_exchange", lambda side: exchange)
    monkeypatch.setattr(
        live_execution.service, "normalize_symbol", lambda symbol: "BTC/USDC"
    )
    monkeypatch.setattr(
        live_execution.service, "load_markets", lambda: {"BTC/USDC": market}
    )
    monkeypatch.setattr(
        live_execution.service,
        "fetch_balance",
        lambda asset: SimpleNamespace(free=Decimal("500")),
    )
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda request: OrderValidation(True, estimated_cost=request.quote_amount),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_kraken_manual_position_candidate",
        lambda symbol: 42,
    )
    monkeypatch.setattr(
        live_execution,
        "create_buy_order",
        lambda **kwargs: submitted.append(kwargs) or {"result": "submitted"},
    )
    monkeypatch.setattr(
        live_execution.config,
        "update_setting",
        lambda name, value: settings_updates.append((name, value)),
    )

    outcome = live_execution.create_kraken_manual_test_buy(
        confirmation=live_execution.KRAKEN_MANUAL_TEST_CONFIRMATION
    )

    assert outcome == {"result": "submitted"}
    assert submitted == [
        {
            "symbol": "BTC/USDC",
            "bot": "manual_live",
            "strategy_id": "kraken_manual_live",
            "strategy_name": "Kraken Manual Live Validation",
            "position_id": 42,
            "strategy_params_json": "{}",
            "quote_amount": Decimal("50"),
        }
    ]
    assert settings_updates == [("run_mode", "test")]


def test_kraken_manual_live_test_wrong_confirmation_disarms_without_order(monkeypatch):
    settings_updates = []
    monkeypatch.setattr(
        live_execution, "_live_exchange", lambda side: pytest.fail("must not arm")
    )
    monkeypatch.setattr(
        live_execution.config,
        "update_setting",
        lambda name, value: settings_updates.append((name, value)),
    )

    with pytest.raises(ValueError, match="Type BUY 50 USDC"):
        live_execution.create_kraken_manual_test_buy(confirmation="wrong")

    assert settings_updates == [("run_mode", "test")]


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


def test_buy_opening_is_detected_only_on_its_first_persisted_fill():
    intent = {
        "Side": "BUY",
        "Position_Id": 9,
        "Applied_Net_Qty": 0,
    }

    assert live_execution._buy_opened_position(
        intent, {"delta_net_qty": 0.1}
    )
    assert not live_execution._buy_opened_position(
        {**intent, "Applied_Net_Qty": 0.1},
        {"delta_net_qty": 0.1},
    )
    assert not live_execution._buy_opened_position(
        {**intent, "Position_Id": None},
        {"delta_net_qty": 0.1},
    )
    assert not live_execution._buy_opened_position(
        intent, {"delta_net_qty": 0}
    )


def test_order_telemetry_separates_execution_and_confirmation_times():
    trade_time = datetime(2026, 8, 21, 10, 0, 2, tzinfo=timezone.utc)
    result = OrderResult(
        exchange_order_id="K-1",
        symbol="BTC/USDC",
        exchange_symbol="XBTUSDC",
        side="buy",
        status=OrderStatus.FILLED,
        requested_quantity=Decimal("0.2"),
        executed_quantity=Decimal("0.2"),
        average_price=Decimal("100"),
        fills=(
            OrderFill(
                price=Decimal("100"),
                quantity=Decimal("0.2"),
                trade_id="T-1",
                timestamp=trade_time,
            ),
        ),
        client_order_id="bec-test",
        raw={"lastUpdateTimestamp": 1_777_000_000_000},
    )

    telemetry = live_execution._order_telemetry(
        _exchange(name="Kraken", execution_environment="production"),
        result,
        intent={
            "Date": "2026-08-21T10:00:00+00:00",
            "Last_Reconciled_At": "2026-08-21T10:15:00+00:00",
        },
        confirmation_source="scheduled reconciliation",
    )

    assert telemetry["submitted_at"] == "2026-08-21T10:00:00Z"
    assert telemetry["executed_at"] == "2026-08-21T10:00:02Z"
    assert telemetry["order_closed_at"] == ""
    assert telemetry["confirmed_at"] == "2026-08-21T10:15:00Z"
    assert telemetry["confirmation_delay"] == "900s"
    assert telemetry["confirmation_source"] == "scheduled reconciliation"

    closed_only = OrderResult(
        exchange_order_id="K-2",
        symbol="BTC/USDC",
        exchange_symbol="XBTUSDC",
        side="buy",
        status=OrderStatus.FILLED,
        requested_quantity=Decimal("0.2"),
        executed_quantity=Decimal("0.2"),
        average_price=Decimal("100"),
        fills=(OrderFill(price=Decimal("100"), quantity=Decimal("0.2")),),
        raw={"timestamp": 1_776_999_000_000, "lastUpdateTimestamp": 1_777_000_000_000},
    )
    close_telemetry = live_execution._order_telemetry(_exchange(), closed_only)
    expected_close = datetime.fromtimestamp(
        1_777_000_000_000 / 1000, tz=timezone.utc
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    assert close_telemetry["executed_at"] == ""
    assert close_telemetry["order_closed_at"] == expected_close


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
    monkeypatch.setattr(
        live_execution.database, "is_symbol_blacklisted", lambda symbol: False
    )
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


def test_regional_restriction_is_rejected_blacklisted_and_reported_with_context(
    monkeypatch,
):
    error = RuntimeError(
        'kraken {"error":["EAccount:Invalid permissions:XMR trading restricted for PT."]}'
    )
    rejected = []
    blacklisted = []
    reported = []
    adapter = SimpleNamespace(
        code="kraken",
        regional_trading_restriction=lambda value: (
            {"asset": "XMR", "region": "PT"}
            if "trading restricted" in str(value)
            else None
        ),
        is_known_submission_rejection=lambda exc: True,
    )
    monkeypatch.setattr(live_execution.service, "get_adapter", lambda: adapter)
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_rejected",
        lambda intent_id, detail: rejected.append((intent_id, detail)),
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_unknown",
        lambda *_args: pytest.fail("regional restriction must not be unknown"),
    )
    monkeypatch.setattr(
        live_execution.database,
        "ensure_symbol_blacklisted",
        lambda symbol: blacklisted.append(symbol) or True,
    )
    monkeypatch.setattr(
        live_execution.telegram,
        "send_error_event",
        lambda **kwargs: reported.append(kwargs),
    )

    live_execution._record_submission_failure(
        17,
        "create exchange buy order",
        error,
        symbol="XMR/USDC",
        timeframe="1d",
        strategy="HMA RSI",
    )

    assert rejected == [(17, repr(error))]
    assert blacklisted == ["XMR"]
    assert reported[0]["symbol"] == "XMR/USDC"
    assert reported[0]["timeframe"] == "1d"
    assert reported[0]["strategy"] == "HMA RSI"
    assert reported[0]["reason"] == "The exchange rejected XMR trading for region PT"
    assert "marked rejected" in reported[0]["impact"]
    assert "blacklist" in reported[0]["impact"]
    assert "No reconciliation" in reported[0]["next_step"]


def test_non_regional_rejection_never_changes_blacklist(monkeypatch):
    adapter = SimpleNamespace(
        code="kraken",
        regional_trading_restriction=lambda value: None,
        is_known_submission_rejection=lambda exc: True,
    )
    monkeypatch.setattr(live_execution.service, "get_adapter", lambda: adapter)
    monkeypatch.setattr(
        live_execution.database, "mark_order_intent_rejected", lambda *_args: None
    )
    monkeypatch.setattr(
        live_execution.database,
        "ensure_symbol_blacklisted",
        lambda symbol: pytest.fail(
            "a non-regional rejection must never modify the blacklist"
        ),
    )
    monkeypatch.setattr(
        live_execution.telegram, "send_error_event", lambda **kwargs: None
    )

    live_execution._record_submission_failure(
        18,
        "create exchange buy order",
        RuntimeError("EOrder:Insufficient funds"),
        symbol="BTC/USDC",
        timeframe="1h",
        strategy="EMA Cross",
    )


def test_blacklisted_base_asset_skips_buy_before_balance_or_intent(monkeypatch):
    market = MarketInfo("XMR/USDC", "XMRUSDC", "XMR", "USDC", True)
    monkeypatch.setattr(
        live_execution, "_guarded_live_exchange", lambda side: (_exchange(), None)
    )
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda symbol: symbol)
    monkeypatch.setattr(live_execution.service, "load_markets", lambda: {market.symbol: market})
    monkeypatch.setattr(
        live_execution.database,
        "is_symbol_blacklisted",
        lambda symbol: symbol == "XMR",
    )
    monkeypatch.setattr(
        live_execution,
        "_stake_amount",
        lambda quote: pytest.fail("a blacklisted buy must not read sizing/balance"),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **kwargs: pytest.fail("a blacklisted buy must not create an intent"),
    )

    result = live_execution.create_buy_order(
        symbol="XMR/USDC", bot="1d", strategy_id="hma_rsi"
    )

    assert result["skipped"] is True
    assert "XMR" in result["reason"]


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


@pytest.mark.parametrize("terminal_query", [1, 2, 3])
def test_kraken_acknowledgement_is_polled_until_terminal_query(
    monkeypatch, terminal_query
):
    intent = {
        "Id": 7,
        "Side": "BUY",
        "Symbol": "BTC/USDC",
        "Bot": "1h",
        "Client_Order_Id": "bec-test",
        "Applied_Executed_Qty": 0,
    }
    sleeps = []
    acknowledgements = []
    applied = []
    responses = [
        _pending_result()
        for _ in range(terminal_query - 1)
    ] + [_result(side="buy")]
    monkeypatch.setattr(live_execution.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        live_execution.database,
        "record_order_acknowledgement",
        lambda intent_id, result: acknowledgements.append((intent_id, result.status)),
    )
    monkeypatch.setattr(
        live_execution.service, "fetch_order", lambda *_args: responses.pop(0)
    )
    monkeypatch.setattr(
        live_execution.database, "get_order_intent", lambda _id: intent
    )
    monkeypatch.setattr(
        live_execution,
        "_apply_order_result_and_notify",
        lambda **kwargs: applied.append(kwargs) or {"delta_executed_qty": 0},
    )

    outcome = live_execution._persist_submitted_order(
        intent=intent,
        acknowledgement=_pending_result(OrderStatus.UNKNOWN),
        exchange=_exchange(),
        strategy_name="EMA",
        reason="Entry condition fulfilled",
    )

    assert acknowledgements == [(7, OrderStatus.UNKNOWN)]
    assert sleeps == [1, 2, 4][:terminal_query]
    assert len(applied) == terminal_query
    assert all(
        item["confirmation_source"] == "immediate order query" for item in applied
    )
    assert outcome["result"].status is OrderStatus.FILLED
    assert outcome["confirmation_deferred"] is False


def test_kraken_three_nonterminal_reads_defer_to_scheduled_reconciliation(monkeypatch):
    intent = {"Id": 7, "Side": "BUY", "Symbol": "BTC/USDC"}
    sleeps = []
    applied = []
    monkeypatch.setattr(live_execution.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        live_execution.database, "record_order_acknowledgement", lambda *_args: None
    )
    monkeypatch.setattr(
        live_execution.service, "fetch_order", lambda *_args: _pending_result()
    )
    monkeypatch.setattr(
        live_execution.database, "get_order_intent", lambda _id: intent
    )
    monkeypatch.setattr(
        live_execution,
        "_apply_order_result_and_notify",
        lambda **kwargs: applied.append(kwargs) or {"delta_executed_qty": 0},
    )

    outcome = live_execution._persist_submitted_order(
        intent=intent,
        acknowledgement=_pending_result(OrderStatus.UNKNOWN),
        exchange=_exchange(),
        strategy_name="EMA",
        reason="Entry condition fulfilled",
    )

    assert sleeps == [1, 2, 4]
    assert len(applied) == 3
    assert outcome["confirmation_deferred"] is True


def test_kraken_first_confirmation_timeout_stops_polling_without_resubmission(
    monkeypatch,
):
    intent = {"Id": 7, "Side": "BUY", "Symbol": "BTC/USDC"}
    sleeps = []
    reads = []
    monkeypatch.setattr(live_execution.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        live_execution.database, "record_order_acknowledgement", lambda *_args: None
    )

    def timeout(*_args):
        reads.append(True)
        raise TimeoutError("read timed out")

    monkeypatch.setattr(live_execution.service, "fetch_order", timeout)
    monkeypatch.setattr(
        live_execution,
        "_apply_order_result_and_notify",
        lambda **kwargs: pytest.fail("a failed first read has no result to apply"),
    )

    outcome = live_execution._persist_submitted_order(
        intent=intent,
        acknowledgement=_pending_result(OrderStatus.UNKNOWN),
        exchange=_exchange(),
        strategy_name="EMA",
        reason="Entry condition fulfilled",
    )

    assert sleeps == [1]
    assert len(reads) == 1
    assert outcome["confirmation_deferred"] is True


def test_duplicate_unsettled_action_returns_structured_skip_without_submission(
    monkeypatch,
):
    market = MarketInfo("BTC/USDC", "XBTUSDC", "BTC", "USDC", True)
    submitted = []
    monkeypatch.setattr(
        live_execution, "_guarded_live_exchange", lambda side: (_exchange(), None)
    )
    monkeypatch.setattr(live_execution.service, "normalize_symbol", lambda value: value)
    monkeypatch.setattr(
        live_execution.service, "load_markets", lambda: {market.symbol: market}
    )
    monkeypatch.setattr(
        live_execution.database, "is_symbol_blacklisted", lambda _asset: False
    )
    monkeypatch.setattr(live_execution, "_stake_amount", lambda _asset: Decimal("10"))
    monkeypatch.setattr(
        live_execution.service,
        "validate_order",
        lambda _request: OrderValidation(True),
    )
    monkeypatch.setattr(
        live_execution.database,
        "create_order_intent",
        lambda **_kwargs: (_ for _ in ()).throw(
            live_execution.database.UnsettledOrderIntentError(
                "An unsettled order intent already exists for this action"
            )
        ),
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_submitting",
        lambda *_args: pytest.fail("a duplicate intent must not be submitted"),
    )
    monkeypatch.setattr(
        live_execution.service,
        "create_market_buy",
        lambda *_args, **_kwargs: submitted.append(True),
    )

    outcome = live_execution.create_buy_order(symbol="BTC/USDC", bot="1h")

    assert outcome == {
        "skipped": True,
        "reason": "An unsettled order intent already exists for this action",
        "reconciliation_required": True,
    }
    assert submitted == []


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


def test_reconciliation_returns_without_creating_adapter_when_no_intents(monkeypatch):
    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=False: _exchange(),
    )
    monkeypatch.setattr(
        live_execution.database, "get_unsettled_order_intents", lambda _id: []
    )
    monkeypatch.setattr(
        live_execution.service,
        "get_adapter",
        lambda: pytest.fail("no adapter or Kraken request is needed"),
    )

    assert live_execution.reconcile_unsettled_orders() == {
        "checked": 0,
        "updated": 0,
        "unresolved": 0,
    }


def test_reconciliation_uses_txid_before_client_order_search(monkeypatch):
    calls = []
    intent = {
        "Id": 1,
        "Side": "BUY",
        "Symbol": "BTC/USDC",
        "Exchange_Order_Id": "K-1",
        "Client_Order_Id": "bec-test",
    }

    class Adapter:
        capabilities = ExchangeCapabilities(supports_reconciliation=True)

        def fetch_order(self, exchange_order_id, symbol):
            calls.append(("txid", exchange_order_id, symbol))
            return _result(side="buy")

        def fetch_order_by_client_id(self, *_args):
            pytest.fail("a resolved txid must not trigger a client-id scan")

    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=False: _exchange(),
    )
    monkeypatch.setattr(
        live_execution.database, "get_unsettled_order_intents", lambda _id: [intent]
    )
    monkeypatch.setattr(live_execution.service, "get_adapter", lambda: Adapter())
    monkeypatch.setattr(
        live_execution,
        "_apply_order_result_and_notify",
        lambda **_kwargs: {"delta_executed_qty": 0},
    )

    assert live_execution.reconcile_unsettled_orders()["updated"] == 1
    assert calls == [("txid", "K-1", "BTC/USDC")]


def test_reconciliation_notifies_when_first_buy_fill_opens_position(monkeypatch):
    notifications = []
    intent = {
        "Id": 1,
        "Side": "BUY",
        "Symbol": "BTC/USDC",
        "Bot": "1h",
        "Strategy_Id": "ema_cross",
        "Position_Id": 9,
        "Applied_Net_Qty": 0,
        "Requested_Quote_Qty": 20,
        "Exchange_Order_Id": "K-1",
        "Client_Order_Id": "bec-test",
    }

    class Adapter:
        capabilities = ExchangeCapabilities(supports_reconciliation=True)

        def fetch_order_by_client_id(self, client_order_id, symbol):
            return _result(side="buy")

    monkeypatch.setattr(
        live_execution.database,
        "get_active_exchange",
        lambda required=False: _exchange(
            name="Kraken",
            execution_environment="production",
        ),
    )
    monkeypatch.setattr(live_execution.service, "get_adapter", lambda: Adapter())
    monkeypatch.setattr(
        live_execution.database,
        "get_unsettled_order_intents",
        lambda exchange_id: [intent],
    )
    monkeypatch.setattr(
        live_execution.database,
        "apply_order_result",
        lambda intent_id, result: {
            "delta_net_qty": 0.2,
            "delta_executed_qty": 0.2,
            "closed_position": False,
            "terminal": True,
        },
    )
    monkeypatch.setattr(
        live_execution.database, "get_num_open_positions", lambda: 1
    )
    monkeypatch.setattr(
        live_execution.database,
        "get_order_intent",
        lambda _id: {
            **intent,
            "Last_Reconciled_At": "2026-08-21T10:15:00+00:00",
        },
    )
    monkeypatch.setattr(
        live_execution,
        "_send_trade_notification",
        lambda **kwargs: notifications.append(kwargs),
    )

    stats = live_execution.reconcile_unsettled_orders()

    assert stats == {"checked": 1, "updated": 1, "unresolved": 0}
    assert len(notifications) == 1
    assert notifications[0]["action"] == "BUY"
    assert notifications[0]["position_opened"] is True
    assert notifications[0]["reason"] == (
        "Entry confirmed during scheduled reconciliation"
    )
    assert notifications[0]["confirmation_source"] == "scheduled reconciliation"


def test_reconciliation_settles_stored_invalid_kraken_client_id_rejection(monkeypatch):
    rejected = []

    class Adapter:
        code = "kraken"
        capabilities = ExchangeCapabilities(supports_reconciliation=True)

        def is_known_persisted_submission_rejection(self, error):
            return "Invalid arguments:cl_ord_id" in error

        def fetch_order_by_client_id(self, *_args):
            pytest.fail("a definitively rejected order must not be looked up")

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
                "Client_Order_Id": "bec" + ("0" * 28),
                "Error_Message": (
                    "ExchangeError('kraken "
                    "{\"error\":[\"EGeneral:Invalid arguments:cl_ord_id\"]}')"
                ),
            }
        ],
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_rejected",
        lambda intent_id, error: rejected.append((intent_id, error)),
    )

    stats = live_execution.reconcile_unsettled_orders()

    assert stats == {"checked": 1, "updated": 1, "unresolved": 0}
    assert rejected[0][0] == 1


def test_reconciliation_blacklists_stored_regional_restriction(monkeypatch):
    rejected = []
    blacklisted = []
    error = (
        "ExchangeError('kraken "
        '{"error":["EAccount:Invalid permissions:XMR trading restricted for PT."]}'
        "')"
    )

    class Adapter:
        code = "kraken"
        capabilities = ExchangeCapabilities(supports_reconciliation=True)

        def regional_trading_restriction(self, value):
            return (
                {"asset": "XMR", "region": "PT"}
                if "trading restricted" in value
                else None
            )

        def is_known_persisted_submission_rejection(self, value):
            return self.regional_trading_restriction(value) is not None

        def fetch_order_by_client_id(self, *_args):
            pytest.fail("a definitively rejected order must not be looked up")

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
                "Id": 22,
                "Side": "BUY",
                "Symbol": "XMR/USDC",
                "Client_Order_Id": "bec-test",
                "Error_Message": error,
            }
        ],
    )
    monkeypatch.setattr(
        live_execution.database,
        "mark_order_intent_rejected",
        lambda intent_id, detail: rejected.append((intent_id, detail)),
    )
    monkeypatch.setattr(
        live_execution.database,
        "ensure_symbol_blacklisted",
        lambda symbol: blacklisted.append(symbol) or True,
    )

    stats = live_execution.reconcile_unsettled_orders()

    assert stats == {"checked": 1, "updated": 1, "unresolved": 0}
    assert rejected == [(22, error)]
    assert blacklisted == ["XMR"]
