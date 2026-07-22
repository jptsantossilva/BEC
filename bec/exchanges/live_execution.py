"""Gated spot live-order workflow with durable intents and reconciliation."""

from __future__ import annotations

from decimal import Decimal

from bec.exchanges import service
from bec.exchanges.base import OrderRequest
from bec.utils import config, database, telegram


def _live_exchange(side: str) -> dict:
    exchange = database.get_active_exchange(required=True)
    adapter = service.get_adapter()
    name = str(exchange.get("name") or adapter.name)
    if not adapter.capabilities.uses_gated_live_execution:
        raise RuntimeError(f"The gated live workflow is unavailable for {name}")
    settings = config.load_settings(refresh=True)
    adapter_code = str(getattr(adapter, "code", exchange.get("code", "")))
    exchange_code = str(exchange.get("code", ""))
    if adapter_code == "okx" and (
        exchange_code != "okx"
        or str(exchange.get("execution_environment") or "") != "production"
    ):
        raise RuntimeError("OKX production execution requires the active okx production identity")
    if adapter_code == "okx_demo" and exchange_code != "okx_demo":
        raise RuntimeError("OKX demo execution requires the active okx_demo identity")
    if adapter_code == "okx_demo" and settings.run_mode != "demo":
        raise RuntimeError("OKX demo execution requires run_mode=demo")
    if adapter_code == "okx" and settings.run_mode != "live":
        raise RuntimeError("OKX production execution requires run_mode=live")
    if adapter_code != "okx_demo" and settings.run_mode == "demo":
        raise RuntimeError("run_mode=demo is reserved for the active OKX Demo identity")
    if adapter_code not in {"okx", "okx_demo"} and settings.run_mode == "test":
        raise RuntimeError(f"{name} live execution is disabled while run_mode=test")
    flag = "buy_enabled" if str(side).lower() == "buy" else "sell_enabled"
    if not bool(exchange[flag]):
        raise RuntimeError(f"{name} {side} operations are disabled in Trading Settings")
    if not bool(getattr(adapter, "private_enabled", False)):
        raise RuntimeError(f"{name} private credentials are not configured")
    if adapter_code == "okx":
        if database.get_compatible_okx_demo_validation(exchange) is None:
            raise RuntimeError(
                "OKX production execution requires a compatible completed OKX Demo validation record"
            )
        try:
            # This is intentionally a balance-only preflight. It proves the
            # selected production identity is authenticated immediately before
            # a submission path can create its durable order intent.
            service.fetch_balance(str(exchange.get("quote_asset") or ""))
        except Exception as exc:
            raise RuntimeError(
                "OKX production private balance preflight failed; run the read-only "
                "check and correct credentials, permissions, IP allowlist, or region"
            ) from exc
        print(
            database.exchange_log_prefix(),
            "OKX production private preflight succeeded "
            f"(adapter={exchange.get('adapter_id')}, quote={exchange.get('quote_asset')})",
        )
    return exchange


def _guarded_live_exchange(side: str) -> tuple[dict | None, dict | None]:
    try:
        return _live_exchange(side), None
    except RuntimeError as exc:
        message = str(exc)
        print(database.exchange_log_prefix(), message)
        return None, {"skipped": True, "reason": message}


def _stake_amount(quote_asset: str) -> Decimal:
    settings = config.load_settings()
    balance = service.fetch_balance(quote_asset)
    available = Decimal(str(balance.free))
    if settings.stake_amount_type == "unlimited":
        open_positions = database.get_num_open_positions()
        if open_positions >= settings.max_number_of_open_positions:
            raise RuntimeError("Maximum open positions already occupied")
        tradable = available * Decimal(str(settings.tradable_balance_ratio))
        if settings.lock_values:
            tradable -= Decimal(str(database.get_total_locked_values()))
        remaining_slots = settings.max_number_of_open_positions - open_positions
        amount = tradable / Decimal(remaining_slots)
        amount = max(amount, Decimal(str(settings.min_position_size)))
    else:
        amount = Decimal(str(settings.stake_amount_type))
    if amount <= 0 or amount > available:
        raise RuntimeError(f"Insufficient {quote_asset} balance")
    return amount


def _is_known_submission_rejection(exc: Exception) -> bool:
    """Return true only when an exception proves the order was not submitted."""
    try:
        return bool(service.get_adapter().is_known_submission_rejection(exc))
    except (AttributeError, RuntimeError):
        # Keep the failure path safe for a partially initialized runtime.
        return isinstance(exc, ValueError)


def _safe_exception_for_operator_reporting(exc: Exception) -> Exception:
    """Avoid forwarding an OKX/CCXT request payload to logs or Telegram."""
    try:
        adapter_code = str(service.get_adapter().code or "").lower()
    except (AttributeError, RuntimeError):
        adapter_code = ""
    if adapter_code in {"okx", "okx_demo"}:
        return RuntimeError(f"{type(exc).__name__} (OKX request details withheld)")
    return exc


def _record_submission_failure(intent_id: int, operation: str, exc: Exception) -> None:
    reported_exception = _safe_exception_for_operator_reporting(exc)
    if _is_known_submission_rejection(exc):
        database.mark_order_intent_rejected(intent_id, repr(exc))
        telegram.send_error_event(
            action=operation,
            reason="Exchange submission was rejected",
            impact="No retry was scheduled and no replacement order was submitted.",
            next_step=(
                "Correct the validation, balance, credential, or permission issue "
                "before creating a new intent."
            ),
            exception=reported_exception,
            notify_main=False,
        )
        return
    database.mark_order_intent_unknown(intent_id, repr(exc))
    telegram.send_error_event(
        action=operation,
        reason="Exchange submission outcome is uncertain",
        impact="The intent was not retried and requires reconciliation.",
        next_step="Check the exchange order and reconciliation logs before any manual action.",
        exception=reported_exception,
        notify_main=False,
    )


def _post_apply_sell(intent: dict, applied: dict, average_price: float) -> None:
    if applied["delta_executed_qty"] <= 0 or intent.get("Position_Id") is None:
        return
    position_id = int(intent["Position_Id"])
    if applied["closed_position"]:
        database.release_value(position_id)
        return
    take_profit_num = int(intent.get("Take_Profit_Num") or 0)
    if take_profit_num > 0 and applied["terminal"]:
        database.mark_position_take_profit(
            bot=str(intent["Bot"]),
            symbol=str(intent["Symbol"]),
            take_profit_num=take_profit_num,
            position_id=position_id,
        )
    if config.load_settings().lock_values:
        position = database.get_position_by_id(position_id)
        buy_order_id = (
            str(position["Buy_Order_Id"].iloc[0]) if not position.empty else "0"
        )
        database.lock_value(
            position_id=position_id,
            buy_order_id=buy_order_id,
            amount=float(average_price) * float(applied["delta_executed_qty"]),
    )


def _send_trade_notification(*, bot: str, **event) -> None:
    """Notify best-effort only after an order result has been persisted.

    A notification failure cannot make a submitted order look unsuccessful to
    the operator, which could otherwise prompt an unsafe duplicate submission.
    """
    try:
        telegram.send_trade_event(
            telegram_token=telegram.get_telegram_token(),
            telegram_prefix=telegram.get_telegram_prefix(bot),
            timeframe=bot,
            **event,
        )
    except Exception as exc:
        print(
            database.exchange_log_prefix(),
            f"Trade notification failed after persistence ({type(exc).__name__})",
        )


def _require_okx_production_strategy_gate(
    exchange: dict, *, bot: str, symbol: str, strategy_id: str, side: str
) -> None:
    """Apply the gates that are meaningful only for scheduled OKX production flow."""
    if str(exchange.get("code") or "") != "okx":
        return
    if not database.is_trade_main_timeframe_enabled(str(bot)):
        raise RuntimeError(
            f"OKX production {side} requires its explicitly enabled {bot} main schedule"
        )
    if str(side).lower() == "buy" and not database.is_active_backtest_approved_for_live_buy(
        symbol=str(symbol), time_frame=str(bot), strategy_id=strategy_id
    ):
        raise RuntimeError(
            "OKX production buy requires an approved backtest with matching exchange, "
            "adapter, quote asset, fee, and fingerprint"
        )


def _order_telemetry(exchange: dict, result) -> dict[str, str]:
    fees: dict[str, Decimal] = {}
    for fill in result.fills:
        asset = str(fill.fee_asset or "").upper()
        if asset:
            fees[asset] = fees.get(asset, Decimal("0")) + Decimal(str(fill.fee_amount))
    fee_text = ", ".join(
        f"{asset} {amount}" for asset, amount in sorted(fees.items())
    )
    return {
        "exchange": str(exchange.get("name") or exchange.get("code") or ""),
        "environment": str(exchange.get("execution_environment") or ""),
        "client_order_id": str(result.client_order_id or ""),
        "order_status": str(result.status.value),
        "fees": fee_text,
        "reconciliation": "persisted",
    }


def _log_order_persistence(exchange: dict, result) -> None:
    telemetry = _order_telemetry(exchange, result)
    print(
        database.exchange_log_prefix(),
        "Order persisted "
        f"exchange={telemetry['exchange']} environment={telemetry['environment']} "
        f"client_order_id={telemetry['client_order_id']} status={telemetry['order_status']} "
        f"fees={telemetry['fees'] or 'none'} reconciliation={telemetry['reconciliation']}",
    )


def create_buy_order(
    *,
    symbol: str,
    bot: str,
    convert_all_balance: bool = False,
    strategy_id: str = "",
    strategy_name: str = "",
    position_id: int | None = None,
    strategy_params_json: str = "",
    quote_amount: Decimal | None = None,
):
    exchange, skipped = _guarded_live_exchange("buy")
    if skipped:
        return skipped
    _require_okx_production_strategy_gate(
        exchange, bot=bot, symbol=symbol, strategy_id=strategy_id, side="buy"
    )
    market = service.load_markets()[service.normalize_symbol(symbol)]
    if quote_amount is not None and convert_all_balance:
        raise ValueError("quote_amount and convert_all_balance are mutually exclusive")
    if quote_amount is not None:
        quote_amount = Decimal(str(quote_amount))
        if quote_amount <= 0:
            raise ValueError("Manual quote amount must be greater than zero")
        balance = service.fetch_balance(market.quote_asset)
        available = Decimal(str(balance.free))
        permitted = available
        if str(exchange["code"]) == "okx_demo":
            reserve = Decimal(str(exchange.get("sizing_buffer_pct") or 0))
            permitted *= (Decimal("100") - reserve) / Decimal("100")
        if quote_amount > permitted:
            raise RuntimeError(
                f"Manual quote amount exceeds available {market.quote_asset} after reserve"
            )
    elif convert_all_balance:
        balance = service.fetch_balance(market.quote_asset)
        quote_amount = Decimal(str(balance.free)) * Decimal(
            str(config.load_settings().tradable_balance_ratio)
        )
        if str(exchange["code"]) == "okx_demo":
            reserve = Decimal(str(exchange.get("sizing_buffer_pct") or 0))
            quote_amount *= (Decimal("100") - reserve) / Decimal("100")
    else:
        quote_amount = _stake_amount(market.quote_asset)
    validation = service.validate_order(
        OrderRequest(symbol=market.symbol, side="buy", quote_amount=quote_amount)
    )
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))

    intent = database.create_order_intent(
        side="BUY",
        symbol=market.symbol,
        bot=bot,
        position_id=None if convert_all_balance else position_id,
        requested_quote_qty=float(quote_amount),
        strategy_id=strategy_id,
        strategy_params_json=strategy_params_json,
    )
    database.mark_order_intent_submitting(intent["Id"])
    try:
        result = service.create_market_buy(
            market.symbol,
            quote_amount=quote_amount,
            client_order_id=intent["Client_Order_Id"],
        )
    except Exception as exc:
        _record_submission_failure(intent["Id"], "create exchange buy order", exc)
        return None
    applied = database.apply_order_result(intent["Id"], result)
    _log_order_persistence(exchange, result)
    _send_trade_notification(
        bot=bot,
        emoji=telegram.EMOJI_ENTER_TRADE,
        action="BUY",
        symbol=result.symbol,
        strategy=strategy_name or strategy_id,
        reason="Entry condition fulfilled",
        unit_price=float(result.average_price or 0),
        quantity=float(result.executed_quantity),
        notional_value=float(quote_amount),
        open_positions=str(database.get_num_open_positions()),
        **_order_telemetry(exchange, result),
    )
    return {"result": result, "applied": applied, "exchange": exchange}


def create_okx_demo_manual_buy(*, symbol: str, quote_amount: Decimal):
    """Submit one operator-confirmed demo buy through the normal lifecycle."""
    exchange = _live_exchange("buy")
    if str(exchange["code"]) != "okx_demo":
        raise RuntimeError("Manual orders are available only for the active OKX Demo identity")
    canonical = service.normalize_symbol(symbol)
    quote_amount = Decimal(str(quote_amount))
    if quote_amount <= 0:
        raise ValueError("Manual quote amount must be greater than zero")
    market = service.load_markets()[canonical]
    balance = service.fetch_balance(market.quote_asset)
    reserve = Decimal(str(exchange.get("sizing_buffer_pct") or 0))
    permitted = Decimal(str(balance.free)) * (Decimal("100") - reserve) / Decimal("100")
    if quote_amount > permitted:
        raise RuntimeError(
            f"Manual quote amount exceeds available {market.quote_asset} after reserve"
        )
    validation = service.validate_order(
        OrderRequest(symbol=canonical, side="buy", quote_amount=quote_amount)
    )
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))
    position_id = database.create_okx_demo_manual_position_candidate(canonical)
    return create_buy_order(
        symbol=canonical,
        bot="manual_demo",
        strategy_id="okx_demo_manual",
        strategy_name="OKX Demo Manual Validation",
        position_id=position_id,
        quote_amount=quote_amount,
    )


def get_okx_demo_manual_sell_quote(*, symbol: str) -> dict[str, str | Decimal | None]:
    """Return the public best bid used as a suggested demo limit-IOC price."""
    canonical = service.normalize_symbol(symbol)
    book = service.fetch_order_book(canonical, limit=1)
    if not book.bids:
        raise RuntimeError("OKX Demo best bid is unavailable; refresh market data before selling")
    best_bid = service.normalize_price(canonical, Decimal(str(book.bids[0][0])))
    if best_bid <= 0:
        raise RuntimeError("OKX Demo best bid is invalid; refresh market data before selling")
    best_ask = (
        service.normalize_price(canonical, Decimal(str(book.asks[0][0])))
        if book.asks
        else None
    )
    return {"symbol": canonical, "best_bid": best_bid, "best_ask": best_ask}


def create_okx_demo_manual_sell(*, position_id: int, limit_price: Decimal):
    """Close one demo position through a genuine explicit-price spot limit IOC."""
    exchange = _live_exchange("sell")
    if str(exchange["code"]) != "okx_demo":
        raise RuntimeError("Manual orders are available only for the active OKX Demo identity")
    position = database.get_position_by_id(int(position_id))
    if position.empty or int(position["Position"].iloc[0]) != 1:
        raise ValueError("Open OKX demo position not found")
    return create_sell_order(
        symbol=str(position["Symbol"].iloc[0]),
        bot=str(position["Bot"].iloc[0] or "manual_demo"),
        reason="Manual OKX demo validation sell",
        percentage=100,
        strategy_id=str(position["Strategy_Id"].iloc[0] or "okx_demo_manual"),
        strategy_name=str(position["Strategy_Name"].iloc[0] or "OKX Demo Manual Validation"),
        position_id=int(position_id),
        execution_style="okx_demo_limit_ioc",
        limit_price=Decimal(str(limit_price)),
    )


def _sell_amount(
    *, symbol: str, position_id: int | None, bot: str, percentage: float,
    convert_all_balance: bool,
) -> tuple[Decimal, object, object]:
    market = service.load_markets()[service.normalize_symbol(symbol)]
    balance = service.fetch_balance(market.base_asset)
    available = Decimal(str(balance.free))
    position = None
    if convert_all_balance:
        requested = available
    else:
        df = (
            database.get_position_by_id(position_id)
            if position_id is not None
            else database.get_positions_by_bot_symbol_position(bot, symbol, 1)
        )
        if df.empty:
            raise RuntimeError("Open position not found")
        position = df.iloc[0]
        requested = min(available, Decimal(str(position["Qty"])))
    requested *= Decimal(str(percentage)) / Decimal("100")
    return service.normalize_amount(market.symbol, requested), market, position


def create_sell_order(
    *,
    symbol: str,
    bot: str,
    reason: str = "",
    percentage: float = 100,
    take_profit_num: int = 0,
    convert_all_balance: bool = False,
    strategy_id: str = "",
    strategy_name: str = "",
    position_id: int | None = None,
    execution_style: str = "market",
    limit_price: Decimal | None = None,
):
    exchange, skipped = _guarded_live_exchange("sell")
    if skipped:
        return skipped
    _require_okx_production_strategy_gate(
        exchange, bot=bot, symbol=symbol, strategy_id=strategy_id, side="sell"
    )
    amount, market, position = _sell_amount(
        symbol=symbol,
        position_id=position_id,
        bot=bot,
        percentage=percentage,
        convert_all_balance=convert_all_balance,
    )
    if execution_style not in {"market", "okx_demo_limit_ioc"}:
        raise ValueError("Unsupported sell execution style")
    if execution_style == "okx_demo_limit_ioc":
        if str(exchange["code"]) != "okx_demo" or bot != "manual_demo":
            raise RuntimeError("Limit IOC sells are restricted to controlled OKX Demo validation")
        if limit_price is None:
            raise ValueError("An explicit OKX Demo limit price is required")
        limit_price = service.normalize_price(market.symbol, Decimal(str(limit_price)))
        quote = get_okx_demo_manual_sell_quote(symbol=market.symbol)
        best_bid = Decimal(str(quote["best_bid"]))
        if limit_price > best_bid:
            raise RuntimeError(
                "The selected limit price is above the current best bid and cannot "
                "execute immediately. Refresh the suggested price before submitting."
            )
        validation_price = limit_price
    else:
        ticker = service.fetch_ticker(market.symbol)
        validation_price = ticker.bid or ticker.last
    if validation_price is None or Decimal(str(validation_price)) <= 0:
        raise RuntimeError("A current exchange price is unavailable for the sell validation")

    def validate(value: Decimal):
        return service.validate_order(
            OrderRequest(
                symbol=market.symbol,
                side="sell",
                amount=value,
                price=validation_price,
            )
        )

    validation = validate(amount)
    if not validation.valid:
        policy = exchange["partial_sell_policy"]
        if policy == "sell_all" and percentage < 100:
            amount, market, position = _sell_amount(
                symbol=symbol,
                position_id=position_id,
                bot=bot,
                percentage=100,
                convert_all_balance=convert_all_balance,
            )
            validation = validate(amount)
            percentage = 100
        if not validation.valid:
            message = (
                f"Exchange sell skipped by {policy} policy: "
                + "; ".join(validation.errors)
            )
            print(database.exchange_log_prefix(), message)
            return {"skipped": True, "policy": policy, "reason": message}

    limit_ioc_creator = None
    if execution_style == "okx_demo_limit_ioc":
        limit_ioc_creator = getattr(service.get_adapter(), "create_limit_sell_ioc", None)
        if not callable(limit_ioc_creator):
            raise RuntimeError("The active OKX Demo adapter cannot create explicit limit IOC sells")
    intent = database.create_order_intent(
        side="SELL",
        symbol=market.symbol,
        bot=bot,
        position_id=None if convert_all_balance else position_id,
        requested_qty=float(amount),
        strategy_id=strategy_id,
        strategy_params_json=(
            str(position.get("Strategy_Params_JSON") or "")
            if position is not None
            else ""
        ),
        exit_reason=reason,
        sell_percentage=percentage,
        take_profit_num=take_profit_num,
    )
    database.mark_order_intent_submitting(intent["Id"])
    try:
        if limit_ioc_creator is not None:
            result = limit_ioc_creator(
                market.symbol,
                amount,
                validation_price,
                client_order_id=intent["Client_Order_Id"],
            )
        else:
            result = service.create_market_sell(
                market.symbol,
                amount,
                client_order_id=intent["Client_Order_Id"],
            )
    except Exception as exc:
        _record_submission_failure(intent["Id"], "create exchange sell order", exc)
        return None
    applied = database.apply_order_result(intent["Id"], result)
    _post_apply_sell(intent, applied, float(result.average_price or 0))
    _log_order_persistence(exchange, result)
    settled_intent = database.get_order_intent(intent["Id"])
    pnl_value = float(settled_intent.get("PnL_Value") or 0)
    pnl_perc = float(settled_intent.get("PnL_Perc") or 0)
    emoji = (
        telegram.EMOJI_TRADE_WITH_PROFIT
        if pnl_value > 0
        else telegram.EMOJI_TRADE_WITH_LOSS
    )
    _send_trade_notification(
        bot=bot,
        emoji=emoji,
        action="SELL",
        symbol=result.symbol,
        strategy=strategy_name or strategy_id,
        reason=reason or "Exit condition fulfilled",
        unit_price=float(result.average_price or 0),
        quantity=float(result.executed_quantity),
        notional_value=float(result.executed_quantity)
        * float(result.average_price or 0),
        pnl_perc=pnl_perc,
        pnl_value=pnl_value,
        open_positions=str(database.get_num_open_positions()),
        **_order_telemetry(exchange, result),
    )
    return {"result": result, "applied": applied, "exchange": exchange}


def reconcile_unsettled_orders() -> dict:
    exchange = database.get_active_exchange(required=False)
    if not exchange:
        return {"checked": 0, "updated": 0, "unresolved": 0}
    adapter = service.get_adapter()
    if not adapter.capabilities.supports_reconciliation:
        return {"checked": 0, "updated": 0, "unresolved": 0}
    stats = {"checked": 0, "updated": 0, "unresolved": 0}
    for intent in database.get_unsettled_order_intents(int(exchange["id"])):
        stats["checked"] += 1
        try:
            resolver = getattr(adapter, "fetch_order_by_client_id", None)
            result = (
                resolver(intent["Client_Order_Id"], intent["Symbol"])
                if resolver and str(intent.get("Client_Order_Id") or "")
                else None
            )
            if result is None:
                exchange_order_id = str(intent.get("Exchange_Order_Id") or "")
                if exchange_order_id:
                    result = adapter.fetch_order(exchange_order_id, intent["Symbol"])
            if result is None:
                stats["unresolved"] += 1
                continue
            applied = database.apply_order_result(intent["Id"], result)
            if str(intent["Side"]).upper() == "SELL":
                _post_apply_sell(intent, applied, float(result.average_price or 0))
            stats["updated"] += 1
        except Exception as exc:
            database.mark_order_intent_unknown(intent["Id"], repr(exc))
            stats["unresolved"] += 1
    print(
        database.exchange_log_prefix(),
        "Exchange reconciliation "
        f"exchange={exchange.get('name') or exchange.get('code')} "
        f"environment={exchange.get('execution_environment')} "
        f"checked={stats['checked']} updated={stats['updated']} unresolved={stats['unresolved']}",
    )
    return stats


def private_api_status() -> tuple[bool, str]:
    try:
        service.fetch_balance()
    except Exception as exc:
        return False, f"Private API unavailable ({type(exc).__name__})."
    return True, (
        "Private balance access succeeded. Confirm the key has only the minimum "
        "balance/order permissions, no withdrawal permission, and an IP allowlist."
    )
