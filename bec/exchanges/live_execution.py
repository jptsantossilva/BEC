"""Gated spot live-order workflow with durable intents and reconciliation."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal

from bec.exchanges import service
from bec.exchanges.base import (
    OrderRequest,
    OrderStatus,
    SellOrderOutcome,
    SellOutcomeStatus,
    SubmissionState,
)
from bec.exchanges.sell_sizing import (
    SellSizingDecision,
    SellSizingStatus,
    format_position_close_without_order,
    prepare_sell_quantity,
)
from bec.utils import config, database, telegram


KRAKEN_MANUAL_TEST_SYMBOL = "BTC/USDC"
KRAKEN_MANUAL_TEST_QUOTE_AMOUNT = Decimal("50")
KRAKEN_MANUAL_TEST_CONFIRMATION = "BUY 50 USDC BTC/USDC LIVE"
KRAKEN_IMMEDIATE_CONFIRMATION_DELAYS = (1, 2, 4)
TERMINAL_ORDER_STATUSES = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
}
_UNRESOLVED_AMOUNT_STEP = object()


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
    if adapter_code == "okx" and settings.run_mode != "live":
        raise RuntimeError("OKX production execution requires run_mode=live")
    if settings.run_mode == "demo":
        raise RuntimeError("run_mode=demo is retired and does not permit live execution")
    if adapter_code != "okx" and settings.run_mode == "test":
        raise RuntimeError(f"{name} live execution is disabled while run_mode=test")
    flag = "buy_enabled" if str(side).lower() == "buy" else "sell_enabled"
    if not bool(exchange[flag]):
        raise RuntimeError(f"{name} {side} operations are disabled in Trading Settings")
    if not bool(getattr(adapter, "private_enabled", False)):
        raise RuntimeError(f"{name} private credentials are not configured")
    if adapter_code == "kraken":
        try:
            service.fetch_balance(str(exchange.get("quote_asset") or ""))
            service.check_reconciliation_access()
        except Exception as exc:
            raise RuntimeError(
                "Kraken private preflight failed; the API key must allow Query Funds, "
                "Create/Modify Orders, Query Open Orders & Trades, and Query Closed "
                "Orders & Trades"
            ) from exc
    if adapter_code == "okx":
        try:
            service.fetch_balance(str(exchange.get("quote_asset") or ""))
            service.check_reconciliation_access()
        except Exception as exc:
            raise RuntimeError(
                "OKX production private preflight failed; run the read-only check "
                "and correct credentials, balance/order-history permissions, IP "
                "allowlist, or region"
            ) from exc
        print(
            database.exchange_log_prefix(),
            "OKX production private preflight succeeded "
            "for balance and order-history access "
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
    usage = database.get_position_capacity_usage()
    if usage["occupied_slots"] >= settings.max_number_of_open_positions:
        raise database.PositionCapacityError(
            open_positions=usage["open_positions"],
            pending_buy_slots=usage["pending_buy_slots"],
            max_positions=settings.max_number_of_open_positions,
        )
    balance = service.fetch_balance(quote_asset)
    available = Decimal(str(balance.free))
    if settings.stake_amount_type == "unlimited":
        tradable = available * Decimal(str(settings.tradable_balance_ratio))
        if settings.lock_values:
            tradable -= Decimal(str(database.get_total_locked_values()))
        remaining_slots = (
            settings.max_number_of_open_positions - usage["occupied_slots"]
        )
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
    if adapter_code == "okx":
        return RuntimeError(f"{type(exc).__name__} (OKX request details withheld)")
    return exc


def _regional_trading_restriction(error: Exception | str) -> dict[str, str] | None:
    try:
        parser = getattr(service.get_adapter(), "regional_trading_restriction", None)
        return parser(error) if callable(parser) else None
    except Exception:
        # Error classification must never hide the original submission failure.
        return None


def _blacklist_restricted_asset(asset: str) -> bool:
    try:
        database.ensure_symbol_blacklisted(asset)
        return True
    except Exception as exc:
        print(
            database.exchange_log_prefix(),
            f"Failed to add restricted asset {asset} to blacklist "
            f"({type(exc).__name__})",
        )
        return False


def _record_submission_failure(
    intent_id: int,
    operation: str,
    exc: Exception,
    *,
    symbol: str = "",
    timeframe: str = "",
    strategy: str = "",
) -> None:
    reported_exception = _safe_exception_for_operator_reporting(exc)
    restriction = _regional_trading_restriction(exc)
    if restriction is not None:
        database.mark_order_intent_rejected(intent_id, repr(exc))
        asset = restriction["asset"]
        region = restriction["region"]
        blacklisted = _blacklist_restricted_asset(asset)
        blacklist_impact = (
            f"{asset} was added to the BEC blacklist to prevent new automatic entries."
            if blacklisted
            else f"BEC could not add {asset} to the blacklist automatically; add it manually."
        )
        telegram.send_error_event(
            action=operation,
            symbol=symbol,
            timeframe=timeframe,
            strategy=strategy,
            reason=(
                f"The exchange rejected {asset} trading for region {region}"
            ),
            impact=(
                f"No order was submitted and intent {intent_id} was marked rejected. "
                + blacklist_impact
            ),
            next_step=(
                "No reconciliation or replacement order is required. Review any existing "
                f"{asset} position separately."
            ),
            exception=reported_exception,
            notify_main=False,
        )
        return
    if _is_known_submission_rejection(exc):
        database.mark_order_intent_rejected(intent_id, repr(exc))
        telegram.send_error_event(
            action=operation,
            symbol=symbol,
            timeframe=timeframe,
            strategy=strategy,
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
        symbol=symbol,
        timeframe=timeframe,
        strategy=strategy,
        reason="Exchange submission outcome is uncertain",
        impact="The intent was not retried and requires reconciliation.",
        next_step="Check the exchange order and reconciliation logs before any manual action.",
        exception=reported_exception,
        notify_main=False,
    )


def _post_apply_sell(intent: dict, applied: dict, average_price: float) -> None:
    if intent.get("Position_Id") is None:
        return
    position_id = int(intent["Position_Id"])
    if applied["closed_position"]:
        database.release_value(position_id)
        return
    if applied["delta_executed_qty"] <= 0:
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


def _buy_opened_position(intent: dict, applied: dict) -> bool:
    """Return true only for the first persisted fill that opens a position."""
    return (
        str(intent.get("Side") or "").upper() == "BUY"
        and intent.get("Position_Id") is not None
        and float(intent.get("Applied_Net_Qty") or 0) <= 0
        and float(applied.get("delta_net_qty") or 0) > 0
    )


def _parse_utc_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc_datetime(value) -> str:
    parsed = _parse_utc_datetime(value)
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z") if parsed else ""


def _raw_order_timestamp(result, field: str) -> datetime | None:
    raw = result.raw or {}
    milliseconds = raw.get(field)
    if milliseconds in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(milliseconds) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _order_telemetry(
    exchange: dict,
    result,
    *,
    intent: dict | None = None,
    confirmation_source: str = "",
) -> dict[str, str]:
    fees: dict[str, Decimal] = {}
    for fill in result.fills:
        asset = str(fill.fee_asset or "").upper()
        if asset:
            fees[asset] = fees.get(asset, Decimal("0")) + Decimal(str(fill.fee_amount))
    fee_text = ", ".join(
        f"{asset} {amount}" for asset, amount in sorted(fees.items())
    )
    exact_fill_times = [
        fill.timestamp
        for fill in result.fills
        if fill.trade_id and fill.timestamp is not None
    ]
    executed_at = (
        min(exact_fill_times)
        if exact_fill_times
        else _raw_order_timestamp(result, "lastTradeTimestamp")
    )
    closed_at = (
        None
        if executed_at
        else _raw_order_timestamp(result, "lastUpdateTimestamp")
    )
    submitted_at = _parse_utc_datetime((intent or {}).get("Date"))
    confirmed_at = _parse_utc_datetime((intent or {}).get("Last_Reconciled_At"))
    delay = ""
    if submitted_at is not None and confirmed_at is not None:
        delay_seconds = max(0, int((confirmed_at - submitted_at).total_seconds()))
        delay = f"{delay_seconds}s"
    return {
        "exchange": str(exchange.get("name") or exchange.get("code") or ""),
        "environment": str(exchange.get("execution_environment") or ""),
        "client_order_id": str(
            result.client_order_id or (intent or {}).get("Client_Order_Id") or ""
        ),
        "order_status": str(result.status.value),
        "fees": fee_text,
        "submitted_at": _format_utc_datetime(submitted_at),
        "executed_at": _format_utc_datetime(executed_at),
        "order_closed_at": _format_utc_datetime(closed_at),
        "confirmed_at": _format_utc_datetime(confirmed_at),
        "confirmation_delay": delay,
        "confirmation_source": confirmation_source,
    }


def _log_order_persistence(
    exchange: dict, result, *, intent: dict | None = None, confirmation_source: str = ""
) -> None:
    telemetry = _order_telemetry(
        exchange,
        result,
        intent=intent,
        confirmation_source=confirmation_source,
    )
    print(
        database.exchange_log_prefix(),
        "Order persisted "
        f"exchange={telemetry['exchange']} environment={telemetry['environment']} "
        f"client_order_id={telemetry['client_order_id']} status={telemetry['order_status']} "
        f"fees={telemetry['fees'] or 'none'} "
        f"confirmation_source={telemetry['confirmation_source'] or 'submission response'}",
    )


def _create_order_intent_or_skip(**kwargs) -> tuple[dict | None, dict | None]:
    try:
        return database.create_order_intent(**kwargs), None
    except database.PositionCapacityError as exc:
        message = str(exc)
        print(database.exchange_log_prefix(), f"Order skipped: {message}")
        return None, {
            "skipped": True,
            "code": "position_capacity_full",
            "reason": message,
            "capacity": exc.capacity,
        }
    except database.UnsettledOrderIntentError as exc:
        message = str(exc)
        print(database.exchange_log_prefix(), f"Order skipped: {message}")
        return None, {
            "skipped": True,
            "reason": message,
            "reconciliation_required": True,
        }


def _confirm_kraken_submission(intent: dict, acknowledgement):
    """Poll a Kraken txid with read-only requests and never resubmit it."""
    database.record_order_acknowledgement(intent["Id"], acknowledgement)
    results = []
    for delay in KRAKEN_IMMEDIATE_CONFIRMATION_DELAYS:
        time.sleep(delay)
        try:
            result = service.fetch_order(
                acknowledgement.exchange_order_id,
                str(intent["Symbol"]),
            )
        except Exception as exc:
            print(
                database.exchange_log_prefix(),
                "Immediate Kraken order confirmation deferred "
                f"intent={intent['Id']} ({type(exc).__name__})",
            )
            break
        results.append(result)
        if result.status in TERMINAL_ORDER_STATUSES:
            break
    return results


def _apply_order_result_and_notify(
    *,
    intent: dict,
    result,
    exchange: dict,
    confirmation_source: str,
    strategy_name: str = "",
    reason: str = "",
    amount_step=_UNRESOLVED_AMOUNT_STEP,
) -> dict:
    if amount_step is _UNRESOLVED_AMOUNT_STEP:
        amount_step = None
        if str(intent.get("Side") or "").upper() == "SELL":
            try:
                market = service.load_markets()[service.normalize_symbol(result.symbol)]
                amount_step = market.amount_step
            except Exception:
                # Fill persistence must never fail only because public market
                # metadata is temporarily unavailable. A later full-exit
                # attempt can still close the dust.
                amount_step = None
    applied = (
        database.apply_order_result(intent["Id"], result, amount_step=amount_step)
        if amount_step is not None
        else database.apply_order_result(intent["Id"], result)
    )
    if str(intent.get("Side") or "").upper() == "SELL":
        _post_apply_sell(intent, applied, float(result.average_price or 0))
    persisted_intent = database.get_order_intent(intent["Id"])
    _log_order_persistence(
        exchange,
        result,
        intent=persisted_intent,
        confirmation_source=confirmation_source,
    )
    first_fill = (
        float(intent.get("Applied_Executed_Qty") or 0) <= 0
        and float(applied.get("delta_executed_qty") or 0) > 0
    )
    if not first_fill:
        if applied.get("dust_closed"):
            # The real fill was already notified when it first appeared. Keep
            # terminal reconciliation idempotent and avoid a second Telegram
            # message for the same exchange execution.
            print(
                database.exchange_log_prefix(),
                result.symbol,
                "Position closed during reconciliation with non-tradable "
                f"residual {applied.get('dust_quantity')} "
                f"(amount step {amount_step})",
            )
        return applied

    side = str(intent.get("Side") or result.side).upper()
    average = float(result.average_price or 0)
    quantity = float(result.executed_quantity)
    telemetry = _order_telemetry(
        exchange,
        result,
        intent=persisted_intent,
        confirmation_source=confirmation_source,
    )
    event = {
        "bot": str(intent.get("Bot") or ""),
        "action": side,
        "symbol": result.symbol,
        "strategy": strategy_name or str(intent.get("Strategy_Id") or ""),
        "reason": reason,
        "unit_price": average,
        "quantity": quantity,
        "notional_value": float(
            intent.get("Requested_Quote_Qty") or (quantity * average)
        ),
        "open_positions": str(database.get_num_open_positions()),
        **telemetry,
    }
    if side == "BUY":
        event.update(
            emoji=telegram.EMOJI_ENTER_TRADE,
            position_opened=_buy_opened_position(intent, applied),
        )
    else:
        pnl_value = float(persisted_intent.get("PnL_Value") or 0)
        event_reason = reason
        if applied.get("dust_closed"):
            event_reason = (
                f"{reason} | Position closed with non-tradable residual "
                f"{applied.get('dust_quantity')} (amount step {amount_step})"
            ).strip(" |")
        event.update(
            emoji=(
                telegram.EMOJI_TRADE_WITH_PROFIT
                if pnl_value > 0
                else telegram.EMOJI_TRADE_WITH_LOSS
            ),
            pnl_perc=float(persisted_intent.get("PnL_Perc") or 0),
            pnl_value=pnl_value,
            reason=event_reason,
        )
    _send_trade_notification(**event)
    return applied


def _persist_submitted_order(
    *,
    intent: dict,
    acknowledgement,
    exchange: dict,
    strategy_name: str,
    reason: str,
    amount_step=_UNRESOLVED_AMOUNT_STEP,
):
    result = acknowledgement
    confirmation_source = (
        "immediate order query"
        if str(exchange.get("code") or "") == "kraken"
        else "submission response"
    )
    if (
        str(exchange.get("code") or "") == "kraken"
        and result.status not in TERMINAL_ORDER_STATUSES
    ):
        if not str(result.exchange_order_id or ""):
            error = RuntimeError("Kraken accepted response did not include an order id")
            _record_submission_failure(
                intent["Id"],
                f"confirm exchange {str(intent.get('Side') or '').lower()} order",
                error,
                symbol=str(intent.get("Symbol") or ""),
                timeframe=str(intent.get("Bot") or ""),
                strategy=strategy_name or str(intent.get("Strategy_Id") or ""),
            )
            return None
        confirmations = _confirm_kraken_submission(intent, result)
        if not confirmations:
            return {
                "result": result,
                "applied": None,
                "exchange": exchange,
                "confirmation_deferred": True,
            }
        confirmation_source = "immediate order query"
        applied = None
        for result in confirmations:
            current_intent = database.get_order_intent(intent["Id"])
            applied = _apply_order_result_and_notify(
                intent=current_intent,
                result=result,
                exchange=exchange,
                confirmation_source=confirmation_source,
                strategy_name=strategy_name,
                reason=reason,
                amount_step=amount_step,
            )
        return {
            "result": result,
            "applied": applied,
            "exchange": exchange,
            "confirmation_deferred": result.status not in TERMINAL_ORDER_STATUSES,
        }
    applied = _apply_order_result_and_notify(
        intent=intent,
        result=result,
        exchange=exchange,
        confirmation_source=confirmation_source,
        strategy_name=strategy_name,
        reason=reason,
        amount_step=amount_step,
    )
    return {"result": result, "applied": applied, "exchange": exchange}


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
    market = service.load_markets()[service.normalize_symbol(symbol)]
    if database.is_symbol_blacklisted(market.base_asset):
        message = (
            f"Buy skipped because {market.base_asset} is present in the BEC blacklist"
        )
        print(database.exchange_log_prefix(), f"{market.symbol}: {message}")
        return {"skipped": True, "reason": message}
    settings = config.load_settings()
    if quote_amount is not None and convert_all_balance:
        raise ValueError("quote_amount and convert_all_balance are mutually exclusive")
    if quote_amount is not None:
        quote_amount = Decimal(str(quote_amount))
        if quote_amount <= 0:
            raise ValueError("Manual quote amount must be greater than zero")
        balance = service.fetch_balance(market.quote_asset)
        available = Decimal(str(balance.free))
        if quote_amount > available:
            raise RuntimeError(
                f"Manual quote amount exceeds available {market.quote_asset}"
            )
    elif convert_all_balance:
        balance = service.fetch_balance(market.quote_asset)
        quote_amount = Decimal(str(balance.free)) * Decimal(
            str(settings.tradable_balance_ratio)
        )
    else:
        try:
            quote_amount = _stake_amount(market.quote_asset)
        except database.PositionCapacityError as exc:
            message = str(exc)
            print(database.exchange_log_prefix(), f"{market.symbol}: {message}")
            return {
                "skipped": True,
                "code": "position_capacity_full",
                "reason": message,
                "capacity": exc.capacity,
            }
    validation = service.validate_order(
        OrderRequest(symbol=market.symbol, side="buy", quote_amount=quote_amount)
    )
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))

    intent, duplicate = _create_order_intent_or_skip(
        side="BUY",
        symbol=market.symbol,
        bot=bot,
        position_id=None if convert_all_balance else position_id,
        requested_quote_qty=float(quote_amount),
        strategy_id=strategy_id,
        strategy_params_json=strategy_params_json,
        max_open_positions=settings.max_number_of_open_positions,
    )
    if duplicate:
        return duplicate
    database.mark_order_intent_submitting(intent["Id"])
    try:
        result = service.create_market_buy(
            market.symbol,
            quote_amount=quote_amount,
            client_order_id=intent["Client_Order_Id"],
        )
    except Exception as exc:
        _record_submission_failure(
            intent["Id"],
            "create exchange buy order",
            exc,
            symbol=market.symbol,
            timeframe=bot,
            strategy=strategy_name or strategy_id,
        )
        return None
    return _persist_submitted_order(
        intent=intent,
        acknowledgement=result,
        exchange=exchange,
        strategy_name=strategy_name or strategy_id,
        reason="Entry condition fulfilled",
    )


def get_kraken_manual_test_blockers() -> tuple[str, ...]:
    """Return every local gate that must be cleared before the live test."""
    blockers: list[str] = []
    exchange = database.get_active_exchange(required=False)
    if exchange is None:
        return ("Select Kraken as the active exchange.",)
    if str(exchange.get("code")) != "kraken" or str(
        exchange.get("execution_environment")
    ) != "production":
        blockers.append("The active exchange must be Kraken production.")
    if str(exchange.get("quote_asset") or "").upper() != "USDC":
        blockers.append("Kraken must use USDC as its active quote asset.")
    if not bool(exchange.get("buy_enabled")):
        blockers.append("Kraken Buy Enabled must be on.")
    if config.read_setting("run_mode") != "live":
        blockers.append("Live run mode must be enabled.")
    enabled_jobs = [
        job
        for job in database.LIVE_TRADING_JOBS
        if database.get_job_schedule_enabled(job)
    ]
    if enabled_jobs:
        blockers.append(
            "Disable every automatic trade schedule: " + ", ".join(enabled_jobs) + "."
        )
    if not database.get_job_schedule_enabled(database.RECONCILIATION_JOB):
        blockers.append("Enable the order reconciliation schedule.")
    if database.get_num_open_positions():
        blockers.append("Close existing Kraken positions before the controlled test.")
    if database.get_unsettled_order_intents(int(exchange["id"])):
        blockers.append("Reconcile existing unsettled orders before the controlled test.")
    return tuple(blockers)


def create_kraken_manual_test_buy(*, confirmation: str):
    """Submit the single fixed-size Kraken live validation buy and disarm live mode."""
    try:
        if str(confirmation).strip() != KRAKEN_MANUAL_TEST_CONFIRMATION:
            raise ValueError(
                f"Type {KRAKEN_MANUAL_TEST_CONFIRMATION} exactly to confirm the live order"
            )
        blockers = get_kraken_manual_test_blockers()
        if blockers:
            raise RuntimeError(
                "Kraken manual live test is blocked: " + " ".join(blockers)
            )
        exchange = _live_exchange("buy")
        if str(exchange["code"]) != "kraken" or str(
            exchange.get("execution_environment")
        ) != "production":
            raise RuntimeError(
                "Manual Kraken live orders require the active Kraken production identity"
            )
        canonical = service.normalize_symbol(KRAKEN_MANUAL_TEST_SYMBOL)
        if canonical != KRAKEN_MANUAL_TEST_SYMBOL:
            raise RuntimeError("The Kraken manual live test requires BTC/USDC")
        market = service.load_markets()[canonical]
        if str(market.quote_asset).upper() != "USDC":
            raise RuntimeError("The Kraken manual live test requires a USDC quote market")
        balance = service.fetch_balance("USDC")
        if KRAKEN_MANUAL_TEST_QUOTE_AMOUNT > Decimal(str(balance.free)):
            raise RuntimeError("The 50 USDC test amount exceeds the available Kraken balance")
        validation = service.validate_order(
            OrderRequest(
                symbol=canonical,
                side="buy",
                quote_amount=KRAKEN_MANUAL_TEST_QUOTE_AMOUNT,
            )
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        position_id = database.create_kraken_manual_position_candidate(canonical)
        return create_buy_order(
            symbol=canonical,
            bot="manual_live",
            strategy_id="kraken_manual_live",
            strategy_name="Kraken Manual Live Validation",
            position_id=position_id,
            strategy_params_json="{}",
            quote_amount=KRAKEN_MANUAL_TEST_QUOTE_AMOUNT,
        )
    finally:
        # The one-shot workflow always closes the master execution gate after
        # any invocation, regardless of validation or exchange response.
        config.update_setting("run_mode", "test")


def _sell_inputs(
    *, symbol: str, position_id: int | None, bot: str,
    convert_all_balance: bool,
) -> tuple[object, object, object]:
    market = service.load_markets()[service.normalize_symbol(symbol)]
    balance = service.fetch_balance(market.base_asset)
    position = None
    if not convert_all_balance:
        df = (
            database.get_position_by_id(position_id)
            if position_id is not None
            else database.get_positions_by_bot_symbol_position(bot, symbol, 1)
        )
        if df.empty:
            raise RuntimeError("Open position not found")
        position = df.iloc[0]
    return market, balance, position


def _close_without_exchange_order(
    *, exchange: dict, market, position, bot: str, decision: SellSizingDecision,
) -> SellOrderOutcome:
    closure_reason = (
        "dust" if decision.status == SellSizingStatus.DUST else "zero_balance"
    )
    closed = database.close_position_without_order(
        position_id=int(position["Id"]),
        expected_qty=decision.position_quantity,
        closure_reason=closure_reason,
        amount_step=decision.amount_step,
    )
    if not closed["closed"]:
        message = f"Position close skipped: {closed['reason']}"
        print(database.exchange_log_prefix(), market.symbol, message)
        return SellOrderOutcome(
            status=SellOutcomeStatus.SKIPPED,
            reason=message,
            reconciliation_required=closed["reason"] == "unsettled_sell_intent",
            requested_quantity=decision.requested_quantity,
            residual_quantity=decision.position_quantity,
            effective_percentage=decision.effective_percentage,
            details={"policy": exchange["partial_sell_policy"]},
        )
    message = format_position_close_without_order(
        exchange=str(exchange.get("name") or exchange.get("code") or ""),
        symbol=market.symbol,
        timeframe=bot,
        decision=decision,
        closure_reason=closure_reason,
    )
    print(database.exchange_log_prefix(), message.replace("\n", " | "))
    try:
        telegram.send_telegram_message(
            telegram.get_telegram_token(), telegram.EMOJI_INFORMATION, message
        )
    except Exception as exc:
        print(
            database.exchange_log_prefix(),
            f"Position closure notification failed ({type(exc).__name__})",
        )
    return SellOrderOutcome(
        status=SellOutcomeStatus.CLOSED_WITHOUT_ORDER,
        reason=message,
        position_closed=True,
        closure_reason=closure_reason,
        requested_quantity=decision.requested_quantity,
        residual_quantity=decision.position_quantity,
        effective_percentage=decision.effective_percentage,
        details={"policy": exchange["partial_sell_policy"]},
    )


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
):
    exchange, skipped = _guarded_live_exchange("sell")
    if skipped:
        return SellOrderOutcome(
            status=SellOutcomeStatus.SKIPPED,
            reason=str(skipped["reason"]),
            effective_percentage=Decimal(str(percentage)),
        )
    market, balance, position = _sell_inputs(
        symbol=symbol,
        position_id=position_id,
        bot=bot,
        convert_all_balance=convert_all_balance,
    )
    ticker = service.fetch_ticker(market.symbol)
    validation_price = ticker.bid or ticker.last
    if validation_price is None or Decimal(str(validation_price)) <= 0:
        raise RuntimeError("A current exchange price is unavailable for the sell validation")

    decision = prepare_sell_quantity(
        market=market,
        balance=balance,
        position_quantity=(position["Qty"] if position is not None else 0),
        percentage=percentage,
        partial_sell_policy=exchange["partial_sell_policy"],
        validation_price=validation_price,
        normalize_amount=service.normalize_amount,
        validate_order=service.validate_order,
        convert_all_balance=convert_all_balance,
    )
    if decision.status in {SellSizingStatus.DUST, SellSizingStatus.ZERO_BALANCE}:
        if position is None:
            return SellOrderOutcome(
                status=SellOutcomeStatus.SKIPPED,
                reason="No position is available for a logical close",
                effective_percentage=decision.effective_percentage,
            )
        return _close_without_exchange_order(
            exchange=exchange,
            market=market,
            position=position,
            bot=bot,
            decision=decision,
        )
    if not decision.ready:
        message = (
            f"Exchange sell skipped by {exchange['partial_sell_policy']} policy: "
            + "; ".join(decision.errors or (decision.status.value,))
        )
        print(database.exchange_log_prefix(), message)
        if decision.status in {
            SellSizingStatus.LOCKED_BALANCE,
            SellSizingStatus.BALANCE_MISMATCH,
        }:
            telegram.send_error_event(
                action="prepare exchange sell order",
                symbol=market.symbol,
                timeframe=bot,
                strategy=strategy_name or strategy_id,
                reason="Exchange balance cannot satisfy the position sell",
                impact="No order was submitted and the position remains open.",
                next_step="Review exchange open orders, locked balance, and BEC reconciliation.",
                main_token=telegram.get_telegram_token(),
                main_prefix=telegram.get_telegram_prefix(bot),
            )
        return SellOrderOutcome(
            status=SellOutcomeStatus.SKIPPED,
            reason=message,
            requested_quantity=decision.requested_quantity,
            residual_quantity=decision.position_quantity,
            effective_percentage=decision.effective_percentage,
            details={"policy": exchange["partial_sell_policy"]},
        )

    amount = decision.normalized_quantity
    percentage = float(decision.effective_percentage)

    intent, duplicate = _create_order_intent_or_skip(
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
    if duplicate:
        return SellOrderOutcome(
            status=SellOutcomeStatus.SKIPPED,
            reason=str(duplicate["reason"]),
            reconciliation_required=True,
            requested_quantity=decision.requested_quantity,
            submitted_quantity=amount,
            residual_quantity=decision.position_quantity,
            effective_percentage=decision.effective_percentage,
        )
    database.mark_order_intent_submitting(intent["Id"])
    try:
        result = service.create_market_sell(
            market.symbol,
            amount,
            client_order_id=intent["Client_Order_Id"],
        )
    except Exception as exc:
        _record_submission_failure(
            intent["Id"],
            "create exchange sell order",
            exc,
            symbol=market.symbol,
            timeframe=bot,
            strategy=strategy_name or strategy_id,
        )
        persisted_intent = database.get_order_intent(intent["Id"])
        unknown = str(persisted_intent.get("Order_Status") or "") == "unknown"
        return SellOrderOutcome(
            status=SellOutcomeStatus.FAILED,
            submission_state=(
                SubmissionState.UNKNOWN if unknown else SubmissionState.NOT_SUBMITTED
            ),
            reason=str(persisted_intent.get("Error_Message") or repr(exc)),
            reconciliation_required=unknown,
            requested_quantity=decision.requested_quantity,
            submitted_quantity=amount,
            residual_quantity=decision.position_quantity,
            effective_percentage=decision.effective_percentage,
        )
    persisted = _persist_submitted_order(
        intent=intent,
        acknowledgement=result,
        exchange=exchange,
        strategy_name=strategy_name or strategy_id,
        reason=reason or "Exit condition fulfilled",
        amount_step=decision.amount_step,
    )
    if persisted is None:
        latest = database.get_order_intent(intent["Id"])
        unknown = str(latest.get("Order_Status") or "") == "unknown"
        return SellOrderOutcome(
            status=SellOutcomeStatus.FAILED,
            submission_state=(
                SubmissionState.UNKNOWN if unknown else SubmissionState.NOT_SUBMITTED
            ),
            reason=str(latest.get("Error_Message") or "Order persistence failed"),
            reconciliation_required=unknown,
            requested_quantity=decision.requested_quantity,
            submitted_quantity=amount,
            residual_quantity=decision.position_quantity,
            effective_percentage=decision.effective_percentage,
        )
    applied = persisted.get("applied") or {}
    residual = applied.get("dust_quantity") or applied.get("remaining_qty")
    return SellOrderOutcome(
        status=SellOutcomeStatus.SUBMITTED,
        submission_state=SubmissionState.SUBMITTED,
        reason=reason or "Exit condition fulfilled",
        position_closed=bool(applied.get("closed_position")),
        reconciliation_required=bool(persisted.get("confirmation_deferred")),
        closure_reason="dust" if applied.get("dust_closed") else "",
        requested_quantity=decision.requested_quantity,
        submitted_quantity=amount,
        residual_quantity=Decimal(str(residual or 0)),
        effective_percentage=decision.effective_percentage,
        order_result=persisted.get("result"),
        details={
            "applied": applied,
            "exchange": exchange,
            "confirmation_deferred": bool(persisted.get("confirmation_deferred")),
            "policy": exchange["partial_sell_policy"],
        },
    )


def reconcile_unsettled_orders() -> dict:
    exchange = database.get_active_exchange(required=False)
    if not exchange:
        return {"checked": 0, "updated": 0, "unresolved": 0}
    intents = database.get_unsettled_order_intents(int(exchange["id"]))
    if not intents:
        return {"checked": 0, "updated": 0, "unresolved": 0}
    adapter = service.get_adapter()
    if not adapter.capabilities.supports_reconciliation:
        return {"checked": 0, "updated": 0, "unresolved": 0}
    stats = {"checked": 0, "updated": 0, "unresolved": 0}
    for intent in intents:
        stats["checked"] += 1
        try:
            stored_rejection = getattr(
                adapter, "is_known_persisted_submission_rejection", None
            )
            if callable(stored_rejection) and stored_rejection(
                str(intent.get("Error_Message") or "")
            ):
                restriction = _regional_trading_restriction(
                    str(intent.get("Error_Message") or "")
                )
                if restriction is not None:
                    _blacklist_restricted_asset(restriction["asset"])
                database.mark_order_intent_rejected(
                    intent["Id"], str(intent.get("Error_Message") or "")
                )
                stats["updated"] += 1
                continue
            exchange_order_id = str(intent.get("Exchange_Order_Id") or "")
            result = None
            if exchange_order_id:
                try:
                    result = adapter.fetch_order(exchange_order_id, intent["Symbol"])
                except Exception:
                    # Retain the existing client-id search only as a read-only
                    # fallback when Kraken cannot resolve the precise txid.
                    result = None
            if result is None:
                resolver = getattr(adapter, "fetch_order_by_client_id", None)
                result = (
                    resolver(intent["Client_Order_Id"], intent["Symbol"])
                    if resolver and str(intent.get("Client_Order_Id") or "")
                    else None
                )
            if result is None:
                stats["unresolved"] += 1
                continue
            side = str(intent.get("Side") or "").upper()
            _apply_order_result_and_notify(
                intent=intent,
                result=result,
                exchange=exchange,
                confirmation_source="scheduled reconciliation",
                strategy_name=str(intent.get("Strategy_Id") or ""),
                reason=(
                    "Entry confirmed during scheduled reconciliation"
                    if side == "BUY"
                    else str(intent.get("Exit_Reason") or "")
                    or "Exit confirmed during scheduled reconciliation"
                ),
            )
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
        exchange = database.get_active_exchange(required=False) or {}
        if str(exchange.get("code") or "") == "kraken":
            service.check_reconciliation_access()
    except Exception as exc:
        return False, f"Private API unavailable ({type(exc).__name__})."
    return True, (
        "Private read-only preflight succeeded. Kraken keys require Query Funds, "
        "Create/Modify Orders, Query Open Orders & Trades, and Query Closed Orders "
        "& Trades. Keep withdrawal permission disabled and use an IP allowlist."
    )
