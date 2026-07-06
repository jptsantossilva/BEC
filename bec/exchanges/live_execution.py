"""Gated Kraken live-order workflow with durable intents and reconciliation."""

from __future__ import annotations

from decimal import Decimal

from bec.exchanges import service
from bec.exchanges.base import OrderRequest
from bec.utils import config, database, telegram


def _live_exchange(side: str) -> dict:
    exchange = database.get_active_exchange(required=True)
    if exchange["code"] != "kraken":
        raise RuntimeError("The CCXT live workflow is only enabled for Kraken")
    settings = config.load_settings(refresh=True)
    if settings.run_mode == "test":
        raise RuntimeError("Kraken live execution is disabled while run_mode=test")
    flag = "buy_enabled" if str(side).lower() == "buy" else "sell_enabled"
    if not bool(exchange[flag]):
        raise RuntimeError(f"Kraken {side} operations are disabled in Trading Settings")
    if not bool(getattr(service.get_adapter(), "private_enabled", False)):
        raise RuntimeError("Kraken private credentials are not configured")
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


def _record_submission_failure(intent_id: int, operation: str, exc: Exception) -> None:
    database.mark_order_intent_unknown(intent_id, repr(exc))
    telegram.send_error_event(
        action=operation,
        reason="Kraken submission outcome is uncertain",
        impact="The intent was not retried and requires reconciliation.",
        next_step="Check the Kraken order and reconciliation logs before any manual action.",
        exception=exc,
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


def create_buy_order(
    *,
    symbol: str,
    bot: str,
    convert_all_balance: bool = False,
    strategy_id: str = "",
    strategy_name: str = "",
    position_id: int | None = None,
    strategy_params_json: str = "",
):
    exchange, skipped = _guarded_live_exchange("buy")
    if skipped:
        return skipped
    market = service.load_markets()[service.normalize_symbol(symbol)]
    if convert_all_balance:
        balance = service.fetch_balance(market.quote_asset)
        quote_amount = Decimal(str(balance.free)) * Decimal(
            str(config.load_settings().tradable_balance_ratio)
        )
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
        _record_submission_failure(intent["Id"], "create Kraken buy order", exc)
        return None
    applied = database.apply_order_result(intent["Id"], result)
    telegram.send_trade_event(
        telegram_token=telegram.get_telegram_token(),
        telegram_prefix=telegram.get_telegram_prefix(bot),
        emoji=telegram.EMOJI_ENTER_TRADE,
        action="BUY",
        symbol=result.symbol,
        timeframe=bot,
        strategy=strategy_name or strategy_id,
        reason="Entry condition fulfilled",
        unit_price=float(result.average_price or 0),
        quantity=float(result.executed_quantity),
        notional_value=float(quote_amount),
        open_positions=str(database.get_num_open_positions()),
    )
    return {"result": result, "applied": applied, "exchange": exchange}


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
):
    exchange, skipped = _guarded_live_exchange("sell")
    if skipped:
        return skipped
    amount, market, position = _sell_amount(
        symbol=symbol,
        position_id=position_id,
        bot=bot,
        percentage=percentage,
        convert_all_balance=convert_all_balance,
    )
    ticker = service.fetch_ticker(market.symbol)

    def validate(value: Decimal):
        return service.validate_order(
            OrderRequest(
                symbol=market.symbol,
                side="sell",
                amount=value,
                price=ticker.bid or ticker.last,
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
                f"Kraken sell skipped by {policy} policy: "
                + "; ".join(validation.errors)
            )
            print(database.exchange_log_prefix(), message)
            return {"skipped": True, "policy": policy, "reason": message}

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
        result = service.create_market_sell(
            market.symbol,
            amount,
            client_order_id=intent["Client_Order_Id"],
        )
    except Exception as exc:
        _record_submission_failure(intent["Id"], "create Kraken sell order", exc)
        return None
    applied = database.apply_order_result(intent["Id"], result)
    _post_apply_sell(intent, applied, float(result.average_price or 0))
    settled_intent = database.get_order_intent(intent["Id"])
    pnl_value = float(settled_intent.get("PnL_Value") or 0)
    pnl_perc = float(settled_intent.get("PnL_Perc") or 0)
    emoji = (
        telegram.EMOJI_TRADE_WITH_PROFIT
        if pnl_value > 0
        else telegram.EMOJI_TRADE_WITH_LOSS
    )
    telegram.send_trade_event(
        telegram_token=telegram.get_telegram_token(),
        telegram_prefix=telegram.get_telegram_prefix(bot),
        emoji=emoji,
        action="SELL",
        symbol=result.symbol,
        timeframe=bot,
        strategy=strategy_name or strategy_id,
        reason=reason or "Exit condition fulfilled",
        unit_price=float(result.average_price or 0),
        quantity=float(result.executed_quantity),
        notional_value=float(result.executed_quantity)
        * float(result.average_price or 0),
        pnl_perc=pnl_perc,
        pnl_value=pnl_value,
        open_positions=str(database.get_num_open_positions()),
    )
    return {"result": result, "applied": applied, "exchange": exchange}


def reconcile_unsettled_orders() -> dict:
    exchange = database.get_active_exchange(required=False)
    if not exchange or exchange["code"] != "kraken":
        return {"checked": 0, "updated": 0, "unresolved": 0}
    adapter = service.get_adapter()
    stats = {"checked": 0, "updated": 0, "unresolved": 0}
    for intent in database.get_unsettled_order_intents(int(exchange["id"])):
        stats["checked"] += 1
        try:
            exchange_order_id = str(intent.get("Exchange_Order_Id") or "")
            if exchange_order_id:
                result = adapter.fetch_order(exchange_order_id, intent["Symbol"])
            else:
                resolver = getattr(adapter, "fetch_order_by_client_id", None)
                result = (
                    resolver(intent["Client_Order_Id"], intent["Symbol"])
                    if resolver
                    else None
                )
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
    return stats


def private_api_status() -> tuple[bool, str]:
    try:
        service.fetch_balance()
    except Exception as exc:
        return False, f"Private API unavailable: {exc}"
    return True, (
        "Private balance access succeeded. Confirm the key has Query Funds and "
        "Create/Modify Orders only, no Withdraw Funds permission, and an IP allowlist."
    )
