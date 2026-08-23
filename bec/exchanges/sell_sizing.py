"""Pure, exchange-neutral sizing decisions for spot position sells."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Callable

from bec.exchanges.base import Balance, MarketInfo, OrderRequest, OrderValidation


class SellSizingStatus(str, Enum):
    READY = "ready"
    DUST = "dust"
    ZERO_BALANCE = "zero_balance"
    LOCKED_BALANCE = "locked_balance"
    BALANCE_MISMATCH = "balance_mismatch"
    INVALID = "invalid"


@dataclass(frozen=True)
class SellSizingDecision:
    status: SellSizingStatus
    requested_quantity: Decimal = Decimal("0")
    normalized_quantity: Decimal = Decimal("0")
    position_quantity: Decimal = Decimal("0")
    free_quantity: Decimal = Decimal("0")
    locked_quantity: Decimal = Decimal("0")
    effective_percentage: Decimal = Decimal("100")
    amount_step: Decimal | None = None
    errors: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        return self.status == SellSizingStatus.READY


def _decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def prepare_sell_quantity(
    *,
    market: MarketInfo,
    balance: Balance,
    position_quantity,
    percentage,
    partial_sell_policy: str,
    validation_price,
    normalize_amount: Callable[[str, Decimal], Decimal],
    validate_order: Callable[[OrderRequest], OrderValidation],
    convert_all_balance: bool = False,
) -> SellSizingDecision:
    """Classify a sell before any durable intent or private order submission."""
    position_qty = Decimal("0") if convert_all_balance else _decimal(position_quantity)
    free_qty = _decimal(balance.free)
    locked_qty = _decimal(balance.locked)
    pct = _decimal(percentage)
    if pct <= 0 or pct > 100:
        return SellSizingDecision(
            SellSizingStatus.INVALID,
            position_quantity=position_qty,
            free_quantity=free_qty,
            locked_quantity=locked_qty,
            effective_percentage=pct,
            amount_step=market.amount_step,
            errors=("sell percentage must be greater than zero and at most 100",),
        )

    full_exit = pct >= 100
    if not convert_all_balance and position_qty <= 0:
        return SellSizingDecision(
            SellSizingStatus.INVALID,
            position_quantity=position_qty,
            free_quantity=free_qty,
            locked_quantity=locked_qty,
            effective_percentage=pct,
            amount_step=market.amount_step,
            errors=("open position quantity must be greater than zero",),
        )

    if not convert_all_balance and free_qty <= 0 < locked_qty:
        return SellSizingDecision(
            SellSizingStatus.LOCKED_BALANCE,
            position_quantity=position_qty,
            free_quantity=free_qty,
            locked_quantity=locked_qty,
            effective_percentage=pct,
            amount_step=market.amount_step,
            errors=("the exchange balance is locked and unavailable for selling",),
        )

    if full_exit and not convert_all_balance:
        if free_qty + locked_qty <= 0:
            return SellSizingDecision(
                SellSizingStatus.ZERO_BALANCE,
                position_quantity=position_qty,
                free_quantity=free_qty,
                locked_quantity=locked_qty,
                effective_percentage=pct,
                amount_step=market.amount_step,
            )
        if (
            market.amount_step is not None
            and market.amount_step > 0
            and position_qty < market.amount_step
        ):
            return SellSizingDecision(
                SellSizingStatus.DUST,
                requested_quantity=position_qty,
                position_quantity=position_qty,
                free_quantity=free_qty,
                locked_quantity=locked_qty,
                effective_percentage=pct,
                amount_step=market.amount_step,
            )

    available_position = free_qty if convert_all_balance else min(free_qty, position_qty)
    requested = available_position * pct / Decimal("100")
    if (
        not convert_all_balance
        and market.amount_step is not None
        and market.amount_step > 0
        and Decimal("0") < free_qty < market.amount_step <= position_qty
    ):
        return SellSizingDecision(
            SellSizingStatus.BALANCE_MISMATCH,
            requested_quantity=requested,
            position_quantity=position_qty,
            free_quantity=free_qty,
            locked_quantity=locked_qty,
            effective_percentage=pct,
            amount_step=market.amount_step,
            errors=("available balance is below the amount step while the position is not dust",),
        )

    normalized = normalize_amount(market.symbol, requested) if requested > 0 else Decimal("0")
    validation = validate_order(
        OrderRequest(
            symbol=market.symbol,
            side="sell",
            amount=normalized,
            price=_decimal(validation_price),
        )
    )
    if validation.valid:
        return SellSizingDecision(
            SellSizingStatus.READY,
            requested_quantity=requested,
            normalized_quantity=normalized,
            position_quantity=position_qty,
            free_quantity=free_qty,
            locked_quantity=locked_qty,
            effective_percentage=pct,
            amount_step=market.amount_step,
        )

    policy = str(partial_sell_policy or "accumulate")
    if policy == "sell_all" and pct < 100 and not convert_all_balance:
        return prepare_sell_quantity(
            market=market,
            balance=balance,
            position_quantity=position_qty,
            percentage=100,
            partial_sell_policy=policy,
            validation_price=validation_price,
            normalize_amount=normalize_amount,
            validate_order=validate_order,
            convert_all_balance=False,
        )
    return SellSizingDecision(
        SellSizingStatus.INVALID,
        requested_quantity=requested,
        normalized_quantity=normalized,
        position_quantity=position_qty,
        free_quantity=free_qty,
        locked_quantity=locked_qty,
        effective_percentage=pct,
        amount_step=market.amount_step,
        errors=tuple(validation.errors),
    )


def format_position_close_without_order(
    *, exchange: str, symbol: str, timeframe: str, decision: SellSizingDecision,
    closure_reason: str,
) -> str:
    reason = (
        "The position quantity is below the exchange amount step."
        if closure_reason == "dust"
        else "The exchange reported a zero total balance for the open position."
    )
    return "\n".join(
        [
            "Position closed without an exchange order",
            f"Exchange: {exchange}",
            f"Symbol: {symbol}",
            f"Timeframe: {timeframe}",
            f"Reason: {reason}",
            f"Position qty: {decision.position_quantity}",
            f"Free balance: {decision.free_quantity}",
            f"Locked balance: {decision.locked_quantity}",
            f"Amount step: {decision.amount_step or 'unknown'}",
            f"Residual qty: {decision.position_quantity}",
            "No order was submitted. The residual remains in the exchange wallet and was excluded from the BEC position PnL.",
        ]
    )
