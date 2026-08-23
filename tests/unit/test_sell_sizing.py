from decimal import Decimal, ROUND_DOWN

from bec.exchanges.base import (
    Balance,
    MarketInfo,
    OrderRequest,
    OrderValidation,
    SellOrderOutcome,
    SellOutcomeStatus,
    SubmissionState,
)
from bec.exchanges.sell_sizing import SellSizingStatus, prepare_sell_quantity


MARKET = MarketInfo(
    "PENGU/USDC",
    "PENGUUSDC",
    "PENGU",
    "USDC",
    True,
    amount_step=Decimal("0.0001"),
    min_amount=Decimal("0.0002"),
    min_cost=Decimal("5"),
)


def _normalize(_symbol: str, amount: Decimal) -> Decimal:
    return amount.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _validate(request: OrderRequest) -> OrderValidation:
    amount = request.amount or Decimal("0")
    errors = []
    if amount <= 0:
        errors.append("amount must be greater than zero")
    if amount < Decimal("0.0002"):
        errors.append("amount is below the exchange minimum")
    if amount * (request.price or Decimal("0")) < Decimal("5"):
        errors.append("cost is below the exchange minimum")
    return OrderValidation(not errors, tuple(errors), normalized_amount=amount)


def _prepare(*, position, free, locked="0", percentage=100, policy="accumulate"):
    return prepare_sell_quantity(
        market=MARKET,
        balance=Balance("PENGU", Decimal(str(free)), Decimal(str(locked))),
        position_quantity=Decimal(str(position)),
        percentage=percentage,
        partial_sell_policy=policy,
        validation_price=Decimal("50000"),
        normalize_amount=_normalize,
        validate_order=_validate,
    )


def test_full_exit_classifies_the_pengus_residual_as_structural_dust():
    decision = _prepare(position="0.000070", free="0.0001")

    assert decision.status == SellSizingStatus.DUST
    assert decision.position_quantity == Decimal("0.000070")
    assert decision.amount_step == Decimal("0.0001")


def test_partial_exit_that_rounds_to_zero_never_closes_the_position():
    decision = _prepare(position="0.00015", free="0.00015", percentage=25)

    assert decision.status == SellSizingStatus.INVALID
    assert decision.normalized_quantity == Decimal("0.0000")


def test_sell_all_rechecks_the_full_position_but_accumulate_does_not():
    accumulate = _prepare(
        position="0.0003", free="0.0003", percentage=25, policy="accumulate"
    )
    sell_all = _prepare(
        position="0.0003", free="0.0003", percentage=25, policy="sell_all"
    )

    assert accumulate.status == SellSizingStatus.INVALID
    assert sell_all.status == SellSizingStatus.READY
    assert sell_all.effective_percentage == Decimal("100")
    assert sell_all.normalized_quantity == Decimal("0.0003")


def test_full_exit_below_current_minimum_is_retained_not_called_dust():
    decision = _prepare(position="0.0001", free="0.0001")

    assert decision.status == SellSizingStatus.INVALID
    assert "amount is below the exchange minimum" in decision.errors


def test_zero_total_balance_and_locked_balance_are_distinct():
    zero = _prepare(position="2", free="0")
    locked = _prepare(position="2", free="0", locked="2")

    assert zero.status == SellSizingStatus.ZERO_BALANCE
    assert locked.status == SellSizingStatus.LOCKED_BALANCE


def test_small_available_balance_does_not_hide_a_larger_position():
    decision = _prepare(position="2", free="0.00007")

    assert decision.status == SellSizingStatus.BALANCE_MISMATCH


def test_partial_exit_also_reports_locked_or_divergent_balance():
    locked = _prepare(position="2", free="0", locked="2", percentage=25)
    mismatch = _prepare(position="2", free="0.00007", percentage=25)

    assert locked.status == SellSizingStatus.LOCKED_BALANCE
    assert mismatch.status == SellSizingStatus.BALANCE_MISMATCH


def test_sell_outcome_distinguishes_actions_skips_and_unknown_submission():
    skipped = SellOrderOutcome(status=SellOutcomeStatus.SKIPPED)
    closed = SellOrderOutcome(
        status=SellOutcomeStatus.CLOSED_WITHOUT_ORDER,
        position_closed=True,
        closure_reason="dust",
    )
    unknown = SellOrderOutcome(
        status=SellOutcomeStatus.FAILED,
        submission_state=SubmissionState.UNKNOWN,
        reconciliation_required=True,
    )

    assert skipped.action_taken is False
    assert skipped.get("skipped") is True
    assert closed.action_taken is True
    assert unknown.action_taken is True
