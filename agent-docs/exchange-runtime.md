# Exchange-Aware Runtime Operations

BEC supports one app-wide active exchange through PR 7. The selected exchange
scopes trading persistence, rankings, balances, backtests, signals and queued
work. Binance remains the only selectable adapter in PR 4.

## Selection Safety

The operator selects the exchange under **Trading → Settings → Exchange**.
Switching is rejected while any `Position=1` row or order in `pending`, `open`,
`partially_filled` or `unknown` state exists. Candidate rows with `Position=0`
do not block switching.

Through PR 4, fresh installations had no active exchange. PR 6 latest-schema
initialization enables and selects Kraken by default, with only public
market-phase analysis enabled. Operators may disable all exchanges, returning
the application to a no-active-exchange state with exchange-dependent
schedules disabled. Selecting Binance enables its normal trading, ranking and
signal schedules; selecting Kraken keeps live trading schedules disabled.

## Compatibility and Rollback

Upgraded installations continue with Binance selected and preserve legacy
symbols and behavior. PR 4 adds no migration and does not change order sizing,
strategy decisions, API credentials or adapter implementation.

To roll back PR 4, stop dashboard and jobs-runner together and deploy the PR 3
application image. The PR 3 schema remains compatible, but records written by
other exchanges must not exist before rollback.

## Exchange-Neutral Sell Remainders

All supported spot exchanges use the active market's amount step before a sell
intent is created. A positive amount below that step normalizes to zero and is
handled as a validation result, never as an unhandled adapter exception.

An effective 100% exit closes the BEC position without creating a synthetic
order when the position quantity itself is below the amount step. The residual
remains in the exchange wallet, realized PnL contains only genuine fills, and
the operator receives one informational message. A successful full sell also
closes a remainder below the step after persisting the actual executed amount.

Amounts that satisfy the step but are below the exchange's current minimum
amount or notional are not written off. They remain open under the configured
`accumulate`, `sell_all`, or `skip` policy because they may become tradable
later. A zero total exchange balance may close a full-exit position as an
external-balance reconciliation event; a positive locked balance never does.

Before any close without an order, BEC rechecks that the position is unchanged
and no unsettled sell intent exists. Partial sells that normalize to zero leave
the position and take-profit state unchanged.

## Batched Live Market Data

The scheduler can group simultaneously due `main_1h`, `main_4h` and `main_1d`
jobs into one process. Strategy requirements are recalculated for every run,
deduplicated by symbol and timeframe, held only in memory and discarded when
the process exits. Public OHLCV reads are bounded by the active adapter policy;
all trading decisions, persistence writes and private exchange calls remain
sequential, with every sell phase completing before any buy phase starts.

The rollout allowlist enables this path for Kraken, OKX and Binance after their
read-only benchmark validation. Unknown exchanges remain on the legacy
per-timeframe scheduler path. Removing an exchange from the allowlist is the
immediate rollback.

## Validation

Automated tests must cover exchange isolation, first activation, switch
blocking, `Position=0` candidates, metadata persistence and job activation.
Inspect every Trading dashboard tab and confirm logs and Telegram output include
the active exchange identity.
