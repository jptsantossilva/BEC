# Exchange-Aware Runtime Operations

BEC supports one app-wide active exchange through PR 7. The selected exchange
scopes trading persistence, rankings, balances, backtests, signals and queued
work. Binance remains the only selectable adapter in PR 4.

## Selection Safety

The operator selects the exchange under **Trading → Settings → Exchange**.
Switching is rejected while any `Position=1` row or order in `pending`, `open`,
`partially_filled` or `unknown` state exists. Candidate rows with `Position=0`
do not block switching.

Fresh installations have no active exchange. Exchange-dependent schedules stay
disabled until selection. Selecting the first exchange enables the trading,
ranking and signal schedules; individual schedule toggles can then be adjusted
normally.

## Compatibility and Rollback

Upgraded installations continue with Binance selected and preserve legacy
symbols and behavior. PR 4 adds no migration and does not change order sizing,
strategy decisions, API credentials or adapter implementation.

To roll back PR 4, stop dashboard and jobs-runner together and deploy the PR 3
application image. The PR 3 schema remains compatible, but records written by
other exchanges must not exist before rollback.

## Validation

Automated tests must cover exchange isolation, first activation, switch
blocking, `Position=0` candidates, metadata persistence and job activation.
Inspect every Trading dashboard tab and confirm logs and Telegram output include
the active exchange identity.
