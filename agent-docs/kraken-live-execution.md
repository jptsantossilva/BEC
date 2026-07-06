# Gated Kraken Live Execution

PR 7 adds private Kraken spot balances and market orders through CCXT. It does
not support withdrawals, margin, futures, paper balances, or simultaneous
multi-exchange trading.

## Safety Gates

- `KRAKEN_API_KEY` and `KRAKEN_API_SECRET` are read only from the environment.
- The key must have Query Funds and Create/Modify Orders permissions only.
  Never enable Withdraw Funds; configure an IP allowlist where possible.
- Kraken `Buy Enabled` and `Sell Enabled` are false after migration and on new
  installations. Enabling the exchange does not enable either operation.
- Live execution additionally requires `run_mode=live` and an explicitly
  enabled `main_*` schedule.
- The dashboard private API check reads balances only and never places orders.

## Order Lifecycle

BEC persists an `Orders` intent with a unique client order id before calling
Kraken. A submitted intent is never submitted a second time. Exceptions with
an uncertain exchange outcome are stored as `unknown` and resolved by client
order id or exchange order id during startup and the 15-minute reconciliation
job.

Canonical order responses persist status, executed quantity, average price,
fees, net quantity and raw response context. Partial-fill application is
idempotent: only newly executed quantity changes a position. Market precision,
amount and cost limits are validated before submission. Quote-based buys are
used when available; base-quantity fallback applies the configured 1% buffer.

Below-minimum partial sells use one of these per-exchange policies:

- `accumulate` (default): retain the remainder for a future valid sell.
- `sell_all`: retry validation with the full available position quantity.
- `skip`: leave the position unchanged.

## Rollout

Apply migration `6:gated_kraken_live_execution`, leave both live flags off and
validate public operation first. Review credentials and permissions, run the
private balance check, then enable only the side and timeframe needed for the
controlled minimum-valid test. Verify Kraken, database, balances, fees, PnL,
logs and Telegram before enabling unattended execution.

Rollback requires both live flags and all `main_*` schedules to be disabled,
all unsettled intents reconciled, and deployment of the PR 6 image. The
additive schema can remain in place.
