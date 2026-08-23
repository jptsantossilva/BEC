# Gated Kraken Live Execution

PR 7 adds private Kraken spot balances and market orders through CCXT. It does
not support withdrawals, margin, futures, paper balances, or simultaneous
multi-exchange trading.

## Safety Gates

- `KRAKEN_API_KEY` and `KRAKEN_API_SECRET` are read only from the environment.
- The key must have Query Funds, Create/Modify Orders, Query Open Orders &
  Trades, and Query Closed Orders & Trades permissions. Never enable Withdraw
  Funds; configure an IP allowlist where possible.
- Kraken `Buy Enabled` and `Sell Enabled` are false after migration and on new
  installations. Enabling the exchange does not enable either operation.
- Live execution additionally requires `run_mode=live` and an explicitly
  enabled `main_*` schedule.
- The dashboard private API check reads balances plus open and closed order
  history and never places orders.

## Order Lifecycle

BEC persists an `Orders` intent with a unique client order id before calling
Kraken. A submitted intent is never submitted a second time. A successful
`AddOrder` acknowledgement stores its `txid` as pending, then BEC queries that
exact order after 1, 2 and 4 seconds. The first read error or the third
non-terminal result defers confirmation to the one-minute reconciliation job.
Exceptions with an uncertain submission outcome are stored as `unknown` and
resolved without resubmission. Reconciliation uses the exchange order id first
and client-order/open/closed searches only as a fallback.

Canonical order responses persist status, executed quantity, average price,
fees, net quantity and raw response context. Partial-fill application is
idempotent: only newly executed quantity changes a position. Market precision,
amount and cost limits are validated before submission. Quote-based buys are
used when available; base-quantity fallback applies the configured 1% buffer.

Below-minimum partial sells use one of these per-exchange policies:

- `accumulate` (default): retain the remainder for a future valid sell.
- `sell_all`: retry validation with the full available position quantity.
- `skip`: leave the position unchanged.

These policies are exchange-neutral and also apply to Binance and OKX. A full
exit whose remaining position is below the market amount step is closed
logically without another exchange order. No synthetic fill or PnL is created;
the residual stays in the exchange wallet and is reported once to the operator.
Amounts below a current minimum amount or notional, but not below the amount
step, remain open under the selected policy.

## Rollout

Apply migration `6:gated_kraken_live_execution`, leave both live flags off and
validate public operation first. Review credentials and permissions, run the
private balance check, then enable only the side and timeframe needed for the
controlled minimum-valid test. Verify Kraken, database, balances, fees, PnL,
logs and Telegram before enabling unattended execution. Trade messages separate
submission time, Kraken trade or close time, persistence time, delay, and the
immediate or scheduled confirmation source.

Rollback requires both live flags and all `main_*` schedules to be disabled,
reconciliation to remain enabled, and deployment of the previous image. The
existing schema remains in place.

See `agent-docs/kraken-order-confirmation-plan.md` for the production audit,
timestamp semantics, detailed tests, and exclusions.
