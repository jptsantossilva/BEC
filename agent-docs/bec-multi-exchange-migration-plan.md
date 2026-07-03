# BEC Multi-Exchange Implementation Plan

## Purpose

This is the canonical implementation plan for migrating BEC from its current
Binance-specific architecture to an exchange-neutral architecture. Implementation
must be delivered through independently reviewable PRs with explicit scope,
exclusions, validation, rollout checks, and completion gates.

The approval and decision record is kept in
`agent-docs/bec_multi_exchange_plan_response.md`.

## PR Sequence

### PR 1 — Exchange Boundary

- Add canonical exchange types and the complete `ExchangeAdapter` contract.
- Implement `BinanceAdapter` using `python-binance`.
- Add an exchange-neutral compatibility service for market data, balances,
  sizing, order execution, persistence, locks, and Telegram integration.
- Migrate strategy, signal, backtesting, ranking, balance, and dashboard callers
  to the neutral boundary.
- Preserve `bec.exchanges.binance` as a temporary compatibility shim where
  required.
- Add an architecture test preventing `python-binance` imports outside the
  adapter layer.
- Do not change the database, dashboard behavior, sizing, strategies, live
  behavior, or add CCXT or Kraken.

Completion gate:

- Run the full test suite and compile checks.
- Add mocked adapter contract tests.
- Inspect the Streamlit dashboard manually.
- Confirm that existing Binance behavior remains unchanged.

### PR 2 — Migration Framework

- Add `bec.db.migrations` with ordered, versioned migrations.
- Support:

  ```bash
  python -m bec.db.migrations --database prod.db --dry-run
  python -m bec.db.migrations --database prod.db --backup --apply
  ```

- Make dry-run migrate and validate a temporary database copy.
- Add timestamped backups, checksums, reports, row-count validation,
  constraint/index validation, foreign-key checks, and rollback-safe table
  swapping.
- Allow safe additive migrations at startup.
- Block startup when a pending migration requires a table rebuild and show the
  required command.
- Initialize new databases directly at the latest schema.

Completion gate:

- Pass migration framework and idempotency tests.
- Verify failed-migration rollback and backup restoration.
- Confirm dry-run never modifies the source database.

### PR 3 — Exchange-Aware Schema

- Create `Exchanges` and supporting exchange metadata tables.
- Add exchange identity and normalized-symbol fields to trading, ranking,
  backtesting, balance, lock, and job records.
- Add order status, execution quantity, average price, fees, and net quantity
  fields.
- Replace symbol-only uniqueness with exchange-aware constraints.
- Backfill legacy rows as Binance while retaining legacy `Symbol` fields.
- Preserve Binance as the active exchange and retain existing behavior for
  upgrades identified by the legacy schema.
- For new installations, create no active/default exchange and disable
  exchange-dependent jobs.
- Mark unresolved legacy symbols as non-tradable and include them in the
  migration report.
- Require manual migration because table rebuilds are involved.

Completion gate:

- Run dry-run and apply against a real copy of the production database.
- Verify row counts, Binance backfills, open positions, orders, dashboard
  queries, backtesting queries, indexes, constraints, and foreign keys.
- Verify repeated execution is idempotent.

### PR 4 — Exchange-Aware Runtime and Dashboard

- Update every query and persistence operation to use exchange-aware keys.
- Add the app-wide active exchange configuration and selector.
- Block exchange switching while open positions or orders in `pending`, `open`,
  `partially_filled`, or `unknown` state exist.
- Do not let `Position=0` candidates block switching.
- Disable exchange-dependent jobs until an exchange is explicitly selected on
  new installations.
- Filter positions, orders, rankings, PnL, balances, candidates, and backtests
  by active exchange.
- Include exchange identity in logs and Telegram events.
- Keep Binance as the only available adapter and preserve its behavior.

Completion gate:

- Pass database/query, switch-blocking, new-install, and upgrade tests.
- Run the full test suite.
- Inspect all affected dashboard views manually.

### PR 5 — CCXT and Kraken Public Data

- Add the pinned CCXT dependency through the dependency-generation workflow.
- Implement the reusable CCXT adapter layer.
- Add Kraken symbol mapping, markets, precision, limits, OHLCV, ticker, order
  book, public health, and metadata caching.
- Support normalized symbols such as `BTC/EUR` and Kraken aliases such as
  `XBT`.
- Keep private Kraken balances and order methods disabled.
- Allow Kraken selection for public data and backtesting preparation while
  clearly indicating that live execution is unavailable.

Completion gate:

- Pass adapter contract fixtures, mocked public-data integration tests,
  symbol/precision tests, and cache tests.
- Validate Kraken public endpoints manually.

### PR 6 — Kraken-Specific Backtesting

- Pass exchange identity through queues, subprocess arguments, results, trades,
  reports, filenames, fingerprints, approvals, and candidate selection.
- Require an explicit configured backtesting fee.
- Recompute results using Kraken market data.
- Prevent Binance results from approving Kraken candidates or live buys.
- Require matching exchange, symbol, timeframe, strategy, fee, and work
  fingerprint for Kraken approval.

Completion gate:

- Prove with tests that Binance and Kraken results cannot collide.
- Pass deterministic fee, queue, and report tests.
- Complete Kraken backtests for the intended symbols.

### PR 7 — Gated Kraken Live Execution

- Enable private Kraken balances and market-order operations through CCXT.
- Implement canonical create, fetch, cancel, fill parsing, partial fills, fee
  handling, net quantities, and reconciliation.
- Persist an order intent before submission and prevent duplicate retries after
  uncertain outcomes.
- Add scheduled/startup reconciliation for unsettled orders.
- Enforce market precision, amount limits, cost limits, and the default 1%
  base-quantity sizing buffer.
- Implement below-minimum partial-sell policies with `accumulate` as the
  default.
- Add operator-controlled exchange, buy, and sell flags.
- Require explicit activation and never auto-enable Kraken live trading.
- Expose API-permission and IP-allowlist warnings without implementing
  withdrawals.

Completion gate:

- Pass mocked private API, failure, retry, and reconciliation tests.
- Review API credentials and permissions manually.
- Execute one controlled minimum-valid buy and sell.
- Verify exchange state, database state, fees, balances, PnL, logs, and Telegram
  output.

### PR 8 — Optional Paper Trading

- This PR is not automatically authorized by this plan.
- Retain the current non-ordering `run_mode=test` through PR 7.
- After Kraken live stabilization, explicitly decide whether to:
  - implement persistent paper balances, fills, positions, and reconciliation;
  - add a smaller validation-only dry-run mode; or
  - skip this PR.
- If selected, create and approve a dedicated implementation plan before coding.

### PR 9 — Future Simultaneous Multi-Exchange Mode

- This PR is not authorized until single-exchange Kraken operation is stable.
- Create a separate design and obtain approval before implementation.
- The future design must cover per-bot exchange assignment, capital allocation,
  risk limits, position limits, and global duplicate-position prevention.

## Delivery Rules

- Start each PR only after the preceding PR has been merged and validated.
- Do not stack schema, adapter, Kraken, and execution changes into one PR.
- Every PR must state compatibility guarantees, rollback procedures, tests run,
  manual checks, and known limitations.
- Mock all external exchange calls in automated tests.
- Do not run a production migration before validating against a production
  database copy.
- Do not place a real Kraken order before PRs 1–6 have passed their gates.
- Keep root compatibility wrappers stable.
- Never commit secrets, runtime databases, logs, or generated reports.

## Validation Baseline

Run the applicable checks for every PR:

```bash
.venv/bin/python -m pytest tests
.venv/bin/python -m py_compile bec/*.py bec/utils/*.py bec/exchanges/*.py bec/signals/*.py
npm run docs:build
```

Also inspect affected Streamlit pages manually and verify Docker services start
with clean logs when runtime behavior changes.

## Assumptions and Non-Goals

- Spot market trading remains the only supported trading mode.
- BEC remains exchange-agnostic and jurisdiction-agnostic.
- Binance stays native through `python-binance`; Kraken uses CCXT.
- Only one app-wide active exchange is supported through PR 7.
- No simultaneous multi-exchange live trading before PR 9.
- No arbitrage, transfers between exchanges, or automatic asset migration.
- No futures, margin, staking, earn, lending, deposits, or withdrawals.
- No automatic legal, regulatory, or regional exchange decisions.
- No automatic activation of Kraken live buys.
- Binance backtests cannot approve Kraken trading.
- Exchange migration must not require strategy changes.

