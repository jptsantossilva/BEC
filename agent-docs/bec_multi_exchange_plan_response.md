# Response to BEC Multi-Exchange Migration Plan

## Overall Position

The proposed plan is approved as the **target architecture** for the BEC multi-exchange migration.

The direction is correct: BEC should move from Binance-specific execution to an exchange-neutral architecture, while preserving existing Binance behavior and historical data during the transition.

However, implementation must be done in **small gated PRs**, not as one large migration. The first implementation step must be limited to Phase 1 only.

---

## 1. App-Wide Exchange Selection

For the first rollout, BEC must support **only one app-wide active exchange at a time**.

Changing the active/default exchange must only be allowed when there are **no open positions**.

### Required behavior

- BEC must have one active app-wide exchange.
- The active exchange controls:
  - live trading;
  - signal execution;
  - market data used for rankings;
  - backtesting candidate selection;
  - dashboard trading actions.
- The operator must not be able to switch the active exchange while open positions exist.
- If open positions exist, the dashboard must block the exchange switch and show a clear warning.

### Rationale

This avoids mixing execution state between exchanges and prevents cases where positions are opened in one exchange and later managed using another exchange’s market data, limits, balances, or symbol mappings.

Before Phase 8, there must be no simultaneous multi-exchange live trading.

---

## 2. SQLite Migration Safety Requirements

The proposed migration approach is good, especially the move toward explicit, transactional, idempotent schema migrations instead of scattered `ALTER TABLE` checks.

However, SQLite table rebuilds are sensitive, especially when replacing old unique constraints with exchange-aware constraints. The migration implementation must therefore be conservative and auditable.

### Required SQLite table-rebuild process

When a table must be rebuilt, the migration must follow this process:

1. Create an automatic backup before applying the migration.
2. Create the new table with the target schema.
3. Copy data from the old table to the new table.
4. Validate row counts.
5. Validate constraints and indexes.
6. Validate foreign-key relationships where applicable.
7. Only after validation, rename/swap the tables.
8. Keep rollback possible if any validation step fails.

No destructive table rebuild should happen without backup and validation.

### Required migration commands

Add a manual migration entry point that supports dry-run and apply modes:

```bash
python -m bec.db.migrations --database prod.db --dry-run
python -m bec.db.migrations --database prod.db --backup --apply
```

### Production rollout requirement

Before running migrations on the VPS production database, the migration must be tested against a real copy of the production database.

The migration must verify:

- row counts before and after;
- migrated Binance backfill values;
- open positions remain readable;
- existing orders remain readable;
- dashboard queries still work;
- backtesting result queries still work;
- repeated migration startup is idempotent.

---

## 3. Paper Trading Scope

The proposed persistent paper trading model is technically strong, but it may be too much complexity for the first rollout.

Paper trading requires additional configuration, simulated balances, simulated fills, dashboard controls, persistence, reconciliation, and mode separation. This may distract from the immediate goal: introducing a safe exchange-neutral architecture and later adding Kraken support.

### Revised position

Paper trading should be considered **optional and deferrable**.

For the first rollout, it is acceptable to either:

- remove paper trading from the initial scope; or
- keep only a very simple dry-run/logging mode; or
- defer persistent paper trading to a later dedicated phase.

### First rollout priority

The first rollout should prioritize:

- ExchangeAdapter boundary;
- BinanceAdapter wrapping existing behavior;
- safe schema migration;
- exchange-aware data model;
- Kraken public data and later live execution;
- no change to existing Binance live behavior during Phase 1.

Persistent paper trading should not block the initial architecture migration.

---

## 4. Default Exchange Behavior

The plan currently proposes seeding Binance as the enabled/default live exchange.

This should be changed.

BEC should **not automatically choose a default exchange** for new installations. The operator must explicitly select the exchange before live trading is allowed.

### Required behavior

For new installations:

- no exchange should be selected as default automatically;
- live trading must remain disabled until the operator selects an active exchange;
- the dashboard must show a clear warning if no active exchange is configured.

For upgraded installations:

- existing Binance behavior may be preserved for compatibility;
- legacy rows must be backfilled as Binance;
- the operator should still be encouraged to review the active exchange configuration after migration.

### Rationale

The BEC project must remain exchange-agnostic and jurisdiction-agnostic. The operator is responsible for selecting an exchange that is appropriate for their own use case and region.

The application should not silently start live trading on any exchange without explicit operator choice.

---

## 5. Non-Goals / Out of Scope for First Rollout

The following items are explicitly out of scope for the first rollout:

- No multi-exchange simultaneous live trading before Phase 8.
- No arbitrage.
- No transfers between exchanges.
- No automatic asset migration from Binance to Kraken.
- No futures or margin trading.
- No staking, earn, lending, deposits, or withdrawals.
- No automatic legal or regulatory decisions.
- No regional/jurisdiction-based exchange policy.
- No auto-enable live Kraken buys.
- No reuse of Binance backtests for Kraken live approval.
- No strategy changes required for exchange migration.
- No private Kraken live trading until the adapter, migration, and exchange-aware backtesting are validated.

---

## 6. Implementation Must Be Split into Gated PRs

The plan is approved as target architecture, but it must not be implemented as a single large PR.

Implementation must be split into small, reviewable, testable, gated PRs.

### Start with Phase 1 only

The first PR must do only the following:

- Add `ExchangeAdapter`.
- Wrap current Binance behavior behind `BinanceAdapter`.
- Do not add Kraken yet.
- Do not add CCXT yet, except possibly as future-facing documentation.
- Do not change live trading behavior.
- Do not change strategy logic.
- Do not change order sizing behavior.
- Do not change dashboard behavior except where required for the adapter boundary.
- Add static tests preventing direct `python-binance` imports outside the adapter layer.
- Ensure existing Binance trading, dashboard, backtesting, and Telegram behavior remain unchanged.

### Phase 1 success criteria

Phase 1 is complete only when:

- BEC runs exactly as before with Binance;
- all current Binance functionality still works;
- no direct `python-binance` calls exist outside `bec/exchanges/`;
- strategy/signal code calls only the exchange-neutral interface;
- dashboard/manual actions use the adapter boundary;
- tests pass;
- manual Streamlit inspection confirms no visible regression.

Only after Phase 1 is merged and tested should Phase 2 migration begin.

---

## 7. Recommended PR Sequence

A safer rollout sequence is:

1. **PR 1 — Exchange boundary only**
   - Add `ExchangeAdapter`.
   - Add `BinanceAdapter`.
   - Preserve all current behavior.

2. **PR 2 — Migration framework**
   - Add versioned, transactional SQLite migrations.
   - Add backup, dry-run, apply, and validation commands.
   - Do not yet perform large exchange-aware schema changes.

3. **PR 3 — Exchange-aware schema migration**
   - Add `Exchanges` and exchange-aware fields.
   - Backfill legacy rows as Binance.
   - Preserve legacy `Symbol` columns during compatibility window.

4. **PR 4 — Exchange-aware queries**
   - Update trading, dashboard, positions, orders, PnL, backtesting, and rankings to use exchange-aware keys.
   - Keep behavior unchanged for Binance.

5. **PR 5 — CCXT/Kraken public data**
   - Add CCXT adapter for public market data only.
   - Add Kraken market metadata, symbols, candles, and health checks.
   - No live Kraken trading yet.

6. **PR 6 — Kraken backtesting**
   - Recompute Kraken-specific backtests.
   - Prevent Binance backtests from approving Kraken live buys.

7. **PR 7 — Kraken live execution gated**
   - Add private Kraken trading only after public data and backtests are validated.
   - Require explicit operator action to enable live buys/sells.

8. **PR 8 — Optional paper trading or dry-run expansion**
   - Add persistent paper trading only if still desired.
   - Alternatively keep simple dry-run as the supported non-live test mode.

9. **PR 9 — Phase 8 multi-exchange mode**
   - Only after single-exchange Kraken live operation is stable.

---

## 8. Final Decision

Proceed with the architecture direction, but apply the following changes to the submitted plan:

- Use only one app-wide active exchange before Phase 8.
- Block exchange switching while open positions exist.
- Strengthen SQLite migration safety requirements.
- Add dry-run/apply migration commands.
- Test migrations on a production database copy before VPS rollout.
- Defer or simplify persistent paper trading.
- Do not automatically choose a default exchange for new installations.
- Add a stronger non-goals/out-of-scope section.
- Start with Phase 1 only.
- Implement the migration through small gated PRs.

The target architecture is approved, but execution must be incremental, conservative, and regression-safe.
