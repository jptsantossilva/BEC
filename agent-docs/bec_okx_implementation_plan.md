# BEC OKX Spot Trading Implementation Plan

## Summary

Add OKX spot support incrementally on top of the completed multi-exchange/Kraken architecture while preserving:

- Exactly one app-wide active exchange.
- Switching blocked by any open position or order in `pending`, `open`, `partially_filled`, or `unknown`.
- Complete Binance, Kraken, OKX production, and OKX demo isolation.
- No automatic activation of live trading.
- No futures, margin, staking, earn, deposits, withdrawals, transfers, arbitrage, or simultaneous exchange operation.

The safe sequence is eight small PRs: generalize the remaining Kraken-specific runtime, harden durable execution, register OKX, add public data, enable isolated backtesting/rankings, add authentication/read-only checks, validate execution in OKX Demo Trading, and finally gate production trading.

## Verified Current State

- The canonical prior plan is `agent-docs/bec-multi-exchange-migration-plan.md`; the requested `bec_multi_exchange_implementation_plan.md` does not exist. `bec_multi_exchange_plan_response.md` is the earlier approval record.
- Git history and code show that prior PRs 1–7 are implemented through commit `ebdfc52`.
- `ExchangeAdapter` currently requires:
  - markets and OHLCV;
  - balances, ticker, and order book;
  - market buy/sell;
  - fetch/cancel order;
  - symbol, amount, and price normalization;
  - order validation and health checks.
- `MarketInfo` holds canonical/native symbols, assets, active state, precision steps, amount/cost limits, and quote-cost-buy support. It does not expose explicit spot/contract metadata.
- Binance remains native through `python-binance`. `BinanceAdapter` implements the canonical contract, while `bec/exchanges/binance.py` still contains the legacy domain workflow used by the compatibility service.
- CCXT `4.5.64` is already pinned. `CcxtExchangeAdapter` implements reusable public and private primitives; `KrakenAdapter` supplies credentials and aliases.
- The installed CCXT version provides both `okx` and EEA `myokx`, requires API key, secret, and password/passphrase, supports spot, sandbox/demo mode, quote-cost market buys, order lookup, cancellation, and order-history endpoints.
- Active-exchange switching is transactionally blocked across the whole database, not merely the current exchange, by all `Position=1` rows and unsettled order statuses. `Position=0` candidates do not block.
- Exchange-aware persistence currently covers:
  - `Exchange_Id`, canonical/native symbol metadata, base, and quote across orders, positions, backtests, jobs, Monte Carlo, rankings, signals, and candidates;
  - exchange-scoped balances and locked values;
  - exchange-scoped uniqueness for results and jobs;
  - exchange identity and fee/settings context in backtesting fingerprints and output filenames.
- Orders persist intent before submission, client and exchange IDs, status, cumulative execution, average price, one aggregate fee asset/amount, net quantity, applied quantities, raw response, errors, and reconciliation state.
- Individual fills are not durably normalized. Multiple fee assets collapse to an empty aggregate, and late fee corrections cannot safely decrease an already-applied net buy quantity.
- Reconciliation, private execution, schedules, UI, and messages remain explicitly Kraken-named or code-gated in several places.
- Public-market persistence is specialized: Binance symbols are parsed locally; only Kraken uses the adapter to resolve canonical/native metadata.
- Fresh databases currently select Kraken for public workflows by default. Adding OKX must not change the active/default exchange on either new or upgraded installations.
- Logs and Telegram messages receive an exchange identity prefix, but several user-facing recovery messages still name Binance or Kraken directly.
- Focused validation produced 70 passing tests and five failures in the Kraken “private disabled” cases. `KrakenAdapter` reloads the project `.env` and can infer private availability even when a fake client is injected, making those tests dependent on local environment state. This must be corrected before cloning the pattern for OKX.

## External OKX Constraints

- OKX requires API key, secret, and passphrase; API permissions are `Read`, `Trade`, and `Withdraw`. BEC must require only Read and Trade, prohibit Withdraw, and recommend IP binding. [OKX API documentation](https://www.okx.com/docs-v5/en/)
- OKX Demo Trading uses separate demo credentials and the `x-simulated-trading: 1` header. CCXT maps this through sandbox mode. [OKX API documentation](https://www.okx.com/docs-v5/en/), [CCXT manual](https://github.com/ccxt/ccxt/wiki/manual)
- A spot market order uses `tgtCcy` to decide whether size is base or quote currency. Quote-cost buys must be explicit; OKX may reserve an additional amount for market-order risk checks. [OKX order documentation](https://www.okx.com/docs-v5/en/)
- `clOrdId` is at most 32 alphanumeric characters and is only guaranteed unique among pending orders. BEC must retain permanent database uniqueness and never reuse an ID.
- The installed CCXT OKX adapter supports `createMarketBuyOrderWithCost`, passphrase credentials, sandbox mode, and client-order IDs. [CCXT OKX adapter](https://github.com/ccxt/ccxt/blob/master/python/ccxt/okx.py)

## Public Interface Changes

- Extend `MarketInfo` with explicit `market_type`, `spot`, `contract`, `contract_size`, `linear`, `inverse`, and settlement-asset metadata. Every trading path must reject non-spot or contract instruments.
- Add an `ExchangeCapabilities` value exposed by every adapter, covering public data, private balance, live spot, demo support, quote-cost market buys, client-ID constraints, and fill retrieval.
- Extend `ExchangeAdapter` with:
  - `fetch_order_by_client_id(...)`;
  - `fetch_order_fills(...)`;
  - explicit client-order-ID validation.
- Extend `OrderFill` with exchange trade ID, timestamp, symbol, and raw context.
- Persist installation-level adapter variant and execution environment separately from the canonical exchange code. OKX production and demo receive separate `Exchange_Id` values so their positions and orders can never collide.

## OKX PR 1 — Remove Remaining Exchange-Code Branching

**Objective**

Make the existing runtime capability-driven without registering or exposing OKX.

**Likely components**

- `bec/exchanges/base.py`, registry, service, and live-execution boundary.
- `bec/utils/database.py`, scheduler/reconciliation entry points.
- Trading settings UI and exchange/runtime tests.

**Concrete changes**

- Add the capability and market metadata contracts described above.
- Implement the new contract for Binance and Kraken without changing their behavior.
- Replace `code == "kraken"`/`code == "binance"` decisions with adapter capabilities except where the native Binance legacy path is intentionally required.
- Generalize `_exchange_symbol_metadata()` so every CCXT-backed adapter resolves canonical/native symbols through its adapter.
- Rename the internal Kraken live workflow and reconciliation functions to exchange-neutral names; leave compatibility imports/wrappers temporarily.
- Introduce the generic `orders_reconcile_15m` schedule. Migrate the enabled state and `last_run` from `kraken_reconcile_15m`, retaining a compatibility alias for one release.
- Make public/backtesting/live availability derive from capabilities rather than hardcoded code sets.
- Make injected test adapters deterministic: explicit constructor arguments must override `.env`, and credential presence must not itself authorize order submission.

**Exclusions**

- No OKX database row, adapter, credentials, or public calls.
- No order schema change.
- No change to strategy, sizing, fees, or active-exchange rules.

**Binance compatibility**

- Binance remains native and uses its existing legacy buy/sell workflows.
- Existing environment variable names, sizing, schedules, positions, and orders remain unchanged.

**Automated tests**

- Adapter contract tests for Binance and Kraken.
- Capability-routing tests proving no Kraken code check is needed.
- Deterministic credential tests with and without a local `.env`.
- Existing exchange-switch, scheduler, and Kraken live suites.

**Manual validation**

- Inspect Binance and Kraken settings.
- Confirm current active exchange and schedules are unchanged.
- Run Binance public health and Kraken public/private health without placing orders.

**Rollback**

Deploy the previous image. The additive schedule alias may remain; disable the new schedule before rollback if it was enabled.

**Completion gate**

- No behavior regression in Binance or Kraken.
- No exchange-code allowlist remains in public/backtesting routing.
- Focused suites and full Python test suite pass independently of local credentials.

**Known risks**

- Compatibility imports named `binance` conceal exchange-neutral behavior.
- Schedule migration must not create duplicate reconciliation runs.

## OKX PR 2 — Durable Fills and Generic Reconciliation Hardening

**Objective**

Correct the existing generic order lifecycle before attaching another live exchange.

**Likely components**

- Exchange contract and CCXT/Binance order parsing.
- Versioned migration and order persistence.
- Generic live workflow and reconciliation tests.

**Concrete changes**

- Add an additive `Order_Fills` table containing local order ID, exchange ID, exchange order ID, trade ID or deterministic fill fingerprint, canonical/native symbol, price, quantity, fee asset/amount, timestamp, and raw JSON.
- Add `Executed_Cost` and `Fees_JSON` to `Orders`; retain `Fee_Asset` and `Fee_Amount` for backward-compatible single-asset displays.
- Enforce unique fill application by exchange/trade ID or deterministic fingerprint.
- Update `apply_order_result()` to:
  - persist fills transactionally;
  - aggregate multiple fee assets;
  - apply only new execution deltas;
  - support a signed net-quantity correction if fee information arrives after the original fill;
  - keep position, PnL, order, and fill updates in one transaction.
- Distinguish known rejection errors from uncertain outcomes:
  - network errors, request timeouts, and ambiguous exchange errors become `unknown`;
  - validation, authentication, permission, and insufficient-balance failures become `rejected` and are not retried.
- Resolve unknown orders by client ID first, then exchange order ID, then fills/trades; never resubmit an existing intent.
- Enforce adapter-specific client-order-ID format and length before persisting an intent.
- Make startup and periodic reconciliation generic and exchange-scoped.

**Exclusions**

- No OKX registration or credentials.
- No change to buy/sell strategy decisions.
- No historical reconstruction of fills that cannot be derived safely from existing raw responses.

**Binance compatibility**

- Legacy filled Binance orders remain readable.
- New Binance adapter results may populate fills, but native order behavior is unchanged.
- Existing Kraken intents remain reconcilable.

**Automated tests**

- Multiple fee assets, base/quote fees, missing initial fees, and later fee correction.
- Partial fill followed by fill, cancellation, or rejection.
- Duplicate response and duplicate fill idempotency.
- Timeout versus known rejection classification.
- Client-ID lookup and permanent duplicate prevention.
- Migration success, idempotency, and rollback.

**Manual validation**

- Run migration dry-run against a production database copy.
- Inspect legacy Binance/Kraken order displays and PnL.
- Confirm reconciliation does not submit an order.

**Rollback**

Disable reconciliation and live flags, reconcile every unsettled intent, then deploy PR 1. Leave additive columns/table in place.

**Completion gate**

All lifecycle tests pass, no fill can change a position twice, and an uncertain submission has no automatic resubmission path.

**Known risks**

- Historical raw payloads may not contain enough information to backfill fills.
- Fee corrections can expose pre-existing quantity/PnL discrepancies.

## OKX PR 3 — Configuration, Registration, and Isolation

**Objective**

Register disabled OKX production and demo environments without enabling API access.

**Likely components**

- Exchange and backtesting schema migrations.
- Exchange settings persistence and dashboard.
- Migration/runtime tests and environment examples.

**Concrete changes**

- Add immutable exchange metadata for:
  - `okx`: production public/backtesting/live identity;
  - `okx_demo`: demo execution identity.
- Add `Adapter_Id` and `Execution_Environment` to `Exchanges`; backfill Binance and Kraken.
- Default OKX adapter variant to EEA `myokx`, with a documented pre-activation override to global `okx`. Once OKX symbols, jobs, results, orders, or positions exist, changing the variant is blocked.
- Initialize both OKX identities as disabled, non-default, with buys and sells disabled.
- Use `USDC` as the initial quote asset, but require successful market validation before saving/enabling it.
- Do not insert an implicit OKX backtesting fee. Backtesting remains blocked until the operator enters an explicit non-negative fee.
- Extend `Exchange_Symbols` with precision, amount/cost limits, market/contract metadata, raw metadata, sync timestamp, and availability status.
- Display the registered identities in Trading settings, clearly labeling Production and Demo.
- Preserve the current active/default exchange and all schedules during migration.

**Exclusions**

- No OKX network access or adapter.
- No credentials or passphrase storage.
- No enabling of public, demo, or production workflows.

**Binance compatibility**

No Binance or Kraken metadata/default/fee/flag is changed.

**Automated tests**

- Upgrade and new-database initialization.
- IDs, unique codes, environments, and disabled defaults.
- No active-exchange or schedule change.
- Switching blockers also apply between OKX demo and production.
- Adapter-variant immutability after persisted OKX activity.

**Manual validation**

Run migration dry-run and apply against a current database copy; inspect exchanges, defaults, fees, flags, schedules, row counts, and foreign keys.

**Rollback**

Deploy PR 2. Leave disabled OKX rows and additive metadata in place.

**Completion gate**

OKX is visible but cannot be selected for execution or backtesting, and migration validation proves no existing data/default changed.

**Known risks**

- Regional account/API selection is installation-specific.
- A wrong variant must be corrected before any OKX data is persisted.

## OKX PR 4 — Public Markets, Symbols, and Market Data

**Objective**

Implement credential-free OKX spot public data and metadata synchronization.

**Likely components**

- New OKX adapter and reusable CCXT adapter.
- Registry, symbol metadata persistence, and Trading health UI.
- Mocked adapter/public-data tests.

**Concrete changes**

- Add `OkxAdapter` using the configured `myokx` or `okx` CCXT class.
- Support canonical `BASE/QUOTE`, CCXT unified symbols, and native OKX `BASE-QUOTE` aliases.
- Persist canonical symbol separately from the native instrument ID.
- Fail closed on alias collisions instead of silently replacing one market.
- Load only active spot, non-contract markets.
- Capture amount/price precision, min/max amount, min/max cost, spot/contract flags, contract size, settlement asset, and raw metadata.
- Upsert the OKX market catalog into `Exchange_Symbols`; mark missing/delisted instruments non-tradable rather than deleting them.
- Add OHLCV pagination, ticker, order book, and health checks through the existing contract.
- Respect current incomplete-candle and date-bound behavior used by backtesting.
- Enable selection of production OKX for public workflows only after a successful market load validates the configured quote asset.
- Keep demo identity unavailable until demo credentials are added.

**Exclusions**

- No private balance, order, cancellation, or credentials.
- No backtesting approval or live schedules.
- No non-spot instruments, even if CCXT returns them.

**Binance compatibility**

Binance remains native; Kraken continues through the same CCXT base. Generalized CCXT changes require Kraken regression fixtures.

**Automated tests**

- Spot-only and contract rejection fixtures.
- Native/canonical/alias normalization.
- Precision modes, amount/cost limits, inactive markets, and collision handling.
- Cache expiry/forced refresh.
- OHLCV bounds/deduplication/incomplete candles.
- Ticker, book, health, and metadata sync.
- Explicit errors for all private operations.

**Manual validation**

Against the selected regional public API, verify market loading and recent OHLCV/ticker/book for BTC/USDT, ETH/USDT, and one low-priced asset. Confirm every returned instrument is spot and non-contract.

**Rollback**

Disable OKX, clear it as active only when switching blockers are zero, disable exchange schedules, and deploy PR 3. Retain synchronized metadata.

**Completion gate**

Public checks pass without credentials, normalized/native symbols are correct, and no private endpoint can be called.

**Known risks**

- Regional APIs can expose different instruments.
- CCXT market metadata can change between pinned versions.
- OKX OHLCV history depth may differ by timeframe and region.

## OKX PR 5 — Exchange-Specific Backtesting and Rankings

**Objective**

Enable OKX rankings, backtesting, Monte Carlo, and approvals with complete isolation.

**Likely components**

- Backtesting/ranking persistence and subprocess interfaces.
- Market-phase workflow and report pages.
- Backtesting, jobs, and isolation tests.

**Concrete changes**

- Permit public analysis and backtesting only for the production `okx` identity.
- Require an explicit operator-configured OKX taker fee; never infer it from Binance, Kraken, CCXT defaults, or an unauthenticated public response.
- Capture exchange ID/code, adapter variant, fee, quote asset, strategy/settings context, and work fingerprint when queuing work.
- Ensure the adapter variant is included in fingerprints so a regional API change invalidates queued/results context.
- Keep OKX result/trade/job/Monte Carlo uniqueness exchange-scoped.
- Include `okx` in report filenames and display exchange/environment in dashboard output.
- Require matching exchange, symbol, timeframe, strategy, fee, variant, and fingerprint before a result can approve an OKX candidate.
- Replace remaining Binance-specific public-data error/help text with active-exchange wording.
- Never allow `okx_demo` results or Binance/Kraken results to approve OKX production trading.

**Exclusions**

- No private credentials or live orders.
- No reuse or conversion of Binance/Kraken candles or results.
- No strategy changes.

**Binance compatibility**

Existing Binance/Kraken results, fees, fingerprints, filenames, and approvals remain valid.

**Automated tests**

- Cross-exchange and demo/production collision prevention.
- Stale fee, quote, variant, and fingerprint rejection.
- Queue/subprocess context propagation.
- Deterministic commission behavior.
- Rankings, reports, Monte Carlo, candidate approval, and deletion scoped to OKX.
- Unavailable pairs are skipped and reported.

**Manual validation**

Configure the real account-tier taker fee and run representative 1d/4h/1h backtests and rankings. Compare stored exchange, symbol, fee, fingerprint, reports, and trades.

**Rollback**

Disable OKX public schedules and deploy PR 4. Existing OKX results remain isolated and inactive.

**Completion gate**

Tests prove that no result from Binance, Kraken, or OKX demo can influence an OKX production candidate.

**Known risks**

- Actual OKX fees are account/tier-specific.
- Pair availability and candle history can differ from Binance/Kraken.

## OKX PR 6 — Authentication and Read-Only Demo/Production Checks

**Objective**

Add safe credential handling and private balance validation without permitting orders.

**Likely components**

- OKX adapter/client factory and environment loader.
- Private status service and Trading settings UI.
- Credential and mocked private-API tests.

**Concrete changes**

- Read production credentials only from:
  - `OKX_API_KEY`;
  - `OKX_API_SECRET`;
  - `OKX_API_PASSPHRASE`.
- Read separate demo credentials from corresponding `OKX_DEMO_*` variables.
- Never persist, log, display, or include credentials in raw responses.
- Configure CCXT `password` with the passphrase and call sandbox mode before any demo API call.
- Require all three credentials for private availability; partial configuration fails with a safe diagnostic.
- Add read-only private checks for trading-account balance, selected quote asset, spot/cash account compatibility, API reachability, clock/authentication errors, and selected regional variant.
- Display operator guidance requiring Read and Trade only, no Withdraw, and an IP allowlist.
- Treat “no withdrawal permission” as a mandatory operator attestation if it cannot be proven through the API.
- Keep both buy/sell flags false and keep `run_mode=test`.
- Do not infer authorization merely because credentials exist.

**Exclusions**

- No create, cancel, or reconcile order.
- No automatic fee update from private account data.
- No funding-account transfers.

**Binance compatibility**

Existing Binance/Kraken credential names and behavior remain unchanged, aside from the deterministic injection fix from PR 1.

**Automated tests**

- Missing/partial/complete credentials without exposing values.
- Passphrase mapping.
- Demo sandbox activation before API calls.
- Production/demo credential separation.
- Read-only balance and account-mode checks.
- Authentication, permission, region, and clock failure messages.
- Proof that private status checks cannot call an order endpoint.

**Manual validation**

Create keys with Read and Trade only, verify Withdraw is absent, bind IPs, and run balance checks separately in demo and production.

**Rollback**

Remove OKX credentials from runtime, disable both identities, and deploy PR 5.

**Completion gate**

Both credential sets can be checked safely, no secret appears in logs/database/UI, and no order method is reachable.

**Known risks**

- The exchange may not expose enough information to verify key permissions programmatically.
- Account mode and regional/KYC restrictions are operator-specific.

## OKX PR 7 — Mandatory Demo Trading Execution

**Objective**

Validate the complete OKX order lifecycle in an isolated demo identity before production is possible.

**Likely components**

- Generic live workflow and OKX-specific order parameters.
- Run-mode/settings UI and reconciliation.
- Mocked order integration tests and demo rollout documentation.

**Concrete changes**

- Add `run_mode=demo`; it may submit only through the active `okx_demo` identity.
- Require explicit demo buy and sell flags. Enabling the demo exchange or adding credentials does not enable either side.
- Require `tdMode=cash` and reject every margin/contract option.
- Use quote-cost market buys through `create_market_buy_order_with_cost` with explicit quote-currency semantics; do not silently reinterpret quote cost as base quantity.
- Use base quantity for market sells.
- Apply a configurable OKX sizing reserve, defaulting to 5%, particularly for convert-all-balance operations.
- Validate free quote/base balance, active market, precision, amount limits, cost limits, and non-contract metadata immediately before intent persistence/submission.
- Persist the intent and permanent client ID before submission; enforce OKX’s 32-character alphanumeric limit.
- Parse create/fetch/cancel results, partial fills, average price, executed cost, multiple fees, and net quantity.
- On uncertain timeout, mark `unknown`, never retry, and reconcile by client ID/order ID/fills.
- Run reconciliation at startup and every 15 minutes while unsettled demo intents exist.
- Require every demo position to be closed and every intent settled before switching away from demo.
- Store a durable demo validation record containing adapter version, variant, tested symbols, buy/sell order IDs, reconciliation result, and completion timestamp. Store no secrets.

**Exclusions**

- No production order submission.
- No automatic switch from demo to production.
- No simulated fills inside BEC; this uses OKX Demo Trading.

**Binance compatibility**

No Binance/Kraken order path or run mode changes. `run_mode=demo` is rejected for them.

**Automated tests**

- Quote-cost buy and base-amount sell request shapes.
- Client-ID constraints and duplicate prevention.
- Precision/amount/cost/balance rejection.
- Partial fills, late fees, cancellation, known reject, timeout, unknown outcome, and reconciliation.
- Restart reconciliation and idempotent position updates.
- Demo/production persistence and credential isolation.
- Switching blocked by demo positions and unsettled intents.

**Manual validation**

In OKX Demo Trading:

1. Validate balances and account mode.
2. Enable demo buy only and submit one minimum-valid buy.
3. Verify intent, exchange order, fills, fees, average, net quantity, logs, dashboard, and Telegram.
4. Enable demo sell and close the position.
5. Exercise fetch/cancel on a safely cancellable order if supported.
6. Simulate a process restart and verify reconciliation.
7. Confirm no demo positions or unsettled orders remain.

**Rollback**

Disable demo buy/sell and schedules, reconcile all intents, close demo positions, set `run_mode=test`, remove demo credentials, and deploy PR 6.

**Completion gate**

The durable demo validation record is complete, no demo position/order remains unsettled, and production live controls remain inaccessible.

**Known risks**

- Demo behavior and liquidity may differ from production.
- Some account-specific responses may require parser adjustments even after mocked tests.

## OKX PR 8 — Gated Production Spot Execution

**Objective**

Enable production OKX spot orders only after explicit human approval of the demo gate.

**Likely components**

- Production execution gating and Trading UI.
- Operational documentation, logs, dashboard, and Telegram.
- Production-mode safety and regression tests.

**Concrete changes**

- Require all of the following before any production order:
  - active production `okx` identity;
  - completed compatible demo validation record;
  - `run_mode=live`;
  - valid production credentials and private health check;
  - explicit production buy or sell flag;
  - explicitly enabled timeframe schedule;
  - current matching adapter variant;
  - no switching blockers;
  - valid OKX-specific backtest approval for buys.
- Demo completion never copies buy/sell flags or run mode to production.
- Add a typed two-step confirmation when enabling production `run_mode=live` or either live side.
- Never enable live flags, schedules, or live run mode in migrations, startup, credential discovery, health checks, or exchange selection.
- Reuse the validated quote-cost buy, base-quantity sell, intent, fill, cancellation, timeout, duplicate prevention, and reconciliation paths.
- Present exchange/environment/client order/status/fee/reconciliation context in dashboard, logs, and Telegram without secrets or full raw payloads.
- Reconciliation stays enabled whenever an OKX production intent is unsettled, even if new buys/sells are disabled.
- Document an emergency procedure: disable new sides/schedules, do not delete unknown intents, reconcile before manual intervention.

**Exclusions**

- No futures, margin, transfer, withdrawal, deposit, staking, earn, or simultaneous exchanges.
- No automatic balance transfer from funding to trading account.
- No automatic production activation after deployment or demo completion.

**Binance compatibility**

Binance and Kraken live behavior remains unchanged. Shared lifecycle changes are covered by their regression suites.

**Automated tests**

- Every gate independently blocks production submission.
- Migration/startup/selection/credential checks cannot enable live.
- Demo completion cannot arm production.
- Buy approval requires matching OKX exchange/fee/variant/fingerprint.
- Buy-only, sell-only, and both-disabled operation.
- Startup/periodic reconciliation while new submissions are disabled.
- Dashboard, logs, and Telegram contain exchange/environment identity.
- Full exchange adapter, runtime, database, backtesting, scheduler, trading, and Telegram suites.

**Manual validation**

After human approval:

1. Reconfirm Read+Trade only, no Withdraw, IP allowlist, account mode, region, quote asset, fee, and sizing.
2. Enable only the required side and one timeframe.
3. Execute one minimum-valid production buy.
4. Verify exchange, database intent/fills, balance, position, fee, average, net quantity, logs, dashboard, and Telegram.
5. Enable sell and close the exact position.
6. Verify final balance/PnL and zero unsettled intents.
7. Only then consider unattended operation.

**Rollback**

Disable production buy/sell, all `main_*` schedules, and `run_mode=live`; reconcile every unsettled intent and close or explicitly hand off any position. Remove credentials and deploy PR 7. Additive schema remains.

**Completion gate**

A controlled minimum-valid production round trip is reconciled end-to-end, all evidence is reviewed, and unattended execution requires a separate explicit operator decision.

**Known risks**

- Real liquidity, fees, regional restrictions, rate limits, and response timing differ from demo.
- Market-order cost reservation can cause rejection near the available balance.
- Manual action during an `unknown` outcome can create an untracked duplicate unless reconciliation is completed first.

## Recommended Order and Dependencies

1. PR 1 — remaining exchange-code generalization.
2. PR 2 — durable fills and generic reconciliation.
3. PR 3 — disabled OKX configuration and identities.
4. PR 4 — public markets and metadata.
5. PR 5 — isolated backtesting and rankings.
6. PR 6 — credentials and read-only checks.
7. PR 7 — mandatory demo execution.
8. PR 8 — production live gate.

Dependencies are strictly linear. PRs must not be stacked or implemented together. PR 8 cannot begin until PR 7’s demo completion gate has been reviewed by a human.

The first PR that can be implemented safely is **OKX PR 1** because it introduces no OKX registration, credentials, schema-backed trading state, or new live behavior.

## Human Approvals Required

- Confirm the EEA `myokx` default or select global `okx` before PR 3 is applied.
- Confirm `USDT` as the initial quote asset after checking the actual regional market catalog.
- Enter and approve the actual account-tier spot taker fee; no code default is authorized.
- Approve the 5% OKX market-buy sizing reserve or provide a safer tested value.
- Review API key permissions and attest that Withdraw is absent.
- Approve the chosen demo and production symbols and minimum valid order sizes.
- Review the PR 7 demo validation record.
- Separately approve PR 8 and each controlled production buy/sell activation.
- Approve unattended production operation only after the controlled round trip.

## Critical Risks

- Current Kraken credential loading is environment-dependent and must not be copied.
- Existing execution and reconciliation are only partially generic despite the adapter boundary.
- Current aggregate fee persistence cannot represent multiple fee assets safely.
- OKX quote-cost/base-quantity semantics can create materially incorrect market buys if implicit.
- Demo and production data would collide if represented by the same exchange identity.
- Regional `okx`/`myokx` selection can affect authentication and market availability.
- An ambiguous timeout must never cause an automatic resubmission.
- Changing CCXT versions can alter precision, sandbox, order, and symbol behavior; dependency upgrades require separate review.

## Questions Requiring Runtime or Operator Inspection

- Which OKX regional account/API variant is valid for the deployment?
- Which spot pairs and quote currencies are available to that exact account and region?
- What is the actual maker/taker fee tier?
- Is the account configured for compatible spot/cash trading?
- Are the API keys limited to Read and Trade, without Withdraw, and IP-bound?
- Is Demo Trading/API access enabled for the account?
- What exact fill/fee payloads are returned for the selected account, symbols, and market-buy mode?
- What schema/migration state and unresolved symbols exist in the real production database copy?
- What minimum production order sizes are acceptable to the operator?

These questions must be answered during the PR-specific manual gates; none should be guessed by implementation code.
