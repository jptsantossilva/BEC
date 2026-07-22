# OKX Demo Trading Rollout

OKX Demo Trading is the prerequisite evidence for the separately gated
production `okx` identity. It is not paper trading and it never copies a
demo run mode, buy/sell side, schedule, or credential into production.

## Prerequisites

- Configure only `OKX_DEMO_API_KEY`, `OKX_DEMO_API_SECRET`, and
  `OKX_DEMO_API_PASSPHRASE` in the deployment environment.
- Use a dedicated key with the minimum read/trade permissions, no withdrawal
  permission, and an IP allowlist. The operator must attest that withdrawal is
  disabled.
- Confirm the account is OKX Demo Trading and the selected adapter variant
  (`myokx` or `okx`) matches the account region.
- Leave production `OKX_*` credentials unused for this rollout.

## Controlled Round Trip

1. Keep `run_mode=test` and run the read-only demo balance check.
2. Enable and select `OKX (Demo)`. Confirm its quote asset and public market
   metadata.
3. Enable Demo run mode, one side, and one timeframe only.
4. For a controlled validation, use **Controlled Manual Demo Orders** in
   Trading Settings. It is available only for the active `okx_demo` identity,
   requires a confirmation, and still uses the normal guarded lifecycle.
   Submit one minimum-valid quote-cost buy. Verify the durable intent, client
   order id, exchange order, fills, fees, average price, net quantity, logs,
   dashboard, and Telegram.
5. Enable the sell side, use the selected open demo position in the same panel
   to submit an explicit-price immediate-or-cancel (IOC) spot limit sell.
   Refresh the public best bid, use that price or a lower one, and confirm it
   before submission. BEC never falls back to an unbounded market sell. A
   canceled or partially filled IOC order must be reconciled before an operator
   considers another confirmed attempt.
6. Reconcile orders after a restart or through the dashboard. Do not manually
   replace an `unknown` intent.
7. When no positions or unsettled intents remain, explicitly record the OKX
   Demo validation in Trading Settings.

## Rollback

Disable demo buy/sell flags and all `main_*` schedules, reconcile every
unsettled intent, close demo positions, set `run_mode=test`, remove demo
credentials, and deploy the previous release. The migration is additive and
the validation record remains as audit evidence.

## Production Spot Gate

Production spot execution requires all of the following at submission time:

- the active `okx` production identity, with the same adapter variant and
  quote asset represented by a completed demo validation record;
- `run_mode=live`, enabled explicitly through the typed confirmation in
  Trading Settings;
- a separately typed confirmation before enabling either production buy or
  sell, an explicit fee, and an enabled `main_*` timeframe;
- a successful production private balance preflight using the selected
  trading/cash account; and
- for buys, an approved backtest whose exchange, adapter, quote asset, fee,
  and work fingerprint match the active production identity.

The dashboard never enables any of these settings automatically. Production
sells remain ordinary market sells, preserving BEC's historical behavior:
they may fill below the expected price in thin liquidity. Every order retains
a durable intent before submission; unknown outcomes are reconciled and never
automatically resubmitted.

### Production Emergency Procedure

1. Disable production buy and sell flags and every `main_*` schedule.
2. Set `run_mode=test` using the production live-mode disable control.
3. Do not delete or replace pending, open, partially filled, or unknown
   intents.
4. Run **Reconcile OKX Production Orders** and inspect the order/fill state on
   OKX before any manual action.
5. Reconcile final balances, positions, fees, logs, dashboard, and Telegram;
   remove production credentials only after the operational handoff is clear.
