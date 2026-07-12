# OKX Demo Trading Rollout

OKX execution is restricted to the `okx_demo` identity in this release.
Production `okx` credentials and controls cannot submit, fetch, or cancel
orders. This is a mandatory demo-validation gate, not paper trading.

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
4. Submit one minimum-valid quote-cost buy. Verify the durable intent, client
   order id, exchange order, fills, fees, average price, net quantity, logs,
   dashboard, and Telegram.
5. Enable the sell side, close the exact demo position, and verify the same
   lifecycle evidence.
6. Reconcile orders after a restart or through the dashboard. Do not manually
   replace an `unknown` intent.
7. When no positions or unsettled intents remain, explicitly record the OKX
   Demo validation in Trading Settings.

## Rollback

Disable demo buy/sell flags and all `main_*` schedules, reconcile every
unsettled intent, close demo positions, set `run_mode=test`, remove demo
credentials, and deploy the previous release. The migration is additive and
the validation record remains as audit evidence.
