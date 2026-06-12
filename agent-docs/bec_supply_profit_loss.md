# BTC Supply in Profit/Loss: Bitview-Only Implementation Plan

## Summary
Implement BTC Supply in Profit/Loss using Bitview as the only on-chain source and
the local SQLite table as the only data source for the Streamlit page.

This remains a macro/on-chain informational indicator only. It must not change
live trading, order execution, sizing, Binance access, strategies, backtesting,
balances, stop loss, take profit, or risk management.

CSV support is intentionally removed. Do not load, import, fallback to, or test
`btc_supply_profit.csv` or any other CSV path for this feature.

## Source And Data Model
- Use Bitview API base URL `https://bitview.space/api`.
- Allow only the public host `https://bitview.space`; do not make requests to
  user-provided hosts or arbitrary URLs for this feature.
- Required daily series:
  - `price`
  - `supply_in_profit_share`
  - `supply_in_loss_share`
- Optional audit/hover series:
  - `supply_in_profit`
  - `supply_in_loss`
- Fetch historical/recent data with:
  - `/series/{series_name}/day?start=-365` for initial backfill.
  - `/series/{series_name}/day?start=-10` for daily updates.
- Restrict daily series requests to:
  - `/api/series/price/day`
  - `/api/series/supply_in_profit_share/day`
  - `/api/series/supply_in_loss_share/day`
  - `/api/series/supply_in_profit/day`
  - `/api/series/supply_in_loss/day`
- Do not rely only on `/latest`; fetching a recent window allows late revisions
  and keeps all series aligned.
- Bitview/BRK is a public external source with no authentication and no BEC SLA.
- Treat `*_share` values as percentages in the 0-100 range.
- Build dates from the payload `stamp` and array length, preferring the last
  complete UTC day when the stamp date could point to the current incomplete day.
- Store `retrieved_at` from Bitview `stamp` for audit.

## Database Changes
Extend the existing `Onchain_Btc_Supply_Profit_Loss` table without replacing it.
Preserve current rows where possible.

Target normalized columns:
```text
date
btc_price
percent_supply_in_profit
percent_supply_in_loss
supply_in_profit_btc
supply_in_loss_btc
source
retrieved_at
created_at
updated_at
```

Implementation requirements:
- Add missing nullable columns with safe migrations:
  `supply_in_profit_btc`, `supply_in_loss_btc`, `retrieved_at`, `created_at`.
- Use upsert by date.
- Keep `Onchain_Signal_Alerts_Sent` for deduplication by
  `signal_name`, `event_type`, `event_date`.
- Remove the obsolete `onchain_supply_profit_loss_csv_path` setting.

## Implementation Changes
- Update `bec/market_indicators/supply_profit_loss.py` with Bitview helpers:
  - `fetch_bitview_series(series_name, days=365, session=None)`.
  - `dates_from_bitview_payload(payload)`.
  - `normalize_bitview_series(payloads)`.
  - `backfill_last_365_days()`.
  - `update_latest_day(days=10)`.
  - `run_btc_supply_profit_loss_update_job()`.
- Validate Bitview payloads strictly:
  - payload must be a JSON object.
  - required series must exist and contain non-empty `data` arrays.
  - required series lengths must match.
  - price must be positive.
  - percentages must be within 0-100.
  - optional absolute BTC series may fail or be missing; persist `NULL`.
- Handle HTTP/network errors conservatively:
  - set timeout.
  - do not crash dashboard or scheduler if Bitview is unavailable.
  - log the failure and keep using existing table data.
  - treat HTTP 429 as a normal external-rate-limit failure; do not retry in a
    tight loop.
- Add CLI support:
  - `python -m bec.market_indicators.supply_profit_loss --backfill 365`
  - `python -m bec.market_indicators.supply_profit_loss --update-latest`

## Scheduler And Settings
- Keep/add schedule `btc_supply_profit_loss_1d`, disabled by default.
- Schedule script:
  `bec/market_indicators/supply_profit_loss.py`
- Daily job behavior:
  - if local history is empty, backfill full history.
  - otherwise fetch only missing days since the latest cached date.
  - upsert rows.
  - load latest table data.
  - detect events only for the latest available date.
  - send deduplicated Telegram alerts.
- Settings:
  - `onchain_supply_profit_loss_source = bitview`
  - `onchain_supply_profit_loss_backfill_days = 365`
  - `onchain_supply_profit_loss_update_days = 10`
  - `onchain_supply_profit_loss_top_threshold = 95.0`
  - `onchain_supply_profit_loss_extreme_top_threshold = 98.0`
  - `onchain_supply_profit_loss_bottom_threshold = 5.0`
  - `onchain_supply_profit_loss_cross_tolerance = 1.0`
  - `onchain_supply_profit_loss_send_telegram_alerts = true`

## Streamlit Page
- Keep `pages/bitcoin_supply_profit_loss.py`.
- The chart must read exclusively from `Onchain_Btc_Supply_Profit_Loss`.
- If the table is empty:
  - show a clear empty state.
  - if the scheduled job is enabled, run the update job with a spinner.
  - if the scheduled job is disabled, show a message explaining that automatic
    updates are disabled.
  - do not show CSV controls.
- Show latest data date, BTC price, and supply in profit/loss percentages.
- Controls:
  - range: 30d, 90d, 180d, 365d, all.
  - show BTC price.
  - price scale: log/linear, default log.
  - show absolute BTC supply values.
  - alert thresholds persisted in `Settings`, including cross tolerance.
- Graph:
  - BTC price on left axis.
  - `% Supply in Profit` and `% Supply in Loss` on right axis.
  - horizontal lines at 5, 50, 95, 98.
  - markers only on the first day of each continuous event regime.
- Keep the visible disclaimer:
  `Macro/on-chain signal only. This is not an automatic buy/sell order.`

## Events And Telegram
- Reuse `telegram.telegram_token_signals` and
  `telegram.telegram_prefix_signals_sl`; do not add a new Telegram token/chat ID.
- Event rules:
  - `SUPPLY_PROFIT_TOP_ZONE`: profit >= 95.
  - `SUPPLY_PROFIT_EXTREME_TOP_ZONE`: profit >= 98.
  - `SUPPLY_PROFIT_BOTTOM_ZONE`: profit <= 5 or loss >= 95.
  - `SUPPLY_PROFIT_LOSS_CROSS_50`: profit or loss is within tolerance of 50.
- `EXTREME_TOP` alerts as its own transition when profit crosses 98, even inside
  an already active `TOP_ZONE`.
- Default cross tolerance: 1.0 percentage point.
- Deduplicate alerts by `signal_name`, `event_type`, `event_date`.
- Alert text must include event type, date, BTC price, supply in profit/loss
  percentages, and the macro/on-chain disclaimer.

## Tests
Add/extend tests without network access:
- Bitview payload parsing.
- Date reconstruction from `stamp`, preferring the last complete UTC day.
- Normalization of aligned required series.
- Failure when required series lengths differ.
- Optional absolute BTC series missing or failing leaves nullable fields.
- HTTP 429 handling does not crash and does not corrupt the local table.
- Upsert updates an existing date without duplication.
- Event detection for top, extreme top, bottom, and cross near 50.
- `EXTREME_TOP` alerts as a separate transition inside `TOP_ZONE`.
- Telegram deduplication uses `Onchain_Signal_Alerts_Sent`.
- Page/data helpers read only SQLite and never CSV.

Suggested test files:
- `tests/unit/test_supply_profit_loss.py`
- `tests/unit/test_supply_profit_loss_bitview.py`

## Validation
Run:
```bash
.venv/bin/python -m pytest tests/unit/test_supply_profit_loss.py
.venv/bin/python -m pytest tests/unit/test_supply_profit_loss_bitview.py
.venv/bin/python -m pytest tests
.venv/bin/python -m py_compile bec/*.py bec/utils/*.py bec/exchanges/*.py bec/signals/*.py bec/market_indicators/*.py
```

Manual checks:
```bash
.venv/bin/python -m bec.market_indicators.supply_profit_loss --backfill 365
.venv/bin/python -m bec.market_indicators.supply_profit_loss --update-latest
.venv/bin/streamlit run dashboard.py --server.port=8080
```

Verify:
- backfill creates approximately 365 normalized daily rows.
- update-latest upserts recent rows without duplicates.
- page works offline after table is populated.
- alerts are not resent for the same event/date.
- no trading behavior changes.
