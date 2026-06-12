# Market Analysis

## What This Page Is For

Use this page to understand BEC market analysis indicators. These pages are for macro context and signal review, not for direct trade execution.

## Market Indicators Dashboard

Open **Market Analysis > Dashboard** to see a consolidated view of available market indicators.

The dashboard separates indicators into:

- **Top Signals** for cycle-top or risk conditions;
- **Bottom Signals** for stress, capitulation, or recovery-watch conditions.

Each grid shows the current value, reference condition, signal state, distance to hit, progress, status, and a link to the detailed indicator page.

`Progress` means proximity to the configured signal condition. It is not a trading score and should not be read as a buy or sell instruction.

## BTC Supply Profit/Loss

Open **Market Analysis > BTC Supply Profit/Loss** to review Bitcoin Percentage of Supply in Profit and Loss.

The chart shows:

- BTC price;
- percentage of supply in profit;
- percentage of supply in loss;
- event markers for top, extreme top, bottom, and profit/loss crossover conditions.

The indicator uses normalized daily rows stored in the local SQLite database. The page reads from local cached data, so previously stored history is preserved if the external data source is unavailable or rate limited.

## Data Source

BTC Supply Profit/Loss uses Bitview/Bitcoin Research Kit as a public external data source at `https://bitview.space`.

The API does not require authentication. BEC does not assume any service-level agreement for availability, freshness, or continuity of this external service.

## Scheduled Updates

The optional `btc_supply_profit_loss_1d` scheduled job updates the local BTC Supply Profit/Loss cache and can send informational Telegram alerts when configured.

The job is disabled by default. If it remains disabled, existing cached history is preserved but new data is not updated automatically.

## Signal Meaning

BTC Supply Profit/Loss currently contributes two rows to the Market Indicators dashboard:

- **BTC Supply Profit/Loss - Top** uses the configured extreme top threshold. Progress starts from the neutral `50%` Supply in Profit level and reaches `100%` at the extreme top threshold.
- **BTC Supply Profit/Loss - Bottom** activates when Supply in Loss is greater than or equal to Supply in Profit. Progress tracks proximity to that crossover.

These signals are informational macro/on-chain alerts only. They do not create or modify Binance orders, automatic trade execution, position sizing, stop loss, take profit, symbol selection, backtesting, balances, risk management, or strategy logic.
