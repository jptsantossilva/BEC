# Dashboard

## What This Page Is For

Use this page to understand the main Streamlit dashboard areas.

## Trading > Dashboard

This is the daily operating view. Use it to monitor:

- realized and unrealized PnL;
- open positions by timeframe;
- top performers;
- signal logs;
- blacklist entries;
- trading settings, strategies, position sizing, risk controls, take-profit levels, and Telegram settings.

Use manual position actions carefully. They change live position records and can affect future bot decisions.

## Daily Top Performers

BEC rebuilds the top performers list through the scheduled `symbol_by_market_phase.py` job. This job is normally run on the daily timeframe.

Each run:

1. creates a daily balance snapshot;
2. refreshes BTC strategy backtests used by the trade-against auto-switch logic;
3. checks whether the quote asset should switch between stablecoin and BTC, if auto-switch is enabled;
4. loads Binance symbols for the configured quote asset, excluding blacklisted symbols and leveraged `UP`/`DOWN` pairs;
5. fetches historical close prices for each symbol;
6. calculates `DSMA50`, `DSMA200`, `% above DSMA50`, and `% above DSMA200`;
7. classifies each symbol into a market phase;
8. keeps only symbols in `accumulation` or `bullish` phase;
9. ranks them by `% above DSMA200`, highest first;
10. stores the top ranked symbols and sends a Telegram summary.

## Market Phase Classification

BEC uses the latest daily price, 50-day simple moving average, and 200-day simple moving average.

| Phase | Condition |
| --- | --- |
| Recovery | `Price > DSMA50`, `Price < DSMA200`, `DSMA50 < DSMA200` |
| Accumulation | `Price > DSMA50`, `Price > DSMA200`, `DSMA50 < DSMA200` |
| Bullish | `Price > DSMA50`, `Price > DSMA200`, `DSMA50 > DSMA200` |
| Warning | `Price < DSMA50`, `Price > DSMA200`, `DSMA50 > DSMA200` |
| Distribution | `Price < DSMA50`, `Price < DSMA200`, `DSMA50 > DSMA200` |
| Bearish | `Price < DSMA50`, `Price < DSMA200`, `DSMA50 < DSMA200` |

Only `accumulation` and `bullish` symbols become top performer candidates for trading. The final ranking favors symbols trading furthest above their 200-day moving average, and the Telegram report summarizes the selected top performers.

## Market Analysis

Use **Market Analysis > Dashboard** to review consolidated bull and bear market indicator summaries.

Use **Market Analysis > BTC Supply Profit/Loss** to inspect the detailed Bitcoin Percentage of Supply in Profit and Loss chart.

For details about market analysis indicators, data sources, scheduled updates, and safety boundaries, see [Market Analysis](/market-analysis).

## Trading > Balances

Use **Balances** to review daily balance snapshots and asset balances. This page helps confirm whether Binance balances match the bot's internal view.

## Trading > Scheduled Jobs

Use **Scheduled Jobs** to enable or disable background jobs. The schedule runs in UTC.

Typical jobs include:

- trading bot runs for `1d`, `4h`, and `1h`;
- market-phase ranking rebuilds;
- signal checks.

Disabling a timeframe stops new entries for that timeframe, but existing positions can still be checked for exits.

## Telegram

Telegram credentials are configured in `.env` and in dashboard settings. Use Telegram alerts to track bot runs, errors, position changes, and signal notifications.
