# Backtesting

## What This Page Is For

Use this page to configure backtests, run queued jobs, and read the results without needing to inspect the database.

## Backtest Settings

Open **Backtesting > Backtest Settings** before running new tests. Review:

- cash and commission assumptions;
- optimization metric;
- Buy & Hold benchmark mode;
- market-phase filters;
- Strategy Quality Score weights;
- approval rules and grade scale.

These settings affect future backtest runs and the approval status used by trading workflows.

## How Backtesting Works

BEC runs strategy tests against historical OHLCV candles for each selected symbol, timeframe, and strategy.

The backtest uses the current settings for:

- starting cash and commission;
- stop-loss, ATR trailing stop, and take-profit rules;
- market-phase filters;
- strategy parameters;
- approval rules.

After each run, BEC stores the result in the database and can generate an HTML report. The dashboard then uses those stored results to approve or reject candidates for trading.

Backtesting is queued. When you select rows in **Backtesting Results** and start a run, the dashboard adds jobs to the queue and the background `jobs_runner` executes them one by one. Use the queue panel to check progress and logs.

## Backtesting Results

Open **Backtesting > Backtesting Results** to filter, select, and queue backtests.

Use the grid to compare:

- return and drawdown;
- trade count and win rate;
- profit factor, expectancy, SQN, and Kelly criterion;
- Strategy Quality Score and grade;
- trading approval status and rejection reasons.

## Strategy Quality Score

The Strategy Quality Score is a 0-100 score designed to avoid ranking strategies by return alone.

It combines five components:

| Component | Default Weight | What It Rewards |
| --- | ---: | --- |
| Return | 20% | Total return, annual return, and performance versus Buy & Hold |
| Risk | 25% | Lower drawdown, shorter drawdown duration, and better Calmar ratio |
| Risk-adjusted | 20% | Better Sharpe and Sortino ratios |
| Trade quality | 20% | Profit factor, expectancy, SQN, win rate, and a reasonable trade count |
| Robustness | 15% | Healthy trade count, balanced exposure, low commission drag, less dependence on one winning trade, and controlled drawdown |

You can change these weights in **Backtesting > Backtest Settings > Strategy Quality Score**. The weights must add up to 100%.

## Penalties

After the weighted score is calculated, BEC can subtract penalties. Penalties are capped at 30 points.

Common penalty reasons include:

- too few trades;
- drawdown above the expected range;
- too little or too much market exposure;
- high commission drag;
- too much dependency on a single winning trade;
- weak performance versus Buy & Hold combined with high drawdown.

The final Quality Score is:

```text
weighted component score - penalties
```

The final score is always kept between 0 and 100.

## Quality Grade

The grade is a simpler label based on the final Quality Score:

| Grade | Quality Score | Meaning |
| --- | ---: | --- |
| A | >= 85 | Excellent |
| B | 70 - 84.99 | Strong |
| C | 55 - 69.99 | Acceptable / moderate |
| D | 40 - 54.99 | Weak |
| F | < 40 | Rejectable |

Approval rules can use the grade. For example, a default `Quality_Grade_Min` of `C` keeps Grade A, B, and C backtests and rejects D and F.

## Backtest Reports

Select one row to render the HTML report. Reports include charts, statistics, configuration, trades, and risk/exit information.

The project also publishes an example report:

[Open the example backtesting report](./BTCUSDC-ema_cross_with_market_phases-1d.html)

## AI Strategy Analysis

AI analysis is available when `OPENAI_API_KEY` is configured. Use it as a review aid, not as a trading signal. It summarizes strengths, risks, recommended tests, and data quality notes.
