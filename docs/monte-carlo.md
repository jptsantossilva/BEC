# Monte Carlo Analysis

## What This Page Is For

Use this page to understand what the Monte Carlo robustness tests measure.

## Trade-Order Shuffling

This method uses the existing backtest trades and randomly changes their order.

Use it to ask:

- did the strategy depend on a lucky sequence of trades?
- would drawdown look worse if wins and losses arrived in a different order?

It is fast because it does not rerun the strategy on candles.

## Candles-Based

This method creates synthetic OHLCV market paths from the original candle history and reruns the full backtest for each path.

For each scenario, BEC:

1. calculates historical close-to-close returns;
2. samples those returns in a new order, with repetition;
3. adds small statistical noise equal to 15% of historical return volatility;
4. rebuilds valid Open, High, Low, Close, and Volume candles;
5. runs the selected strategy again on that synthetic path.

It does not edit the original candles. It creates new DataFrames for each scenario.

## Robustness Score

The score compares the original result with the scenario distribution. It considers median return, worst 5% return, worst drawdown, and how many scenarios were valid.

Common interpretations:

- **Robust**: scenario results remain close enough to the original.
- **Moderate robustness**: usable signal, but not strongly stable.
- **Sequence-sensitive**: trade order has a large effect.
- **Market-path fragile**: candles-based scenarios degrade the result heavily.
- **Insufficient scenarios**: too few valid scenarios to trust the result.

Use Monte Carlo as a filter, not as proof that a strategy will work live.
