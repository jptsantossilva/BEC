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

The **MC Score** is a 0-100 robustness score. It compares the original result with the Monte Carlo scenario distribution.

The score uses three components:

| Component | Weight | What It Measures |
| --- | ---: | --- |
| Median return | 55% | Whether the typical Monte Carlo scenario keeps returns close to the original backtest |
| Worst 5% return | 25% | Whether the lower-tail scenarios still hold up |
| Drawdown | 20% | Whether the worst 5% drawdowns are controlled versus the original drawdown |

The final score is adjusted by the valid scenario ratio:

```text
MC Score =
(
  median return component * 55%
  + worst 5% return component * 25%
  + drawdown component * 20%
)
* valid scenarios / total scenarios
```

For example, if BEC runs 200 scenarios and all 200 are valid, the valid scenario ratio is `1.0`. If only 120 of 200 are valid, the score is multiplied by `0.6`.

### Components

The **median return component** compares the median scenario return with the original return. If the median scenario return is much lower than the original backtest, the MC Score drops.

The **worst 5% return component** looks at the lower tail of the scenario distribution. This penalizes strategies that only look strong in the original path but break down in weaker synthetic paths.

The **drawdown component** compares the original drawdown with the worst 5% scenario drawdown. If Monte Carlo scenarios create much larger drawdowns than the original backtest, the score drops.

All components are capped between 0 and 100 before weighting.

Common interpretations:

| Interpretation | Rule | Meaning |
| --- | --- | --- |
| **Robust** | Score >= 75 | Scenario results remain close enough to the original |
| **Moderate robustness** | Score >= 55 and < 75 | Usable signal, but not strongly stable |
| **Sequence-sensitive** | Score < 55 for trade-order shuffling | Trade order has a large effect |
| **Market-path fragile** | Score < 45 for candles-based tests | Synthetic market paths degrade the result heavily |
| **Insufficient scenarios** | Too few valid scenarios or insufficient trade sample | The result should not be trusted |

The MC Score is not the same as the Backtesting **Strategy Quality Score**. MC Score is only about robustness under Monte Carlo scenarios; Strategy Quality Score combines return, risk, trade quality, and other backtest quality factors.

Use Monte Carlo as a filter, not as proof that a strategy will work live.
