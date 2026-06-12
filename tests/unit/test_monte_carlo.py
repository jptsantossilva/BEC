import numpy as np
import pandas as pd

from bec.utils import monte_carlo


def test_trade_metrics_handles_zero_trades():
    metrics = monte_carlo._trade_metrics([10000.0], np.array([]))

    assert metrics["Net Profit"] == 0.0
    assert metrics["Max Drawdown"] == 0.0
    assert metrics["Total Trades"] == 0.0


def test_pnl_metrics_reconstructs_equity_without_compounding_trade_return_pct():
    metrics = monte_carlo._pnl_metrics([10000.0, 11000.0, 12174.0], np.array([1000.0, 1174.0]))

    assert round(metrics["Net Profit"], 2) == 21.74
    assert metrics["Total Trades"] == 2.0


def test_metric_percentiles_order_profit_and_drawdown():
    original = {
        "Net Profit": 10.0,
        "Max Drawdown": 20.0,
        "Sharpe Ratio": 1.0,
        "Win Rate": 50.0,
        "Total Trades": 4.0,
        "Annual Return": 10.0,
        "Calmar Ratio": 0.5,
        "Expectancy": 2.5,
    }
    scenarios = [
        {**original, "Net Profit": -10.0, "Max Drawdown": 50.0},
        {**original, "Net Profit": 5.0, "Max Drawdown": 25.0},
        {**original, "Net Profit": 20.0, "Max Drawdown": 10.0},
        {**original, "Net Profit": 40.0, "Max Drawdown": 5.0},
    ]

    metrics = monte_carlo._summarize_metrics(original, scenarios)

    assert metrics["Net Profit"]["worst_5"] < metrics["Net Profit"]["median"]
    assert metrics["Net Profit"]["best_5"] > metrics["Net Profit"]["median"]
    assert metrics["Max Drawdown"]["worst_5"] > metrics["Max Drawdown"]["median"]
    assert metrics["Max Drawdown"]["best_5"] < metrics["Max Drawdown"]["median"]


def test_perturb_candles_generates_valid_ohlc():
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0, 102.0, 101.5, 103.0],
            "High": [102.0, 103.0, 103.5, 104.0, 105.0],
            "Low": [99.0, 100.0, 100.5, 100.0, 102.0],
            "Close": [101.0, 102.0, 101.5, 103.0, 104.0],
            "Volume": [10.0, 11.0, 12.0, 13.0, 14.0],
        }
    )

    synthetic = monte_carlo._perturb_candles(df, np.random.default_rng(42))

    assert len(synthetic) == len(df)
    assert (synthetic["High"] >= synthetic[["Open", "Close"]].max(axis=1)).all()
    assert (synthetic["Low"] <= synthetic[["Open", "Close"]].min(axis=1)).all()
    assert (synthetic[["Open", "High", "Low", "Close"]] > 0).all().all()


def test_perturb_candles_uses_configured_percent_bounds():
    df = pd.DataFrame(
        {
            "Open": [100.0, 110.0, 120.0],
            "High": [105.0, 115.0, 125.0],
            "Low": [95.0, 105.0, 115.0],
            "Close": [102.0, 112.0, 122.0],
            "Volume": [10.0, 11.0, 12.0],
        }
    )

    synthetic = monte_carlo._perturb_candles(
        df,
        np.random.default_rng(42),
        min_pct=0.1,
        max_pct=0.5,
    )

    deltas = (
        synthetic[["Open", "High", "Low", "Close"]]
        / df[["Open", "High", "Low", "Close"]]
        - 1.0
    ).abs()
    assert (deltas >= 0.001 - 1e-12).all().all()
    assert (deltas <= 0.005 + 1e-12).all().all()
    assert synthetic["Volume"].equals(df["Volume"])


def test_candle_perturbation_bounds_normalize_reversed_values():
    min_bound, max_bound = monte_carlo._candle_perturbation_bounds(
        {
            "Monte_Carlo_Candle_Perturb_Min_Pct": 0.5,
            "Monte_Carlo_Candle_Perturb_Max_Pct": 0.1,
        }
    )

    assert min_bound == 0.001
    assert max_bound == 0.005


def test_robustness_interpretation_for_no_valid_scenarios():
    assert monte_carlo._robustness_score({}, 0, 100) == 0.0
    assert monte_carlo._interpretation(0.0, 0, 100, monte_carlo.METHOD_CANDLES) == "Insufficient scenarios"
