import pandas as pd
from pandas import DataFrame, Series
import sys
import os
import json
import re
import argparse
import builtins
import html
from numbers import Real
from datetime import date
from itertools import product
from types import SimpleNamespace
from dateutil.relativedelta import relativedelta
import time
import logging
import timeit
import numpy as np
import plotly.graph_objects as go

from backtesting import Backtest, Strategy
from backtesting.lib import FractionalBacktest
from backtesting.lib import crossover

import bec.utils.telegram as telegram
import bec.utils.database as database
from bec.utils.strategy_quality_score import calculate_strategy_quality_score

# import bec.utils.config as config
import bec.exchanges.binance as binance
from bec.strategy_builder import engine as strategy_engine
from bec.strategy_builder import schema as strategy_schema
from bec.utils.take_profit import normalize_take_profit_levels, take_profit_enabled

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "symbol_by_market_phase.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p -",
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PERSIST_DIR = os.path.join(PROJECT_ROOT, "persist")
FOLDER_BACKTEST_RESULTS_URL = "static/backtest_results"
FOLDER_BACKTEST_RESULTS = (
    os.path.join(PERSIST_DIR, "backtest_results")
    if os.path.isdir(PERSIST_DIR)
    else os.path.join(PROJECT_ROOT, FOLDER_BACKTEST_RESULTS_URL)
)
FOLDER_BACKTEST_RESULTS_FALLBACK = os.path.join(
    PROJECT_ROOT, FOLDER_BACKTEST_RESULTS_URL
)
DEFAULT_OPTIMIZATION_MAX_COMBINATIONS = 300

# backtest with 4 years of price data
# -------------------------------------
today = date.today()
# today - 4 years - 200 days (DSMA200)
pastdate = today - relativedelta(years=4) - relativedelta(days=200)
# print(pastdate)
tuple = pastdate.timetuple()
timestamp = time.mktime(tuple)

startdate = str(timestamp)
# startdate = "15 Dec, 2018 UTC"
# startdate = "12 May, 2022 UTC"
# startdate = "4 year ago UTC"
# startdate = "10 day ago UTC"
# -------------------------------------
timeframe = ""


def EMA(values, n):
    """
    Return exp moving average of `values`, at
    each step taking into account `n` previous values.
    """
    return pd.Series(values).ewm(span=n, adjust=False).mean()


def SMA(values, n):
    """
    Return simple moving average of `values`, at
    each step taking into account `n` previous values.
    """
    return pd.Series(values).rolling(n).mean()


def WMA(values, n):
    """
    Return weighted moving average of `values`.
    """
    n = max(int(n), 1)
    weights = np.arange(1, n + 1)
    return (
        pd.Series(values)
        .rolling(n)
        .apply(
            lambda prices: np.dot(prices, weights) / weights.sum(),
            raw=True,
        )
    )


def HMA(values, n):
    """
    Return Hull moving average of `values`.
    """
    n = max(int(n), 1)
    half_n = max(int(n / 2), 1)
    sqrt_n = max(int(np.sqrt(n)), 1)
    values = pd.Series(values)
    return WMA(2 * WMA(values, half_n) - WMA(values, n), sqrt_n)


def RSI(values, n):
    """
    Return Wilder RSI of `values`.
    """
    n = max(int(n), 1)
    close = pd.Series(values)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(100).where(avg_loss != 0, 100)


def LINREG(values, n):
    """
    Return the rolling linear regression value at the last point of each window.
    """
    n = max(int(n), 1)
    x = np.arange(n, dtype=float)
    x_mean = x.mean()
    denominator = ((x - x_mean) ** 2).sum()

    def _linreg_endpoint(y):
        y_mean = y.mean()
        slope = (
            ((x - x_mean) * (y - y_mean)).sum() / denominator if denominator else 0.0
        )
        intercept = y_mean - slope * x_mean
        return intercept + slope * (n - 1)

    return pd.Series(values).rolling(n).apply(_linreg_endpoint, raw=True)


def ATR(high, low, close, n):
    """
    Return Average True Range using simple rolling mean.
    """
    high = pd.Series(high)
    low = pd.Series(low)
    close = pd.Series(close)
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    n = max(int(n), 1)
    return tr.rolling(n, min_periods=n).mean()


def _identity_indicator(values):
    return np.asarray(values, dtype=float)


def _declarative_indicator_params(operand: dict, parameters: dict) -> dict:
    try:
        return strategy_engine._indicator_params(operand, parameters)
    except Exception:
        params = (
            operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
        )
        if operand.get("period") is not None:
            params = {**params, "period": operand.get("period")}
        return params


def _declarative_indicator_plot_label(
    operand: dict, parameters: dict, base_timeframe: str = "current"
) -> str:
    name = str(operand.get("name", "") or "").upper()
    params = _declarative_indicator_params(operand, parameters)
    period = params.get("period")
    output = str(operand.get("output", "value") or "value")
    timeframe = str(operand.get("timeframe", "current") or "current")
    label = name
    if period not in (None, ""):
        try:
            label += f" {int(float(period))}"
        except (TypeError, ValueError):
            label += f" {period}"
    if output != "value":
        label += f" {output}"
    if timeframe != "current" and timeframe != str(base_timeframe):
        label += f" {timeframe}"
    return label.strip()


def _declarative_indicator_overlay(operand: dict) -> bool:
    name = str(operand.get("name", "") or "").upper()
    output = str(operand.get("output", "value") or "value")
    return name in {
        "EMA",
        "SMA",
        "WMA",
        "HMA",
        "DEMA",
        "RMA",
        "LINREG",
        "BB",
        "VWAP",
    } and output not in {
        "width",
        "percent_b",
        "histogram",
    }


def _iter_declarative_indicator_plot_specs(
    definition: dict, parameters: dict, base_timeframe: str = "current"
):
    seen = set()
    for operand in strategy_engine.iter_operands(definition):
        if operand.get("type") != "indicator":
            continue
        try:
            column = f"SB_{strategy_engine._series_key(operand, parameters)}"
        except Exception:
            continue
        label = _declarative_indicator_plot_label(
            operand, parameters, base_timeframe=base_timeframe
        )
        if not label or label in seen:
            continue
        timeframe = str(operand.get("timeframe", "current") or "current")
        normalized_timeframe = (
            str(base_timeframe or "current") if timeframe == "current" else timeframe
        )
        seen.add(label)
        yield {
            "column": column,
            "label": label,
            "overlay": _declarative_indicator_overlay(operand),
            "timeframe": normalized_timeframe,
        }


def _ema_fast_less_than_slow(param):
    return param.ema_fast < param.ema_slow


def _hma_fast_less_than_slow(param):
    return param.hma_fast < param.hma_slow


def _legacy_optimize_constraint_fallback(optimized_names):
    names = set(optimized_names)
    if {"ema_fast", "ema_slow"}.issubset(names):
        return _ema_fast_less_than_slow
    if {"hma_fast", "hma_slow"}.issubset(names):
        return _hma_fast_less_than_slow
    return None


def _n1_less_than_n2(param):
    return param.n1 < param.n2


def _parameter_range_values(spec: dict):
    value_type = str(spec.get("type", "float") or "float")
    if value_type == "bool":
        return [False, True]
    minimum = spec.get("min", spec.get("default", 0))
    maximum = spec.get("max", spec.get("default", minimum))
    step = spec.get("step", 1)
    try:
        if value_type == "int":
            start = int(float(minimum))
            stop = int(float(maximum))
            increment = max(int(float(step)), 1)
            return range(start, stop + 1, increment)
        start = float(minimum)
        stop = float(maximum)
        increment = float(step)
        if increment <= 0:
            increment = 1.0
    except (TypeError, ValueError):
        return []
    values = []
    current = start
    while current <= stop + 1e-12:
        values.append(round(current, 10))
        current += increment
    return values


def build_declarative_optimize_params(definition: dict, maximize: str):
    parameters = (
        definition.get("parameters", {}) if isinstance(definition, dict) else {}
    )
    optimize_params = {
        "maximize": maximize,
        "return_heatmap": True,
    }
    optimized_names = []
    if not isinstance(parameters, dict):
        return optimize_params, optimized_names
    for name, spec in parameters.items():
        if not isinstance(spec, dict) or not bool(spec.get("optimizable", False)):
            continue
        values = _parameter_range_values(spec)
        if not values:
            continue
        optimize_params[str(name)] = values
        optimized_names.append(str(name))
    constraint_fn = strategy_schema.build_optimize_constraint_fn(
        definition, optimized_names
    )
    if constraint_fn is None:
        constraint_fn = _legacy_optimize_constraint_fallback(optimized_names)
    if constraint_fn is not None:
        optimize_params["constraint"] = constraint_fn
    return optimize_params, optimized_names


def _serial_grid_maximize_value(stats, maximize):
    if callable(maximize):
        return maximize(stats)
    try:
        return stats[maximize]
    except Exception:
        return np.nan


def run_serial_grid_optimize(bt, optimize_params: dict):
    maximize = optimize_params.get("maximize", "SQN")
    constraint = optimize_params.get("constraint")
    max_tries = optimize_params.get("max_tries")
    return_heatmap = bool(optimize_params.get("return_heatmap", False))
    reserved_keys = {
        "constraint",
        "maximize",
        "max_tries",
        "method",
        "random_state",
        "return_heatmap",
        "return_optimization",
    }
    param_grid = {
        str(name): list(values)
        for name, values in optimize_params.items()
        if name not in reserved_keys
    }
    param_names = list(param_grid.keys())
    param_combos = []
    for values in product(*(param_grid[name] for name in param_names)):
        params = dict(zip(param_names, values))
        if constraint is not None and not constraint(SimpleNamespace(**params)):
            continue
        param_combos.append(params)

    if not param_combos:
        raise ValueError("No admissible parameter combinations to test")

    total_param_combos = len(param_combos)
    if max_tries is not None:
        try:
            max_tries = int(float(max_tries))
        except (TypeError, ValueError):
            max_tries = None
    if max_tries is not None and 0 < max_tries < len(param_combos):
        sampled_indexes = np.linspace(
            0,
            len(param_combos) - 1,
            num=max_tries,
            dtype=int,
        )
        param_combos = [param_combos[index] for index in dict.fromkeys(sampled_indexes)]

    print(
        "Declarative optimization grid = "
        f"{len(param_combos)}/{total_param_combos} combinations",
        flush=True,
    )

    heatmap = pd.Series(
        np.nan,
        name=maximize if isinstance(maximize, str) else None,
        index=pd.MultiIndex.from_tuples(
            [builtins.tuple(params.values()) for params in param_combos],
            names=param_names,
        ),
    )
    for params in param_combos:
        stats = bt.run(**params)
        if stats is not None:
            heatmap.loc[builtins.tuple(params.values())] = _serial_grid_maximize_value(
                stats, maximize
            )

    if pd.isnull(heatmap).all():
        stats = bt.run(**param_combos[0])
    else:
        best_params = heatmap.idxmax(skipna=True)
        if not isinstance(best_params, builtins.tuple):
            best_params = (best_params,)
        stats = bt.run(**dict(zip(heatmap.index.names, best_params)))

    if return_heatmap:
        return stats, heatmap
    return stats


def count_declarative_optimization_combinations(
    definition: dict,
    maximize: str = "SQN",
) -> builtins.tuple[int, list[str]]:
    optimize_params, optimized_names = build_declarative_optimize_params(
        definition,
        maximize,
    )
    if not optimized_names:
        return 0, optimized_names

    constraint = optimize_params.get("constraint")
    total = 0
    for values in product(*(list(optimize_params[name]) for name in optimized_names)):
        params = dict(zip(optimized_names, values))
        if constraint is not None and not constraint(SimpleNamespace(**params)):
            continue
        total += 1
    return total, optimized_names


class RiskManagedStrategy(Strategy):
    # Populated from app settings before each backtest run.
    stop_loss_pct = 0.0
    atr_trailing_enabled = False
    atr_period = 14
    atr_multiplier = 1.8
    atr_activation_pnl = 2.0
    take_profit_enabled = False
    take_profits = []
    use_current_timeframe_market_phase_filter = False
    use_daily_market_phase_filter = False
    daily_market_phase_timeframe = ""
    daily_market_phase_alignment = ""
    _last_exit_reason_map = {}
    _last_exit_reason_records = []

    def init(self):
        super().init()
        self.atr = (
            self.I(ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
            if bool(self.atr_trailing_enabled)
            else None
        )
        self._rm_highest = None
        self._rm_trailing_started = False
        self._rm_was_in_position = False
        self._tp_done = set()
        self._pending_exit_reasons = []
        self._processed_closed_count = 0
        self._exit_reason_map = {}
        self._exit_reason_records = []
        type(self)._last_exit_reason_map = {}
        type(self)._last_exit_reason_records = []

    def _reset_risk_state(self):
        self._rm_highest = None
        self._rm_trailing_started = False
        self._rm_was_in_position = False
        self._tp_done = set()
        self._pending_exit_reasons = []

    def _build_tp_steps(self):
        levels = normalize_take_profit_levels(getattr(self, "take_profits", []))
        return [
            (int(level["level"]), float(level["pnl_pct"]), float(level["amount_pct"]))
            for level in levels
        ]

    def _queue_exit_reason(self, reason: str):
        entry_bar = int(self.trades[-1].entry_bar) if len(self.trades) > 0 else None
        self._pending_exit_reasons.append(
            {"entry_bar": entry_bar, "reason": str(reason)}
        )

    def _pop_pending_exit_reason(self, trade):
        for idx, pending in enumerate(self._pending_exit_reasons):
            if pending["entry_bar"] is None or int(pending["entry_bar"]) == int(
                trade.entry_bar
            ):
                return self._pending_exit_reasons.pop(idx)["reason"]
        return ""

    def _process_new_closed_trades(self):
        closed = self.closed_trades
        if self._processed_closed_count >= len(closed):
            return

        new_closed = closed[self._processed_closed_count :]
        for trade in new_closed:
            reason = self._pop_pending_exit_reason(trade)
            if not reason:
                reason = "atr_trailing" if self._rm_trailing_started else "hard_sl"

            key = (int(trade.entry_bar), int(trade.exit_bar))
            self._exit_reason_map.setdefault(key, []).append(reason)
            self._exit_reason_records.append(
                {
                    "entry_bar": int(trade.entry_bar),
                    "exit_bar": int(trade.exit_bar),
                    "reason": reason,
                }
            )

        self._processed_closed_count = len(closed)
        type(self)._last_exit_reason_map = dict(self._exit_reason_map)
        type(self)._last_exit_reason_records = list(self._exit_reason_records)

        if not self.position:
            self._reset_risk_state()

    def _apply_take_profits(self, current_pnl_pct: float):
        if not self.position or not bool(self.take_profit_enabled):
            return

        for tp_num, tp_level, tp_amount in self._build_tp_steps():
            if tp_level <= 0 or tp_amount <= 0:
                continue
            if tp_num in self._tp_done:
                continue
            if current_pnl_pct < tp_level:
                continue

            portion = max(0.0, min(1.0, tp_amount / 100.0))
            if portion <= 0:
                continue

            # Close a fraction of the remaining open position.
            self._queue_exit_reason(f"tp{tp_num}")
            self.position.close(portion=portion)
            self._tp_done.add(tp_num)

            # If TP requests full close, no point checking further levels.
            if portion >= 1.0:
                break

    def _update_risk_stop(self):
        if not self.position or len(self.trades) == 0:
            return

        trade = self.trades[-1]
        entry_price = float(trade.entry_price)
        close_price = float(self.data.Close[-1])

        if self._rm_highest is None:
            self._rm_highest = entry_price
        self._rm_highest = max(float(self._rm_highest), close_price)

        hard_sl = None
        if float(self.stop_loss_pct) > 0:
            hard_sl = entry_price * (1.0 - float(self.stop_loss_pct) / 100.0)

        active_sl = hard_sl
        atr_value = None
        if bool(self.atr_trailing_enabled) and self.atr is not None:
            atr_last = self.atr[-1]
            atr_value = None if pd.isna(atr_last) else float(atr_last)

        pnl_pct = 0.0
        if entry_price > 0:
            pnl_pct = ((close_price - entry_price) / entry_price) * 100.0

        if (
            bool(self.atr_trailing_enabled)
            and float(self.atr_multiplier) > 0
            and atr_value is not None
            and atr_value > 0
        ):
            if self._rm_trailing_started or pnl_pct >= float(self.atr_activation_pnl):
                self._rm_trailing_started = True
                trail_sl = float(self._rm_highest) - (
                    float(self.atr_multiplier) * atr_value
                )
                active_sl = (
                    trail_sl
                    if active_sl is None
                    else max(float(active_sl), float(trail_sl))
                )

        if active_sl is not None and active_sl > 0:
            prev_sl = trade.sl
            if prev_sl is None or pd.isna(prev_sl):
                trade.sl = float(active_sl)
            else:
                trade.sl = max(float(prev_sl), float(active_sl))

        self._rm_was_in_position = True


# -------------------------------------
# Use SMA50 and SMA200
# BUY when price close > SMA50 and price close > SMA200 and SMA50<SMA200 (Accumulation Phase)
# BUY when price close > SMA50 and price close > SMA200 and SMA50 > SMA200
# SELL when price close < SMA50 or SMA200 (whatever happens first)
# -------------------------------------
class market_phases(RiskManagedStrategy):
    nFastSMA = 50
    nSlowSMA = 200
    use_current_timeframe_market_phase_filter = True

    def init(self):
        super().init()
        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)

    def next(self):
        super().next()
        self._process_new_closed_trades()
        self._update_risk_stop()
        if self.position and len(self.trades) > 0:
            entry_price = float(self.trades[-1].entry_price)
            close_price = float(self.data.Close[-1])
            current_pnl_pct = (
                ((close_price - entry_price) / entry_price) * 100
                if entry_price > 0
                else 0.0
            )
            self._apply_take_profits(current_pnl_pct)
        SMA50 = self.sma50
        SMA200 = self.sma200
        priceClose = self.data.Close

        accumulationPhase = (
            (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        )
        bullishPhase = (
            (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)
        )

        if not self.position:
            if accumulationPhase or bullishPhase:
                self.buy()

        else:
            if not (accumulationPhase or bullishPhase):
                self._queue_exit_reason("strategy")
                self.position.close()


# -------------------------------------
# we will use 2 exponencial moving averages:
# BUY when fast ema > slow ema
# SELL when slow ema > fast ema
# -------------------------------------
class ema_cross(RiskManagedStrategy):
    n1 = 2
    n2 = 14
    nFastSMA = 50
    nSlowSMA = 200

    def init(self):
        super().init()
        self.emaFast = self.I(EMA, self.data.Close, self.n1)
        self.emaSlow = self.I(EMA, self.data.Close, self.n2)

    def next(self):
        super().next()
        self._process_new_closed_trades()
        self._update_risk_stop()
        if self.position and len(self.trades) > 0:
            entry_price = float(self.trades[-1].entry_price)
            close_price = float(self.data.Close[-1])
            current_pnl_pct = (
                ((close_price - entry_price) / entry_price) * 100
                if entry_price > 0
                else 0.0
            )
            self._apply_take_profits(current_pnl_pct)
        fastEMA = self.emaFast
        slowEMA = self.emaSlow
        daily_market_phase_ok = True
        if bool(getattr(self, "use_daily_market_phase_filter", False)):
            daily_market_phase_ok = bool(self.data.Daily_Market_Phase_OK[-1])

        if not self.position:
            if daily_market_phase_ok and crossover(fastEMA, slowEMA):
                self.buy()

        else:
            if crossover(slowEMA, fastEMA):
                self._queue_exit_reason("strategy")
                self.position.close()


class ema_cross_with_market_phases(RiskManagedStrategy):
    n1 = 20
    n2 = 40
    nFastSMA = 50
    nSlowSMA = 200
    use_current_timeframe_market_phase_filter = True

    def init(self):
        super().init()
        self.emaFast = self.I(EMA, self.data.Close, self.n1)
        self.emaSlow = self.I(EMA, self.data.Close, self.n2)
        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)

    def next(self):
        super().next()
        self._process_new_closed_trades()
        self._update_risk_stop()
        if self.position and len(self.trades) > 0:
            entry_price = float(self.trades[-1].entry_price)
            close_price = float(self.data.Close[-1])
            current_pnl_pct = (
                ((close_price - entry_price) / entry_price) * 100
                if entry_price > 0
                else 0.0
            )
            self._apply_take_profits(current_pnl_pct)
        fastEMA = self.emaFast
        slowEMA = self.emaSlow
        SMA50 = self.sma50
        SMA200 = self.sma200
        priceClose = self.data.Close

        accumulationPhase = (
            (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        )
        bullishPhase = (
            (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)
        )
        current_market_phase_ok = accumulationPhase or bullishPhase
        if not bool(getattr(self, "use_current_timeframe_market_phase_filter", True)):
            current_market_phase_ok = True

        daily_market_phase_ok = True
        if bool(getattr(self, "use_daily_market_phase_filter", False)):
            daily_market_phase_ok = bool(self.data.Daily_Market_Phase_OK[-1])

        if not self.position:
            if (
                daily_market_phase_ok
                and current_market_phase_ok
                and crossover(fastEMA, slowEMA)
            ):
                self.buy()

        else:
            if crossover(slowEMA, fastEMA):
                self._queue_exit_reason("strategy")
                self.position.close()


class hma_rsi_linreg(RiskManagedStrategy):
    n1 = 16
    n2 = 65
    nFastHMA = 16
    nSlowHMA = 65
    rsi_period = 14
    rsi_min = 52
    linreg_period = 50
    use_daily_linreg_filter = True
    daily_linreg_timeframe = "1d"
    daily_linreg_alignment = "previous_closed_candle"

    def init(self):
        super().init()
        self.hmaFast = self.I(HMA, self.data.Close, self.n1)
        self.hmaSlow = self.I(HMA, self.data.Close, self.n2)
        self.rsi = self.I(RSI, self.data.Close, self.rsi_period)

    def next(self):
        super().next()
        self._process_new_closed_trades()
        self._update_risk_stop()
        if self.position and len(self.trades) > 0:
            entry_price = float(self.trades[-1].entry_price)
            close_price = float(self.data.Close[-1])
            current_pnl_pct = (
                ((close_price - entry_price) / entry_price) * 100
                if entry_price > 0
                else 0.0
            )
            self._apply_take_profits(current_pnl_pct)

        daily_linreg_ok = True
        if bool(getattr(self, "use_daily_linreg_filter", False)):
            daily_linreg_ok = bool(self.data.Daily_Close_Above_LINREG50[-1])

        if not self.position:
            if (
                daily_linreg_ok
                and crossover(self.hmaFast, self.hmaSlow)
                and float(self.rsi[-1]) > float(self.rsi_min)
            ):
                self.buy()

        else:
            if crossover(self.hmaSlow, self.hmaFast):
                self._queue_exit_reason("strategy")
                self.position.close()


class DeclarativeStrategy(RiskManagedStrategy):
    definition = {}
    parameter_values = {}
    data_price_scale = 1.0

    def init(self):
        super().init()
        self._definition = self.definition or {}
        parameter_overrides = dict(self.parameter_values or {})
        definition_parameters = (
            self._definition.get("parameters", {})
            if isinstance(self._definition, dict)
            else {}
        )
        if isinstance(definition_parameters, dict):
            for parameter_name in definition_parameters:
                if hasattr(self, str(parameter_name)):
                    parameter_overrides[str(parameter_name)] = getattr(
                        self, str(parameter_name)
                    )
        self._parameters = strategy_engine.resolve_parameters(
            self._definition,
            parameter_overrides,
        )
        base_df = pd.DataFrame(
            {
                "Open": np.asarray(self.data.Open, dtype=float),
                "High": np.asarray(self.data.High, dtype=float),
                "Low": np.asarray(self.data.Low, dtype=float),
                "Close": np.asarray(self.data.Close, dtype=float),
                "Volume": np.asarray(self.data.Volume, dtype=float),
            }
        )
        try:
            base_df.index = pd.to_datetime(self.data.index)
        except Exception:
            pass
        self._indicator_df = strategy_engine.add_indicators(
            base_df,
            self._definition,
            self._parameters,
            symbol=str(getattr(self, "execution_symbol", "") or ""),
            base_timeframe=str(
                getattr(self, "execution_timeframe", "current") or "current"
            ),
            data_loader=self._load_strategy_data,
        )
        for spec in _iter_declarative_indicator_plot_specs(
            self._definition,
            self._parameters,
            base_timeframe=str(
                getattr(self, "execution_timeframe", "current") or "current"
            ),
        ):
            column = spec["column"]
            if column not in self._indicator_df.columns:
                continue
            self.I(
                _identity_indicator,
                np.asarray(self._indicator_df[column], dtype=float),
                name=spec["label"],
                overlay=bool(spec["overlay"]),
            )

    def _current_slice(self):
        return self._indicator_df.iloc[: len(self.data)]

    def next(self):
        super().next()
        self._process_new_closed_trades()
        self._update_risk_stop()
        if self.position and len(self.trades) > 0:
            entry_price = float(self.trades[-1].entry_price)
            close_price = float(self.data.Close[-1])
            current_pnl_pct = (
                ((close_price - entry_price) / entry_price) * 100
                if entry_price > 0
                else 0.0
            )
            self._apply_take_profits(current_pnl_pct)

        current_df = self._current_slice()
        if not self.position:
            if strategy_engine.evaluate_entry(
                current_df,
                self._definition,
                self._parameters,
                symbol=str(getattr(self, "execution_symbol", "") or ""),
                base_timeframe=str(
                    getattr(self, "execution_timeframe", "current") or "current"
                ),
                data_loader=self._load_strategy_data,
            ):
                self.buy()
        else:
            if strategy_engine.evaluate_exit(
                current_df,
                self._definition,
                self._parameters,
                symbol=str(getattr(self, "execution_symbol", "") or ""),
                base_timeframe=str(
                    getattr(self, "execution_timeframe", "current") or "current"
                ),
                data_loader=self._load_strategy_data,
            ):
                self._queue_exit_reason("strategy")
                self.position.close()

    def _load_strategy_data(self, symbol, timeframe):
        df = get_data(symbol, timeframe)
        scale = float(getattr(self, "data_price_scale", 1.0) or 1.0)
        if scale == 1.0 or df.empty:
            return df
        result = df.copy()
        price_columns = [
            column
            for column in ("Open", "High", "Low", "Close")
            if column in result.columns
        ]
        result[price_columns] = result[price_columns] * scale
        return result


def build_declarative_strategy_class(strategy_id: str):
    df_strategy = database.get_strategy_by_id(strategy_id)
    if df_strategy.empty:
        return None
    row = df_strategy.iloc[0]
    try:
        definition = strategy_schema.validate_definition(
            row.get("Definition_JSON", "{}")
        )
    except Exception:
        return None
    if definition.get("engine") != "bec_strategy_ast_v2":
        return None
    parameter_values = strategy_engine.resolve_parameters(definition)
    class_name = f"{strategy_id}_declarative"
    class_attrs = {
        "definition": definition,
        "parameter_values": parameter_values,
        "strategy_id": str(strategy_id),
        "__module__": __name__,
        "__qualname__": class_name,
    }
    for parameter_name, value in parameter_values.items():
        class_attrs[str(parameter_name)] = value
    strategy_class = type(
        class_name,
        (DeclarativeStrategy,),
        class_attrs,
    )
    globals()[class_name] = strategy_class
    return strategy_class


def resolve_strategy(strategy_id: str):
    declarative_strategy = build_declarative_strategy_class(strategy_id)
    if declarative_strategy is not None:
        return declarative_strategy
    strategy = globals().get(str(strategy_id))
    if strategy is not None:
        return strategy
    return None


def get_data(symbol, timeframe):
    df = binance.get_ohlcv(
        symbol=symbol,
        interval=timeframe,
    )

    if df.empty:
        msg = f"Failed after max tries to get historical data for {symbol} ({timeframe}). "
        msg = msg + sys._getframe().f_code.co_name + " - " + symbol
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)

        telegram.send_telegram_message(
            telegram.telegram_token_main, telegram.EMOJI_WARNING, msg
        )
        return pd.DataFrame()

    return df


def _normalize_datetime_index(index):
    normalized = pd.to_datetime(index)
    if getattr(normalized, "tz", None) is not None:
        normalized = normalized.tz_convert(None)
    return normalized


def _positive_market_phase(close, sma_fast, sma_slow):
    accumulation = (close > sma_fast) & (close > sma_slow) & (sma_fast < sma_slow)
    bullish = (close > sma_fast) & (close > sma_slow) & (sma_fast > sma_slow)
    return accumulation | bullish


def add_daily_market_phase_filter(df, symbol, sma_fast=50, sma_slow=200):
    """
    Add the runtime-like 1d market phase filter to intraday backtests.

    The daily phase is shifted by one candle so 1h/4h candles only use the
    previous closed daily candle, avoiding lookahead from the current day close.
    """
    df = df.copy()
    df["Daily_Market_Phase_OK"] = False

    df_daily = get_data(symbol, "1d")
    if df_daily.empty:
        return df

    intraday_index = _normalize_datetime_index(df.index)
    daily_index = _normalize_datetime_index(df_daily.index)
    df.index = intraday_index
    df_daily = df_daily.copy()
    df_daily.index = daily_index

    daily_close = pd.to_numeric(df_daily["Close"], errors="coerce")
    daily_sma_fast = daily_close.rolling(int(sma_fast)).mean()
    daily_sma_slow = daily_close.rolling(int(sma_slow)).mean()
    daily_ok = _positive_market_phase(daily_close, daily_sma_fast, daily_sma_slow)
    daily_ok = daily_ok.fillna(False).astype(bool).shift(1, fill_value=False)

    daily_filter = pd.DataFrame(
        {"Daily_Market_Phase_OK": daily_ok}, index=df_daily.index
    )
    aligned_filter = daily_filter.reindex(df.index, method="ffill")
    df["Daily_Market_Phase_OK"] = (
        aligned_filter["Daily_Market_Phase_OK"].fillna(False).astype(bool)
    )

    return df


def add_daily_linreg_filter(df, symbol, linreg_period=50):
    """
    Add a 1d close > LINREG filter to intraday backtests.

    The daily condition is shifted by one candle so intraday candles only use
    the previous closed daily candle.
    """
    df = df.copy()
    df["Daily_Close_Above_LINREG50"] = False

    df_daily = get_data(symbol, "1d")
    if df_daily.empty:
        return df

    intraday_index = _normalize_datetime_index(df.index)
    daily_index = _normalize_datetime_index(df_daily.index)
    df.index = intraday_index
    df_daily = df_daily.copy()
    df_daily.index = daily_index

    daily_close = pd.to_numeric(df_daily["Close"], errors="coerce")
    daily_linreg = LINREG(daily_close, linreg_period)
    daily_ok = (
        (daily_close > daily_linreg)
        .fillna(False)
        .astype(bool)
        .shift(1, fill_value=False)
    )

    daily_filter = pd.DataFrame(
        {"Daily_Close_Above_LINREG50": daily_ok}, index=df_daily.index
    )
    aligned_filter = daily_filter.reindex(df.index, method="ffill")
    df["Daily_Close_Above_LINREG50"] = (
        aligned_filter["Daily_Close_Above_LINREG50"].fillna(False).astype(bool)
    )

    return df


def add_current_timeframe_linreg_filter(df, linreg_period=50):
    df = df.copy()
    close = pd.to_numeric(df["Close"], errors="coerce")
    linreg = LINREG(close, linreg_period)
    df["Daily_Close_Above_LINREG50"] = (close > linreg).fillna(False).astype(bool)
    return df


def get_strategy_name(strategy):
    if isinstance(strategy, str):
        return strategy

    if isinstance(strategy, type):
        return strategy.__name__

    strategy_class = getattr(strategy, "__class__", None)
    if strategy_class is not None:
        return strategy_class.__name__

    return str(strategy)


def get_strategy_id(strategy):
    if isinstance(strategy, str):
        return strategy

    if isinstance(strategy, type):
        return str(getattr(strategy, "strategy_id", "") or strategy.__name__)

    strategy_id = getattr(strategy, "strategy_id", "")
    if strategy_id:
        return str(strategy_id)

    strategy_class = getattr(strategy, "__class__", None)
    if strategy_class is not None:
        return str(getattr(strategy_class, "strategy_id", "") or strategy_class.__name__)

    return str(strategy)


def get_base_strategy_name(strategy):
    return get_strategy_name(strategy).split("(")[0]


def _add_exit_size_percentages(df_trades):
    if "Size" not in df_trades.columns or "EntryBar" not in df_trades.columns:
        return df_trades

    size = pd.to_numeric(df_trades["Size"], errors="coerce").abs()
    entry_size = size.groupby(df_trades["EntryBar"]).transform("sum")
    df_trades["Exit_Size_Pct"] = (size / entry_size.replace(0, pd.NA) * 100).round(8)

    sort_columns = [
        column
        for column in ["EntryBar", "ExitBar", "ExitTime"]
        if column in df_trades.columns
    ]
    sorted_trades = df_trades.assign(_bec_original_index=df_trades.index)
    sorted_trades = sorted_trades.sort_values(
        sort_columns + ["_bec_original_index"],
        kind="mergesort",
    )

    remaining_exit_pct = pd.Series(pd.NA, index=df_trades.index, dtype="Float64")
    for _entry_bar, group in sorted_trades.groupby("EntryBar", sort=False):
        remaining_size = size.loc[group.index].sum()
        for index in group.index:
            closed_size = size.loc[index]
            if pd.isna(closed_size) or pd.isna(remaining_size) or remaining_size <= 0:
                remaining_exit_pct.loc[index] = pd.NA
                continue

            remaining_exit_pct.loc[index] = round(
                float(closed_size / remaining_size * 100),
                8,
            )
            remaining_size = remaining_size - closed_size

    df_trades["Exit_Remaining_Size_Pct"] = remaining_exit_pct
    return df_trades


def build_backtesting_trades_df(stats, strategy=None):
    df_trades = pd.DataFrame(stats._trades)
    if df_trades.empty:
        return df_trades

    hard_stop_pct = float(getattr(strategy, "stop_loss_pct", 0.0) or 0.0)
    atr_multiplier = float(getattr(strategy, "atr_multiplier", 0.0) or 0.0)

    if hard_stop_pct > 0 and "EntryPrice" in df_trades.columns:
        df_trades["Hard_Stop_Loss"] = (
            df_trades["EntryPrice"].astype(float) * (1.0 - hard_stop_pct / 100.0)
        ).round(8)
    else:
        df_trades["Hard_Stop_Loss"] = pd.NA

    df_trades["Active_Stop_Loss"] = df_trades["SL"]
    active_sl = pd.to_numeric(df_trades["Active_Stop_Loss"], errors="coerce")
    hard_sl = pd.to_numeric(df_trades["Hard_Stop_Loss"], errors="coerce")
    df_trades["ATR_Stop_Loss"] = active_sl.where(
        active_sl.notna() & (hard_sl.isna() | ((active_sl - hard_sl).abs() > 1e-8)),
        pd.NA,
    )

    df_trades = _add_exit_size_percentages(df_trades)

    exit_reason_records = (
        getattr(strategy, "_last_exit_reason_records", [])
        if strategy is not None
        else []
    )
    if len(exit_reason_records) == len(df_trades):
        df_trades["Exit_Reason"] = [record["reason"] for record in exit_reason_records]
    else:
        exit_reason_map = (
            getattr(strategy, "_last_exit_reason_map", {})
            if strategy is not None
            else {}
        )
        if exit_reason_map:
            reason_map = {key: list(value) for key, value in exit_reason_map.items()}

            def _resolve_exit_reason(row):
                key = (int(row["EntryBar"]), int(row["ExitBar"]))
                reasons = reason_map.get(key, [])
                if reasons:
                    return str(reasons.pop(0))
                return "unknown"

            df_trades["Exit_Reason"] = df_trades.apply(_resolve_exit_reason, axis=1)

    return df_trades


def enrich_backtesting_plot_tooltips(html_content, df_trades):
    html_content = html_content.replace("@size{0,0}", "@size{0,0.00000000}")
    html_content = re.sub(
        r'("@\{ATR\(H,L,C,\d+\)_\d+_\d+\})\{[^"]+\}"',
        r'\1{0,0.00000000}"',
        html_content,
    )

    if df_trades.empty or "Exit_Reason" not in df_trades.columns:
        return html_content

    exit_reasons = [str(value) for value in df_trades["Exit_Reason"].fillna("")]
    exit_reason_json = json.dumps(exit_reasons, ensure_ascii=True)

    html_content = re.sub(
        r'(\["returns_positive",\{"type":"ndarray".*?\}\])',
        rf'\1,["exit_reason",{{"type":"ndarray","array":{exit_reason_json},"shape":[{len(exit_reasons)}],"dtype":"object","order":"little"}}]',
        html_content,
        count=1,
    )

    html_content = html_content.replace(
        '["Size","@size{0,0.00000000}"],["P/L","@returns{+0.[000]%}"]',
        '["Size","@size{0,0.00000000}"],["Exit","@exit_reason"],["P/L","@returns{+0.[000]%}"]',
    )
    html_content = html_content.replace(
        '["Size","@size{0,0}"],["P/L","@returns{+0.[000]%}"]',
        '["Size","@size{0,0.00000000}"],["Exit","@exit_reason"],["P/L","@returns{+0.[000]%}"]',
    )

    return html_content


def prepare_backtesting_trades_display(df_trades):
    if df_trades.empty:
        return df_trades

    df_display = df_trades.copy()
    if "ReturnPct" in df_display.columns:
        df_display["Return_Pct"] = (df_display["ReturnPct"].astype(float) * 100).round(
            4
        )

    for column in [
        "Exit_Size_Pct",
        "Exit_Remaining_Size_Pct",
        "Size",
        "EntryPrice",
        "ExitPrice",
        "PnL",
        "Commission",
        "Hard_Stop_Loss",
        "ATR_Stop_Loss",
        "Active_Stop_Loss",
    ]:
        if column in df_display.columns:
            df_display[column] = pd.to_numeric(
                df_display[column], errors="coerce"
            ).round(8)

    ordered_columns = [
        "EntryTime",
        "ExitTime",
        "Duration",
        "Exit_Reason",
        "Exit_Size_Pct",
        "Exit_Remaining_Size_Pct",
        "Size",
        "EntryPrice",
        "ExitPrice",
        "Return_Pct",
        "PnL",
        "Commission",
        "Hard_Stop_Loss",
        "ATR_Stop_Loss",
        "Active_Stop_Loss",
        "EntryBar",
        "ExitBar",
    ]
    return df_display[
        [column for column in ordered_columns if column in df_display.columns]
    ]


def add_backtesting_trade_header_tooltips(trades_html_table):
    header_tooltips = {
        "Exit_Size_Pct": "Percent of the original position size closed by this exit.",
        "Exit_Remaining_Size_Pct": "Percent of the remaining open position closed immediately before this exit.",
        "Size": "Quantity of the traded base asset, not the position value in quote currency.",
    }
    for column, tooltip in header_tooltips.items():
        escaped_column = html.escape(column)
        escaped_tooltip = html.escape(tooltip, quote=True)
        trades_html_table = trades_html_table.replace(
            f"<th>{escaped_column}</th>",
            f'<th title="{escaped_tooltip}" aria-label="{escaped_tooltip}">{escaped_column}</th>',
            1,
        )
    return trades_html_table


def flatten_backtesting_config(backtest_config):
    rows = []

    def _append_rows(section, prefix, value):
        if isinstance(value, dict):
            for key, nested_value in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                _append_rows(section, next_prefix, nested_value)
            return

        rows.append(
            {
                "Section": section,
                "Setting": prefix,
                "Value": value,
            }
        )

    for section, section_config in (backtest_config or {}).items():
        _append_rows(str(section), "", section_config)

    return pd.DataFrame(rows, columns=["Section", "Setting", "Value"])


def _json_default(value):
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def build_strategy_parameters_config(strategy):
    params = {}

    definition = getattr(strategy, "_definition", None) or getattr(
        strategy, "definition", None
    )
    if (
        isinstance(definition, dict)
        and definition.get("engine") == "bec_strategy_ast_v2"
    ):
        parameters = (
            getattr(strategy, "_parameters", None)
            or getattr(strategy, "parameter_values", None)
            or {}
        )
        if parameters:
            params["parameters"] = dict(parameters)
        indicators = []
        seen_indicators = set()
        base_timeframe = str(
            getattr(strategy, "execution_timeframe", "current") or "current"
        )
        for operand in strategy_engine.iter_operands(definition):
            if operand.get("type") != "indicator":
                continue
            name = str(operand.get("name", "") or "").upper()
            indicator_params = _declarative_indicator_params(operand, parameters)
            period = indicator_params.get("period")
            timeframe = str(operand.get("timeframe", "current") or "current")
            normalized_timeframe = (
                base_timeframe if timeframe == "current" else timeframe
            )
            key = (name, str(period), normalized_timeframe)
            if key in seen_indicators:
                continue
            seen_indicators.add(key)
            indicators.append(
                {
                    "name": name,
                    "period": period,
                    "timeframe": normalized_timeframe,
                }
            )
        if indicators:
            params["definition_indicators"] = indicators
        return params

    if (hasattr(strategy, "n1") or hasattr(strategy, "n2")) and not hasattr(
        strategy, "nFastHMA"
    ):
        params["moving_averages"] = {
            "ema_fast": int(getattr(strategy, "n1", 0) or 0),
            "ema_slow": int(getattr(strategy, "n2", 0) or 0),
        }

    if hasattr(strategy, "nFastSMA") or hasattr(strategy, "nSlowSMA"):
        params.setdefault("market_phase_filter", {})
        params["market_phase_filter"].update(
            {
                "enabled": bool(
                    getattr(
                        strategy, "use_current_timeframe_market_phase_filter", False
                    )
                ),
                "timeframe": getattr(strategy, "current_market_phase_timeframe", "")
                or "current",
                "sma_fast": int(getattr(strategy, "nFastSMA", 0) or 0),
                "sma_slow": int(getattr(strategy, "nSlowSMA", 0) or 0),
            }
        )

    if getattr(strategy, "use_daily_market_phase_filter", False):
        params["higher_timeframe_market_phase_filter"] = {
            "enabled": True,
            "timeframe": getattr(strategy, "daily_market_phase_timeframe", "1d")
            or "1d",
            "sma_fast": int(getattr(strategy, "daily_market_phase_sma_fast", 0) or 0),
            "sma_slow": int(getattr(strategy, "daily_market_phase_sma_slow", 0) or 0),
            "alignment": getattr(strategy, "daily_market_phase_alignment", "")
            or "previous_closed_candle",
        }

    if hasattr(strategy, "nFastHMA") or hasattr(strategy, "nSlowHMA"):
        params["hma_rsi_linreg"] = {
            "hma_fast": int(
                getattr(strategy, "n1", getattr(strategy, "nFastHMA", 0)) or 0
            ),
            "hma_slow": int(
                getattr(strategy, "n2", getattr(strategy, "nSlowHMA", 0)) or 0
            ),
            "rsi_period": int(getattr(strategy, "rsi_period", 0) or 0),
            "rsi_min": float(getattr(strategy, "rsi_min", 0) or 0),
            "daily_linreg_enabled": bool(
                getattr(strategy, "use_daily_linreg_filter", False)
            ),
            "daily_linreg_period": int(getattr(strategy, "linreg_period", 0) or 0),
            "daily_linreg_timeframe": getattr(strategy, "daily_linreg_timeframe", "1d")
            or "1d",
            "daily_linreg_alignment": getattr(strategy, "daily_linreg_alignment", "")
            or "previous_closed_candle",
        }

    return params


def get_market_phase_sma_settings(bt_settings, timeframe):
    normalized_timeframe = str(timeframe).lower()
    if normalized_timeframe not in {"1h", "4h", "1d"}:
        normalized_timeframe = "1d"

    key_prefix = f"Market_Phase_{normalized_timeframe}_SMA"
    return (
        int(bt_settings.get(f"{key_prefix}_Fast", 50) or 50),
        int(bt_settings.get(f"{key_prefix}_Slow", 200) or 200),
    )


def build_strategy_quality_context(
    stats, backtest_config, df_trades, strategy_name, timeframe, symbol
):
    def _is_missing_scalar(value):
        try:
            missing = pd.isna(value)
        except Exception:
            return False
        if missing is pd.NA:
            return True
        try:
            return bool(missing)
        except Exception:
            return False

    def _round(value, digits=4):
        try:
            if _is_missing_scalar(value):
                return None
            return round(float(value), digits)
        except Exception:
            return None

    stats_dict = {}
    df_stats = pd.DataFrame(stats)
    for key in df_stats.index:
        if str(key).startswith("_"):
            continue
        try:
            value = df_stats.loc[key].iloc[0]
        except Exception:
            continue
        if isinstance(value, (DataFrame, Series, dict, list)):
            continue
        if _is_missing_scalar(value):
            stats_dict[str(key)] = None
        elif isinstance(value, Real):
            stats_dict[str(key)] = float(value)
        else:
            stats_dict[str(key)] = str(value)

    trades = df_trades.copy()
    if "ReturnPct" in trades.columns and "Return_Pct" not in trades.columns:
        trades["Return_Pct"] = (
            pd.to_numeric(trades["ReturnPct"], errors="coerce") * 100.0
        )
    winners = (
        trades[trades["Return_Pct"] > 0]
        if "Return_Pct" in trades.columns
        else pd.DataFrame()
    )
    losers = (
        trades[trades["Return_Pct"] <= 0]
        if "Return_Pct" in trades.columns
        else pd.DataFrame()
    )

    return {
        "strategy": {
            "id": strategy_name,
            "name": strategy_name,
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "market": "crypto",
            "asset_type": "spot_long_only",
        },
        "config": backtest_config or {},
        "stats": stats_dict,
        "trade_summary": {
            "count_total": int(len(trades)),
            "count_winners": int(len(winners)),
            "count_losers": int(len(losers)),
            "avg_return_pct": _round(
                trades["Return_Pct"].mean() if "Return_Pct" in trades.columns else None
            ),
            "median_return_pct": _round(
                trades["Return_Pct"].median()
                if "Return_Pct" in trades.columns
                else None
            ),
            "total_pnl": _round(
                trades["PnL"].sum() if "PnL" in trades.columns else None
            ),
        },
    }


BUY_HOLD_START_MODE_INDICATOR_WARMUP = "indicator_warmup"
BUY_HOLD_START_MODE_FULL_PERIOD = "full_period"


def apply_buy_hold_start_mode(
    stats, df, start_mode=BUY_HOLD_START_MODE_INDICATOR_WARMUP
):
    start_mode = str(start_mode or BUY_HOLD_START_MODE_INDICATOR_WARMUP)
    if start_mode != BUY_HOLD_START_MODE_FULL_PERIOD:
        stats.loc["Buy & Hold Start Mode"] = BUY_HOLD_START_MODE_INDICATOR_WARMUP
        return stats

    if df.empty:
        return stats

    close = pd.to_numeric(df["Close"], errors="coerce")
    if (
        close.empty
        or pd.isna(close.iloc[0])
        or pd.isna(close.iloc[-1])
        or float(close.iloc[0]) == 0
    ):
        return stats

    start_price = float(close.iloc[0])
    end_price = float(close.iloc[-1])
    full_return = ((end_price - start_price) / start_price) * 100

    # Optional benchmark from the first dataset candle across all strategies.
    stats.loc["Buy & Hold Return [%]"] = full_return
    stats.loc["Buy & Hold Start Price"] = start_price
    stats.loc["Buy & Hold End Price"] = end_price
    stats.loc["Buy & Hold Start Mode"] = BUY_HOLD_START_MODE_FULL_PERIOD
    return stats


def _stats_impact_class(key, value):
    key = str(key)
    positive_keys = {
        "Equity Final [$]",
        "Equity Peak [$]",
        "Return [%]",
        "Return (Ann.) [%]",
        "CAGR [%]",
        "Alpha [%]",
        "Sharpe Ratio",
        "Sortino Ratio",
        "Calmar Ratio",
        "Win Rate [%]",
        "Best Trade [%]",
        "Avg. Trade [%]",
        "Profit Factor",
        "Expectancy [%]",
        "SQN",
        "Kelly Criterion",
    }
    negative_keys = {
        "Max. Drawdown [%]",
        "Avg. Drawdown [%]",
        "Max. Drawdown Duration",
        "Worst Trade [%]",
    }
    neutral_keys = {
        "Start",
        "End",
        "Duration",
        "Exposure Time [%]",
        "Buy & Hold Return [%]",
        "Buy & Hold Start Mode",
        "Buy & Hold Start Price",
        "Buy & Hold End Price",
        "Beta",
        "# Trades",
        "Max. Trade Duration",
        "Volatility (Ann.) [%]",
    }

    try:
        number = float(value)
    except (TypeError, ValueError):
        number = None

    if key in neutral_keys:
        return "neutral"
    if key in negative_keys:
        return "negative"
    if key == "Win Rate [%]" and number is not None:
        return "positive" if number >= 50 else "negative" if number < 40 else "neutral"
    if key == "Sharpe Ratio" and number is not None:
        return (
            "positive" if number >= 0.8 else "negative" if number < 0.5 else "neutral"
        )
    if key == "Sortino Ratio" and number is not None:
        return (
            "positive" if number >= 1.0 else "negative" if number < 0.5 else "neutral"
        )
    if key == "Calmar Ratio" and number is not None:
        return (
            "positive" if number >= 1.0 else "negative" if number < 0.5 else "neutral"
        )
    if key == "Profit Factor" and number is not None:
        return "positive" if number > 1.0 else "negative" if number < 1.0 else "neutral"
    if key == "SQN" and number is not None:
        return (
            "positive" if number >= 1.0 else "negative" if number < 0.5 else "neutral"
        )
    if key in positive_keys:
        if number is None:
            return "neutral"
        return "positive" if number > 0 else "negative" if number < 0 else "neutral"

    return "neutral"


def _format_report_value(value, digits=2, suffix="", thousands=True):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return f"{value}{suffix}"

    if pd.isna(number):
        return f"n/a{suffix}"

    if digits == 0:
        formatted = f"{number:,.0f}" if thousands else f"{number:.0f}"
    else:
        formatted = f"{number:,.{digits}f}" if thousands else f"{number:.{digits}f}"
        formatted = formatted.rstrip("0").rstrip(".")
    return f"{formatted}{suffix}"


def _stats_display_value(df_stats, key, digits=2):
    try:
        value = df_stats.loc[key].iloc[0]
    except Exception:
        return "n/a"

    if isinstance(value, Real):
        return _format_report_value(value, digits=digits)
    if isinstance(value, str):
        try:
            return _format_report_value(float(value), digits=digits)
        except ValueError:
            return value
    return str(value)


def build_stats_detail_html(df_stats):
    groups = {
        "Period": ["Start", "End", "Duration", "Exposure Time [%]"],
        "Performance": [
            "Equity Final [$]",
            "Equity Peak [$]",
            "Return [%]",
            "Buy & Hold Return [%]",
            "Buy & Hold Start Mode",
            "Buy & Hold Start Price",
            "Buy & Hold End Price",
            "Return (Ann.) [%]",
            "CAGR [%]",
            "Alpha [%]",
            "Beta",
        ],
        "Risk": [
            "Max. Drawdown [%]",
            "Avg. Drawdown [%]",
            "Max. Drawdown Duration",
            "Volatility (Ann.) [%]",
            "Sharpe Ratio",
            "Sortino Ratio",
            "Calmar Ratio",
        ],
        "Trades": [
            "# Trades",
            "Win Rate [%]",
            "Best Trade [%]",
            "Worst Trade [%]",
            "Avg. Trade [%]",
            "Max. Trade Duration",
            "Profit Factor",
            "Expectancy [%]",
            "SQN",
            "Kelly Criterion",
        ],
    }

    cards = []
    for group_name, keys in groups.items():
        rows = []
        for key in keys:
            if key not in df_stats.index:
                continue
            digits = 0 if key == "# Trades" else 2
            raw_value = df_stats.loc[key].iloc[0]
            impact_class = _stats_impact_class(key, raw_value)
            rows.append(
                "<div class='bec-stat-row'>"
                f"<span>{key}</span><strong class='bec-impact-{impact_class}'>{_stats_display_value(df_stats, key, digits=digits)}</strong>"
                "</div>"
            )
        if rows:
            cards.append(
                "<div class='bec-detail-card bec-stats-section'>"
                f"<h3>{group_name}</h3>"
                f"{''.join(rows)}"
                "</div>"
            )

    return f"<div class='bec-detail-grid'>{''.join(cards)}</div>"


def build_config_detail_html(backtest_config):
    backtesting = (backtest_config or {}).get("backtesting", {})
    risk = (backtest_config or {}).get("risk_management", {})
    take_profits = (backtest_config or {}).get("take_profits", {})
    strategy_params = (backtest_config or {}).get("strategy_parameters", {})

    def _fmt_config_number(value, digits=2, suffix=""):
        return _format_report_value(value, digits=digits, suffix=suffix)

    def _config_rows(items):
        rows = []
        for item in items:
            value = str(item[1])
            state_class = ""
            impact_class = "neutral"
            if value == "Enabled":
                state_class = "is-enabled"
                impact_class = "positive"
            elif value == "Disabled":
                state_class = "is-disabled"
                impact_class = "negative"

            faded_class = "is-faded" if len(item) > 2 and item[2] else ""
            rows.append(
                f"<div class='bec-stat-row bec-config-row {faded_class} {state_class}'>"
                f"<span>{item[0]}</span><strong class='bec-impact-{impact_class}'>{item[1]}</strong>"
                "</div>"
            )
        return "".join(rows)

    def _config_section(title, items):
        if not items:
            return ""
        return (
            "<div class='bec-detail-card bec-config-section bec-config-list-section'>"
            f"<h3>{title}</h3>"
            f"{_config_rows(items)}"
            "</div>"
        )

    atr_enabled = bool(risk.get("atr_trailing_enabled"))
    risk_items = [
        (
            "Hard Stop",
            _fmt_config_number(risk.get("hard_stop_loss_pct", "n/a"), suffix="%"),
        ),
        ("ATR Trail", "Enabled" if risk.get("atr_trailing_enabled") else "Disabled"),
        (
            "ATR Period",
            _fmt_config_number(risk.get("atr_period", "n/a"), digits=0),
            not atr_enabled,
        ),
        (
            "ATR Multiplier",
            _fmt_config_number(risk.get("atr_multiplier", "n/a")),
            not atr_enabled,
        ),
        (
            "Activation PnL",
            _fmt_config_number(risk.get("atr_activation_pnl_pct", "n/a"), suffix="%"),
            not atr_enabled,
        ),
    ]

    backtesting_items = [
        ("Optimize", "Enabled" if backtesting.get("optimize_enabled") else "Disabled"),
        ("Optimize Metric", backtesting.get("optimize_maximize") or "n/a"),
        ("Initial Cash", _fmt_config_number(backtesting.get("initial_cash", "n/a"))),
        (
            "Commission",
            _fmt_config_number(backtesting.get("commission_pct", "n/a"), suffix="%"),
        ),
    ]

    take_profit_items = []
    take_profits_enabled = bool(take_profits.get("enabled", True))
    take_profit_items.append(
        ("Take Profits", "Enabled" if take_profits_enabled else "Disabled")
    )
    configured_levels = take_profits.get("levels", [])
    if not isinstance(configured_levels, list):
        configured_levels = []
    legacy_levels = [
        {"level": tp_name, **values}
        for tp_name, values in take_profits.items()
        if tp_name != "enabled" and isinstance(values, dict)
    ]
    for values in configured_levels or legacy_levels:
        if not isinstance(values, dict):
            continue
        tp_name = f"tp{values.get('level', '')}"
        pnl_pct = float(values.get("pnl_pct", 0) or 0)
        amount_pct = float(values.get("amount_pct", 0) or 0)
        enabled = take_profits_enabled and pnl_pct > 0
        take_profit_items.extend(
            [
                (
                    f"{str(tp_name).upper()} PnL",
                    _fmt_config_number(pnl_pct, suffix="%"),
                    not enabled,
                ),
                (
                    f"{str(tp_name).upper()} Position Remaining",
                    _fmt_config_number(amount_pct, suffix="%"),
                    not enabled,
                ),
                (
                    f"{str(tp_name).upper()} Status",
                    "Enabled" if enabled else "Disabled",
                ),
            ]
        )

    strategy_items = []
    moving_averages = (
        strategy_params.get("moving_averages", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    market_phase_filter = (
        strategy_params.get("market_phase_filter", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    higher_timeframe_filter = (
        strategy_params.get("higher_timeframe_market_phase_filter", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    if moving_averages:
        strategy_items.extend(
            [
                ("EMA Fast", moving_averages.get("ema_fast", "n/a")),
                ("EMA Slow", moving_averages.get("ema_slow", "n/a")),
            ]
        )
    if market_phase_filter:
        current_phase_timeframe = market_phase_filter.get("timeframe", "current")
        strategy_items.extend(
            [
                (
                    f"{str(current_phase_timeframe).upper()} Phase",
                    "Enabled" if market_phase_filter.get("enabled") else "Disabled",
                ),
                ("SMA Fast", market_phase_filter.get("sma_fast", "n/a")),
                ("SMA Slow", market_phase_filter.get("sma_slow", "n/a")),
            ]
        )
    if higher_timeframe_filter:
        strategy_items.extend(
            [
                (
                    "HTF Phase",
                    "Enabled" if higher_timeframe_filter.get("enabled") else "Disabled",
                ),
                ("HTF Timeframe", higher_timeframe_filter.get("timeframe", "n/a")),
                ("HTF SMA Fast", higher_timeframe_filter.get("sma_fast", "n/a")),
                ("HTF SMA Slow", higher_timeframe_filter.get("sma_slow", "n/a")),
            ]
        )
    hma_rsi_linreg_params = (
        strategy_params.get("hma_rsi_linreg", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    if hma_rsi_linreg_params:
        strategy_items.extend(
            [
                ("HMA Fast", hma_rsi_linreg_params.get("hma_fast", "n/a")),
                ("HMA Slow", hma_rsi_linreg_params.get("hma_slow", "n/a")),
                ("RSI Period", hma_rsi_linreg_params.get("rsi_period", "n/a")),
                ("RSI Min", hma_rsi_linreg_params.get("rsi_min", "n/a")),
                (
                    "Daily LINREG",
                    (
                        "Enabled"
                        if hma_rsi_linreg_params.get("daily_linreg_enabled")
                        else "Disabled"
                    ),
                ),
                (
                    "LINREG Period",
                    hma_rsi_linreg_params.get("daily_linreg_period", "n/a"),
                ),
                (
                    "LINREG TF",
                    hma_rsi_linreg_params.get("daily_linreg_timeframe", "n/a"),
                ),
            ]
        )
    return (
        "<div class='bec-config-visual bec-config-list-grid'>"
        f"{_config_section('Backtest Settings', backtesting_items)}"
        f"{_config_section('Strategy Parameters', strategy_items)}"
        f"{_config_section('Risk Management', risk_items)}"
        f"{_config_section('Take Profits', take_profit_items)}"
        "</div>"
    )


def build_backtesting_tables_html(df_stats, backtest_config, df_trades):
    df_stats_display = df_stats[~df_stats.index.astype(str).str.startswith("_")].copy()
    stats_html = build_stats_detail_html(df_stats_display)

    config_html = ""
    if backtest_config:
        config_html = (
            "<section class='bec-panel'>"
            "<h2>CONFIG</h2>"
            f"{build_config_detail_html(backtest_config)}"
            "</section>"
        )

    trades_display = prepare_backtesting_trades_display(df_trades)
    trades_html_table = trades_display.to_html(
        index=False,
        table_id="trades-table",
        classes="bec-table bec-trades-table display compact stripe",
        border=0,
    )
    trades_html_table = add_backtesting_trade_header_tooltips(trades_html_table)

    return (
        "<div class='bec-info-grid'>"
        "<section class='bec-panel bec-stats-panel'><h2>DETAILED STATISTICS</h2>"
        f"{stats_html}</section>"
        f"{config_html}"
        "</div>"
        "<section class='bec-panel'><h2>TRADES</h2>"
        f"{trades_html_table}</section>"
    )


def _stats_value(df_stats, key, default="n/a", digits=2):
    try:
        value = df_stats.loc[key].iloc[0]
        if pd.isna(value):
            return default
        if isinstance(value, Real):
            return f"{float(value):.{digits}f}"
        if isinstance(value, str):
            try:
                return f"{float(value):.{digits}f}"
            except ValueError:
                return value
        return str(value)
    except Exception:
        return default


def _quality_grade_class(grade):
    grade = str(grade or "").upper()
    return grade if grade in {"A", "B", "C", "D", "F"} else "na"


def _quality_score_note(quality_score):
    penalties = (
        quality_score.get("penalties", {}) if isinstance(quality_score, dict) else {}
    )
    if penalties:
        main_penalty = max(penalties, key=penalties.get)
        return f"Main penalty: {str(main_penalty).replace('_', ' ')}."

    components = (
        quality_score.get("components", {}) if isinstance(quality_score, dict) else {}
    )
    if components:
        weakest_component = min(components, key=components.get)
        return f"Weakest area: {str(weakest_component).replace('_', ' ')}."

    return ""


def _quality_score_highlights(quality_score):
    components = (
        quality_score.get("components", {}) if isinstance(quality_score, dict) else {}
    )
    penalties = (
        quality_score.get("penalties", {}) if isinstance(quality_score, dict) else {}
    )

    strongest = None
    weakest = None
    main_penalty = None
    if components:
        strongest = max(components, key=components.get)
        weakest = min(components, key=components.get)
    if penalties:
        main_penalty = max(penalties, key=penalties.get)

    def _label(value):
        return str(value).replace("_", " ").title() if value else "n/a"

    return {
        "strongest": _label(strongest),
        "weakest": _label(weakest),
        "main_penalty": _label(main_penalty) if main_penalty else "None",
    }


def _quality_metric_label(value):
    label = str(value or "").replace("_score", "").replace("_", " ").title()
    return html.escape(label or "n/a")


def _quality_metric_value(value, default=0.0):
    try:
        number = float(value)
        if pd.isna(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def _quality_detail_text(details):
    if not isinstance(details, dict) or not details:
        return ""
    detail_items = []
    for key, value in details.items():
        if value is None:
            continue
        label = str(key or "").replace("_score", "").replace("_", " ").title()
        detail_items.append(f"{label}: {value}")
    return "; ".join(detail_items)


def _quality_component_tooltips(quality_score):
    details = (
        quality_score.get("component_details", {})
        if isinstance(quality_score, dict)
        else {}
    )
    tooltip_config = {
        "return_score": (
            "Weighted blend of absolute total return (30%), CAGR/annualized return (45%) "
            "and return versus buy & hold (25%).",
            "return",
        ),
        "risk_score": (
            "Weighted blend of max drawdown (40%), average drawdown (15%), drawdown duration (20%) "
            "and Calmar ratio (25%). Lower drawdown and stronger Calmar score higher.",
            "risk",
        ),
        "risk_adjusted_score": (
            "Weighted blend of Sharpe ratio (55%) and Sortino ratio (45%). Higher risk-adjusted returns score higher.",
            "risk_adjusted",
        ),
        "trade_quality_score": (
            "Weighted blend of Profit Factor (30%), expectancy (25%), SQN (25%), win rate (10%) "
            "and trade count quality (10%).",
            "trade_quality",
        ),
        "robustness_score": (
            "Weighted blend of trade count (25%), exposure balance (25%), commission drag (20%), "
            "single-winner dependency (20%) and drawdown robustness (10%).",
            "robustness",
        ),
    }
    tooltips = {}
    for key, (description, detail_key) in tooltip_config.items():
        detail_text = _quality_detail_text(details.get(detail_key, {}))
        tooltips[key] = (
            f"{description} Details: {detail_text}" if detail_text else description
        )
    return tooltips


def _quality_penalty_tooltips():
    return {
        "few_trades": "Applied when the backtest has fewer than 10 trades. Penalty = (10 - trades) / 10 * 12.",
        "limited_trades": "Applied when the backtest has 10 to 19 trades. Penalty = (20 - trades) / 10 * 5.",
        "excessive_drawdown": "Applied when max drawdown is above 50%. Penalty is capped at 15.",
        "aggressive_drawdown": "Applied when max drawdown is between 35% and 50%. Penalty = (drawdown - 35) / 15 * 5.",
        "too_little_exposure": "Applied when exposure time is below 10%. Penalty = (10 - exposure) / 10 * 8.",
        "too_much_exposure": "Applied when exposure time is above 80%. Penalty is capped at 10.",
        "high_commission_drag": "Applied when estimated commissions are more than 30% of net profit. Penalty is capped at 8.",
        "single_winner_dependency": "Applied when the best trade represents more than 50% of total return. Penalty is capped at 8.",
        "weak_vs_buy_hold_with_high_drawdown": "Applied when strategy return is below 30% of buy & hold return while max drawdown is above 35%. Penalty is capped at 10.",
    }


def _quality_bar_rows(items, *, scale=100.0, invert=False, tooltips=None):
    rows = []
    for key, value in (items or {}).items():
        number = _quality_metric_value(value)
        width = max(0.0, min((number / scale) * 100.0 if scale else 0.0, 100.0))
        tooltip = html.escape((tooltips or {}).get(key, ""), quote=True)
        help_html = (
            f'<span class=\'bec-quality-help\' title="{tooltip}" aria-label="{tooltip}">?</span>'
            if tooltip
            else ""
        )
        rows.append(
            "<div class='bec-quality-bar-row'>"
            "<div class='bec-quality-bar-meta'>"
            f"<span><span>{_quality_metric_label(key)}</span>{help_html}</span><strong>{number:.1f}</strong>"
            "</div>"
            f"<div class='bec-quality-bar-track'><i class='{'is-inverted' if invert else ''}' style='--value:{width:.1f}%;'></i></div>"
            "</div>"
        )
    return "".join(rows)


def _quality_score_detail_html(quality_score):
    components = (
        quality_score.get("components", {}) if isinstance(quality_score, dict) else {}
    )
    penalties = (
        quality_score.get("penalties", {}) if isinstance(quality_score, dict) else {}
    )
    components_html = _quality_bar_rows(
        components,
        scale=100.0,
        tooltips=_quality_component_tooltips(quality_score or {}),
    )
    penalties_html = (
        _quality_bar_rows(
            penalties,
            scale=10.0,
            invert=True,
            tooltips=_quality_penalty_tooltips(),
        )
        if penalties
        else "<p class='bec-quality-empty'>No explicit penalties</p>"
    )
    return (
        "<div class='bec-quality-detail-list'>"
        "<section><h3>Components</h3>"
        f"{components_html or '<p class=\"bec-quality-empty\">No component scores</p>'}"
        "</section>"
        "<section><h3>Penalties</h3>"
        f"{penalties_html}"
        "</section>"
        "</div>"
    )


def _stats_float(df_stats, key, default=None):
    try:
        value = df_stats.loc[key].iloc[0]
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _value_tone(value, invert=False, neutral_zero=True):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "neutral"
    if neutral_zero and abs(number) < 1e-12:
        return "neutral"
    is_good = number < 0 if invert else number > 0
    return "positive" if is_good else "negative"


def _metric_card(kind, label, value, note="", tone="neutral"):
    return (
        f"<div class='bec-perf-card bec-perf-card-{kind} bec-tone-{tone}'>"
        f"<span>{label}</span><strong>{value}</strong>"
        f"{f'<small>{note}</small>' if note else ''}"
        "</div>"
    )


def _trade_win_loss_note(df_trades):
    if not isinstance(df_trades, pd.DataFrame) or df_trades.empty:
        return "Completed trades"

    return_col = None
    if "ReturnPct" in df_trades.columns:
        return_col = pd.to_numeric(df_trades["ReturnPct"], errors="coerce") * 100
    elif "Return_Pct" in df_trades.columns:
        return_col = pd.to_numeric(df_trades["Return_Pct"], errors="coerce")

    if return_col is None:
        return "Completed trades"

    wins = int((return_col > 0).sum())
    losses = int((return_col <= 0).sum())
    return f"{wins} wins / {losses} losses"


def build_backtesting_report_header(
    df_stats, strategy_name, timeframe, symbol, quality_score=None, df_trades=None
):
    score_value = None
    grade = "n/a"
    grade_class = "na"
    highlights = {"strongest": "n/a", "weakest": "n/a", "main_penalty": "None"}
    if quality_score:
        score_value = float(quality_score.get("score", 0.0) or 0.0)
        grade = str(quality_score.get("grade", "n/a"))
        grade_class = _quality_grade_class(grade)
        highlights = _quality_score_highlights(quality_score)

    score_display = f"{score_value:.1f}" if score_value is not None else "n/a"
    score_ring_value = max(0.0, min(float(score_value or 0.0), 100.0))
    return_value = _stats_float(df_stats, "Return [%]", 0.0)
    buy_hold_value = _stats_float(df_stats, "Buy & Hold Return [%]", 0.0)
    drawdown_value = _stats_float(df_stats, "Max. Drawdown [%]", 0.0)
    winrate_value = _stats_float(df_stats, "Win Rate [%]", 0.0)
    sharpe_value = _stats_float(df_stats, "Sharpe Ratio", 0.0)
    profit_factor_value = _stats_float(df_stats, "Profit Factor", 0.0)
    avg_trade_value = _stats_float(df_stats, "Avg. Trade [%]", 0.0)
    trades_value = _stats_float(df_stats, "# Trades", 0.0)
    equity_final = _stats_value(df_stats, "Equity Final [$]", digits=2)
    initial_cash = "n/a"
    equity_start = _stats_float(df_stats, "Equity Final [$]", None)
    return_pct = _stats_float(df_stats, "Return [%]", None)
    if equity_start is not None and return_pct not in (None, -100):
        initial_cash = f"${equity_start / (1 + return_pct / 100):,.0f}"
    equity_final_note = f"{initial_cash} -> ${float(_stats_float(df_stats, 'Equity Final [$]', 0.0)):,.0f}"
    if buy_hold_value:
        equity_final_note = f"{equity_final_note} | B&H {buy_hold_value:+.2f}%"

    quality_detail_html = _quality_score_detail_html(quality_score or {})
    top_cards_html = (
        f"<div class='bec-quality-dial bec-quality-grade-{grade_class}' style='--score:{score_ring_value};'>"
        "<div class='bec-quality-summary'>"
        "<div class='bec-quality-visual'>"
        "<div class='bec-quality-ring'>"
        f"<strong>{score_display}</strong><small>Quality Score</small>"
        "</div>"
        f"<span>Grade {grade}</span>"
        "</div>"
        "<div class='bec-quality-breakdown'>"
        f"<p><span>Strongest</span><strong>{highlights['strongest']}</strong></p>"
        f"<p><span>Weakest</span><strong>{highlights['weakest']}</strong></p>"
        f"<p><span>Main Penalty</span><strong>{highlights['main_penalty']}</strong></p>"
        "</div>"
        "</div>"
        f"{quality_detail_html}"
        "</div>"
    )
    secondary_cards_html = "".join(
        [
            _metric_card(
                "return",
                "Total Return",
                f"{return_value:+.2f}%",
                equity_final_note,
                _value_tone(return_value),
            ),
            _metric_card(
                "drawdown",
                "Max Drawdown",
                f"{drawdown_value:.2f}%",
                "Worst peak-to-trough",
                "negative" if float(drawdown_value or 0.0) < 0 else "positive",
            ),
            _metric_card(
                "winrate",
                "Win Rate",
                f"{winrate_value:.2f}%",
                "Completed winning trades",
                _value_tone(winrate_value - 50),
            ),
            _metric_card(
                "sharpe",
                "Sharpe Ratio",
                f"{sharpe_value:.2f}",
                "Risk-adjusted return",
                _value_tone(sharpe_value - 0.8),
            ),
            _metric_card(
                "profit",
                "Profit Factor",
                f"{profit_factor_value:.2f}",
                "Gross profit / loss",
                _value_tone(profit_factor_value - 1),
            ),
            _metric_card(
                "avgtrade",
                "Avg Trade",
                f"{avg_trade_value:+.2f}%",
                "Mean return per trade",
                _value_tone(avg_trade_value),
            ),
        ]
    ).replace("Completed winning trades", _trade_win_loss_note(df_trades), 1)

    return (
        "<section class='bec-report-shell'>"
        "<header class='bec-topbar'>"
        "<div>"
        "<p class='bec-kicker'><span></span>Backtest Report</p>"
        f"<h1>{symbol}</h1>"
        f"<p class='bec-subtitle'><strong>{strategy_name}</strong><span>{timeframe}</span></p>"
        "</div>"
        "<div class='bec-report-actions'>"
        "<div class='bec-share-menu'>"
        "<button class='bec-share-toggle' type='button' data-share-toggle aria-haspopup='true' aria-expanded='false' aria-label='Share report' title='Share report'><span class='material-symbols-outlined'>share</span></button>"
        "<div class='bec-share-options' data-share-options>"
        "<button type='button' data-download-html>Download HTML</button>"
        "<button type='button' data-export-pdf>Export PDF</button>"
        "</div>"
        "</div>"
        "<button class='bec-theme-toggle' type='button' data-theme-toggle aria-label='Toggle theme' title='Toggle theme'><span class='material-symbols-outlined'>dark_mode</span></button>"
        "</div>"
        "</header>"
        f"<div class='bec-performance-grid bec-performance-grid-top'>{top_cards_html}</div>"
        f"<div class='bec-performance-grid bec-performance-grid-secondary'>{secondary_cards_html}</div>"
        "</section>"
    )


def _plotly_html(fig, include_plotlyjs=False):
    return fig.to_html(
        full_html=False,
        include_plotlyjs="cdn" if include_plotlyjs else False,
        config={"displayModeBar": True, "responsive": True},
    )


def build_performance_charts_html(bt, stats):
    equity_curve = stats.get("_equity_curve") if hasattr(stats, "get") else None
    if not isinstance(equity_curve, pd.DataFrame) or equity_curve.empty:
        return ""

    charts = []
    equity = pd.to_numeric(equity_curve.get("Equity"), errors="coerce")
    equity = equity.dropna()
    close = None
    data = getattr(bt, "_data", None)
    if isinstance(data, pd.DataFrame) and "Close" in data.columns:
        close = (
            pd.to_numeric(data["Close"], errors="coerce")
            .reindex(equity.index)
            .ffill()
            .dropna()
        )

    if not equity.empty:
        equity_fig = go.Figure()
        equity_fig.add_trace(
            go.Scatter(
                x=equity.index,
                y=equity,
                mode="lines",
                name="Equity Curve",
                line={"color": "#14f1d9", "width": 2},
            )
        )
        if close is not None and not close.empty and float(close.iloc[0]) != 0:
            aligned_equity = equity.reindex(close.index).ffill()
            buy_hold = float(aligned_equity.iloc[0]) * (close / float(close.iloc[0]))
            equity_fig.add_trace(
                go.Scatter(
                    x=buy_hold.index,
                    y=buy_hold,
                    mode="lines",
                    name="Buy & Hold",
                    line={"color": "#facc15", "width": 2, "dash": "dot"},
                )
            )
        equity_fig.update_layout(
            height=330,
            margin={"l": 45, "r": 20, "t": 22, "b": 38},
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#8190b5", "size": 11},
            legend={"orientation": "h", "y": 1.12, "x": 0},
            xaxis={
                "gridcolor": "rgba(129,144,181,0.16)",
                "zerolinecolor": "rgba(129,144,181,0.20)",
            },
            yaxis={
                "gridcolor": "rgba(129,144,181,0.16)",
                "zerolinecolor": "rgba(129,144,181,0.20)",
            },
        )
        charts.append(
            "<section class='bec-chart-card'><header><strong>Equity Curve</strong><span>vs Buy & Hold</span></header>"
            f"{_plotly_html(equity_fig, include_plotlyjs=True)}</section>"
        )

    drawdown = pd.to_numeric(equity_curve.get("DrawdownPct"), errors="coerce").dropna()
    if not drawdown.empty:
        drawdown_pct = -drawdown.abs() * 100.0
        drawdown_fig = go.Figure()
        drawdown_fig.add_trace(
            go.Scatter(
                x=drawdown_pct.index,
                y=drawdown_pct,
                mode="lines",
                name="Drawdown",
                line={"color": "#ff4444", "width": 2},
                fill="tozeroy",
                fillcolor="rgba(255,68,68,0.22)",
            )
        )
        drawdown_fig.update_layout(
            height=330,
            margin={"l": 45, "r": 20, "t": 22, "b": 38},
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#8190b5", "size": 11},
            showlegend=False,
            xaxis={
                "gridcolor": "rgba(129,144,181,0.16)",
                "zerolinecolor": "rgba(129,144,181,0.20)",
            },
            yaxis={
                "gridcolor": "rgba(129,144,181,0.16)",
                "zerolinecolor": "rgba(129,144,181,0.20)",
                "ticksuffix": "%",
            },
        )
        charts.append(
            "<section class='bec-chart-card'><header><strong>Drawdown</strong><span>Peak-to-trough</span></header>"
            f"{_plotly_html(drawdown_fig, include_plotlyjs=not charts)}</section>"
        )

    if not charts:
        return ""

    return "<section class='bec-charts-grid'>" + "".join(charts) + "</section>"


def save_backtesting_to_csv(csv_file_path, df_stats, backtest_config, df_trades):
    df_stats_display = df_stats[~df_stats.index.astype(str).str.startswith("_")].copy()
    config_display = flatten_backtesting_config(backtest_config)
    trades_display = prepare_backtesting_trades_display(df_trades)

    with open(csv_file_path, "w", encoding="utf-8") as file:
        file.write("# STATS\n")
        df_stats_display.to_csv(file, index=True)
        file.write("\n# CONFIG\n")
        config_display.to_csv(file, index=False)
        file.write("\n# TRADES\n")
        trades_display.to_csv(file, index=False)


def save_backtesting_to_html(
    bt,
    stats,
    strategy,
    timeframe,
    symbol,
    backtest_config=None,
    report_strategy_name=None,
):
    # stats
    df_stats = pd.DataFrame(stats)
    # trades
    df_trades = build_backtesting_trades_df(stats, strategy=strategy)
    quality_score = (backtest_config or {}).get("strategy_quality_score_result")

    strategy_name = report_strategy_name or get_base_strategy_name(strategy)
    filename = f"{strategy_name} - {timeframe} - {symbol}"

    # Create the folder if it doesn't exist
    if not os.path.exists(FOLDER_BACKTEST_RESULTS):
        os.makedirs(FOLDER_BACKTEST_RESULTS)

    # Specify the CSV file path
    csv_file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename + ".csv")

    save_backtesting_to_csv(csv_file_path, df_stats, backtest_config, df_trades)

    filename_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename)

    bt.plot(
        # plot_return = True,
        # plot_drawdown = True,
        filename=filename_path,
        open_browser=False,
    )

    # add stats and trade to html file

    # add style
    html_file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename + ".html")
    with open(html_file_path, "r") as file:
        html_content = file.read()

    material_symbols_link = '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&icon_names=dark_mode,light_mode,share" />'
    if "Material+Symbols+Outlined" not in html_content:
        head_tag_end = html_content.find("</head>")
        if head_tag_end >= 0:
            html_content = (
                html_content[:head_tag_end]
                + material_symbols_link
                + html_content[head_tag_end:]
            )

    html_content = enrich_backtesting_plot_tooltips(html_content, df_trades)
    with open(html_file_path, "w") as file:
        file.write(html_content)

    # Locate the style tag in the HTML content
    style_tag_start = html_content.find("<style>")
    if style_tag_start == -1:
        head_tag_end = html_content.find("</head>")

        style_content_to_add = """<style>
</style>"""
        modified_html_content = (
            html_content[: head_tag_end - 1]
            + style_content_to_add
            + html_content[head_tag_end - 1 :]
        )
        with open(html_file_path, "w") as file:
            file.write(modified_html_content)
    # -----

    with open(html_file_path, "r") as file:
        html_content = file.read()
    # Locate the style tag in the HTML content
    style_tag_start = html_content.find("<style>")
    style_tag_end = html_content.find("</style>", style_tag_start)

    # Append or modify the content of the style tag

    # dataframe {
    #     text-align: left;
    # }
    style_content_to_add = """
        :root {
            --bec-bg: #f5f7fb;
            --bec-surface: #ffffff;
            --bec-surface-soft: #f8fafc;
            --bec-border: #dbe4ee;
            --bec-text: #172033;
            --bec-muted: #64748b;
            --bec-green: #10b981;
            --bec-blue: #2563eb;
            --bec-cyan: #0891b2;
            --bec-red: #ef4444;
            --bec-amber: #f59e0b;
        }
        body {
            background:
                radial-gradient(circle at top left, rgba(16, 185, 129, 0.10), transparent 28%),
                linear-gradient(180deg, #f8fbff 0%, var(--bec-bg) 42%, #eef3f8 100%);
            color: var(--bec-text);
            font-family: "Segoe UI", Helvetica, Arial, sans-serif;
        }
        .bec-report-hero,
        .bec-panel {
            width: min(96%, 1500px);
            margin: 22px auto;
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid var(--bec-border);
            border-radius: 18px;
            box-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
        }
        .bec-info-grid {
            width: min(96%, 1500px);
            margin: 22px auto;
            display: grid;
            grid-template-columns: 1fr;
            gap: 18px;
            align-items: start;
        }
        .bec-info-grid .bec-panel {
            width: auto;
            margin: 0;
            min-height: 0;
        }
        .bec-report-hero { padding: 24px; }
        .bec-panel { padding: 20px 22px; }
        .bec-panel h2 {
            margin: 0 0 16px 0;
            color: var(--bec-text);
            font-size: 14px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
        }
        .bec-kicker {
            display: flex;
            align-items: center;
            gap: 8px;
            margin: 0 0 8px 0;
            color: var(--bec-muted);
            font-size: 12px;
            font-weight: 750;
            letter-spacing: 0.16em;
            text-transform: uppercase;
        }
        .bec-kicker span {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: var(--bec-green);
            box-shadow: 0 0 0 5px rgba(16, 185, 129, 0.13);
        }
        .bec-report-hero h1 {
            margin: 0;
            color: var(--bec-text);
            font-size: clamp(28px, 4vw, 44px);
            line-height: 1.05;
            letter-spacing: -0.04em;
        }
        .bec-subtitle {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 10px;
            margin: 10px 0 0 0;
            color: var(--bec-muted);
            font-size: 14px;
        }
        .bec-subtitle strong { color: var(--bec-text); }
        .bec-subtitle span {
            display: inline-flex;
            align-items: center;
            border: 1px solid var(--bec-border);
            border-radius: 999px;
            padding: 3px 9px;
            background: var(--bec-surface-soft);
            color: var(--bec-blue);
            font-weight: 700;
        }
        .bec-detail-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
            gap: 14px;
        }
        .bec-stats-panel {
            background: var(--bec-surface);
        }
        .bec-stats-panel .bec-detail-grid {
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
        }
        .bec-detail-card,
        .bec-config-section {
            position: relative;
            overflow: hidden;
            border: 1px solid var(--bec-border);
            border-radius: 16px;
            background:
                radial-gradient(circle at top right, rgba(37, 99, 235, 0.08), transparent 30%),
                linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
        }
        .bec-detail-card { padding: 14px; }
        .bec-stats-section {
            background: var(--bec-surface-soft);
            box-shadow: none;
        }
        .bec-detail-card h3,
        .bec-config-section h3 {
            margin: 0 0 12px 0;
            color: #475569;
            font-size: 11px;
            font-weight: 850;
            letter-spacing: 0.14em;
            text-transform: uppercase;
        }
        .bec-stat-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 14px;
            min-height: 36px;
            padding: 0;
            border-top: 1px solid var(--bec-border);
        }
        .bec-stat-row:first-of-type { border-top: 0; }
        .bec-stat-row span {
            color: var(--bec-muted);
            font-size: 12px;
            font-weight: 700;
            line-height: 1.25;
        }
        .bec-stat-row strong {
            color: var(--bec-text);
            font-size: 14px;
            font-weight: 850;
            text-align: right;
            white-space: nowrap;
        }
        .bec-stat-row strong.bec-impact-positive { color: var(--bec-green); }
        .bec-stat-row strong.bec-impact-negative { color: var(--bec-red); }
        .bec-stat-row strong.bec-impact-neutral { color: var(--bec-text); }
        .bec-stat-row.is-faded span,
        .bec-stat-row.is-faded strong {
            color: var(--bec-muted) !important;
            opacity: 0.52;
        }
        .bec-config-visual {
            display: grid;
            gap: 14px;
        }
        .bec-config-list-grid {
            grid-template-columns: repeat(4, minmax(0, 1fr));
        }
        .bec-config-section {
            padding: 14px;
        }
        .bec-config-list-section {
            background: var(--bec-surface-soft);
            box-shadow: none;
        }
        .bec-config-row strong {
            color: var(--bec-text);
        }
        .bec-config-row.is-enabled span,
        .bec-config-row.is-enabled strong {
            color: var(--bec-green);
        }
        .bec-config-row.is-disabled span,
        .bec-config-row.is-disabled strong {
            color: var(--bec-red);
        }
        .bec-config-pill-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 10px;
        }
        .bec-config-pill {
            border: 1px solid #dce8f3;
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.76);
            padding: 11px 12px;
        }
        .bec-config-pill span,
        .bec-tp-card span {
            display: block;
            color: var(--bec-muted);
            font-size: 10px;
            font-weight: 850;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }
        .bec-config-pill strong {
            display: block;
            margin-top: 6px;
            color: var(--bec-text);
            font-size: 18px;
            letter-spacing: -0.03em;
        }
        .bec-tp-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 10px;
        }
        .bec-tp-card {
            position: relative;
            border: 1px solid rgba(16, 185, 129, 0.24);
            border-radius: 14px;
            background: linear-gradient(180deg, #ecfdf5 0%, #ffffff 100%);
            padding: 12px;
        }
        .bec-tp-card::before {
            content: "";
            position: absolute;
            inset: 10px auto 10px 0;
            width: 3px;
            border-radius: 999px;
            background: var(--bec-green);
        }
        .bec-tp-card.is-disabled {
            border-color: #e2e8f0;
            background: linear-gradient(180deg, #f8fafc 0%, #ffffff 100%);
            opacity: 0.72;
        }
        .bec-tp-card.is-disabled::before { background: #94a3b8; }
        .bec-tp-card strong {
            display: block;
            margin-top: 8px;
            color: var(--bec-text);
            font-size: 22px;
            letter-spacing: -0.04em;
        }
        .bec-tp-card em {
            display: block;
            margin-top: 3px;
            color: var(--bec-muted);
            font-size: 12px;
            font-style: normal;
        }
        .bec-tp-card small {
            display: inline-flex;
            margin-top: 10px;
            border-radius: 999px;
            background: rgba(16, 185, 129, 0.10);
            color: #047857;
            padding: 4px 8px;
            font-size: 11px;
            font-weight: 800;
        }
        .bec-tp-card.is-disabled small {
            background: #eef2f7;
            color: #64748b;
        }
        @media screen and (max-width: 900px) {
            .bec-info-grid {
                grid-template-columns: 1fr;
            }
            .bec-stats-panel .bec-detail-grid,
            .bec-config-list-grid {
                grid-template-columns: 1fr;
            }
        }
        .bec-table {
            width: auto;
            max-width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 13px;
            table-layout: auto;
            overflow: hidden;
            border-radius: 12px;
        }
        .bec-compact-table {
            display: inline-table;
            border: 1px solid var(--bec-border);
        }
        .bec-trades-table { width: 100%; display: table; }
        .bec-table th {
            background: #eef4fb;
            color: #475569;
            font-weight: 750;
            text-align: left;
            border-bottom: 1px solid var(--bec-border);
            padding: 9px 11px;
            white-space: nowrap;
        }
        .bec-table td {
            border-bottom: 1px solid #edf2f7;
            padding: 8px 11px;
            white-space: nowrap;
        }
        .bec-table tbody tr:hover { background-color: #f8fafc; }
        .bec-stats-table td:first-child,
        .bec-stats-table th:first-child,
        .bec-config-table td:first-child,
        .bec-config-table th:first-child,
        .bec-config-table td:nth-child(2),
        .bec-config-table th:nth-child(2) {
            background: #f1f6fb;
            color: #1f334d;
            font-weight: 750;
        }
        .bec-config-table td:nth-child(3),
        .bec-config-table th:nth-child(3) { text-align: right; }
        .bec-loss-row { background-color: #fff1f2 !important; }
        .dataTables_wrapper { font-size: 13px; }
        .bec-trades-download {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 32px;
            margin-right: 10px;
            border: 1px solid var(--bec-border);
            border-radius: 8px;
            background: var(--bec-surface-soft);
            color: var(--bec-text);
            cursor: pointer;
            font-size: 12px;
            font-weight: 800;
            line-height: 1;
            padding: 8px 11px;
            white-space: nowrap;
        }
        .bec-trades-download:hover {
            border-color: var(--bec-blue);
            color: var(--bec-blue);
        }
        .bec-trades-toolbar {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
            flex-wrap: wrap;
            margin: 0 0 12px 0;
        }
        .bec-trades-search {
            min-height: 32px;
            border: 1px solid var(--bec-border);
            border-radius: 8px;
            background: var(--bec-surface);
            color: var(--bec-text);
            font-size: 12px;
            padding: 7px 10px;
            width: min(260px, 100%);
        }
        .dt-search,
        .dataTables_filter {
            display: none !important;
        }
        html:not([data-bec-theme]),
        html[data-bec-theme="light"] {
            --bec-bg: #f5f7fb;
            --bec-surface: #ffffff;
            --bec-surface-soft: #f8fafc;
            --bec-border: #dbe4ee;
            --bec-text: #172033;
            --bec-muted: #64748b;
            --bec-green: #10b981;
            --bec-blue: #2563eb;
            --bec-cyan: #0891b2;
            --bec-red: #ef4444;
            --bec-amber: #f59e0b;
        }
        html[data-bec-theme="dark"] {
            --bec-bg: #0f1018;
            --bec-surface: #151620;
            --bec-surface-soft: #1c1e2b;
            --bec-border: #25283a;
            --bec-text: #f4f7ff;
            --bec-muted: #8993bd;
            --bec-green: #00e596;
            --bec-blue: #7c7cff;
            --bec-cyan: #14f1d9;
            --bec-red: #ff4444;
            --bec-amber: #facc15;
        }
        body {
            background: var(--bec-bg) !important;
            color: var(--bec-text) !important;
        }
        .bec-report-shell,
        .bec-panel,
        .bec-chart-card {
            width: min(96%, 1500px);
            margin: 22px auto;
            background: var(--bec-surface);
            border: 1px solid var(--bec-border);
            border-radius: 18px;
            box-shadow: none;
        }
        .bec-report-shell {
            padding: 22px;
        }
        .bec-topbar {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 20px;
        }
        .bec-topbar h1 {
            margin: 0;
            color: var(--bec-text);
            font-size: clamp(34px, 5vw, 56px);
            line-height: 0.95;
            letter-spacing: 0;
        }
        .bec-report-actions {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .bec-share-menu {
            position: relative;
        }
        .bec-share-toggle {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 36px;
            height: 36px;
            border: 1px solid var(--bec-border);
            border-radius: 999px;
            background: var(--bec-surface-soft);
            color: var(--bec-text);
            cursor: pointer;
            font-size: 18px;
            font-weight: 850;
            line-height: 1;
            padding: 0;
        }
        .bec-share-toggle:hover,
        .bec-theme-toggle:hover {
            border-color: var(--bec-blue);
            color: var(--bec-blue);
        }
        .bec-share-toggle .material-symbols-outlined {
            font-size: 20px;
            font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 24;
            line-height: 1;
        }
        .bec-theme-toggle .material-symbols-outlined {
            font-size: 20px;
            font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 24;
            line-height: 1;
        }
        .bec-share-options {
            position: absolute;
            top: calc(100% + 8px);
            right: 0;
            z-index: 20;
            display: none;
            min-width: 160px;
            overflow: hidden;
            border: 1px solid var(--bec-border);
            border-radius: 10px;
            background: var(--bec-surface);
            box-shadow: 0 14px 32px rgba(15, 23, 42, 0.14);
        }
        .bec-share-menu.is-open .bec-share-options {
            display: block;
        }
        .bec-share-menu:focus-within .bec-share-options {
            display: block;
        }
        .bec-share-options button {
            display: block;
            width: 100%;
            border: 0;
            background: transparent;
            color: var(--bec-text);
            cursor: pointer;
            font-size: 12px;
            font-weight: 750;
            padding: 10px 12px;
            text-align: left;
            white-space: nowrap;
        }
        .bec-share-options button:hover {
            background: var(--bec-surface-soft);
            color: var(--bec-blue);
        }
        .bec-theme-toggle {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 36px;
            height: 36px;
            border: 1px solid var(--bec-border);
            border-radius: 999px;
            background: var(--bec-surface-soft);
            color: var(--bec-text);
            cursor: pointer;
            font-size: 18px;
            font-weight: 800;
            line-height: 1;
            padding: 0;
        }
        .bec-insight-row {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 18px;
            margin-top: 20px;
            border-top: 1px solid var(--bec-border);
            padding-top: 18px;
        }
        .bec-insight-row p {
            margin: 0;
            min-width: 0;
        }
        .bec-insight-row span,
        .bec-perf-card span,
        .bec-quality-dial > span,
        .bec-chart-card header span {
            display: block;
            color: var(--bec-muted);
            font-size: 11px;
            font-weight: 850;
            letter-spacing: 0.13em;
            text-transform: uppercase;
        }
        .bec-insight-row strong {
            display: block;
            margin-top: 6px;
            color: var(--bec-text);
            font-size: 14px;
            line-height: 1.25;
        }
        .bec-performance-grid {
            display: grid;
            gap: 18px;
            margin-top: 22px;
        }
        .bec-performance-grid-top {
            grid-template-columns: 1fr;
        }
        .bec-performance-grid-secondary {
            grid-template-columns: repeat(6, minmax(0, 1fr));
            margin-top: 18px;
        }
        .bec-quality-dial,
        .bec-perf-card {
            min-height: 108px;
            border: 1px solid var(--bec-border);
            border-radius: 14px;
            background: var(--bec-surface-soft);
            padding: 18px;
        }
        .bec-quality-dial {
            display: grid;
            grid-template-columns: minmax(420px, 0.95fr) minmax(0, 1.05fr);
            gap: 22px;
            align-items: center;
        }
        .bec-performance-grid-top .bec-quality-dial,
        .bec-performance-grid-top .bec-perf-card {
            min-height: 150px;
            padding: 24px 26px;
        }
        .bec-quality-visual {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 10px;
        }
        .bec-quality-ring {
            display: flex;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            width: 118px;
            height: 118px;
            border-radius: 999px;
            background:
                radial-gradient(circle at center, var(--bec-surface-soft) 58%, transparent 59%),
                conic-gradient(var(--grade-color, var(--bec-cyan)) calc(var(--score) * 1%), rgba(129, 144, 181, 0.20) 0);
        }
        .bec-quality-ring strong {
            color: var(--grade-color, var(--bec-cyan));
            font-size: 30px;
            line-height: 1;
        }
        .bec-quality-ring small {
            margin-top: 5px;
            color: var(--bec-muted);
            font-size: 11px;
        }
        .bec-quality-dial > span {
            color: var(--grade-color, var(--bec-text));
            letter-spacing: 0;
            text-transform: none;
        }
        .bec-quality-visual > span {
            color: var(--grade-color, var(--bec-text));
            font-size: 12px;
            font-weight: 850;
        }
        .bec-quality-summary {
            display: grid;
            grid-template-columns: minmax(130px, 0.85fr) minmax(220px, 1.15fr);
            align-items: center;
            gap: 20px;
        }
        .bec-quality-breakdown {
            min-width: 0;
        }
        .bec-quality-breakdown p {
            display: grid;
            grid-template-columns: minmax(104px, max-content) minmax(0, 1fr);
            gap: 14px;
            align-items: baseline;
            margin: 0;
            padding: 10px 0;
            border-top: 1px solid var(--bec-border);
        }
        .bec-quality-breakdown p:first-child {
            border-top: 0;
        }
        .bec-quality-breakdown span {
            color: var(--bec-muted);
            font-size: 11px;
            font-weight: 850;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }
        .bec-quality-breakdown strong {
            min-width: 0;
            color: var(--bec-text);
            font-size: 13px;
            line-height: 1.25;
            text-align: right;
            overflow-wrap: anywhere;
        }
        .bec-quality-detail-list {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 18px;
            padding-left: 22px;
            border-left: 1px solid var(--bec-border);
        }
        .bec-quality-detail-list section {
            min-width: 0;
        }
        .bec-quality-detail-list h3 {
            margin: 0 0 10px;
            color: var(--bec-muted);
            font-size: 11px;
            font-weight: 850;
            letter-spacing: 0.13em;
            line-height: 1;
            text-transform: uppercase;
        }
        .bec-quality-bar-row {
            display: grid;
            gap: 5px;
            margin-top: 8px;
        }
        .bec-quality-bar-meta {
            display: grid;
            grid-template-columns: minmax(0, 1fr) auto;
            gap: 10px;
            align-items: baseline;
        }
        .bec-quality-bar-meta span,
        .bec-quality-bar-meta strong {
            color: var(--bec-text);
            font-size: 12px;
            font-weight: 750;
            line-height: 1.15;
        }
        .bec-quality-bar-meta span {
            min-width: 0;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            overflow: hidden;
            white-space: nowrap;
        }
        .bec-quality-bar-meta span span {
            display: block;
            min-width: 0;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .bec-quality-bar-meta strong {
            font-variant-numeric: tabular-nums;
        }
        .bec-quality-bar-meta .bec-quality-help {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            flex: 0 0 auto;
            width: 14px;
            height: 14px;
            border: 1px solid var(--bec-border);
            border-radius: 999px;
            background: transparent;
            color: var(--bec-muted);
            cursor: help;
            font-size: 9px;
            font-weight: 900;
            line-height: 1;
        }
        .bec-quality-bar-meta .bec-quality-help:hover {
            border-color: var(--bec-blue);
            color: var(--bec-blue);
        }
        .bec-quality-bar-track {
            position: relative;
            height: 4px;
            overflow: hidden;
            border-radius: 999px;
            background: rgba(100, 116, 139, 0.22);
        }
        .bec-quality-bar-track i {
            position: absolute;
            inset: 0;
            display: block;
            border-radius: inherit;
            background: linear-gradient(90deg, #ef4444 0%, #f59e0b 48%, #22c55e 100%);
            clip-path: inset(0 calc(100% - var(--value, 0%)) 0 0);
        }
        .bec-quality-bar-track i.is-inverted {
            background: linear-gradient(90deg, #22c55e 0%, #f59e0b 48%, #ef4444 100%);
        }
        .bec-quality-empty {
            margin: 0;
            color: var(--bec-muted);
            font-size: 12px;
            line-height: 1.35;
        }
        .bec-perf-card strong {
            display: block;
            margin-top: 10px;
            color: var(--bec-text);
            font-size: 25px;
            line-height: 1;
            letter-spacing: 0;
        }
        .bec-perf-card small {
            display: block;
            margin-top: 12px;
            color: var(--bec-muted);
            font-size: 12px;
        }
        .bec-tone-positive strong { color: var(--bec-green); }
        .bec-tone-negative strong { color: var(--bec-red); }
        .bec-tone-neutral strong { color: var(--bec-text); }
        .bec-perf-card-return strong,
        .bec-perf-card-drawdown strong {
            font-size: 25px;
        }
        .bec-quality-dial.bec-quality-grade-A,
        .bec-quality-dial.bec-quality-grade-B { --grade-color: var(--bec-green); }
        .bec-quality-dial.bec-quality-grade-C { --grade-color: var(--bec-amber); }
        .bec-quality-dial.bec-quality-grade-D { --grade-color: #f97316; }
        .bec-quality-dial.bec-quality-grade-F { --grade-color: var(--bec-red); }
        .bec-charts-grid {
            width: min(96%, 1500px);
            margin: 22px auto;
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 18px;
        }
        .bec-chart-card {
            width: auto;
            margin: 0;
            padding: 18px;
        }
        .bec-chart-card header {
            display: flex;
            align-items: baseline;
            gap: 8px;
            margin-bottom: 4px;
        }
        .bec-chart-card header strong {
            color: var(--bec-text);
            font-size: 13px;
            letter-spacing: 0.10em;
            text-transform: uppercase;
        }
        html[data-bec-theme="dark"] .bec-detail-card,
        html[data-bec-theme="dark"] .bec-config-section,
        html[data-bec-theme="dark"] .bec-config-pill,
        html[data-bec-theme="dark"] .bec-tp-card {
            background: var(--bec-surface-soft);
            border-color: var(--bec-border);
            box-shadow: none;
        }
        html[data-bec-theme="dark"] .bec-table th,
        html[data-bec-theme="dark"] .bec-stats-table td:first-child,
        html[data-bec-theme="dark"] .bec-stats-table th:first-child,
        html[data-bec-theme="dark"] .bec-config-table td:first-child,
        html[data-bec-theme="dark"] .bec-config-table th:first-child,
        html[data-bec-theme="dark"] .bec-config-table td:nth-child(2),
        html[data-bec-theme="dark"] .bec-config-table th:nth-child(2) {
            background: #1c1e2b;
            color: var(--bec-muted);
        }
        html[data-bec-theme="dark"] .bec-table td,
        html[data-bec-theme="dark"] .bec-stat-row {
            border-color: var(--bec-border);
        }
        html[data-bec-theme="dark"] .bec-table tbody tr:hover {
            background-color: #1b1d2a;
        }
        html[data-bec-theme="dark"] .bec-loss-row {
            background-color: rgba(255, 68, 68, 0.11) !important;
        }
        @media screen and (max-width: 1000px) {
            .bec-performance-grid,
            .bec-performance-grid-top,
            .bec-performance-grid-secondary,
            .bec-charts-grid,
            .bec-insight-row {
                grid-template-columns: 1fr;
            }
            .bec-quality-dial,
            .bec-quality-summary,
            .bec-quality-detail-list,
            .bec-quality-breakdown p {
                grid-template-columns: 1fr;
            }
            .bec-quality-breakdown strong {
                text-align: left;
            }
            .bec-quality-detail-list {
                padding-left: 0;
                padding-top: 16px;
                border-left: 0;
                border-top: 1px solid var(--bec-border);
            }
            .bec-topbar {
                flex-direction: column;
            }
            .bec-report-actions {
                align-self: flex-end;
            }
        }
        @media print {
            @page {
                size: landscape;
                margin: 10mm;
            }
            * {
                -webkit-print-color-adjust: exact !important;
                print-color-adjust: exact !important;
            }
            .bec-report-actions,
            .bec-trades-toolbar,
            .dt-search,
            .dataTables_filter,
            .dt-length,
            .dt-info,
            .dt-paging,
            .dataTables_length,
            .dataTables_info,
            .dataTables_paginate {
                display: none !important;
            }
            body {
                background: #ffffff !important;
            }
            .bec-report-shell,
            .bec-panel,
            .bec-chart-card {
                box-shadow: none !important;
                break-inside: avoid;
            }
        }
        """
    modified_html_content = (
        html_content[:style_tag_end]
        + style_content_to_add
        + html_content[style_tag_end:]
    )

    with open(html_file_path, "w") as file:
        file.write(modified_html_content)
    # -----

    report_header_html = build_backtesting_report_header(
        df_stats=df_stats,
        strategy_name=strategy_name,
        timeframe=timeframe,
        symbol=symbol,
        quality_score=quality_score,
        df_trades=df_trades,
    )
    performance_charts_html = build_performance_charts_html(bt, stats)
    tables_html = build_backtesting_tables_html(df_stats, backtest_config, df_trades)

    # -----
    # add style
    # html_file_path = filename+".html"
    with open(html_file_path, "r") as file:
        html_content = file.read()

    # Locate the style tag in the HTML content
    body_tag_start = html_content.find("<body>")
    body_insert_at = body_tag_start + len("<body>")

    body_content_to_add = (
        """
        <script>
            (function () {
                const themeStorageKey = "bec-report-theme-v2";
                const savedTheme = localStorage.getItem(themeStorageKey) || "light";
                document.documentElement.setAttribute("data-bec-theme", savedTheme);
                window.addEventListener("DOMContentLoaded", function () {
                    function updateThemeLabels(theme) {
                        document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
                            button.innerHTML = `<span class="material-symbols-outlined">${theme === "dark" ? "light_mode" : "dark_mode"}</span>`;
                            button.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
                            button.setAttribute("title", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
                        });
                    }
                    updateThemeLabels(savedTheme);
                    document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
                        button.addEventListener("click", function () {
                            const current = document.documentElement.getAttribute("data-bec-theme") || "light";
                            const next = current === "dark" ? "light" : "dark";
                            document.documentElement.setAttribute("data-bec-theme", next);
                            localStorage.setItem(themeStorageKey, next);
                            updateThemeLabels(next);
                        });
                    });

                    function reportFilename(extension) {
                        const reportTitle = document.querySelector(".bec-topbar h1")?.textContent?.trim() || "strategy";
                        const reportSubtitle = Array.from(document.querySelectorAll(".bec-subtitle strong, .bec-subtitle span"))
                            .map(element => element.textContent.trim())
                            .filter(Boolean)
                            .join("-");
                        return `${reportTitle}-${reportSubtitle || "report"}.${extension}`
                            .replace(/[^a-z0-9._-]+/gi, "_")
                            .replace(/^_+|_+$/g, "");
                    }

                    function downloadReportHtml() {
                        const html = "<!doctype html>\\n" + document.documentElement.outerHTML;
                        const filename = reportFilename("html");
                        try {
                            const blob = new Blob([html], {type: "text/html;charset=utf-8;"});
                            const url = URL.createObjectURL(blob);
                            if (window.self !== window.top) {
                                const popupLink = document.createElement("a");
                                popupLink.href = url;
                                popupLink.target = "_blank";
                                popupLink.rel = "noopener";
                                document.body.appendChild(popupLink);
                                popupLink.click();
                                popupLink.remove();
                                window.setTimeout(function () {
                                    URL.revokeObjectURL(url);
                                }, 30000);
                                return;
                            }
                            const link = document.createElement("a");
                            link.href = url;
                            link.download = filename;
                            link.target = "_blank";
                            document.body.appendChild(link);
                            link.click();
                            link.remove();
                            window.setTimeout(function () {
                                URL.revokeObjectURL(url);
                            }, 1000);
                        } catch (error) {
                            const fallbackUrl = "data:text/html;charset=utf-8," + encodeURIComponent(html);
                            window.open(fallbackUrl, "_blank", "noopener");
                        }
                    }

                    if (!window.__becReportShareBound) {
                        window.__becReportShareBound = true;
                        document.addEventListener("click", function (event) {
                            const shareToggle = event.target.closest("[data-share-toggle]");
                            if (shareToggle) {
                                event.stopPropagation();
                                const menu = shareToggle.closest(".bec-share-menu");
                                const isOpen = menu?.classList.toggle("is-open");
                                shareToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
                                return;
                            }

                            const htmlDownloadButton = event.target.closest("[data-download-html]");
                            if (htmlDownloadButton) {
                                htmlDownloadButton.closest(".bec-share-menu")?.classList.remove("is-open");
                                downloadReportHtml();
                                return;
                            }

                            const pdfExportButton = event.target.closest("[data-export-pdf]");
                            if (pdfExportButton) {
                                pdfExportButton.closest(".bec-share-menu")?.classList.remove("is-open");
                                window.print();
                                return;
                            }

                            document.querySelectorAll(".bec-share-menu.is-open").forEach(function (menu) {
                                menu.classList.remove("is-open");
                                menu.querySelector("[data-share-toggle]")?.setAttribute("aria-expanded", "false");
                            });
                        });
                    }
                });
            })();
        </script>
        """
        + report_header_html
        + performance_charts_html
        + """
        """
    )
    tables_content_to_add = "<div id='bec-report-content'>" + tables_html + """
        <!-- bec-trades-toolbar-v2 -->
        <link rel="stylesheet" href="https://cdn.datatables.net/2.0.8/css/dataTables.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
        <script src="https://cdn.datatables.net/2.0.8/js/dataTables.min.js"></script>
        <script>
            window.addEventListener("DOMContentLoaded", function () {
                const table = document.querySelector("#trades-table");
                if (!table) return;

                const headers = Array.from(table.querySelectorAll("thead th")).map(th => th.textContent.trim());
                const tradeRows = Array.from(table.querySelectorAll("tbody tr")).map(row =>
                    Array.from(row.querySelectorAll("td")).map(cell => cell.textContent.trim())
                );
                const reportTitle = document.querySelector(".bec-topbar h1")?.textContent?.trim() || "strategy";
                const reportSubtitle = Array.from(document.querySelectorAll(".bec-subtitle strong, .bec-subtitle span"))
                    .map(element => element.textContent.trim())
                    .filter(Boolean)
                    .join("-");
                const csvFilename = `${reportTitle}-${reportSubtitle || "trades"}-trades.csv`
                    .replace(/[^a-z0-9._-]+/gi, "_")
                    .replace(/^_+|_+$/g, "");

                function csvEscape(value) {
                    const text = String(value ?? "");
                    if (
                        text.includes(",")
                        || text.includes('"')
                        || text.includes(String.fromCharCode(10))
                        || text.includes(String.fromCharCode(13))
                    ) {
                        return `"${text.replace(/"/g, '""')}"`;
                    }
                    return text;
                }

                function downloadTradesCsv() {
                    const csvRows = [headers, ...tradeRows].map(row => row.map(csvEscape).join(","));
                    const blob = new Blob([csvRows.join("\\n")], {type: "text/csv;charset=utf-8;"});
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement("a");
                    link.href = url;
                    link.download = csvFilename;
                    document.body.appendChild(link);
                    link.click();
                    link.remove();
                    URL.revokeObjectURL(url);
                }

                let tradesDataTable = null;
                if (window.DataTable && !document.querySelector("#trades-table_wrapper")) {
                    try {
                        tradesDataTable = new DataTable("#trades-table", {
                            pageLength: 25,
                            order: [[1, "asc"]],
                            scrollX: true,
                            rowCallback: function(row, data) {
                                const returnIndex = headers.indexOf("Return_Pct");
                                if (returnIndex >= 0 && parseFloat(data[returnIndex]) < 0) {
                                    row.classList.add("bec-loss-row");
                                }
                            }
                        });
                    } catch (error) {
                        tradesDataTable = null;
                    }
                } else if (window.DataTable && document.querySelector("#trades-table_wrapper")) {
                    try {
                        tradesDataTable = new DataTable.Api("#trades-table");
                    } catch (error) {
                        tradesDataTable = null;
                    }
                }

                if (!document.querySelector(".bec-trades-toolbar")) {
                    const toolbar = document.createElement("div");
                    toolbar.className = "bec-trades-toolbar";

                    const searchInput = document.createElement("input");
                    searchInput.type = "search";
                    searchInput.className = "bec-trades-search";
                    searchInput.placeholder = "Search trades";
                    searchInput.setAttribute("aria-label", "Search trades");

                    const button = document.createElement("button");
                    button.type = "button";
                    button.className = "bec-trades-download";
                    button.textContent = "Download CSV";
                    button.addEventListener("click", downloadTradesCsv);

                    searchInput.addEventListener("input", function () {
                        const query = searchInput.value.trim().toLowerCase();
                        if (tradesDataTable) {
                            tradesDataTable.search(query).draw();
                            return;
                        }

                        Array.from(table.querySelectorAll("tbody tr")).forEach(row => {
                            row.style.display = row.textContent.toLowerCase().includes(query) ? "" : "none";
                        });
                    });

                    toolbar.appendChild(button);
                    toolbar.appendChild(searchInput);
                    const wrapper = document.querySelector("#trades-table_wrapper");
                    const anchor = wrapper || table;
                    anchor.parentNode.insertBefore(toolbar, anchor);
                }
            });
        </script>
        </div>
        """

    modified_html_content = (
        html_content[:body_insert_at]
        + body_content_to_add
        + html_content[body_insert_at:]
    )
    body_tag_end = modified_html_content.find("</body>", body_insert_at)
    modified_html_content = (
        modified_html_content[:body_tag_end]
        + tables_content_to_add
        + modified_html_content[body_tag_end:]
    )

    with open(html_file_path, "w") as file:
        file.write(modified_html_content)
    # ------


def run_backtest(symbol, timeframe, strategy, optimize):

    # vars initialization
    n1 = 0
    n2 = 0

    strategy_name = get_strategy_id(strategy)
    df_strategy_meta = database.get_strategy_by_id(strategy_name)
    is_custom_strategy = (
        not df_strategy_meta.empty
        and str(df_strategy_meta.iloc[0].get("Type", "builtin") or "builtin")
        == "custom"
    )
    strategy_definition = database.get_strategy_definition(strategy_name)
    is_declarative_strategy = (
        isinstance(strategy_definition, dict)
        and strategy_definition.get("engine") == "bec_strategy_ast_v2"
    )
    df = get_data(symbol, timeframe)

    if df.empty:
        return  # exit function

    bt_settings = database.get_backtesting_settings()
    if is_declarative_strategy:
        _, optimizable_names = build_declarative_optimize_params(
            strategy_definition,
            str(bt_settings["Maximize"]),
        )
        optimize = bool(optimize and optimizable_names)
    commission_value = float(bt_settings["Commission_Value"])
    cash_value = float(bt_settings["Cash_Value"])

    strategy_risk = database.get_strategy_risk(strategy_name)
    atr_risk = (
        strategy_risk.get("atr_trailing", {}) if isinstance(strategy_risk, dict) else {}
    )
    take_profits = normalize_take_profit_levels(
        strategy_risk.get("take_profits", []) if isinstance(strategy_risk, dict) else []
    )
    strategy.stop_loss_pct = float(strategy_risk.get("stop_loss_pct", 0.0) or 0.0)
    strategy.atr_trailing_enabled = bool(atr_risk.get("enabled", False))
    strategy.atr_period = int(atr_risk.get("period", 14) or 14)
    strategy.atr_multiplier = float(atr_risk.get("multiplier", 1.8) or 1.8)
    strategy.atr_activation_pnl = float(atr_risk.get("activation_pnl_pct", 0.0) or 0.0)
    strategy.take_profits = take_profits
    strategy.take_profit_enabled = take_profit_enabled(take_profits)
    current_market_phase_sma_fast, current_market_phase_sma_slow = (
        get_market_phase_sma_settings(
            bt_settings,
            timeframe,
        )
    )
    daily_market_phase_sma_fast, daily_market_phase_sma_slow = (
        get_market_phase_sma_settings(
            bt_settings,
            "1d",
        )
    )
    strategy.nFastSMA = current_market_phase_sma_fast
    strategy.nSlowSMA = current_market_phase_sma_slow
    strategy.current_market_phase_timeframe = str(timeframe)
    strategy.daily_market_phase_sma_fast = 0
    strategy.daily_market_phase_sma_slow = 0
    strategy.use_current_timeframe_market_phase_filter = strategy_name in {
        "market_phases",
        "ema_cross_with_market_phases",
    }
    strategy.use_daily_market_phase_filter = False
    strategy.daily_market_phase_timeframe = ""
    strategy.daily_market_phase_alignment = ""
    strategy.use_daily_linreg_filter = False
    strategy.daily_linreg_timeframe = ""
    strategy.daily_linreg_alignment = ""
    strategy.execution_symbol = str(symbol)
    strategy.execution_timeframe = str(timeframe)

    if is_declarative_strategy:
        pass
    elif (
        strategy_name in {"ema_cross", "ema_cross_with_market_phases"}
        and str(timeframe) != "1d"
    ):
        if strategy_name == "ema_cross_with_market_phases":
            strategy.use_current_timeframe_market_phase_filter = bool(
                int(
                    bt_settings.get(
                        "Use_Intraday_Current_Timeframe_Market_Phase_Filter", 1
                    )
                )
            )
        strategy.use_daily_market_phase_filter = True
        strategy.daily_market_phase_timeframe = "1d"
        strategy.daily_market_phase_alignment = "previous_closed_candle"
        strategy.daily_market_phase_sma_fast = daily_market_phase_sma_fast
        strategy.daily_market_phase_sma_slow = daily_market_phase_sma_slow
        df = add_daily_market_phase_filter(
            df,
            symbol,
            sma_fast=daily_market_phase_sma_fast,
            sma_slow=daily_market_phase_sma_slow,
        )
    elif strategy_name == "hma_rsi_linreg":
        strategy.use_daily_linreg_filter = True
        strategy.daily_linreg_timeframe = "1d"
        linreg_period = int(getattr(strategy, "linreg_period", 50) or 50)
        if str(timeframe) == "1d":
            strategy.daily_linreg_alignment = "current_closed_candle"
            df = add_current_timeframe_linreg_filter(df, linreg_period=linreg_period)
        else:
            strategy.daily_linreg_alignment = "previous_closed_candle"
            df = add_daily_linreg_filter(
                df,
                symbol,
                linreg_period=linreg_period,
            )

    backtest_config = {
        "backtesting": {
            "optimize_enabled": bool(optimize),
            "optimize_maximize": str(bt_settings["Maximize"]) if optimize else "",
            "initial_cash": cash_value,
            "commission_pct": commission_value * 100.0,
            "buy_hold_start_mode": str(
                bt_settings.get(
                    "Buy_Hold_Start_Mode", BUY_HOLD_START_MODE_INDICATOR_WARMUP
                )
            ),
            "use_intraday_current_timeframe_market_phase_filter": bool(
                int(
                    bt_settings.get(
                        "Use_Intraday_Current_Timeframe_Market_Phase_Filter", 1
                    )
                )
            ),
        },
        "strategy_quality_score": {
            "weights": {
                "return_weight": float(
                    bt_settings.get("Strategy_Quality_Return_Weight", 20.0)
                ),
                "risk_weight": float(
                    bt_settings.get("Strategy_Quality_Risk_Weight", 25.0)
                ),
                "risk_adjusted_weight": float(
                    bt_settings.get("Strategy_Quality_Risk_Adjusted_Weight", 20.0)
                ),
                "trade_quality_weight": float(
                    bt_settings.get("Strategy_Quality_Trade_Quality_Weight", 20.0)
                ),
                "robustness_weight": float(
                    bt_settings.get("Strategy_Quality_Robustness_Weight", 15.0)
                ),
            },
        },
        "risk_management": {
            "hard_stop_loss_pct": float(strategy.stop_loss_pct),
            "atr_trailing_enabled": bool(strategy.atr_trailing_enabled),
            "atr_period": int(strategy.atr_period),
            "atr_multiplier": float(strategy.atr_multiplier),
            "atr_activation_pnl_pct": float(strategy.atr_activation_pnl),
        },
        "take_profits": {
            "enabled": bool(strategy.take_profit_enabled),
            "levels": normalize_take_profit_levels(
                getattr(strategy, "take_profits", [])
            ),
        },
    }
    backtest_config["strategy_definition"] = strategy_definition
    backtest_config["strategy_risk"] = strategy_risk

    # Checking the value of strategy
    # bt = Backtest(df, strategy=strategy, cash=cash_value, commission=commission_value, finalize_trades=True, exclusive_orders=True)
    bt = FractionalBacktest(
        df,
        strategy=strategy,
        cash=cash_value,
        commission=commission_value,
        finalize_trades=True,
        exclusive_orders=True,
        trade_on_close=True,
    )
    strategy.data_price_scale = float(getattr(bt, "_fractional_unit", 1.0) or 1.0)

    stats = bt.run()
    # print(stats)
    # bt.plot()

    if optimize:
        if is_declarative_strategy:
            optimize_params, optimized_param_names = build_declarative_optimize_params(
                strategy_definition,
                bt_settings["Maximize"],
            )
            optimize_params["max_tries"] = int(
                bt_settings.get(
                    "Optimization_Max_Combinations",
                    DEFAULT_OPTIMIZATION_MAX_COMBINATIONS,
                )
            )
        else:
            optimize_params = {
                "n1": range(10, 101, 10),
                "n2": range(20, 201, 10),
                "constraint": _n1_less_than_n2,
                "maximize": bt_settings["Maximize"],
                "return_heatmap": True,
            }
            # Keep RSI fixed at the strategy default; optimizing it increased overfitting risk in backtests.
            # if strategy_name == "hma_rsi_linreg":
            #     optimize_params["rsi_min"] = range(50, 61, 2)

            optimized_param_names = [
                param_name
                for param_name in optimize_params
                if param_name not in {"constraint", "maximize", "return_heatmap"}
            ]
        if is_declarative_strategy:
            stats, heatmap = run_serial_grid_optimize(bt, optimize_params)
        else:
            stats, heatmap = bt.optimize(**optimize_params)

        if not is_declarative_strategy:
            dfbema = pd.DataFrame(heatmap.sort_values().iloc[-1:])
            n1 = dfbema.index.get_level_values(0)[0]
            n2 = dfbema.index.get_level_values(1)[0]

    result_strategy = getattr(stats, "_strategy", strategy)
    if is_declarative_strategy:
        n1 = int(
            getattr(
                result_strategy,
                "ema_fast",
                getattr(
                    result_strategy, "hma_fast", getattr(result_strategy, "fast", n1)
                ),
            )
            or n1
            or 0
        )
        n2 = int(
            getattr(
                result_strategy,
                "ema_slow",
                getattr(
                    result_strategy, "hma_slow", getattr(result_strategy, "slow", n2)
                ),
            )
            or n2
            or 0
        )
    else:
        n1 = int(getattr(result_strategy, "n1", n1) or n1 or 0)
        n2 = int(getattr(result_strategy, "n2", n2) or n2 or 0)
    stats = apply_buy_hold_start_mode(
        stats,
        df,
        start_mode=bt_settings.get(
            "Buy_Hold_Start_Mode", BUY_HOLD_START_MODE_INDICATOR_WARMUP
        ),
    )
    backtest_config["strategy_parameters"] = build_strategy_parameters_config(
        result_strategy
    )
    df_trades = build_backtesting_trades_df(stats, strategy=result_strategy)
    quality_context = build_strategy_quality_context(
        stats=stats,
        backtest_config=backtest_config,
        df_trades=df_trades,
        strategy_name=strategy_name,
        timeframe=timeframe,
        symbol=symbol,
    )
    strategy_quality_score = calculate_strategy_quality_score(quality_context)
    backtest_config["strategy_quality_score_result"] = strategy_quality_score

    def _num(value, digits=2):
        try:
            return round(float(value), digits)
        except Exception:
            return None

    return_perc = _num(stats["Return [%]"], 2)
    buy_hold_return_Perc = _num(stats["Buy & Hold Return [%]"], 2)
    backtest_start_date = str(df.index[0])
    backtest_end_date = str(df.index[-1])

    max_drawdown_perc = _num(stats["Max. Drawdown [%]"], 8)
    trades = _num(stats["# Trades"], 0)
    win_rate_perc = _num(stats["Win Rate [%]"], 8)
    best_trade_perc = _num(stats["Best Trade [%]"], 8)
    worst_trade_perc = _num(stats["Worst Trade [%]"], 8)
    avg_trade_perc = _num(stats["Avg. Trade [%]"], 8)
    max_trade_duration = str(stats["Max. Trade Duration"])
    avg_trade_duration = str(stats["Avg. Trade Duration"])
    profit_factor = _num(stats["Profit Factor"], 8)
    expectancy_perc = _num(stats["Expectancy [%]"], 8)
    sqn = _num(stats["SQN"], 8)
    kelly_criterion = _num(stats["Kelly Criterion"], 8)

    # lista
    print(f"Strategy = {strategy_name}")

    if optimize:
        for param_name in optimized_param_names:
            print(f"{param_name} = ", getattr(result_strategy, param_name, "n/a"))

    print("Backtest start date = ", backtest_start_date)
    print("Backtest end date =", backtest_end_date)
    print("Return [%] = ", return_perc)
    print("Buy & Hold Return [%] = ", buy_hold_return_Perc)
    print("Max. Drawdown [%] = ", max_drawdown_perc)
    print("# Trades = ", trades)
    print("Win Rate [%] = ", win_rate_perc)
    print("Best Trade [%] = ", best_trade_perc)
    print("Worst Trade [%] = ", worst_trade_perc)
    print("Avg. Trade [%] = ", avg_trade_perc)
    print("Max. Trade Duration = ", max_trade_duration)
    print("Avg. Trade Duration = ", avg_trade_duration)
    print("Profit Factor = ", profit_factor)
    print("Expectancy [%] = ", expectancy_perc)
    print("SQN = ", sqn)
    print("Kelly Criterion = ", kelly_criterion)
    backtest_config_json = json.dumps(
        backtest_config, ensure_ascii=True, default=_json_default
    )
    print("Risk/TP Config = ", backtest_config_json)

    # save results as html file
    save_backtesting_to_html(
        bt,
        stats,
        result_strategy,
        timeframe,
        symbol,
        backtest_config=backtest_config,
        report_strategy_name=strategy_name,
    )

    database.add_backtesting_results(
        timeframe=timeframe,
        symbol=symbol,
        return_perc=return_perc,
        buy_hold_return_perc=buy_hold_return_Perc,
        backtest_start_date=backtest_start_date,
        backtest_end_date=backtest_end_date,
        max_drawdown_perc=max_drawdown_perc,
        trades=trades,
        win_rate_perc=win_rate_perc,
        best_trade_perc=best_trade_perc,
        worst_trade_perc=worst_trade_perc,
        avg_trade_perc=avg_trade_perc,
        max_trade_duration=max_trade_duration,
        avg_trade_duration=avg_trade_duration,
        profit_factor=profit_factor,
        expectancy_perc=expectancy_perc,
        sqn=sqn,
        kelly_criterion=kelly_criterion,
        strategy_Id=strategy_name,
        quality_score=strategy_quality_score.get("score"),
        quality_grade=strategy_quality_score.get("grade"),
        backtest_config_json=backtest_config_json,
    )
    if is_custom_strategy:
        database.mark_strategy_backtested(strategy_name)

    # trades
    df_trades_for_db = df_trades.drop(
        columns=["Size", "Exit_Size_Pct", "Exit_Remaining_Size_Pct"],
        errors="ignore",
    )

    # Insert the new columns at the beginning of the DataFrame
    df_trades_for_db.insert(0, "Symbol", symbol)
    df_trades_for_db.insert(1, "Time_Frame", timeframe)
    df_trades_for_db.insert(2, "Strategy_Id", strategy_name)

    # convert data type to string
    df_trades_for_db["Duration"] = df_trades_for_db["Duration"].astype(str)

    df_trades_for_db["ReturnPct"] = df_trades_for_db["ReturnPct"] * 100

    # delete existing trades
    database.delete_backtesting_trades_symbol_timeframe_strategy(
        symbol=symbol, timeframe=timeframe, strategy_id=strategy_name
    )

    # Insert new trades to database
    for index, row in df_trades_for_db.iterrows():
        exit_key = (int(row["EntryBar"]), int(row["ExitBar"]))
        exit_reason = str(row["Exit_Reason"]) if "Exit_Reason" in row else "unknown"
        database.add_backtesting_trade(
            symbol=row["Symbol"],
            timeframe=row["Time_Frame"],
            strategy_id=row["Strategy_Id"],
            entry_bar=row["EntryBar"],
            exit_bar=row["ExitBar"],
            entry_price=row["EntryPrice"],
            exit_price=row["ExitPrice"],
            pnl=row["PnL"],
            return_pct=row["ReturnPct"],
            entry_time=row["EntryTime"],
            exit_time=row["ExitTime"],
            duration=row["Duration"],
            exit_reason=exit_reason,
            hard_stop_loss=row["Hard_Stop_Loss"] if "Hard_Stop_Loss" in row else None,
            atr_stop_loss=row["ATR_Stop_Loss"] if "ATR_Stop_Loss" in row else None,
            active_stop_loss=(
                row["Active_Stop_Loss"] if "Active_Stop_Loss" in row else None
            ),
        )


def get_backtesting_results(strategy_id, symbol, time_frame):
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(
        symbol=symbol, time_frame=time_frame, strategy_id=strategy_id
    )

    if not df.empty:
        row = df.iloc[0]
        config = database.parse_strategy_params(row.get("Backtest_Config_JSON", ""))
        strategy_params = (
            config.get("strategy_parameters") if isinstance(config, dict) else {}
        )
        parameters = (
            strategy_params.get("parameters")
            if isinstance(strategy_params, dict)
            else {}
        )
        if isinstance(parameters, dict) and parameters:
            values = list(parameters.values())
            if len(values) >= 2:
                return int(float(values[0])), int(float(values[1]))

    return 0, 0


def calc_backtesting(symbol, time_frame, strategy, optimize):

    result = False

    try:
        # calculate run time
        start = timeit.default_timer()

        print("")
        # get strategy id from strategy class
        strategy_name = get_strategy_id(strategy)
        print(f"Backtest strategy {strategy_name} - {symbol} - {time_frame} - Start")

        run_backtest(symbol, time_frame, strategy, optimize)

        print(f"Backtest strategy {strategy_name} - {symbol} - {time_frame} - End")

        stop = timeit.default_timer()
        total_seconds = stop - start
        duration = database.calc_duration(total_seconds)
        msg = f"Execution Time: {duration}"
        print(msg)

        result = True
        return result

    except Exception as e:
        msg = sys._getframe().f_code.co_name + f" - " + repr(e)
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(
            telegram.telegram_token_main, telegram.EMOJI_WARNING, msg
        )

        return False


def main():
    parser = argparse.ArgumentParser(description="Run one BEC backtest.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timeframe", required=True, choices=["1d", "4h", "1h", "15m"])
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()

    strategy = resolve_strategy(args.strategy)
    if strategy is None:
        raise ValueError(f"Strategy '{args.strategy}' is not available.")

    result = calc_backtesting(
        symbol=args.symbol,
        time_frame=args.timeframe,
        strategy=strategy,
        optimize=args.optimize,
    )
    raise SystemExit(0 if result else 1)


if __name__ == "__main__":
    main()
