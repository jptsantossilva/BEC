import hashlib
import json

import numpy as np
import pandas as pd
from backtesting.lib import crossover

from bec.strategy_builder import schema
from bec.strategy_builder import registry

TIMEFRAME_MINUTES = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}


def _series_key(operand: dict, parameters: dict) -> str:
    payload = {
        "type": operand.get("type"),
        "name": str(operand.get("name", "")).upper(),
        "field": operand.get("field", "Close"),
        "period": _resolve_period(operand, parameters),
        "timeframe": operand.get("timeframe", "current"),
        "output": operand.get("output", "value"),
        "params": operand.get("params", {}),
        "source": operand.get("source", "Close"),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _resolve_period(operand: dict, parameters: dict) -> int:
    if operand.get("period_param"):
        value = parameters.get(str(operand["period_param"]))
    else:
        value = operand.get("period")
    try:
        return max(int(float(value)), 1)
    except (TypeError, ValueError):
        return 1


def ema(values, period):
    return pd.Series(values).ewm(span=max(int(period), 1), adjust=False).mean()


def sma(values, period):
    return pd.Series(values).rolling(max(int(period), 1)).mean()


def wma(values, period):
    period = max(int(period), 1)
    weights = np.arange(1, period + 1)
    return pd.Series(values).rolling(period).apply(lambda prices: np.dot(prices, weights) / weights.sum(), raw=True)


def hma(values, period):
    period = max(int(period), 1)
    half_period = max(int(period / 2), 1)
    sqrt_period = max(int(np.sqrt(period)), 1)
    values = pd.Series(values)
    return wma(2 * wma(values, half_period) - wma(values, period), sqrt_period)


def rsi(values, period):
    period = max(int(period), 1)
    close = pd.Series(values)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    result = 100 - (100 / (1 + rs))
    return result.fillna(100).where(avg_loss != 0, 100)


def linreg(values, period):
    period = max(int(period), 1)
    x = np.arange(period, dtype=float)
    x_mean = x.mean()
    denominator = ((x - x_mean) ** 2).sum()

    def _endpoint(y):
        y_mean = y.mean()
        slope = ((x - x_mean) * (y - y_mean)).sum() / denominator if denominator else 0.0
        intercept = y_mean - slope * x_mean
        return intercept + slope * (period - 1)

    return pd.Series(values).rolling(period).apply(_endpoint, raw=True)


def dema(values, period):
    first = ema(values, period)
    second = ema(first, period)
    return 2 * first - second


def rma(values, period):
    return pd.Series(values).ewm(alpha=1 / max(int(period), 1), adjust=False).mean()


def atr(high, low, close, period):
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
    return tr.rolling(max(int(period), 1), min_periods=max(int(period), 1)).mean()


def macd(values, fast=12, slow=26, signal=9):
    macd_line = ema(values, fast) - ema(values, slow)
    signal_line = ema(macd_line, signal)
    return {
        "macd": macd_line,
        "signal": signal_line,
        "histogram": macd_line - signal_line,
    }


def stoch(high, low, close, k=14, d=3, smooth=3):
    high = pd.Series(high)
    low = pd.Series(low)
    close = pd.Series(close)
    k_period = max(int(k), 1)
    lowest_low = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    raw_k = ((close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)) * 100
    k_line = raw_k.rolling(max(int(smooth), 1)).mean()
    d_line = k_line.rolling(max(int(d), 1)).mean()
    return {"k": k_line, "d": d_line}


def bollinger(values, period=20, stddev=2.0):
    values = pd.Series(values)
    period = max(int(period), 1)
    middle = values.rolling(period).mean()
    std = values.rolling(period).std()
    upper = middle + float(stddev) * std
    lower = middle - float(stddev) * std
    width = ((upper - lower) / middle.replace(0, np.nan)) * 100
    percent_b = (values - lower) / (upper - lower).replace(0, np.nan)
    return {
        "upper": upper,
        "middle": middle,
        "lower": lower,
        "width": width,
        "percent_b": percent_b,
    }


def obv(close, volume):
    close = pd.Series(close)
    volume = pd.Series(volume)
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def vwap(high, low, close, volume, period=14):
    typical = (pd.Series(high) + pd.Series(low) + pd.Series(close)) / 3
    volume = pd.Series(volume)
    period = max(int(period), 1)
    return (typical * volume).rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)


def resolve_parameters(definition: dict, overrides: dict | None = None) -> dict:
    result = schema.parameter_defaults(definition)
    for key, value in (overrides or {}).items():
        if key in result:
            result[key] = value
    return result


def iter_operands(definition: dict):
    yield from iter_ast_operands(definition)


def iter_ast_operands(definition: dict):
    def walk(value):
        if isinstance(value, dict):
            value_type = value.get("type")
            if value_type in {"price", "indicator", "value", "parameter", "transform"}:
                yield value
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for item in value:
                yield from walk(item)

    yield from walk(definition.get("entry", {}))
    yield from walk(definition.get("exit", {}))
    yield from walk(definition.get("risk", {}))


def ast_timeframes(definition: dict) -> set[str]:
    timeframes = set()
    for operand in iter_ast_operands(definition):
        if operand.get("type") in {"price", "indicator"} and operand.get("timeframe"):
            timeframes.add(str(operand.get("timeframe")))
    return timeframes


def requires_multi_timeframe_data(definition: dict) -> bool:
    non_current = {timeframe for timeframe in ast_timeframes(definition) if timeframe != "current"}
    return len(non_current) > 1


def _normalize_timeframe(value: str | None, base_timeframe: str | None = None) -> str:
    timeframe = str(value or "current")
    if timeframe == "current":
        return str(base_timeframe or "current")
    return timeframe


def _validate_timeframe_alignment(definition: dict, base_timeframe: str):
    base = _normalize_timeframe(base_timeframe)
    if base not in TIMEFRAME_MINUTES:
        return
    for timeframe in ast_timeframes(definition):
        resolved = _normalize_timeframe(timeframe, base)
        if resolved not in TIMEFRAME_MINUTES:
            continue
        if TIMEFRAME_MINUTES[resolved] < TIMEFRAME_MINUTES[base]:
            raise schema.StrategyValidationError(
                f"Definition_JSON uses lower timeframe '{resolved}' while running on '{base}'. "
                "Lower fixed timeframes are not supported for this execution timeframe."
            )


def _is_current_timeframe(operand: dict, base_timeframe: str | None = None) -> bool:
    timeframe = str(operand.get("timeframe", "current") or "current")
    if timeframe == "current":
        return True
    if base_timeframe:
        return timeframe == str(base_timeframe)
    return False


def _normalize_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.index = pd.to_datetime(result.index)
    result = result.sort_index(kind="mergesort")
    return result


def _source_field(operand: dict) -> str:
    source = operand.get("source", "Close")
    if isinstance(source, dict):
        return str(source.get("field", "Close"))
    return str(source or "Close")


def _indicator_params(operand: dict, parameters: dict) -> dict:
    name = registry.normalize_indicator_name(operand.get("name", ""))
    params = registry.indicator_defaults(name)
    params.update(operand.get("params", {}) if isinstance(operand.get("params"), dict) else {})
    if operand.get("period_param"):
        params["period"] = parameters.get(str(operand.get("period_param")), params.get("period", 1))
    elif operand.get("period") is not None:
        params["period"] = operand.get("period")
    return params


def _compute_indicator(df: pd.DataFrame, operand: dict, parameters: dict):
    name = registry.normalize_indicator_name(operand.get("name", ""))
    params = _indicator_params(operand, parameters)
    output = operand.get("output", "value")
    source = _source_field(operand)
    values = df[source]
    if name == "EMA":
        return ema(values, params.get("period", 9))
    if name == "SMA":
        return sma(values, params.get("period", 9))
    if name == "WMA":
        return wma(values, params.get("period", 9))
    if name == "HMA":
        return hma(values, params.get("period", 21))
    if name == "DEMA":
        return dema(values, params.get("period", 20))
    if name == "RMA":
        return rma(values, params.get("period", 14))
    if name == "RSI":
        return rsi(values, params.get("period", 14))
    if name == "LINREG":
        return linreg(values, params.get("period", 100))
    if name == "ATR":
        return atr(df["High"], df["Low"], df["Close"], params.get("period", 14))
    if name == "MACD":
        return macd(values, params.get("fast", 12), params.get("slow", 26), params.get("signal", 9))[output]
    if name == "STOCH":
        return stoch(df["High"], df["Low"], df["Close"], params.get("k", 14), params.get("d", 3), params.get("smooth", 3))[output]
    if name == "BB":
        return bollinger(values, params.get("period", 20), params.get("stddev", 2.0))[output]
    if name == "OBV":
        return obv(df["Close"], df["Volume"])
    if name == "VWAP":
        return vwap(df["High"], df["Low"], df["Close"], df["Volume"], params.get("period", 14))
    return pd.Series(np.nan, index=df.index)


def _load_timeframe_data(symbol: str, timeframe: str, data_loader):
    if data_loader is None:
        raise schema.StrategyValidationError(
            f"Definition_JSON needs '{timeframe}' data. Pass a data_loader for multi-timeframe evaluation."
        )
    try:
        return data_loader(symbol, timeframe)
    except TypeError:
        return data_loader(timeframe)


def _align_higher_timeframe_series(base_df: pd.DataFrame, source_df: pd.DataFrame, series: pd.Series) -> pd.Series:
    base_index = pd.to_datetime(base_df.index)
    source = pd.Series(series.values, index=pd.to_datetime(source_df.index)).sort_index(kind="mergesort")
    # Use only the previous closed higher-timeframe candle for each base candle.
    source = source.shift(1)
    return source.reindex(base_index, method="ffill")


def add_indicators(
    df: pd.DataFrame,
    definition: dict,
    parameters: dict | None = None,
    *,
    symbol: str = "",
    base_timeframe: str = "current",
    data_loader=None,
) -> pd.DataFrame:
    definition = schema.validate_definition(definition)
    parameters = resolve_parameters(definition, parameters)
    result = _normalize_datetime_index(df)
    base_timeframe = _normalize_timeframe(base_timeframe)
    if base_timeframe == "current":
        fixed_timeframes = sorted(timeframe for timeframe in ast_timeframes(definition) if timeframe != "current")
        if len(fixed_timeframes) == 1:
            base_timeframe = fixed_timeframes[0]
    if base_timeframe != "current":
        _validate_timeframe_alignment(definition, base_timeframe)

    data_by_timeframe = {base_timeframe: result}
    for operand in iter_operands(definition):
        operand_type = operand.get("type")
        if operand_type not in {"indicator", "price"}:
            continue
        key = _series_key(operand, parameters)
        column = f"SB_{key}"
        if column in result.columns:
            continue
        operand_timeframe = _normalize_timeframe(operand.get("timeframe", "current"), base_timeframe)
        if _is_current_timeframe(operand, base_timeframe):
            if operand_type == "indicator":
                result[column] = _compute_indicator(result, operand, parameters)
            continue

        source_df = data_by_timeframe.get(operand_timeframe)
        if source_df is None:
            source_df = _normalize_datetime_index(_load_timeframe_data(symbol, operand_timeframe, data_loader))
            data_by_timeframe[operand_timeframe] = source_df
        if source_df.empty:
            result[column] = np.nan
            continue
        if operand_type == "indicator":
            series = _compute_indicator(source_df, operand, parameters)
        else:
            series = source_df[str(operand.get("field", "Close"))]
        result[column] = _align_higher_timeframe_series(result, source_df, series).values
    return result


def _operand_value(df: pd.DataFrame, operand: dict, parameters: dict, idx: int):
    operand_type = operand.get("type")
    if operand_type == "indicator":
        return df[f"SB_{_series_key(operand, parameters)}"].iloc[idx]
    if operand_type == "price":
        column = f"SB_{_series_key(operand, parameters)}"
        if column in df.columns:
            return df[column].iloc[idx]
        return df[str(operand.get("field", "Close"))].iloc[idx]
    if operand_type == "value":
        return operand.get("value")
    if operand_type == "parameter":
        return parameters.get(str(operand.get("name")))
    if operand_type == "transform":
        return _transform_value(df, operand, parameters, idx)
    return np.nan


def _transform_value(df: pd.DataFrame, operand: dict, parameters: dict, idx: int):
    operator = operand.get("operator")
    source = operand.get("source", {})
    bars = max(int(float(operand.get("bars", operand.get("period", 1)) or 1)), 1)
    if operator == "lookback":
        target_idx = idx - bars
        if target_idx < 0:
            return np.nan
        return _operand_value(df, source, parameters, target_idx)
    series = [_operand_value(df, source, parameters, pos) for pos in range(max(0, idx - bars + 1), idx + 1)]
    if not series:
        return np.nan
    if operator == "highest_high":
        return np.nanmax(series)
    if operator == "lowest_low":
        return np.nanmin(series)
    return np.nan


def _is_ready(*values) -> bool:
    for value in values:
        try:
            if pd.isna(value):
                return False
        except TypeError:
            pass
    return True


def evaluate_rule(df: pd.DataFrame, rule: dict, parameters: dict, idx: int = -1) -> bool:
    if len(df) == 0:
        return False
    if idx < 0:
        idx = len(df) + idx
    if rule.get("type") == "window_condition":
        return evaluate_window_condition(df, rule, parameters, idx)
    left = _operand_value(df, rule["left"], parameters, idx)
    operator = rule.get("operator")
    if operator == "between":
        lower = rule.get("lower")
        upper = rule.get("upper")
        if lower is None or upper is None:
            right = rule.get("right", [])
            if isinstance(right, list) and len(right) == 2:
                lower, upper = right
        lower_value = _operand_value(df, lower, parameters, idx)
        upper_value = _operand_value(df, upper, parameters, idx)
        return _is_ready(left, lower_value, upper_value) and float(lower_value) <= float(left) <= float(upper_value)
    right = _operand_value(df, rule["right"], parameters, idx)
    if not _is_ready(left, right):
        return False
    if operator in {"greater_than", "above"}:
        return float(left) > float(right)
    if operator in {"less_than", "below"}:
        return float(left) < float(right)
    if operator == "greater_than_or_equal":
        return float(left) >= float(right)
    if operator == "less_than_or_equal":
        return float(left) <= float(right)
    if operator in {"crosses_above", "crosses_below"}:
        if idx <= 0:
            return False
        left_prev = _operand_value(df, rule["left"], parameters, idx - 1)
        right_prev = _operand_value(df, rule["right"], parameters, idx - 1)
        if not _is_ready(left_prev, right_prev):
            return False
        if operator == "crosses_above":
            return bool(left_prev < right_prev and left > right)
        return bool(left_prev > right_prev and left < right)
    return False


def evaluate_window_condition(df: pd.DataFrame, rule: dict, parameters: dict, idx: int = -1) -> bool:
    if len(df) == 0:
        return False
    if idx < 0:
        idx = len(df) + idx
    operator = rule.get("operator")
    bars = max(int(float(rule.get("bars", 1) or 1)), 1)
    if operator == "wait_n_bars":
        return True
    start = max(0, idx - bars + 1)
    condition = rule.get("condition", {})
    results = [evaluate_ast_condition(df, condition, parameters, pos) for pos in range(start, idx + 1)]
    if operator == "for_n_bars":
        return len(results) == bars and all(results)
    if operator == "within_n_bars":
        return any(results)
    return False


def evaluate_group(df: pd.DataFrame, group: dict, parameters: dict, idx: int = -1) -> bool:
    return evaluate_ast_group(df, group, parameters, idx)


def evaluate_ast_condition(df: pd.DataFrame, condition: dict, parameters: dict, idx: int = -1) -> bool:
    condition_type = condition.get("type", "comparison")
    if condition_type == "group":
        return evaluate_ast_group(df, condition, parameters, idx)
    return evaluate_rule(df, condition, parameters, idx)


def evaluate_ast_group(df: pd.DataFrame, group: dict, parameters: dict, idx: int = -1) -> bool:
    conditions = group.get("conditions", []) if isinstance(group, dict) else []
    if not conditions:
        return False
    operators = group.get("operators", []) if isinstance(group, dict) else []
    if isinstance(operators, list) and len(operators) == len(conditions) - 1:
        result = evaluate_ast_condition(df, conditions[0], parameters, idx)
        for operator, condition in zip(operators, conditions[1:]):
            condition_result = evaluate_ast_condition(df, condition, parameters, idx)
            if operator == "any":
                result = result or condition_result
            else:
                result = result and condition_result
        return bool(result)
    if group.get("logic") == "any":
        return any(evaluate_ast_condition(df, condition, parameters, idx) for condition in conditions)
    return all(evaluate_ast_condition(df, condition, parameters, idx) for condition in conditions)


def evaluate_entry(
    df: pd.DataFrame,
    definition: dict,
    parameters: dict | None = None,
    idx: int = -1,
    *,
    symbol: str = "",
    base_timeframe: str = "current",
    data_loader=None,
) -> bool:
    definition = schema.validate_definition(definition)
    parameters = resolve_parameters(definition, parameters)
    prepared = add_indicators(
        df,
        definition,
        parameters,
        symbol=symbol,
        base_timeframe=base_timeframe,
        data_loader=data_loader,
    )
    return evaluate_ast_group(prepared, definition.get("entry", {}), parameters, idx)


def evaluate_exit(
    df: pd.DataFrame,
    definition: dict,
    parameters: dict | None = None,
    idx: int = -1,
    *,
    symbol: str = "",
    base_timeframe: str = "current",
    data_loader=None,
) -> bool:
    definition = schema.validate_definition(definition)
    parameters = resolve_parameters(definition, parameters)
    prepared = add_indicators(
        df,
        definition,
        parameters,
        symbol=symbol,
        base_timeframe=base_timeframe,
        data_loader=data_loader,
    )
    return evaluate_ast_group(prepared, definition.get("exit", {}), parameters, idx)
