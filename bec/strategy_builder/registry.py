from __future__ import annotations

from dataclasses import dataclass


ALLOWED_TIMEFRAMES = {"15m", "1h", "4h", "1d", "1w"}
ALLOWED_PRICE_FIELDS = {"Open", "High", "Low", "Close", "Volume"}
ALLOWED_ACTIONS = {"buy", "sell"}
ALLOWED_ORDER_TYPES = {"market"}

COMPARISON_OPERATORS = {
    "above",
    "below",
    "greater_than",
    "less_than",
    "greater_than_or_equal",
    "less_than_or_equal",
    "between",
    "crosses_above",
    "crosses_below",
}
WINDOW_OPERATORS = {"for_n_bars", "within_n_bars", "wait_n_bars"}
TRANSFORM_OPERATORS = {"lookback", "highest_high", "lowest_low"}
RISK_RULE_TYPES = {
    "take_profit_pct",
    "stop_loss_pct",
    "take_profit_indicator",
    "stop_loss_indicator",
    "atr_stop",
    "trailing_indicator",
    "take_profit_r_multiple",
}


@dataclass(frozen=True)
class IndicatorSpec:
    name: str
    category: str
    params: dict
    outputs: tuple[str, ...] = ("value",)
    default_source: str = "Close"


INDICATORS = {
    "SMA": IndicatorSpec("SMA", "trend", {"period": 9}),
    "EMA": IndicatorSpec("EMA", "trend", {"period": 9}),
    "WMA": IndicatorSpec("WMA", "trend", {"period": 9}),
    "HMA": IndicatorSpec("HMA", "trend", {"period": 21}),
    "DEMA": IndicatorSpec("DEMA", "trend", {"period": 20}),
    "RMA": IndicatorSpec("RMA", "trend", {"period": 14}),
    "RSI": IndicatorSpec("RSI", "momentum", {"period": 14}),
    "MACD": IndicatorSpec("MACD", "momentum", {"fast": 12, "slow": 26, "signal": 9}, ("macd", "signal", "histogram")),
    "STOCH": IndicatorSpec("STOCH", "momentum", {"k": 14, "d": 3, "smooth": 3}, ("k", "d")),
    "ATR": IndicatorSpec("ATR", "volatility", {"period": 14}),
    "BB": IndicatorSpec("BB", "volatility", {"period": 20, "stddev": 2.0}, ("upper", "middle", "lower", "width", "percent_b")),
    "OBV": IndicatorSpec("OBV", "volume", {}, ("value",), "Volume"),
    "VWAP": IndicatorSpec("VWAP", "volume", {"period": 14}, ("value",), "Close"),
    "LINREG": IndicatorSpec("LINREG", "other", {"period": 100}),
}


def normalize_indicator_name(name: str) -> str:
    return str(name or "").strip().upper()


def get_indicator_spec(name: str) -> IndicatorSpec | None:
    return INDICATORS.get(normalize_indicator_name(name))


def indicator_defaults(name: str) -> dict:
    spec = get_indicator_spec(name)
    return dict(spec.params) if spec else {}
