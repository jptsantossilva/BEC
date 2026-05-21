import copy
import json


PACKAGE_VERSION = 1
SCHEMA_VERSION = 2
ENGINE = "bec_strategy_ast_v2"
BUILTIN_TEMPLATE_IDS = {
    "ema_cross",
    "ema_cross_with_market_phases",
    "market_phases",
    "hma_rsi_linreg",
    "bullmarketsupportband",
    "wema20",
}


def _constraints():
    return {
        "market": "spot",
        "side": "long",
        "order_type": "market",
        "allowed_actions": ["buy", "sell"],
        "allowed_timeframes": ["15m", "1h", "4h", "1d", "1w"],
    }


def _indicator(name, params=None, source=None, timeframe="current", output="value", period_param=""):
    result = {
        "type": "indicator",
        "name": name,
        "timeframe": timeframe,
        "source": source or {"type": "price", "field": "Close", "timeframe": timeframe},
        "params": params or {},
        "output": output,
    }
    if period_param:
        result["period_param"] = period_param
    return result


def _price(field="Close", timeframe="current"):
    return {"type": "price", "field": field, "timeframe": timeframe}


def _value(value):
    return {"type": "value", "value": value}


def _rule(left, operator, right):
    return {"type": "comparison", "left": left, "operator": operator, "right": right}


def _definition(
    name,
    description,
    parameters,
    entry_conditions,
    exit_conditions,
    risk_rules=None,
    parameter_constraints=None,
):
    return {
        "schema_version": SCHEMA_VERSION,
        "engine": ENGINE,
        "name": name,
        "description": description,
        "constraints": _constraints(),
        "parameters": parameters,
        "parameter_constraints": parameter_constraints or [],
        "entry": {
            "logic": "all",
            "conditions": entry_conditions,
            "action": {"type": "buy", "order_type": "market", "size_pct": 100},
        },
        "exit": {
            "logic": "any",
            "conditions": exit_conditions,
            "action": {"type": "sell", "order_type": "market", "size_pct": 100},
        },
        "risk": {"rules": risk_rules or []},
        "metadata": {"builder": "bec_strategy_builder", "source": "builtin_template"},
    }


def get_empty_strategy_template(name: str = "New Strategy") -> dict:
    strategy_name = str(name or "").strip() or "New Strategy"
    definition = _definition(
        strategy_name,
        "User-created draft strategy.",
        {},
        [],
        [],
    )
    definition["metadata"] = {
        "builder": "bec_strategy_builder",
        "source": "user_created",
    }
    return definition


BUILTIN_TEMPLATES = {
    "ema_cross": _definition(
        "EMA Cross",
        "Buy when the fast EMA crosses above the slow EMA. Sell on the reverse cross.",
        {
            "ema_fast": {"type": "int", "default": 10, "min": 10, "max": 101, "step": 10, "optimizable": True},
            "ema_slow": {"type": "int", "default": 20, "min": 10, "max": 201, "step": 10, "optimizable": True},
        },
        [_rule(_indicator("EMA", {"period": 20}, period_param="ema_fast"), "crosses_above", _indicator("EMA", {"period": 40}, period_param="ema_slow"))],
        [_rule(_indicator("EMA", {"period": 20}, period_param="ema_fast"), "crosses_below", _indicator("EMA", {"period": 40}, period_param="ema_slow"))],
        parameter_constraints=[
            {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
        ],
    ),
    "ema_cross_with_market_phases": _definition(
        "EMA Cross with Market Phases",
        "Buy on EMA cross only when price is above SMA50 and SMA200. Sell on reverse EMA cross.",
        {
            "ema_fast": {"type": "int", "default": 10, "min": 10, "max": 101, "step": 10, "optimizable": True},
            "ema_slow": {"type": "int", "default": 20, "min": 10, "max": 201, "step": 10, "optimizable": True},
            "sma_fast": {"type": "int", "default": 50, "min": 10, "max": 100, "step": 1, "optimizable": False},
            "sma_slow": {"type": "int", "default": 200, "min": 100, "max": 300, "step": 1, "optimizable": False},
        },
        [
            _rule(_indicator("EMA", {"period": 20}, period_param="ema_fast"), "crosses_above", _indicator("EMA", {"period": 40}, period_param="ema_slow")),
            _rule(_price("Close"), "above", _indicator("SMA", {"period": 50}, period_param="sma_fast")),
            _rule(_price("Close"), "above", _indicator("SMA", {"period": 200}, period_param="sma_slow")),
        ],
        [_rule(_indicator("EMA", {"period": 20}, period_param="ema_fast"), "crosses_below", _indicator("EMA", {"period": 40}, period_param="ema_slow"))],
        parameter_constraints=[
            {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
        ],
    ),
    "market_phases": _definition(
        "Market Phases",
        "Buy when price is above SMA50 and SMA200. Sell when price loses either moving average.",
        {
            "sma_fast": {"type": "int", "default": 50, "min": 10, "max": 100, "step": 1, "optimizable": False},
            "sma_slow": {"type": "int", "default": 200, "min": 100, "max": 300, "step": 1, "optimizable": False},
        },
        [
            _rule(_price("Close"), "above", _indicator("SMA", {"period": 50}, period_param="sma_fast")),
            _rule(_price("Close"), "above", _indicator("SMA", {"period": 200}, period_param="sma_slow")),
        ],
        [
            _rule(_price("Close"), "below", _indicator("SMA", {"period": 50}, period_param="sma_fast")),
            _rule(_price("Close"), "below", _indicator("SMA", {"period": 200}, period_param="sma_slow")),
        ],
    ),
    "hma_rsi_linreg": _definition(
        "HMA RSI LINREG",
        "Buy on HMA cross with RSI confirmation and linear-regression trend filter. Sell on reverse HMA cross.",
        {
            "hma_fast": {"type": "int", "default": 20, "min": 10, "max": 101, "step": 10, "optimizable": True},
            "hma_slow": {"type": "int", "default": 70, "min": 10, "max": 201, "step": 10, "optimizable": True},
            "rsi_period": {"type": "int", "default": 14, "min": 5, "max": 50, "step": 1, "optimizable": False},
            "rsi_min": {"type": "float", "default": 52.0, "min": 1.0, "max": 99.0, "step": 0.5, "optimizable": False},
            "linreg_period": {"type": "int", "default": 50, "min": 10, "max": 200, "step": 1, "optimizable": False},
        },
        [
            _rule(_indicator("HMA", {"period": 16}, period_param="hma_fast"), "crosses_above", _indicator("HMA", {"period": 65}, period_param="hma_slow")),
            _rule(_indicator("RSI", {"period": 14}, period_param="rsi_period"), "above", {"type": "parameter", "name": "rsi_min"}),
            _rule(_price("Close"), "above", _indicator("LINREG", {"period": 50}, period_param="linreg_period")),
        ],
        [_rule(_indicator("HMA", {"period": 16}, period_param="hma_fast"), "crosses_below", _indicator("HMA", {"period": 65}, period_param="hma_slow"))],
        parameter_constraints=[
            {"left": "hma_fast", "operator": "less_than", "right": "hma_slow"},
        ],
    ),
    "bullmarketsupportband": _definition(
        "BullMarketSupportBand",
        "Buy when weekly EMA21 crosses above weekly SMA20. Sell on the reverse cross.",
        {},
        [
            _rule(
                _indicator("EMA", {"period": 21}, timeframe="1w"),
                "crosses_above",
                _indicator("SMA", {"period": 20}, timeframe="1w"),
            ),
        ],
        [
            _rule(
                _indicator("EMA", {"period": 21}, timeframe="1w"),
                "crosses_below",
                _indicator("SMA", {"period": 20}, timeframe="1w"),
            ),
        ],
    ),
    "wema20": _definition(
        "WEMA20",
        "Buy when weekly close is above EMA20. Sell when weekly close is below EMA20.",
        {},
        [
            _rule(
                _price("Close", timeframe="1w"),
                "greater_than",
                _indicator("EMA", {"period": 20}, timeframe="1w"),
            ),
        ],
        [
            _rule(
                _price("Close", timeframe="1w"),
                "less_than",
                _indicator("EMA", {"period": 20}, timeframe="1w"),
            ),
        ],
    ),
}


def get_builtin_template(strategy_id: str) -> dict:
    template = BUILTIN_TEMPLATES.get(str(strategy_id).strip())
    return copy.deepcopy(template) if template else {}


def dumps_json(value: dict) -> str:
    return json.dumps(value or {}, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
