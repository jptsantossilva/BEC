import json


def _to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default=0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def parse_json_value(value, default):
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def normalize_take_profit_levels(value) -> list[dict]:
    """Normalize take-profit settings to ordered level/pnl_pct/amount_pct dicts."""
    parsed = parse_json_value(value, [])
    if isinstance(parsed, dict):
        parsed = parsed.get("levels", parsed.get("take_profits", []))
    if not isinstance(parsed, list):
        return []

    levels = []
    for idx, item in enumerate(parsed):
        if not isinstance(item, dict):
            continue
        level = _to_int(item.get("level", idx + 1), idx + 1)
        pnl_pct = _to_float(item.get("pnl_pct", item.get("pct", 0.0)), 0.0)
        amount_pct = _to_float(item.get("amount_pct", item.get("size_pct", 100.0)), 100.0)
        if level <= 0 or pnl_pct < 0 or amount_pct <= 0:
            continue
        amount_pct = min(100.0, amount_pct)
        levels.append(
            {
                "level": level,
                "pnl_pct": float(pnl_pct),
                "amount_pct": float(amount_pct),
            }
        )
    return levels


def dumps_take_profit_levels(levels: list[dict]) -> str:
    return json.dumps(normalize_take_profit_levels(levels), separators=(",", ":"))


def parse_executed_take_profit_levels(value) -> set[int]:
    parsed = parse_json_value(value, [])
    if not isinstance(parsed, list):
        return set()
    return {level for level in (_to_int(item, 0) for item in parsed) if level > 0}


def dumps_executed_take_profit_levels(levels) -> str:
    normalized = sorted({level for level in (_to_int(item, 0) for item in levels) if level > 0})
    return json.dumps(normalized, separators=(",", ":"))


def take_profit_enabled(levels: list[dict]) -> bool:
    return any(float(level.get("pnl_pct", 0.0) or 0.0) > 0 for level in levels if isinstance(level, dict))


def remaining_position_pct(levels: list[dict], active_only=True) -> list[float]:
    remaining = 100.0
    result = []
    for level in normalize_take_profit_levels(levels):
        if not active_only or float(level.get("pnl_pct", 0.0) or 0.0) > 0:
            remaining *= 1.0 - (float(level.get("amount_pct", 0.0) or 0.0) / 100.0)
        result.append(round(max(0.0, remaining), 2))
    return result
