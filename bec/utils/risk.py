import bec.utils.database as database
from bec.utils.take_profit import normalize_take_profit_levels, take_profit_enabled


def _empty_runtime_risk_settings() -> dict:
    result = {
        "stop_loss": 0.0,
        "atr_trailing_enabled": False,
        "atr_period": 14,
        "atr_multiplier": 1.8,
        "atr_activation_pnl": 0.0,
        "take_profit_enabled": False,
        "take_profits": [],
    }
    return result


def get_runtime_risk_settings(settings, strategy_id: str, pos_row=None) -> dict:
    """Resolve live risk controls from position snapshot or strategy definition."""
    del settings  # kept for call-site compatibility; global settings are not used.

    snapshot = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", "")) if pos_row is not None else {}
    risk = snapshot.get("risk") if isinstance(snapshot, dict) else None
    if not isinstance(risk, dict):
        risk = database.get_strategy_risk(strategy_id)
    if not isinstance(risk, dict):
        return _empty_runtime_risk_settings()

    atr = risk.get("atr_trailing", {}) if isinstance(risk.get("atr_trailing"), dict) else {}
    take_profits = normalize_take_profit_levels(risk.get("take_profits", []))
    result = {
        "stop_loss": float(risk.get("stop_loss_pct", 0.0) or 0.0),
        "atr_trailing_enabled": bool(atr.get("enabled", False)),
        "atr_period": int(atr.get("period", 14) or 14),
        "atr_multiplier": float(atr.get("multiplier", 1.8) or 1.8),
        "atr_activation_pnl": float(atr.get("activation_pnl_pct", 2.0) or 2.0),
        "take_profit_enabled": take_profit_enabled(take_profits),
        "take_profits": take_profits,
    }
    return result
