from types import SimpleNamespace

import pandas as pd

import bec.utils.database as database
from bec.utils.risk import get_runtime_risk_settings


def _settings():
    return SimpleNamespace(
        stop_loss=11.0,
        atr_trailing_enabled=True,
        atr_period=21,
        atr_multiplier=3.0,
        atr_activation_pnl=4.0,
        take_profit_enabled=True,
        take_profits=[{"level": 1, "pnl_pct": 9.0, "amount_pct": 25.0}],
    )


def test_strategy_uses_strategy_stop_loss_not_global_settings(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 8.0,
            "atr_trailing": {"enabled": False, "period": 14, "multiplier": 1.8, "activation_pnl_pct": 2.0},
            "take_profits": [],
        },
    )

    risk = get_runtime_risk_settings(_settings(), "ema_cross")

    assert risk["stop_loss"] == 8.0
    assert risk["atr_trailing_enabled"] is False


def test_strategy_without_stop_loss_disables_hard_stop(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 0.0,
            "atr_trailing": {"enabled": False, "period": 14, "multiplier": 1.8, "activation_pnl_pct": 2.0},
            "take_profits": [],
        },
    )

    risk = get_runtime_risk_settings(_settings(), "market_phases")

    assert risk["stop_loss"] == 0.0
    assert risk["atr_trailing_enabled"] is False


def test_strategy_uses_strategy_atr_stop_not_global_settings(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 0.0,
            "atr_trailing": {"enabled": True, "period": 7, "multiplier": 1.25, "activation_pnl_pct": 6.5},
            "take_profits": [],
        },
    )

    risk = get_runtime_risk_settings(_settings(), "ema_cross")

    assert risk["atr_trailing_enabled"] is True
    assert risk["atr_period"] == 7
    assert risk["atr_multiplier"] == 1.25
    assert risk["atr_activation_pnl"] == 6.5


def test_strategy_risk_snapshot_has_priority_over_current_definition(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 8.0,
            "atr_trailing": {"enabled": True, "period": 14, "multiplier": 1.8, "activation_pnl_pct": 2.0},
            "take_profits": [],
        },
    )
    pos_row = pd.Series(
        {
            "Strategy_Params_JSON": (
                '{"risk":{"stop_loss_pct":5.0,'
                '"atr_trailing":{"enabled":true,"period":10,"multiplier":2.5,"activation_pnl_pct":3.0},'
                '"take_profits":[]}}'
            )
        }
    )

    risk = get_runtime_risk_settings(_settings(), "custom_strategy", pos_row=pos_row)

    assert risk["stop_loss"] == 5.0
    assert risk["atr_period"] == 10
    assert risk["atr_multiplier"] == 2.5
    assert risk["atr_activation_pnl"] == 3.0


def test_strategy_without_valid_definition_disables_all_risk_controls(monkeypatch):
    monkeypatch.setattr(database, "get_strategy_risk", lambda strategy_id: {})

    risk = get_runtime_risk_settings(_settings(), "legacy_strategy")

    assert risk["stop_loss"] == 0.0
    assert risk["atr_trailing_enabled"] is False
    assert risk["take_profit_enabled"] is False
    assert risk["take_profits"] == []


def test_global_settings_are_ignored_even_when_present(monkeypatch):
    monkeypatch.setattr(database, "get_strategy_risk", lambda strategy_id: {})

    risk = get_runtime_risk_settings(_settings(), "missing_strategy")

    assert risk["stop_loss"] == 0.0
    assert risk["atr_trailing_enabled"] is False
