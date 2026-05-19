from types import SimpleNamespace

import bec.utils.database as database
from bec.utils import monte_carlo
from bec.utils.take_profit import take_profit_enabled
from bec.strategy_builder.templates import get_builtin_template


def test_configure_strategy_applies_strategy_risk(monkeypatch):
    strategy = SimpleNamespace()
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 7.5,
            "atr_trailing": {"enabled": True, "period": 9, "multiplier": 2.2, "activation_pnl_pct": 5.0},
            "take_profits": [
                {"level": 1, "pnl_pct": 12.0, "amount_pct": 50.0},
                {"level": 2, "pnl_pct": 24.0, "amount_pct": 50.0},
            ],
        },
    )
    monkeypatch.setattr(
        database,
        "get_backtesting_settings",
        lambda: {
            "Commission_Value": 0.001,
            "Cash_Value": 10000,
            "1d_Fast": 50,
            "1d_Slow": 200,
        },
    )

    monte_carlo._configure_strategy(strategy, "custom_strategy", "1d")

    assert strategy.stop_loss_pct == 7.5
    assert strategy.atr_trailing_enabled is True
    assert strategy.atr_period == 9
    assert strategy.atr_multiplier == 2.2
    assert strategy.atr_activation_pnl == 5.0
    assert strategy.take_profit_enabled is take_profit_enabled(strategy.take_profits)


def test_configure_strategy_with_empty_risk_disables_controls(monkeypatch):
    strategy = SimpleNamespace()
    monkeypatch.setattr(database, "get_strategy_risk", lambda strategy_id: {})
    monkeypatch.setattr(
        database,
        "get_backtesting_settings",
        lambda: {
            "Commission_Value": 0.001,
            "Cash_Value": 10000,
            "1d_Fast": 50,
            "1d_Slow": 200,
        },
    )

    monte_carlo._configure_strategy(strategy, "ema_cross", "1d")

    assert strategy.stop_loss_pct == 0.0
    assert strategy.atr_trailing_enabled is False
    assert strategy.take_profit_enabled is False
    assert strategy.take_profits == []


def test_prepare_backtest_df_skips_legacy_filters_for_declarative_strategy():
    strategy = SimpleNamespace(definition=get_builtin_template("ema_cross"))
    df = object()

    prepared = monte_carlo._prepare_backtest_df(df, "BTCUSDC", "1h", "ema_cross", strategy, 50, 200)

    assert prepared is df
    assert strategy.execution_symbol == "BTCUSDC"
    assert strategy.execution_timeframe == "1h"
