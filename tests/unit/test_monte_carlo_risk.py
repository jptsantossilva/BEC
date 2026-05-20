import json
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


def test_apply_saved_strategy_parameters_uses_declarative_backtest_config():
    strategy = SimpleNamespace(parameter_values={"hma_fast": 10, "hma_slow": 20})
    backtest_row = monte_carlo.pd.Series(
        {
            "Backtest_Config_JSON": json.dumps(
                {
                    "strategy_parameters": {
                        "parameters": {"hma_fast": 30, "hma_slow": 160}
                    }
                }
            ),
            "Ema_Fast": 10,
            "Ema_Slow": 20,
        }
    )

    monte_carlo._apply_saved_strategy_parameters(strategy, backtest_row)

    assert strategy.hma_fast == 30
    assert strategy.hma_slow == 160
    assert strategy.parameter_values == {"hma_fast": 30, "hma_slow": 160}


def test_apply_saved_strategy_parameters_keeps_legacy_fallback():
    strategy = SimpleNamespace()
    backtest_row = monte_carlo.pd.Series({"Ema_Fast": 12, "Ema_Slow": 48})

    monte_carlo._apply_saved_strategy_parameters(strategy, backtest_row)

    assert strategy.n1 == 12
    assert strategy.nFastHMA == 12
    assert strategy.n2 == 48
    assert strategy.nSlowHMA == 48


def test_resolve_strategy_supports_declarative_my_strategies(monkeypatch):
    strategy = object()
    calls = []
    monkeypatch.setattr(
        monte_carlo.my_backtesting,
        "resolve_strategy",
        lambda strategy_id: calls.append(strategy_id) or strategy,
    )

    resolved = monte_carlo._resolve_strategy("hma_rsi_linreg_copy")

    assert resolved is strategy
    assert calls == ["hma_rsi_linreg_copy"]


def test_resolve_strategy_falls_back_to_legacy_attribute(monkeypatch):
    strategy = object()
    monkeypatch.setattr(
        monte_carlo.my_backtesting,
        "resolve_strategy",
        lambda strategy_id: None,
    )
    monkeypatch.setattr(
        monte_carlo.my_backtesting,
        "legacy_test_strategy",
        strategy,
        raising=False,
    )

    assert monte_carlo._resolve_strategy("legacy_test_strategy") is strategy


def test_candles_based_preloads_declarative_strategy_cache(monkeypatch):
    definition = get_builtin_template("hma_rsi_linreg")
    strategy = SimpleNamespace(
        definition=definition,
        parameter_values={},
        strategy_data_cache={},
    )
    base_df = monte_carlo.pd.DataFrame(
        {
            "Open": [1.0, 1.1, 1.2],
            "High": [1.1, 1.2, 1.3],
            "Low": [0.9, 1.0, 1.1],
            "Close": [1.0, 1.1, 1.2],
            "Volume": [100.0, 110.0, 120.0],
        },
        index=monte_carlo.pd.date_range("2026-01-01", periods=3, freq="4h"),
    )
    backtest_row = monte_carlo.pd.DataFrame([{"Ema_Fast": 10, "Ema_Slow": 20}])
    cache_calls = []
    cleared = []

    monkeypatch.setattr(monte_carlo, "_resolve_strategy", lambda strategy_id: strategy)
    monkeypatch.setattr(
        monte_carlo.database,
        "get_backtesting_settings",
        lambda: {"Cash_Value": 1000.0, "Commission_Value": 0.0},
    )
    monkeypatch.setattr(monte_carlo.database, "get_strategy_risk", lambda strategy_id: {})
    monkeypatch.setattr(
        monte_carlo.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, timeframe, strategy_id: backtest_row,
    )
    monkeypatch.setattr(monte_carlo.my_backtesting, "get_data", lambda symbol, timeframe: base_df)
    monkeypatch.setattr(
        monte_carlo.my_backtesting,
        "build_declarative_strategy_data_cache",
        lambda definition_arg, symbol, timeframe: cache_calls.append(
            (definition_arg, symbol, timeframe)
        )
        or {("BOMEUSDC", "1d"): base_df},
    )

    def fake_set_cache(strategy_arg, cache):
        if cache:
            strategy_arg.strategy_data_cache = cache
        else:
            cleared.append(True)
            strategy_arg.strategy_data_cache = {}

    monkeypatch.setattr(
        monte_carlo.my_backtesting,
        "set_declarative_strategy_data_cache",
        fake_set_cache,
    )
    monkeypatch.setattr(monte_carlo, "_prepare_backtest_df", lambda df, *args: df)
    monkeypatch.setattr(monte_carlo, "_run_strategy_on_df", lambda *args: object())
    monkeypatch.setattr(monte_carlo, "_stats_equity_curve", lambda stats, initial_cash: [initial_cash])
    monkeypatch.setattr(monte_carlo, "_stats_to_metrics", lambda stats: {"Net Profit": 0.0})
    monkeypatch.setattr(
        monte_carlo,
        "_build_result",
        lambda *args, **kwargs: {"summary": {"valid_scenarios": 1, "total_scenarios": 1}},
    )

    result = monte_carlo.run_candles_based(
        "BOMEUSDC",
        "4h",
        "hma_rsi_linreg_copy",
        scenarios=1,
        seed=42,
    )

    assert result["summary"]["valid_scenarios"] == 1
    assert cache_calls == [(definition, "BOMEUSDC", "4h")]
    assert cleared == [True]
