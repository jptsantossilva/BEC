from types import SimpleNamespace
import json

import bec.utils.database as database
import bec.my_backtesting as my_backtesting
import numpy as np
import pandas as pd
from bec.strategy_builder import schema
from bec.strategy_builder.templates import get_builtin_template
from bec.utils.take_profit import normalize_take_profit_levels, take_profit_enabled


def _apply_strategy_risk(strategy, strategy_risk: dict):
    atr_risk = strategy_risk.get("atr_trailing", {}) if isinstance(strategy_risk, dict) else {}
    take_profits = normalize_take_profit_levels(strategy_risk.get("take_profits", []) if isinstance(strategy_risk, dict) else [])
    strategy.stop_loss_pct = float(strategy_risk.get("stop_loss_pct", 0.0) or 0.0)
    strategy.atr_trailing_enabled = bool(atr_risk.get("enabled", False))
    strategy.atr_period = int(atr_risk.get("period", 14) or 14)
    strategy.atr_multiplier = float(atr_risk.get("multiplier", 1.8) or 1.8)
    strategy.atr_activation_pnl = float(atr_risk.get("activation_pnl_pct", 0.0) or 0.0)
    strategy.take_profits = take_profits
    strategy.take_profit_enabled = take_profit_enabled(take_profits)



def test_builtin_template_has_no_risk_controls():
    definition = get_builtin_template("ema_cross")
    risk = schema.extract_execution_risk(definition)
    strategy = SimpleNamespace()

    _apply_strategy_risk(strategy, risk)

    assert strategy.stop_loss_pct == 0.0
    assert strategy.atr_trailing_enabled is False
    assert strategy.take_profit_enabled is False
    assert strategy.take_profits == []


def test_custom_definition_applies_stop_atr_and_take_profits():
    definition = {
        "schema_version": 2,
        "engine": "bec_strategy_ast_v2",
        "name": "Custom",
        "description": "test",
        "constraints": {
            "market": "spot",
            "side": "long",
            "order_type": "market",
            "allowed_actions": ["buy", "sell"],
            "allowed_timeframes": ["1h"],
        },
        "parameters": {},
        "entry": {
            "logic": "all",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {"type": "price", "field": "Close", "timeframe": "1h"},
                    "operator": "above",
                    "right": {"type": "value", "value": 0},
                }
            ],
            "action": {"type": "buy", "order_type": "market", "size_pct": 100},
        },
        "exit": {
            "logic": "any",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {"type": "price", "field": "Close", "timeframe": "1h"},
                    "operator": "below",
                    "right": {"type": "value", "value": 0},
                }
            ],
            "action": {"type": "sell", "order_type": "market", "size_pct": 100},
        },
        "risk": {
            "rules": [
                {"type": "stop_loss_pct", "pct": 6},
                {"type": "take_profit_pct", "pct": 10, "size_pct": 30},
                {"type": "take_profit_pct", "pct": 20, "size_pct": 40},
                {
                    "type": "atr_stop",
                    "indicator": {
                        "type": "indicator",
                        "name": "ATR",
                        "timeframe": "1h",
                        "source": {"type": "price", "field": "Close", "timeframe": "1h"},
                        "params": {"period": 10},
                        "output": "value",
                    },
                    "multiplier": 2.0,
                    "activation_pnl_pct": 3.5,
                },
            ]
        },
    }
    risk = schema.extract_execution_risk(definition)
    strategy = SimpleNamespace()

    _apply_strategy_risk(strategy, risk)

    assert strategy.stop_loss_pct == 6.0
    assert strategy.atr_trailing_enabled is True
    assert strategy.atr_period == 10
    assert strategy.atr_multiplier == 2.0
    assert strategy.atr_activation_pnl == 3.5
    assert strategy.take_profit_enabled is True
    assert len(strategy.take_profits) == 2
    assert strategy.take_profits[0] == {"level": 1, "pnl_pct": 10.0, "amount_pct": 30.0}
    assert strategy.take_profits[1] == {"level": 2, "pnl_pct": 20.0, "amount_pct": 40.0}


def test_run_backtest_risk_path_uses_database_strategy_risk(monkeypatch):
    strategy = SimpleNamespace()
    monkeypatch.setattr(
        database,
        "get_strategy_risk",
        lambda strategy_id: {
            "stop_loss_pct": 9.0,
            "atr_trailing": {"enabled": True, "period": 12, "multiplier": 1.5, "activation_pnl_pct": 4.0},
            "take_profits": [{"level": 1, "pnl_pct": 8.0, "amount_pct": 25.0}],
        },
    )

    _apply_strategy_risk(strategy, database.get_strategy_risk("custom_x"))

    assert strategy.stop_loss_pct == 9.0
    assert strategy.atr_period == 12
    assert strategy.take_profits[0] == {"level": 1, "pnl_pct": 8.0, "amount_pct": 25.0}


def test_declarative_indicator_plot_specs_include_definition_indicators_without_duplicates():
    definition = {
        "schema_version": 2,
        "engine": "bec_strategy_ast_v2",
        "name": "Custom",
        "description": "test",
        "constraints": {
            "market": "spot",
            "side": "long",
            "order_type": "market",
            "allowed_actions": ["buy", "sell"],
            "allowed_timeframes": ["1d"],
        },
        "parameters": {},
        "entry": {
            "logic": "all",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {
                        "type": "indicator",
                        "name": "EMA",
                        "timeframe": "current",
                        "source": {"type": "price", "field": "Close", "timeframe": "current"},
                        "params": {"period": 20},
                        "output": "value",
                    },
                    "operator": "crosses_above",
                    "right": {
                        "type": "indicator",
                        "name": "EMA",
                        "timeframe": "current",
                        "source": {"type": "price", "field": "Close", "timeframe": "current"},
                        "params": {"period": 42},
                        "output": "value",
                    },
                },
                {
                    "type": "comparison",
                    "left": {"type": "price", "field": "Close", "timeframe": "current"},
                    "operator": "above",
                    "right": {
                        "type": "indicator",
                        "name": "SMA",
                        "timeframe": "current",
                        "source": {"type": "price", "field": "Close", "timeframe": "current"},
                        "params": {"period": 50},
                        "output": "value",
                    },
                },
            ],
            "action": {"type": "buy", "order_type": "market", "size_pct": 100},
        },
        "exit": {
            "logic": "any",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {
                        "type": "indicator",
                        "name": "EMA",
                        "timeframe": "current",
                        "source": {"type": "price", "field": "Close", "timeframe": "current"},
                        "params": {"period": 20},
                        "output": "value",
                    },
                    "operator": "crosses_below",
                    "right": {
                        "type": "indicator",
                        "name": "EMA",
                        "timeframe": "current",
                        "source": {"type": "price", "field": "Close", "timeframe": "current"},
                        "params": {"period": 42},
                        "output": "value",
                    },
                }
            ],
            "action": {"type": "sell", "order_type": "market", "size_pct": 100},
        },
        "risk": {"rules": [{"type": "stop_loss_pct", "pct": 10}]},
    }

    specs = list(
        my_backtesting._iter_declarative_indicator_plot_specs(
            definition,
            {},
            base_timeframe="1d",
        )
    )

    assert [spec["label"] for spec in specs] == ["EMA 20", "EMA 42", "SMA 50"]
    assert all(spec["overlay"] for spec in specs)


def test_declarative_strategy_loader_uses_cache_and_scales_price_data(monkeypatch):
    daily = pd.DataFrame(
        {
            "Open": [0.01, 0.02],
            "High": [0.011, 0.022],
            "Low": [0.009, 0.018],
            "Close": [0.0105, 0.021],
            "Volume": [1000, 2000],
        }
    )
    monkeypatch.setattr(
        my_backtesting,
        "get_data",
        lambda symbol, timeframe: (_ for _ in ()).throw(
            AssertionError("cache should avoid get_data")
        ),
    )
    strategy = SimpleNamespace(
        data_price_scale=1e-8,
        strategy_data_cache={("BABYUSDC", "1d"): daily},
    )

    scaled = my_backtesting.DeclarativeStrategy._load_strategy_data(
        strategy,
        "BABYUSDC",
        "1d",
    )

    assert scaled["Close"].iloc[-1] == daily["Close"].iloc[-1] * 1e-8
    assert scaled["Volume"].iloc[-1] == daily["Volume"].iloc[-1]
    assert daily["Close"].iloc[-1] == 0.021


def test_build_declarative_strategy_data_cache_loads_only_external_timeframes(monkeypatch):
    loaded = []
    daily = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [1.0],
            "Low": [1.0],
            "Close": [1.0],
            "Volume": [100.0],
        }
    )

    def fake_get_data(symbol, timeframe):
        loaded.append((symbol, timeframe))
        return daily

    monkeypatch.setattr(my_backtesting, "get_data", fake_get_data)
    definition = {
        "entry": {
            "conditions": [
                {
                    "left": {"type": "price", "field": "Close", "timeframe": "current"},
                    "operator": "greater_than",
                    "right": {
                        "type": "indicator",
                        "name": "LINREG",
                        "timeframe": "1d",
                        "source": {
                            "type": "price",
                            "field": "Close",
                            "timeframe": "1d",
                        },
                        "params": {"period": 50},
                    },
                }
            ]
        },
        "exit": {"conditions": []},
    }

    cache = my_backtesting.build_declarative_strategy_data_cache(
        definition,
        "babyusdc",
        "4h",
    )

    assert loaded == [("BABYUSDC", "1d")]
    assert list(cache.keys()) == [("BABYUSDC", "1d")]
    assert cache[("BABYUSDC", "1d")].equals(daily)


def test_build_declarative_strategy_data_cache_skips_current_and_base(monkeypatch):
    loaded = []
    monkeypatch.setattr(
        my_backtesting,
        "get_data",
        lambda symbol, timeframe: loaded.append((symbol, timeframe)) or pd.DataFrame(),
    )
    definition = {
        "entry": {
            "conditions": [
                {
                    "left": {"type": "price", "field": "Close", "timeframe": "current"},
                    "operator": "greater_than",
                    "right": {"type": "price", "field": "Close", "timeframe": "4h"},
                }
            ]
        },
        "exit": {"conditions": []},
    }

    cache = my_backtesting.build_declarative_strategy_data_cache(
        definition,
        "BABYUSDC",
        "4h",
    )

    assert cache == {}
    assert loaded == []


def test_declarative_strategy_parameters_config_uses_definition_indicators_only():
    strategy = SimpleNamespace(
        _definition={
            "engine": "bec_strategy_ast_v2",
            "entry": {
                "conditions": [
                    {
                        "left": {
                            "type": "indicator",
                            "name": "EMA",
                            "timeframe": "current",
                            "params": {"period": 20},
                        },
                        "right": {
                            "type": "indicator",
                            "name": "EMA",
                            "timeframe": "current",
                            "params": {"period": 42},
                        },
                    }
                ]
            },
            "exit": {},
            "risk": {},
        },
        _parameters={},
        execution_timeframe="1d",
        nFastSMA=50,
        nSlowSMA=200,
    )

    params = my_backtesting.build_strategy_parameters_config(strategy)

    assert params == {
        "definition_indicators": [
            {"name": "EMA", "period": 20, "timeframe": "1d"},
            {"name": "EMA", "period": 42, "timeframe": "1d"},
        ]
    }


def test_declarative_optimize_params_use_definition_parameter_ranges():
    definition = {
        "parameters": {
            "ema_fast": {
                "type": "int",
                "default": 20,
                "min": 10,
                "max": 30,
                "step": 10,
                "optimizable": True,
            },
            "ema_slow": {
                "type": "int",
                "default": 40,
                "min": 40,
                "max": 60,
                "step": 10,
                "optimizable": True,
            },
            "ignored": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 10,
                "step": 1,
                "optimizable": False,
            },
        },
        "parameter_constraints": [
            {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
        ],
    }

    optimize_params, names = my_backtesting.build_declarative_optimize_params(definition, "Return [%]")

    assert names == ["ema_fast", "ema_slow"]
    assert list(optimize_params["ema_fast"]) == [10, 20, 30]
    assert list(optimize_params["ema_slow"]) == [40, 50, 60]
    assert optimize_params["maximize"] == "Return [%]"
    assert optimize_params["constraint"](SimpleNamespace(ema_fast=20, ema_slow=40)) is True
    assert optimize_params["constraint"](SimpleNamespace(ema_fast=40, ema_slow=20)) is False


def test_declarative_optimize_params_fallback_ema_constraint_without_definition_rules():
    definition = {
        "parameters": {
            "ema_fast": {
                "type": "int",
                "default": 20,
                "min": 10,
                "max": 30,
                "step": 10,
                "optimizable": True,
            },
            "ema_slow": {
                "type": "int",
                "default": 40,
                "min": 40,
                "max": 60,
                "step": 10,
                "optimizable": True,
            },
        }
    }

    optimize_params, _ = my_backtesting.build_declarative_optimize_params(definition, "Return [%]")

    assert optimize_params["constraint"](SimpleNamespace(ema_fast=20, ema_slow=40)) is True
    assert optimize_params["constraint"](SimpleNamespace(ema_fast=40, ema_slow=20)) is False


def test_declarative_optimize_params_hma_constraint_from_definition():
    definition = {
        "parameters": {
            "hma_fast": {
                "type": "int",
                "default": 16,
                "min": 10,
                "max": 30,
                "step": 10,
                "optimizable": True,
            },
            "hma_slow": {
                "type": "int",
                "default": 65,
                "min": 40,
                "max": 60,
                "step": 10,
                "optimizable": True,
            },
        },
        "parameter_constraints": [
            {"left": "hma_fast", "operator": "less_than", "right": "hma_slow"},
        ],
    }

    optimize_params, names = my_backtesting.build_declarative_optimize_params(definition, "Return [%]")

    assert names == ["hma_fast", "hma_slow"]
    assert optimize_params["constraint"](SimpleNamespace(hma_fast=10, hma_slow=40)) is True
    assert optimize_params["constraint"](SimpleNamespace(hma_fast=30, hma_slow=20)) is False


def test_serial_grid_optimize_applies_constraints_and_returns_best_heatmap():
    class FakeBacktest:
        def __init__(self):
            self.runs = []

        def run(self, **params):
            self.runs.append(params)
            return pd.Series({"SQN": params["hma_slow"] - params["hma_fast"]})

    optimize_params = {
        "hma_fast": [10, 20],
        "hma_slow": [10, 30],
        "constraint": lambda param: param.hma_fast < param.hma_slow,
        "maximize": "SQN",
        "return_heatmap": True,
    }

    stats, heatmap = my_backtesting.run_serial_grid_optimize(FakeBacktest(), optimize_params)

    assert stats["SQN"] == 20
    assert list(heatmap.dropna().index) == [(10, 30), (20, 30)]
    assert heatmap.loc[(10, 30)] == 20
    assert heatmap.loc[(20, 30)] == 10


def test_serial_grid_optimize_limits_large_grids_deterministically():
    class FakeBacktest:
        def __init__(self):
            self.runs = []

        def run(self, **params):
            self.runs.append(params)
            return pd.Series({"SQN": params["slow"] - params["fast"]})

    bt = FakeBacktest()
    optimize_params = {
        "fast": range(1, 21),
        "slow": range(1, 21),
        "constraint": lambda param: param.fast < param.slow,
        "maximize": "SQN",
        "max_tries": 5,
        "return_heatmap": True,
    }

    _stats, heatmap = my_backtesting.run_serial_grid_optimize(bt, optimize_params)

    assert len(heatmap) == 5
    assert len(bt.runs) == 6  # sampled runs plus final best-params run
    assert heatmap.index[0] == (1, 2)
    assert heatmap.index[-1] == (19, 20)


def test_declarative_native_optimize_uses_full_definition_params(capsys):
    class FakeBacktest:
        def __init__(self):
            self.optimize_calls = []

        def optimize(self, **params):
            self.optimize_calls.append(params)
            return pd.Series({"SQN": 1.0}), pd.Series(
                [1.0],
                index=pd.MultiIndex.from_tuples(
                    [(10, 20)],
                    names=["hma_fast", "hma_slow"],
                ),
            )

    optimize_params = {
        "hma_fast": [10, 20],
        "hma_slow": [20, 30],
        "constraint": lambda param: param.hma_fast < param.hma_slow,
        "maximize": "SQN",
        "return_heatmap": True,
    }
    bt = FakeBacktest()

    stats, heatmap = my_backtesting.run_declarative_native_optimize(
        bt,
        optimize_params,
        combination_count=3,
        max_combinations=10,
    )

    assert stats["SQN"] == 1.0
    assert heatmap.index.names == ["hma_fast", "hma_slow"]
    assert bt.optimize_calls == [optimize_params]
    assert "max_tries" not in bt.optimize_calls[0]
    assert "3 combinations" in capsys.readouterr().out


def test_declarative_native_optimize_warns_without_limiting(capsys):
    class FakeBacktest:
        def __init__(self):
            self.optimize_calls = []

        def optimize(self, **params):
            self.optimize_calls.append(params)
            return pd.Series({"SQN": 2.0}), pd.Series(dtype=float)

    optimize_params = {
        "fast": [1, 2, 3],
        "slow": [4, 5, 6],
        "maximize": "SQN",
        "return_heatmap": True,
    }
    bt = FakeBacktest()

    my_backtesting.run_declarative_native_optimize(
        bt,
        optimize_params,
        combination_count=9,
        max_combinations=5,
    )

    assert bt.optimize_calls == [optimize_params]
    output = capsys.readouterr().out
    assert "overfitting alert threshold" in output
    assert "Continuing with the full native optimizer" in output


def test_declarative_strategy_next_reuses_prepared_indicators(monkeypatch):
    definition = {
        "schema_version": 2,
        "engine": "bec_strategy_ast_v2",
        "name": "Prepared Eval",
        "description": "test",
        "constraints": {
            "market": "spot",
            "side": "long",
            "order_type": "market",
            "allowed_actions": ["buy", "sell"],
            "allowed_timeframes": ["1h"],
        },
        "parameters": {},
        "entry": {
            "logic": "all",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {"type": "price", "field": "Close", "timeframe": "current"},
                    "operator": "greater_than",
                    "right": {"type": "value", "value": 0},
                }
            ],
            "action": {"type": "buy", "order_type": "market", "size_pct": 100},
        },
        "exit": {
            "logic": "any",
            "conditions": [
                {
                    "type": "comparison",
                    "left": {"type": "price", "field": "Close", "timeframe": "current"},
                    "operator": "less_than",
                    "right": {"type": "value", "value": 0},
                }
            ],
            "action": {"type": "sell", "order_type": "market", "size_pct": 100},
        },
    }
    strategy = type(
        "prepared_eval_declarative",
        (my_backtesting.DeclarativeStrategy,),
        {
            "definition": definition,
            "parameter_values": {},
            "execution_symbol": "BTCUSDC",
            "execution_timeframe": "1h",
        },
    )
    df = pd.DataFrame(
        {
            "Open": [1.0, 1.0, 1.0, 1.0],
            "High": [1.1, 1.1, 1.1, 1.1],
            "Low": [0.9, 0.9, 0.9, 0.9],
            "Close": [1.0, 1.0, 1.0, 1.0],
            "Volume": [100.0, 100.0, 100.0, 100.0],
        },
        index=pd.date_range("2026-01-01", periods=4, freq="h"),
    )
    add_indicator_calls = 0
    original_add_indicators = my_backtesting.strategy_engine.add_indicators

    def count_add_indicators(*args, **kwargs):
        nonlocal add_indicator_calls
        add_indicator_calls += 1
        return original_add_indicators(*args, **kwargs)

    monkeypatch.setattr(
        my_backtesting.strategy_engine,
        "add_indicators",
        count_add_indicators,
    )

    bt = my_backtesting.FractionalBacktest(
        df,
        strategy=strategy,
        cash=1000,
        commission=0,
        finalize_trades=True,
        exclusive_orders=True,
        trade_on_close=True,
    )

    stats = bt.run()

    assert stats["# Trades"] == 1
    assert add_indicator_calls == 1


def test_count_declarative_optimization_combinations_respects_constraints():
    definition = {
        "parameters": {
            "fast": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 3,
                "step": 1,
                "optimizable": True,
            },
            "slow": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 4,
                "step": 1,
                "optimizable": True,
            },
        },
        "parameter_constraints": [
            {"left": "fast", "operator": "less_than", "right": "slow"},
        ],
    }

    count, names = my_backtesting.count_declarative_optimization_combinations(
        definition,
        "SQN",
    )

    assert names == ["fast", "slow"]
    assert count == 6


def test_builtin_templates_include_parameter_constraints():
    ema_definition = get_builtin_template("ema_cross")
    hma_definition = get_builtin_template("hma_rsi_linreg")

    assert ema_definition["parameter_constraints"] == [
        {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
    ]
    assert hma_definition["parameter_constraints"] == [
        {"left": "hma_fast", "operator": "less_than", "right": "hma_slow"},
    ]


def test_backtest_config_json_default_serializes_numpy_scalars():
    payload = {
        "strategy_parameters": {
            "definition_indicators": [
                {"name": "EMA", "period": np.int64(80), "timeframe": "1d"},
            ]
        }
    }

    encoded = json.dumps(payload, default=my_backtesting._json_default)

    assert '"period": 80' in encoded


def test_exchange_fee_has_deterministic_effect_on_backtest_equity():
    class BuyAndHold(my_backtesting.Strategy):
        def init(self):
            pass

        def next(self):
            if not self.position and len(self.data) == 2:
                self.buy(size=0.9)

    index = pd.date_range("2026-01-01", periods=6, freq="h")
    close = [100.0, 101.0, 103.0, 105.0, 107.0, 110.0]
    frame = pd.DataFrame(
        {
            "Open": close,
            "High": [value + 1 for value in close],
            "Low": [value - 1 for value in close],
            "Close": close,
            "Volume": [100.0] * len(close),
        },
        index=index,
    )

    def final_equity(commission):
        stats = my_backtesting.FractionalBacktest(
            frame,
            strategy=BuyAndHold,
            cash=1000,
            commission=commission,
            finalize_trades=True,
            exclusive_orders=True,
            trade_on_close=True,
        ).run()
        return float(stats["Equity Final [$]"])

    without_fee = final_equity(0.0)
    kraken_fee = final_equity(0.004)

    assert kraken_fee == final_equity(0.004)
    assert kraken_fee < without_fee


def test_report_basename_is_exchange_scoped_and_filesystem_safe():
    kraken = my_backtesting.backtest_report_basename(
        "ema", "1h", "BTC/USDC", "kraken"
    )
    binance = my_backtesting.backtest_report_basename(
        "ema", "1h", "BTC/USDC", "binance"
    )

    assert kraken != binance
    assert kraken == "kraken - ema - 1h - BTC-USDC"
    assert "/" not in kraken
