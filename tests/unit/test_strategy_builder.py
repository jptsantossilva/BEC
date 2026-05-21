import json
import pickle
import sqlite3
import uuid

import pandas as pd
import pytest
import requests

import bec.utils.database as database
import bec.my_backtesting as my_backtesting
import bec.main as live_main
from pages import strategy_builder
from bec.strategy_builder import ai_builder, engine, packages, schema
from bec.strategy_builder.templates import get_builtin_template, get_empty_strategy_template


def _strategy_id(prefix="test_strategy"):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def test_builtin_templates_validate():
    for strategy_id in [
        "ema_cross",
        "ema_cross_with_market_phases",
        "market_phases",
        "hma_rsi_linreg",
        "bullmarketsupportband",
        "wema20",
    ]:
        definition = get_builtin_template(strategy_id)
        assert schema.validate_definition(definition)["engine"] == "bec_strategy_ast_v2"
        assert "1w" in definition["constraints"]["allowed_timeframes"]


def test_ast_v2_accepts_weekly_timeframe():
    definition = _ast_definition(
        [_comparison(_price("Close", "1w"), "above", _indicator("SMA", {"period": 3}, timeframe="1w"))]
    )

    validated = schema.validate_definition(definition)

    assert validated["entry"]["conditions"][0]["left"]["timeframe"] == "1w"


def test_builtin_optimization_ranges_are_capped():
    expected_counts = {
        "ema_cross": 145,
        "ema_cross_with_market_phases": 145,
        "market_phases": 0,
        "hma_rsi_linreg": 145,
        "bullmarketsupportband": 0,
        "wema20": 0,
    }

    for strategy_id, expected_count in expected_counts.items():
        definition = get_builtin_template(strategy_id)
        count, _ = my_backtesting.count_declarative_optimization_combinations(definition)

        assert count == expected_count

    ema_params = get_builtin_template("ema_cross")["parameters"]
    assert ema_params["ema_fast"] == {
        "type": "int",
        "default": 10,
        "min": 10,
        "max": 101,
        "step": 10,
        "optimizable": True,
    }
    assert ema_params["ema_slow"] == {
        "type": "int",
        "default": 20,
        "min": 10,
        "max": 201,
        "step": 10,
        "optimizable": True,
    }


def test_empty_strategy_template_validates_and_does_not_signal():
    definition = get_empty_strategy_template("Blank Draft")
    df = _ohlcv([10, 11, 12, 13])

    validated = schema.validate_definition(definition)

    assert validated["metadata"]["source"] == "user_created"
    assert engine.evaluate_entry(df, definition, {}) is False
    assert engine.evaluate_exit(df, definition, {}) is False


def test_builtin_templates_bind_indicators_to_definition_parameters():
    definition = get_builtin_template("ema_cross")

    assert definition["entry"]["conditions"][0]["left"]["period_param"] == "ema_fast"
    assert definition["entry"]["conditions"][0]["right"]["period_param"] == "ema_slow"


def test_builtin_strategy_resolves_to_declarative_strategy(monkeypatch):
    definition = get_builtin_template("ema_cross")
    monkeypatch.setattr(
        database,
        "get_strategy_by_id",
        lambda strategy_id: pd.DataFrame(
            [
                {
                    "Id": strategy_id,
                    "Type": "builtin",
                    "Definition_JSON": json.dumps(definition),
                }
            ]
        ),
    )

    strategy_class = my_backtesting.resolve_strategy("ema_cross")

    assert issubclass(strategy_class, my_backtesting.DeclarativeStrategy)
    assert strategy_class.definition["engine"] == "bec_strategy_ast_v2"


def test_declarative_strategy_class_is_picklable(monkeypatch):
    definition = get_builtin_template("ema_cross")
    monkeypatch.setattr(
        database,
        "get_strategy_by_id",
        lambda strategy_id: pd.DataFrame(
            [
                {
                    "Id": strategy_id,
                    "Type": "builtin",
                    "Definition_JSON": json.dumps(definition),
                }
            ]
        ),
    )

    strategy_class = my_backtesting.resolve_strategy("ema_cross")
    restored = pickle.loads(pickle.dumps(strategy_class))

    assert restored is strategy_class
    assert getattr(my_backtesting, "ema_cross_declarative") is strategy_class


def test_live_declarative_helpers_use_parameter_overrides(monkeypatch):
    definition = get_builtin_template("ema_cross")
    df = _ohlcv([10, 9, 8, 9, 11])
    monkeypatch.setattr(live_main.database, "get_strategy_definition", lambda strategy_id: definition)

    parameters = live_main.get_strategy_parameter_overrides("ema_cross", 2, 4)
    live_main.apply_strategy_technicals(
        df,
        "ema_cross",
        2,
        4,
        symbol="TESTUSDC",
        timeframe="1h",
        parameters=parameters,
    )

    assert live_main.get_strategy_buy_condition(
        "ema_cross",
        "TESTUSDC",
        "1h",
        df,
        df.iloc[-1],
        parameters=parameters,
    )[0] is True


def test_live_runtime_context_maps_optimized_parameters_from_definition(monkeypatch):
    definition = get_builtin_template("hma_rsi_linreg")
    row = pd.Series({"Strategy_Params_JSON": ""})
    backtest_config = {
        "strategy_parameters": {
            "parameters": {"hma_fast": 30, "hma_slow": 90, "rsi_period": 14, "rsi_min": 52, "linreg_period": 50}
        }
    }
    monkeypatch.setattr(live_main.database, "get_strategy_definition", lambda strategy_id: definition)
    monkeypatch.setattr(
        live_main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: pd.DataFrame([{"Backtest_Config_JSON": json.dumps(backtest_config)}]),
    )

    context = live_main.get_strategy_runtime_context("any_strategy_id", "TESTUSDC", "1h", pos_row=row)

    assert context["parameters"]["hma_fast"] == 30
    assert context["parameters"]["hma_slow"] == 90
    assert context["primary_parameter_names"] == ("hma_fast", "hma_slow")
    assert context["setup"] == "30/90"


def test_live_runtime_context_prefers_position_definition_snapshot(monkeypatch):
    position_definition = get_builtin_template("hma_rsi_linreg")
    strategy_definition = get_builtin_template("ema_cross")
    snapshot = {
        "engine": "bec_strategy_ast_v2",
        "definition": position_definition,
        "parameters": {"hma_fast": 18, "hma_slow": 77, "rsi_period": 14, "rsi_min": 52, "linreg_period": 50},
    }
    row = pd.Series({"Strategy_Params_JSON": json.dumps(snapshot)})
    monkeypatch.setattr(live_main.database, "get_strategy_definition", lambda strategy_id: strategy_definition)
    monkeypatch.setattr(
        live_main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: pd.DataFrame(),
    )

    context = live_main.get_strategy_runtime_context("ema_cross", "TESTUSDC", "1h", pos_row=row, prefer_position_params=True)

    assert context["definition"]["name"] == "HMA RSI LINREG"
    assert context["parameters"]["hma_fast"] == 18
    assert context["parameters"]["hma_slow"] == 77
    assert context["setup"] == "18/77"


def test_live_runtime_context_reads_backtest_config_parameters(monkeypatch):
    definition = get_builtin_template("hma_rsi_linreg")
    row = pd.Series({"Strategy_Params_JSON": ""})
    backtest_config = {
        "strategy_parameters": {
            "parameters": {"hma_fast": 22, "hma_slow": 88, "rsi_period": 14, "rsi_min": 52, "linreg_period": 50}
        }
    }
    monkeypatch.setattr(live_main.database, "get_strategy_definition", lambda strategy_id: definition)
    monkeypatch.setattr(
        live_main.database,
        "get_backtesting_results_by_symbol_timeframe_strategy",
        lambda symbol, time_frame, strategy_id: pd.DataFrame(
            [{"Backtest_Config_JSON": json.dumps(backtest_config)}]
        ),
    )

    context = live_main.get_strategy_runtime_context("hma_rsi_linreg", "TESTUSDC", "1h", pos_row=row)

    assert context["parameters"]["hma_fast"] == 22
    assert context["parameters"]["hma_slow"] == 88
    assert context["setup"] == "22/88"


def test_live_strategy_uses_tuned_parameters_is_definition_driven(monkeypatch):
    quick = _indicator("EMA", {"period": 3})
    quick["period_param"] = "quick"
    slow_line = _indicator("EMA", {"period": 8})
    slow_line["period_param"] = "slow_line"
    definition = _ast_definition(
        [_comparison(_price(), "above", quick)],
        [_comparison(_price(), "below", slow_line)],
    )
    definition["parameters"] = {
        "quick": {"type": "int", "default": 3, "optimizable": True},
        "slow_line": {"type": "int", "default": 8, "optimizable": True},
    }
    monkeypatch.setattr(live_main.database, "get_strategy_definition", lambda strategy_id: definition)

    assert live_main.strategy_uses_tuned_parameters("custom_without_hardcoded_id") is True


def test_strategy_params_json_snapshots_declarative_builtin(monkeypatch):
    definition = get_builtin_template("ema_cross")
    monkeypatch.setattr(database, "get_strategy_definition", lambda strategy_id: definition)

    params = database.parse_strategy_params(database.build_strategy_params_json("ema_cross", 12, 34))

    assert params["engine"] == "bec_strategy_ast_v2"
    assert params["parameters"]["ema_fast"] == 12
    assert params["parameters"]["ema_slow"] == 34
    assert params["definition"]["entry"]["conditions"][0]["left"]["period_param"] == "ema_fast"


def test_component_diagram_omits_operational_flow_blocks():
    definition = get_builtin_template("ema_cross")
    html = strategy_builder.build_component_diagram_html(definition, definition.get("risk", {}), "ema_cross")

    assert "Market Data" not in html
    assert "OHLCV candles" not in html
    assert "Trade" not in html
    assert "Enter Long" not in html
    assert "Open Position" not in html
    assert "Close Long" not in html
    assert "Entry Conditions" in html
    assert "Exit Conditions" in html
    assert "<a" in html
    assert "target=\"_top\"" in html
    assert "strategy_block=" in html


def test_component_diagram_summarizes_group_conditions():
    definition = get_builtin_template("ema_cross")
    definition["entry"]["conditions"].append(
        {
            "type": "group",
            "logic": "any",
            "conditions": [
                _comparison(_price("Close", "1h"), "above", _indicator("SMA", {"period": 50}, timeframe="1h")),
                _comparison(_price("Close", "1h"), "above", _indicator("SMA", {"period": 200}, timeframe="1h")),
            ],
        }
    )

    html = strategy_builder.build_component_diagram_html(definition, definition.get("risk", {}), "ema_cross")

    assert "? ? ?" not in html
    assert "ANY group" not in html
    assert "OR" in html
    assert "Price Close above SMA 50" in html
    assert "Price Close above SMA 200" in html


def test_component_diagram_shows_and_or_separators_between_conditions():
    definition = get_builtin_template("ema_cross")
    definition["entry"]["conditions"].append(
        {
            "type": "group",
            "logic": "any",
            "conditions": [
                _comparison(_price("Close", "1d"), "above", _indicator("SMA", {"period": 50}, timeframe="1d")),
                _comparison(_price("Close", "1d"), "above", _indicator("SMA", {"period": 200}, timeframe="1d")),
            ],
        }
    )

    html = strategy_builder.build_component_diagram_html(definition, definition.get("risk", {}), "ema_cross")

    assert 'logic-separator logic-and' in html
    assert 'logic-separator logic-or' in html
    assert "AND" in html
    assert "OR" in html


def test_component_diagram_entry_exit_and_ast_risk_blocks_are_clickable():
    definition = get_builtin_template("ema_cross")
    definition["risk"]["rules"] = [
        {"type": "stop_loss_pct", "pct": 10},
        {"type": "take_profit_pct", "pct": 8, "size_pct": 50},
    ]

    html = strategy_builder.build_component_diagram_html(definition, definition.get("risk", {}), "ema_cross")

    assert "%22kind%22%3A%22rule%22%2C%22group%22%3A%22entry%22" in html
    assert "%22kind%22%3A%22rule%22%2C%22group%22%3A%22exit%22" in html
    assert "%22kind%22%3A%22ast_risk%22%2C%22index%22%3A0" in html
    assert "%22kind%22%3A%22ast_risk%22%2C%22index%22%3A1" in html
    assert 'class="node disabled' not in html
    assert "Risk Management" in html
    assert "Take Profits" in html


def test_engine_evaluates_ema_cross_entry_and_exit():
    definition = get_builtin_template("ema_cross")
    parameters = {"ema_fast": 2, "ema_slow": 4}
    df = pd.DataFrame(
        {
            "Open": [10, 9, 8, 9, 11, 13, 12, 10],
            "High": [10, 9, 8, 9, 11, 13, 12, 10],
            "Low": [10, 9, 8, 9, 11, 13, 12, 10],
            "Close": [10, 9, 8, 9, 11, 13, 12, 10],
            "Volume": [100] * 8,
        }
    )

    prepared = engine.add_indicators(df, definition, parameters)

    assert any(engine.evaluate_entry(prepared.iloc[: idx + 1], definition, parameters) for idx in range(len(prepared)))
    assert any(engine.evaluate_exit(prepared.iloc[: idx + 1], definition, parameters) for idx in range(len(prepared)))


def test_package_round_trip_validates_without_code_execution():
    strategy_id = _strategy_id()
    definition = get_builtin_template("market_phases")
    try:
        database.upsert_custom_strategy(
            strategy_id=strategy_id,
            name="Test Strategy Package",
            definition=definition,
            metadata={"author": "test"},
            status="draft",
        )
        exported = database.export_strategy_package(strategy_id)
        imported = packages.validate_import_package(exported)

        assert imported["strategy"]["name"] == "Test Strategy Package"
        assert imported["definition"]["entry"] == definition["entry"]
        assert "risk" not in imported
    finally:
        with sqlite3.connect("data.db") as conn:
            conn.execute("DELETE FROM Strategies WHERE Id = ?", (strategy_id,))
            conn.commit()


def test_draft_custom_strategy_is_hidden_from_live_main_strategies():
    strategy_id = _strategy_id()
    try:
        database.upsert_custom_strategy(
            strategy_id=strategy_id,
            name="Draft Hidden Strategy",
            definition=get_builtin_template("market_phases"),
            status="draft",
            main_strategy=True,
        )

        live_ids = set(database.get_strategies_for_main()["Id"].astype(str))

        assert strategy_id not in live_ids
    finally:
        with sqlite3.connect("data.db") as conn:
            conn.execute("DELETE FROM Strategies WHERE Id = ?", (strategy_id,))
            conn.commit()


def test_custom_strategy_usage_controls_live_strategy_lists():
    strategy_id = _strategy_id()
    try:
        database.upsert_custom_strategy(
            strategy_id=strategy_id,
            name="BTC Eligible Strategy",
            definition=get_builtin_template("market_phases"),
            status="draft",
            main_strategy=True,
            btc_strategy=True,
        )

        assert strategy_id not in set(database.get_strategies_for_main()["Id"].astype(str))
        assert strategy_id not in set(database.get_strategies_for_btc()["Id"].astype(str))

        database.approve_strategy_for_live(strategy_id)

        assert strategy_id in set(database.get_strategies_for_main()["Id"].astype(str))
        assert strategy_id in set(database.get_strategies_for_btc()["Id"].astype(str))

        database.set_strategy_usage(strategy_id, main_strategy=False, btc_strategy=False)

        assert strategy_id not in set(database.get_strategies_for_main()["Id"].astype(str))
        assert strategy_id not in set(database.get_strategies_for_btc()["Id"].astype(str))
    finally:
        with sqlite3.connect("data.db") as conn:
            conn.execute("DELETE FROM Strategies WHERE Id = ?", (strategy_id,))
            conn.commit()


def test_create_custom_strategy_creates_unique_user_draft():
    strategy_name = f"Blank Draft {uuid.uuid4().hex[:8]}"
    created_ids = []
    try:
        first_id = database.create_custom_strategy(strategy_name)
        second_id = database.create_custom_strategy(strategy_name)
        created_ids.extend([first_id, second_id])

        first = database.get_strategy_by_id(first_id).iloc[0]
        second = database.get_strategy_by_id(second_id).iloc[0]
        first_metadata = json.loads(first["Metadata_JSON"])

        assert first_id != second_id
        assert first["Type"] == "custom"
        assert first["Status"] == "draft"
        assert first["Parent_Strategy_Id"] == ""
        assert first["Version"] == 1
        assert first_metadata["source"] == "user_created"
        assert second["Id"].startswith(first_id)
    finally:
        with sqlite3.connect("data.db") as conn:
            for strategy_id in created_ids:
                conn.execute("DELETE FROM Strategies WHERE Id = ?", (strategy_id,))
            conn.commit()


def test_clone_name_sequence_persists_after_deleted_clone():
    source_id = _strategy_id("clone_source")
    created_ids = []
    try:
        database.upsert_custom_strategy(
            strategy_id=source_id,
            name="EMA Cross",
            definition=get_builtin_template("ema_cross"),
            status="draft",
            main_strategy=False,
        )

        assert database.get_next_strategy_clone_name(source_id) == "EMA Cross Copy"
        created_ids.append(database.clone_strategy(source_id, "EMA Cross Copy"))
        assert database.get_next_strategy_clone_name(source_id) == "EMA Cross Copy 2"
        second_id = database.clone_strategy(source_id, "EMA Cross Copy 2")
        created_ids.append(second_id)
        assert database.get_next_strategy_clone_name(source_id) == "EMA Cross Copy 3"
        created_ids.append(database.clone_strategy(source_id, "EMA Cross Copy 3"))

        database.delete_custom_strategy(second_id)

        assert database.get_next_strategy_clone_name(source_id) == "EMA Cross Copy 4"
    finally:
        with sqlite3.connect("data.db") as conn:
            for strategy_id in created_ids + [source_id]:
                conn.execute("DELETE FROM Strategies WHERE Id = ?", (strategy_id,))
            conn.commit()


def test_invalid_operator_is_rejected():
    definition = get_builtin_template("market_phases")
    definition["entry"]["conditions"][0]["operator"] = "exec_python"

    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)


def test_ai_prompt_classifier_blocks_off_topic_requests():
    weather = ai_builder.classify_strategy_prompt("podes falar sobre se amanha vai chover no Porto?")
    geography = ai_builder.classify_strategy_prompt("what is the capital of France?")

    assert weather["action"] == "block"
    assert weather["allowed"] is False
    assert weather["language"] == "pt-PT"
    assert "estratégias de trading" in weather["message"]
    assert geography["action"] == "block"
    assert geography["language"] == "en"
    assert "trading strategies" in geography["message"]


def test_ai_prompt_classifier_defers_contextual_requests():
    yes = ai_builder.classify_strategy_prompt("sim")
    greeting = ai_builder.classify_strategy_prompt("olá")

    assert yes["action"] == "defer"
    assert yes["allowed"] is True
    assert greeting["action"] == "defer"
    assert greeting["allowed"] is True


def test_ai_prompt_classifier_allows_strategy_requests():
    stop_loss = ai_builder.classify_strategy_prompt("adiciona stop loss de 10%")
    rsi_strategy = ai_builder.classify_strategy_prompt("cria estratégia RSI oversold com TP 8%")
    english_strategy = ai_builder.classify_strategy_prompt("Create an EMA crossover strategy with a 7% stop loss.")

    assert stop_loss["action"] == "allow"
    assert stop_loss["allowed"] is True
    assert rsi_strategy["action"] == "allow"
    assert rsi_strategy["allowed"] is True
    assert english_strategy["action"] == "allow"
    assert english_strategy["allowed"] is True
    assert english_strategy["language"] == "en"


class _FakeOpenAIResponse:
    def __init__(self, output_text):
        self._output_text = output_text

    def raise_for_status(self):
        return None

    def json(self):
        return {"output_text": self._output_text}


def test_ai_builder_explain_only_does_not_require_definition(monkeypatch):
    monkeypatch.setattr(ai_builder, "validate_openai_configuration", lambda model=None: ("test-key", "test-model"))
    response = {
        "response_type": "explain_only",
        "assistant_message": "A estratégia usa EMA 20 e EMA 40.",
        "strategy_name": "",
        "definition_json": "",
    }
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: _FakeOpenAIResponse(json.dumps(response)))

    result = ai_builder.build_strategy_with_openai(
        "sim",
        current_definition=get_builtin_template("ema_cross"),
        chat_history=[
            {"role": "user", "content": "quais os indicadores usados na estratégia?"},
            {"role": "assistant", "content": "Queres que mostre a definição completa?"},
            {"role": "user", "content": "sim"},
        ],
    )

    assert result["response_type"] == "explain_only"
    assert result["definition"] == {}
    assert "EMA 20" in result["assistant_message"]


def test_ai_builder_update_strategy_validates_definition_and_sends_history(monkeypatch):
    captured_payload = {}
    definition = get_builtin_template("ema_cross")
    response = {
        "response_type": "update_strategy",
        "assistant_message": "Atualizei a estratégia.",
        "strategy_name": "EMA Cross",
        "definition_json": json.dumps(definition),
    }

    def fake_post(*args, **kwargs):
        captured_payload.update(kwargs.get("json", {}))
        return _FakeOpenAIResponse(json.dumps(response))

    monkeypatch.setattr(ai_builder, "validate_openai_configuration", lambda model=None: ("test-key", "test-model"))
    monkeypatch.setattr(requests, "post", fake_post)

    result = ai_builder.build_strategy_with_openai(
        "adiciona stop loss de 10%",
        current_definition=definition,
        chat_history=[{"role": "user", "content": f"message {idx}"} for idx in range(12)],
    )

    payload_content = captured_payload["input"][1]["content"]
    assert result["response_type"] == "update_strategy"
    assert result["definition"]["engine"] == "bec_strategy_ast_v2"
    assert "recent_chat_history" in payload_content
    assert "message 0" not in payload_content
    assert "message 2" in payload_content
    assert "message 11" in payload_content


def test_v1_definition_is_rejected():
    definition = {
        "schema_version": 1,
        "engine": "bec_declarative_v1",
        "side": "long",
        "entry_rules": {"all": []},
        "exit_rules": {"any": []},
    }

    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)


def _price(field="Close", timeframe="4h"):
    return {"type": "price", "field": field, "timeframe": timeframe}


def _indicator(name, params=None, output="value", timeframe="4h", source=None):
    return {
        "type": "indicator",
        "name": name,
        "timeframe": timeframe,
        "source": source or {"type": "price", "field": "Close"},
        "params": params or {},
        "output": output,
    }


def _comparison(left, operator, right):
    return {"type": "comparison", "left": left, "operator": operator, "right": right}


def _ast_definition(entry_conditions, exit_conditions=None, risk_rules=None):
    return {
        "schema_version": 2,
        "engine": "bec_strategy_ast_v2",
        "name": "AST Test Strategy",
        "description": "Test AST strategy",
        "constraints": {
            "market": "spot",
            "side": "long",
            "order_type": "market",
            "allowed_actions": ["buy", "sell"],
            "allowed_timeframes": ["15m", "1h", "4h", "1d"],
        },
        "parameters": {},
        "entry": {
            "logic": "all",
            "conditions": entry_conditions,
            "action": {"type": "buy", "order_type": "market", "size_pct": 100},
        },
        "exit": {
            "logic": "any",
            "conditions": exit_conditions or [_comparison(_price(), "below", _indicator("EMA", {"period": 3}))],
            "action": {"type": "sell", "order_type": "market", "size_pct": 100},
        },
        "risk": {"rules": risk_rules or []},
        "metadata": {"builder": "bec_strategy_builder", "source": "test"},
    }


def _ohlcv(values):
    return pd.DataFrame(
        {
            "Open": values,
            "High": [value + 1 for value in values],
            "Low": [value - 1 for value in values],
            "Close": values,
            "Volume": [100 + idx for idx, _ in enumerate(values)],
        }
    )


def test_ast_v2_validates_allowed_registry_elements():
    definition = _ast_definition(
        [
            _comparison(_indicator("MACD", {"fast": 3, "slow": 6, "signal": 3}, "histogram", "1h"), "greater_than", {"type": "value", "value": 0}),
            _comparison(_price("Close", "1d"), "above", _indicator("BB", {"period": 5, "stddev": 2}, "middle", "1d")),
            _comparison(_indicator("STOCH", {"k": 5, "d": 3, "smooth": 1}, "k"), "between", [{"type": "value", "value": 20}, {"type": "value", "value": 80}]),
        ],
        risk_rules=[
            {"type": "stop_loss_pct", "pct": 7},
            {"type": "take_profit_pct", "pct": 10, "size_pct": 50},
            {"type": "atr_stop", "indicator": _indicator("ATR", {"period": 14}), "multiplier": 1.5},
        ],
    )

    validated = schema.validate_definition(definition)

    assert validated["schema_version"] == 2
    assert validated["engine"] == "bec_strategy_ast_v2"


def test_ast_v2_accepts_current_timeframe_operands():
    definition = _ast_definition(
        [
            _comparison(
                _price("Close", "current"),
                "above",
                _indicator("EMA", {"period": 3}, timeframe="current", source={"type": "price", "field": "Close", "timeframe": "current"}),
            )
        ]
    )

    assert schema.validate_definition(definition)["entry"]["conditions"][0]["left"]["timeframe"] == "current"
    assert strategy_builder._timeframe_label("current") == "selected timeframe"
    assert "on selected timeframe" in strategy_builder._rule_summary(
        definition["entry"]["conditions"][0],
        definition["parameters"],
    )


def test_optimization_parameters_round_trip_dataframe():
    parameters = {
        "ema_fast": {"type": "int", "default": 20, "min": 2, "max": 100, "step": 1, "optimizable": True},
        "rsi_min": {"type": "float", "default": 52.5, "min": 1.0, "max": 99.0, "step": 0.5, "optimizable": False},
        "use_daily_confirmation": {"type": "bool", "default": True, "optimizable": True},
    }

    df = strategy_builder._parameters_to_dataframe(parameters)
    parsed = strategy_builder._parameters_from_dataframe(df)

    assert parsed["ema_fast"] == parameters["ema_fast"]
    assert parsed["rsi_min"] == parameters["rsi_min"]
    assert parsed["use_daily_confirmation"] == parameters["use_daily_confirmation"]


def test_parameter_constraints_round_trip_dataframe():
    constraints = [
        {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
    ]
    parameter_names = ["ema_fast", "ema_slow", "rsi_min"]

    df = strategy_builder._parameter_constraints_to_dataframe(constraints)
    parsed = strategy_builder._parameter_constraints_from_dataframe(df, parameter_names)

    assert parsed == constraints


def test_validate_definition_parameter_constraints():
    definition = get_builtin_template("ema_cross")
    validated = schema.validate_definition(definition)

    assert validated["parameter_constraints"] == [
        {"left": "ema_fast", "operator": "less_than", "right": "ema_slow"},
    ]


def test_validate_definition_rejects_invalid_parameter_constraint():
    definition = get_builtin_template("ema_cross")
    definition["parameter_constraints"] = [
        {"left": "ema_fast", "operator": "less_than", "right": "missing_param"},
    ]

    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)


def test_definition_has_optimizable_parameters():
    assert strategy_builder._definition_has_optimizable_parameters(
        {"parameters": {"ema_fast": {"type": "int", "default": 20, "optimizable": True}}}
    )
    assert not strategy_builder._definition_has_optimizable_parameters(
        {"parameters": {"ema_fast": {"type": "int", "default": 20, "optimizable": False}}}
    )


def test_definition_description_reads_definition_json():
    definition = get_builtin_template("wema20")

    assert strategy_builder._definition_description(json.dumps(definition)) == definition["description"]
    assert strategy_builder._definition_description("{broken") == ""


def test_strategy_execution_requires_entry_and_exit_conditions():
    definition = get_empty_strategy_template("Incomplete")

    assert not strategy_builder._strategy_has_entry_exit_conditions(definition)

    definition = strategy_builder._add_default_condition(definition, "entry")
    assert not strategy_builder._strategy_has_entry_exit_conditions(definition)

    definition = strategy_builder._add_default_condition(definition, "exit")
    assert strategy_builder._strategy_has_entry_exit_conditions(definition)


def test_add_default_condition_produces_valid_editable_condition():
    definition = get_empty_strategy_template("Diagram Draft")

    definition = strategy_builder._add_default_condition(definition, "entry", "any")
    definition = strategy_builder._add_default_condition(definition, "exit", "all")
    validated = schema.validate_definition(definition)

    assert validated["entry"]["logic"] == "any"
    assert validated["exit"]["logic"] == "all"
    assert validated["entry"]["conditions"][0]["operator"] == "less_than"
    assert validated["exit"]["conditions"][0]["right"]["value"] == 0.0


def test_set_group_logic_updates_ast_group_without_changing_conditions():
    definition = get_builtin_template("market_phases")

    edited = strategy_builder._set_group_logic(definition, "entry", "any")
    validated = schema.validate_definition(edited)

    assert validated["entry"]["logic"] == "any"
    assert validated["entry"]["conditions"] == definition["entry"]["conditions"]


def test_set_group_operator_updates_only_one_condition_separator():
    definition = get_builtin_template("market_phases")
    definition = strategy_builder._add_default_condition(definition, "entry", "all")

    edited = strategy_builder._set_group_operator(definition, "entry", 1, "any")
    validated = schema.validate_definition(edited)

    assert validated["entry"]["operators"] == ["all", "any"]
    assert len(validated["entry"]["conditions"]) == 3


def test_engine_evaluates_mixed_group_operators_left_to_right():
    definition = _ast_definition(
        [
            _comparison(_price(), "above", {"type": "value", "value": 100}),
            _comparison(_price(), "above", {"type": "value", "value": 100}),
            _comparison(_price(), "above", {"type": "value", "value": 10}),
        ]
    )
    definition["entry"]["logic"] = "all"
    definition["entry"]["operators"] = ["all", "any"]

    assert schema.validate_definition(definition)["entry"]["operators"] == ["all", "any"]
    assert engine.evaluate_entry(_ohlcv([20]), definition, {}) is True


def test_insert_condition_uses_operator_before_new_condition():
    definition = get_builtin_template("market_phases")
    condition = _comparison(_price(), "below", {"type": "value", "value": 100000})

    edited = strategy_builder._insert_condition(definition, "entry", condition, "any")
    validated = schema.validate_definition(edited)

    assert len(validated["entry"]["conditions"]) == 3
    assert validated["entry"]["operators"] == ["all", "any"]


def test_update_condition_preserves_group_operators():
    definition = get_builtin_template("market_phases")
    definition = strategy_builder._insert_condition(
        definition,
        "entry",
        _comparison(_price(), "below", {"type": "value", "value": 100000}),
        "any",
    )
    updated_condition = _comparison(_price(), "below", {"type": "value", "value": 90000})

    edited = strategy_builder._update_condition(definition, "entry", 2, updated_condition)

    assert edited["entry"]["operators"] == ["all", "any"]
    assert edited["entry"]["conditions"][2]["right"]["value"] == 90000


def test_remove_condition_keeps_operators_consistent():
    definition = get_builtin_template("market_phases")
    definition = strategy_builder._insert_condition(
        definition,
        "entry",
        _comparison(_price(), "below", {"type": "value", "value": 100000}),
        "any",
    )

    edited = strategy_builder._remove_condition(definition, "entry", 1)

    assert len(edited["entry"]["conditions"]) == 2
    assert edited["entry"]["operators"] == ["all"]


def test_risk_rule_helpers_add_update_and_remove_rules():
    definition = get_empty_strategy_template("Risk Dialog")

    edited = strategy_builder._upsert_risk_rule(definition, strategy_builder._default_risk_rule("stop_loss_pct"))
    edited = strategy_builder._upsert_risk_rule(edited, strategy_builder._default_risk_rule("take_profit_pct"))
    updated_stop = {"type": "stop_loss_pct", "pct": 7.0}
    edited = strategy_builder._upsert_risk_rule(edited, updated_stop, 0)

    assert [rule["type"] for rule in edited["risk"]["rules"]] == ["stop_loss_pct", "take_profit_pct"]
    assert edited["risk"]["rules"][0]["pct"] == 7.0

    edited = strategy_builder._remove_risk_rule(edited, 1)

    assert [rule["type"] for rule in edited["risk"]["rules"]] == ["stop_loss_pct"]


def test_strategy_builder_risk_labels_come_from_definition_risk_rules_only():
    risk = {
        "rules": [
            {"type": "stop_loss_pct", "pct": 10},
            {"type": "take_profit_pct", "pct": 8, "size_pct": 50},
            {"type": "atr_stop", "indicator": _indicator("ATR", {"period": 14}), "multiplier": 1.8},
        ],
        "stop_loss_pct": 99,
        "take_profits": [{"level": 1, "pnl_pct": 99, "amount_pct": 99}],
    }

    risk_labels, take_profit_labels = strategy_builder._active_risk_and_take_profit_labels(risk)

    assert [label for label, _, _ in risk_labels] == ["SL\n10%", "ATR Stop\nP14 x1.8 Act0%"]
    assert [label for label, _, _ in take_profit_labels] == ["TP\n8% / 50%"]


def test_strategy_builder_empty_risk_rules_do_not_use_legacy_risk_fields():
    risk = {"rules": [], "stop_loss_pct": 10, "take_profits": [{"level": 1, "pnl_pct": 8, "amount_pct": 50}]}

    risk_labels, take_profit_labels = strategy_builder._active_risk_and_take_profit_labels(risk)

    assert risk_labels == [("No risk rules\nconfigured", {"kind": "risk_empty"}, "No risk rules configured")]
    assert take_profit_labels == []


def test_strategy_builder_risk_block_index_preserves_zero():
    assert strategy_builder._block_index({"index": 0}) == 0
    assert strategy_builder._block_index({"index": "0"}) == 0
    assert strategy_builder._block_index({"index": None}) == -1


def test_strategy_builder_duplicate_stop_loss_labels_keep_distinct_indexes():
    labels = strategy_builder._risk_block_labels(
        {"rules": [{"type": "stop_loss_pct", "pct": 10}, {"type": "stop_loss_pct", "pct": 10}]}
    )

    assert [block for _, block, _ in labels] == [
        {"kind": "ast_risk", "index": 0},
        {"kind": "ast_risk", "index": 1},
    ]


def test_strategy_builder_default_risk_rules_validate_in_definition():
    definition = _ast_definition(
        [_comparison(_price(), "above", _indicator("EMA", {"period": 3}))],
        risk_rules=[
            strategy_builder._default_risk_rule("stop_loss_pct"),
            strategy_builder._default_risk_rule("atr_stop"),
            strategy_builder._default_risk_rule("take_profit_pct"),
        ],
    )

    validated = schema.validate_definition(definition)

    assert [rule["type"] for rule in validated["risk"]["rules"]] == ["stop_loss_pct", "atr_stop", "take_profit_pct"]


def test_ast_v2_engine_evaluates_entry_and_exit():
    definition = _ast_definition(
        [
            _comparison(_price(), "above", _indicator("SMA", {"period": 2})),
            {
                "type": "window_condition",
                "operator": "for_n_bars",
                "bars": 2,
                "condition": _comparison(_price(), "greater_than_or_equal", {"type": "value", "value": 11}),
            },
        ],
        [_comparison(_price(), "below", _indicator("SMA", {"period": 2}))],
    )
    df = _ohlcv([8, 10, 12, 14, 13, 9])

    prepared = engine.add_indicators(df, definition)

    assert engine.evaluate_entry(prepared.iloc[:4], definition)
    assert engine.evaluate_exit(prepared, definition)


def test_ast_v2_rejects_instrument_short_and_limit_order():
    definition = _ast_definition([_comparison(_price(), "above", _indicator("EMA", {"period": 3}))])
    definition["instrument"] = "BTCUSDT"
    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)

    definition = _ast_definition([_comparison(_price(), "above", _indicator("EMA", {"period": 3}))])
    definition["constraints"]["side"] = "short"
    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)

    definition = _ast_definition([_comparison(_price(), "above", _indicator("EMA", {"period": 3}))])
    definition["entry"]["action"]["order_type"] = "limit"
    with pytest.raises(schema.StrategyValidationError):
        schema.validate_definition(definition)


def test_ast_v2_risk_rules_extract_execution_risk_controls():
    definition = _ast_definition(
        [_comparison(_price(), "above", _indicator("EMA", {"period": 3}))],
        risk_rules=[
            {"type": "stop_loss_pct", "pct": 5},
            {"type": "take_profit_pct", "pct": 8, "size_pct": 50},
            {"type": "atr_stop", "indicator": _indicator("ATR", {"period": 14}), "multiplier": 1.5},
        ],
    )

    risk = schema.extract_execution_risk(definition)

    assert risk["stop_loss_pct"] == 5
    assert risk["take_profits"][0] == {"level": 1, "pnl_pct": 8.0, "amount_pct": 50.0}
    assert risk["atr_trailing"]["enabled"] is True
    assert risk["atr_trailing"]["multiplier"] == 1.5


def test_ast_v2_extracts_more_than_four_take_profit_levels():
    definition = _ast_definition(
        [_comparison(_price(), "above", _indicator("EMA", {"period": 3}))],
        risk_rules=[
            {"type": "take_profit_pct", "pct": 5, "size_pct": 10},
            {"type": "take_profit_pct", "pct": 10, "size_pct": 20},
            {"type": "take_profit_pct", "pct": 15, "size_pct": 30},
            {"type": "take_profit_pct", "pct": 20, "size_pct": 40},
            {"type": "take_profit_pct", "pct": 25, "size_pct": 50},
            {"type": "take_profit_pct", "pct": 30, "size_pct": 60},
        ],
    )

    risk = schema.extract_execution_risk(definition)

    assert len(risk["take_profits"]) == 6
    assert risk["take_profits"][0] == {"level": 1, "pnl_pct": 5.0, "amount_pct": 10.0}
    assert risk["take_profits"][4] == {"level": 5, "pnl_pct": 25.0, "amount_pct": 50.0}
    assert risk["take_profits"][5] == {"level": 6, "pnl_pct": 30.0, "amount_pct": 60.0}


def test_ast_v2_normalizes_risk_percent_strings_from_ai_builder():
    definition = _ast_definition(
        [_comparison(_price(), "above", _indicator("EMA", {"period": 3}))],
        risk_rules=[
            {"type": "stop_loss_pct", "pct": "10%"},
            {"type": "take_profit_pct", "pct": "8", "size_pct": "50%"},
        ],
    )

    validated = schema.validate_definition(definition)
    risk = schema.extract_execution_risk(validated)

    assert validated["risk"]["rules"][0]["pct"] == 10.0
    assert validated["risk"]["rules"][1]["pct"] == 8.0
    assert validated["risk"]["rules"][1]["size_pct"] == 50.0
    assert risk["stop_loss_pct"] == 10.0
    assert risk["take_profits"][0] == {"level": 1, "pnl_pct": 8.0, "amount_pct": 50.0}


def test_ast_v2_current_and_higher_timeframe_evaluate_with_loader_without_lookahead():
    definition = _ast_definition(
        [
            _comparison(_price("Close", "current"), "greater_than", {"type": "value", "value": 0}),
            _comparison(_price("Close", "1d"), "above", {"type": "value", "value": 100}),
        ]
    )
    base = _ohlcv([10, 11, 12, 13, 14])
    base.index = pd.to_datetime(
        ["2024-01-01 00:00", "2024-01-01 04:00", "2024-01-01 08:00", "2024-01-01 12:00", "2024-01-02 00:00"]
    )
    daily = _ohlcv([90, 120, 80])
    daily.index = pd.to_datetime(["2023-12-31", "2024-01-01", "2024-01-02"])

    def loader(symbol, timeframe):
        assert symbol == "BTCUSDT"
        assert timeframe == "1d"
        return daily

    prepared = engine.add_indicators(
        base,
        definition,
        symbol="BTCUSDT",
        base_timeframe="4h",
        data_loader=loader,
    )

    assert engine.evaluate_entry(prepared.iloc[:4], definition, base_timeframe="4h") is False
    assert engine.evaluate_entry(prepared, definition, base_timeframe="4h") is True


def test_ast_v2_rejects_fixed_lower_timeframe_for_base_timeframe():
    definition = _ast_definition(
        [_comparison(_price("Close", "15m"), "above", {"type": "value", "value": 0})]
    )

    assert schema.validate_definition(definition)["schema_version"] == 2
    with pytest.raises(schema.StrategyValidationError):
        engine.add_indicators(_ohlcv([8, 10, 12, 14]), definition, base_timeframe="1h")
