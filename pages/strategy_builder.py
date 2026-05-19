import os
import copy
import html
import json
import subprocess
import sys
import time
from urllib.parse import quote, unquote

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import bec.my_backtesting as my_backtesting
import bec.utils.database as database
import bec.utils.icons as icons
from bec.page_config import configure_page
from bec.strategy_builder import ai_builder
from bec.strategy_builder import packages as strategy_packages
from bec.strategy_builder import schema as strategy_schema
from bec.strategy_builder import templates as strategy_templates
from bec.utils import ai_strategy_analysis
from bec.utils.take_profit import normalize_take_profit_levels


configure_page()

INDICATOR_OPTIONS = ["EMA", "SMA", "HMA", "RSI", "LINREG"]
PRICE_FIELD_OPTIONS = ["Open", "High", "Low", "Close", "Volume"]
TIMEFRAME_OPTIONS = ["current", "15m", "1h", "4h", "1d"]
PARAMETER_TYPES = ["int", "float", "bool"]
PARAMETER_CONSTRAINT_OPERATOR_OPTIONS = [
    "less_than",
    "less_than_or_equal",
    "greater_than",
    "greater_than_or_equal",
]
PARAMETER_CONSTRAINT_OPERATOR_LABELS = {
    "less_than": "<",
    "less_than_or_equal": "<=",
    "greater_than": ">",
    "greater_than_or_equal": ">=",
}
CURRENT_TIMEFRAME_UI_LABEL = "Current TF"
OPERATORS = {
    "crosses_above": "crosses above",
    "crosses_below": "crosses below",
    "greater_than": "above",
    "less_than": "below",
}
OPERAND_TYPES = {
    "indicator": "Indicator",
    "price": "Price",
    "value": "Value",
    "parameter": "Parameter",
}


def _timeframe_label(timeframe: str) -> str:
    return "selected timeframe" if str(timeframe or "current") == "current" else str(timeframe)


def _timeframe_option_label(timeframe: str) -> str:
    return CURRENT_TIMEFRAME_UI_LABEL if str(timeframe or "current") == "current" else str(timeframe)


def _definition_has_optimizable_parameters(definition: dict) -> bool:
    parameters = definition.get("parameters", {}) if isinstance(definition, dict) else {}
    if not isinstance(parameters, dict):
        return False
    return any(bool(spec.get("optimizable", False)) for spec in parameters.values() if isinstance(spec, dict))


def _indicator_period_selector(
    host,
    operand: dict,
    parameters: dict,
    key_prefix: str,
    *,
    selector_width: int = 125,
    value_width: int = 95,
) -> dict:
    parameter_names = list(parameters.keys())
    params = operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
    current_period_param = str(operand.get("period_param", "") or "")
    current_fixed_period = int(operand.get("period", params.get("period", 14)) or 14)
    period_source_options = ["__fixed__"] + parameter_names
    current_source = current_period_param if current_period_param in parameter_names else "__fixed__"
    selected_source = host.selectbox(
        "Period",
        period_source_options,
        index=period_source_options.index(current_source),
        format_func=lambda value: "Fixed" if value == "__fixed__" else value,
        key=f"{key_prefix}_period_source",
        label_visibility="collapsed",
        width=selector_width,
    )
    if selected_source != "__fixed__":
        return {"period_param": selected_source}
    return {
        "period": int(
            host.number_input(
                "Period value",
                min_value=1,
                value=current_fixed_period,
                step=1,
                key=f"{key_prefix}_period_value",
                label_visibility="collapsed",
                width=value_width,
            )
        )
    }
AI_CHAT_WELCOME_MESSAGE = (
    "Tell me how entry, trade, exit, and risk management should work."
)
AI_CHAT_WELCOME_MESSAGES = {
    AI_CHAT_WELCOME_MESSAGE,
    "Diz-me como queres que funcionem a entrada, trade, saída e gestão de risco.",
}
AI_STRATEGY_EXAMPLES = {
    "Golden Cross / Death Cross": (
        "Create a Golden Cross / Death Cross strategy using SMA 50 and SMA 200. "
        "Enter long when SMA 50 crosses above SMA 200, exit when SMA 50 crosses below SMA 200. "
        "Use a 10% hard stop loss."
    ),
    "RSI Oversold Bounce": (
        "Create an RSI oversold bounce strategy. Enter when RSI 14 crosses above 30 and price is above SMA 200. "
        "Exit when RSI 14 goes above 70 or price closes below SMA 200. Use a 7% stop loss and take profit at 8% for 50%."
    ),
    "HMA RSI LINREG": (
        "Create an HMA crossover strategy using HMA 16 and HMA 65. Enter when HMA 16 crosses above HMA 65, "
        "RSI 14 is above 52, and close is above LINREG 50. Exit when HMA 16 crosses below HMA 65. "
        "Add take profits at 5%, 10%, 15%, and 20%."
    ),
}

st.markdown(
    """
    <style>
    .bec-block-section {
        margin-top: 1.1rem;
        padding-left: 0.85rem;
        border-left: 1px solid rgba(49, 51, 63, 0.18);
    }
    .bec-block-section-title {
        color: #ff4b4b;
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.12rem;
        text-transform: uppercase;
        margin: 0.25rem 0 0.7rem 0;
    }
    .bec-rule-title {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
    }
    .bec-dot-entry {
        width: 0.62rem;
        height: 0.62rem;
        border-radius: 50%;
        background: #4da3ff;
        display: inline-block;
    }
    .bec-dot-exit {
        width: 0.62rem;
        height: 0.62rem;
        border-radius: 50%;
        background: #ffc533;
        display: inline-block;
    }
    .bec-rule-summary {
        color: rgba(49, 51, 63, 0.62);
        font-family: monospace;
        font-size: 0.78rem;
        font-weight: 600;
        margin-left: 0.35rem;
    }
    .bec-joiner {
        display: block;
        width: fit-content;
        margin: 0.35rem auto 0.55rem auto;
        padding: 0.12rem 0.55rem;
        border-radius: 0.35rem;
        border: 1px solid rgba(255, 75, 75, 0.25);
        color: #ff4b4b;
        background: rgba(255, 75, 75, 0.08);
        font-size: 0.72rem;
        font-weight: 800;
        text-transform: uppercase;
    }
    .bec-diagram-group {
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.08rem;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }
    .bec-diagram-hint {
        color: rgba(49, 51, 63, 0.62);
        font-size: 0.82rem;
        margin: 0.25rem 0 0.75rem 0;
    }
    div[data-testid="stButton"] button {
        height: auto;
        min-height: 2.5rem;
    }
    div[data-testid="stButton"] button p {
        white-space: pre-line;
        text-align: center;
        width: 100%;
    }
    .bec-condition-card {
        display: block;
        width: 100%;
        min-height: 2.5rem;
        box-sizing: border-box;
        padding: 0.48rem 0.6rem;
        border: 1px solid rgba(49, 51, 63, 0.2);
        border-radius: 0.45rem;
        color: rgb(49, 51, 63);
        text-align: center;
        text-decoration: none;
        background: #ffffff;
        line-height: 1.85;
    }
    .bec-condition-card:hover {
        border-color: #ff4b4b;
        text-decoration: none;
    }
    .bec-condition-group-line {
        margin: 0.12rem 0;
    }
    .bec-chip {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 1.35rem;
        padding: 0.05rem 0.42rem;
        border-radius: 0.35rem;
        border: 1px solid transparent;
        font-size: 0.78rem;
        font-weight: 800;
        line-height: 1.15;
        white-space: nowrap;
    }
    .bec-chip-price { color: #0369a1; background: #e0f2fe; border-color: #bae6fd; }
    .bec-chip-indicator { color: #047857; background: #d1fae5; border-color: #a7f3d0; }
    .bec-chip-period { color: #0f766e; background: #ccfbf1; border-color: #99f6e4; }
    .bec-chip-timeframe { color: #7e22ce; background: #f3e8ff; border-color: #e9d5ff; }
    .bec-chip-operator { color: #1d4ed8; background: #dbeafe; border-color: #bfdbfe; }
    .bec-chip-value { color: #475569; background: #f1f5f9; border-color: #e2e8f0; }
    .bec-chip-parameter { color: #a16207; background: #fef3c7; border-color: #fde68a; }
    .bec-chip-logic { color: #be185d; background: rgba(244, 114, 182, 0.13); border-color: rgba(190, 24, 93, 0.35); }
    .bec-token-text {
        color: rgba(49, 51, 63, 0.58);
        font-family: monospace;
        font-size: 0.78rem;
        font-weight: 700;
        margin: 0 0.18rem;
    }
    div[data-testid="stDialog"] div[role="dialog"] {
        width: min(900px, calc(100vw - 2rem));
        max-width: min(900px, calc(100vw - 2rem));
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _json_text(value) -> str:
    try:
        parsed = strategy_schema.parse_json_object(value, "JSON")
        return json.dumps(parsed, ensure_ascii=True, sort_keys=True, indent=2)
    except Exception:
        return str(value or "{}")


def _strategy_badge(row) -> str:
    return f"{row.get('Name', row.get('Id'))} ({row.get('Type', 'builtin')} / {row.get('Status', 'approved')})"


def _block_index(block: dict, default: int = -1) -> int:
    value = block.get("index", default)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _compact_operand_label(operand: dict, parameters: dict) -> str:
    operand_type = operand.get("type")
    if operand_type == "indicator":
        name = str(operand.get("name", "Indicator")).upper()
        params = operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
        period = operand.get("period", params.get("period"))
        if operand.get("period_param"):
            period = f"<{operand.get('period_param')}>"
        output = operand.get("output", "value")
        label = f"{name} {period}" if period not in (None, "") else name
        return f"{label}.{output}" if output not in ("", "value", None) else label
    if operand_type == "price":
        return f"Price {operand.get('field', 'Close')}"
    if operand_type == "value":
        return str(operand.get("value", "0"))
    if operand_type == "parameter":
        spec = parameters.get(str(operand.get("name")), {})
        if isinstance(spec, dict):
            return str(spec.get("default", operand.get("name")))
        return str(operand.get("name", "parameter"))
    return "?"


def _condition_timeframes(rule: dict) -> list[str]:
    result = []

    def walk(value):
        if isinstance(value, dict):
            value_type = value.get("type")
            if value_type in {"price", "indicator"}:
                timeframe = str(value.get("timeframe", "current") or "current")
                if timeframe not in result:
                    result.append(timeframe)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(rule)
    return result


def _append_timeframe_summary(summary: str, rule: dict) -> str:
    timeframes = _condition_timeframes(rule)
    if not timeframes:
        return f"{summary} on selected timeframe"
    labels = [_timeframe_label(timeframe) for timeframe in timeframes]
    return f"{summary} on {' + '.join(labels)}"


def _rule_summary(rule: dict, parameters: dict) -> str:
    if rule.get("type") == "group":
        logic = str(rule.get("logic", "all") or "all").lower()
        conditions = rule.get("conditions", []) if isinstance(rule.get("conditions"), list) else []
        summaries = [_rule_summary(condition, parameters) for condition in conditions if isinstance(condition, dict)]
        if not summaries:
            return logic.upper()
        operators = _group_operators(rule, logic, len(conditions))
        result = summaries[0]
        for operator, summary in zip(operators, summaries[1:]):
            result = f"{result}\n{'AND' if operator == 'all' else 'OR'}\n{summary}"
        return result
    if rule.get("type") == "window_condition":
        nested = _rule_summary(rule.get("condition", {}), parameters)
        return f"{rule.get('operator', 'window')} {rule.get('bars', 1)} bars: {nested}"
    left = _compact_operand_label(rule.get("left", {}), parameters)
    operator = OPERATORS.get(rule.get("operator"), rule.get("operator", "?"))
    if rule.get("operator") == "between":
        lower = rule.get("lower")
        upper = rule.get("upper")
        right = rule.get("right", [])
        if (lower is None or upper is None) and isinstance(right, list) and len(right) == 2:
            lower, upper = right
        summary = f"{left} between {_compact_operand_label(lower or {}, parameters)} and {_compact_operand_label(upper or {}, parameters)}"
        return _append_timeframe_summary(summary, rule)
    right = _compact_operand_label(rule.get("right", {}), parameters)
    summary = f"{left} {operator} {right}"
    return _append_timeframe_summary(summary, rule)


def _chip_html(text: str, css_class: str) -> str:
    return f'<span class="bec-chip {css_class}">{html.escape(str(text))}</span>'


def _token_html(text: str) -> str:
    return f'<span class="bec-token-text">{html.escape(str(text))}</span>'


def _operand_chips_html(operand: dict, parameters: dict) -> str:
    operand_type = operand.get("type")
    if operand_type == "indicator":
        name = str(operand.get("name", "Indicator")).upper()
        params = operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
        period = operand.get("period", params.get("period"))
        if operand.get("period_param"):
            period = f"<{operand.get('period_param')}>"
        timeframe = _timeframe_label(str(operand.get("timeframe", "current") or "current"))
        parts = [_chip_html(name, "bec-chip-indicator")]
        if period not in (None, ""):
            parts.append(_chip_html(period, "bec-chip-period"))
        parts.append(_token_html("on"))
        parts.append(_chip_html(timeframe, "bec-chip-timeframe"))
        return " ".join(parts)
    if operand_type == "price":
        timeframe = _timeframe_label(str(operand.get("timeframe", "current") or "current"))
        return " ".join(
            [
                _chip_html(f"Price {operand.get('field', 'Close')}", "bec-chip-price"),
                _token_html("on"),
                _chip_html(timeframe, "bec-chip-timeframe"),
            ]
        )
    if operand_type == "value":
        return _chip_html(str(operand.get("value", "0")), "bec-chip-value")
    if operand_type == "parameter":
        name = str(operand.get("name", "parameter"))
        return _chip_html(name, "bec-chip-parameter")
    return _chip_html("?", "bec-chip-value")


def _rule_chips_html(rule: dict, parameters: dict) -> str:
    if isinstance(rule, dict) and rule.get("type") == "group":
        joiner, rules = _group_key_and_rules(rule)
        operators = _group_operators(rule, joiner, len(rules))
        parts = []
        for idx, nested_rule in enumerate(rules):
            if idx > 0:
                operator = operators[idx - 1] if idx - 1 < len(operators) else joiner
                joiner_label = "AND" if operator == "all" else "OR"
                parts.append(f'<div class="bec-condition-group-line">{_chip_html(joiner_label, "bec-chip-logic")}</div>')
            parts.append(f'<div class="bec-condition-group-line">{_rule_chips_html(nested_rule, parameters)}</div>')
        return "".join(parts)
    if not isinstance(rule, dict):
        return _chip_html("?", "bec-chip-value")
    if rule.get("type") == "window_condition":
        return " ".join(
            [
                _chip_html(str(rule.get("operator", "window")), "bec-chip-operator"),
                _chip_html(str(rule.get("bars", 1)), "bec-chip-value"),
                _token_html("bars"),
                _rule_chips_html(rule.get("condition", {}), parameters),
            ]
        )
    operator = OPERATORS.get(rule.get("operator"), rule.get("operator", "?"))
    if rule.get("operator") == "between":
        lower = rule.get("lower")
        upper = rule.get("upper")
        right = rule.get("right", [])
        if (lower is None or upper is None) and isinstance(right, list) and len(right) == 2:
            lower, upper = right
        return " ".join(
            [
                _operand_chips_html(rule.get("left", {}), parameters),
                _chip_html("between", "bec-chip-operator"),
                _operand_chips_html(lower or {}, parameters),
                _token_html("and"),
                _operand_chips_html(upper or {}, parameters),
            ]
        )
    return " ".join(
        [
            _operand_chips_html(rule.get("left", {}), parameters),
            _chip_html(operator, "bec-chip-operator"),
            _operand_chips_html(rule.get("right", {}), parameters),
        ]
    )


def _dot_escape(value) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _group_key_and_rules(group: dict) -> tuple[str, list]:
    if not isinstance(group, dict):
        return "all", []
    if "any" in group:
        return "any", group.get("any", []) if isinstance(group.get("any"), list) else []
    if "logic" in group:
        logic = group.get("logic") if group.get("logic") in {"all", "any"} else "all"
        return logic, group.get("conditions", []) if isinstance(group.get("conditions"), list) else []
    return "all", group.get("all", []) if isinstance(group.get("all"), list) else []


def _group_operators(group: dict, logic: str, rules_count: int) -> list[str]:
    operators = group.get("operators", []) if isinstance(group, dict) else []
    if isinstance(operators, list) and len(operators) == max(rules_count - 1, 0):
        return [operator if operator in {"all", "any"} else logic for operator in operators]
    return [logic for _ in range(max(rules_count - 1, 0))]


def _is_ast_definition(definition: dict) -> bool:
    try:
        return int(definition.get("schema_version", 1) or 1) == 2
    except (TypeError, ValueError):
        return False


def _diagram_groups(definition: dict) -> tuple[str, list, str, list, str, list]:
    entry_joiner, entry_rules = _group_key_and_rules(definition.get("entry", {"logic": "all", "conditions": []}))
    exit_joiner, exit_rules = _group_key_and_rules(definition.get("exit", {"logic": "any", "conditions": []}))
    return "all", [], entry_joiner, entry_rules, exit_joiner, exit_rules


def _default_strategy_condition() -> dict:
    return {
        "type": "comparison",
        "left": {"type": "price", "field": "Close", "timeframe": "current"},
        "operator": "less_than",
        "right": {"type": "value", "value": 0.0},
    }


def _add_default_condition(definition: dict, group_name: str, logic: str = "") -> dict:
    edited = copy.deepcopy(definition)
    fallback_logic = "any" if group_name == "exit" else "all"
    group = edited.get(group_name, {"logic": fallback_logic, "conditions": []})
    current_logic, rules = _group_key_and_rules(group)
    operators = _group_operators(group, current_logic, len(rules))
    selected_logic = logic if logic in {"all", "any"} else current_logic
    updated_rules = list(rules) + [_default_strategy_condition()]
    updated_operators = operators + ([selected_logic] if rules else [])
    if group_name in {"entry", "exit"}:
        edited[group_name] = {
            "logic": selected_logic if selected_logic in {"all", "any"} else fallback_logic,
            "conditions": updated_rules,
            "operators": updated_operators,
            "action": {
                "type": "sell" if group_name == "exit" else "buy",
                "order_type": "market",
                "size_pct": 100,
            },
        }
    else:
        edited[group_name] = {selected_logic: updated_rules, "operators": updated_operators}
    return strategy_schema.validate_definition(edited)


def _set_group_logic(definition: dict, group_name: str, logic: str) -> dict:
    edited = copy.deepcopy(definition)
    if logic not in {"all", "any"}:
        return strategy_schema.validate_definition(edited)
    fallback_logic = "any" if group_name == "exit" else "all"
    group = edited.get(group_name, {"logic": fallback_logic, "conditions": []})
    _, rules = _group_key_and_rules(group)
    operators = [logic for _ in range(max(len(rules) - 1, 0))]
    if group_name in {"entry", "exit"}:
        action = (
            group.get("action", {})
            if isinstance(group, dict) and isinstance(group.get("action"), dict)
            else {}
        )
        edited[group_name] = {
            "logic": logic,
            "conditions": list(rules),
            "operators": operators,
            "action": {
                "type": str(action.get("type") or ("sell" if group_name == "exit" else "buy")),
                "order_type": str(action.get("order_type") or "market"),
                "size_pct": float(action.get("size_pct", 100) or 100),
            },
        }
    else:
        edited[group_name] = {logic: list(rules), "operators": operators}
    return strategy_schema.validate_definition(edited)


def _set_group_operator(definition: dict, group_name: str, operator_index: int, logic: str) -> dict:
    edited = copy.deepcopy(definition)
    if logic not in {"all", "any"}:
        return strategy_schema.validate_definition(edited)
    fallback_logic = "any" if group_name == "exit" else "all"
    group = edited.get(group_name, {"logic": fallback_logic, "conditions": []})
    current_logic, rules = _group_key_and_rules(group)
    operators = _group_operators(group, current_logic, len(rules))
    if operator_index < 0 or operator_index >= len(operators):
        return strategy_schema.validate_definition(edited)
    operators[operator_index] = logic
    if group_name in {"entry", "exit"} and isinstance(group, dict):
        group["conditions"] = list(rules)
        group["operators"] = operators
        group["logic"] = operators[0] if operators else current_logic
        edited[group_name] = group
    else:
        edited[group_name] = {
            "logic": operators[0] if operators else current_logic,
            "conditions": list(rules),
            "operators": operators,
        }
    return strategy_schema.validate_definition(edited)


def _operators_after_rule_edit(group: dict, logic: str, old_rule_count: int, removed_index: int | None = None) -> list[str]:
    operators = _group_operators(group, logic, old_rule_count)
    if removed_index is None:
        return operators
    if not operators:
        return []
    remove_operator_index = min(max(int(removed_index), 0), len(operators) - 1)
    operators.pop(remove_operator_index)
    return operators


def _group_action(group_name: str, group: dict) -> dict:
    if isinstance(group, dict) and isinstance(group.get("action"), dict):
        action = group["action"]
    else:
        action = {}
    return {
        "type": str(action.get("type") or ("sell" if group_name == "exit" else "buy")),
        "order_type": str(action.get("order_type") or "market"),
        "size_pct": float(action.get("size_pct", 100) or 100),
    }


def _replace_ast_group(definition: dict, group_name: str, rules: list, operators: list[str], fallback_logic: str = "") -> dict:
    edited = copy.deepcopy(definition)
    fallback_logic = fallback_logic if fallback_logic in {"all", "any"} else ("any" if group_name == "exit" else "all")
    logic = operators[0] if operators else fallback_logic
    group = edited.get(group_name, {"logic": fallback_logic, "conditions": []})
    if group_name in {"entry", "exit"}:
        edited[group_name] = {
            "logic": logic,
            "conditions": list(rules),
            "operators": list(operators),
            "action": _group_action(group_name, group if isinstance(group, dict) else {}),
        }
    else:
        edited[group_name] = {
            "logic": logic,
            "conditions": list(rules),
            "operators": list(operators),
        }
    return strategy_schema.validate_definition(edited)


def _insert_condition(definition: dict, group_name: str, condition: dict, operator_before: str = "") -> dict:
    fallback_logic = "any" if group_name == "exit" else "all"
    group = definition.get(group_name, {"logic": fallback_logic, "conditions": []})
    current_logic, rules = _group_key_and_rules(group)
    operators = _group_operators(group, current_logic, len(rules))
    selected_operator = operator_before if operator_before in {"all", "any"} else current_logic
    updated_rules = list(rules) + [copy.deepcopy(condition)]
    updated_operators = operators + ([selected_operator] if rules else [])
    return _replace_ast_group(definition, group_name, updated_rules, updated_operators, selected_operator)


def _update_condition(definition: dict, group_name: str, index: int, condition: dict) -> dict:
    group = definition.get(group_name, {})
    current_logic, rules = _group_key_and_rules(group)
    if index < 0 or index >= len(rules):
        return strategy_schema.validate_definition(definition)
    operators = _group_operators(group, current_logic, len(rules))
    updated_rules = list(rules)
    updated_rules[index] = copy.deepcopy(condition)
    return _replace_ast_group(definition, group_name, updated_rules, operators, current_logic)


def _remove_condition(definition: dict, group_name: str, index: int) -> dict:
    group = definition.get(group_name, {})
    current_logic, rules = _group_key_and_rules(group)
    if index < 0 or index >= len(rules):
        return strategy_schema.validate_definition(definition)
    updated_rules = list(rules)
    updated_rules.pop(index)
    updated_operators = _operators_after_rule_edit(
        group,
        current_logic,
        len(rules),
        removed_index=index,
    )
    return _replace_ast_group(definition, group_name, updated_rules, updated_operators, current_logic)


def _upsert_risk_rule(definition: dict, rule: dict, index: int | None = None) -> dict:
    edited = copy.deepcopy(definition)
    risk = edited.get("risk", {}) if isinstance(edited.get("risk"), dict) else {}
    rules = risk.get("rules", []) if isinstance(risk.get("rules"), list) else []
    if index is None or index < 0 or index >= len(rules):
        rules.append(copy.deepcopy(rule))
    else:
        rules[index] = copy.deepcopy(rule)
    risk["rules"] = rules
    edited["risk"] = risk
    return strategy_schema.validate_definition(edited)


def _remove_risk_rule(definition: dict, index: int) -> dict:
    edited = copy.deepcopy(definition)
    risk = edited.get("risk", {}) if isinstance(edited.get("risk"), dict) else {}
    rules = risk.get("rules", []) if isinstance(risk.get("rules"), list) else []
    if 0 <= index < len(rules):
        rules.pop(index)
    risk["rules"] = rules
    edited["risk"] = risk
    return strategy_schema.validate_definition(edited)


def _strategy_has_entry_exit_conditions(definition: dict) -> bool:
    _, entry_rules = _group_key_and_rules(definition.get("entry", {}))
    _, exit_rules = _group_key_and_rules(definition.get("exit", {}))
    return bool(entry_rules) and bool(exit_rules)


def _append_node(lines: list[str], node_id: str, label: str, *, fill: str, shape: str = "box"):
    lines.append(
        f'{node_id} [label="{_dot_escape(label)}", shape={shape}, style="rounded,filled", fillcolor="{fill}", color="#cbd5e1", penwidth=1.35];'
    )


def _append_cluster_start(lines: list[str], cluster_id: str, label: str, *, color: str, fill: str):
    lines.extend(
        [
            f"subgraph cluster_{cluster_id} {{",
            f'label="{_dot_escape(label)}";',
            'style="rounded,filled";',
            f'color="{color}";',
            f'fillcolor="{fill}";',
            'penwidth=1.4;',
            'fontsize=12;',
            'fontname="Inter";',
        ]
    )


def _append_cluster_end(lines: list[str]):
    lines.append("}")


def build_strategy_diagram_dot(definition: dict, risk: dict) -> str:
    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    filters_joiner, filters = _group_key_and_rules(definition.get("filters", {"all": []}))
    entry_joiner, entry_rules = _group_key_and_rules(definition.get("entry_rules", {"all": []}))
    exit_joiner, exit_rules = _group_key_and_rules(definition.get("exit_rules", {"any": []}))
    take_profits = risk.get("take_profits", []) if isinstance(risk.get("take_profits"), list) else []
    enabled_tps = [
        tp for tp in take_profits
        if isinstance(tp, dict)
        and (float(tp.get("pnl_pct", 0.0) or 0.0) > 0 or float(tp.get("amount_pct", 0.0) or 0.0) > 0)
    ]
    atr = risk.get("atr_trailing", {}) if isinstance(risk.get("atr_trailing"), dict) else {}
    stop_loss = float(risk.get("stop_loss_pct", 0.0) or 0.0)

    lines = [
        "digraph Strategy {",
        'graph [rankdir=LR, bgcolor="transparent", pad="0.30", nodesep="0.38", ranksep="0.65", splines=ortho, compound=true];',
        'node [fontname="Inter", fontsize=10, margin="0.13,0.08"];',
        'edge [fontname="Inter", fontsize=9, color="#64748b", arrowsize=0.7, penwidth=1.2];',
    ]

    _append_node(lines, "start", "Strategy starts\\non closed candle", fill="#f8fafc", shape="circle")

    _append_cluster_start(lines, "entry", "ENTRY CONDITIONS", color="#93c5fd", fill="#eff6ff")
    _append_node(lines, "market", "Market Data\\nOHLCV candles", fill="#dbeafe", shape="box")
    _append_node(lines, "entry_gate", f"Entry Gate\\n{entry_joiner.upper()}", fill="#bfdbfe", shape="diamond")
    if filters:
        _append_node(lines, "filters_gate", f"Filter Gate\\n{filters_joiner.upper()}", fill="#ddd6fe", shape="diamond")
        for idx, rule in enumerate(filters):
            node_id = f"filter_{idx}"
            _append_node(lines, node_id, f"F{idx + 1}\\n{_rule_summary(rule, parameters)}", fill="#f5f3ff")
            lines.append(f"{node_id} -> filters_gate [color=\"#8b5cf6\"];")
        lines.append('market -> filters_gate [label="feed"];')
        lines.append('filters_gate -> entry_gate [label="pass"];')
    else:
        lines.append('market -> entry_gate [label="feed"];')
    for idx, rule in enumerate(entry_rules):
        node_id = f"entry_{idx}"
        _append_node(lines, node_id, f"E{idx + 1}\\n{_rule_summary(rule, parameters)}", fill="#dbeafe")
        lines.append(f"{node_id} -> entry_gate [color=\"#2563eb\"];")
    _append_cluster_end(lines)

    _append_cluster_start(lines, "trade", "TRADE", color="#86efac", fill="#f0fdf4")
    _append_node(lines, "enter_long", "Enter Long\\nmarket buy", fill="#dcfce7", shape="box")
    _append_node(lines, "position", "Open Position\\ntrack PnL", fill="#bbf7d0", shape="ellipse")
    _append_cluster_end(lines)

    _append_cluster_start(lines, "exit", "EXIT CONDITIONS", color="#facc15", fill="#fefce8")
    _append_node(lines, "exit_gate", f"Exit Gate\\n{exit_joiner.upper()}", fill="#fde68a", shape="diamond")
    for idx, rule in enumerate(exit_rules):
        node_id = f"exit_{idx}"
        _append_node(lines, node_id, f"X{idx + 1}\\n{_rule_summary(rule, parameters)}", fill="#fffbeb")
        lines.append(f"{node_id} -> exit_gate [color=\"#ca8a04\"];")
    _append_cluster_end(lines)

    _append_cluster_start(lines, "risk", "RISK MANAGEMENT", color="#fca5a5", fill="#fff1f2")
    if stop_loss > 0:
        _append_node(lines, "stop_loss", f"SL\\n{stop_loss:g}% loss", fill="#fee2e2")

    if atr.get("enabled"):
        atr_label = (
            f"ATR Trail\\nATR({int(atr.get('period', 14) or 14)}) "
            f"x {float(atr.get('multiplier', 0.0) or 0.0):g}\\n"
            f"activate {float(atr.get('activation_pnl_pct', 0.0) or 0.0):g}% PnL"
        )
        _append_node(lines, "atr_stop", atr_label, fill="#fee2e2")

    if enabled_tps:
        for idx, tp in enumerate(enabled_tps):
            level = int(tp.get("level", idx + 1) or idx + 1)
            pnl_pct = float(tp.get("pnl_pct", 0.0) or 0.0)
            amount_pct = float(tp.get("amount_pct", 0.0) or 0.0)
            node_id = f"tp_{level}"
            _append_node(lines, node_id, f"TP{level}\\n{pnl_pct:g}% PnL\\nsell {amount_pct:g}%", fill="#ffedd5")
    if stop_loss <= 0 and not atr.get("enabled") and not enabled_tps:
        _append_node(lines, "risk_none", "No active\\nrisk rules", fill="#f8fafc", shape="box")
    _append_cluster_end(lines)

    _append_node(lines, "close_long", "Close Long\\nmarket sell", fill="#fecaca", shape="box")
    _append_node(lines, "done", "Trade closed", fill="#f8fafc", shape="circle")

    lines.append('start -> market [lhead=cluster_entry];')
    lines.append('entry_gate -> enter_long [label="approved", ltail=cluster_entry, lhead=cluster_trade, color="#16a34a"];')
    lines.append('enter_long -> position [color="#16a34a"];')
    lines.append('position -> exit_gate [label="evaluate", ltail=cluster_trade, lhead=cluster_exit];')
    lines.append('exit_gate -> close_long [label="true", ltail=cluster_exit, color="#dc2626"];')

    risk_nodes = []
    if stop_loss > 0:
        risk_nodes.append("stop_loss")
    if atr.get("enabled"):
        risk_nodes.append("atr_stop")
    for tp in enabled_tps:
        level = int(tp.get("level", 1) or 1)
        risk_nodes.append(f"tp_{level}")
    if not risk_nodes:
        risk_nodes.append("risk_none")

    for node_id in risk_nodes:
        lines.append(f'position -> {node_id} [label="monitor", ltail=cluster_trade, lhead=cluster_risk, color="#f97316"];')
        if node_id.startswith("tp_"):
            lines.append(f'{node_id} -> position [label="partial / remaining", color="#16a34a"];')
        elif node_id != "risk_none":
            lines.append(f'{node_id} -> close_long [label="trigger", color="#dc2626"];')

    lines.append('close_long -> done [color="#dc2626"];')

    lines.append("}")
    return "\n".join(lines)


def render_strategy_diagrams(definition: dict, risk: dict):
    st.markdown("### Strategy Diagram1")
    st.caption("Visual flow generated from Definition_JSON.")
    st.graphviz_chart(build_strategy_diagram_dot(definition, risk), use_container_width=True)


def _active_block_key(strategy_id: str) -> str:
    return f"{strategy_id}_active_diagram_block"


def _set_active_block(strategy_id: str, block: dict):
    st.session_state[_active_block_key(strategy_id)] = block


def _get_active_block(strategy_id: str) -> dict:
    block = st.session_state.get(_active_block_key(strategy_id), {})
    return block if isinstance(block, dict) else {}


def _clear_active_block(strategy_id: str):
    st.session_state.pop(_active_block_key(strategy_id), None)
    try:
        del st.query_params["strategy_block"]
    except Exception:
        pass


def _block_token(block: dict) -> str:
    return quote(json.dumps(block, ensure_ascii=True, separators=(",", ":")))


def _block_from_token(token: str) -> dict:
    try:
        value = json.loads(unquote(str(token or "")))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def sync_active_block_from_query(strategy_id: str):
    if _get_active_block(strategy_id):
        return
    raw_value = st.query_params.get("strategy_block", "")
    if isinstance(raw_value, list):
        raw_value = raw_value[0] if raw_value else ""
    prefix = f"{strategy_id}:"
    if not raw_value or not str(raw_value).startswith(prefix):
        return
    block = _block_from_token(str(raw_value)[len(prefix):])
    if block:
        st.session_state[_active_block_key(strategy_id)] = block


def _diagram_button(label: str, block: dict, strategy_id: str, *, key: str, help_text: str = ""):
    if st.button(label, key=key, help=help_text, use_container_width=True):
        _set_active_block(strategy_id, block)
        try:
            del st.query_params["strategy_block"]
        except Exception:
            pass
        st.rerun()


def _diagram_condition_card(rule: dict, parameters: dict, block: dict, strategy_id: str):
    _diagram_button(
        _native_condition_label(rule, parameters),
        block,
        strategy_id,
        key=f"{strategy_id}_native_diagram_{block.get('group')}_{block.get('index')}",
        help_text=_rule_summary(rule, parameters),
    )


def _html_button(label: str, block: dict, strategy_id: str, css_class: str = "", title: str = "") -> str:
    token = _block_token(block)
    label_html = "<br>".join(html.escape(str(part)) for part in str(label).split("\n"))
    href = f"?strategy_block={quote(str(strategy_id))}:{token}"
    return (
        f'<a class="node {css_class}" title="{html.escape(title or str(label))}" '
        f'href="{html.escape(href)}" target="_top">{label_html}</a>'
    )


def _html_node_button(content_html: str, block: dict, strategy_id: str, css_class: str = "", title: str = "") -> str:
    token = _block_token(block)
    href = f"?strategy_block={quote(str(strategy_id))}:{token}"
    return (
        f'<a class="node {css_class}" title="{html.escape(title)}" '
        f'href="{html.escape(href)}" target="_top">{content_html}</a>'
    )


def _html_disabled_node(label: str, css_class: str = "") -> str:
    label_html = "<br>".join(html.escape(str(part)) for part in str(label).split("\n"))
    return f'<div class="node disabled {css_class}">{label_html}</div>'


def _html_group(title: str, body: str, css_class: str) -> str:
    return f"""
    <section class="group {css_class}">
        <div class="group-title">{html.escape(title)}</div>
        <div class="group-body">{body}</div>
    </section>
    """


def _logic_pill(logic: str) -> str:
    label = "All conditions must pass" if logic == "all" else "Any condition can pass"
    return f'<div class="logic-pill">{html.escape(label)}</div>'


def _logic_separator(logic: str) -> str:
    label = "AND" if logic == "all" else "OR"
    css_class = "logic-and" if logic == "all" else "logic-or"
    return f'<div class="logic-separator {css_class}">{label}</div>'


def _summary_html(summary: str) -> str:
    return "<br>".join(html.escape(str(part)) for part in str(summary).split("\n"))


def _html_condition_content(rule: dict, parameters: dict) -> str:
    if isinstance(rule, dict) and rule.get("type") == "group":
        joiner, rules = _group_key_and_rules(rule)
        operators = _group_operators(rule, joiner, len(rules))
        nested_parts = []
        for idx, nested_rule in enumerate(rules):
            if idx > 0:
                operator = operators[idx - 1] if idx - 1 < len(operators) else joiner
                nested_parts.append(_logic_separator(operator))
            nested_parts.append(f'<div class="nested-rule">{_html_condition_content(nested_rule, parameters)}</div>')
        return '<div class="nested-rule-stack">' + "".join(nested_parts) + "</div>"
    return _summary_html(_rule_summary(rule, parameters))


def _html_condition_sequence(rules: list, joiner: str, parameters: dict, strategy_id: str, group_name: str, css_class: str, operators: list[str] | None = None) -> str:
    operators = operators if isinstance(operators, list) else [joiner for _ in range(max(len(rules) - 1, 0))]
    parts = []
    for idx, rule in enumerate(rules):
        if idx > 0:
            operator = operators[idx - 1] if idx - 1 < len(operators) else joiner
            parts.append(_logic_separator(operator))
        summary = _rule_summary(rule, parameters)
        parts.append(
            _html_node_button(
                _html_condition_content(rule, parameters),
                {"kind": "rule", "group": group_name, "index": idx},
                strategy_id,
                css_class,
                summary,
            )
        )
    return "".join(parts)


def _is_take_profit_risk_rule(rule: dict) -> bool:
    return str(rule.get("type", "")) in {"take_profit_pct", "take_profit_r_multiple"}


def _active_risk_and_take_profit_labels(risk: dict) -> tuple[list[tuple[str, dict, str]], list[tuple[str, dict, str]]]:
    risk_labels = []
    take_profit_labels = []
    for label, block, help_text in _risk_block_labels(risk):
        index = _block_index(block)
        rules = risk.get("rules", []) if isinstance(risk.get("rules"), list) else []
        rule = rules[index] if index >= 0 and index < len(rules) else {}
        if isinstance(rule, dict) and _is_take_profit_risk_rule(rule):
            take_profit_labels.append((label, block, help_text))
        else:
            risk_labels.append((label, block, help_text))
    return risk_labels, take_profit_labels


def _diagram_label_nodes(labels: list[tuple[str, dict, str]], strategy_id: str, css_class: str) -> str:
    return "".join(
        _html_button(label, block, strategy_id, css_class, help_text)
        for label, block, help_text in labels
    )


def _component_diagram_height(definition: dict, risk: dict) -> int:
    _, _, _, entry_rules, _, exit_rules = _diagram_groups(definition)
    risk_labels, take_profit_labels = _active_risk_and_take_profit_labels(risk)
    visible_counts = [
        len(entry_rules) + 1 if entry_rules else 0,
        len(exit_rules) + 1 if exit_rules else 0,
        len(risk_labels),
        len(take_profit_labels),
    ]
    max_count = max(visible_counts or [1], default=1)
    return min(max(220, 112 + (max_count * 62)), 760)


def build_component_diagram_html(definition: dict, risk: dict, strategy_id: str) -> str:
    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    filters_joiner, filters, entry_joiner, entry_rules, exit_joiner, exit_rules = _diagram_groups(definition)
    del filters_joiner, filters
    entry_group = definition.get("entry", {}) if isinstance(definition.get("entry"), dict) else {}
    exit_group = definition.get("exit", {}) if isinstance(definition.get("exit"), dict) else {}
    entry_operators = _group_operators(entry_group, entry_joiner, len(entry_rules))
    exit_operators = _group_operators(exit_group, exit_joiner, len(exit_rules))

    entry_nodes = _html_condition_sequence(entry_rules, entry_joiner, parameters, strategy_id, "entry", "entry", entry_operators)
    exit_nodes = _html_condition_sequence(exit_rules, exit_joiner, parameters, strategy_id, "exit", "exit", exit_operators)
    risk_labels, take_profit_labels = _active_risk_and_take_profit_labels(definition.get("risk", {}))
    risk_nodes = _diagram_label_nodes(risk_labels, strategy_id, "risk")
    take_profit_nodes = _diagram_label_nodes(take_profit_labels, strategy_id, "take-profit")

    sections = []
    if entry_nodes:
        entry_body = _logic_pill(entry_joiner) + f'<div class="node-stack">{entry_nodes}</div>'
        sections.append(_html_group("Entry Conditions", entry_body, "entry-group"))
    if exit_nodes:
        exit_body = _logic_pill(exit_joiner) + f'<div class="node-stack">{exit_nodes}</div>'
        sections.append(_html_group("Exit Conditions", exit_body, "exit-group"))
    if risk_nodes:
        sections.append(_html_group("Risk Management", f'<div class="node-stack">{risk_nodes}</div>', "risk-group"))
    if take_profit_nodes:
        sections.append(_html_group("Take Profits", f'<div class="node-stack">{take_profit_nodes}</div>', "take-profit-group"))
    if not sections:
        sections.append(_html_group("Strategy", '<div class="empty">No configured conditions or risk rules</div>', "empty-group"))

    section_count = max(len(sections), 1)
    grid_columns = f"repeat({section_count}, minmax(220px, 1fr))"

    return f"""
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8" />
        <style>
            :root {{
                color-scheme: light;
                font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            }}
            body {{
                margin: 0;
                padding: 4px;
                background: transparent;
                color: #0f172a;
            }}
            .diagram {{
                display: grid;
                grid-template-columns: {grid_columns};
                gap: 12px;
                align-items: stretch;
                width: 100%;
            }}
            .group {{
                border: 1.5px solid #cbd5e1;
                border-radius: 8px;
                padding: 10px;
                box-sizing: border-box;
            }}
            .entry-group {{ background: #eff6ff; border-color: #93c5fd; }}
            .exit-group {{ background: #fefce8; border-color: #facc15; }}
            .risk-group {{ background: #fff1f2; border-color: #fca5a5; }}
            .take-profit-group {{ background: #fff7ed; border-color: #fdba74; }}
            .empty-group {{ background: #f8fafc; border-color: #cbd5e1; }}
            .group-title {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: .08em;
                text-transform: uppercase;
                text-align: center;
                margin-bottom: 10px;
            }}
            .group-body {{
                display: flex;
                flex-direction: column;
                gap: 8px;
            }}
            .node-stack {{
                display: grid;
                gap: 8px;
            }}
            .node {{
                width: 100%;
                min-height: 44px;
                border: 1px solid #cbd5e1;
                border-radius: 9px;
                background: #ffffff;
                color: #0f172a;
                padding: 9px 10px;
                font-size: 12px;
                font-weight: 650;
                line-height: 1.25;
                text-align: left;
                box-sizing: border-box;
                box-shadow: 0 1px 0 rgba(15, 23, 42, .04);
                white-space: normal;
            }}
            a.node {{
                display: block;
                text-decoration: none;
            }}
            a.node {{
                cursor: pointer;
                transition: transform .08s ease, box-shadow .08s ease, border-color .08s ease;
            }}
            a.node:hover {{
                transform: translateY(-1px);
                box-shadow: 0 8px 18px rgba(15, 23, 42, .08);
                border-color: #ff4b4b;
            }}
            .disabled {{
                cursor: default;
                opacity: .88;
            }}
            .gate {{ background: #e0f2fe; border-color: #7dd3fc; text-align: center; }}
            .filter {{ background: #f5f3ff; border-color: #c4b5fd; }}
            .entry {{ background: #dbeafe; border-color: #93c5fd; }}
            .exit {{ background: #fffbeb; border-color: #fcd34d; }}
            .risk {{ background: #fee2e2; border-color: #fca5a5; }}
            .take-profit {{ background: #ffedd5; border-color: #fdba74; }}
            .logic-pill {{
                border: 1px solid rgba(100, 116, 139, .28);
                border-radius: 999px;
                background: rgba(255,255,255,.6);
                color: #475569;
                padding: 5px 8px;
                font-size: 11px;
                font-weight: 800;
                text-transform: uppercase;
                text-align: center;
                margin-bottom: 8px;
            }}
            .logic-separator {{
                justify-self: start;
                border: 1px solid rgba(100, 116, 139, .28);
                border-radius: 7px;
                padding: 3px 10px;
                font-size: 10px;
                font-weight: 900;
                letter-spacing: .05em;
                line-height: 1.2;
            }}
            .logic-and {{
                color: #be185d;
                border-color: rgba(190, 24, 93, .35);
                background: rgba(244, 114, 182, .13);
            }}
            .logic-or {{
                color: #b91c1c;
                border-color: rgba(185, 28, 28, .35);
                background: rgba(248, 113, 113, .14);
            }}
            .nested-rule-stack {{
                display: grid;
                gap: 6px;
            }}
            .nested-rule {{
                border-left: 3px solid rgba(37, 99, 235, .35);
                padding-left: 8px;
            }}
            .nested-rule .logic-separator {{
                margin: 1px 0;
            }}
            .empty {{
                padding: 10px;
                border: 1px dashed #cbd5e1;
                border-radius: 8px;
                color: #64748b;
                font-size: 12px;
                text-align: center;
                background: rgba(255,255,255,.55);
            }}
        </style>
    </head>
    <body>
        <div class="diagram">
            {"".join(sections)}
        </div>
        <script>
            function selectBlock(strategyId, token) {{
                const parentLocation = window.parent.location;
                const params = new URLSearchParams(parentLocation.search);
                params.set("strategy_block", strategyId + ":" + token);
                window.parent.location.href = parentLocation.pathname + "?" + params.toString();
            }}
        </script>
    </body>
    </html>
    """


def render_component_strategy_diagram(definition: dict, risk: dict, strategy_id: str):
    st.space()
    st.markdown("### Strategy Diagram")
    st.caption("Click a block to edit it directly below the diagram.")
    render_native_strategy_diagram(definition, risk, strategy_id)


def render_native_strategy_diagram(definition: dict, risk: dict, strategy_id: str):
    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    _, _, entry_joiner, entry_rules, exit_joiner, exit_rules = _diagram_groups(definition)
    risk_labels, take_profit_labels = _active_risk_and_take_profit_labels(definition.get("risk", {}))

    sections = [
        ("Entry Conditions", "entry", entry_joiner, entry_rules, "entry"),
        ("Exit Conditions", "exit", exit_joiner, exit_rules, "exit"),
        ("Risk Management", "risk", None, risk_labels, "risk"),
        ("Take Profits", "take-profit", None, take_profit_labels, "take-profit"),
    ]

    columns = st.columns(len(sections), gap="small")
    for col, (title, css_class, joiner, items, group_name) in zip(columns, sections):
        with col.container(border=True):
            st.markdown(f'<div class="bec-diagram-group">{html.escape(title)}</div>', unsafe_allow_html=True)
            if group_name in {"entry", "exit"}:
                selected_joiner = joiner
                if not items:
                    selected_joiner = st.segmented_control(
                        "Condition logic",
                        options=["all", "any"],
                        default=joiner if items else None,
                        format_func=lambda value: "AND" if value == "all" else "OR",
                        key=f"{strategy_id}_diagram_add_{group_name}_logic",
                        label_visibility="collapsed",
                        width="content",
                    )
                if items:
                    _render_native_condition_sequence(
                        definition,
                        items,
                        joiner,
                        parameters,
                        strategy_id,
                        group_name,
                        css_class,
                    )
                else:
                    st.info(f"No {group_name} conditions configured.")
                add_label = f"Add {group_name.title()} Condition"
                if st.button(
                    add_label,
                    icon=icons.ICON_ADD,
                    type="primary",
                    key=f"{strategy_id}_diagram_add_{group_name}_condition",
                    use_container_width=True,
                    disabled=selected_joiner not in {"all", "any"},
                    help="Choose AND or OR first." if selected_joiner not in {"all", "any"} else None,
                ):
                    condition_dialog(strategy_id, definition, group_name, None, selected_joiner)
            else:
                for idx, (label, block, help_text) in enumerate(items):
                    _diagram_button(
                        label,
                        block,
                        strategy_id,
                        key=f"{strategy_id}_native_diagram_{group_name}_{idx}",
                        help_text=help_text,
                    )
                if group_name == "risk":
                    _risk_dialog_button(st, definition, strategy_id, "Add Stop Loss", "stop_loss_pct", "diagram_add_risk")
                    _risk_dialog_button(st, definition, strategy_id, "Add ATR Stop", "atr_stop", "diagram_add_risk")
                elif group_name == "take-profit":
                    if not items:
                        st.info("No take profits configured.")
                    _risk_dialog_button(st, definition, strategy_id, "Add Take Profit", "take_profit_pct", "diagram_add_take_profit")


def _render_native_condition_sequence(definition: dict, rules: list, joiner: str, parameters: dict, strategy_id: str, group_name: str, css_class: str):
    group = definition.get(group_name, {}) if isinstance(definition.get(group_name), dict) else {}
    operators = _group_operators(group, joiner, len(rules))
    for idx, rule in enumerate(rules):
        if idx > 0:
            current_operator = operators[idx - 1] if idx - 1 < len(operators) else joiner
            operator_container = st.container(horizontal=True, horizontal_alignment="center")
            selected_joiner = operator_container.segmented_control(
                "Condition logic",
                options=["all", "any"],
                default=current_operator,
                format_func=lambda value: "AND" if value == "all" else "OR",
                key=f"{strategy_id}_diagram_{group_name}_joiner_{idx}",
                label_visibility="collapsed",
                width="content",
            )
            if selected_joiner in {"all", "any"} and selected_joiner != current_operator:
                edited = _set_group_operator(definition, group_name, idx - 1, selected_joiner)
                autosaved_id = _autosave_strategy_definition(strategy_id, edited)
                st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
                st.rerun()
        _diagram_condition_card(
            rule,
            parameters,
            {"kind": "rule", "group": group_name, "index": idx},
            strategy_id,
        )


def _native_condition_label(rule: dict, parameters: dict) -> str:
    if isinstance(rule, dict) and rule.get("type") == "group":
        joiner, rules = _group_key_and_rules(rule)
        separator = f"\n{'AND' if joiner == 'all' else 'OR'}\n"
        return separator.join(_native_condition_label(nested_rule, parameters) for nested_rule in rules)
    return _rule_summary(rule, parameters)


def _risk_block_labels(risk: dict) -> list[tuple[str, dict, str]]:
    labels = []
    rules = risk.get("rules", []) if isinstance(risk, dict) and isinstance(risk.get("rules"), list) else []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            continue
        rule_type = str(rule.get("type", "risk"))
        if rule_type == "stop_loss_pct":
            label = f"SL\n{float(rule.get('pct', 0.0) or 0.0):g}%"
        elif rule_type == "take_profit_pct":
            label = f"TP\n{float(rule.get('pct', 0.0) or 0.0):g}% / {float(rule.get('size_pct', 100.0) or 100.0):g}%"
        elif rule_type == "take_profit_r_multiple":
            label = f"TP\n{float(rule.get('r_multiple', 0.0) or 0.0):g}R / {float(rule.get('size_pct', 100.0) or 100.0):g}%"
        elif rule_type == "atr_stop":
            indicator = rule.get("indicator", {}) if isinstance(rule.get("indicator"), dict) else {}
            params = indicator.get("params", {}) if isinstance(indicator.get("params"), dict) else {}
            period = int(params.get("period", 14) or 14)
            multiplier = float(rule.get("multiplier", 0.0) or 0.0)
            activation = float(rule.get("activation_pnl_pct", 0.0) or 0.0)
            label = f"ATR Stop\nP{period} x{multiplier:g} Act{activation:g}%"
        elif rule_type == "trailing_indicator":
            label = "Trailing\nindicator"
        else:
            label = rule_type.replace("_", " ").title()
        labels.append((label, {"kind": "ast_risk", "index": idx}, rule_type.replace("_", " ").title()))
    return labels or [("No risk rules\nconfigured", {"kind": "risk_empty"}, "No risk rules configured")]


def render_clickable_strategy_diagram(definition: dict, risk: dict, strategy_id: str):
    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    filters_joiner, filters = _group_key_and_rules(definition.get("filters", {"all": []}))
    entry_joiner, entry_rules = _group_key_and_rules(definition.get("entry_rules", {"all": []}))
    exit_joiner, exit_rules = _group_key_and_rules(definition.get("exit_rules", {"any": []}))

    st.markdown("### Strategy Diagram")
    st.caption("Click a block to edit it directly below the diagram.")
    cols = st.columns([1.4, 1.0, 1.15, 1.35], gap="medium")

    with cols[0].container(border=True):
        st.markdown('<div class="bec-diagram-group">Entry</div>', unsafe_allow_html=True)
        _diagram_button(
            f"Filter Gate\n{filters_joiner.upper()}",
            {"kind": "joiner", "group": "filters"},
            strategy_id,
            key=f"{strategy_id}_diagram_filter_gate",
        )
        if filters:
            for idx, rule in enumerate(filters):
                _diagram_button(
                    f"F{idx + 1}\n{_rule_summary(rule, parameters)}",
                    {"kind": "rule", "group": "filters", "index": idx},
                    strategy_id,
                    key=f"{strategy_id}_diagram_filter_{idx}",
                )
        else:
            st.caption("No entry filters.")
        _diagram_button(
            f"Entry Gate\n{entry_joiner.upper()}",
            {"kind": "joiner", "group": "entry_rules"},
            strategy_id,
            key=f"{strategy_id}_diagram_entry_gate",
        )
        for idx, rule in enumerate(entry_rules):
            _diagram_button(
                f"E{idx + 1}\n{_rule_summary(rule, parameters)}",
                {"kind": "rule", "group": "entry_rules", "index": idx},
                strategy_id,
                key=f"{strategy_id}_diagram_entry_{idx}",
            )

    with cols[1].container(border=True):
        st.markdown('<div class="bec-diagram-group">Trade</div>', unsafe_allow_html=True)
        st.button("Enter Long\nmarket buy", disabled=True, use_container_width=True, key=f"{strategy_id}_diagram_enter_disabled")
        st.button("Open Position\ntrack PnL", disabled=True, use_container_width=True, key=f"{strategy_id}_diagram_position_disabled")

    with cols[2].container(border=True):
        st.markdown('<div class="bec-diagram-group">Exit</div>', unsafe_allow_html=True)
        _diagram_button(
            f"Exit Gate\n{exit_joiner.upper()}",
            {"kind": "joiner", "group": "exit_rules"},
            strategy_id,
            key=f"{strategy_id}_diagram_exit_gate",
        )
        for idx, rule in enumerate(exit_rules):
            _diagram_button(
                f"X{idx + 1}\n{_rule_summary(rule, parameters)}",
                {"kind": "rule", "group": "exit_rules", "index": idx},
                strategy_id,
                key=f"{strategy_id}_diagram_exit_{idx}",
            )

    with cols[3].container(border=True):
        st.markdown('<div class="bec-diagram-group">Risk</div>', unsafe_allow_html=True)
        for idx, (label, block, help_text) in enumerate(_risk_block_labels(risk)):
            _diagram_button(
                label,
                block,
                strategy_id,
                key=f"{strategy_id}_diagram_risk_{idx}",
                help_text=help_text,
            )


def render_joiner_editor(definition: dict, block: dict, strategy_id: str) -> dict:
    edited = copy.deepcopy(definition)
    group_name = block.get("group", "entry_rules")
    group = edited.get(group_name, {"all": []})
    joiner, rules = _group_key_and_rules(group)
    old_rule_count = len(rules)
    selected_joiner = st.segmented_control(
        "Condition logic",
        options=["all", "any"],
        default=joiner,
        format_func=lambda value: "AND - all conditions must pass" if value == "all" else "OR - any condition can pass",
        key=f"{strategy_id}_diagram_joiner_{group_name}",
    )
    selected_joiner = selected_joiner or joiner
    if group_name in {"entry", "exit"} and isinstance(group, dict):
        group["logic"] = selected_joiner
        group["conditions"] = rules
        edited[group_name] = group
    else:
        edited[group_name] = {selected_joiner: rules}
    if edited != definition:
        autosaved_id = _autosave_strategy_definition(strategy_id, edited)
        st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
        st.rerun()
    return edited


def render_selected_rule_editor(definition: dict, block: dict, strategy_id: str) -> dict:
    edited = copy.deepcopy(definition)
    group_name = block.get("group", "entry_rules")
    group = edited.get(group_name, {"all": []})
    joiner, rules = _group_key_and_rules(group)
    old_rule_count = len(rules)
    index = int(block.get("index", 0) or 0)
    if index < 0 or index >= len(rules):
        st.warning("The selected rule no longer exists.")
        return edited
    parameters = edited.get("parameters", {}) if isinstance(edited.get("parameters"), dict) else {}
    selected_rule = rules[index]
    rule_type = "exit" if group_name in {"exit", "exit_rules"} else "entry"
    if isinstance(selected_rule, dict) and selected_rule.get("type") == "group":
        edited_rule = render_condition_group_block(
            selected_rule,
            parameters,
            f"{strategy_id}_diagram_rule_edit_{group_name}_{index}",
            rule_type,
            f"{group_name.replace('_', ' ').title()} {index + 1}",
        )
    else:
        edited_rule = render_rule_block(
            selected_rule,
            parameters,
            f"{strategy_id}_diagram_rule_edit_{group_name}_{index}",
            rule_type,
            f"{group_name.replace('_', ' ').title()} {index + 1}",
        )
    removed_rule = edited_rule is None
    if removed_rule:
        rules.pop(index)
    else:
        rules[index] = edited_rule
    operators = _operators_after_rule_edit(
        group,
        joiner,
        old_rule_count,
        removed_index=index if removed_rule else None,
    )
    if group_name in {"entry", "exit"} and isinstance(group, dict):
        group["logic"] = joiner
        group["conditions"] = rules
        group["operators"] = operators
        edited[group_name] = group
    else:
        edited[group_name] = {"logic": joiner, "conditions": rules, "operators": operators}
    if edited != definition:
        autosaved_id = _autosave_strategy_definition(strategy_id, edited)
        st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
        if removed_rule:
            st.session_state.pop(_active_block_key(autosaved_id), None)
            try:
                del st.query_params["strategy_block"]
            except Exception:
                pass
        st.rerun()
    return edited


def render_condition_group_block(rule: dict, parameters: dict, key_prefix: str, rule_type: str, title_prefix: str) -> dict | None:
    nested_rules = rule.get("conditions", []) if isinstance(rule.get("conditions"), list) else []
    joiner = rule.get("logic") if rule.get("logic") in {"all", "any"} else "all"
    with st.container(border=True):
        st.markdown(f"#### {title_prefix}")
        selected_joiner = joiner
        if len(nested_rules) <= 1:
            selected_joiner = st.segmented_control(
                "Nested condition logic",
                options=["all", "any"],
                default=joiner,
                format_func=lambda value: "AND" if value == "all" else "OR",
                key=f"{key_prefix}_joiner",
            ) or joiner
        edited_nested_rules = []
        for nested_idx, nested_rule in enumerate(nested_rules):
            if nested_idx > 0:
                joiner_container = st.container(horizontal=True, horizontal_alignment="center")
                selected_joiner = joiner_container.segmented_control(
                    "Nested condition logic",
                    options=["all", "any"],
                    default=selected_joiner,
                    format_func=lambda value: "AND" if value == "all" else "OR",
                    key=f"{key_prefix}_joiner_{nested_idx}",
                    label_visibility="collapsed",
                )
                selected_joiner = selected_joiner or joiner
            if isinstance(nested_rule, dict) and nested_rule.get("type") == "group":
                st.info("Nested groups inside groups are best edited in Advanced JSON.")
                edited_nested_rules.append(nested_rule)
                continue
            edited_nested = render_rule_block(
                nested_rule,
                parameters,
                f"{key_prefix}_nested_{nested_idx}",
                rule_type,
                f"Condition {nested_idx + 1}",
            )
            if edited_nested is not None:
                edited_nested_rules.append(edited_nested)
        if st.button("Remove group", icon=icons.ICON_DELETE, key=f"{key_prefix}_remove_group"):
            return None
    return {"type": "group", "logic": selected_joiner, "conditions": edited_nested_rules}


def render_selected_ast_risk_editor(definition: dict, block: dict, strategy_id: str) -> dict:
    edited = copy.deepcopy(definition)
    risk = edited.get("risk", {}) if isinstance(edited.get("risk"), dict) else {"rules": []}
    rules = risk.get("rules", []) if isinstance(risk.get("rules"), list) else []
    index = _block_index(block)
    if index < 0 or index >= len(rules):
        st.warning("The selected risk rule no longer exists.")
        return edited

    original_rules = copy.deepcopy(rules)
    rule = copy.deepcopy(rules[index] if isinstance(rules[index], dict) else {})
    rule_type = str(rule.get("type", "risk"))
    removed_rule = False
    with st.container(border=True):
        st.markdown(f"#### {rule_type.replace('_', ' ').title()}")
        if rule_type == "stop_loss_pct":
            rule["type"] = "stop_loss_pct"
            rule["pct"] = float(
                st.number_input(
                    "Stop loss %",
                    min_value=0.0,
                    value=float(rule.get("pct", 0.0) or 0.0),
                    width=125,
                    step=0.5,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_pct",
                )
            )
        elif rule_type == "take_profit_pct":
            row = st.container(horizontal=True, vertical_alignment="bottom")
            rule["type"] = "take_profit_pct"
            rule["pct"] = float(
                row.number_input(
                    "PnL %",
                    min_value=0.0,
                    value=float(rule.get("pct", 0.0) or 0.0),
                    step=0.5,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_pct",
                    width=125,
                )
            )
            rule["size_pct"] = float(
                row.number_input(
                    "Amount %",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(rule.get("size_pct", 100.0) or 100.0),
                    step=5.0,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_size",
                    width=125,
                )
            )
        elif rule_type == "take_profit_r_multiple":
            row = st.container(horizontal=True, vertical_alignment="bottom")
            rule["type"] = "take_profit_r_multiple"
            rule["r_multiple"] = float(
                row.number_input(
                    "R multiple",
                    min_value=0.0,
                    value=float(rule.get("r_multiple", 0.0) or 0.0),
                    step=0.25,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_r_multiple",
                    width=125,
                )
            )
            rule["size_pct"] = float(
                row.number_input(
                    "Amount %",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(rule.get("size_pct", 100.0) or 100.0),
                    step=5.0,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_size",
                    width=125,
                )
            )
        elif rule_type == "atr_stop":
            row = st.container(horizontal=True, vertical_alignment="bottom")
            rule["type"] = "atr_stop"
            indicator = rule.get("indicator", {}) if isinstance(rule.get("indicator"), dict) else {}
            params = indicator.get("params", {}) if isinstance(indicator.get("params"), dict) else {}
            period = int(
                row.number_input(
                    "ATR period",
                    min_value=1,
                    value=int(params.get("period", 14) or 14),
                    step=1,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_atr_period",
                    width=125,
                )
            )
            rule["indicator"] = {
                "type": "indicator",
                "name": "ATR",
                "params": {"period": period},
                "timeframe": str(indicator.get("timeframe", "current") or "current"),
                "source": {"type": "price", "field": "Close", "timeframe": str(indicator.get("timeframe", "current") or "current")},
                "output": str(indicator.get("output", "value") or "value"),
            }
            rule["multiplier"] = float(
                row.number_input(
                    "Multiplier",
                    min_value=0.0,
                    value=float(rule.get("multiplier", 1.8) or 1.8),
                    step=0.1,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_multiplier",
                    width=125,
                )
            )
            rule["activation_pnl_pct"] = float(
                row.number_input(
                    "Activation PnL %",
                    min_value=0.0,
                    value=float(rule.get("activation_pnl_pct", 0.0) or 0.0),
                    step=0.5,
                    key=f"{strategy_id}_diagram_ast_risk_{index}_activation",
                    width=125,
                )
            )
        else:
            st.info("This risk rule type is edited as JSON.")
            risk_text = st.text_area(
                "Risk rule JSON",
                value=json.dumps(rule, ensure_ascii=True, sort_keys=True, indent=2),
                key=f"{strategy_id}_diagram_ast_risk_{index}_json",
            )
            try:
                parsed_rule = strategy_schema.parse_json_object(risk_text, "Risk rule JSON")
                rule = parsed_rule
            except Exception as exc:
                st.error(str(exc))

        if st.button("Remove risk rule", icon=icons.ICON_DELETE, key=f"{strategy_id}_diagram_ast_risk_{index}_remove"):
            rules.pop(index)
            removed_rule = True
        else:
            rules[index] = rule

    risk["rules"] = rules
    edited["risk"] = risk
    if rules != original_rules:
        autosaved_id = _autosave_strategy_definition(strategy_id, edited)
        st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
        if removed_rule:
            st.session_state.pop(_active_block_key(strategy_id), None)
            try:
                del st.query_params["strategy_block"]
            except Exception:
                pass
        st.rerun()
    return edited


def _render_risk_rule_form(rule: dict, key_prefix: str) -> dict:
    rule = copy.deepcopy(rule if isinstance(rule, dict) else {})
    rule_type = str(rule.get("type", "stop_loss_pct") or "stop_loss_pct")
    if rule_type == "stop_loss_pct":
        rule["type"] = "stop_loss_pct"
        rule["pct"] = float(
            st.number_input(
                "Stop loss %",
                min_value=0.0,
                value=float(rule.get("pct", 0.0) or 0.0),
                width=125,
                step=0.5,
                key=f"{key_prefix}_pct",
            )
        )
        return rule
    if rule_type == "take_profit_pct":
        row = st.container(horizontal=True, vertical_alignment="bottom")
        rule["type"] = "take_profit_pct"
        rule["pct"] = float(
            row.number_input(
                "PnL %",
                min_value=0.0,
                value=float(rule.get("pct", 0.0) or 0.0),
                step=0.5,
                key=f"{key_prefix}_pct",
                width=125,
            )
        )
        rule["size_pct"] = float(
            row.number_input(
                "Amount %",
                min_value=0.0,
                max_value=100.0,
                value=float(rule.get("size_pct", 100.0) or 100.0),
                step=5.0,
                key=f"{key_prefix}_size",
                width=125,
            )
        )
        return rule
    if rule_type == "take_profit_r_multiple":
        row = st.container(horizontal=True, vertical_alignment="bottom")
        rule["type"] = "take_profit_r_multiple"
        rule["r_multiple"] = float(
            row.number_input(
                "R multiple",
                min_value=0.0,
                value=float(rule.get("r_multiple", 0.0) or 0.0),
                step=0.25,
                key=f"{key_prefix}_r_multiple",
                width=125,
            )
        )
        rule["size_pct"] = float(
            row.number_input(
                "Amount %",
                min_value=0.0,
                max_value=100.0,
                value=float(rule.get("size_pct", 100.0) or 100.0),
                step=5.0,
                key=f"{key_prefix}_size",
                width=125,
            )
        )
        return rule
    if rule_type == "atr_stop":
        row = st.container(horizontal=True, vertical_alignment="bottom")
        indicator = rule.get("indicator", {}) if isinstance(rule.get("indicator"), dict) else {}
        params = indicator.get("params", {}) if isinstance(indicator.get("params"), dict) else {}
        period = int(
            row.number_input(
                "ATR period",
                min_value=1,
                value=int(params.get("period", 14) or 14),
                step=1,
                key=f"{key_prefix}_atr_period",
                width=125,
            )
        )
        timeframe = str(indicator.get("timeframe", "current") or "current")
        rule["type"] = "atr_stop"
        rule["indicator"] = {
            "type": "indicator",
            "name": "ATR",
            "params": {"period": period},
            "timeframe": timeframe,
            "source": {"type": "price", "field": "Close", "timeframe": timeframe},
            "output": str(indicator.get("output", "value") or "value"),
        }
        rule["multiplier"] = float(
            row.number_input(
                "Multiplier",
                min_value=0.0,
                value=float(rule.get("multiplier", 1.8) or 1.8),
                step=0.1,
                key=f"{key_prefix}_multiplier",
                width=125,
            )
        )
        rule["activation_pnl_pct"] = float(
            row.number_input(
                "Activation PnL %",
                min_value=0.0,
                value=float(rule.get("activation_pnl_pct", 0.0) or 0.0),
                step=0.5,
                key=f"{key_prefix}_activation",
                width=125,
            )
        )
        return rule

    risk_text = st.text_area(
        "Risk rule JSON",
        value=json.dumps(rule, ensure_ascii=True, sort_keys=True, indent=2),
        key=f"{key_prefix}_json",
    )
    return strategy_schema.parse_json_object(risk_text, "Risk rule JSON")


def _default_risk_rule(rule_type: str) -> dict:
    if rule_type == "stop_loss_pct":
        return {"type": "stop_loss_pct", "pct": 10.0}
    if rule_type == "atr_stop":
        return {
            "type": "atr_stop",
            "indicator": {
                "type": "indicator",
                "name": "ATR",
                "params": {"period": 14},
                "timeframe": "current",
                "source": {"type": "price", "field": "Close", "timeframe": "current"},
                "output": "value",
            },
            "multiplier": 1.8,
            "activation_pnl_pct": 0.0,
        }
    if rule_type == "take_profit_pct":
        return {"type": "take_profit_pct", "pct": 10.0, "size_pct": 25.0}
    return {"type": "stop_loss_pct", "pct": 10.0}


def _risk_rules(definition: dict) -> list:
    risk = definition.get("risk", {}) if isinstance(definition.get("risk"), dict) else {}
    rules = risk.get("rules", []) if isinstance(risk.get("rules"), list) else []
    return rules


def _risk_rule_existing_index(rules: list, rule_type: str) -> int | None:
    return next(
        (
            idx
            for idx, rule in enumerate(rules)
            if isinstance(rule, dict) and rule.get("type") == rule_type and rule_type in {"stop_loss_pct", "atr_stop"}
        ),
        None,
    )


def _risk_dialog_button(host, definition: dict, strategy_id: str, label: str, rule_type: str, key_suffix: str):
    rules = _risk_rules(definition)
    existing_index = _risk_rule_existing_index(rules, rule_type)
    disabled = existing_index is not None
    if host.button(
        label,
        icon=icons.ICON_ADD,
        type="primary",
        key=f"{strategy_id}_{key_suffix}_{rule_type}",
        disabled=disabled,
        help=f"{label.replace('Add ', '')} already exists." if disabled else None,
        use_container_width=True,
    ):
        risk_rule_dialog(strategy_id, definition, rule_type, None)


@st.dialog("Strategy Condition")
def condition_dialog(
    strategy_id: str,
    definition: dict,
    group_name: str,
    index: int | None = None,
    operator_before: str = "",
):
    group = definition.get(group_name, {})
    joiner, rules = _group_key_and_rules(group)
    is_new = index is None
    rule_type = "exit" if group_name == "exit" else "entry"
    title_action = "Add" if is_new else "Edit"
    st.markdown(f"### {title_action} {rule_type.title()} Condition")

    selected_operator = operator_before if operator_before in {"all", "any"} else joiner
    if is_new and rules:
        selected_operator = st.segmented_control(
            "Operator before this condition",
            options=["all", "any"],
            default=selected_operator,
            format_func=lambda value: "AND" if value == "all" else "OR",
            key=f"{strategy_id}_{group_name}_dialog_operator_before",
        ) or selected_operator

    if is_new:
        selected_rule = _default_strategy_condition()
        key_index = "new"
    else:
        if index < 0 or index >= len(rules):
            st.warning("The selected condition no longer exists.")
            if st.button("Close", icon=icons.ICON_CANCEL, key=f"{strategy_id}_{group_name}_condition_missing_close"):
                _clear_active_block(strategy_id)
                st.rerun()
            return
        selected_rule = rules[index]
        key_index = str(index)

    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    edited_rule = render_rule_block(
        selected_rule,
        parameters,
        f"{strategy_id}_condition_dialog_{group_name}_{key_index}",
        rule_type,
        f"{rule_type.title()} Condition",
        show_remove=False,
        stacked=True,
    )

    actions = st.container(horizontal=True)
    if actions.button("Save", icon=icons.ICON_SAVE, type="primary", key=f"{strategy_id}_{group_name}_{key_index}_condition_save"):
        try:
            if is_new:
                edited = _insert_condition(definition, group_name, edited_rule, selected_operator)
            else:
                edited = _update_condition(definition, group_name, int(index), edited_rule)
            autosaved_id = _autosave_strategy_definition(strategy_id, edited)
            _, updated_rules = _group_key_and_rules(edited.get(group_name, {}))
            target_index = len(updated_rules) - 1 if is_new else int(index)
            st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
            st.session_state[f"{autosaved_id}_last_saved_diagram_block"] = {
                "kind": "rule",
                "group": group_name,
                "index": target_index,
            }
            _clear_active_block(autosaved_id)
            st.rerun()
        except Exception as exc:
            st.error(f"Save failed: {exc}")

    if not is_new and actions.button("Remove", icon=icons.ICON_DELETE, key=f"{strategy_id}_{group_name}_{key_index}_condition_remove"):
        try:
            edited = _remove_condition(definition, group_name, int(index))
            autosaved_id = _autosave_strategy_definition(strategy_id, edited)
            st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
            _clear_active_block(autosaved_id)
            st.rerun()
        except Exception as exc:
            st.error(f"Remove failed: {exc}")

    if actions.button("Cancel", icon=icons.ICON_CANCEL, key=f"{strategy_id}_{group_name}_{key_index}_condition_cancel"):
        if not is_new:
            _clear_active_block(strategy_id)
        st.rerun()


@st.dialog("Risk Rule")
def risk_rule_dialog(
    strategy_id: str,
    definition: dict,
    rule_type: str,
    index: int | None = None,
):
    rules = _risk_rules(definition)
    is_new = index is None
    if is_new:
        rule = _default_risk_rule(rule_type)
    else:
        if index < 0 or index >= len(rules):
            st.warning("The selected risk rule no longer exists.")
            if st.button("Close", icon=icons.ICON_CANCEL, key=f"{strategy_id}_risk_missing_close"):
                _clear_active_block(strategy_id)
                st.rerun()
            return
        rule = copy.deepcopy(rules[index])
        rule_type = str(rule.get("type", rule_type) or rule_type)

    title = f"{'Add' if is_new else 'Edit'} {rule_type.replace('_', ' ').title()}"
    st.markdown(f"### {title}")
    try:
        edited_rule = _render_risk_rule_form(
            rule,
            f"{strategy_id}_risk_dialog_{index if index is not None else 'new'}_{rule_type}",
        )
    except Exception as exc:
        st.error(str(exc))
        return

    actions = st.container(horizontal=True)
    if actions.button("Save", icon=icons.ICON_SAVE, type="primary", key=f"{strategy_id}_risk_{index}_save"):
        try:
            edited = _upsert_risk_rule(definition, edited_rule, index)
            autosaved_id = _autosave_strategy_definition(strategy_id, edited)
            updated_rules = _risk_rules(edited)
            target_index = len(updated_rules) - 1 if is_new else int(index)
            st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
            st.session_state[f"{autosaved_id}_last_saved_diagram_block"] = {
                "kind": "ast_risk",
                "index": target_index,
            }
            _clear_active_block(autosaved_id)
            st.rerun()
        except Exception as exc:
            st.error(f"Save failed: {exc}")

    if not is_new and actions.button("Remove", icon=icons.ICON_DELETE, key=f"{strategy_id}_risk_{index}_remove"):
        try:
            edited = _remove_risk_rule(definition, int(index))
            autosaved_id = _autosave_strategy_definition(strategy_id, edited)
            st.session_state[_ai_state_key(autosaved_id, "definition")] = copy.deepcopy(edited)
            _clear_active_block(autosaved_id)
            st.rerun()
        except Exception as exc:
            st.error(f"Remove failed: {exc}")

    if actions.button("Cancel", icon=icons.ICON_CANCEL, key=f"{strategy_id}_risk_{index}_cancel"):
        if not is_new:
            _clear_active_block(strategy_id)
        st.rerun()


def render_diagram_click_editor(definition: dict, risk: dict, strategy_id: str) -> tuple[dict, dict]:
    edited_definition = copy.deepcopy(definition)
    edited_risk = copy.deepcopy(risk)
    active_block = _get_active_block(strategy_id)
    if not active_block:
        try:
            edited_risk = strategy_schema.extract_execution_risk(edited_definition)
        except Exception:
            pass
        return edited_definition, edited_risk

    block_kind = active_block.get("kind")
    if block_kind == "rule":
        group_name = str(active_block.get("group", "entry"))
        condition_dialog(
            strategy_id,
            edited_definition,
            group_name,
            _block_index(active_block),
        )
    elif block_kind == "ast_risk":
        rules = (
            edited_definition.get("risk", {}).get("rules", [])
            if isinstance(edited_definition.get("risk"), dict)
            else []
        )
        index = _block_index(active_block)
        if isinstance(rules, list) and 0 <= index < len(rules):
            rule = rules[index] if isinstance(rules[index], dict) else {}
            risk_rule_dialog(
                strategy_id,
                edited_definition,
                str(rule.get("type", "risk")),
                index,
            )
        else:
            st.warning("The selected risk rule no longer exists.")
    elif block_kind in {"risk_empty", "risk"}:
        risk_rule_dialog(strategy_id, edited_definition, "stop_loss_pct", None)
    elif block_kind == "joiner":
        _clear_active_block(strategy_id)

    try:
        edited_risk = strategy_schema.extract_execution_risk(edited_definition)
    except Exception:
        pass

    return edited_definition, edited_risk


def _normalize_operand(operand: dict, parameters: dict) -> dict:
    operand = operand if isinstance(operand, dict) else {}
    operand_type = operand.get("type")
    if operand_type in OPERAND_TYPES:
        return copy.deepcopy(operand)
    first_parameter = next(iter(parameters.keys()), "")
    if first_parameter:
        return {"type": "parameter", "name": first_parameter}
    return {"type": "price", "field": "Close"}


def render_operand_editor(operand: dict, parameters: dict, key_prefix: str, host=st) -> dict:
    operand = _normalize_operand(operand, parameters)
    parameter_names = list(parameters.keys())
    operand_type = host.selectbox(
        "Type",
        list(OPERAND_TYPES.keys()),
        index=list(OPERAND_TYPES.keys()).index(operand.get("type", "price")),
        format_func=lambda value: OPERAND_TYPES[value],
        key=f"{key_prefix}_type",
        label_visibility="collapsed",
    )

    if operand_type == "indicator":
        name = str(operand.get("name", "EMA")).upper()
        if name not in INDICATOR_OPTIONS:
            name = "EMA"
        params = operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
        timeframe = str(operand.get("timeframe", "current") or "current")
        if timeframe not in TIMEFRAME_OPTIONS:
            timeframe = "current"
        name_col, period_col, timeframe_col = host.columns([1.2, 1, 1.2])
        selected_name = name_col.selectbox(
            "Indicator",
            INDICATOR_OPTIONS,
            index=INDICATOR_OPTIONS.index(name),
            key=f"{key_prefix}_indicator_name",
            label_visibility="collapsed",
        )
        selected_timeframe = timeframe_col.selectbox(
            "Timeframe",
            TIMEFRAME_OPTIONS,
            index=TIMEFRAME_OPTIONS.index(timeframe),
            format_func=_timeframe_option_label,
            key=f"{key_prefix}_indicator_timeframe",
            label_visibility="collapsed",
        )
        result = {
            "type": "indicator",
            "name": selected_name,
            "source": {"type": "price", "field": "Close", "timeframe": selected_timeframe},
            "timeframe": selected_timeframe,
            "params": {},
            "output": str(operand.get("output", "value") or "value"),
        }
        period_config = _indicator_period_selector(period_col, operand, parameters, key_prefix)
        if "period_param" in period_config:
            result["period_param"] = period_config["period_param"]
        else:
            result["params"]["period"] = period_config["period"]
        return result

    if operand_type == "price":
        field = str(operand.get("field", "Close"))
        if field not in PRICE_FIELD_OPTIONS:
            field = "Close"
        timeframe = str(operand.get("timeframe", "current") or "current")
        if timeframe not in TIMEFRAME_OPTIONS:
            timeframe = "current"
        field_col, timeframe_col = host.columns([1, 1.2])
        return {
            "type": "price",
            "field": field_col.selectbox(
                "Price",
                PRICE_FIELD_OPTIONS,
                index=PRICE_FIELD_OPTIONS.index(field),
                key=f"{key_prefix}_price_field",
                label_visibility="collapsed",
            ),
            "timeframe": timeframe_col.selectbox(
                "Timeframe",
                TIMEFRAME_OPTIONS,
                index=TIMEFRAME_OPTIONS.index(timeframe),
                format_func=_timeframe_option_label,
                key=f"{key_prefix}_price_timeframe",
                label_visibility="collapsed",
            ),
        }

    if operand_type == "parameter":
        fallback = parameter_names[0] if parameter_names else ""
        current = str(operand.get("name", fallback))
        if current not in parameter_names and parameter_names:
            current = fallback
        if not parameter_names:
            host.caption("No parameters available.")
            return {"type": "value", "value": 0.0}
        return {
            "type": "parameter",
            "name": host.selectbox(
                "Parameter",
                parameter_names,
                index=parameter_names.index(current),
                key=f"{key_prefix}_parameter_name",
                label_visibility="collapsed",
            ),
        }

    return {
        "type": "value",
        "value": float(
            host.number_input(
                "Value",
                value=float(operand.get("value", 0.0) or 0.0),
                step=0.1,
                key=f"{key_prefix}_value",
                label_visibility="collapsed",
            )
        ),
    }


def render_operand_editor_inline(operand: dict, parameters: dict, key_prefix: str, host, *, allow_value: bool = True) -> dict:
    operand = _normalize_operand(operand, parameters)
    parameter_names = list(parameters.keys())
    operand_type_options = [option for option in OPERAND_TYPES.keys() if option != "parameter"]
    if not allow_value:
        operand_type_options = [option for option in operand_type_options if option != "value"]
    if operand.get("type") not in operand_type_options:
        operand = {"type": "price", "field": "Close", "timeframe": "current"}
    operand_type = host.selectbox(
        "Type",
        operand_type_options,
        index=operand_type_options.index(operand.get("type", "price")),
        format_func=lambda value: OPERAND_TYPES[value],
        key=f"{key_prefix}_type",
        label_visibility="collapsed",
        width=145,
    )

    if operand_type == "indicator":
        name = str(operand.get("name", "EMA")).upper()
        if name not in INDICATOR_OPTIONS:
            name = "EMA"
        params = operand.get("params", {}) if isinstance(operand.get("params"), dict) else {}
        timeframe = str(operand.get("timeframe", "current") or "current")
        if timeframe not in TIMEFRAME_OPTIONS:
            timeframe = "current"
        selected_name = host.selectbox(
            "Indicator",
            INDICATOR_OPTIONS,
            index=INDICATOR_OPTIONS.index(name),
            key=f"{key_prefix}_indicator_name",
            label_visibility="collapsed",
            width=125,
        )
        period_config = _indicator_period_selector(host, operand, parameters, key_prefix, selector_width=125, value_width=95)
        selected_timeframe = host.selectbox(
            "Timeframe",
            TIMEFRAME_OPTIONS,
            index=TIMEFRAME_OPTIONS.index(timeframe),
            format_func=_timeframe_option_label,
            key=f"{key_prefix}_indicator_timeframe",
            label_visibility="collapsed",
            width=125,
        )
        result = {
            "type": "indicator",
            "name": selected_name,
            "source": {"type": "price", "field": "Close", "timeframe": selected_timeframe},
            "timeframe": selected_timeframe,
            "params": {},
            "output": str(operand.get("output", "value") or "value"),
        }
        if "period_param" in period_config:
            result["period_param"] = period_config["period_param"]
        else:
            result["params"]["period"] = period_config["period"]
        return result

    if operand_type == "price":
        field = str(operand.get("field", "Close"))
        if field not in PRICE_FIELD_OPTIONS:
            field = "Close"
        timeframe = str(operand.get("timeframe", "current") or "current")
        if timeframe not in TIMEFRAME_OPTIONS:
            timeframe = "current"
        return {
            "type": "price",
            "field": host.selectbox(
                "Price",
                PRICE_FIELD_OPTIONS,
                index=PRICE_FIELD_OPTIONS.index(field),
                key=f"{key_prefix}_price_field",
                label_visibility="collapsed",
                width=125,
            ),
            "timeframe": host.selectbox(
                "Timeframe",
                TIMEFRAME_OPTIONS,
                index=TIMEFRAME_OPTIONS.index(timeframe),
                format_func=_timeframe_option_label,
                key=f"{key_prefix}_price_timeframe",
                label_visibility="collapsed",
                width=125,
            ),
        }

    if operand_type == "parameter":
        fallback = parameter_names[0] if parameter_names else ""
        current = str(operand.get("name", fallback))
        if current not in parameter_names and parameter_names:
            current = fallback
        if not parameter_names:
            host.caption("No parameters")
            return {"type": "value", "value": 0.0}
        return {
            "type": "parameter",
            "name": host.selectbox(
                "Parameter",
                parameter_names,
                index=parameter_names.index(current),
                key=f"{key_prefix}_parameter_name",
                label_visibility="collapsed",
                width=160,
            ),
        }

    return {
        "type": "value",
        "value": float(
            host.number_input(
                "Value",
                value=float(operand.get("value", 0.0) or 0.0),
                step=0.1,
                key=f"{key_prefix}_value",
                label_visibility="collapsed",
                width=125,
            )
        ),
    }


def render_rule_block(
    rule: dict,
    parameters: dict,
    key_prefix: str,
    rule_type: str,
    title_prefix: str,
    *,
    show_remove: bool = True,
    stacked: bool = False,
) -> dict | None:
    dot_class = "bec-dot-exit" if rule_type == "exit" else "bec-dot-entry"
    summary = _rule_chips_html(rule, parameters)
    with st.container(border=True):
        st.markdown(
            f"""
            <div class="bec-rule-title">
                <span class="{dot_class}"></span>
                <span>{title_prefix}</span>
                <span class="bec-rule-summary">{summary}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        row = st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center", gap="small")
        row.markdown("**IF**", width=40)
        left = render_operand_editor_inline(
            rule.get("left", {}),
            parameters,
            f"{key_prefix}_left",
            row,
            allow_value=False,
        )
        if stacked:
            operator_host = st.container(horizontal=True, vertical_alignment="bottom", horizontal_alignment="center", gap="small")
            right_host = st.container(horizontal=True, vertical_alignment="bottom", horizontal_alignment="center", gap="small")
        else:
            operator_host = row
        operator_keys = list(OPERATORS.keys())
        operator = str(rule.get("operator", "greater_than"))
        if operator not in operator_keys:
            operator = "greater_than"
        selected_operator = operator_host.selectbox(
            "Operator",
            operator_keys,
            index=operator_keys.index(operator),
            format_func=lambda value: OPERATORS[value],
            key=f"{key_prefix}_operator",
            label_visibility="collapsed",
            width=170,
        )
        right = render_operand_editor_inline(
            rule.get("right", {}),
            parameters,
            f"{key_prefix}_right",
            right_host if stacked else operator_host,
        )
        button_host = operator_host if stacked else row
        if show_remove and button_host.button("Remove", icon=icons.ICON_DELETE, key=f"{key_prefix}_remove"):
            return None
        return {"left": left, "operator": selected_operator, "right": right}


def render_rule_group_editor(definition: dict, group_name: str, title: str, key_prefix: str, rule_type: str) -> dict:
    group = copy.deepcopy(definition.get(group_name, {"all": []}))
    parameters = definition.get("parameters", {}) if isinstance(definition.get("parameters"), dict) else {}
    joiner = "all" if "all" in group else "any"
    rules = group.get(joiner, [])
    st.markdown('<div class="bec-block-section">', unsafe_allow_html=True)
    st.markdown(f'<div class="bec-block-section-title">{title}</div>', unsafe_allow_html=True)
    selected_joiner = st.segmented_control(
        "Joiner",
        options=["all", "any"],
        default=joiner,
        format_func=lambda value: "AND" if value == "all" else "OR",
        key=f"{key_prefix}_joiner",
        label_visibility="collapsed",
    )
    selected_joiner = selected_joiner or joiner
    edited_rules = []
    for idx, rule in enumerate(rules):
        if idx > 0:
            st.markdown(
                f'<span class="bec-joiner">{"AND" if selected_joiner == "all" else "OR"}</span>',
                unsafe_allow_html=True,
            )
        edited_rule = render_rule_block(
            rule,
            parameters,
            f"{key_prefix}_{idx}",
            rule_type,
            f"{title} {idx + 1}",
        )
        if edited_rule is not None:
            edited_rules.append(edited_rule)
    if st.button("Add condition", icon=icons.ICON_ADD, key=f"{key_prefix}_add"):
        edited_rules.append(
            {
                "left": {"type": "price", "field": "Close"},
                "operator": "greater_than",
                "right": {"type": "value", "value": 0.0},
            }
        )
    st.markdown("</div>", unsafe_allow_html=True)
    return {selected_joiner: edited_rules}


def render_strategy_block_editor(definition: dict, selected_id: str) -> dict:
    edited = copy.deepcopy(definition)
    st.markdown("### Strategy Blocks")
    if edited.get("description"):
        st.info(str(edited.get("description")))
    edited["filters"] = render_rule_group_editor(
        edited,
        "filters",
        "Entry Filters",
        f"{selected_id}_filters",
        "entry",
    )
    edited["entry_rules"] = render_rule_group_editor(
        edited,
        "entry_rules",
        "Entry",
        f"{selected_id}_entry",
        "entry",
    )
    edited["exit_rules"] = render_rule_group_editor(
        edited,
        "exit_rules",
        "Exit",
        f"{selected_id}_exit",
        "exit",
    )
    return edited


def _format_parameter_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parameters_to_dataframe(parameters: dict) -> pd.DataFrame:
    rows = []
    if isinstance(parameters, dict):
        for name, spec in parameters.items():
            spec = spec if isinstance(spec, dict) else {}
            value_type = str(spec.get("type", "float") or "float")
            if value_type not in PARAMETER_TYPES:
                value_type = "float"
            rows.append(
                {
                    "Name": str(name),
                    "Type": value_type,
                    "Default": _format_parameter_value(spec.get("default")),
                    "Min": _format_parameter_value(spec.get("min")),
                    "Max": _format_parameter_value(spec.get("max")),
                    "Step": _format_parameter_value(spec.get("step")),
                    "Optimizable": bool(spec.get("optimizable", False)),
                }
            )
    return pd.DataFrame(rows, columns=["Name", "Type", "Default", "Min", "Max", "Step", "Optimizable"])


def _parse_parameter_value(value, value_type: str, fallback):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        value = fallback
    text = str(value).strip()
    if text == "":
        text = str(fallback)
    if value_type == "bool":
        return text.lower() in {"1", "true", "yes", "y", "on"}
    try:
        number = float(text)
    except (TypeError, ValueError):
        number = float(fallback or 0)
    if value_type == "int":
        return int(number)
    return float(number)


def _parameters_from_dataframe(df: pd.DataFrame) -> dict:
    parameters = {}
    if df is None or df.empty:
        return parameters
    for row in df.to_dict("records"):
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        value_type = str(row.get("Type", "float") or "float")
        if value_type not in PARAMETER_TYPES:
            value_type = "float"
        default_fallback = False if value_type == "bool" else 0
        spec = {
            "type": value_type,
            "default": _parse_parameter_value(row.get("Default"), value_type, default_fallback),
            "optimizable": bool(row.get("Optimizable", False)),
        }
        if value_type != "bool":
            default_value = spec["default"]
            minimum = _parse_parameter_value(row.get("Min"), value_type, default_value)
            maximum = _parse_parameter_value(row.get("Max"), value_type, default_value)
            step = _parse_parameter_value(row.get("Step"), value_type, 1)
            if maximum < minimum:
                minimum, maximum = maximum, minimum
            if float(step or 0) <= 0:
                step = 1 if value_type == "int" else 1.0
            spec.update({"min": minimum, "max": maximum, "step": step})
        parameters[name] = spec
    return parameters


def _parameter_constraints_to_dataframe(constraints) -> pd.DataFrame:
    rows = []
    if isinstance(constraints, list):
        for rule in constraints:
            if not isinstance(rule, dict):
                continue
            operator = str(rule.get("operator", "less_than") or "less_than")
            if operator not in PARAMETER_CONSTRAINT_OPERATOR_OPTIONS:
                operator = "less_than"
            rows.append(
                {
                    "Left": str(rule.get("left", "") or ""),
                    "Operator": operator,
                    "Right": str(rule.get("right", "") or ""),
                }
            )
    return pd.DataFrame(rows, columns=["Left", "Operator", "Right"])


def _parameter_constraints_from_dataframe(df: pd.DataFrame, parameter_names: list[str]) -> list[dict]:
    constraints = []
    allowed_names = {str(name).strip() for name in parameter_names if str(name).strip()}
    if df is None or df.empty:
        return constraints
    for row in df.to_dict("records"):
        left = str(row.get("Left", "") or "").strip()
        right = str(row.get("Right", "") or "").strip()
        operator = str(row.get("Operator", "less_than") or "less_than").strip()
        if not left or not right:
            continue
        if left not in allowed_names or right not in allowed_names:
            continue
        if operator not in PARAMETER_CONSTRAINT_OPERATOR_OPTIONS:
            operator = "less_than"
        constraints.append({"left": left, "operator": operator, "right": right})
    return constraints


def render_parameter_constraints_editor(
    definition: dict,
    strategy_id: str,
    parameter_names: list[str],
) -> list[dict]:
    st.markdown("#### Parameter rules")
    st.caption(
        "Optional rules between parameters during optimization (for example, fast period < slow period). "
        "Invalid combinations are skipped, reducing backtest time."
    )
    constraints = definition.get("parameter_constraints", [])
    constraints_df = _parameter_constraints_to_dataframe(constraints)
    name_options = sorted({str(name).strip() for name in parameter_names if str(name).strip()})
    column_config = {
        "Left": st.column_config.SelectboxColumn(
            "Left",
            options=name_options or [""],
            required=True,
            width=160,
        ),
        "Operator": st.column_config.SelectboxColumn(
            "Operator",
            options=PARAMETER_CONSTRAINT_OPERATOR_OPTIONS,
            required=True,
            width=170,
            format_func=lambda value: PARAMETER_CONSTRAINT_OPERATOR_LABELS.get(value, value),
        ),
        "Right": st.column_config.SelectboxColumn(
            "Right",
            options=name_options or [""],
            required=True,
            width=160,
        ),
    }
    if not name_options:
        st.info("Add parameters above before defining parameter rules.")
        return []
    edited_df = st.data_editor(
        constraints_df,
        num_rows="dynamic",
        hide_index=True,
        width="content",
        key=f"{strategy_id}_parameter_constraints_editor",
        column_config=column_config,
    )
    return _parameter_constraints_from_dataframe(edited_df, name_options)


def render_optimization_parameters_editor(definition: dict, strategy_id: str) -> dict:
    edited = copy.deepcopy(definition)
    st.markdown("### Optimization Parameters")
    st.caption("Define default values and the ranges that can be used by backtest optimization.")
    current_parameters = edited.get("parameters", {}) if isinstance(edited.get("parameters"), dict) else {}
    params_df = _parameters_to_dataframe(current_parameters)
    edited_df = st.data_editor(
        params_df,
        num_rows="dynamic",
        hide_index=True,
        # width="stretch",
        width="content",
        key=f"{strategy_id}_optimization_parameters_editor",
        column_config={
            "Name": st.column_config.TextColumn("Name", required=True, width=180),
            "Type": st.column_config.SelectboxColumn("Type", options=PARAMETER_TYPES, required=True, width=95),
            "Default": st.column_config.TextColumn("Default", required=True, width=110),
            "Min": st.column_config.TextColumn("Min", width=100),
            "Max": st.column_config.TextColumn("Max", width=100),
            "Step": st.column_config.TextColumn("Step", width=90),
            "Optimizable": st.column_config.CheckboxColumn("Optimizable", width=120),
        },
    )
    edited["parameters"] = _parameters_from_dataframe(edited_df)
    parameter_names = list(edited["parameters"].keys())
    edited["parameter_constraints"] = render_parameter_constraints_editor(
        edited,
        strategy_id,
        parameter_names,
    )
    return edited


def _ai_state_key(strategy_id: str, name: str) -> str:
    return f"{strategy_id}_ai_builder_{name}"


def _apply_ai_result_to_state(strategy_id: str, result: dict):
    st.session_state[_ai_state_key(strategy_id, "definition")] = result["definition"]
    st.session_state[_ai_state_key(strategy_id, "risk")] = result["risk"]
    st.session_state[_ai_state_key(strategy_id, "suggested_name")] = result["strategy_name"]


def _get_ai_override(strategy_id: str, current_definition: dict, current_risk: dict) -> tuple[dict, dict]:
    definition = st.session_state.get(_ai_state_key(strategy_id, "definition"))
    risk = st.session_state.get(_ai_state_key(strategy_id, "risk"))
    return (
        definition if isinstance(definition, dict) else current_definition,
        risk if isinstance(risk, dict) else current_risk,
    )


def _autosave_strategy_definition(strategy_id: str, definition: dict) -> str:
    definition = strategy_schema.validate_definition(definition)
    df_strategy = database.get_strategy_by_id(strategy_id)
    if df_strategy.empty:
        return strategy_id
    row = df_strategy.iloc[0]
    metadata = strategy_schema.parse_json_object(row.get("Metadata_JSON"), "Metadata_JSON")
    status = str(row.get("Status", "draft") or "draft")
    if status == "approved":
        new_id = database.create_strategy_draft_version(strategy_id, definition, metadata=metadata)
        st.session_state["strategy_builder_selected_id"] = new_id
        for suffix in ("definition", "risk", "suggested_name"):
            st.session_state.pop(_ai_state_key(strategy_id, suffix), None)
        st.toast(f"Auto-saved as new draft: {new_id}")
        st.rerun()
    database.upsert_custom_strategy(
        strategy_id=strategy_id,
        name=str(row.get("Name") or strategy_id),
        definition=definition,
        metadata=metadata,
        status="draft",
        parent_strategy_id=str(row.get("Parent_Strategy_Id") or ""),
        version=int(row.get("Version", 1) or 1),
        main_strategy=True,
        btc_strategy=False,
        backtest_optimize=_definition_has_optimizable_parameters(definition),
    )
    st.toast("Condition auto-saved.")
    return strategy_id


def render_ai_chat_styles():
    st.markdown(
        """
        <style>
        .bec-ai-chat-row {
            display: flex;
            width: 100%;
            margin: 0.62rem 0;
        }
        .bec-ai-chat-row.assistant {
            justify-content: flex-start;
        }
        .bec-ai-chat-row.user {
            justify-content: flex-end;
        }
        .bec-ai-chat-bubble {
            max-width: min(78%, 980px);
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 10px;
            padding: 0.72rem 0.9rem;
            line-height: 1.55;
            font-size: 0.95rem;
            overflow-wrap: anywhere;
            white-space: normal;
        }
        .bec-ai-chat-row.assistant .bec-ai-chat-bubble {
            background: #ffffff;
            color: #1f2937;
            border-color: rgba(49, 51, 63, 0.14);
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .bec-ai-chat-row.user .bec-ai-chat-bubble {
            background: #f3f6fb;
            color: #111827;
            border-color: rgba(99, 102, 241, 0.10);
        }
        .bec-ai-chat-bubble p {
            margin: 0.2rem 0;
        }
        .bec-ai-chat-bubble code {
            font-size: 0.88em;
            background: rgba(15, 23, 42, 0.06);
            padding: 0.08rem 0.24rem;
            border-radius: 4px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _chat_message_html(role: str, content: str) -> str:
    safe_role = "user" if role == "user" else "assistant"
    escaped = html.escape(str(content or "")).replace("\n", "<br>")
    return (
        f'<div class="bec-ai-chat-row {safe_role}">'
        f'<div class="bec-ai-chat-bubble">{escaped}</div>'
        "</div>"
    )


def render_ai_chat_message(role: str, content: str, target=None):
    renderer = target.markdown if target is not None else st.markdown
    renderer(_chat_message_html(role, content), unsafe_allow_html=True)


def _stream_chunks(content: str, chunk_size: int = 8):
    text = str(content or "")
    for index in range(0, len(text), chunk_size):
        yield text[: index + chunk_size]


def stream_ai_chat_message(role: str, content: str):
    placeholder = st.empty()
    for partial in _stream_chunks(content):
        render_ai_chat_message(role, partial, target=placeholder)
        time.sleep(0.008)
    render_ai_chat_message(role, content, target=placeholder)


def _is_ai_chat_welcome_message(content: str) -> bool:
    return str(content or "").strip() in AI_CHAT_WELCOME_MESSAGES


def render_ai_strategy_chat(strategy_id: str, current_definition: dict, current_risk: dict):
    st.markdown("### AI Strategy Assistant")
    st.caption(
        "Describe the strategy or block you want to change. You can chat in English or Portuguese; "
        "the assistant replies in the same language as your message. Responses are validated before updating the draft."
    )
    render_ai_chat_styles()

    messages_key = _ai_state_key(strategy_id, "messages")
    if messages_key not in st.session_state:
        st.session_state[messages_key] = [
            {
                "role": "assistant",
                "content": AI_CHAT_WELCOME_MESSAGE,
            }
        ]

    suggestions = st.container(horizontal=True)
    for label, prompt in AI_STRATEGY_EXAMPLES.items():
        if suggestions.button(label, key=f"{strategy_id}_ai_suggestion_{label}"):
            st.session_state[_ai_state_key(strategy_id, "pending_prompt")] = prompt
            st.rerun()

    with st.container(border=True):
        messages_area = st.container()
        with messages_area:
            for message in st.session_state[messages_key]:
                render_ai_chat_message(message["role"], message["content"])

        user_prompt = st.chat_input(
            "Describe or refine your strategy (English or Portuguese)...",
            key=f"{strategy_id}_ai_chat_input",
        )

    pending_prompt_key = _ai_state_key(strategy_id, "pending_prompt")
    pending_prompt = st.session_state.pop(pending_prompt_key, None)
    prompt = pending_prompt or user_prompt
    if not prompt:
        return

    prior_messages = list(st.session_state[messages_key])
    st.session_state[messages_key].append({"role": "user", "content": prompt})
    with messages_area:
        render_ai_chat_message("user", prompt)

    prompt_classification = ai_builder.classify_strategy_prompt(prompt)
    useful_context_messages = [
        str(message.get("content", ""))
        for message in prior_messages
        if message.get("role") == "assistant"
    ]
    has_context = any(
        content
        and content not in ai_builder.STRATEGY_PROMPT_REFUSALS.values()
        and not _is_ai_chat_welcome_message(content)
        for content in useful_context_messages
    )
    if prompt_classification["action"] == "block" or (prompt_classification["action"] == "defer" and not has_context):
        assistant_message = prompt_classification["message"]
        st.session_state[messages_key].append({"role": "assistant", "content": assistant_message})
        with messages_area:
            stream_ai_chat_message("assistant", assistant_message)
        return

    model = os.getenv("BEC_OPENAI_MODEL", ai_strategy_analysis.DEFAULT_MODEL)
    try:
        with st.status("Generating strategy schema...", expanded=True) as status:
            result = ai_builder.build_strategy_with_openai(
                prompt,
                current_definition=current_definition,
                current_risk=current_risk,
                chat_history=prior_messages + [{"role": "user", "content": prompt}],
                model=model,
            )
            status.update(
                label="Response generated and validated.",
                state="complete",
            )
    except Exception as exc:
        error_message = str(exc)
        assistant_message = f"Could not generate strategy: {error_message}"
        st.session_state[messages_key].append({"role": "assistant", "content": assistant_message})
        with messages_area:
            stream_ai_chat_message("assistant", assistant_message)
        return

    assistant_message = result["assistant_message"]
    st.session_state[messages_key].append({"role": "assistant", "content": assistant_message})
    with messages_area:
        stream_ai_chat_message("assistant", assistant_message)
        if result.get("response_type") == "update_strategy":
            _apply_ai_result_to_state(strategy_id, result)
            st.success(
                "Definition_JSON updated in the draft preview. Review the diagram and save the draft."
            )
    if result.get("response_type") == "update_strategy":
        time.sleep(0.5)
        st.rerun()


def _load_strategies():
    df = database.get_all_strategies()
    if df.empty:
        return df
    return df.sort_values(["Type", "Name"], kind="stable")


def _run_backtest(strategy_id: str, symbol: str, timeframe: str, optimize: bool = False) -> bool:
    strategy_id = str(strategy_id).strip()
    symbol = str(symbol).strip().upper()
    timeframe = str(timeframe).strip()
    backtesting_script = os.path.abspath(my_backtesting.__file__)
    project_root = os.path.dirname(os.path.dirname(backtesting_script))
    command = [
        sys.executable,
        "-m",
        "bec.my_backtesting",
        "--symbol",
        symbol,
        "--timeframe",
        timeframe,
        "--strategy",
        strategy_id,
    ]
    if optimize:
        command.append("--optimize")
    optimize_suffix = " optimize" if optimize else ""
    with st.status(f"Running backtest{optimize_suffix}: {strategy_id} - {symbol} - {timeframe}", expanded=True) as status:
        output_placeholder = st.empty()
        process = subprocess.Popen(
            command,
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        output = []
        for line in process.stdout:
            output.append(line)
            output_placeholder.code("".join(output)[-50000:], language="text")
        result = process.wait() == 0
        if result:
            database.mark_strategy_backtested(strategy_id)
            status.update(label="Backtest finished. Strategy marked as backtested.", state="complete")
        else:
            status.update(label="Backtest failed. Check output above.", state="error")
        return result


@st.dialog("Clone Strategy")
def clone_strategy_dialog(source_strategy_id: str):
    source = database.get_strategy_by_id(source_strategy_id)
    if source.empty:
        st.error("Strategy not found.")
        return

    row = source.iloc[0]
    source_name = str(row.get("Name") or source_strategy_id)
    st.write(f"Clone **{source_name}**.")
    clone_name = st.text_input(
        "Name",
        value=database.get_next_strategy_clone_name(source_strategy_id),
        key=f"clone_dialog_name_{source_strategy_id}",
    )
    actions = st.container(horizontal=True)
    if actions.button("Clone", icon=icons.ICON_ADD, type="primary", key=f"clone_dialog_submit_{source_strategy_id}"):
        if not str(clone_name or "").strip():
            st.error("Name is required.")
            return
        new_id = database.clone_strategy(source_strategy_id, clone_name)
        st.session_state["strategy_builder_selected_id"] = new_id
        st.success(f"Created draft strategy: {new_id}")
        time.sleep(0.5)
        st.rerun()
    if actions.button("Cancel", icon=icons.ICON_CANCEL, key=f"clone_dialog_cancel_{source_strategy_id}"):
        st.rerun()


@st.dialog("New Strategy")
def new_strategy_dialog():
    strategy_name = st.text_input(
        "Name",
        value="",
        placeholder="My Strategy",
        key="new_strategy_name",
    )
    creation_mode = st.segmented_control(
        "Start with",
        options=["Blank diagram", "AI assistant"],
        default="Blank diagram",
        key="new_strategy_mode",
    )
    prompt = ""
    if creation_mode == "AI assistant":
        prompt = st.text_area(
            "Initial prompt",
            placeholder="Describe entry, exit and risk rules...",
            key="new_strategy_ai_prompt",
            height=140,
        )
    actions = st.container(horizontal=True)
    if actions.button("Create", icon=icons.ICON_ADD, type="primary", key="new_strategy_submit"):
        if not str(strategy_name or "").strip():
            st.error("Name is required.")
            return
        try:
            new_id = database.create_custom_strategy(
                strategy_name,
                definition=strategy_templates.get_empty_strategy_template(strategy_name),
            )
            if creation_mode == "AI assistant" and str(prompt or "").strip():
                st.session_state[_ai_state_key(new_id, "pending_prompt")] = str(prompt).strip()
            st.session_state["strategy_builder_selected_id"] = new_id
            st.success(f"Created draft strategy: {new_id}")
            time.sleep(0.5)
            st.rerun()
        except Exception as exc:
            st.error(f"Create failed: {exc}")
    if actions.button("Cancel", icon=icons.ICON_CANCEL, key="new_strategy_cancel"):
        st.rerun()


def render_templates(df: pd.DataFrame):
    st.subheader("Built-in Templates")
    st.caption(
        "Official approved read-only templates. They run from Definition_JSON; clone one to create an editable strategy."
    )
    builtins = df[df["Type"].fillna("builtin") == "builtin"] if not df.empty else pd.DataFrame()
    if builtins.empty:
        st.info("No built-in strategies found.")
        return

    grid_columns = [
        "Id",
        "Name",
        "Status",
        "Backtest_Optimize",
        "Main_Strategy",
        "BTC_Strategy",
        "Updated_At",
    ]
    display_columns = [column for column in grid_columns if column in builtins.columns]
    grid_df = builtins[display_columns].copy()

    signature = int(
        pd.util.hash_pandas_object(
            grid_df[[column for column in ["Id", "Name", "Status"] if column in grid_df.columns]].astype(str),
            index=False,
        ).sum()
    )
    dataframe_event = st.dataframe(
        grid_df,
        width="content",
        hide_index=True,
        key=f"strategy_templates_grid_{len(grid_df)}_{signature}",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Id": st.column_config.TextColumn("Id"),
            "Name": st.column_config.TextColumn("Strategy"),
            "Status": st.column_config.TextColumn("Status"),
            "Backtest_Optimize": st.column_config.CheckboxColumn("Optimize"),
            "Main_Strategy": st.column_config.CheckboxColumn("Main"),
            "BTC_Strategy": st.column_config.CheckboxColumn("BTC"),
            "Updated_At": st.column_config.TextColumn("Updated"),
        },
    )

    selected_rows = dataframe_event.selection.rows
    valid_selected_rows = [
        row_index for row_index in selected_rows if 0 <= row_index < len(grid_df)
    ]
    if not valid_selected_rows:
        st.caption("Select one template row to clone it or inspect its definition.")
        return

    selected_row = builtins.iloc[valid_selected_rows[0]]
    selected_id = str(selected_row["Id"])
    st.markdown(f"### {selected_row.get('Name') or selected_id}")
    if st.button("Clone", icon=icons.ICON_COPY, key=f"clone_{selected_id}"):
        clone_strategy_dialog(selected_id)

    st.code(_json_text(selected_row.get("Definition_JSON")), language="json")


def render_import_export(df: pd.DataFrame):
    st.subheader("Import / Export")
    uploaded = st.file_uploader("Import .bec-strategy.json", type=["json"], key="strategy_import_upload")
    if uploaded is not None:
        package_text = uploaded.getvalue().decode("utf-8")
        try:
            imported = strategy_packages.validate_import_package(package_text)
            st.json(
                {
                    "name": imported["strategy"].get("name"),
                    "source_id": imported["strategy"].get("id"),
                    "engine": imported["definition"].get("engine"),
                    "side": imported["definition"].get("side"),
                }
            )
            if st.button("Import as draft", icon=icons.ICON_UPLOAD):
                new_id = database.import_strategy_package(package_text)
                st.session_state["strategy_builder_selected_id"] = new_id
                st.success(f"Imported draft strategy: {new_id}")
                time.sleep(0.5)
                st.rerun()
        except Exception as exc:
            st.error(f"Import rejected: {exc}")

    custom = df[df["Definition_JSON"].fillna("") != ""] if not df.empty else pd.DataFrame()
    if custom.empty:
        return
    export_id = st.selectbox(
        "Export strategy",
        custom["Id"].tolist(),
        format_func=lambda strategy_id: database.get_strategy_name(strategy_id) or strategy_id,
        key="strategy_export_id",
    )
    if export_id:
        try:
            export_text = database.export_strategy_package(export_id)
            st.download_button(
                "Download .bec-strategy.json",
                data=export_text,
                file_name=f"{export_id}.bec-strategy.json",
                mime="application/json",
                icon=icons.ICON_DOWNLOAD,
            )
        except Exception as exc:
            st.error(f"Export failed: {exc}")


def render_editor(df: pd.DataFrame):
    st.subheader("My Strategies")
    st.caption("Editable user strategies. Drafts are not eligible for live trading until approved.")
    if st.button("New Strategy", icon=icons.ICON_ADD, type="primary", key="new_strategy_open"):
        new_strategy_dialog()
    editable = df[df["Type"].fillna("builtin") == "custom"] if not df.empty else pd.DataFrame()
    if editable.empty:
        st.info("Create a strategy from scratch, clone a built-in template, or import a package to create your first custom strategy.")
        return

    grid_columns = [
        "Id",
        "Name",
        "Status",
        "Version",
        "Parent_Strategy_Id",
        "Created_At",
        "Updated_At",
    ]
    display_columns = [column for column in grid_columns if column in editable.columns]
    grid_df = editable[display_columns].copy()
    signature = int(
        pd.util.hash_pandas_object(
            grid_df[[column for column in ["Id", "Name", "Status", "Version"] if column in grid_df.columns]].astype(str),
            index=False,
        ).sum()
    )
    dataframe_event = st.dataframe(
        grid_df,
        width="content",
        hide_index=True,
        key=f"strategy_custom_grid_{len(grid_df)}_{signature}",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Id": st.column_config.TextColumn("Id"),
            "Name": st.column_config.TextColumn("Strategy"),
            "Status": st.column_config.TextColumn("Status"),
            "Version": st.column_config.NumberColumn("Version", format="%d"),
            "Parent_Strategy_Id": st.column_config.TextColumn("Parent"),
            "Created_At": st.column_config.TextColumn("Created"),
            "Updated_At": st.column_config.TextColumn("Updated"),
        },
    )

    selected_rows = dataframe_event.selection.rows
    valid_selected_rows = [
        row_index for row_index in selected_rows if 0 <= row_index < len(grid_df)
    ]
    if valid_selected_rows:
        selected_id = str(editable.iloc[valid_selected_rows[0]]["Id"])
    else:
        saved_id = str(st.session_state.get("strategy_builder_selected_id", "") or "")
        editable_ids = set(editable["Id"].astype(str))
        selected_id = saved_id if saved_id in editable_ids else ""

    if not selected_id:
        st.caption("Select one strategy row to edit, backtest, approve, archive or export it.")
        return

    st.session_state["strategy_builder_selected_id"] = selected_id
    row = database.get_strategy_by_id(selected_id).iloc[0]
    st.caption(f"Status: {row.get('Status')} | Version: {row.get('Version')} | Parent: {row.get('Parent_Strategy_Id') or '-'}")

    name = st.text_input("Name", value=str(row.get("Name") or selected_id), key=f"edit_name_{selected_id}")
    try:
        current_definition = strategy_schema.validate_definition(row.get("Definition_JSON"))
    except Exception as exc:
        st.error(f"Definition_JSON is invalid: {exc}")
        current_definition = {}
    current_risk = strategy_schema.extract_execution_risk(current_definition) if current_definition else {}
    current_definition, current_risk = _get_ai_override(selected_id, current_definition, current_risk)
    suggested_name = st.session_state.get(_ai_state_key(selected_id, "suggested_name"))
    if suggested_name:
        name = suggested_name
        st.caption(f"AI suggested name: {suggested_name}")

    st.space()

    render_ai_strategy_chat(selected_id, current_definition, current_risk)

    edited_definition = copy.deepcopy(current_definition)
    edited_risk = copy.deepcopy(current_risk)
    if current_definition:
        sync_active_block_from_query(selected_id)
        render_component_strategy_diagram(current_definition, current_risk, selected_id)
        edited_definition, edited_risk = render_diagram_click_editor(
            current_definition,
            current_risk,
            selected_id,
        )

    edited_definition = render_optimization_parameters_editor(edited_definition, selected_id)
    strategy_ready_for_execution = _strategy_has_entry_exit_conditions(edited_definition)

    with st.expander("Advanced JSON"):
        st.markdown("Definition_JSON")
        st.code(json.dumps(edited_definition, ensure_ascii=True, sort_keys=True, indent=2), language="json")

    metadata_text = st.text_area(
        "Metadata_JSON",
        value=_json_text(row.get("Metadata_JSON")),
        height=140,
        key=f"edit_metadata_{selected_id}",
    )

    actions = st.container(horizontal=True)
    with actions:
        if st.button("Save draft", icon=icons.ICON_SAVE, key=f"save_{selected_id}"):
            try:
                definition = strategy_schema.validate_definition(edited_definition)
                metadata = strategy_schema.parse_json_object(metadata_text, "Metadata_JSON")
                if str(row.get("Status", "draft")) == "approved":
                    new_id = database.create_strategy_draft_version(selected_id, definition, metadata=metadata)
                    st.session_state["strategy_builder_selected_id"] = new_id
                    st.toast(f"Approved strategy was versioned into new draft: {new_id}")
                else:
                    database.upsert_custom_strategy(
                        strategy_id=selected_id,
                        name=name,
                        definition=definition,
                        metadata=metadata,
                        status="draft",
                        parent_strategy_id=str(row.get("Parent_Strategy_Id") or ""),
                        version=int(row.get("Version", 1) or 1),
                        main_strategy=True,
                        btc_strategy=False,
                        backtest_optimize=_definition_has_optimizable_parameters(definition),
                    )
                    st.toast("Draft saved.")
                    for suffix in ("definition", "risk", "suggested_name"):
                        st.session_state.pop(_ai_state_key(selected_id, suffix), None)
                time.sleep(0.5)
                st.rerun()
            except Exception as exc:
                st.error(f"Save failed: {exc}")

        if st.button(
            "Approve for Live",
            icon=icons.ICON_SELECT,
            key=f"approve_{selected_id}",
            disabled=not strategy_ready_for_execution,
            help="Add at least one entry condition and one exit condition first." if not strategy_ready_for_execution else None,
        ):
            if not strategy_ready_for_execution:
                st.error("Add at least one entry condition and one exit condition before approving for live.")
            elif str(row.get("Status", "")) not in {"backtested", "approved"}:
                st.error("Run a successful local backtest before approving for live.")
            else:
                database.approve_strategy_for_live(selected_id)
                st.success("Strategy approved for live selection.")
                time.sleep(0.5)
                st.rerun()

        if st.button("Archive/Delete", icon=icons.ICON_DELETE, key=f"delete_{selected_id}"):
            result = database.delete_custom_strategy(selected_id)
            st.success(f"Strategy {result}.")
            time.sleep(0.5)
            st.rerun()

    st.markdown("### Backtest")
    bt_controls = st.container(horizontal=True, vertical_alignment="bottom")
    symbol = bt_controls.text_input("Symbol", value="BTCUSDC", width=180, key=f"bt_symbol_{selected_id}")
    timeframe = bt_controls.selectbox("Timeframe", ["15m", "1h", "4h", "1d"], index=3, width=120, key=f"bt_tf_{selected_id}")
    optimize_default = _definition_has_optimizable_parameters(edited_definition)
    optimize = bt_controls.checkbox(
        "Optimize",
        value=optimize_default,
        key=f"bt_optimize_{selected_id}",
        disabled=not optimize_default,
    )
    if bt_controls.button(
        "Run Backtest",
        icon=icons.ICON_EXECUTE,
        key=f"bt_run_{selected_id}",
        disabled=not strategy_ready_for_execution,
        help="Add at least one entry condition and one exit condition first." if not strategy_ready_for_execution else None,
    ):
        _run_backtest(selected_id, symbol, timeframe, optimize=optimize)


st.markdown("## Strategy Builder")
st.caption("Create, clone, import, export, backtest and approve no-code strategies. Imported strategies never execute code.")

df_strategies = _load_strategies()
templates_tab, editor_tab, package_tab = st.tabs(["Templates", "My Strategies", "Import / Export"])
with templates_tab:
    render_templates(df_strategies)
with editor_tab:
    render_editor(df_strategies)
with package_tab:
    render_import_export(df_strategies)
