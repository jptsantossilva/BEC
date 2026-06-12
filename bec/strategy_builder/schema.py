import json
import copy
from collections.abc import Callable

from bec.strategy_builder import registry
from bec.utils.take_profit import normalize_take_profit_levels


SUPPORTED_GROUPS = {"all", "any"}
AST_SCHEMA_VERSION = 2
AST_ENGINE = "bec_strategy_ast_v2"
PARAMETER_CONSTRAINT_OPERATORS = {
    "less_than",
    "less_than_or_equal",
    "greater_than",
    "greater_than_or_equal",
}


class StrategyValidationError(ValueError):
    pass


def parse_json_object(value, field_name: str) -> dict:
    if value in (None, ""):
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StrategyValidationError(f"{field_name} must be valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise StrategyValidationError(f"{field_name} must be a JSON object.")
    return parsed


def parameter_defaults(definition: dict) -> dict:
    params = definition.get("parameters", {})
    result = {}
    if not isinstance(params, dict):
        return result
    for name, spec in params.items():
        if isinstance(spec, dict) and "default" in spec:
            result[str(name)] = spec["default"]
    return result


def validate_definition(definition) -> dict:
    definition = parse_json_object(definition, "Definition_JSON")
    return validate_ast_definition(definition)


def _ensure_object(value, path: str) -> dict:
    if not isinstance(value, dict):
        raise StrategyValidationError(f"{path} must be an object.")
    return value


def _ensure_number(value, path: str) -> float:
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("%"):
            value = value[:-1].strip()
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise StrategyValidationError(f"{path} must be a number.") from exc


def _validate_timeframe(value, path: str):
    if value == "current":
        return
    if value not in registry.ALLOWED_TIMEFRAMES:
        raise StrategyValidationError(f"{path} has unsupported timeframe '{value}'.")


def _validate_price_operand(operand: dict, path: str):
    field = operand.get("field", "Close")
    if field not in registry.ALLOWED_PRICE_FIELDS:
        raise StrategyValidationError(f"{path}.field has unsupported price field '{field}'.")
    _validate_timeframe(operand.get("timeframe", "1h"), f"{path}.timeframe")


def _validate_indicator_operand(operand: dict, path: str):
    name = registry.normalize_indicator_name(operand.get("name", ""))
    spec = registry.get_indicator_spec(name)
    if spec is None:
        raise StrategyValidationError(f"{path}.name has unsupported indicator '{name}'.")
    _validate_timeframe(operand.get("timeframe", "1h"), f"{path}.timeframe")
    params = operand.get("params", {})
    if not isinstance(params, dict):
        raise StrategyValidationError(f"{path}.params must be an object.")
    allowed_params = set(spec.params)
    for key, value in params.items():
        if key not in allowed_params:
            raise StrategyValidationError(f"{path}.params.{key} is not supported for {name}.")
        _ensure_number(value, f"{path}.params.{key}")
    output = operand.get("output", "value")
    if output not in spec.outputs:
        raise StrategyValidationError(f"{path}.output has unsupported output '{output}' for {name}.")
    source = operand.get("source")
    if source is not None:
        _validate_ast_operand(source, f"{path}.source")


def _validate_transform_operand(operand: dict, path: str):
    operator = operand.get("operator")
    if operator not in registry.TRANSFORM_OPERATORS:
        raise StrategyValidationError(f"{path}.operator has unsupported transform '{operator}'.")
    _validate_ast_operand(operand.get("source"), f"{path}.source")
    window = int(_ensure_number(operand.get("bars", operand.get("period", 1)), f"{path}.bars"))
    if window < 1:
        raise StrategyValidationError(f"{path}.bars must be >= 1.")


def _validate_ast_operand(operand, path: str):
    operand = _ensure_object(operand, path)
    operand_type = operand.get("type")
    if operand_type == "price":
        _validate_price_operand(operand, path)
        return
    if operand_type == "indicator":
        _validate_indicator_operand(operand, path)
        return
    if operand_type == "value":
        if "value" not in operand:
            raise StrategyValidationError(f"{path}.value is required.")
        _ensure_number(operand.get("value"), f"{path}.value")
        return
    if operand_type == "parameter":
        name = str(operand.get("name", "")).strip()
        if not name:
            raise StrategyValidationError(f"{path}.name is required.")
        return
    if operand_type == "entry_state":
        name = str(operand.get("name", "")).strip()
        if not name:
            raise StrategyValidationError(f"{path}.name is required.")
        return
    if operand_type == "transform":
        _validate_transform_operand(operand, path)
        return
    raise StrategyValidationError(f"{path}.type has unsupported operand type '{operand_type}'.")


def _validate_entry_state(definition: dict):
    state = definition.get("state", {})
    if state in (None, ""):
        return
    state = _ensure_object(state, "state")
    entry = state.get("entry", {})
    if entry in (None, ""):
        return
    entry = _ensure_object(entry, "state.entry")
    for name, operand in entry.items():
        state_name = str(name).strip()
        if not state_name:
            raise StrategyValidationError("state.entry names must not be empty.")
        _validate_ast_operand(operand, f"state.entry.{state_name}")


def _validate_comparison_condition(condition: dict, path: str):
    operator = condition.get("operator")
    if operator not in registry.COMPARISON_OPERATORS:
        raise StrategyValidationError(f"{path}.operator has unsupported operator '{operator}'.")
    _validate_ast_operand(condition.get("left"), f"{path}.left")
    if operator == "between":
        lower = condition.get("lower")
        upper = condition.get("upper")
        if lower is None or upper is None:
            right = condition.get("right")
            if isinstance(right, list) and len(right) == 2:
                lower, upper = right
        _validate_ast_operand(lower, f"{path}.lower")
        _validate_ast_operand(upper, f"{path}.upper")
    else:
        _validate_ast_operand(condition.get("right"), f"{path}.right")


def _validate_window_condition(condition: dict, path: str):
    operator = condition.get("operator")
    if operator not in registry.WINDOW_OPERATORS:
        raise StrategyValidationError(f"{path}.operator has unsupported window operator '{operator}'.")
    bars = int(_ensure_number(condition.get("bars", 1), f"{path}.bars"))
    if bars < 1:
        raise StrategyValidationError(f"{path}.bars must be >= 1.")
    if operator != "wait_n_bars":
        _validate_ast_condition(condition.get("condition"), f"{path}.condition")


def _validate_ast_condition(condition, path: str):
    condition = _ensure_object(condition, path)
    condition_type = condition.get("type", "comparison")
    if condition_type == "comparison":
        _validate_comparison_condition(condition, path)
        return
    if condition_type == "window_condition":
        _validate_window_condition(condition, path)
        return
    if condition_type == "group":
        _validate_ast_group(condition, path, allow_empty=False)
        return
    raise StrategyValidationError(f"{path}.type has unsupported condition type '{condition_type}'.")


def _validate_ast_group(group, path: str, *, allow_empty: bool):
    group = _ensure_object(group, path)
    logic = group.get("logic")
    if logic not in SUPPORTED_GROUPS:
        raise StrategyValidationError(f"{path}.logic must be one of: all, any.")
    conditions = group.get("conditions", [])
    if not isinstance(conditions, list):
        raise StrategyValidationError(f"{path}.conditions must be a list.")
    if not conditions and not allow_empty:
        raise StrategyValidationError(f"{path}.conditions must contain at least one condition.")
    operators = group.get("operators", [])
    if operators is None:
        group["operators"] = []
        operators = []
    if not isinstance(operators, list):
        raise StrategyValidationError(f"{path}.operators must be a list.")
    if operators and len(operators) != max(len(conditions) - 1, 0):
        raise StrategyValidationError(f"{path}.operators must have one item between each condition.")
    for idx, operator in enumerate(operators):
        if operator not in SUPPORTED_GROUPS:
            raise StrategyValidationError(f"{path}.operators[{idx}] must be one of: all, any.")
    for idx, condition in enumerate(conditions):
        _validate_ast_condition(condition, f"{path}.conditions[{idx}]")


def _validate_action(action, path: str, expected_type: str):
    action = _ensure_object(action, path)
    action_type = str(action.get("type", "")).lower()
    if action_type != expected_type:
        raise StrategyValidationError(f"{path}.type must be '{expected_type}'.")
    order_type = str(action.get("order_type", "market")).lower()
    if order_type not in registry.ALLOWED_ORDER_TYPES:
        raise StrategyValidationError(f"{path}.order_type must be 'market'.")
    size_pct = _ensure_number(action.get("size_pct", 100), f"{path}.size_pct")
    if size_pct <= 0 or size_pct > 100:
        raise StrategyValidationError(f"{path}.size_pct must be between 0 and 100.")


def _compare_parameter_constraint(left_value, operator: str, right_value) -> bool:
    if operator == "less_than":
        return left_value < right_value
    if operator == "less_than_or_equal":
        return left_value <= right_value
    if operator == "greater_than":
        return left_value > right_value
    if operator == "greater_than_or_equal":
        return left_value >= right_value
    return False


def build_optimize_constraint_fn(definition: dict, optimized_names) -> Callable | None:
    optimized = {str(name) for name in (optimized_names or [])}
    rules = definition.get("parameter_constraints") if isinstance(definition, dict) else None
    if not isinstance(rules, list):
        rules = []
    applicable = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        left = str(rule.get("left", "")).strip()
        right = str(rule.get("right", "")).strip()
        operator = str(rule.get("operator", "")).strip()
        if left not in optimized or right not in optimized:
            continue
        if operator not in PARAMETER_CONSTRAINT_OPERATORS:
            continue
        applicable.append((left, operator, right))
    if not applicable:
        return None

    def constraint(param):
        for left, operator, right in applicable:
            if not _compare_parameter_constraint(
                getattr(param, left),
                operator,
                getattr(param, right),
            ):
                return False
        return True

    return constraint


def _validate_parameter_constraints(definition: dict):
    constraints = definition.get("parameter_constraints", [])
    if constraints is None:
        definition["parameter_constraints"] = []
        return
    if not isinstance(constraints, list):
        raise StrategyValidationError("parameter_constraints must be a list.")
    parameters = definition.get("parameters", {})
    param_names = set(parameters) if isinstance(parameters, dict) else set()
    seen = set()
    normalized = []
    for idx, rule in enumerate(constraints):
        path = f"parameter_constraints[{idx}]"
        rule = _ensure_object(rule, path)
        left = str(rule.get("left", "")).strip()
        right = str(rule.get("right", "")).strip()
        operator = str(rule.get("operator", "")).strip()
        if not left or left not in param_names:
            raise StrategyValidationError(f"{path}.left must reference an existing parameter.")
        if not right or right not in param_names:
            raise StrategyValidationError(f"{path}.right must reference an existing parameter.")
        if left == right:
            raise StrategyValidationError(f"{path}.left and right must be different parameters.")
        if operator not in PARAMETER_CONSTRAINT_OPERATORS:
            raise StrategyValidationError(
                f"{path}.operator must be one of: {', '.join(sorted(PARAMETER_CONSTRAINT_OPERATORS))}."
            )
        key = (left, operator, right)
        if key in seen:
            raise StrategyValidationError(f"{path} duplicates an existing parameter constraint.")
        seen.add(key)
        normalized.append({"left": left, "operator": operator, "right": right})
    definition["parameter_constraints"] = normalized


def _validate_constraints(definition: dict):
    constraints = definition.get("constraints", {})
    constraints = _ensure_object(constraints, "constraints")
    if constraints.get("market", "spot") != "spot":
        raise StrategyValidationError("constraints.market must be 'spot'.")
    if constraints.get("side", "long") != "long":
        raise StrategyValidationError("constraints.side must be 'long'.")
    if constraints.get("order_type", "market") != "market":
        raise StrategyValidationError("constraints.order_type must be 'market'.")
    actions = constraints.get("allowed_actions", ["buy", "sell"])
    if set(actions) != registry.ALLOWED_ACTIONS:
        raise StrategyValidationError("constraints.allowed_actions must be ['buy', 'sell'].")
    timeframes = constraints.get("allowed_timeframes", sorted(registry.ALLOWED_TIMEFRAMES))
    if set(timeframes) - registry.ALLOWED_TIMEFRAMES:
        raise StrategyValidationError("constraints.allowed_timeframes contains unsupported timeframes.")


def _validate_risk_rule(rule, path: str):
    rule = _ensure_object(rule, path)
    rule_type = rule.get("type")
    if rule_type not in registry.RISK_RULE_TYPES:
        raise StrategyValidationError(f"{path}.type has unsupported risk rule '{rule_type}'.")
    if rule_type in {"take_profit_pct", "stop_loss_pct"}:
        pct = _ensure_number(rule.get("pct"), f"{path}.pct")
        if pct < 0:
            raise StrategyValidationError(f"{path}.pct cannot be negative.")
        rule["pct"] = pct
    if rule_type == "take_profit_r_multiple":
        r_multiple = _ensure_number(rule.get("r_multiple"), f"{path}.r_multiple")
        if r_multiple <= 0:
            raise StrategyValidationError(f"{path}.r_multiple must be > 0.")
        rule["r_multiple"] = r_multiple
    if "size_pct" in rule:
        size_pct = _ensure_number(rule.get("size_pct"), f"{path}.size_pct")
        if size_pct <= 0 or size_pct > 100:
            raise StrategyValidationError(f"{path}.size_pct must be between 0 and 100.")
        rule["size_pct"] = size_pct
    if rule_type in {"take_profit_indicator", "stop_loss_indicator", "trailing_indicator"}:
        _validate_ast_condition(rule.get("condition") or {
            "type": "comparison",
            "operator": "above",
            "left": rule.get("indicator"),
            "right": rule.get("trigger", {"type": "value", "value": 0}),
        }, f"{path}.condition")
    if rule_type == "atr_stop":
        _validate_indicator_operand(rule.get("indicator", {}), f"{path}.indicator")
        multiplier = _ensure_number(rule.get("multiplier", 1), f"{path}.multiplier")
        if multiplier <= 0:
            raise StrategyValidationError(f"{path}.multiplier must be > 0.")
        rule["multiplier"] = multiplier
        if "activation_pnl_pct" in rule:
            rule["activation_pnl_pct"] = _ensure_number(rule.get("activation_pnl_pct"), f"{path}.activation_pnl_pct")


def validate_ast_definition(definition) -> dict:
    definition = copy.deepcopy(parse_json_object(definition, "Definition_JSON"))
    if int(definition.get("schema_version", 0) or 0) != AST_SCHEMA_VERSION:
        raise StrategyValidationError(f"Unsupported schema_version. Expected {AST_SCHEMA_VERSION}.")
    if definition.get("engine") != AST_ENGINE:
        raise StrategyValidationError(f"Unsupported strategy engine. Expected {AST_ENGINE}.")
    if "instrument" in {str(key).lower() for key in definition}:
        raise StrategyValidationError("Definition_JSON must not contain an instrument/symbol.")
    _validate_constraints(definition)
    parameters = definition.get("parameters", {})
    if not isinstance(parameters, dict):
        raise StrategyValidationError("parameters must be an object.")
    _validate_parameter_constraints(definition)
    _validate_ast_group(definition.get("entry"), "entry", allow_empty=True)
    _validate_action(definition.get("entry", {}).get("action"), "entry.action", "buy")
    _validate_ast_group(definition.get("exit"), "exit", allow_empty=True)
    _validate_action(definition.get("exit", {}).get("action"), "exit.action", "sell")
    _validate_entry_state(definition)
    risk = _ensure_object(definition.get("risk", {"rules": []}), "risk")
    rules = risk.get("rules", [])
    if not isinstance(rules, list):
        raise StrategyValidationError("risk.rules must be a list.")
    for idx, rule in enumerate(rules):
        _validate_risk_rule(rule, f"risk.rules[{idx}]")
    metadata = definition.get("metadata", {})
    if metadata is not None and not isinstance(metadata, dict):
        raise StrategyValidationError("metadata must be an object.")
    return definition


def extract_execution_risk(definition: dict) -> dict:
    definition = validate_definition(definition)
    risk = {
        "stop_loss_pct": 0.0,
        "atr_trailing": {"enabled": False, "period": 14, "multiplier": 1.8, "activation_pnl_pct": 2.0},
        "take_profits": [],
    }
    tp_level = 1
    for rule in definition.get("risk", {}).get("rules", []):
        rule_type = rule.get("type")
        if rule_type == "stop_loss_pct":
            risk["stop_loss_pct"] = float(rule.get("pct", 0.0) or 0.0)
        elif rule_type == "take_profit_pct":
            risk["take_profits"].append({
                "level": tp_level,
                "pnl_pct": float(rule.get("pct", 0.0) or 0.0),
                "amount_pct": float(rule.get("size_pct", 100.0) or 100.0),
            })
            tp_level += 1
        elif rule_type == "atr_stop":
            indicator = rule.get("indicator", {})
            params = indicator.get("params", {}) if isinstance(indicator, dict) else {}
            risk["atr_trailing"] = {
                "enabled": True,
                "period": int(params.get("period", 14) or 14),
                "multiplier": float(rule.get("multiplier", 1.8) or 1.8),
                "activation_pnl_pct": float(rule.get("activation_pnl_pct", 0.0) or 0.0),
            }
    risk["take_profits"] = normalize_take_profit_levels(risk["take_profits"])
    return risk
