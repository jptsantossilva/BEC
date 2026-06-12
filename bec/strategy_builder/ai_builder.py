import json
import os
import re
import unicodedata

import requests
from requests import HTTPError, Timeout

from bec.strategy_builder import schema as strategy_schema
from bec.utils.ai_strategy_analysis import (
    OPENAI_RESPONSES_URL,
    _extract_response_text,
    _raise_openai_http_error,
    validate_openai_configuration,
)


STRATEGY_BUILDER_SYSTEM_PROMPT = """
You are an assistant that builds crypto spot long-only trading strategies for BEC.

Convert the user's natural-language request into BEC's Definition_JSON v2 AST format.

Rules:
- Only create long-only spot strategies.
- Never include an instrument/symbol. The user chooses the symbol in backtesting/live settings.
- Use only these timeframes on price and indicator operands: current, 15m, 1h, 4h, 1d, 1w.
- Use timeframe "current" when a rule should run on the selected backtest/live timeframe.
- Use only Market Order actions.
- Use only Buy for entry and Sell for exit.
- Use only these indicators: SMA, EMA, WMA, HMA, DEMA, RMA, RSI, ROC, MACD, STOCH, ATR, BB, OBV, VWAP, LINREG.
- Use only these price fields: Open, High, Low, Close, Volume.
- Use only these comparison operators: above, below, greater_than, less_than, greater_than_or_equal, less_than_or_equal, between, crosses_above, crosses_below.
- Use window_condition only for: for_n_bars, within_n_bars, wait_n_bars.
- Use transform operands only for: lookback, highest_high, lowest_low, rolling_mean.
- To compare current close with the previous N-period high, use highest_high over a lookback(1) price High source so the current candle is excluded.
- To compare an indicator or Volume with its M-period average, use rolling_mean over the same source.
- To sell on a failed breakout at the original breakout level, store that level in Definition_JSON.state.entry, then compare Close with an entry_state operand of the same name in exit.conditions.
- Use risk.rules for take_profit_pct, stop_loss_pct, take_profit_indicator, stop_loss_indicator, atr_stop, trailing_indicator, take_profit_r_multiple.
- For stop_loss_pct and take_profit_pct, pct must be a JSON number such as 10, never a string such as "10%".
- For size_pct, multiplier, r_multiple, and activation_pnl_pct, use JSON numbers only.
- Build entry as a group with logic and conditions plus a buy market action.
- Build exit as a group with logic and conditions plus a sell market action.
- Put risk inside Definition_JSON.risk.rules.
- The assistant_message must describe the returned definition_json exactly. Do not mention an entry filter, exit rule, stop loss, take profit, trailing stop, ATR stop, or risk control unless it exists in definition_json.
- If the user requests a stop loss or take profit, add an explicit risk.rules item. Do not leave risk.rules empty.
- If the user asks to refine the current strategy, preserve the existing strategy rules unless the latest request explicitly removes or replaces them.
- Prefer simple, testable rules.
- Do not claim profitability.
- If the user asks for unsupported concepts, approximate with supported rules and explain the limitation.
- Refuse requests that are not about creating, editing, explaining, or validating BEC trading strategies.
- Do not answer weather, news, sports, geography, email-writing, general chat, or generic financial advice requests.
- Use recent chat history to interpret short follow-up messages such as yes, no, ok, show it, apply it, sim, nao, ok, mostra, aplica.
- Use response_type=update_strategy only when the user asks to create or change strategy rules.
- Use response_type=explain_only when the user asks to explain the current strategy, list indicators, show JSON, or answer a strategy-builder question without changing rules.
- Use response_type=refuse when the user asks about anything outside BEC strategy creation, editing, explanation, or validation.
- Detect the user's language from the latest request and reply in the same language.
- If the user's language is ambiguous, reply in English.
- For response_type=update_strategy, return a valid JSON string for definition_json.
- For response_type=explain_only or response_type=refuse, return an empty string for definition_json.
""".strip()


STRATEGY_PROMPT_REFUSALS = {
    "pt-PT": (
        "Este assistente é apenas para criar, alterar ou explicar estratégias de trading no BEC. "
        "Descreve regras de entrada, saída, stop loss, take profits ou filtros que queres usar."
    ),
    "en": (
        "This assistant is only for creating, editing, or explaining BEC trading strategies. "
        "Describe the entry, exit, stop loss, take profit, or filters you want to use."
    ),
}

_PT_LANGUAGE_HINTS = {
    "a", "ao", "as", "com", "como", "cria", "criar", "de", "do", "em", "entrada",
    "estrategia", "estrategias", "falar", "filtro", "gestao", "o", "ola", "para",
    "podes", "quero", "saida", "se", "sobre", "um", "uma", "vender",
}
_EN_LANGUAGE_HINTS = {
    "about", "add", "buy", "create", "edit", "entry", "exit", "explain", "filter",
    "hello", "how", "sell", "strategy", "the", "trade", "trading", "what", "with",
}
_GREETING_ONLY = {
    "hi", "hello", "hey", "ola", "olá", "bom dia", "boa tarde", "boa noite",
}
_CONTEXTUAL_TERMS = {
    "aplica", "aplicar", "apply", "do it", "faz", "faz isso", "mostra", "mostrar",
    "nao", "não", "no", "ok", "okay", "sim", "show", "show it", "yes",
}
_OFF_TOPIC_TERMS = {
    "capital", "chuva", "chover", "email", "futebol", "geografia", "jogo", "meteorologia",
    "noticia", "noticias", "porto", "previsao do tempo", "sport", "sports", "tempo",
    "weather", "rain", "football", "soccer", "geography", "write an email",
}
_STRATEGY_TERMS = {
    "15m", "1h", "4h", "1d", "1w", "above", "atr", "bb", "below", "bollinger", "buy",
    "comprar", "condition", "conditions", "cria estrategia", "criar estrategia",
    "cross", "crosses", "crosses above", "crosses below", "cruza", "dema", "ema",
    "entrada", "entry", "exit", "filtro", "filtros", "hma", "indicador",
    "indicadores", "indicator", "indicators", "linreg", "long", "macd", "open",
    "operador", "operator", "rsi", "saida", "sell", "sinal", "sinais", "sma",
    "stoch", "stop", "stop loss", "strategy", "estrategia", "estrategias",
    "take profit", "take profits", "timeframe", "tp", "trailing", "vender", "vwap",
    "wma",
}


def _normalize_prompt_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    without_accents = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", without_accents)


def _detect_prompt_language(prompt: str) -> str:
    text = _normalize_prompt_text(prompt)
    words = set(re.findall(r"[a-z]+", text))
    pt_score = len(words & _PT_LANGUAGE_HINTS)
    en_score = len(words & _EN_LANGUAGE_HINTS)
    if en_score > pt_score:
        return "en"
    return "pt-PT"


def classify_strategy_prompt(prompt: str) -> dict:
    text = _normalize_prompt_text(prompt)
    language = _detect_prompt_language(prompt)
    refusal = STRATEGY_PROMPT_REFUSALS[language]
    if not text:
        return {"action": "block", "allowed": False, "language": language, "message": refusal, "reason": "empty"}
    if text in {_normalize_prompt_text(term) for term in _CONTEXTUAL_TERMS}:
        return {"action": "defer", "allowed": True, "language": language, "message": "", "reason": "contextual"}
    if text in {_normalize_prompt_text(term) for term in _GREETING_ONLY}:
        return {"action": "defer", "allowed": True, "language": language, "message": "", "reason": "greeting"}

    has_strategy_term = any(_normalize_prompt_text(term) in text for term in _STRATEGY_TERMS)
    has_off_topic_term = any(_normalize_prompt_text(term) in text for term in _OFF_TOPIC_TERMS)
    if has_strategy_term:
        return {"action": "allow", "allowed": True, "language": language, "message": "", "reason": "strategy"}
    if has_off_topic_term:
        return {"action": "block", "allowed": False, "language": language, "message": refusal, "reason": "off_topic"}
    return {"action": "defer", "allowed": True, "language": language, "message": "", "reason": "ambiguous"}


BUILDER_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["response_type", "assistant_message", "strategy_name", "definition_json"],
    "properties": {
        "response_type": {"type": "string", "enum": ["update_strategy", "explain_only", "refuse"]},
        "assistant_message": {"type": "string"},
        "strategy_name": {"type": "string"},
        "definition_json": {"type": "string"},
    },
}


def _sanitize_chat_history(chat_history, limit=10):
    sanitized = []
    for message in list(chat_history or [])[-limit:]:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        content = str(message.get("content", "")).strip()
        if role not in {"user", "assistant"} or not content:
            continue
        sanitized.append({"role": role, "content": content[:2000]})
    return sanitized


def _builder_context(current_definition=None, chat_history=None):
    return {
        "language_policy": {
            "mode": "auto_detect_from_user_request",
            "fallback": "en",
            "supported_languages": ["pt-PT", "en"],
        },
        "schema_notes": {
            "definition_required_fields": [
                "schema_version",
                "engine",
                "name",
                "description",
                "constraints",
                "parameters",
                "entry",
                "exit",
                "risk",
            ],
            "schema_version": 2,
            "engine": "bec_strategy_ast_v2",
            "constraints": {
                "market": "spot",
                "side": "long",
                "order_type": "market",
                "allowed_actions": ["buy", "sell"],
                "allowed_timeframes": ["15m", "1h", "4h", "1d", "1w"],
            },
            "operand_types": {
                "indicator": {"type": "indicator", "name": "EMA", "timeframe": "4h", "source": {"type": "price", "field": "Close"}, "params": {"period": 50}, "output": "value"},
                "price": {"type": "price", "field": "Close", "timeframe": "4h"},
                "value": {"type": "value", "value": 52.0},
                "transform": {"type": "transform", "operator": "lookback", "bars": 1, "source": {"type": "price", "field": "Close", "timeframe": "4h"}},
            },
            "entry_shape": {"logic": "all", "conditions": [], "action": {"type": "buy", "order_type": "market", "size_pct": 100}},
            "exit_shape": {"logic": "any", "conditions": [], "action": {"type": "sell", "order_type": "market", "size_pct": 100}},
            "risk_examples": {
                "fixed_stop_loss_10_pct": {"type": "stop_loss_pct", "pct": 10},
                "take_profit_8_pct_half_position": {"type": "take_profit_pct", "pct": 8, "size_pct": 50},
                "atr_stop_1_5x": {
                    "type": "atr_stop",
                    "indicator": {"type": "indicator", "name": "ATR", "timeframe": "4h", "params": {"period": 14}, "output": "value"},
                    "multiplier": 1.5,
                },
            },
        },
        "recent_chat_history": _sanitize_chat_history(chat_history),
        "current_definition": current_definition or {},
    }


def _parse_json_string(value, field_name):
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"OpenAI returned invalid {field_name}.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"OpenAI returned {field_name} that is not a JSON object.")
    return parsed


def build_strategy_with_openai(
    user_prompt: str,
    *,
    current_definition=None,
    current_risk=None,
    chat_history=None,
    model=None,
    timeout=180,
):
    api_key, configured_model = validate_openai_configuration(model=model)
    model = model or configured_model
    base_url = os.getenv("OPENAI_BASE_URL", OPENAI_RESPONSES_URL)
    context = _builder_context(current_definition=current_definition, chat_history=chat_history)

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": STRATEGY_BUILDER_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Build or refine a BEC strategy from this request:\n"
                    f"{user_prompt}\n\n"
                    "Current context and output format requirements:\n"
                    f"{json.dumps(context, ensure_ascii=True)}"
                ),
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "bec_strategy_builder",
                "strict": True,
                "schema": BUILDER_RESPONSE_SCHEMA,
            }
        },
    }

    try:
        response = requests.post(
            base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
    except Timeout as exc:
        raise RuntimeError(f"OpenAI request timed out after {timeout}s.") from exc
    except requests.RequestException as exc:
        raise RuntimeError("Could not connect to OpenAI. Check internet access and API configuration.") from exc

    try:
        response.raise_for_status()
    except HTTPError as exc:
        _raise_openai_http_error(response, model)
        raise exc

    data = response.json()
    output_text = data.get("output_text") or _extract_response_text(data)
    if not output_text:
        raise RuntimeError("OpenAI response did not include output text.")

    result = json.loads(output_text)
    response_type = str(result.get("response_type") or "update_strategy")
    definition = {}
    if response_type == "update_strategy":
        definition = _parse_json_string(result.get("definition_json", "{}"), "definition_json")
        definition = strategy_schema.validate_definition(definition)
    elif response_type not in {"explain_only", "refuse"}:
        raise RuntimeError(f"OpenAI returned unsupported response_type '{response_type}'.")

    return {
        "response_type": response_type,
        "assistant_message": str(result.get("assistant_message") or ""),
        "strategy_name": str(result.get("strategy_name") or definition.get("name") or "AI Strategy"),
        "definition": definition,
        "risk": {},
    }
