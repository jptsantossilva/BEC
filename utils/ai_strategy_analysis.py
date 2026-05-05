import csv
import json
import os
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from requests import HTTPError
from requests import Timeout

from utils.env_loader import load_env_file
import utils.database as database
from utils.strategy_quality_score import calculate_strategy_quality_score

load_env_file(override=True)


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_MODEL = "gpt-5.5"
INVALID_CONFIG_VALUES = {"", "xxx", "your_api_key_here", "your-model-id"}


SYSTEM_PROMPT = """
You are a quantitative trading strategy analyst.

Analyze backtesting results for a crypto spot long-only trading bot.

Your role:
- Evaluate risk management, exits, take-profits, stop-loss behavior, drawdown, trade quality, and market regime sensitivity.
- Identify weaknesses in the strategy using only the supplied data.
- Suggest practical improvements that can be tested in future backtests.
- Do not give financial advice.
- Do not claim future profitability.
- Prefer concrete parameter experiments over vague recommendations.
- If data is insufficient, explicitly say what additional data is needed.

Important:
- The strategy trades crypto markets, which are volatile and regime-dependent.
- The bot is long-only.
- Take-profit settings are optional. A take-profit level with pnl_pct = 0 is disabled by user choice and must not be treated as an inconsistent configuration.
- Only evaluate take-profit behavior when there are active TP levels and/or trades with Exit_Reason containing tp.
- Take-profits may create multiple partial exits from the same original position when enabled.
- Exit_Reason describes why each partial or full exit happened.
- Hard_Stop_Loss, ATR_Stop_Loss and Active_Stop_Loss are snapshots at exit time.
- config.strategy_parameters contains the actual indicator parameters used in the backtest when available, including EMA fast/slow periods, SMA market phase filters, and any higher-timeframe market phase filter.
- config.backtesting.use_intraday_current_timeframe_market_phase_filter controls whether 1h/4h backtests also require the current timeframe market phase filter, or only the higher-timeframe 1d market phase filter.
- Buy & Hold Return [%] is normalized by BEC to use the full dataset period, from the first candle to the last candle, so it is comparable across strategies using the same symbol/timeframe/date range.
""".strip()


USER_PROMPT = """
Analyze the following backtest result.

Focus on:
1. Whether the strategy performance is acceptable compared with buy-and-hold.
2. Whether the drawdown is too high for the return achieved.
3. Whether active take-profit levels are helping or cutting winners too early. If all take-profit pnl_pct values are 0, state that TPs are disabled and do not recommend fixing them as a data issue.
4. Whether ATR trailing stop-loss is too tight, too loose, or useful.
5. Whether hard stop-loss is being triggered too often.
6. Whether exits by strategy signal are better or worse than exits by stop-loss.
7. How the supplied strategy indicator parameters affect behavior, including EMA/SMA periods when provided.
8. Which parameters should be tested next.

Return the answer in English US.

Backtest data:
""".strip()


ANALYSIS_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "summary",
        "main_findings",
        "risk_assessment",
        "recommended_tests",
        "data_quality_notes",
        "final_recommendation",
    ],
    "properties": {
        "summary": {"type": "string"},
        "main_findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["severity", "title", "evidence", "interpretation"],
                "properties": {
                    "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                    "title": {"type": "string"},
                    "evidence": {"type": "string"},
                    "interpretation": {"type": "string"},
                },
            },
        },
        "risk_assessment": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "drawdown_comment",
                "stop_loss_comment",
                "atr_trailing_comment",
                "take_profit_comment",
            ],
            "properties": {
                "drawdown_comment": {"type": "string"},
                "stop_loss_comment": {"type": "string"},
                "atr_trailing_comment": {"type": "string"},
                "take_profit_comment": {"type": "string"},
            },
        },
        "recommended_tests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "priority",
                    "parameter",
                    "current_value",
                    "test_values",
                    "reason",
                    "expected_effect",
                ],
                "properties": {
                    "priority": {"type": "integer"},
                    "parameter": {"type": "string"},
                    "current_value": {"type": "string"},
                    "test_values": {"type": "array", "items": {"type": "string"}},
                    "reason": {"type": "string"},
                    "expected_effect": {"type": "string"},
                },
            },
        },
        "data_quality_notes": {"type": "array", "items": {"type": "string"}},
        "final_recommendation": {"type": "string"},
    },
}


def _json_safe(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _round(value, digits=4):
    try:
        if pd.isna(value):
            return None
        return round(float(value), digits)
    except Exception:
        return None


def _read_backtest_csv(csv_path):
    path = Path(csv_path)
    lines = path.read_text(encoding="utf-8").splitlines()

    known_sections = {"# STATS", "# CONFIG", "# TRADES"}
    section_indexes = {
        line.strip(): idx for idx, line in enumerate(lines) if line.strip() in known_sections
    }

    if "# STATS" in section_indexes and "# TRADES" in section_indexes:
        stats_start = section_indexes["# STATS"] + 1
        stats_end = section_indexes.get("# CONFIG", section_indexes["# TRADES"])
        config_start = section_indexes.get("# CONFIG")
        trades_start = section_indexes["# TRADES"] + 1

        stats = _read_stats_lines(lines[stats_start:stats_end])
        config = (
            _read_config_lines(lines[config_start + 1:section_indexes["# TRADES"]])
            if config_start is not None
            else {}
        )
        trades = pd.read_csv(StringIO("\n".join(lines[trades_start:])))
        return stats, config, trades

    header_idx = next(
        (idx for idx, line in enumerate(lines) if line.startswith("Size,EntryBar,ExitBar")),
        None,
    )

    if header_idx is None:
        raise ValueError(f"Trades header not found in backtest CSV: {csv_path}")

    stats = _read_stats_lines(lines[:header_idx])

    trades = pd.read_csv(StringIO("\n".join(lines[header_idx:])))
    return stats, {}, trades


def _read_stats_lines(lines):
    stats = {}
    for row in csv.reader(lines):
        if len(row) < 2:
            continue
        key = row[0]
        value = row[1]
        if not key or key.startswith("_"):
            continue
        stats[key] = _parse_scalar(value)
    return stats


def _read_config_lines(lines):
    text = "\n".join(lines).strip()
    if not text:
        return {}

    df_config = pd.read_csv(StringIO(text))
    config = {}
    for _, row in df_config.iterrows():
        section = str(row.get("Section", "")).strip()
        setting = str(row.get("Setting", "")).strip()
        if not section or not setting:
            continue

        target = config.setdefault(section, {})
        parts = setting.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = _parse_scalar(row.get("Value"))

    return config


def _parse_scalar(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return float(value)
    except ValueError:
        return value


def _get_backtest_config(symbol, timeframe, strategy_id):
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(
        str(symbol),
        str(timeframe),
        str(strategy_id),
    )
    if df.empty or "Backtest_Config_JSON" not in df.columns:
        return {}

    config_json = df.iloc[0].get("Backtest_Config_JSON")
    if not config_json:
        return {}

    try:
        return json.loads(config_json)
    except json.JSONDecodeError:
        return {"raw": str(config_json)}


def _get_result_indicator_params(row, symbol, timeframe, strategy_id):
    ema_fast = row.get("Ema_Fast") if hasattr(row, "get") else None
    ema_slow = row.get("Ema_Slow") if hasattr(row, "get") else None

    if pd.isna(ema_fast) or pd.isna(ema_slow):
        df = database.get_backtesting_results_by_symbol_timeframe_strategy(
            str(symbol),
            str(timeframe),
            str(strategy_id),
        )
        if not df.empty:
            ema_fast = df.iloc[0].get("Ema_Fast", ema_fast)
            ema_slow = df.iloc[0].get("Ema_Slow", ema_slow)

    params = {}
    try:
        ema_fast = int(float(ema_fast))
        ema_slow = int(float(ema_slow))
        if ema_fast > 0 and ema_slow > 0:
            params["moving_averages"] = {
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
            }
    except Exception:
        pass

    if str(strategy_id) in {"market_phases", "ema_cross", "ema_cross_with_market_phases"}:
        settings = database.get_backtesting_settings()
        current_timeframe = str(timeframe).lower()
        if current_timeframe not in {"1h", "4h", "1d"}:
            current_timeframe = "1d"
        current_prefix = f"Market_Phase_{current_timeframe}_SMA"
        daily_prefix = "Market_Phase_1d_SMA"
        params["market_phase_filter"] = {
            "enabled": str(strategy_id) in {"market_phases", "ema_cross_with_market_phases"},
            "timeframe": current_timeframe,
            "sma_fast": int(settings.get(f"{current_prefix}_Fast", 50)),
            "sma_slow": int(settings.get(f"{current_prefix}_Slow", 200)),
        }
        if str(strategy_id) in {"ema_cross", "ema_cross_with_market_phases"} and str(timeframe) != "1d":
            params["higher_timeframe_market_phase_filter"] = {
                "enabled": True,
                "timeframe": "1d",
                "sma_fast": int(settings.get(f"{daily_prefix}_Fast", 50)),
                "sma_slow": int(settings.get(f"{daily_prefix}_Slow", 200)),
                "alignment": "previous_closed_candle",
            }

    if params:
        params["note"] = (
            "ema_cross and ema_cross_with_market_phases use EMA crossovers. "
            "ema_cross uses the higher-timeframe SMA market phase filter in intraday backtests. "
            "market_phases uses the SMA market phase filter only."
        )

    return params


def _ensure_strategy_parameters(config, row, symbol, timeframe, strategy_id):
    config = dict(config or {})
    if "strategy_quality_score" not in config:
        settings = database.get_backtesting_settings()
        config["strategy_quality_score"] = {
            "weights": {
                "return_weight": float(settings.get("Strategy_Quality_Return_Weight", 20.0)),
                "risk_weight": float(settings.get("Strategy_Quality_Risk_Weight", 25.0)),
                "risk_adjusted_weight": float(settings.get("Strategy_Quality_Risk_Adjusted_Weight", 20.0)),
                "trade_quality_weight": float(settings.get("Strategy_Quality_Trade_Quality_Weight", 20.0)),
                "robustness_weight": float(settings.get("Strategy_Quality_Robustness_Weight", 15.0)),
            },
        }
    if config.get("strategy_parameters"):
        return config

    params = _get_result_indicator_params(row, symbol, timeframe, strategy_id)
    if params:
        config["strategy_parameters"] = params

    return config


def _prepare_trades_for_analysis(df_trades):
    if df_trades.empty:
        return df_trades

    df = df_trades.copy()
    if "Return_Pct" not in df.columns and "ReturnPct" in df.columns:
        df["Return_Pct"] = pd.to_numeric(df["ReturnPct"], errors="coerce") * 100

    for column in [
        "EntryPrice",
        "ExitPrice",
        "PnL",
        "Commission",
        "Hard_Stop_Loss",
        "ATR_Stop_Loss",
        "Active_Stop_Loss",
        "Exit_Size_Pct",
        "Return_Pct",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "Exit_Reason" in df.columns:
        df["Exit_Reason"] = df["Exit_Reason"].fillna("unknown").astype(str).str.lower()
    else:
        df["Exit_Reason"] = "unknown"

    return df


def _records(df, limit):
    if df.empty:
        return []

    columns = [
        "EntryTime",
        "ExitTime",
        "Exit_Reason",
        "EntryPrice",
        "ExitPrice",
        "Return_Pct",
        "PnL",
        "Exit_Size_Pct",
        "Hard_Stop_Loss",
        "ATR_Stop_Loss",
    ]
    selected = df[[column for column in columns if column in df.columns]].head(limit).copy()
    for column in selected.columns:
        if pd.api.types.is_numeric_dtype(selected[column]):
            selected[column] = selected[column].round(8)
    return [
        {key: _json_safe(value) for key, value in row.items()}
        for row in selected.to_dict(orient="records")
    ]


def _exit_reason_summary(df):
    if df.empty:
        return []

    grouped = (
        df.groupby("Exit_Reason", dropna=False)
        .agg(
            count=("Exit_Reason", "size"),
            avg_return_pct=("Return_Pct", "mean"),
            median_return_pct=("Return_Pct", "median"),
            total_pnl=("PnL", "sum"),
            avg_exit_size_pct=("Exit_Size_Pct", "mean"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )

    for column in ["avg_return_pct", "median_return_pct", "total_pnl", "avg_exit_size_pct"]:
        grouped[column] = grouped[column].round(4)

    return grouped.to_dict(orient="records")


def _take_profit_summary(config, trades):
    take_profits = (config or {}).get("take_profits", {})
    levels = []
    for name, values in take_profits.items():
        pnl_pct = _round(values.get("pnl_pct") if isinstance(values, dict) else None)
        amount_pct = _round(values.get("amount_pct") if isinstance(values, dict) else None)
        levels.append(
            {
                "level": str(name),
                "pnl_pct": pnl_pct,
                "amount_pct": amount_pct,
                "enabled": bool(pnl_pct and pnl_pct > 0),
            }
        )

    active_levels = [level for level in levels if level["enabled"]]
    tp_exit_count = 0
    if not trades.empty and "Exit_Reason" in trades.columns:
        tp_exit_count = int(trades["Exit_Reason"].str.contains("tp", na=False).sum())

    return {
        "levels": levels,
        "active_levels_count": len(active_levels),
        "all_take_profits_disabled": len(levels) > 0 and len(active_levels) == 0,
        "tp_exit_count": tp_exit_count,
        "interpretation_hint": (
            "All take-profit levels are disabled because pnl_pct is 0. This is a valid user configuration, not a data quality issue."
            if len(levels) > 0 and len(active_levels) == 0
            else "Evaluate take-profit behavior only for enabled levels and trades with Exit_Reason containing tp."
        ),
    }


def build_backtest_analysis_context(row, csv_path):
    stats, csv_config, trades = _read_backtest_csv(csv_path)
    trades = _prepare_trades_for_analysis(trades)

    symbol = str(row["Symbol"])
    timeframe = str(row["Time_Frame"])
    strategy_id = str(row["Strategy_Id"])

    winners = trades[trades["Return_Pct"] > 0] if "Return_Pct" in trades.columns else pd.DataFrame()
    losers = trades[trades["Return_Pct"] <= 0] if "Return_Pct" in trades.columns else pd.DataFrame()
    config = csv_config or _get_backtest_config(symbol, timeframe, strategy_id)
    config = _ensure_strategy_parameters(config, row, symbol, timeframe, strategy_id)

    context = {
        "strategy": {
            "id": strategy_id,
            "name": str(row.get("Strategy_Name", strategy_id)),
            "symbol": symbol,
            "timeframe": timeframe,
            "market": "crypto",
            "asset_type": "spot_long_only",
        },
        "config": config,
        "config_interpretation": {
            "take_profits": _take_profit_summary(config, trades),
        },
        "stats": stats,
        "trade_summary": {
            "count_total": int(len(trades)),
            "count_winners": int(len(winners)),
            "count_losers": int(len(losers)),
            "avg_return_pct": _round(trades["Return_Pct"].mean() if "Return_Pct" in trades.columns else None),
            "median_return_pct": _round(trades["Return_Pct"].median() if "Return_Pct" in trades.columns else None),
            "total_pnl": _round(trades["PnL"].sum() if "PnL" in trades.columns else None),
            "exit_reason_counts": trades["Exit_Reason"].value_counts().to_dict(),
            "exit_reason_summary": _exit_reason_summary(trades),
        },
        "sample_trades": {
            "best_trades": _records(trades.sort_values("Return_Pct", ascending=False), 5),
            "worst_trades": _records(trades.sort_values("Return_Pct", ascending=True), 5),
            "atr_stop_examples": _records(trades[trades["Exit_Reason"].str.contains("atr", na=False)], 5),
            "hard_stop_examples": _records(trades[trades["Exit_Reason"].str.contains("hard|stop_loss", na=False)], 5),
            "strategy_exit_examples": _records(trades[trades["Exit_Reason"].str.contains("strategy", na=False)], 5),
            "take_profit_examples": _records(trades[trades["Exit_Reason"].str.contains("tp", na=False)], 5),
        },
    }
    context["strategy_quality_score"] = calculate_strategy_quality_score(context)
    return context


def analyze_backtest_with_openai(context, model=None, timeout=180):
    api_key, configured_model = validate_openai_configuration(model=model)
    model = model or configured_model
    base_url = os.getenv("OPENAI_BASE_URL", OPENAI_RESPONSES_URL)

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"{USER_PROMPT}\n{json.dumps(context, ensure_ascii=True)}",
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "strategy_analysis",
                "strict": True,
                "schema": ANALYSIS_SCHEMA,
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
        raise RuntimeError(
            f"OpenAI request timed out after {timeout}s. Try again, or use a faster model."
        ) from exc
    except requests.RequestException as exc:
        raise RuntimeError(
            "Could not connect to OpenAI. Check internet access and the configured API endpoint."
        ) from exc

    try:
        response.raise_for_status()
    except HTTPError as exc:
        _raise_openai_http_error(response, model)
        raise exc

    data = response.json()
    output_text = data.get("output_text") or _extract_response_text(data)
    if not output_text:
        raise RuntimeError("OpenAI response did not include output text.")

    return json.loads(output_text)


def _extract_response_text(response_json):
    chunks = []
    for output in response_json.get("output", []):
        for content in output.get("content", []):
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                chunks.append(content["text"])
    return "".join(chunks)


def _is_missing_or_placeholder(value):
    if value is None:
        return True
    return str(value).strip().strip('"').strip("'").lower() in INVALID_CONFIG_VALUES


def validate_openai_configuration(api_key=None, model=None):
    api_key = os.getenv("OPENAI_API_KEY") if api_key is None else api_key
    model = os.getenv("BEC_OPENAI_MODEL", DEFAULT_MODEL) if model is None else model

    if _is_missing_or_placeholder(api_key):
        raise RuntimeError(
            "OPENAI_API_KEY is missing or still using a placeholder value in the .env file."
        )
    if _is_missing_or_placeholder(model):
        raise RuntimeError(
            "BEC_OPENAI_MODEL is missing or still using a placeholder value in the .env file."
        )

    return str(api_key).strip(), str(model).strip()


def _extract_api_error_message(response):
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    error = payload.get("error")
    if isinstance(error, dict):
        return error.get("message") or error.get("code") or response.text
    if isinstance(error, str):
        return error
    return response.text


def _raise_openai_http_error(response, model):
    status_code = response.status_code
    api_message = _extract_api_error_message(response)

    if status_code == 401:
        raise RuntimeError(
            "OpenAI authentication failed. Check whether OPENAI_API_KEY exists, is valid, and belongs to a project with API access."
        )
    if status_code == 404:
        raise RuntimeError(
            f"OpenAI model '{model}' was not found or is not available for this API key. Choose a supported model or update BEC_OPENAI_MODEL."
        )
    if status_code == 429:
        raise RuntimeError(
            "OpenAI rate limit reached or quota exceeded. Check your billing/quota and try again later."
        )
    if 400 <= status_code < 500:
        raise RuntimeError(
            f"OpenAI request was rejected ({status_code}). {api_message}".strip()
        )
    if status_code >= 500:
        raise RuntimeError(
            f"OpenAI service error ({status_code}). Try again in a moment."
        )
