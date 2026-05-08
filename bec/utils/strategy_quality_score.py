import math
import re
from copy import deepcopy


COMPONENT_WEIGHTS = {
    "return_score": 0.20,
    "risk_score": 0.25,
    "risk_adjusted_score": 0.20,
    "trade_quality_score": 0.20,
    "robustness_score": 0.15,
}

CONFIG_WEIGHT_KEYS = {
    "return_score": "return_weight",
    "risk_score": "risk_weight",
    "risk_adjusted_score": "risk_adjusted_weight",
    "trade_quality_score": "trade_quality_weight",
    "robustness_score": "robustness_weight",
}


def _clamp(value, low=0.0, high=1.0):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return low
    if math.isnan(value) or math.isinf(value):
        return low
    return max(low, min(high, value))


def _num(value, default=None):
    if value is None:
        return default
    if isinstance(value, str):
        cleaned = value.strip().replace("%", "").replace(",", "")
        if cleaned == "":
            return default
        value = cleaned
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _metric(stats, *names, default=None):
    for name in names:
        if isinstance(stats, dict) and name in stats:
            value = _num(stats.get(name), default=None)
            if value is not None:
                return value
    return default


def _linear_score(value, low, high):
    value = _num(value, default=None)
    if value is None:
        return 0.5
    if high == low:
        return 1.0 if value >= high else 0.0
    return _clamp((value - low) / (high - low))


def _piecewise_score(value, points, default=0.5):
    value = _num(value, default=None)
    if value is None:
        return default
    points = sorted((float(x), float(y)) for x, y in points)
    if value <= points[0][0]:
        return _clamp(points[0][1])
    if value >= points[-1][0]:
        return _clamp(points[-1][1])
    for idx in range(1, len(points)):
        x0, y0 = points[idx - 1]
        x1, y1 = points[idx]
        if x0 <= value <= x1:
            if x1 == x0:
                return _clamp(y1)
            ratio = (value - x0) / (x1 - x0)
            return _clamp(y0 + ratio * (y1 - y0))
    return default


def duration_to_days(value):
    """Parse pandas-style durations such as '839 days 16:00:00' into days."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _num(value)

    text = str(value).strip()
    if not text:
        return None

    days = 0.0
    day_match = re.search(r"(-?\d+(?:\.\d+)?)\s+days?", text)
    if day_match:
        days += float(day_match.group(1))

    time_match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}(?:\.\d+)?))?", text)
    if time_match:
        hours = float(time_match.group(1))
        minutes = float(time_match.group(2))
        seconds = float(time_match.group(3) or 0)
        days += (hours / 24.0) + (minutes / 1440.0) + (seconds / 86400.0)

    if days > 0:
        return days

    numeric_match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(numeric_match.group(0)) if numeric_match else None


def _extract_context(backtest_json):
    if not isinstance(backtest_json, dict):
        return {}
    return backtest_json.get("context_sent_to_openai") or backtest_json


def _normalize_weights(weights):
    normalized = {}
    total = 0.0
    for component, default_weight in COMPONENT_WEIGHTS.items():
        value = _num((weights or {}).get(component), default=None)
        if value is None:
            value = default_weight
        if value > 1.0:
            value = value / 100.0
        value = max(0.0, value)
        normalized[component] = value
        total += value

    if total <= 0:
        return deepcopy(COMPONENT_WEIGHTS)

    return {
        component: value / total
        for component, value in normalized.items()
    }


def _extract_weights(config):
    quality_config = (config or {}).get("strategy_quality_score", {})
    configured_weights = (quality_config or {}).get("weights", {})
    if not isinstance(configured_weights, dict):
        return deepcopy(COMPONENT_WEIGHTS)

    weights = {}
    for component, config_key in CONFIG_WEIGHT_KEYS.items():
        if config_key in configured_weights:
            weights[component] = configured_weights.get(config_key)
        elif component in configured_weights:
            weights[component] = configured_weights.get(component)
    return _normalize_weights(weights)


def _commission_drag_score(stats, config):
    commissions = _metric(stats, "Commissions [$]", "Commission [$]", default=None)
    equity_final = _metric(stats, "Equity Final [$]", default=None)
    initial_cash = _num(
        ((config or {}).get("backtesting") or {}).get("initial_cash"),
        default=None,
    )
    if commissions is None or equity_final is None or initial_cash is None:
        return 0.7, None

    net_profit = equity_final - initial_cash
    if net_profit <= 0:
        return 0.2, None

    ratio = commissions / net_profit
    score = _piecewise_score(
        ratio,
        [
            (0.00, 1.00),
            (0.05, 1.00),
            (0.15, 0.75),
            (0.30, 0.40),
            (0.50, 0.10),
            (0.75, 0.00),
        ],
    )
    return score, ratio


def _single_winner_score(stats):
    best_trade = _metric(stats, "Best Trade [%]", default=None)
    total_return = _metric(stats, "Return [%]", default=None)
    if best_trade is None or total_return is None:
        return 0.7, None
    if total_return <= 0:
        return 0.2, None

    ratio = max(0.0, best_trade) / total_return
    score = _piecewise_score(
        ratio,
        [
            (0.00, 1.00),
            (0.15, 1.00),
            (0.30, 0.80),
            (0.50, 0.50),
            (0.80, 0.20),
            (1.00, 0.00),
        ],
    )
    return score, ratio


def _return_score(stats):
    total_return = _metric(stats, "Return [%]", default=None)
    cagr = _metric(stats, "CAGR [%]", "Return (Ann.) [%]", default=None)
    buy_hold_return = _metric(stats, "Buy & Hold Return [%]", default=None)

    absolute_score = _piecewise_score(
        total_return,
        [(-50, 0.0), (0, 0.25), (100, 0.55), (300, 0.80), (600, 1.0)],
    )
    cagr_score = _piecewise_score(
        cagr,
        [(-10, 0.0), (0, 0.20), (15, 0.55), (30, 0.80), (60, 1.0)],
    )

    relative_ratio = None
    if total_return is not None and buy_hold_return is not None and buy_hold_return > 0:
        relative_ratio = total_return / buy_hold_return
    relative_score = _piecewise_score(
        relative_ratio,
        [(0.00, 0.15), (0.10, 0.30), (0.25, 0.55), (0.50, 0.75), (1.00, 1.00), (1.50, 1.00)],
        default=0.6,
    )

    score = (absolute_score * 0.30) + (cagr_score * 0.45) + (relative_score * 0.25)
    return score * 100.0, {
        "absolute_return_score": round(absolute_score * 100.0, 2),
        "cagr_score": round(cagr_score * 100.0, 2),
        "buy_hold_relative_score": round(relative_score * 100.0, 2),
        "return_vs_buy_hold_ratio": round(relative_ratio, 4) if relative_ratio is not None else None,
    }


def _risk_score(stats):
    max_dd = abs(_metric(stats, "Max. Drawdown [%]", default=100.0))
    avg_dd = abs(_metric(stats, "Avg. Drawdown [%]", default=25.0))
    duration_days = duration_to_days(stats.get("Max. Drawdown Duration") if isinstance(stats, dict) else None)
    calmar = _metric(stats, "Calmar Ratio", default=None)

    max_dd_score = _piecewise_score(
        max_dd,
        [(0, 1.0), (20, 1.0), (35, 0.75), (50, 0.35), (70, 0.10), (90, 0.0)],
    )
    avg_dd_score = _piecewise_score(
        avg_dd,
        [(0, 1.0), (5, 1.0), (10, 0.75), (20, 0.35), (35, 0.0)],
    )
    duration_score = _piecewise_score(
        duration_days,
        [(0, 1.0), (60, 1.0), (180, 0.80), (365, 0.55), (730, 0.25), (1095, 0.0)],
        default=0.6,
    )
    calmar_score = _piecewise_score(
        calmar,
        [(-0.5, 0.0), (0, 0.15), (0.5, 0.50), (1.0, 0.75), (2.0, 1.0), (3.0, 1.0)],
    )

    score = (
        max_dd_score * 0.40
        + avg_dd_score * 0.15
        + duration_score * 0.20
        + calmar_score * 0.25
    )
    return score * 100.0, {
        "max_drawdown_score": round(max_dd_score * 100.0, 2),
        "avg_drawdown_score": round(avg_dd_score * 100.0, 2),
        "drawdown_duration_score": round(duration_score * 100.0, 2),
        "calmar_score": round(calmar_score * 100.0, 2),
        "max_drawdown_duration_days": round(duration_days, 2) if duration_days is not None else None,
    }


def _risk_adjusted_score(stats):
    sharpe = _metric(stats, "Sharpe Ratio", default=None)
    sortino = _metric(stats, "Sortino Ratio", default=None)
    sharpe_score = _piecewise_score(
        sharpe,
        [(-0.5, 0.0), (0, 0.10), (0.5, 0.35), (0.8, 0.60), (1.5, 1.0), (2.0, 1.0)],
    )
    sortino_score = _piecewise_score(
        sortino,
        [(-0.5, 0.0), (0, 0.10), (0.8, 0.50), (1.5, 0.75), (2.5, 1.0), (3.5, 1.0)],
    )
    score = (sharpe_score * 0.55) + (sortino_score * 0.45)
    return score * 100.0, {
        "sharpe_score": round(sharpe_score * 100.0, 2),
        "sortino_score": round(sortino_score * 100.0, 2),
    }


def _trade_quality_score(stats, trade_summary):
    trades = _metric(stats, "# Trades", default=None)
    if trades is None and isinstance(trade_summary, dict):
        trades = _num(trade_summary.get("count_total"), default=None)

    profit_factor = _metric(stats, "Profit Factor", default=None)
    expectancy = _metric(stats, "Expectancy [%]", default=None)
    sqn = _metric(stats, "SQN", default=None)
    win_rate = _metric(stats, "Win Rate [%]", default=None)

    pf_score = _piecewise_score(
        profit_factor,
        [(0, 0.0), (1.0, 0.25), (1.5, 0.55), (2.5, 0.80), (5.0, 1.0), (10.0, 1.0)],
    )
    expectancy_score = _piecewise_score(
        expectancy,
        [(-10, 0.0), (0, 0.20), (5, 0.45), (15, 0.75), (30, 1.0), (60, 1.0)],
    )
    sqn_score = _piecewise_score(
        sqn,
        [(-1, 0.0), (0, 0.10), (1, 0.40), (2, 0.75), (3, 1.0), (5, 1.0)],
    )
    win_rate_score = _piecewise_score(
        win_rate,
        [(0, 0.0), (25, 0.30), (40, 0.65), (55, 0.90), (70, 1.0), (90, 1.0)],
    )
    trades_score = _piecewise_score(
        trades,
        [(0, 0.0), (5, 0.15), (10, 0.35), (20, 1.0), (100, 1.0), (200, 0.70), (400, 0.35)],
    )

    score = (
        pf_score * 0.30
        + expectancy_score * 0.25
        + sqn_score * 0.25
        + win_rate_score * 0.10
        + trades_score * 0.10
    )
    return score * 100.0, {
        "profit_factor_score": round(pf_score * 100.0, 2),
        "expectancy_score": round(expectancy_score * 100.0, 2),
        "sqn_score": round(sqn_score * 100.0, 2),
        "win_rate_score": round(win_rate_score * 100.0, 2),
        "trade_count_score": round(trades_score * 100.0, 2),
    }


def _robustness_score(stats, config, trade_summary):
    trades = _metric(stats, "# Trades", default=None)
    if trades is None and isinstance(trade_summary, dict):
        trades = _num(trade_summary.get("count_total"), default=None)
    exposure = _metric(stats, "Exposure Time [%]", default=None)

    trade_count_score = _piecewise_score(
        trades,
        [(0, 0.0), (5, 0.10), (10, 0.35), (20, 1.0), (100, 1.0), (200, 0.75), (400, 0.40)],
    )
    exposure_score = _piecewise_score(
        exposure,
        [(0, 0.10), (10, 0.35), (20, 0.80), (25, 1.0), (50, 1.0), (75, 0.70), (95, 0.25), (100, 0.10)],
        default=0.7,
    )
    commission_score, commission_ratio = _commission_drag_score(stats, config)
    winner_score, winner_ratio = _single_winner_score(stats)

    max_dd = abs(_metric(stats, "Max. Drawdown [%]", default=100.0))
    drawdown_score = _piecewise_score(
        max_dd,
        [(0, 1.0), (35, 1.0), (50, 0.45), (70, 0.10), (90, 0.0)],
    )

    score = (
        trade_count_score * 0.25
        + exposure_score * 0.25
        + commission_score * 0.20
        + winner_score * 0.20
        + drawdown_score * 0.10
    )
    return score * 100.0, {
        "trade_count_robustness_score": round(trade_count_score * 100.0, 2),
        "exposure_score": round(exposure_score * 100.0, 2),
        "commission_drag_score": round(commission_score * 100.0, 2),
        "single_winner_dependency_score": round(winner_score * 100.0, 2),
        "drawdown_robustness_score": round(drawdown_score * 100.0, 2),
        "commission_to_net_profit_ratio": round(commission_ratio, 4) if commission_ratio is not None else None,
        "best_trade_to_total_return_ratio": round(winner_ratio, 4) if winner_ratio is not None else None,
    }


def _calculate_penalties(stats, config, trade_summary):
    penalties = {}
    total_return = _metric(stats, "Return [%]", default=None)
    buy_hold_return = _metric(stats, "Buy & Hold Return [%]", default=None)
    max_dd = abs(_metric(stats, "Max. Drawdown [%]", default=0.0))
    trades = _metric(stats, "# Trades", default=None)
    if trades is None and isinstance(trade_summary, dict):
        trades = _num(trade_summary.get("count_total"), default=None)
    exposure = _metric(stats, "Exposure Time [%]", default=None)
    _commission_score, commission_ratio = _commission_drag_score(stats, config)
    _winner_score, winner_ratio = _single_winner_score(stats)

    if trades is not None:
        if trades < 10:
            penalties["few_trades"] = round((10 - trades) / 10 * 12.0, 2)
        elif trades < 20:
            penalties["limited_trades"] = round((20 - trades) / 10 * 5.0, 2)

    if max_dd > 50:
        penalties["excessive_drawdown"] = round(min(15.0, (max_dd - 50) / 40 * 15.0), 2)
    elif max_dd > 35:
        penalties["aggressive_drawdown"] = round((max_dd - 35) / 15 * 5.0, 2)

    if exposure is not None:
        if exposure < 10:
            penalties["too_little_exposure"] = round((10 - exposure) / 10 * 8.0, 2)
        elif exposure > 80:
            penalties["too_much_exposure"] = round(min(10.0, (exposure - 80) / 20 * 10.0), 2)

    if commission_ratio is not None and commission_ratio > 0.30:
        penalties["high_commission_drag"] = round(min(8.0, (commission_ratio - 0.30) / 0.45 * 8.0), 2)

    if winner_ratio is not None and winner_ratio > 0.50:
        penalties["single_winner_dependency"] = round(min(8.0, (winner_ratio - 0.50) / 0.50 * 8.0), 2)

    if (
        total_return is not None
        and buy_hold_return is not None
        and buy_hold_return > 0
        and total_return / buy_hold_return < 0.30
        and max_dd > 35
    ):
        underperformance = 0.30 - (total_return / buy_hold_return)
        penalties["weak_vs_buy_hold_with_high_drawdown"] = round(min(10.0, underperformance / 0.30 * 10.0), 2)

    return penalties


def _grade(score):
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _summary(score, grade, components, penalties):
    weakest_component = min(components, key=components.get)
    strongest_component = max(components, key=components.get)
    penalty_text = " No major explicit penalties were applied."
    if penalties:
        main_penalty = max(penalties, key=penalties.get)
        penalty_text = f" Main penalty: {main_penalty.replace('_', ' ')}."
    return (
        f"Strategy Quality Score is {score:.1f}/100 ({grade}). "
        f"Strongest area: {strongest_component.replace('_', ' ')}. "
        f"Weakest area: {weakest_component.replace('_', ' ')}."
        f"{penalty_text}"
    )


def calculate_strategy_quality_score(backtest_json: dict) -> dict:
    """
    Calculate a 0-100 quality score for a crypto spot long-only backtest.

    Accepts either a full exported AI-analysis JSON payload or the inner
    context_sent_to_openai dict. Component scores are capped so one outlier
    metric, such as a very high Profit Factor, cannot dominate the result.
    """
    context = _extract_context(backtest_json)
    stats = context.get("stats", {}) if isinstance(context, dict) else {}
    config = context.get("config", {}) if isinstance(context, dict) else {}
    trade_summary = context.get("trade_summary", {}) if isinstance(context, dict) else {}

    return_score, return_details = _return_score(stats)
    risk_score, risk_details = _risk_score(stats)
    risk_adjusted_score, risk_adjusted_details = _risk_adjusted_score(stats)
    trade_quality_score, trade_quality_details = _trade_quality_score(stats, trade_summary)
    robustness_score, robustness_details = _robustness_score(stats, config, trade_summary)

    components = {
        "return_score": round(return_score, 2),
        "risk_score": round(risk_score, 2),
        "risk_adjusted_score": round(risk_adjusted_score, 2),
        "trade_quality_score": round(trade_quality_score, 2),
        "robustness_score": round(robustness_score, 2),
    }
    weights = _extract_weights(config)
    weighted_score = sum(components[name] * weights[name] for name in weights)
    penalties = _calculate_penalties(stats, config, trade_summary)
    penalty_total = min(30.0, sum(penalties.values()))
    final_score = round(_clamp(weighted_score - penalty_total, 0.0, 100.0), 2)
    grade = _grade(final_score)

    return {
        "score": final_score,
        "grade": grade,
        "components": components,
        "component_details": {
            "return": return_details,
            "risk": risk_details,
            "risk_adjusted": risk_adjusted_details,
            "trade_quality": trade_quality_details,
            "robustness": robustness_details,
        },
        "weights": {key: round(value, 4) for key, value in weights.items()},
        "weights_pct": {key: round(value * 100.0, 2) for key, value in weights.items()},
        "weighted_score_before_penalties": round(weighted_score, 2),
        "penalties": penalties,
        "penalty_total": round(penalty_total, 2),
        "summary": _summary(final_score, grade, components, penalties),
    }


def rank_backtests(backtests: list[dict]) -> list[dict]:
    """Score and rank backtest JSON payloads from best to worst."""
    ranked = []
    for index, backtest in enumerate(backtests):
        result = calculate_strategy_quality_score(backtest)
        context = _extract_context(backtest)
        strategy = context.get("strategy", {}) if isinstance(context, dict) else {}
        ranked.append(
            {
                "rank": None,
                "index": index,
                "score": result["score"],
                "grade": result["grade"],
                "strategy": strategy,
                "quality_score": result,
                "backtest": backtest,
            }
        )

    ranked.sort(key=lambda item: item["score"], reverse=True)
    for rank, item in enumerate(ranked, start=1):
        item["rank"] = rank
    return ranked
