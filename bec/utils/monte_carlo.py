import argparse
import html
import json
import math
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go

import bec.my_backtesting as my_backtesting
import bec.utils.database as database
from bec.utils.take_profit import normalize_take_profit_levels, take_profit_enabled
from backtesting.lib import FractionalBacktest


METHOD_TRADE_SHUFFLE = "trade_order_shuffle"
METHOD_CANDLES = "candles_based"
METHOD_LABELS = {
    METHOD_TRADE_SHUFFLE: "Monte Carlo Trades",
    METHOD_CANDLES: "Monte Carlo Candles",
}
MIN_TRADE_SHUFFLE_TRADES = 10
DEFAULT_CANDLE_PERTURB_MIN_PCT = 0.1
DEFAULT_CANDLE_PERTURB_MAX_PCT = 0.5

OUTPUT_DIR = os.path.join(
    my_backtesting.PROJECT_ROOT,
    my_backtesting.FOLDER_BACKTEST_RESULTS_URL,
    "monte_carlo",
)


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
    except TypeError:
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _pct(value):
    value = _safe_float(value, 0.0)
    return f"{value:.1f}%"


def _num(value):
    value = _safe_float(value, 0.0)
    return f"{value:.2f}"


def _value_tone(value, invert=False, neutral_zero=True):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "neutral"
    if neutral_zero and abs(number) < 1e-12:
        return "neutral"
    is_good = number < 0 if invert else number > 0
    return "positive" if is_good else "negative"


def _metric_card(kind, label, value, note="", tone="neutral"):
    return (
        f"<div class='bec-perf-card bec-perf-card-{kind} bec-tone-{tone}'>"
        f"<span>{html.escape(str(label))}</span><strong>{html.escape(str(value))}</strong>"
        f"{f'<small>{html.escape(str(note))}</small>' if note else ''}"
        "</div>"
    )


def _metric_format(metric, value):
    if metric in {"Net Profit", "Max Drawdown", "Win Rate", "Annual Return", "Expectancy"}:
        return _pct(value)
    if metric == "Total Trades":
        return f"{_safe_float(value, 0.0):.0f}"
    return _num(value)


def _max_drawdown(equity):
    equity = np.asarray(equity, dtype=float)
    if equity.size == 0:
        return 0.0
    peaks = np.maximum.accumulate(equity)
    drawdowns = np.where(peaks > 0, (equity - peaks) / peaks * 100.0, 0.0)
    return abs(float(np.nanmin(drawdowns)))


def _sharpe(returns):
    returns = np.asarray(returns, dtype=float)
    returns = returns[np.isfinite(returns)]
    if returns.size < 2:
        return 0.0
    std = np.std(returns, ddof=1)
    if std <= 0:
        return 0.0
    return float(np.mean(returns) / std * np.sqrt(returns.size))


def _calmar(net_profit_pct, max_drawdown_pct):
    max_drawdown_pct = abs(_safe_float(max_drawdown_pct, 0.0))
    if max_drawdown_pct <= 0:
        return 0.0
    return float(_safe_float(net_profit_pct, 0.0) / max_drawdown_pct)


def _expectancy(returns_pct):
    returns_pct = np.asarray(returns_pct, dtype=float)
    returns_pct = returns_pct[np.isfinite(returns_pct)]
    if returns_pct.size == 0:
        return 0.0
    return float(np.mean(returns_pct))


def _trade_metrics(equity, returns_pct):
    equity = np.asarray(equity, dtype=float)
    returns_pct = np.asarray(returns_pct, dtype=float)
    initial = float(equity[0]) if equity.size else 0.0
    final = float(equity[-1]) if equity.size else initial
    net_profit = ((final / initial) - 1.0) * 100.0 if initial > 0 else 0.0
    max_dd = _max_drawdown(equity)
    return {
        "Net Profit": net_profit,
        "Max Drawdown": max_dd,
        "Sharpe Ratio": _sharpe(returns_pct / 100.0),
        "Win Rate": float((returns_pct > 0).mean() * 100.0) if returns_pct.size else 0.0,
        "Total Trades": float(returns_pct.size),
        "Annual Return": net_profit,
        "Calmar Ratio": _calmar(net_profit, max_dd),
        "Expectancy": _expectancy(returns_pct),
    }


def _annualized_return_from_dates(net_profit_pct, start_date, end_date):
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start) or pd.isna(end) or end <= start:
        return None
    years = (end - start).total_seconds() / (365.25 * 24 * 60 * 60)
    if years <= 0:
        return None
    total_return = 1.0 + (_safe_float(net_profit_pct, 0.0) / 100.0)
    if total_return <= 0:
        return -100.0
    return (math.pow(total_return, 1.0 / years) - 1.0) * 100.0


def _pnl_metrics(equity, pnl_values, annual_return_pct=None):
    equity = np.asarray(equity, dtype=float)
    pnl_values = np.asarray(pnl_values, dtype=float)
    initial = float(equity[0]) if equity.size else 0.0
    final = float(equity[-1]) if equity.size else initial
    net_profit = ((final / initial) - 1.0) * 100.0 if initial > 0 else 0.0
    max_dd = _max_drawdown(equity)
    returns = np.where(initial > 0, pnl_values / initial, 0.0)
    annual_return = _safe_float(annual_return_pct, net_profit)
    return {
        "Net Profit": net_profit,
        "Max Drawdown": max_dd,
        "SQN": _sharpe(returns),
        "Win Rate": float((pnl_values > 0).mean() * 100.0) if pnl_values.size else 0.0,
        "Total Trades": float(pnl_values.size),
        "Annual Return": annual_return,
        "Calmar Ratio": _calmar(annual_return, max_dd),
        "Expectancy": float(np.mean(returns) * 100.0) if returns.size else 0.0,
    }


def _stats_metric(stats, key, default=0.0):
    try:
        return _safe_float(stats[key], default)
    except Exception:
        return default


def _stats_to_metrics(stats):
    return {
        "Net Profit": _stats_metric(stats, "Return [%]"),
        "Max Drawdown": abs(_stats_metric(stats, "Max. Drawdown [%]")),
        "Sharpe Ratio": _stats_metric(stats, "Sharpe Ratio"),
        "Win Rate": _stats_metric(stats, "Win Rate [%]"),
        "Total Trades": _stats_metric(stats, "# Trades"),
        "Annual Return": _stats_metric(stats, "Return (Ann.) [%]", _stats_metric(stats, "Return [%]")),
        "Calmar Ratio": _stats_metric(stats, "Calmar Ratio"),
        "Expectancy": _stats_metric(stats, "Expectancy [%]"),
    }


def _backtest_row_to_trade_original_metrics(row, fallback_metrics):
    metrics = dict(fallback_metrics or {})
    net_profit = _safe_float(row.get("Return_Perc"), metrics.get("Net Profit", 0.0))
    max_drawdown = abs(_safe_float(row.get("Max_Drawdown_Perc"), metrics.get("Max Drawdown", 0.0)))
    annual_return = _annualized_return_from_dates(
        net_profit,
        row.get("Backtest_Start_Date"),
        row.get("Backtest_End_Date"),
    )
    annual_return = _safe_float(annual_return, metrics.get("Annual Return", net_profit))

    metrics["Net Profit"] = net_profit
    metrics["Max Drawdown"] = max_drawdown
    metrics["Win Rate"] = _safe_float(row.get("Win_Rate_Perc"), metrics.get("Win Rate", 0.0))
    metrics["Total Trades"] = _safe_float(row.get("Trades"), metrics.get("Total Trades", 0.0))
    metrics["Annual Return"] = annual_return
    metrics["Calmar Ratio"] = _calmar(annual_return, max_drawdown)
    metrics["Expectancy"] = _safe_float(row.get("Expectancy_Perc"), metrics.get("Expectancy", 0.0))
    metrics["SQN"] = _safe_float(row.get("SQN"), metrics.get("SQN", 0.0))
    metrics.pop("Sharpe Ratio", None)
    return metrics


def _official_trade_original_metrics(symbol, timeframe, strategy_id, fallback_metrics):
    try:
        result = database.get_backtesting_results_by_symbol_timeframe_strategy(symbol, timeframe, strategy_id)
    except Exception:
        return fallback_metrics
    if result is None or result.empty:
        return fallback_metrics
    return _backtest_row_to_trade_original_metrics(result.iloc[0], fallback_metrics)


def _stats_equity_curve(stats, fallback_initial):
    curve = stats.get("_equity_curve") if hasattr(stats, "get") else None
    if isinstance(curve, pd.DataFrame) and "Equity" in curve.columns:
        equity = pd.to_numeric(curve["Equity"], errors="coerce").dropna().astype(float)
        if not equity.empty:
            return equity.tolist()
    return [float(fallback_initial)]


def _summarize_metrics(original_metrics, scenario_metrics):
    preferred_metric_names = [
        "Net Profit",
        "Max Drawdown",
        "Sharpe Ratio",
        "SQN",
        "Win Rate",
        "Total Trades",
        "Annual Return",
        "Calmar Ratio",
        "Expectancy",
    ]
    available_metric_names = set(original_metrics or {})
    for item in scenario_metrics:
        available_metric_names.update(item or {})
    metric_names = [metric for metric in preferred_metric_names if metric in available_metric_names]
    metric_names.extend(sorted(available_metric_names.difference(metric_names)))

    summary = {}
    for metric in metric_names:
        values = np.asarray(
            [_safe_float(item.get(metric), np.nan) for item in scenario_metrics],
            dtype=float,
        )
        values = values[np.isfinite(values)]
        if values.size == 0:
            summary[metric] = {
                "original": _safe_float(original_metrics.get(metric), 0.0),
                "worst_5": None,
                "median": None,
                "best_5": None,
            }
            continue

        if metric == "Max Drawdown":
            worst = np.percentile(values, 95)
            best = np.percentile(values, 5)
        else:
            worst = np.percentile(values, 5)
            best = np.percentile(values, 95)
        summary[metric] = {
            "original": _safe_float(original_metrics.get(metric), 0.0),
            "worst_5": float(worst),
            "median": float(np.percentile(values, 50)),
            "best_5": float(best),
        }
    return summary


def _robustness_score(metrics, valid_scenarios, total_scenarios, min_sample_ok=True):
    if total_scenarios <= 0 or valid_scenarios <= 0:
        return 0.0
    if not min_sample_ok:
        return 0.0
    valid_ratio = valid_scenarios / total_scenarios
    net = metrics.get("Net Profit", {})
    dd = metrics.get("Max Drawdown", {})
    original_return = _safe_float(net.get("original"), 0.0)
    median_return = _safe_float(net.get("median"), 0.0)
    worst_return = _safe_float(net.get("worst_5"), 0.0)
    original_dd = abs(_safe_float(dd.get("original"), 0.0))
    worst_dd = abs(_safe_float(dd.get("worst_5"), 0.0))

    median_component = 50.0 if original_return == 0 else max(0.0, min(100.0, (median_return / abs(original_return)) * 100.0))
    tail_component = 100.0 if original_return <= 0 else max(0.0, min(100.0, ((worst_return + original_return) / (2.0 * original_return)) * 100.0))
    drawdown_component = 100.0 if worst_dd <= original_dd else max(0.0, min(100.0, (original_dd / worst_dd) * 100.0))
    return round(((median_component * 0.55) + (tail_component * 0.25) + (drawdown_component * 0.20)) * valid_ratio, 2)


def _interpretation(score, valid_scenarios, total_scenarios, method, min_sample_ok=True):
    if not min_sample_ok:
        return "Insufficient trades" if method == METHOD_TRADE_SHUFFLE else "Insufficient scenarios"
    if total_scenarios <= 0 or valid_scenarios <= 0:
        return "Insufficient scenarios"
    if valid_scenarios / total_scenarios < 0.5:
        return "Insufficient scenarios"
    if score >= 75:
        return "Robust"
    if method == METHOD_CANDLES and score < 45:
        return "Market-path fragile"
    if score < 55:
        return "Sequence-sensitive"
    return "Moderate robustness"


def _target_slug(exchange_code, symbol, timeframe, strategy_id, method):
    cleaned = f"{exchange_code}-{symbol}-{strategy_id}-{timeframe}-{method}"
    return "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in cleaned)


def _build_equity_figure(title, original_curve, scenario_curves, initial_cash):
    fig = go.Figure()
    max_points = max([len(original_curve)] + [len(curve) for curve in scenario_curves[:150]] or [1])
    x_original = list(range(len(original_curve)))

    for curve in scenario_curves[:150]:
        fig.add_trace(
            go.Scatter(
                x=list(range(len(curve))),
                y=curve,
                mode="lines",
                line={"color": "rgba(37, 99, 235, 0.18)", "width": 1},
                hoverinfo="skip",
                showlegend=False,
            )
        )

    fig.add_trace(
        go.Scatter(
            x=x_original,
            y=original_curve,
            mode="lines",
            name="Original Strategy",
            line={"color": "#10b981", "width": 3},
            hovertemplate="Step %{x}<br>Equity %{y:,.2f}<extra></extra>",
        )
    )
    fig.add_hline(
        y=float(initial_cash),
        line_dash="dot",
        line_color="rgba(100, 116, 139, 0.55)",
    )
    fig.update_layout(
        title="",
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=430,
        margin={"l": 40, "r": 30, "t": 20, "b": 40},
        legend={"orientation": "h", "y": 1.05},
        xaxis_title="Step",
        yaxis_title="Equity",
        xaxis={"range": [0, max_points - 1]},
    )
    return fig


def _metrics_table(metrics):
    rows = []
    for metric, values in metrics.items():
        rows.append(
            {
                "Metric": metric,
                "Original": _metric_format(metric, values.get("original")),
                "Worst 5%": _metric_format(metric, values.get("worst_5")),
                "Median": _metric_format(metric, values.get("median")),
                "Best 5%": _metric_format(metric, values.get("best_5")),
            }
        )
    return pd.DataFrame(rows)


def _monte_carlo_report_style():
    return """
    :root {
      --bec-bg: #f5f7fb;
      --bec-surface: #ffffff;
      --bec-surface-soft: #f8fafc;
      --bec-border: #dbe4ee;
      --bec-text: #172033;
      --bec-muted: #64748b;
      --bec-green: #10b981;
      --bec-blue: #2563eb;
      --bec-red: #ef4444;
      --bec-amber: #f59e0b;
    }
    html[data-bec-theme="dark"] {
      --bec-bg: #0f1018;
      --bec-surface: #151620;
      --bec-surface-soft: #1c1e2b;
      --bec-border: #25283a;
      --bec-text: #f4f7ff;
      --bec-muted: #8993bd;
      --bec-green: #00e596;
      --bec-blue: #7c7cff;
      --bec-red: #ff4444;
      --bec-amber: #facc15;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bec-bg);
      color: var(--bec-text);
      font-family: "Segoe UI", Helvetica, Arial, sans-serif;
    }
    .bec-report-shell,
    .bec-panel,
    .bec-chart-card {
      width: min(96%, 1500px);
      margin: 22px auto;
      background: var(--bec-surface);
      border: 1px solid var(--bec-border);
      border-radius: 18px;
    }
    .bec-report-shell { padding: 22px; }
    .bec-panel,
    .bec-chart-card { padding: 18px; }
    .bec-topbar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
    }
    .bec-kicker {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 0 0 8px 0;
      color: var(--bec-muted);
      font-size: 12px;
      font-weight: 750;
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }
    .bec-kicker span {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--bec-green);
      box-shadow: 0 0 0 5px rgba(16, 185, 129, 0.13);
    }
    .bec-topbar h1 {
      margin: 0;
      color: var(--bec-text);
      font-size: clamp(34px, 5vw, 56px);
      line-height: 0.95;
      letter-spacing: 0;
    }
    .bec-subtitle {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
      margin: 10px 0 0 0;
      color: var(--bec-muted);
      font-size: 14px;
    }
    .bec-subtitle strong { color: var(--bec-text); }
    .bec-subtitle span {
      display: inline-flex;
      align-items: center;
      border: 1px solid var(--bec-border);
      border-radius: 999px;
      padding: 3px 9px;
      background: var(--bec-surface-soft);
      color: var(--bec-blue);
      font-weight: 700;
    }
    .bec-report-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .bec-share-menu { position: relative; }
    .bec-share-toggle,
    .bec-theme-toggle {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 36px;
      height: 36px;
      border: 1px solid var(--bec-border);
      border-radius: 999px;
      background: var(--bec-surface-soft);
      color: var(--bec-text);
      cursor: pointer;
      padding: 0;
    }
    .bec-share-toggle:hover,
    .bec-theme-toggle:hover {
      border-color: var(--bec-blue);
      color: var(--bec-blue);
    }
    .material-symbols-outlined {
      font-size: 20px;
      font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 24;
      line-height: 1;
    }
    .bec-share-options {
      position: absolute;
      top: calc(100% + 8px);
      right: 0;
      z-index: 20;
      display: none;
      min-width: 160px;
      overflow: hidden;
      border: 1px solid var(--bec-border);
      border-radius: 10px;
      background: var(--bec-surface);
      box-shadow: 0 14px 32px rgba(15, 23, 42, 0.14);
    }
    .bec-share-menu.is-open .bec-share-options,
    .bec-share-menu:focus-within .bec-share-options { display: block; }
    .bec-share-options button {
      display: block;
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--bec-text);
      cursor: pointer;
      font-size: 12px;
      font-weight: 750;
      padding: 10px 12px;
      text-align: left;
      white-space: nowrap;
    }
    .bec-share-options button:hover {
      background: var(--bec-surface-soft);
      color: var(--bec-blue);
    }
    .bec-performance-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .bec-perf-card {
      min-height: 108px;
      border: 1px solid var(--bec-border);
      border-radius: 14px;
      background: var(--bec-surface-soft);
      padding: 18px;
    }
    .bec-perf-card span,
    .bec-chart-card header span,
    .bec-panel h2 {
      display: block;
      color: var(--bec-muted);
      font-size: 11px;
      font-weight: 850;
      letter-spacing: 0.13em;
      text-transform: uppercase;
    }
    .bec-perf-card strong {
      display: block;
      margin-top: 10px;
      color: var(--bec-text);
      font-size: 25px;
      line-height: 1;
      letter-spacing: 0;
    }
    .bec-perf-card small {
      display: block;
      margin-top: 12px;
      color: var(--bec-muted);
      font-size: 12px;
      line-height: 1.35;
    }
    .bec-tone-positive strong { color: var(--bec-green); }
    .bec-tone-negative strong { color: var(--bec-red); }
    .bec-tone-neutral strong { color: var(--bec-text); }
    .bec-chart-card header {
      display: flex;
      align-items: baseline;
      gap: 8px;
      margin-bottom: 4px;
    }
    .bec-chart-card header strong {
      color: var(--bec-text);
      font-size: 13px;
      letter-spacing: 0.10em;
      text-transform: uppercase;
    }
    .bec-panel h2 { margin: 0 0 16px 0; }
    .bec-table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      overflow: hidden;
      border: 1px solid var(--bec-border);
      border-radius: 12px;
      font-size: 13px;
    }
    .bec-table th {
      background: #eef4fb;
      color: #475569;
      font-weight: 750;
      text-align: left;
      border-bottom: 1px solid var(--bec-border);
      padding: 9px 11px;
      white-space: nowrap;
    }
    .bec-table td {
      border-bottom: 1px solid #edf2f7;
      padding: 8px 11px;
      white-space: nowrap;
    }
    .bec-table tbody tr:hover { background-color: #f8fafc; }
    html[data-bec-theme="dark"] .bec-table th {
      background: #1c1e2b;
      color: var(--bec-muted);
    }
    html[data-bec-theme="dark"] .bec-table td {
      border-color: var(--bec-border);
    }
    html[data-bec-theme="dark"] .bec-table tbody tr:hover {
      background-color: #1b1d2a;
    }
    @media screen and (max-width: 1000px) {
      .bec-performance-grid { grid-template-columns: 1fr; }
      .bec-topbar { flex-direction: column; }
      .bec-report-actions { align-self: flex-end; }
    }
    @media print {
      @page { size: landscape; margin: 10mm; }
      * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
      .bec-report-actions { display: none !important; }
      body { background: #ffffff !important; }
    }
    """


def _monte_carlo_report_script():
    return """
    <script>
      (function () {
        const themeStorageKey = "bec-report-theme-v2";
        const savedTheme = localStorage.getItem(themeStorageKey) || "light";
        document.documentElement.setAttribute("data-bec-theme", savedTheme);
        window.addEventListener("DOMContentLoaded", function () {
          function updateThemeLabels(theme) {
            document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
              button.innerHTML = `<span class="material-symbols-outlined">${theme === "dark" ? "light_mode" : "dark_mode"}</span>`;
              button.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
              button.setAttribute("title", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
            });
          }
          updateThemeLabels(savedTheme);
          document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
            button.addEventListener("click", function () {
              const current = document.documentElement.getAttribute("data-bec-theme") || "light";
              const next = current === "dark" ? "light" : "dark";
              document.documentElement.setAttribute("data-bec-theme", next);
              localStorage.setItem(themeStorageKey, next);
              updateThemeLabels(next);
            });
          });
          document.addEventListener("click", function (event) {
            const shareToggle = event.target.closest("[data-share-toggle]");
            if (shareToggle) {
              event.stopPropagation();
              const menu = shareToggle.closest(".bec-share-menu");
              const isOpen = menu?.classList.toggle("is-open");
              shareToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
              return;
            }
            if (event.target.closest("[data-download-html]")) {
              event.target.closest(".bec-share-menu")?.classList.remove("is-open");
              const html = "<!doctype html>\\n" + document.documentElement.outerHTML;
              const blob = new Blob([html], {type: "text/html;charset=utf-8;"});
              const url = URL.createObjectURL(blob);
              const link = document.createElement("a");
              const title = document.querySelector(".bec-topbar h1")?.textContent?.trim() || "monte-carlo";
              link.href = url;
              link.download = `${title}-monte-carlo.html`.replace(/[^a-z0-9._-]+/gi, "_");
              document.body.appendChild(link);
              link.click();
              link.remove();
              window.setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
              return;
            }
            if (event.target.closest("[data-export-pdf]")) {
              event.target.closest(".bec-share-menu")?.classList.remove("is-open");
              window.print();
              return;
            }
            document.querySelectorAll(".bec-share-menu.is-open").forEach(function (menu) {
              menu.classList.remove("is-open");
              menu.querySelector("[data-share-toggle]")?.setAttribute("aria-expanded", "false");
            });
          });
        });
      })();
    </script>
    """


def _write_outputs(result):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    slug = _target_slug(
        result["exchange_code"],
        result["symbol"],
        result["timeframe"],
        result["strategy_id"],
        result["method"],
    )
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    base = f"{slug}-{timestamp}"
    html_path = os.path.join(OUTPUT_DIR, f"{base}.html")
    csv_path = os.path.join(OUTPUT_DIR, f"{base}.csv")
    json_path = os.path.join(OUTPUT_DIR, f"{base}.json")

    table = _metrics_table(result["metrics"])
    table.to_csv(csv_path, index=False)
    result["html_path"] = os.path.relpath(html_path, my_backtesting.PROJECT_ROOT)
    result["csv_path"] = os.path.relpath(csv_path, my_backtesting.PROJECT_ROOT)
    result["json_path"] = os.path.relpath(json_path, my_backtesting.PROJECT_ROOT)
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=True)

    fig = _build_equity_figure(
        METHOD_LABELS.get(result["method"], "Monte Carlo"),
        result["original_equity_curve"],
        result["scenario_equity_curves"],
        result["initial_cash"],
    )
    fig_html = fig.to_html(
        full_html=False,
        include_plotlyjs="cdn",
        config={"displaylogo": False, "responsive": True},
    )
    summary = result["summary"]
    table_html = table.to_html(index=False, escape=False, classes="bec-table bec-compact-table")
    net_metrics = result.get("metrics", {}).get("Net Profit", {})
    dd_metrics = result.get("metrics", {}).get("Max Drawdown", {})
    original_return = _safe_float(net_metrics.get("original"), 0.0)
    median_return = _safe_float(net_metrics.get("median"), 0.0)
    worst_return = _safe_float(net_metrics.get("worst_5"), 0.0)
    worst_drawdown = abs(_safe_float(dd_metrics.get("worst_5"), 0.0))
    robustness_score = _safe_float(summary.get("robustness_score"), 0.0)
    valid_pct = _safe_float(summary.get("valid_pct"), 0.0)
    performance_cards = "".join(
        [
            _metric_card("robustness", "Robustness", f"{robustness_score:.1f}", str(summary.get("interpretation", "n/a")), _value_tone(robustness_score - 55.0)),
            _metric_card("valid", "Valid Scenarios", f"{int(summary.get('valid_scenarios', 0)):,}", f"{valid_pct:.1f}% of {int(summary.get('total_scenarios', 0)):,}", _value_tone(valid_pct - 50.0)),
            _metric_card("return", "Original Return", f"{original_return:+.1f}%", "Original strategy result", _value_tone(original_return)),
            _metric_card("median", "Median Return", f"{median_return:+.1f}%", "Median synthetic scenario", _value_tone(median_return)),
            _metric_card("tail", "Worst 5% Return", f"{worst_return:+.1f}%", "Lower-tail scenario return", _value_tone(worst_return)),
            _metric_card("drawdown", "Worst 5% Drawdown", f"{worst_drawdown:.1f}%", "Higher drawdown is worse", _value_tone(worst_drawdown, invert=True)),
        ]
    )
    title = METHOD_LABELS.get(result["method"], "Monte Carlo")
    escaped_title = html.escape(title)
    escaped_symbol = html.escape(str(result["symbol"]))
    escaped_timeframe = html.escape(str(result["timeframe"]))
    escaped_strategy = html.escape(str(result["strategy_id"]))
    escaped_seed = html.escape(str(result.get("seed", "")))
    report_html = f"""
<!doctype html>
<html data-bec-theme="light">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escaped_title}</title>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined" />
  <style>{_monte_carlo_report_style()}</style>
</head>
<body>
  <section class="bec-report-shell">
    <header class="bec-topbar">
      <div>
        <p class="bec-kicker"><span></span>Monte Carlo Report</p>
        <h1>{escaped_symbol}</h1>
        <p class="bec-subtitle"><strong>{escaped_strategy}</strong><span>{escaped_timeframe}</span><span>{escaped_title}</span><span>Seed {escaped_seed}</span></p>
      </div>
      <div class="bec-report-actions">
        <div class="bec-share-menu">
          <button class="bec-share-toggle" type="button" data-share-toggle aria-haspopup="true" aria-expanded="false" aria-label="Share report" title="Share report"><span class="material-symbols-outlined">share</span></button>
          <div class="bec-share-options" data-share-options>
            <button type="button" data-download-html>Download HTML</button>
            <button type="button" data-export-pdf>Export PDF</button>
          </div>
        </div>
        <button class="bec-theme-toggle" type="button" data-theme-toggle aria-label="Toggle theme" title="Toggle theme"><span class="material-symbols-outlined">dark_mode</span></button>
      </div>
    </header>
    <div class="bec-performance-grid">{performance_cards}</div>
  </section>
  <section class="bec-chart-card">
    <header><strong>Equity Paths</strong><span>Original strategy versus simulated scenarios</span></header>
    {fig_html}
  </section>
  <section class="bec-panel">
    <h2>Monte Carlo Metrics</h2>
    {table_html}
  </section>
  {_monte_carlo_report_script()}
</body>
</html>
"""
    with open(html_path, "w", encoding="utf-8") as file:
        file.write(report_html)

    return result


def _build_result(
    symbol,
    timeframe,
    strategy_id,
    method,
    scenarios,
    seed,
    initial_cash,
    original_curve,
    original_metrics,
    scenario_curves,
    scenario_metrics,
    min_sample_ok=True,
    sample_note="",
):
    metrics = _summarize_metrics(original_metrics, scenario_metrics)
    valid = len(scenario_metrics)
    score = _robustness_score(metrics, valid, scenarios, min_sample_ok=min_sample_ok)
    result = {
        "exchange_code": database.get_active_exchange_code(),
        "symbol": str(symbol).upper(),
        "timeframe": str(timeframe),
        "strategy_id": str(strategy_id),
        "method": method,
        "scenarios": int(scenarios),
        "seed": int(seed),
        "initial_cash": float(initial_cash),
        "summary": {
            "valid_scenarios": int(valid),
            "total_scenarios": int(scenarios),
            "valid_pct": round((valid / scenarios) * 100.0, 2) if scenarios else 0.0,
            "robustness_score": score,
            "interpretation": _interpretation(score, valid, scenarios, method, min_sample_ok=min_sample_ok),
            "sample_note": str(sample_note or ""),
        },
        "metrics": metrics,
        "original_equity_curve": [float(value) for value in original_curve],
        "scenario_equity_curves": [[float(value) for value in curve] for curve in scenario_curves[:200]],
    }
    result = _write_outputs(result)
    database.upsert_monte_carlo_result(result)
    return result


def run_trade_order_shuffle(symbol, timeframe, strategy_id, scenarios=1000, seed=42):
    settings = database.get_backtesting_settings()
    if settings.get("Commission_Value") is None:
        raise RuntimeError("Configure an explicit exchange fee before Monte Carlo analysis")
    initial_cash = float(settings["Cash_Value"])
    trades = database.get_backtesting_trades_by_symbol_timeframe_strategy(symbol, timeframe, strategy_id)
    if trades.empty or "ReturnPct" not in trades.columns:
        return _build_result(
            symbol,
            timeframe,
            strategy_id,
            METHOD_TRADE_SHUFFLE,
            scenarios,
            seed,
            initial_cash,
            [initial_cash],
            {},
            [],
            [],
            min_sample_ok=False,
            sample_note="No completed trades found for this backtest.",
        )

    pnl_values = pd.to_numeric(trades.get("PnL"), errors="coerce").dropna().astype(float).to_numpy()
    if pnl_values.size == 0:
        return _build_result(
            symbol,
            timeframe,
            strategy_id,
            METHOD_TRADE_SHUFFLE,
            scenarios,
            seed,
            initial_cash,
            [initial_cash],
            {},
            [],
            [],
            min_sample_ok=False,
            sample_note="No valid trade returns found for this backtest.",
        )

    rng = np.random.default_rng(int(seed))
    original_pnl = pnl_values.copy()
    original_curve = [initial_cash]
    for pnl in original_pnl:
        original_curve.append(original_curve[-1] + float(pnl))
    original_metrics = _official_trade_original_metrics(
        symbol,
        timeframe,
        strategy_id,
        _pnl_metrics(original_curve, original_pnl),
    )
    annual_return = _safe_float(original_metrics.get("Annual Return"))

    scenario_curves = []
    scenario_metrics = []
    for index in range(int(scenarios)):
        shuffled = rng.permutation(pnl_values)
        equity = [initial_cash]
        for pnl in shuffled:
            equity.append(equity[-1] + float(pnl))
        scenario_curves.append(equity)
        scenario_metrics.append(_pnl_metrics(equity, shuffled, annual_return_pct=annual_return))
        if (index + 1) % 25 == 0 or index + 1 == int(scenarios):
            print(f"Running Trade Shuffle Simulation... ({index + 1}/{int(scenarios)})", flush=True)

    min_sample_ok = int(pnl_values.size) >= MIN_TRADE_SHUFFLE_TRADES
    sample_note = ""
    if not min_sample_ok:
        sample_note = (
            f"Trade-order shuffling has only {int(pnl_values.size)} completed trades. "
            f"Use at least {MIN_TRADE_SHUFFLE_TRADES} trades for a reliable robustness score."
        )

    return _build_result(
        symbol,
        timeframe,
        strategy_id,
        METHOD_TRADE_SHUFFLE,
        int(scenarios),
        int(seed),
        initial_cash,
        original_curve,
        original_metrics,
        scenario_curves,
        scenario_metrics,
        min_sample_ok=min_sample_ok,
        sample_note=sample_note,
    )


def _apply_saved_strategy_parameters(strategy, backtest_row):
    try:
        config = database.parse_strategy_params(backtest_row.get("Backtest_Config_JSON", ""))
    except Exception:
        config = {}
    strategy_parameters = (
        config.get("strategy_parameters") if isinstance(config, dict) else {}
    )
    parameters = (
        strategy_parameters.get("parameters")
        if isinstance(strategy_parameters, dict)
        else {}
    )
    if isinstance(parameters, dict) and parameters:
        current_values = dict(getattr(strategy, "parameter_values", {}) or {})
        for name, value in parameters.items():
            setattr(strategy, str(name), value)
            current_values[str(name)] = value
        strategy.parameter_values = current_values
        return

    try:
        n1 = int(float(backtest_row.get("Ema_Fast", 0) or 0))
        n2 = int(float(backtest_row.get("Ema_Slow", 0) or 0))
    except Exception:
        n1, n2 = 0, 0
    if n1 > 0:
        strategy.n1 = n1
        strategy.nFastHMA = n1
    if n2 > 0:
        strategy.n2 = n2
        strategy.nSlowHMA = n2


def _configure_strategy(strategy, strategy_id, timeframe):
    strategy_risk = database.get_strategy_risk(strategy_id)
    atr_risk = strategy_risk.get("atr_trailing", {}) if isinstance(strategy_risk, dict) else {}
    take_profits = normalize_take_profit_levels(strategy_risk.get("take_profits", []) if isinstance(strategy_risk, dict) else [])
    strategy.stop_loss_pct = float(strategy_risk.get("stop_loss_pct", 0.0) or 0.0)
    strategy.atr_trailing_enabled = bool(atr_risk.get("enabled", False))
    strategy.atr_period = int(atr_risk.get("period", 14) or 14)
    strategy.atr_multiplier = float(atr_risk.get("multiplier", 1.8) or 1.8)
    strategy.atr_activation_pnl = float(atr_risk.get("activation_pnl_pct", 0.0) or 0.0)
    strategy.take_profit_enabled = take_profit_enabled(take_profits)
    strategy.take_profits = take_profits
    for level in range(1, 5):
        tp = next((item for item in take_profits if int(item.get("level", 0) or 0) == level), {})
        setattr(strategy, f"take_profit_{level}", float(tp.get("pnl_pct", 0.0) or 0.0))
        setattr(strategy, f"take_profit_{level}_amount", float(tp.get("amount_pct", 0.0) or 0.0))
    strategy.use_daily_linreg_filter = False
    strategy.daily_linreg_timeframe = ""
    strategy.daily_linreg_alignment = ""
    strategy.execution_timeframe = str(timeframe)


def _prepare_backtest_df(df, symbol, timeframe, strategy_id, strategy):
    definition = getattr(strategy, "definition", None)
    if isinstance(definition, dict) and definition.get("engine") == "bec_strategy_ast_v2":
        strategy.execution_symbol = str(symbol)
        strategy.execution_timeframe = str(timeframe)
        return df
    if strategy_id == "hma_rsi_linreg":
        strategy.use_daily_linreg_filter = True
        strategy.daily_linreg_timeframe = "1d"
        linreg_period = int(getattr(strategy, "linreg_period", 50) or 50)
        if str(timeframe) == "1d":
            strategy.daily_linreg_alignment = "current_closed_candle"
            return my_backtesting.add_current_timeframe_linreg_filter(df, linreg_period=linreg_period)
        strategy.daily_linreg_alignment = "previous_closed_candle"
        return my_backtesting.add_daily_linreg_filter(df, symbol, linreg_period=linreg_period)
    return df


def _resolve_strategy(strategy_id):
    if hasattr(my_backtesting, "resolve_strategy"):
        strategy = my_backtesting.resolve_strategy(str(strategy_id))
        if strategy is not None:
            return strategy
    return getattr(my_backtesting, str(strategy_id), None)


def _run_strategy_on_df(df, strategy, cash, commission):
    bt = FractionalBacktest(
        df,
        strategy=strategy,
        cash=float(cash),
        commission=float(commission),
        finalize_trades=True,
        exclusive_orders=True,
        trade_on_close=True,
    )
    return bt.run()


def _candle_perturbation_bounds(settings):
    min_pct = _safe_float(
        settings.get("Monte_Carlo_Candle_Perturb_Min_Pct"),
        DEFAULT_CANDLE_PERTURB_MIN_PCT,
    )
    max_pct = _safe_float(
        settings.get("Monte_Carlo_Candle_Perturb_Max_Pct"),
        DEFAULT_CANDLE_PERTURB_MAX_PCT,
    )
    min_pct = max(0.0, float(min_pct))
    max_pct = max(0.0, float(max_pct))
    if min_pct > max_pct:
        min_pct, max_pct = max_pct, min_pct
    return min_pct / 100.0, max_pct / 100.0


def _perturb_candles(
    df,
    rng,
    min_pct=DEFAULT_CANDLE_PERTURB_MIN_PCT,
    max_pct=DEFAULT_CANDLE_PERTURB_MAX_PCT,
):
    base = df.copy()
    required_columns = ["Open", "High", "Low", "Close"]
    if any(column not in base.columns for column in required_columns):
        return pd.DataFrame()

    min_bound, max_bound = _candle_perturbation_bounds(
        {
            "Monte_Carlo_Candle_Perturb_Min_Pct": min_pct,
            "Monte_Carlo_Candle_Perturb_Max_Pct": max_pct,
        }
    )
    ohlc = base[required_columns].apply(pd.to_numeric, errors="coerce").astype(float)
    if ohlc.empty or ohlc.isna().any().any():
        return pd.DataFrame()

    magnitudes = rng.uniform(min_bound, max_bound, size=ohlc.shape)
    signs = rng.choice(np.array([-1.0, 1.0]), size=ohlc.shape)
    perturbed = ohlc.to_numpy(dtype=float) * (1.0 + (magnitudes * signs))

    perturbed = np.maximum(perturbed, 0.00000001)
    synthetic = base.copy()
    synthetic["Open"] = perturbed[:, 0]
    synthetic["Close"] = perturbed[:, 3]
    synthetic["High"] = np.maximum.reduce(
        [perturbed[:, 1], synthetic["Open"].to_numpy(), synthetic["Close"].to_numpy()]
    )
    synthetic["Low"] = np.minimum.reduce(
        [perturbed[:, 2], synthetic["Open"].to_numpy(), synthetic["Close"].to_numpy()]
    )
    return synthetic


def run_candles_based(symbol, timeframe, strategy_id, scenarios=200, seed=42):
    settings = database.get_backtesting_settings()
    if settings.get("Commission_Value") is None:
        raise RuntimeError("Configure an explicit exchange fee before Monte Carlo analysis")
    initial_cash = float(settings["Cash_Value"])
    commission = float(settings["Commission_Value"])
    perturb_min_pct = float(
        settings.get(
            "Monte_Carlo_Candle_Perturb_Min_Pct",
            DEFAULT_CANDLE_PERTURB_MIN_PCT,
        )
    )
    perturb_max_pct = float(
        settings.get(
            "Monte_Carlo_Candle_Perturb_Max_Pct",
            DEFAULT_CANDLE_PERTURB_MAX_PCT,
        )
    )
    strategy = _resolve_strategy(strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy '{strategy_id}' is not available.")

    bt_row = database.get_backtesting_results_by_symbol_timeframe_strategy(symbol, timeframe, strategy_id)
    if bt_row.empty:
        raise ValueError(f"No backtest result found for {strategy_id} - {symbol} - {timeframe}.")
    _apply_saved_strategy_parameters(strategy, bt_row.iloc[0])
    _configure_strategy(strategy, strategy_id, timeframe)
    strategy.execution_symbol = str(symbol)
    strategy.execution_timeframe = str(timeframe)
    df = my_backtesting.get_data(symbol, timeframe)
    if df.empty:
        raise ValueError(f"No OHLCV data found for {symbol} - {timeframe}.")
    definition = getattr(strategy, "definition", None)
    if isinstance(definition, dict) and definition.get("engine") == "bec_strategy_ast_v2":
        my_backtesting.set_declarative_strategy_data_cache(
            strategy,
            my_backtesting.build_declarative_strategy_data_cache(
                definition,
                symbol,
                timeframe,
            ),
        )

    try:
        prepared_original = _prepare_backtest_df(df.copy(), symbol, timeframe, strategy_id, strategy)
        original_stats = _run_strategy_on_df(prepared_original, strategy, initial_cash, commission)
        original_curve = _stats_equity_curve(original_stats, initial_cash)
        original_metrics = _stats_to_metrics(original_stats)

        rng = np.random.default_rng(int(seed))
        scenario_curves = []
        scenario_metrics = []
        for index in range(int(scenarios)):
            try:
                scenario_df = _perturb_candles(
                    df,
                    rng,
                    min_pct=perturb_min_pct,
                    max_pct=perturb_max_pct,
                )
                if scenario_df.empty:
                    continue
                scenario_df = _prepare_backtest_df(scenario_df, symbol, timeframe, strategy_id, strategy)
                stats = _run_strategy_on_df(scenario_df, strategy, initial_cash, commission)
                scenario_curves.append(_stats_equity_curve(stats, initial_cash))
                scenario_metrics.append(_stats_to_metrics(stats))
            except Exception as exc:
                print(f"Invalid candles scenario {index + 1}: {repr(exc)}", flush=True)
            if (index + 1) % 5 == 0 or index + 1 == int(scenarios):
                print(f"Running Candles Simulation... ({index + 1}/{int(scenarios)})", flush=True)
    finally:
        my_backtesting.set_declarative_strategy_data_cache(strategy, {})

    return _build_result(
        symbol,
        timeframe,
        strategy_id,
        METHOD_CANDLES,
        int(scenarios),
        int(seed),
        initial_cash,
        original_curve,
        original_metrics,
        scenario_curves,
        scenario_metrics,
    )


def run_monte_carlo(symbol, timeframe, strategy_id, method=METHOD_TRADE_SHUFFLE, scenarios=None, seed=42):
    method = str(method)
    if method == METHOD_TRADE_SHUFFLE:
        return run_trade_order_shuffle(symbol, timeframe, strategy_id, scenarios or 1000, seed)
    if method == METHOD_CANDLES:
        return run_candles_based(symbol, timeframe, strategy_id, scenarios or 200, seed)
    raise ValueError(f"Unsupported Monte Carlo method: {method}")


def main():
    parser = argparse.ArgumentParser(description="Run one BEC Monte Carlo analysis.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timeframe", required=True, choices=["1d", "4h", "1h", "15m"])
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--method", required=True, choices=[METHOD_TRADE_SHUFFLE, METHOD_CANDLES])
    parser.add_argument("--scenarios", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--exchange-id", type=int)
    parser.add_argument("--exchange-code")
    args = parser.parse_args()
    exchange = database.require_backtesting_execution_available()
    if args.exchange_id is not None and int(args.exchange_id) != int(exchange["id"]):
        raise RuntimeError("Queued Monte Carlo exchange no longer matches the active exchange")
    if args.exchange_code and str(args.exchange_code) != str(exchange["code"]):
        raise RuntimeError("Queued Monte Carlo exchange code does not match the active exchange")
    result = run_monte_carlo(
        symbol=args.symbol,
        timeframe=args.timeframe,
        strategy_id=args.strategy,
        method=args.method,
        scenarios=args.scenarios,
        seed=args.seed,
    )
    print(
        f"Monte Carlo completed: {result['summary']['valid_scenarios']} valid scenarios "
        f"out of {result['summary']['total_scenarios']} total."
    )


if __name__ == "__main__":
    main()
