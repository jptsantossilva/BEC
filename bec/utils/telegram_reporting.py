from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

import bec.utils.database as database


def _to_float(value, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_value(value, decimals: int = 2) -> str:
    return f"{_to_float(value):.{decimals}f}"


def _fmt_signed(value, decimals: int = 2, suffix: str = "") -> str:
    numeric = _to_float(value)
    return f"{numeric:+.{decimals}f}{suffix}"


def _compact_duration(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.replace("  ", " ").split()
    if len(parts) >= 2 and parts[0].endswith("d"):
        return " ".join(parts[:2])
    return " ".join(parts[:2]) if len(parts) > 2 else text


def _weighted_pnl_perc(df: pd.DataFrame, value_column: str = "Position_Value") -> float:
    if df.empty or value_column not in df.columns or "PnL_Perc" not in df.columns:
        return 0.0
    values = pd.to_numeric(df[value_column], errors="coerce").fillna(0.0)
    pnl = pd.to_numeric(df["PnL_Perc"], errors="coerce").fillna(0.0)
    denominator = float(values.sum())
    if denominator == 0:
        return 0.0
    return float((pnl * values).sum() / denominator)


def _strategy_label(row) -> str:
    strategy_name = str(row.get("Strategy_Name", "") or "").strip()
    strategy_id = str(row.get("Strategy_Id", "") or "").strip()
    if strategy_name:
        return strategy_name
    if strategy_id:
        return database.get_strategy_name(strategy_id) or strategy_id
    return "Unknown"


def format_positions_summary(timeframe: str, settings=None) -> str:
    df = database.get_unrealized_pnl_by_bot(timeframe)
    if df.empty:
        return "Positions: no open positions"

    n_decimals = int(getattr(settings, "n_decimals", 2) if settings is not None else 2)
    max_positions = getattr(settings, "max_number_of_open_positions", None)
    df = df.copy()
    df["PnL_Perc"] = pd.to_numeric(df.get("PnL_Perc", 0.0), errors="coerce").fillna(0.0)
    df["PnL_Value"] = pd.to_numeric(df.get("PnL_Value", 0.0), errors="coerce").fillna(0.0)
    df["Position_Value"] = pd.to_numeric(df.get("Position_Value", 0.0), errors="coerce").fillna(0.0)
    df = df.sort_values("PnL_Perc", ascending=False)

    total_value = float(df["Position_Value"].sum())
    total_pnl = float(df["PnL_Value"].sum())
    weighted_pnl = _weighted_pnl_perc(df)
    winners = int((df["PnL_Perc"] > 0).sum())
    losers = int((df["PnL_Perc"] < 0).sum())
    position_count = len(df)
    capacity = f"{position_count}/{max_positions}" if max_positions else str(position_count)
    trade_against = getattr(settings, "trade_against", "") if settings is not None else ""
    suffix = f" {trade_against}" if trade_against else ""

    lines = [
        "Positions",
        f"Open: {capacity}",
        f"Value: {_fmt_value(total_value, n_decimals)}{suffix}",
        f"uPnL: {_fmt_signed(total_pnl, n_decimals)}{suffix} ({_fmt_signed(weighted_pnl, 2, '%')})",
        f"W/L: {winners}/{losers}",
    ]
    for index, (_, row) in enumerate(df.iterrows(), start=1):
        symbol = str(row.get("Symbol", "") or "")
        strategy = _strategy_label(row)
        pnl_perc = _fmt_signed(row.get("PnL_Perc", 0.0), 2, "%")
        pnl_value = _fmt_signed(row.get("PnL_Value", 0.0), n_decimals)
        duration = _compact_duration(row.get("Duration", ""))
        duration_text = f" | {duration}" if duration else ""
        lines.append(f"{index}. {symbol} | PnL {pnl_perc} | {pnl_value}{suffix} | {strategy}{duration_text}")
    return "\n".join(lines)


def get_orders_by_side_date_range(side: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    return database.get_orders_by_side_date_range(
        side=side,
        start_utc=start_utc.replace(tzinfo=None).isoformat(sep=" "),
        end_utc=end_utc.replace(tzinfo=None).isoformat(sep=" "),
    )


def _all_open_positions() -> pd.DataFrame:
    frames = []
    for timeframe in ["1d", "4h", "1h"]:
        df = database.get_unrealized_pnl_by_bot(timeframe)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def format_daily_summary(now_utc: datetime | None = None, settings=None) -> str:
    now_utc = now_utc or datetime.now(timezone.utc)
    end_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = end_utc - timedelta(days=1)
    buys = get_orders_by_side_date_range("BUY", start_utc, end_utc)
    sells = get_orders_by_side_date_range("SELL", start_utc, end_utc)
    open_positions = _all_open_positions()

    n_decimals = int(getattr(settings, "n_decimals", 2) if settings is not None else 2)
    realized_pnl = 0.0
    realized_weighted = 0.0
    take_profits = stops = strategy_exits = 0
    best_line = "n/a"
    worst_line = "n/a"
    if not sells.empty:
        sells = sells.copy()
        sells["PnL_Value"] = pd.to_numeric(sells.get("PnL_Value", 0.0), errors="coerce").fillna(0.0)
        sells["PnL_Perc"] = pd.to_numeric(sells.get("PnL_Perc", 0.0), errors="coerce").fillna(0.0)
        sells["Sell_Position_Value"] = pd.to_numeric(sells.get("Sell_Position_Value", 0.0), errors="coerce").fillna(0.0)
        realized_pnl = float(sells["PnL_Value"].sum())
        realized_weighted = _weighted_pnl_perc(sells, "Sell_Position_Value")
        stop_type = sells.get("Stop_Type", pd.Series(dtype=str)).fillna("").astype(str)
        exit_reason = sells.get("Exit_Reason", pd.Series(dtype=str)).fillna("").astype(str)
        take_profits = int((stop_type == "tp").sum() or exit_reason.str.startswith("Take-Profit").sum())
        stops = int(stop_type.isin(["hard_sl", "atr_trailing"]).sum())
        strategy_exits = int((stop_type == "strategy").sum())
        sorted_sells = sells.sort_values("PnL_Perc", ascending=False)
        best = sorted_sells.iloc[0]
        worst = sorted_sells.iloc[-1]
        best_line = f"{best.Symbol} {_fmt_signed(best.PnL_Perc, 2, '%')} ({_fmt_signed(best.PnL_Value, n_decimals)})"
        worst_line = f"{worst.Symbol} {_fmt_signed(worst.PnL_Perc, 2, '%')} ({_fmt_signed(worst.PnL_Value, n_decimals)})"

    open_count = len(open_positions)
    open_pnl = 0.0
    open_weighted = 0.0
    if not open_positions.empty:
        open_positions = open_positions.copy()
        open_positions["PnL_Value"] = pd.to_numeric(open_positions.get("PnL_Value", 0.0), errors="coerce").fillna(0.0)
        open_pnl = float(open_positions["PnL_Value"].sum())
        open_weighted = _weighted_pnl_perc(open_positions)

    period = f"{start_utc.date().isoformat()} UTC"
    return "\n".join(
        [
            "Daily Summary",
            f"Period: {period}",
            f"Buys opened: {len(buys)}",
            f"Sells closed: {len(sells)}",
            f"Realized PnL: {_fmt_signed(realized_pnl, n_decimals)} ({_fmt_signed(realized_weighted, 2, '%')})",
            f"Take profits / Stops / Strategy exits: {take_profits}/{stops}/{strategy_exits}",
            f"Open positions: {open_count}",
            f"Open PnL: {_fmt_signed(open_pnl, n_decimals)} ({_fmt_signed(open_weighted, 2, '%')})",
            f"Best closed: {best_line}",
            f"Worst closed: {worst_line}",
        ]
    )


def format_market_phase_report(
    *,
    timeframe: str,
    trade_against: str,
    duration: str,
    symbols_scanned: int,
    df_result: pd.DataFrame,
    df_top: pd.DataFrame,
    backtesting_stats: dict | None = None,
    warnings: int = 0,
    tradingview_attached: bool = False,
) -> str:
    backtesting_stats = backtesting_stats or {}
    phases = (
        df_result.get("Market_Phase", pd.Series(dtype=str))
        .fillna("unknown")
        .astype(str)
        .value_counts()
        .to_dict()
        if not df_result.empty
        else {}
    )
    positive_count = sum(int(phases.get(phase, 0)) for phase in ["bullish", "accumulation"])
    top_count = len(df_top) if df_top is not None else 0

    lines = [
        "MKT Report",
        f"Status: completed in {duration}",
        f"Timeframe: {timeframe}",
        f"Trade against: {trade_against}",
        f"Symbols scanned: {symbols_scanned}",
        f"Positive phases: {positive_count}",
        f"Top selected: {top_count}",
        f"Warnings: {warnings}" + (" | See Errors channel" if warnings else ""),
        "",
        "Market phases",
    ]
    for phase in ["bullish", "accumulation", "recovery", "warning", "distribution", "bearish", "unknown"]:
        lines.append(f"{phase}: {int(phases.get(phase, 0))}")

    lines.extend(
        [
            "",
            "Backtesting",
            f"Pending symbols: {int(backtesting_stats.get('symbols_pending', 0))}",
            f"Strategies tested: {int(backtesting_stats.get('strategies_tested', 0))}",
            f"Runs: {int(backtesting_stats.get('backtest_runs', 0))}",
            f"Approved: {int(backtesting_stats.get('approved_candidates', 0))}",
            f"Rejected: {int(backtesting_stats.get('rejected_candidates', 0))}",
            "",
            "Top performers",
        ]
    )

    if df_top is None or df_top.empty:
        lines.append("none")
    else:
        top_preview = df_top.head(5)
        for index, (_, row) in enumerate(top_preview.iterrows(), start=1):
            symbol = str(row.get("Symbol", "") or "")
            phase = str(row.get("Market_Phase", "") or "unknown")
            strength = _fmt_signed(row.get("Perc_Above_DSMA200", 0.0), 2, "%")
            lines.append(f"{index}. {symbol} | {phase} | {strength} vs DSMA200")

    if tradingview_attached:
        lines.extend(["", "TradingView list attached."])

    return "\n".join(lines)


def format_trade_against_switch_event(*, direction: str, reason: str, actions: list[str]) -> str:
    lines = [
        "Trade Against Switch",
        f"Direction: {direction}",
        f"Reason: {reason}",
        "Actions:",
    ]
    lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines)
