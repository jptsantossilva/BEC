from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

import bec.utils.database as database
from bec.market_indicators import supply_profit_loss as spl


@dataclass
class MarketIndicatorSummary:
    name: str
    signal_group: str
    category: str
    bias: str
    current: str
    reference: str
    hit: bool
    distance_to_hit: str
    progress_pct: float
    status: str
    latest_date: str
    updated_at: str
    detail_page: str
    detail_url: str
    available: bool = True


def _read_float_setting(name: str, default: float) -> float:
    try:
        value = float(database.get_setting(name))
    except (TypeError, ValueError):
        return float(default)
    if value <= 0:
        return float(default)
    return value


def _clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, float(value)))


def _utc_now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _unavailable_supply_profit_loss_summary(
    name: str,
    reference: str,
    detail_page: str,
) -> MarketIndicatorSummary:
    return MarketIndicatorSummary(
        name=name,
        signal_group="Top" if "Top" in name else "Bottom",
        category="Macro / On-chain",
        bias="Neutral",
        current="No data",
        reference=reference,
        hit=False,
        distance_to_hit="n/a",
        progress_pct=0.0,
        status="Unavailable",
        latest_date="n/a",
        updated_at="n/a",
        detail_page=detail_page,
        detail_url="/bitcoin_supply_profit_loss",
        available=False,
    )


def summarize_btc_supply_profit_loss() -> list[MarketIndicatorSummary]:
    df = spl.load_cached_supply_profit_loss()
    detail_page = "pages/bitcoin_supply_profit_loss.py"

    summary_top_threshold = _read_float_setting(
        "onchain_supply_profit_loss_extreme_top_threshold", 98.0
    )
    cross_tolerance = _read_float_setting(
        "onchain_supply_profit_loss_cross_tolerance", spl.DEFAULT_CROSS_TOLERANCE
    )

    if df.empty:
        return [
            _unavailable_supply_profit_loss_summary(
                "BTC Supply Profit/Loss - Top",
                f">= {summary_top_threshold:.2f}%",
                detail_page,
            ),
            _unavailable_supply_profit_loss_summary(
                "BTC Supply Profit/Loss - Bottom",
                "Loss >= Profit",
                detail_page,
            ),
        ]

    data = df.copy()
    data["date"] = pd.to_datetime(data["date"], utc=True, errors="coerce")
    data = data.dropna(subset=["date"]).sort_values("date")
    if data.empty:
        return [
            _unavailable_supply_profit_loss_summary(
                "BTC Supply Profit/Loss - Top",
                f">= {summary_top_threshold:.2f}%",
                detail_page,
            ),
            _unavailable_supply_profit_loss_summary(
                "BTC Supply Profit/Loss - Bottom",
                "Loss >= Profit",
                detail_page,
            ),
        ]

    latest = data.iloc[-1]
    profit = float(latest["percent_supply_in_profit"])
    loss = float(latest["percent_supply_in_loss"])
    top_hit = profit >= summary_top_threshold
    top_distance = max(summary_top_threshold - profit, 0.0)
    top_progress = _clamp(
        ((profit - 50.0) / (summary_top_threshold - 50.0)) * 100
        if summary_top_threshold > 50.0
        else 100.0
    )

    if top_hit:
        top_status = "Risk"
        top_bias = "Risk"
    else:
        top_status = "Neutral"
        top_bias = "Neutral"

    loss_profit_gap = loss - profit
    distance_to_bottom = max(profit - loss, 0.0)
    bottom_hit = loss >= profit
    total_share = profit + loss
    crossover_level = total_share / 2
    bottom_progress = (
        100.0
        if bottom_hit
        else _clamp((loss / crossover_level) * 100 if crossover_level > 0 else 0.0)
    )
    if bottom_hit:
        bottom_status = "Stress"
        bottom_bias = "Bearish"
    elif abs(loss_profit_gap) <= cross_tolerance:
        bottom_status = "Watch"
        bottom_bias = "Neutral"
    else:
        bottom_status = "Neutral"
        bottom_bias = "Neutral"

    updated_at = str(latest.get("retrieved_at") or "").strip() or _utc_now_label()
    latest_date = latest["date"].strftime("%Y-%m-%d")
    return [
        MarketIndicatorSummary(
            name="BTC Supply Profit/Loss - Top",
            signal_group="Top",
            category="Macro / On-chain",
            bias=top_bias,
            current=f"{profit:.2f}%",
            reference=f">= {summary_top_threshold:.2f}%",
            hit=top_hit,
            distance_to_hit=f"{top_distance:.2f} p.p.",
            progress_pct=round(top_progress, 2),
            status=top_status,
            latest_date=latest_date,
            updated_at=updated_at,
            detail_page=detail_page,
            detail_url="/bitcoin_supply_profit_loss",
            available=True,
        ),
        MarketIndicatorSummary(
            name="BTC Supply Profit/Loss - Bottom",
            signal_group="Bottom",
            category="Macro / On-chain",
            bias=bottom_bias,
            current=f"Profit {profit:.2f}% / Loss {loss:.2f}%",
            reference="Loss >= Profit",
            hit=bottom_hit,
            distance_to_hit=f"{distance_to_bottom:.2f} p.p.",
            progress_pct=round(bottom_progress, 2),
            status=bottom_status,
            latest_date=latest_date,
            updated_at=updated_at,
            detail_page=detail_page,
            detail_url="/bitcoin_supply_profit_loss",
            available=True,
        ),
    ]


def get_market_indicator_summaries() -> list[MarketIndicatorSummary]:
    return summarize_btc_supply_profit_loss()


def summaries_to_dataframe(
    summaries: list[MarketIndicatorSummary],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(summaries, start=1):
        row = asdict(item)
        row["#"] = index
        row["signal"] = "Hit" if item.hit else "Not hit"
        row["progress"] = float(item.progress_pct)
        row["historical_data"] = item.detail_url
        rows.append(row)
    return pd.DataFrame(rows)


def aggregate_summary_metrics(
    summaries: list[MarketIndicatorSummary],
    signal_group: str | None = None,
) -> dict[str, str]:
    if signal_group is not None:
        summaries = [item for item in summaries if item.signal_group == signal_group]
    available = [item for item in summaries if item.available]
    active = [item for item in available if item.hit]
    if available:
        average_progress = sum(item.progress_pct for item in available) / len(available)
        latest_update = max(item.updated_at for item in available if item.updated_at)
    else:
        average_progress = 0.0
        latest_update = "n/a"

    return {
        "indicators": str(len(available)),
        "active_signals": f"{len(active)}/{len(available)}",
        "average_progress": f"{average_progress:.2f}%",
        "latest_update": latest_update,
    }
