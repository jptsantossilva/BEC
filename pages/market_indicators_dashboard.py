import pandas as pd
import streamlit as st

from bec.market_indicators.summary import (
    aggregate_summary_metrics,
    get_market_indicator_summaries,
    summaries_to_dataframe,
)
from bec.page_config import configure_page


POSITIVE_BG = "#8FBC8F"
NEGATIVE_BG = "#E9967A"


configure_page(page_title="Market Indicators")

st.header("Market Indicators")
st.caption(
    "Consolidated view of bull and bear market signals across market analysis "
    "indicators."
)

summaries = get_market_indicator_summaries()
table = summaries_to_dataframe(summaries)

if table.empty:
    st.info("No market indicators are configured yet.")
    st.stop()


def _signal_style(value: str) -> str:
    if value == "Hit":
        return f"background-color: {POSITIVE_BG}"
    if value == "Not hit":
        return f"background-color: {NEGATIVE_BG}"
    return ""


def _render_signal_grid(title: str, signal_group: str) -> None:
    group_metrics = aggregate_summary_metrics(summaries, signal_group=signal_group)
    group_df = table[table["signal_group"] == signal_group].copy()

    st.subheader(title)
    if group_df.empty:
        st.info(f"No {signal_group.lower()} signals are configured yet.")
        return

    st.caption(f"Hit: {group_metrics['active_signals']}")
    try:
        progress_value = float(group_metrics["average_progress"].replace("%", "")) / 100
    except ValueError:
        progress_value = 0.0
    st.progress(
        value=min(max(progress_value, 0.0), 1.0),
        text=f"Average Progress: {group_metrics['average_progress']}",
        width=200,)

    display = group_df[
        [
            "#",
            "name",
            "category",
            "current",
            "reference",
            "signal",
            "distance_to_hit",
            "progress",
            "status",
            "historical_data",
        ]
    ].rename(
        columns={
            "name": "Indicator",
            "category": "Category",
            "current": "Current",
            "reference": "Reference",
            "signal": "Signal",
            "distance_to_hit": "Distance",
            "progress": "Progress",
            "status": "Status",
            "historical_data": "Historical Data",
        }
    )
    display["Progress"] = pd.to_numeric(display["Progress"], errors="coerce").fillna(0.0)

    styled = display.style.map(_signal_style, subset=["Signal"])
    st.dataframe(
        styled,
        width="stretch",
        hide_index=True,
        column_config={
            "Progress": st.column_config.ProgressColumn(
                "Progress",
                format="%.2f%%",
                min_value=0.0,
                max_value=100.0,
            ),
            "Historical Data": st.column_config.LinkColumn(
                "Historical Data",
                display_text="Historical Data",
            ),
        },
    )


_render_signal_grid("Top Signals", "Top")
_render_signal_grid("Bottom Signals", "Bottom")
