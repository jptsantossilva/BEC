import math

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from bec.market_indicators import supply_profit_loss as spl
from bec.page_config import configure_page
import bec.utils.database as database


configure_page(page_title="Bitcoin Supply Profit/Loss")


ALERT_SETTING_KEYS = {
    "spl_top_threshold": ("onchain_supply_profit_loss_top_threshold", 95.0),
    "spl_extreme_top_threshold": (
        "onchain_supply_profit_loss_extreme_top_threshold",
        98.0,
    ),
    "spl_bottom_threshold": ("onchain_supply_profit_loss_bottom_threshold", 5.0),
    "spl_cross_tolerance": ("onchain_supply_profit_loss_cross_tolerance", 1.0),
}


def _read_float_setting(name: str, default: float) -> float:
    try:
        return float(database.get_setting(name))
    except (TypeError, ValueError):
        return float(default)


def _save_alert_setting(widget_key: str) -> None:
    setting_name, _ = ALERT_SETTING_KEYS[widget_key]
    database.set_setting(setting_name, st.session_state[widget_key])


for widget_key, (setting_name, default) in ALERT_SETTING_KEYS.items():
    if widget_key not in st.session_state:
        st.session_state[widget_key] = _read_float_setting(setting_name, default)

st.header("Bitcoin Supply in Profit / Loss")
st.caption(
    "Macro on-chain indicator comparing the percentage of BTC supply in profit "
    "and in loss. This is not an automatic buy/sell trigger."
)

schedule_enabled = database.get_job_schedule_enabled("btc_supply_profit_loss_1d")
if not schedule_enabled:
    st.warning(
        "The BTC Supply Profit/Loss scheduled job is disabled. Existing cached "
        "history is preserved, but market data will not be updated automatically."
    )
    if st.button("Update market data now", type="primary"):
        with st.spinner("Updating BTC Supply Profit/Loss data..."):
            try:
                spl.run_btc_supply_profit_loss_update_job()
            except Exception as exc:
                st.error(f"Manual data update failed: {exc}")
            else:
                st.success("Market data update completed.")


with st.container():
    chart_controls = st.container(horizontal=True, horizontal_alignment="center")
    with chart_controls:
        range_label = st.segmented_control(
            "Range",
            ["30d", "90d", "180d", "365d", "all"],
            default="all",
        )
        price_scale_label = st.segmented_control(
            "BTC price scale",
            ["Linear", "Logarithmic"],
            default="Logarithmic",
        )
        show_price_label = st.segmented_control(
            "BTC price",
            ["Hide", "Show"],
            default="Show",
        )
        # absolute_supply_label = st.segmented_control(
        #     "Absolute BTC supply",
        #     ["Hide", "Show"],
        #     default="Hide",
        # )
        absolute_supply_label = "Hide" 

show_price = show_price_label == "Show"
price_scale = "log" if price_scale_label == "Logarithmic" else "linear"
show_absolute_supply = absolute_supply_label == "Show"

df = spl.load_cached_supply_profit_loss()

if df.empty:
    if schedule_enabled:
        with st.spinner("Loading BTC Supply Profit/Loss data..."):
            try:
                spl.run_btc_supply_profit_loss_update_job()
            except Exception as exc:
                st.error(f"Data load failed: {exc}")
                st.stop()
        df = spl.load_cached_supply_profit_loss()
    if df.empty:
        st.info(
            "No BTC Supply in Profit/Loss rows are cached yet. Enable the scheduled "
            "job to let BEC populate the local table automatically."
        )
        st.stop()

plot_df = df.copy()
plot_df["date"] = pd.to_datetime(plot_df["date"], utc=True, errors="coerce")
plot_df = plot_df.dropna(subset=["date"]).sort_values("date")

if range_label != "all":
    days = int(range_label.replace("d", ""))
    cutoff = plot_df["date"].max() - pd.Timedelta(days=days)
    plot_df = plot_df[plot_df["date"] >= cutoff]

if plot_df.empty:
    st.info("No rows available for the selected range.")
    st.stop()

top_threshold = float(st.session_state.get("spl_top_threshold", 95.0))
extreme_top_threshold = float(st.session_state.get("spl_extreme_top_threshold", 98.0))
bottom_threshold = float(st.session_state.get("spl_bottom_threshold", 5.0))
cross_tolerance = float(st.session_state.get("spl_cross_tolerance", 1.0))

events = spl.detect_supply_profit_loss_events(
    plot_df,
    top_threshold=top_threshold,
    extreme_top_threshold=extreme_top_threshold,
    bottom_threshold=bottom_threshold,
    cross_tolerance=cross_tolerance,
)

latest = plot_df.iloc[-1]
retrieved_at = latest.get("retrieved_at", "")
latest_summary = (
    f"Latest: {latest['date'].strftime('%Y-%m-%d')} | "
    f"BTC price: ${float(latest['btc_price']):,.0f} | "
    f"Supply in Profit: {float(latest['percent_supply_in_profit']):.2f}% | "
    f"Supply in Loss: {float(latest['percent_supply_in_loss']):.2f}%"
)
if retrieved_at:
    latest_summary = f"{latest_summary} | Retrieved at: {retrieved_at}"
st.caption(latest_summary)

fig = make_subplots(specs=[[{"secondary_y": True}]])

if show_price:
    fig.add_trace(
        go.Scatter(
            x=plot_df["date"],
            y=plot_df["btc_price"],
            name="BTC Price",
            line=dict(color="#1f77b4", width=1.5),
            hovertemplate="BTC Price: %{y:,.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

fig.add_trace(
    go.Scatter(
        x=plot_df["date"],
        y=plot_df["percent_supply_in_loss"],
        name="% Supply In Loss",
        line=dict(color="#d62728", width=2),
        hovertemplate="% Supply In Loss: %{y:.2f}<extra></extra>",
    ),
    secondary_y=True,
)
fig.add_trace(
    go.Scatter(
        x=plot_df["date"],
        y=plot_df["percent_supply_in_profit"],
        name="% Supply In Profit",
        line=dict(color="#f59f00", width=2),
        hovertemplate="% Supply In Profit: %{y:.2f}<extra></extra>",
    ),
    secondary_y=True,
)

if show_absolute_supply:
    for column, name, color in (
        ("supply_in_profit_btc", "Supply in Profit BTC", "#b57600"),
        ("supply_in_loss_btc", "Supply in Loss BTC", "#8c1d18"),
    ):
        if column in plot_df.columns and plot_df[column].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=plot_df["date"],
                    y=plot_df[column],
                    name=name,
                    line=dict(color=color, width=1, dash="dot"),
                    hovertemplate=f"{name}: " + "%{y:,.0f}<extra></extra>",
                ),
                secondary_y=False,
            )

event_colors = {
    spl.EVENT_TOP_ZONE: "#005f73",
    spl.EVENT_EXTREME_TOP_ZONE: "#7f2704",
    spl.EVENT_BOTTOM_ZONE: "#2ca02c",
    spl.EVENT_CROSS_50: "#9467bd",
}
if not events.empty:
    for event_type, event_df in events.groupby("event_type"):
        fig.add_trace(
            go.Scatter(
                x=event_df["date"],
                y=event_df["percent_supply_in_profit"],
                mode="markers",
                name=event_type,
                marker=dict(size=9, color=event_colors.get(event_type, "#111111")),
                customdata=event_df[["message"]],
                hoverinfo="skip",
                hovertemplate=None,
            ),
            secondary_y=True,
        )

for level, color in [(5, "#999999"), (50, "#666666"), (95, "#999999"), (98, "#7f2704")]:
    fig.add_shape(
        type="line",
        x0=0,
        x1=1,
        xref="paper",
        y0=level,
        y1=level,
        yref="y2",
        line=dict(color=color, width=1, dash="dot"),
    )
    fig.add_annotation(
        x=1.005,
        xref="paper",
        y=level,
        yref="y2",
        text=f"{level}%",
        showarrow=False,
        xanchor="left",
        font=dict(color=color, size=11),
    )

primary_values = []
if show_price:
    primary_values.extend(plot_df["btc_price"].dropna().astype(float).tolist())
if show_absolute_supply:
    for column in ("supply_in_profit_btc", "supply_in_loss_btc"):
        if column in plot_df.columns:
            primary_values.extend(plot_df[column].dropna().astype(float).tolist())

primary_axis_range = None
positive_primary_values = [value for value in primary_values if value > 0]
if price_scale == "log" and positive_primary_values:
    min_log = math.log10(min(positive_primary_values))
    max_log = math.log10(max(positive_primary_values))
    padding = max((max_log - min_log) * 0.06, 0.15)
    primary_axis_range = [min_log - padding, max_log + padding]

fig.update_layout(
    height=720,
    dragmode="zoom",
    hovermode="x unified",
    hoverlabel=dict(
        bgcolor="rgba(248, 250, 252, 0.99)",
        bordercolor="rgba(148, 163, 184, 0.95)",
        font=dict(color="#334155", size=13),
        align="left",
    ),
    legend=dict(orientation="h", yanchor="top", y=-0.16, xanchor="center", x=0.5),
    margin=dict(l=20, r=20, t=40, b=95),
)
fig.update_xaxes(
    title_text="Date",
    showspikes=True,
    spikemode="across",
    spikesnap="cursor",
    spikedash="dot",
    spikecolor="#666666",
    spikethickness=1,
)
try:
    fig.update_xaxes(unifiedhovertitle_text="%{x|%d %B '%y}")
except ValueError:
    pass
fig.update_yaxes(
    title_text="BTC Price",
    type=price_scale,
    range=primary_axis_range,
    fixedrange=True,
    secondary_y=False,
)
fig.update_yaxes(
    title_text="% of total supply",
    range=[0, 100],
    fixedrange=True,
    secondary_y=True,
)

st.plotly_chart(fig, width="stretch")

st.subheader("Description")
st.caption(
    "This chart shows the percentage of Bitcoin supply whose last on-chain "
    "movement happened at a lower or higher price than the current BTC price. "
    "Supply in Profit represents coins whose last moved price is below the "
    "current price. Supply in Loss represents coins whose last moved price is "
    "above the current price."
)

st.subheader("Usage")
st.caption(
    "When Bitcoin sets new all-time highs, Supply in Profit can move close to "
    "100% and remain elevated during strong bull markets. Readings above 95% "
    "are historical euphoria/distribution zones, and readings above 98% mark "
    "more extreme euphoria with higher correction risk. Readings below 5% are "
    "historical capitulation or macro-bottom zones. A bottom stress transition "
    "is highlighted when Supply in Loss crosses above Supply in Profit. These "
    "are context signals only, not automatic buy or sell orders."
)

with st.container():
    st.subheader("Signal Thresholds")
    signal_cols = st.columns([1, 1, 1, 1])
    with signal_cols[0]:
        st.number_input(
            "Top threshold (%)",
            min_value=0.0,
            max_value=100.0,
            step=0.5,
            key="spl_top_threshold",
            on_change=_save_alert_setting,
            args=("spl_top_threshold",),
            help=(
                "Triggers TOP_ZONE when Supply in Profit enters this percentage "
                "or higher. This is an informational macro/on-chain alert only."
            ),
        )
    with signal_cols[1]:
        st.number_input(
            "Extreme top threshold (%)",
            min_value=0.0,
            max_value=100.0,
            step=0.5,
            key="spl_extreme_top_threshold",
            on_change=_save_alert_setting,
            args=("spl_extreme_top_threshold",),
            help=(
                "Triggers EXTREME_TOP when Supply in Profit enters this "
                "percentage or higher, even if TOP_ZONE is already active."
            ),
        )
    with signal_cols[2]:
        st.number_input(
            "Bottom threshold (%)",
            min_value=0.0,
            max_value=100.0,
            step=0.5,
            key="spl_bottom_threshold",
            on_change=_save_alert_setting,
            args=("spl_bottom_threshold",),
            help=(
                "Triggers BOTTOM_ZONE when Supply in Profit is at or below this "
                "percentage, or when Supply in Loss is at the equivalent high level."
            ),
        )
    with signal_cols[3]:
        st.number_input(
            "50% cross tolerance (p.p.)",
            min_value=0.0,
            max_value=20.0,
            step=0.5,
            key="spl_cross_tolerance",
            on_change=_save_alert_setting,
            args=("spl_cross_tolerance",),
            help=(
                "p.p. means percentage points. This tolerance defines the "
                "near-cross watch zone around Supply in Loss and Supply in "
                "Profit; the main bottom stress signal triggers when Supply in "
                "Loss crosses above Supply in Profit."
            ),
        )

with st.expander("Detected Events", expanded=False):
    if events.empty:
        st.info("No events detected with the selected thresholds.")
    else:
        show_events = events.copy()
        show_events["date"] = pd.to_datetime(show_events["date"]).dt.strftime("%Y-%m-%d")
        st.dataframe(
            show_events[
                [
                    "date",
                    "event_type",
                    "severity",
                    "btc_price",
                    "percent_supply_in_profit",
                    "percent_supply_in_loss",
                    "message",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

with st.expander("Latest Rows", expanded=False):
    latest_rows = plot_df.tail(20).copy()
    latest_rows["date"] = latest_rows["date"].dt.strftime("%Y-%m-%d")
    st.dataframe(latest_rows, width="stretch", hide_index=True)

st.caption("Macro/on-chain signal only. This is not an automatic buy/sell order.")
