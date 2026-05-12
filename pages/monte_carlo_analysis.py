import json
import os
import re
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import bec.my_backtesting as my_backtesting
import bec.utils.database as database
from bec.utils import monte_carlo
from bec.page_config import configure_page

configure_page()

METHOD_OPTIONS = {
    "Trade-order shuffling": monte_carlo.METHOD_TRADE_SHUFFLE,
    "Candles-based": monte_carlo.METHOD_CANDLES,
}
DEFAULT_MONTE_CARLO_RETENTION_DAYS = 30
MC_SCORE_HELP = (
    "MC Score combines median scenario return (55%), worst 5% scenario return (25%), "
    "and worst 5% drawdown versus the original drawdown (20%), then multiplies by "
    "valid scenarios / total scenarios. Higher is more robust."
)
MC_INTERPRETATION_HELP = (
    "Robust: score >= 75. Moderate robustness: score >= 55. "
    "Sequence-sensitive: score < 55 for trade shuffling. "
    "Market-path fragile: score < 45 for candles-based tests."
)


def _format_strategy_factory(strategy_map):
    def _format(strategy_id):
        return strategy_map.get(strategy_id, strategy_id)

    return _format


def _restore_multiselect(state_key, widget_key, options):
    saved = [value for value in st.session_state.get(state_key, []) if value in options]
    st.session_state[state_key] = saved
    if widget_key not in st.session_state:
        st.session_state[widget_key] = saved


def _persist(state_key, widget_key):
    st.session_state[state_key] = st.session_state.get(widget_key)


def load_top_performer_symbols():
    df_top_perf = database.get_all_symbols_by_market_phase()
    top_perf_symbol_list = (
        df_top_perf["Symbol"].dropna().astype(str).str.upper().to_list()
    )
    if not top_perf_symbol_list:
        st.session_state["mc_top_performers_message"] = (
            "info",
            "No top performers found.",
        )
        return

    st.session_state["mc_pending_symbol_selection"] = top_perf_symbol_list
    st.session_state["mc_top_performers_message"] = (
        "success",
        f"Loaded {len(top_perf_symbol_list)} top performer symbol(s).",
    )


def _static_url(path):
    if not path:
        return ""
    normalized = str(path).replace("\\", "/")
    abs_path = os.path.join(my_backtesting.PROJECT_ROOT, normalized)
    if os.path.exists(abs_path):
        return os.path.join("app", normalized)
    if os.path.exists(normalized):
        return os.path.join("app", normalized)
    return ""


def _tail_text_file(path, max_chars=12000):
    if not path:
        return ""
    abs_path = os.path.join(my_backtesting.PROJECT_ROOT, str(path))
    if not os.path.exists(abs_path):
        return ""
    with open(abs_path, "r", encoding="utf-8", errors="replace") as file:
        return file.read()[-max_chars:]


def _enqueue_rows(rows, method, scenarios, seed):
    jobs = []
    for row in rows:
        jobs.append(
            {
                "strategy_id": str(row["Strategy_Id"]).strip(),
                "symbol": str(row["Symbol"]).strip().upper(),
                "timeframe": str(row["Time_Frame"]).strip(),
                "method": method,
                "scenarios": int(scenarios),
                "seed": int(seed),
            }
        )
    return database.enqueue_monte_carlo_jobs(jobs)


def _cleanup_summary_text(summary):
    deleted_results = int(summary.get("deleted_results", 0) or 0)
    deleted_files = int(summary.get("deleted_files", 0) or 0)
    skipped_files = int(summary.get("skipped_files", 0) or 0)
    unsafe_paths = len(summary.get("unsafe_paths", []) or [])
    file_errors = len(summary.get("file_errors", []) or [])
    text = f"Deleted {deleted_results} Monte Carlo result(s) and {deleted_files} file(s)."
    if skipped_files:
        text += f" Skipped {skipped_files} missing/ignored file(s)."
    if unsafe_paths:
        text += f" Ignored {unsafe_paths} unsafe path(s)."
    if file_errors:
        text += f" {file_errors} file deletion error(s)."
    return text


def _row_result_id(row):
    try:
        value = row.get("Monte_Carlo_Result_Id")
    except AttributeError:
        return None
    try:
        if pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def render_mc_score_help():
    with st.expander("How MC Score is calculated", expanded=False):
        st.markdown(
            """
The **MC Score** is a 0-100 robustness score. It compares the original backtest with the Monte Carlo scenario distribution.

```text
MC Score =
(
  median return component * 55%
  + worst 5% return component * 25%
  + drawdown component * 20%
)
* valid scenarios / total scenarios
```

- **Median return component**: compares the median scenario return with the original return.
- **Worst 5% return component**: penalizes weak lower-tail scenario returns.
- **Drawdown component**: penalizes scenarios where the worst 5% drawdown is larger than the original drawdown.

Interpretation thresholds:

| Interpretation | Rule |
| --- | --- |
| Robust | Score >= 75 |
| Moderate robustness | Score >= 55 and < 75 |
| Sequence-sensitive | Score < 55 for trade-order shuffling |
| Market-path fragile | Score < 45 for candles-based tests |
| Insufficient scenarios | No valid scenarios, too few valid scenarios, or insufficient trade sample |
"""
        )


def _extract_progress(text):
    matches = re.findall(r"Running (?:Candles|Trade Shuffle) Simulation\.\.\. \((\d+)/(\d+)\)", text or "")
    if not matches:
        return None
    current, total = matches[-1]
    return int(current), int(total)


def style_job_status(value):
    status = str(value or "").strip().lower()
    colors = {
        "queued": "#7c3aed",
        "pending": "#7c3aed",
        "running": "#2563eb",
        "completed": "#16a34a",
        "failed": "#dc2626",
        "cancelled": "#64748b",
        "canceled": "#64748b",
        "skipped": "#ca8a04",
        "unknown": "#475569",
    }
    color = colors.get(status, "#475569")
    return f"color: {color};"


@st.fragment(run_every=3)
def render_monte_carlo_jobs_status():
    counts = database.get_monte_carlo_job_counts()
    count_map = dict(zip(counts["status"], counts["count"])) if not counts.empty else {}
    jobs = database.get_monte_carlo_jobs(limit=50)
    queued = int(count_map.get("queued", 0))
    running = int(count_map.get("running", 0))
    completed = int(count_map.get("completed", 0))
    failed = int(count_map.get("failed", 0))
    summary = (
        f"Monte Carlo Queue: {queued} queued | {running} running | "
        f"{completed} completed | {failed} failed"
    )

    st.subheader("Monte Carlo Queue")
    if jobs.empty:
        st.caption(summary.replace("Monte Carlo Queue: ", ""))
        return

    running_jobs = jobs[jobs["status"] == "running"]
    if not running_jobs.empty:
        current = running_jobs.iloc[0]
        log_text = _tail_text_file(current.get("log_path"))
        progress = _extract_progress(log_text)
        label = (
            f"Running Monte Carlo job #{current['id']}: "
            f"{current['method']} - {current['strategy_id']} - {current['symbol']} - {current['timeframe']}"
        )
        st.status(label, state="running", expanded=False)
        if progress:
            current_step, total_steps = progress
            value = current_step / total_steps if total_steps else 0.0
            st.progress(
                value,
                text=f"Running Simulation... ({current_step}/{total_steps})",
            )
    elif queued > 0:
        st.status(
            f"Waiting for jobs_runner to start {queued} queued Monte Carlo job(s)...",
            state="running",
            expanded=False,
        )

    with st.expander(summary.replace("Monte Carlo Queue: ", ""), expanded=(queued > 0 or running > 0 or failed > 0)):
        jobs_display = jobs.copy()
        jobs_display["Target"] = (
            jobs_display["strategy_id"].astype(str)
            + " - "
            + jobs_display["symbol"].astype(str)
            + " - "
            + jobs_display["timeframe"].astype(str)
        )
        jobs_display["Log"] = jobs_display["log_path"].apply(_static_url)
        jobs_display = jobs_display[
            [
                "id",
                "batch_id",
                "Target",
                "method",
                "scenarios",
                "seed",
                "status",
                "created_at",
                "started_at",
                "finished_at",
                "return_code",
                "Log",
                "error_message",
            ]
        ]
        styled_jobs_display = jobs_display.style.map(
            style_job_status,
            subset=["status"],
        )
        st.dataframe(
            styled_jobs_display,
            width="content",
            hide_index=True,
            height=260,
            column_config={
                "Log": st.column_config.LinkColumn("Log", display_text="Open"),
            },
        )
        if not running_jobs.empty:
            current = running_jobs.iloc[0]
            log_text = _tail_text_file(current.get("log_path"))
            if log_text:
                st.code(log_text, language="text")


def _build_equity_figure(result):
    original = result.get("original_equity_curve", [])
    scenarios = result.get("scenario_equity_curves", [])
    initial_cash = float(result.get("initial_cash", original[0] if original else 0.0))
    title = monte_carlo.METHOD_LABELS.get(result.get("method"), "Monte Carlo")
    base_theme = (st.get_option("theme.base") or "light").lower()
    is_dark = base_theme == "dark"
    template = "plotly_dark" if is_dark else "plotly_white"
    paper_bg = "#201d1d" if is_dark else "#ffffff"
    plot_bg = "#171313" if is_dark else "#ffffff"
    scenario_color = "rgba(105, 94, 255, 0.22)" if is_dark else "rgba(37, 99, 235, 0.20)"
    original_color = "#f5c84b" if is_dark else "#047857"
    initial_line_color = "rgba(245, 200, 75, 0.55)" if is_dark else "rgba(100, 116, 139, 0.55)"
    fig = go.Figure()
    max_points = max([len(original)] + [len(curve) for curve in scenarios[:150]] or [1])
    for curve in scenarios[:150]:
        fig.add_trace(
            go.Scatter(
                x=list(range(len(curve))),
                y=curve,
                mode="lines",
                line={"color": scenario_color, "width": 1},
                hoverinfo="skip",
                showlegend=False,
            )
        )
    fig.add_trace(
        go.Scatter(
            x=list(range(len(original))),
            y=original,
            mode="lines",
            name="Original Strategy",
            line={"color": original_color, "width": 3},
            hovertemplate="Step %{x}<br>Equity %{y:,.2f}<extra></extra>",
        )
    )
    fig.add_hline(y=initial_cash, line_dash="dot", line_color=initial_line_color)
    fig.update_layout(
        title=title,
        template=template,
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        height=430,
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
        legend={"orientation": "h", "y": 1.05},
        xaxis_title="Step",
        yaxis_title="Equity",
        xaxis={"range": [0, max_points - 1]},
    )
    return fig


def _metric_format(metric, value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if metric in {"Net Profit", "Max Drawdown", "Win Rate", "Annual Return", "Expectancy"}:
        return f"{value:.1f}%"
    if metric == "Total Trades":
        return f"{value:.0f}"
    return f"{value:.2f}"


def _parse_metric_display_value(value):
    try:
        text = str(value).strip().replace("%", "").replace(",", "")
        if text.lower() in {"", "n/a", "nan", "none"}:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def monte_carlo_metric_row_style(row):
    metric = str(row.get("Metric", ""))
    comparison_columns = ["Worst 5%", "Median", "Best 5%"]
    original = _parse_metric_display_value(row.get("Original"))
    values = {column: _parse_metric_display_value(row.get(column)) for column in comparison_columns}
    styles = {column: "" for column in row.index}
    if "Metric" in styles:
        styles["Metric"] = (
            "background-color: #f8fafc; color: #334155; "
            "font-weight: 600; border-right: 1px solid #dbe4ee;"
        )
    if original is None:
        return [styles[column] for column in row.index]

    lower_is_better = metric in {"Max Drawdown"}
    absolute_thresholds = {
        "Sharpe Ratio": (0.05, 0.20),
        "SQN": (0.05, 0.20),
        "Calmar Ratio": (0.10, 0.50),
        "Total Trades": (1.0, 3.0),
    }
    pct_point_thresholds = {
        "Net Profit": (1.0, 5.0),
        "Max Drawdown": (1.0, 5.0),
        "Win Rate": (1.0, 5.0),
        "Annual Return": (1.0, 5.0),
        "Expectancy": (0.25, 1.0),
    }

    def _impact_style(value):
        delta = value - original
        beneficial_delta = -delta if lower_is_better else delta
        small_threshold, large_threshold = absolute_thresholds.get(
            metric,
            pct_point_thresholds.get(metric, (0.01, 0.05)),
        )
        if abs(delta) < small_threshold:
            return ""
        if beneficial_delta > 0:
            if abs(delta) >= large_threshold:
                return "background-color: #dcfce7; color: #166534; font-weight: 700;"
            return "background-color: #ecfdf5; color: #047857; font-weight: 700;"
        if abs(delta) >= large_threshold:
            return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"
        return "background-color: #ffedd5; color: #9a3412; font-weight: 700;"

    for column, value in values.items():
        if value is None:
            continue
        styles[column] = _impact_style(value)

    return [styles[column] for column in row.index]


def quality_score_cell_style(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return ""

    if pd.isna(value):
        return ""
    if value >= 85:
        return "background-color: #dcfce7; color: #166534; font-weight: 700;"
    if value >= 70:
        return "background-color: #ecfdf5; color: #047857; font-weight: 700;"
    if value >= 55:
        return "background-color: #fef9c3; color: #854d0e; font-weight: 700;"
    if value >= 40:
        return "background-color: #ffedd5; color: #9a3412; font-weight: 700;"
    return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"


def quality_grade_cell_style(value):
    styles = {
        "A": "background-color: #dcfce7; color: #166534; font-weight: 800; text-align: center;",
        "B": "background-color: #ecfdf5; color: #047857; font-weight: 800; text-align: center;",
        "C": "background-color: #fef9c3; color: #854d0e; font-weight: 800; text-align: center;",
        "D": "background-color: #ffedd5; color: #9a3412; font-weight: 800; text-align: center;",
        "F": "background-color: #fee2e2; color: #991b1b; font-weight: 800; text-align: center;",
    }
    return styles.get(str(value).upper(), "")


def monte_carlo_interpretation_cell_style(value):
    styles = {
        "robust": "background-color: #ecfdf5; color: #047857; font-weight: 800; text-align: center;",
        "moderate robustness": "background-color: #fef9c3; color: #854d0e; font-weight: 800; text-align: center;",
        "sequence-sensitive": "background-color: #ffedd5; color: #9a3412; font-weight: 800; text-align: center;",
        "market-path fragile": "background-color: #fee2e2; color: #991b1b; font-weight: 800; text-align: center;",
        "insufficient scenarios": "background-color: #fee2e2; color: #991b1b; font-weight: 800; text-align: center;",
        "insufficient trades": "background-color: #fee2e2; color: #991b1b; font-weight: 800; text-align: center;",
    }
    return styles.get(str(value or "").strip().lower(), "")


def _metrics_df(result):
    rows = []
    for metric, values in (result.get("metrics") or {}).items():
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


def render_result_detail(row, method):
    with st.spinner("Loading Monte Carlo report..."):
        result_df = database.get_monte_carlo_result(
            symbol=str(row["Symbol"]),
            timeframe=str(row["Time_Frame"]),
            strategy_id=str(row["Strategy_Id"]),
            method=method,
        )
        if not result_df.empty:
            result_row = result_df.iloc[0]
            try:
                result = json.loads(result_row.get("Result_JSON") or "{}")
            except json.JSONDecodeError:
                st.warning("Stored Monte Carlo result JSON is invalid.")
                return
            equity_figure = _build_equity_figure(result)

    if result_df.empty:
        st.caption("No Monte Carlo result found for the selected row and method.")
        return

    summary = result.get("summary", {})
    title = monte_carlo.METHOD_LABELS.get(method, "Monte Carlo")
    st.subheader(title)
    cols = st.columns(4)
    cols[0].metric("Valid scenarios", int(summary.get("valid_scenarios", 0)))
    cols[1].metric("Total scenarios", int(summary.get("total_scenarios", 0)))
    cols[2].metric("Valid %", f"{float(summary.get('valid_pct', 0.0)):.1f}%")
    cols[3].metric("Robustness", f"{float(summary.get('robustness_score', 0.0)):.1f}")
    st.caption(f"Interpretation: {summary.get('interpretation', 'n/a')}")
    if summary.get("sample_note"):
        st.warning(str(summary.get("sample_note")))
    st.plotly_chart(equity_figure, width="stretch")
    metrics = _metrics_df(result)
    if not metrics.empty:
        st.dataframe(
            metrics.style.apply(monte_carlo_metric_row_style, axis=1),
            hide_index=True,
            width="content",
        )

    links = st.container(horizontal=True)
    html_url = _static_url(result_row.get("Html_Path"))
    csv_url = _static_url(result_row.get("Csv_Path"))
    json_url = _static_url(result_row.get("Json_Path"))
    if html_url:
        links.link_button("Open HTML Report", html_url, icon=":material/open_in_new:")
    if csv_url:
        links.link_button("Open CSV", csv_url, icon=":material/table:")
    if json_url:
        links.link_button("Open JSON", json_url, icon=":material/data_object:")


def render_monte_carlo_cleanup(method, selected_rows):
    selected_result_ids = [
        result_id
        for result_id in (_row_result_id(row) for row in selected_rows)
        if result_id is not None
    ]
    method_label = monte_carlo.METHOD_LABELS.get(method, method)

    with st.expander("Monte Carlo Cleanup", expanded=False):
        st.caption(
            "Cleanup deletes Monte Carlo result rows and their HTML/CSV/JSON files. "
            "Queue history and job logs are preserved."
        )

        selected_col, method_col, old_col = st.columns(3)
        with selected_col:
            st.markdown("###### Selected result")
            st.caption(f"{len(selected_result_ids)} selected result(s) with stored Monte Carlo output.")
            confirm_selected = st.checkbox(
                "Confirm selected delete",
                key=f"mc_cleanup_confirm_selected_{method}",
                disabled=not selected_result_ids,
            )
            if st.button(
                "Delete Selected",
                key=f"mc_cleanup_delete_selected_{method}",
                icon=":material/delete:",
                disabled=not selected_result_ids or not confirm_selected,
            ):
                summary = database.delete_monte_carlo_results(selected_result_ids)
                st.session_state["mc_cleanup_message"] = ("success", _cleanup_summary_text(summary))
                st.rerun()

        with method_col:
            st.markdown("###### Current method")
            candidates = database.get_monte_carlo_cleanup_candidates(method=method)
            st.caption(f"{len(candidates)} stored {method_label} result(s).")
            confirm_method = st.checkbox(
                f"Confirm delete all {method_label}",
                key=f"mc_cleanup_confirm_method_{method}",
                disabled=candidates.empty,
            )
            if st.button(
                "Delete Method Results",
                key=f"mc_cleanup_delete_method_{method}",
                icon=":material/delete_sweep:",
                disabled=candidates.empty or not confirm_method,
            ):
                summary = database.delete_monte_carlo_results_by_method(method)
                st.session_state["mc_cleanup_message"] = ("success", _cleanup_summary_text(summary))
                st.rerun()

        with old_col:
            st.markdown("###### Older than")
            retention_days = st.number_input(
                "Days",
                min_value=1,
                max_value=3650,
                value=DEFAULT_MONTE_CARLO_RETENTION_DAYS,
                step=1,
                key="mc_cleanup_retention_days",
                help="Deletes stored Monte Carlo results older than this many days across all methods.",
            )
            old_candidates = database.get_monte_carlo_cleanup_candidates(older_than_days=int(retention_days))
            st.caption(f"{len(old_candidates)} result(s) older than {int(retention_days)} day(s).")
            confirm_old = st.checkbox(
                "Confirm old results delete",
                key="mc_cleanup_confirm_old",
                disabled=old_candidates.empty,
            )
            if st.button(
                "Delete Old Results",
                key="mc_cleanup_delete_old",
                icon=":material/event_busy:",
                disabled=old_candidates.empty or not confirm_old,
            ):
                summary = database.delete_old_monte_carlo_results(days=int(retention_days))
                st.session_state["mc_cleanup_message"] = ("success", _cleanup_summary_text(summary))
                st.rerun()


def main():
    st.markdown("## Monte Carlo Analysis")

    if not st.session_state.get("mc_auto_cleanup_done"):
        summary = database.delete_old_monte_carlo_results(days=DEFAULT_MONTE_CARLO_RETENTION_DAYS)
        st.session_state["mc_auto_cleanup_done"] = True
        if int(summary.get("deleted_results", 0) or 0) > 0 or int(summary.get("deleted_files", 0) or 0) > 0:
            st.session_state["mc_cleanup_message"] = (
                "info",
                "Automatic cleanup: " + _cleanup_summary_text(summary),
            )

    cleanup_message = st.session_state.pop("mc_cleanup_message", None)
    if cleanup_message:
        msg_type, msg = cleanup_message
        if msg_type == "success":
            st.success(msg)
        elif msg_type == "warning":
            st.warning(msg)
        else:
            st.info(msg)

    df_strategies = database.get_all_strategies()
    strategy_map = (
        dict(zip(df_strategies["Id"], df_strategies["Name"]))
        if not df_strategies.empty
        else {}
    )
    format_strategy = _format_strategy_factory(strategy_map)
    df_results = database.get_all_backtesting_results()
    if df_results.empty:
        st.info("No backtesting results found. Run backtests before Monte Carlo analysis.")
        return

    strategy_options = list(strategy_map.keys())
    timeframes = ["1d", "4h", "1h"]

    primary_filters = st.container(horizontal=True)
    with primary_filters:
        _restore_multiselect("mc_saved_strategy", "_mc_strategy", strategy_options)
        selected_strategies = st.multiselect(
            "Strategy",
            strategy_options,
            format_func=format_strategy,
            key="_mc_strategy",
            on_change=lambda: _persist("mc_saved_strategy", "_mc_strategy"),
        )

        _restore_multiselect("mc_saved_timeframe", "_mc_timeframe", timeframes)
        selected_timeframes = st.multiselect(
            "Time-Frame",
            timeframes,
            key="_mc_timeframe",
            on_change=lambda: _persist("mc_saved_timeframe", "_mc_timeframe"),
        )

    symbols = sorted(df_results["Symbol"].dropna().astype(str).unique().tolist())
    pending_symbol_selection = st.session_state.pop("mc_pending_symbol_selection", None)
    if pending_symbol_selection:
        st.session_state["mc_saved_symbol"] = pending_symbol_selection
        st.session_state["_mc_symbol"] = pending_symbol_selection
    if "mc_saved_symbol" in st.session_state:
        symbols = sorted(set(symbols).union(st.session_state["mc_saved_symbol"]))

    symbol_filters = st.container(horizontal=True, vertical_alignment="bottom")
    with symbol_filters:
        _restore_multiselect("mc_saved_symbol", "_mc_symbol", symbols)
        selected_symbols = st.multiselect(
            "Symbol",
            symbols,
            key="_mc_symbol",
            on_change=lambda: _persist("mc_saved_symbol", "_mc_symbol"),
        )

        if st.button(
            "Load Top Performers",
            key="mc_load_top_performers",
            icon=":material/add:",
        ):
            load_top_performer_symbols()
            st.rerun()

    top_performers_message = st.session_state.pop("mc_top_performers_message", None)
    if top_performers_message:
        message_type, message_text = top_performers_message
        if message_type == "success":
            st.success(message_text)
        else:
            st.info(message_text)

    today = datetime.now()
    four_years_ago = today.replace(year=today.year - 4)
    with st.expander("Advanced filters", expanded=False):
        advanced_result_filters = st.container(horizontal=True, vertical_alignment="bottom")
        with advanced_result_filters:
            search_date_ini = st.date_input(
                "Start date",
                value=four_years_ago,
                min_value=four_years_ago,
                max_value=today,
                format="DD.MM.YYYY",
                key="mc_start_date",
            )
            search_date_end = st.date_input(
                "End date",
                value=today,
                min_value=search_date_ini,
                max_value=today,
                format="DD.MM.YYYY",
                key="mc_end_date",
            )

            selected_grades = st.multiselect("Quality Grade", ["A", "B", "C", "D", "F"], key="mc_quality_grade")
            selected_approval = st.selectbox("Trading Approved", ["All", "Approved", "Rejected"], key="mc_trading_approved")
            return_positive = st.checkbox("Return % > 0", value=False, key="mc_return_positive")

    st.markdown("##### Monte Carlo Robustness Test")

    method_label = st.radio(
        label="Robustness Test",
        label_visibility="collapsed",
        options=list(METHOD_OPTIONS.keys()),
        horizontal=True,
        captions=[
            "Fast sequence robustness test using existing trades.",
            "Market-path robustness test by rerunning perturbed candles.",
        ],
        help=(
            "**Trade-order shuffling** keeps the original trade results and randomly "
            "changes their order to test whether performance depends on a lucky trade sequence.\n\n"
            "**Candles-based** builds new synthetic OHLCV market paths from the original "
            "candle returns. It samples historical close-to-close returns in a new order, "
            "adds small statistical noise equal to 15% of the historical return volatility, "
            "reconstructs valid candles, and reruns the full backtest for each path."
        ),
    )
    method = METHOD_OPTIONS[method_label]
    default_scenarios = 1000 if method == monte_carlo.METHOD_TRADE_SHUFFLE else 200
    render_mc_score_help()

    df = df_results.copy()
    df["Backtest_Start_Date"] = pd.to_datetime(df["Backtest_Start_Date"])
    df["Backtest_End_Date"] = pd.to_datetime(df["Backtest_End_Date"])
    if selected_strategies:
        df = df[df["Strategy_Id"].isin(selected_strategies)]
    if selected_timeframes:
        df = df[df["Time_Frame"].isin(selected_timeframes)]
    if selected_symbols:
        df = df[df["Symbol"].isin(selected_symbols)]
    if return_positive:
        df = df[df["Return_Perc"] > 0]
    if selected_grades and "Quality_Grade" in df.columns:
        df = df[df["Quality_Grade"].astype(str).str.upper().isin(selected_grades)]
    if selected_approval != "All" and "Trading_Approved" in df.columns:
        approved_value = 1 if selected_approval == "Approved" else 0
        df = df[df["Trading_Approved"].fillna(0).astype(int) == approved_value]
    if search_date_ini and search_date_end:
        start_date = datetime(search_date_ini.year, search_date_ini.month, search_date_ini.day)
        end_date = datetime(search_date_end.year, search_date_end.month, search_date_end.day)
        df = df[(df["Backtest_Start_Date"] <= end_date) & (df["Backtest_End_Date"] >= start_date)]

    mc_results = database.get_all_monte_carlo_results()
    if not mc_results.empty:
        mc_results = mc_results[mc_results["Method"] == method].copy()
        mc_results = mc_results.drop_duplicates(
            subset=["Symbol", "Time_Frame", "Strategy_Id", "Method"],
            keep="first",
        )
        mc_results = mc_results.rename(columns={"id": "Monte_Carlo_Result_Id"})
        df = df.merge(
            mc_results[
                [
                    "Monte_Carlo_Result_Id",
                    "Symbol",
                    "Time_Frame",
                    "Strategy_Id",
                    "Valid_Scenarios",
                    "Scenarios",
                    "Robustness_Score",
                    "Interpretation",
                    "Created_At",
                    "Html_Path",
                ]
            ],
            on=["Symbol", "Time_Frame", "Strategy_Id"],
            how="left",
        )
    else:
        df["Valid_Scenarios"] = pd.NA
        df["Scenarios"] = pd.NA
        df["Robustness_Score"] = pd.NA
        df["Interpretation"] = ""
        df["Created_At"] = ""
        df["Html_Path"] = ""
        df["Monte_Carlo_Result_Id"] = pd.NA

    display_columns = [
        "Monte_Carlo_Result_Id",
        "Symbol",
        "Strategy_Name",
        "Time_Frame",
        "Quality_Score",
        "Quality_Grade",
        "Trading_Approved",
        "Return_Perc",
        "Max_Drawdown_Perc",
        "Profit_Factor",
        "SQN",
        "Trades",
        "Robustness_Score",
        "Interpretation",
        "Valid_Scenarios",
        "Scenarios",
        "Created_At",
        "Strategy_Id",
    ]
    df_display = df[[column for column in display_columns if column in df.columns]].copy()

    styled_display = df_display.style
    if "Quality_Score" in df_display.columns:
        styled_display = styled_display.map(
            quality_score_cell_style,
            subset=["Quality_Score"],
        )
    if "Quality_Grade" in df_display.columns:
        styled_display = styled_display.map(
            quality_grade_cell_style,
            subset=["Quality_Grade"],
        )
    if "Robustness_Score" in df_display.columns:
        styled_display = styled_display.map(
            quality_score_cell_style,
            subset=["Robustness_Score"],
        )
    if "Interpretation" in df_display.columns:
        styled_display = styled_display.map(
            monte_carlo_interpretation_cell_style,
            subset=["Interpretation"],
        )

    grid_keys = [column for column in ["Strategy_Id", "Symbol", "Time_Frame"] if column in df_display.columns]
    grid_signature = 0
    if grid_keys and not df_display.empty:
        grid_signature = int(pd.util.hash_pandas_object(df_display[grid_keys].astype(str), index=False).sum())

    
    st.space()

    event = st.dataframe(
        styled_display,
        width="content",
        key=f"mc_results_grid_{len(df_display)}_{grid_signature}_{method}",
        on_select="rerun",
        selection_mode="multi-row",
        column_config={
            "Strategy_Id": None,
            "Monte_Carlo_Result_Id": None,
            "Symbol": st.column_config.TextColumn("Symbol", pinned=True),
            "Strategy_Name": st.column_config.TextColumn("Strategy", pinned=True),
            "Time_Frame": st.column_config.TextColumn("TF"),
            "Quality_Score": st.column_config.NumberColumn("Quality", format="%.1f"),
            "Quality_Grade": st.column_config.TextColumn("Grade"),
            "Trading_Approved": st.column_config.CheckboxColumn("Approved"),
            "Return_Perc": st.column_config.NumberColumn("Return %", format="%.2f"),
            "Max_Drawdown_Perc": st.column_config.NumberColumn("Max DD %", format="%.2f"),
            "Profit_Factor": st.column_config.NumberColumn("Profit Factor", format="%.2f"),
            "SQN": st.column_config.NumberColumn("SQN", format="%.2f"),
            "Robustness_Score": st.column_config.NumberColumn("MC Score", format="%.1f", help=MC_SCORE_HELP),
            "Interpretation": st.column_config.TextColumn("Interpretation", help=MC_INTERPRETATION_HELP),
            "Valid_Scenarios": st.column_config.NumberColumn("Valid", format="%d"),
            "Scenarios": st.column_config.NumberColumn("Total", format="%d"),
        },
    )
    st.caption(f"{len(df_display)} backtesting result row(s).")

    selected_rows = [
        df_display.iloc[index]
        for index in event.selection.rows
        if 0 <= index < len(df_display)
    ]

    st.markdown("##### Run Settings")
    scenario_label = (
        "Trade Sequences"
        if method == monte_carlo.METHOD_TRADE_SHUFFLE
        else "Market Paths"
    )
    scenario_help = (
        "Number of shuffled trade-order simulations to run."
        if method == monte_carlo.METHOD_TRADE_SHUFFLE
        else (
            "Number of synthetic market paths to generate and backtest. "
            "Higher values make the robustness statistics more stable, "
            "but take longer because each path reruns the full strategy."
        )
    )
    controls = st.container(horizontal=True)
    scenarios = controls.number_input(
        scenario_label,
        min_value=1,
        max_value=5000,
        value=default_scenarios,
        step=50,
        key=f"mc_scenarios_value_{method}",
        help=scenario_help,
        width=180,
    )
    seed = controls.number_input(
        "Randomization Seed",
        min_value=0,
        max_value=999999999,
        value=42,
        step=1,
        help=(
            "Controls the random sample used to build the simulations. "
            "Keep the same seed to reproduce the same result with the same inputs; "
            "change it to test a different random sample."
        ),
        width=220,
    )
    actions = st.container(horizontal=True)
    if actions.button("Refresh", icon=":material/refresh:", key="mc_refresh"):
        st.rerun()
    if actions.button(
        "Run Selected Monte Carlo",
        icon=":material/play_arrow:",
        disabled=not selected_rows,
        help="Select one or more backtesting result rows.",
    ):
        result = _enqueue_rows(selected_rows, method, scenarios, seed)
        queued = len(result["queued"])
        skipped = len(result["skipped"])
        if queued:
            st.session_state["mc_queue_message"] = (
                "success",
                f"Queued {queued} Monte Carlo job(s). Skipped {skipped} already queued/running job(s).",
            )
        elif skipped:
            st.session_state["mc_queue_message"] = ("info", "All selected analyses are already queued or running.")
        st.rerun()

    queue_message = st.session_state.pop("mc_queue_message", None)
    if queue_message:
        msg_type, msg = queue_message
        if msg_type == "success":
            st.success(msg)
        else:
            st.info(msg)

    render_monte_carlo_jobs_status()
    render_monte_carlo_cleanup(method, selected_rows)

    if selected_rows:
        st.divider()
        render_result_detail(selected_rows[0], method)
    else:
        st.caption("Select a row to render the latest Monte Carlo report for the chosen method.")


if __name__ == "__main__":
    main()
