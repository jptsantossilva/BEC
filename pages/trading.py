import pandas as pd
import time
import os
import calendar
import json
from datetime import datetime, timedelta

import streamlit as st
from millify import millify
import streamlit_authenticator as stauth
import altair as alt

import bec.utils.config as config
import bec.utils.database as database
import bec.exchanges.service as binance
from bec.exchanges.registry import get_adapter_for_code
import bec.utils.trading_service as trading_service
from bec.page_config import configure_page
from bec.utils.take_profit import (
    normalize_take_profit_levels,
    parse_executed_take_profit_levels,
)

from bec.my_backtesting import FOLDER_BACKTEST_RESULTS

configure_page()

TRADING_TAB_OPTIONS = [
    "Unrealized PnL",
    "Realized PnL",
    "Signals",
    "Top Performers",
    "Blacklist",
    "Settings",
]
TRADING_ACTIVE_TAB_KEY = "trading_active_tab_saved"
TRADING_ACTIVE_TAB_WIDGET_KEY = "_trading_active_tab_widget"
MAIN_STRATEGIES_WIDGET_KEY = "_main_strategies_widget"
POSITIONS_DISPLAY_COLUMNS = [
    "Symbol",
    "PnL_Perc",
    "PnL_Value",
    "Take Profits",
    "RPQ%",
    "Qty",
    "Buy_Price",
    "Position_Value",
    "Date",
    "Duration",
    "Strategy",
    "Signal_Setup",
]
TAKE_PROFIT_NOT_DEFINED_LABEL = "No TP Defined"
REALIZED_PNL_BOTS = ("1d", "4h", "1h")
ALL_TIME_FILTER = "__all_time__"
ALL_STRATEGIES_FILTER = "__all_strategies__"
MISSING_STRATEGY_FILTER = "__missing_strategy__"
MISSING_STRATEGY_LABEL = "(Missing strategy)"
MONTHLY_RETURNS_MONTHS = [
    {"Month_Number": month, "Month": calendar.month_abbr[month]}
    for month in range(1, 13)
]
REALIZED_PNL_ROLLING_PERIODS = [
    ("24h", timedelta(hours=24)),
    ("7d", timedelta(days=7)),
    ("14d", timedelta(days=14)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
    ("6m", pd.DateOffset(months=6)),
    ("1y", pd.DateOffset(years=1)),
]

# for testing purposes
# st.session_state


def realized_pnl():
    with tab_rpnl:

        st.header("Realized PnL")

        # get years
        years = get_years()

        # Render an empty-state view without crashing the filter widgets.
        if len(years) == 0:
            st.info("There are no closed positions 🤞")
            year = str(datetime.now().year)
            month_number = 13
            strategy_filter = ALL_STRATEGIES_FILTER
            strategy_labels = {ALL_STRATEGIES_FILTER: "All strategies"}
        else:
            filter_rpnl = st.container(horizontal=True, vertical_alignment="bottom")
            # col1, col2, col3, col4 = st.columns([4, 6, 4, 10], vertical_alignment='bottom')
            # years selectbox
            year_options = [ALL_TIME_FILTER] + years
            current_year = str(datetime.now().year)
            default_year_index = (
                year_options.index(current_year) if current_year in year_options else 0
            )
            year = filter_rpnl.selectbox(
                "Year",
                year_options,
                index=default_year_index,
                format_func=lambda option: "All time"
                if option == ALL_TIME_FILTER
                else option,
                width=150,
            )

            # get months
            if year == ALL_TIME_FILTER:
                months_dict = {}
                month_names = []
            else:
                months_dict = get_orders_by_month(year)
                month_names = list(months_dict.values())
            current_month_name = calendar.month_name[datetime.now().month]
            default_month_index = (
                month_names.index(current_month_name)
                if current_month_name in month_names
                else 0
            )

            # months selectbox
            month_selected_name = filter_rpnl.selectbox(
                "Month",
                month_names,
                index=default_month_index,
                width=200,
                disabled=year == ALL_TIME_FILTER,
            )

            disable_full_year = (
                year == ALL_TIME_FILTER or month_selected_name == None
            )
            if year == ALL_TIME_FILTER:
                month_number = 13
            elif month_selected_name == None:
                month_number = 1
            else:  # get month number from month name using months dictionary
                month_number = list(months_dict.keys())[
                    list(months_dict.values()).index(month_selected_name)
                ]

            if filter_rpnl.checkbox("Full Year", disabled=disable_full_year):
                month_number = 13

            period_trades = _get_realized_trades_for_period(str(year), str(month_number))
            strategy_options, strategy_labels = get_realized_strategy_filter_options(
                period_trades
            )
            strategy_filter = filter_rpnl.selectbox(
                "Strategy",
                strategy_options,
                format_func=lambda option: strategy_labels.get(option, option),
                width=300,
            )

        (
            result_closed_positions,
            trades_month_1d,
            trades_month_4h,
            trades_month_1h,
            trades_all,
            strategy_summary,
            strategy_timeframe_matrix,
            exit_reason_summary,
            live_vs_backtest,
            kpis,
        ) = calculate_realized_pnl(str(year), str(month_number), strategy_filter)
        # print("\nPnL - Total")
        # print(result_closed_positions)

        render_realized_kpis(kpis)
        render_realized_rolling_periods(strategy_filter, strategy_labels)

        st.header("Realized PnL - Total")
        result_closed_positions = result_closed_positions.style.map(
            set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
        )
        st.dataframe(result_closed_positions, width="content", hide_index=True)

        render_realized_monthly_returns(years, strategy_filter, strategy_labels)

        st.header("Realized PnL - Strategies")
        if strategy_summary.empty:
            st.info("No closed trades for this strategy filter.")
        else:
            st.dataframe(
                strategy_summary.style.map(
                    set_pnl_color,
                    subset=["PnL_Perc", "PnL_Value", "Best_Trade", "Worst_Trade"],
                ),
                width="content",
                hide_index=True,
                column_config={
                    "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                    "PnL_Value": st.column_config.NumberColumn(
                        format=f"%.{num_decimals}f"
                    ),
                    "Win_Rate": st.column_config.NumberColumn(format="%.2f"),
                    "Best_Trade": st.column_config.NumberColumn(format="%.2f"),
                    "Worst_Trade": st.column_config.NumberColumn(format="%.2f"),
                    "Avg_Trade": st.column_config.NumberColumn(format="%.2f"),
                },
            )

        with st.expander("Strategy x Timeframe Matrix", expanded=False):
            if strategy_timeframe_matrix.empty:
                st.info("No strategy/timeframe data for this filter.")
            else:
                st.dataframe(
                    strategy_timeframe_matrix.style.map(
                        set_pnl_color, subset=["1d", "4h", "1h", "TOTAL"]
                    ),
                    width="content",
                    hide_index=True,
                )

        with st.expander("Exit Reason Analytics", expanded=False):
            if exit_reason_summary.empty:
                st.info("No exit reason data for this filter.")
            else:
                st.dataframe(
                    exit_reason_summary.style.map(
                        set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
                    ),
                    width="content",
                    hide_index=True,
                    column_config={
                        "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                        "PnL_Value": st.column_config.NumberColumn(
                            format=f"%.{num_decimals}f"
                        ),
                    },
                )

        with st.expander("Live vs Backtest", expanded=False):
            if live_vs_backtest.empty:
                st.info("No backtest comparison data for this filter.")
            else:
                st.dataframe(
                    live_vs_backtest.style.map(
                        set_pnl_color,
                        subset=["Live_PnL_Perc", "Live_PnL_Value", "Backtest_Return_Perc"],
                    ),
                    width="content",
                    hide_index=True,
                    column_config={
                        "Live_PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                        "Live_PnL_Value": st.column_config.NumberColumn(
                            format=f"%.{num_decimals}f"
                        ),
                        "Live_Win_Rate": st.column_config.NumberColumn(format="%.2f"),
                        "Backtest_Return_Perc": st.column_config.NumberColumn(
                            format="%.2f"
                        ),
                        "Backtest_Win_Rate_Perc": st.column_config.NumberColumn(
                            format="%.2f"
                        ),
                        "Quality_Score": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header("Realized PnL - Detail")

        with st.expander("All closed trades", expanded=False):
            if trades_all.empty:
                st.info("No closed trades for this filter.")
            else:
                period_label = "all_time" if year == ALL_TIME_FILTER else f"{year}_{month_number}"
                st.download_button(
                    "Export CSV",
                    trades_all.to_csv(index=False).encode("utf-8"),
                    file_name=f"realized_pnl_{period_label}.csv",
                    mime="text/csv",
                    icon=":material/download:",
                )
                st.dataframe(
                    trades_all.style.map(
                        set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
                    ),
                    width="content",
                    hide_index=True,
                    column_config=realized_detail_column_config(),
                )

        st.subheader("Bot 1d")
        st.dataframe(
            trades_month_1d.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config=realized_detail_column_config(),
        )

        st.subheader("Bot 4h")
        st.dataframe(
            trades_month_4h.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config=realized_detail_column_config(),
        )

        st.subheader("Bot 1h")
        st.dataframe(
            trades_month_1h.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config=realized_detail_column_config(),
        )

        # print('\n----------------------------\n')


@st.dialog("Delete Position")
def delete_position(symbol, timeframe, position_id=None, strategy_name=""):
    st.info(
        "As an example, use this to close a position when a symbol is delisted and you need to mark it as sold."
    )
    strategy_label = f" using **{strategy_name}**" if strategy_name else ""
    st.write(
        f"Are you sure you want to delete **{symbol}** from **{timeframe}** timeframe{strategy_label}?"
    )
    unit_price = st.number_input(
        "Unit value", min_value=0.0, value=0.0, step=0.0001, format="%.8f", width=200
    )
    reason = st.text_input(
        "Reason",
        value="Symbol delisted from exchange",
        max_chars=100,
        help="Reason for deleting the position.",
    )
    if st.button("Delete", key="delete_position"):
        trading_service.delete_position(
            symbol=symbol,
            bot=timeframe,
            unit_price=float(unit_price),
            reason=reason,
            position_id=position_id,
        )
        st.rerun()


def _executed_take_profit_levels_from_row(row: pd.Series) -> set[int]:
    levels = parse_executed_take_profit_levels(row.get("Take_Profits_JSON", "[]"))
    for level in (1, 2, 3, 4):
        value = row.get(f"TP{level}", 0)
        try:
            triggered = 0 if pd.isna(value) else int(value)
        except (TypeError, ValueError):
            triggered = 0
        if triggered:
            levels.add(level)
    return levels


def _configured_take_profit_levels_for_row(row: pd.Series) -> list[dict]:
    strategy_params = database.parse_strategy_params(
        row.get("Strategy_Params_JSON", "")
    )
    risk = strategy_params.get("risk") if isinstance(strategy_params, dict) else {}
    if isinstance(risk, dict):
        custom_levels = normalize_take_profit_levels(risk.get("take_profits", []))
        if custom_levels:
            return custom_levels
    strategy_id = str(row.get("Strategy_Id", "") or "").strip()
    if strategy_id:
        strategy_risk = database.get_strategy_risk(strategy_id)
        if isinstance(strategy_risk, dict):
            return normalize_take_profit_levels(strategy_risk.get("take_profits", []))
    return []


def _position_has_atr_trailing(row: pd.Series) -> bool:
    strategy_params = database.parse_strategy_params(
        row.get("Strategy_Params_JSON", "")
    )
    risk = strategy_params.get("risk") if isinstance(strategy_params, dict) else {}
    if isinstance(risk, dict):
        atr = risk.get("atr_trailing", {})
        if isinstance(atr, dict) and bool(atr.get("enabled", False)):
            return True
    strategy_id = str(row.get("Strategy_Id", "") or "").strip()
    if not strategy_id:
        return False
    strategy_risk = database.get_strategy_risk(strategy_id)
    atr = (
        strategy_risk.get("atr_trailing", {}) if isinstance(strategy_risk, dict) else {}
    )
    return bool(atr.get("enabled", False))


def _dataframe_has_atr_trailing(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    return any(_position_has_atr_trailing(row) for _, row in df.iterrows())


def _add_take_profit_display_column(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    if df.empty:
        return df, [], []
    df = df.copy()
    all_options = []
    colors_by_option = {}

    def _labels(row):
        configured = _configured_take_profit_levels_for_row(row)
        if not configured:
            if TAKE_PROFIT_NOT_DEFINED_LABEL not in all_options:
                all_options.append(TAKE_PROFIT_NOT_DEFINED_LABEL)
                colors_by_option[TAKE_PROFIT_NOT_DEFINED_LABEL] = "#F2F2F2"
            return [TAKE_PROFIT_NOT_DEFINED_LABEL]

        executed = _executed_take_profit_levels_from_row(row)
        labels = []
        for tp in configured:
            level = int(tp.get("level", 0) or 0)
            if level <= 0:
                continue
            state = "Triggered" if level in executed else "Pending"
            label = f"TP{level} {state}"
            labels.append(label)
            if label not in all_options:
                all_options.append(label)
                colors_by_option[label] = (
                    "#8FBC8F" if state == "Triggered" else "#EBEBEB"
                )
        return labels

    df["Take Profits"] = df.apply(_labels, axis=1)
    if "Take Profits" in df.columns:
        values = df.pop("Take Profits")
        insert_at = (
            df.columns.get_loc("PnL_Value") + 1
            if "PnL_Value" in df.columns
            else len(df.columns)
        )
        df.insert(insert_at, "Take Profits", values)
    def _take_profit_sort_key(label: str):
        if label == TAKE_PROFIT_NOT_DEFINED_LABEL:
            return (999, 0, 0)
        return (
            0,
            int(label.split(" ", 1)[0].replace("TP", "")),
            0 if label.endswith("Triggered") else 1,
        )

    options = sorted(all_options, key=_take_profit_sort_key)
    colors = [colors_by_option[option] for option in options]
    return df, options, colors


def _take_profit_format(label: str) -> str:
    if label == TAKE_PROFIT_NOT_DEFINED_LABEL:
        return "No TP"
    return label.split(" ", 1)[0]


@st.dialog("Manual Sell")
def forced_sale_position(
    symbol, timeframe, position_id, strategy_id="", strategy_name=""
):
    strategy_label = f" using **{strategy_name}**" if strategy_name else ""
    st.write(f"Sell **{symbol}** from **{timeframe}** timeframe{strategy_label}.")

    sell_amount_perc = st.slider(
        label="Amount",
        min_value=10,
        max_value=100,
        value=25,
        step=5,
        format="%d%%",
    )

    df_pos = database.get_position_by_id(int(position_id))
    if not df_pos.empty:
        balance_qty = df_pos["Qty"].iloc[0]
    else:
        balance_qty = 0

    sell_amount = balance_qty * (sell_amount_perc / 100)
    st.text_input(
        label="Sell Amount / Position Balance",
        value=f"{sell_amount} / {balance_qty}",
        disabled=True,
    )

    sell_reason = f"Manual Sell of {sell_amount_perc}%"
    sell_reason_input = st.text_input("Reason", value=sell_reason)
    if sell_reason_input:
        sell_reason = sell_reason_input

    sell_confirmation = st.checkbox(
        f"I confirm the Sell of **{sell_amount_perc}%** of **{symbol}** from **{timeframe}** bot"
    )

    if sell_confirmation and st.button(
        "Sell Position",
        key=f"sell_position_{position_id}",
        icon=":material/sell:",
        type="primary",
    ):
        sell_result = binance.create_sell_order(
            symbol=symbol,
            bot=timeframe,
            reason=sell_reason,
            percentage=sell_amount_perc,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            position_id=int(position_id),
        )
        if sell_result is None:
            st.warning("No sell order was placed.")
            return

        result, msg = sell_result

        if result:
            st.success(
                f"SOLD **{sell_amount_perc}%** of {symbol} from **{timeframe}** bot!"
            )
            time.sleep(1)
            st.rerun()
        else:
            st.error(msg)


def unrealized_pnl():
    with tab_upnl:

        def _highlight_trading_status(row):
            approved = str(row.get("Trading_Approved", "")).lower() in (
                "approved",
                "1",
                "true",
            )
            color = "#8FBC8F" if approved else "#E9967A"
            change_style = f"background-color: {color}"
            return [
                (
                    change_style
                    if col in ["Trading_Approved", "Trading_Rejection_Reasons"]
                    else ""
                )
                for col in row.index
            ]

        result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = (
            calculate_unrealized_pnl()
        )
        settings_snapshot = config.load_settings()
        positions_df_1d, tp_options_1d, tp_colors_1d = _add_take_profit_display_column(
            positions_df_1d
        )
        positions_df_4h, tp_options_4h, tp_colors_4h = _add_take_profit_display_column(
            positions_df_4h
        )
        positions_df_1h, tp_options_1h, tp_colors_1h = _add_take_profit_display_column(
            positions_df_1h
        )
        tp_options = []
        tp_colors_by_option = {}
        for options, colors in (
            (tp_options_1d, tp_colors_1d),
            (tp_options_4h, tp_colors_4h),
            (tp_options_1h, tp_colors_1h),
        ):
            for option, color in zip(options, colors):
                if option not in tp_colors_by_option:
                    tp_options.append(option)
                    tp_colors_by_option[option] = color
        df_trading_status_all = database.get_top_performers_trading_status(
            strategy_id=settings_snapshot.main_strategies
        )

        def _prepare_trading_status_for_tf(tf: str):
            if df_trading_status_all.empty:
                return pd.DataFrame()
            df = df_trading_status_all[df_trading_status_all["Time_Frame"] == tf].copy()
            if df.empty:
                return df
            df["Trading_Approved"] = df["Trading_Approved"].apply(
                lambda x: "Approved" if int(x) == 1 else "Rejected"
            )
            df["Trading_Rejection_Reasons"] = df["Trading_Rejection_Reasons"].fillna("")
            df.loc[
                (df["Trading_Approved"] == "Rejected")
                & (df["Trading_Rejection_Reasons"] == ""),
                "Trading_Rejection_Reasons",
            ] = "Not evaluated yet"
            df = df.sort_values("Symbol", kind="stable")
            columns_after_symbol = ["Trading_Approved", "Trading_Rejection_Reasons"]
            hidden_columns = ["Strategy_Id"]
            ordered_columns = (
                ["Symbol"]
                + [col for col in columns_after_symbol if col in df.columns]
                + [
                    col
                    for col in df.columns
                    if col != "Symbol"
                    and col not in columns_after_symbol + hidden_columns
                ]
            )
            df = df[ordered_columns]
            return df

        def _prepare_position_sales(position_id: int):
            df = database.get_sell_orders_by_position_id(position_id)
            if df.empty:
                return df

            df = df.copy()
            for column in [
                "Buy_Price",
                "Sell_Price",
                "Buy_Position_Value",
                "Sell_Position_Value",
            ]:
                if column in df.columns:
                    df[column] = df[column].apply(lambda x: f"{float(x):.8f}")
            if "PnL_Perc" in df.columns:
                df["PnL_Perc"] = df["PnL_Perc"].apply(lambda x: f"{float(x):.2f}")
            if "PnL_Value" in df.columns:
                df["PnL_Value"] = df["PnL_Value"].apply(
                    lambda x: f"{{:.{num_decimals}f}}".format(float(x))
                )

            preferred_columns = [
                "Sell_Date",
                "Symbol",
                "Sell_Perc",
                "Sell_Qty",
                "Sell_Price",
                "Sell_Position_Value",
                "PnL_Perc",
                "PnL_Value",
                "Exit_Reason",
                "Stop_Type",
                "Buy_Date",
                "Buy_Price",
            ]
            ordered_columns = [col for col in preferred_columns if col in df.columns]
            ordered_columns += [col for col in df.columns if col not in ordered_columns]
            return df[ordered_columns]

        def _render_selected_position_actions(
            event_positions, positions_df, key_suffix: str
        ):
            selected_position = _resolve_selected_position(
                event_positions, positions_df
            )
            if selected_position is None:
                return

            selected_symbol = selected_position["Symbol"]
            selected_bot = selected_position["Bot"]
            selected_position_id = int(selected_position["Id"])
            selected_strategy_id = str(selected_position.get("Strategy_Id", "") or "")
            selected_strategy_name = str(
                selected_position.get("Strategy_Name", "") or ""
            )
            show_sales_key = f"show_position_sales_{key_suffix}"
            sales_visible = st.session_state.get(show_sales_key) == selected_position_id

            with st.container(horizontal=True):
                if st.button(
                    "Delete Position",
                    key=f"delete_{key_suffix}",
                    icon=":material/delete:",
                ):
                    delete_position(
                        symbol=selected_symbol,
                        timeframe=selected_bot,
                        position_id=selected_position_id,
                        strategy_name=selected_strategy_name,
                    )
                if st.button(
                    "Manual Sell",
                    key=f"forced_sale_{key_suffix}",
                    icon=":material/sell:",
                ):
                    forced_sale_position(
                        symbol=selected_symbol,
                        timeframe=selected_bot,
                        position_id=selected_position_id,
                        strategy_id=selected_strategy_id,
                        strategy_name=selected_strategy_name,
                    )
                sales_button_label = "Hide Sales" if sales_visible else "Show Sales"
                if st.button(
                    sales_button_label,
                    key=f"sales_{key_suffix}",
                    icon=":material/receipt_long:",
                ):
                    st.session_state[show_sales_key] = (
                        None if sales_visible else selected_position_id
                    )
                    st.rerun()

            if st.session_state.get(show_sales_key) == selected_position_id:
                sales_df = _prepare_position_sales(selected_position_id)
                st.caption(f"Sales for {selected_symbol} on {selected_bot}.")
                if sales_df.empty:
                    st.info("No sales found for this position.")
                else:
                    sales_display = sales_df.style.map(
                        set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
                    )
                    st.dataframe(
                        sales_display,
                        width="content",
                        hide_index=True,
                        column_config={
                            "Id": None,
                            "Bot": None,
                            "Strategy_Id": None,
                            "Strategy_Params_JSON": None,
                            "Stop_Trigger_Price": None,
                            "Trail_Stop_ATR_At_Exit": None,
                            "Highest_Price_Since_Entry_At_Exit": None,
                            "Atr_Params_At_Exit": None,
                            "Stop_Type": st.column_config.TextColumn("Exit Type"),
                            "Exit_Reason": st.column_config.TextColumn(
                                "Exit Reason", width="large"
                            ),
                            "Sell_Perc": st.column_config.NumberColumn("Sell %"),
                            "Sell_Qty": st.column_config.NumberColumn("Sell Qty"),
                        },
                    )

        # print("\nUnrealized PnL - Total")
        # print('-------------------------------')
        # print(result_open_positions)

        if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
            st.info("There are no open positions 🤞")

        st.header("Unrealized PnL - Total")

        # Force column to string to avoid Arrow serialization errors (mixed int/str types like "14/24")
        result_open_positions["Positions"] = result_open_positions["Positions"].astype(
            str
        )

        result_open_positions = result_open_positions.style.map(
            set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
        )
        st.dataframe(result_open_positions, width="content", hide_index=True)

        st.header(f"Unrealized PnL - Detail")

        st.subheader("Positions 1d")
        show_trail_stop_atr = _dataframe_has_atr_trailing(
            pd.concat(
                [positions_df_1d, positions_df_4h, positions_df_1h], ignore_index=True
            )
        )

        col_config = {
            "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
            "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
            "Strategy": st.column_config.TextColumn("Strategy"),
            "Signal_Setup": st.column_config.TextColumn("Signal Setup", width=None),
            "Take Profits": st.column_config.MultiselectColumn(
                "Take Profits",
                width=None,
                options=tp_options,
                color=[tp_colors_by_option[option] for option in tp_options],
                format_func=_take_profit_format,
                help="Take-profit levels that are pending or already triggered for this position.",
            ),
            "RPQ%": st.column_config.TextColumn(
                "RPQ%", help="Remaining Position Quantity (%)"
            ),
        }
        if show_trail_stop_atr:
            col_config["Trail_Stop_ATR"] = st.column_config.NumberColumn(format="%.8f")

        positions_display_df_1d = _prepare_positions_display_grid(
            positions_df_1d, show_trail_stop_atr
        )
        positions_display_df_4h = _prepare_positions_display_grid(
            positions_df_4h, show_trail_stop_atr
        )
        positions_display_df_1h = _prepare_positions_display_grid(
            positions_df_1h, show_trail_stop_atr
        )

        event_positions_1d = st.dataframe(
            positions_display_df_1d.style.map(
                set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
            ),
            width="content",
            key="positions_df_1d",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        _render_selected_position_actions(event_positions_1d, positions_df_1d, "1d")

        with st.expander("Top performers eligibility 1d", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 1d.")
            df_trading_status_1d = _prepare_trading_status_for_tf("1d")
            if df_trading_status_1d.empty:
                st.info("No trading approval data for 1d.")
            else:
                styled_status_1d = df_trading_status_1d.style.apply(
                    _highlight_trading_status, axis=1
                )
                st.dataframe(styled_status_1d, width="content", hide_index=True)

        #########################
        st.space()

        st.subheader("Positions 4h")

        event_positions_4h = st.dataframe(
            positions_display_df_4h.style.map(
                set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
            ),
            width="content",
            key="positions_df_4h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        _render_selected_position_actions(event_positions_4h, positions_df_4h, "4h")

        with st.expander("Top performers eligibility 4h", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 4h.")
            df_trading_status_4h = _prepare_trading_status_for_tf("4h")
            if df_trading_status_4h.empty:
                st.info("No trading approval data for 4h.")
            else:
                styled_status_4h = df_trading_status_4h.style.apply(
                    _highlight_trading_status, axis=1
                )
                st.dataframe(styled_status_4h, width="content", hide_index=True)

        #########################
        st.space()

        st.subheader("Positions 1h")

        event_positions_1h = st.dataframe(
            positions_display_df_1h.style.map(
                set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
            ),
            width="content",
            key="positions_df_1h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        _render_selected_position_actions(event_positions_1h, positions_df_1h, "1h")

        with st.expander("Top performers eligibility 1h", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 1h.")
            df_trading_status_1h = _prepare_trading_status_for_tf("1h")
            if df_trading_status_1h.empty:
                st.info("No trading approval data for 1h.")
            else:
                styled_status_1h = df_trading_status_1h.style.apply(
                    _highlight_trading_status, axis=1
                )
                st.dataframe(styled_status_1h, width="content", hide_index=True)


def top_performers():
    with tab_top_perf:
        top_perf = config.read_setting("trade_top_performance")
        st.subheader(f"Top {top_perf} Performers")
        st.caption(
            "The top performers are those in accumulation phase (Price > 50DSMA and Price > 200DSMA and 50DSMA < 200DSMA) and bullish phase (Price > 50DSMA and Price > 200DSMA and 50DSMA > 200DSMA) and then sorted by the price above the 200-day moving average (DSMA) in percentage terms. [Click here for more details](https://twitter.com/jptsantossilva/status/1539976855469428738?s=20)."
        )
        df_mp = database.get_all_symbols_by_market_phase()
        df_mp["Price"] = df_mp["Price"].apply(lambda x: f"{{:.{8}f}}".format(x))
        df_mp["DSMA50"] = df_mp["DSMA50"].apply(lambda x: f"{{:.{8}f}}".format(x))
        df_mp["DSMA200"] = df_mp["DSMA200"].apply(lambda x: f"{{:.{8}f}}".format(x))
        df_mp["Perc_Above_DSMA50"] = df_mp["Perc_Above_DSMA50"].apply(
            lambda x: "{:.2f}".format(x)
        )
        df_mp["Perc_Above_DSMA200"] = df_mp["Perc_Above_DSMA200"].apply(
            lambda x: "{:.2f}".format(x)
        )
        df_mp["ROC_30"] = df_mp["ROC_30"].apply(
            lambda x: "" if pd.isna(x) else "{:.2f}%".format(float(x) * 100)
        )
        df_mp["ROC_60"] = df_mp["ROC_60"].apply(
            lambda x: "" if pd.isna(x) else "{:.2f}%".format(float(x) * 100)
        )
        st.dataframe(df_mp, width="content", hide_index=True)

        filename = "Top_performers_" + trade_against + ".txt"
        if os.path.exists(filename):
            with open(filename, "rb") as file:
                st.download_button(
                    label="Download as TradingView List",
                    data=file,
                    file_name=filename,
                    mime="text/csv",
                )

        st.subheader(f"Historical Top Performers")
        st.caption(
            "Symbols that spend the most number of days in the bullish or accumulating phases"
        )
        df_symbols_days_at_top = (
            database.symbols_by_market_phase_Historical_get_symbols_days_at_top()
        )
        st.dataframe(df_symbols_days_at_top, width="content", hide_index=True)


def signals():
    with tab_signals:
        st.subheader(f"Signals Log")
        st.caption(
            "These signals are just informative. They do not automatically trigger buy and sell orders. You can use these to help you make decisions about when to force a manual exit from an unrealized position."
        )
        expander_signals = st.expander(label="Signals", expanded=False)
        with expander_signals:
            st.write(
                """**SUPER-RSI** - Triggered when all time-frames are below or above a defined level.
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m <= 25
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m >= 80"""
            )
            # st.divider()  # Draws a horizontal line
        df_s = database.get_all_signals_log(num_rows=100)
        st.dataframe(df_s, width="content")


@st.fragment
def blacklist():

    st.subheader("Blacklist")

    st.caption(
        """
        The blacklist allows you to exclude specific symbol tickers from trading.<br>
        When adding a ticker, enter only the base symbol (e.g., ETH, SOL, LTC) instead of the full trading pair (e.g., ETHUSDT, SOLUSDC, LTCBTC).""",
        unsafe_allow_html=True,
    )

    df_blacklist = database.get_symbol_blacklist()

    # Hide 'Id' but keep it for internal tracking
    df_blacklist_display = df_blacklist[["Symbol"]]  # Only show 'Symbol' column

    # Allow user to edit the blacklist without showing 'Id'
    edited_blacklist_display = st.data_editor(
        df_blacklist_display, num_rows="dynamic", width="content"
    )

    # Detect deleted rows (Symbols that were in df_blacklist but are missing in edited_blacklist_display)
    deleted_symbols = df_blacklist[
        ~df_blacklist["Symbol"].isin(edited_blacklist_display["Symbol"])
    ]

    # Merge back to retain Ids and capture new symbols
    edited_blacklist = df_blacklist.merge(
        edited_blacklist_display, on="Symbol", how="right"
    )

    # Ensure Ids for existing rows remain the same
    edited_blacklist["Id"] = edited_blacklist["Id"].apply(
        lambda x: None if pd.isna(x) else int(x)
    )

    # Save button
    if st.button("Save", key="save_blacklist"):
        # Update the database (Insert/Update)
        database.update_blacklist(edited_blacklist)

        # Remove deleted rows
        if not deleted_symbols.empty:
            database.delete_from_blacklist(deleted_symbols)

        st.rerun(scope="fragment")


def _check_exchange_public_apis(exchanges: pd.DataFrame) -> tuple[dict, dict]:
    statuses = {}
    quote_assets = {}
    for _, row in exchanges.iterrows():
        code = str(row["Code"])
        try:
            adapter = get_adapter_for_code(code)
            health = adapter.health_check()
            markets = adapter.load_markets()
            quote_assets[code] = sorted(
                {
                    market.quote_asset
                    for market in markets.values()
                    if market.active and market.quote_asset
                }
            )
            statuses[code] = "Available" if health.available else health.message
        except Exception as exc:
            statuses[code] = f"Unavailable: {exc}"
    return statuses, quote_assets


def _save_exchange_editor(edited: pd.DataFrame, quote_assets: dict) -> None:
    current = database.get_exchange_settings_table().set_index("Code")
    records = []
    for _, row in edited.iterrows():
        code = str(row["Code"])
        quote_asset = str(row["Quote Asset"] or "").strip().upper()
        available_quotes = set(quote_assets.get(code, ()))
        current_quote = str(current.loc[code, "Quote_Asset"] or "").upper()
        if quote_asset != current_quote and not available_quotes:
            raise ValueError(
                f"Check {code} API health before changing its quote asset."
            )
        if available_quotes and quote_asset not in available_quotes:
            raise ValueError(
                f"{quote_asset} is not an active spot quote asset on {code}."
            )
        records.append(
            {
                "Id": int(row["Id"]),
                "Enabled": bool(row["Enabled"]),
                "Quote_Asset": quote_asset,
                "Taker_Fee": float(row["Spot Taker Fee %"]) / 100.0,
                "Buy_Enabled": bool(row.get("Buy Enabled", False)),
                "Sell_Enabled": bool(row.get("Sell Enabled", False)),
                "Partial_Sell_Policy": str(
                    row.get("Partial Sell Policy", "accumulate")
                ),
                "Sizing_Buffer_Pct": float(row.get("Sizing Buffer %", 1.0)),
            }
        )
    database.update_exchange_settings(records)


def _render_trade_schedule_toggles(*, exchange_code: str, disabled: bool = False):
    marker = "_job_toggle_exchange"
    if st.session_state.get(marker) != exchange_code:
        for schedule_name in ("main_1d", "main_4h", "main_1h"):
            st.session_state[f"job_{schedule_name}_enabled"] = (
                database.get_job_schedule_enabled(schedule_name)
            )
        st.session_state[marker] = exchange_code
    for schedule_name, label in (
        ("main_1d", "Enable 1d"),
        ("main_4h", "Enable 4h"),
        ("main_1h", "Enable 1h"),
    ):
        state_key = f"job_{schedule_name}_enabled"

        def _toggle(name=schedule_name, key=state_key):
            database.set_job_schedule_enabled(name, bool(st.session_state[key]))

        st.toggle(
            label,
            key=state_key,
            on_change=_toggle,
            disabled=disabled,
            help=(
                "Enable scheduled strategy execution for this timeframe. Buy and "
                "sell operations remain independently controlled by exchange flags."
            ),
        )


def settings():
    with tab_settings:
        st.markdown("### Exchange")
        exchanges = database.get_exchanges()
        exchanges = exchanges[exchanges["Code"].isin(("binance", "kraken"))]
        active_exchange = database.get_active_exchange(required=False)
        if exchanges.empty:
            st.error("No exchange adapters are configured.")
            return
        st.markdown("#### Exchange Configuration")
        st.caption(
            "Configure availability, the active spot quote asset and the market-order "
            "fee used by backtesting."
        )
        if st.button("Check API Health", icon=":material/health_and_safety:"):
            with st.spinner("Loading public exchange markets..."):
                statuses, quote_assets = _check_exchange_public_apis(exchanges)
            st.session_state["exchange_api_statuses"] = statuses
            st.session_state["exchange_quote_assets"] = quote_assets

        statuses = st.session_state.get("exchange_api_statuses", {})
        quote_assets = st.session_state.get("exchange_quote_assets", {})
        editor = database.get_exchange_settings_table()
        editor = editor[editor["Code"].isin(("binance", "kraken"))].copy()
        editor["Enabled"] = editor["Enabled"].astype(bool)
        editor["Active"] = editor["Is_Default"].astype(bool)
        editor["Quote Asset"] = editor["Quote_Asset"].fillna("USDC")
        editor["Spot Taker Fee %"] = editor["Taker_Fee"].fillna(0.0) * 100.0
        editor["Buy Enabled"] = editor["Buy_Enabled"].astype(bool)
        editor["Sell Enabled"] = editor["Sell_Enabled"].astype(bool)
        editor["Partial Sell Policy"] = editor["Partial_Sell_Policy"].fillna(
            "accumulate"
        )
        editor["Sizing Buffer %"] = editor["Sizing_Buffer_Pct"].fillna(1.0)
        editor["API Status"] = editor["Code"].map(statuses).fillna("Not checked")
        quote_options = sorted(
            {"USDC"}
            | set(editor["Quote Asset"].astype(str))
            | {
                quote
                for values in quote_assets.values()
                for quote in values
            }
        )
        edited = st.data_editor(
            editor[
                [
                    "Id",
                    "Code",
                    "Name",
                    "Enabled",
                    "Active",
                    "Quote Asset",
                    "Spot Taker Fee %",
                    "Buy Enabled",
                    "Sell Enabled",
                    "Partial Sell Policy",
                    "Sizing Buffer %",
                    "Trading_Mode",
                    "API Status",
                ]
            ],
            hide_index=True,
            width="content",
            disabled=["Id", "Code", "Name", "Active", "Trading_Mode", "API Status"],
            column_config={
                "Id": None,
                "Code": st.column_config.TextColumn("Code"),
                "Name": st.column_config.TextColumn("Name"),
                "Enabled": st.column_config.CheckboxColumn("Enabled"),
                "Active": st.column_config.CheckboxColumn("Active"),
                "Quote Asset": st.column_config.SelectboxColumn(
                    "Quote Asset", options=quote_options, required=True
                ),
                "Spot Taker Fee %": st.column_config.NumberColumn(
                    "Spot Taker Fee %", min_value=0.0, max_value=10.0, format="%.4f"
                ),
                "Buy Enabled": st.column_config.CheckboxColumn("Buy Enabled"),
                "Sell Enabled": st.column_config.CheckboxColumn("Sell Enabled"),
                "Partial Sell Policy": st.column_config.SelectboxColumn(
                    "Partial Sell Policy",
                    options=["accumulate", "sell_all", "skip"],
                    required=True,
                ),
                "Sizing Buffer %": st.column_config.NumberColumn(
                    "Sizing Buffer %", min_value=0.0, max_value=10.0, format="%.2f"
                ),
                "Trading_Mode": st.column_config.TextColumn("Trading Mode"),
                "API Status": st.column_config.TextColumn("API Status", width="large"),
            },
            key="exchange_settings_editor",
        )
        if (edited["Spot Taker Fee %"].astype(float) == 0).any():
            st.warning(
                "A zero spot taker fee is configured. Backtest results will not "
                "include exchange trading costs for that exchange."
            )
        if st.button("Save Exchange Settings", icon=":material/save:"):
            try:
                _save_exchange_editor(edited, quote_assets)
                st.success("Exchange settings updated.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))

        exchanges = database.get_exchanges()
        exchanges = exchanges[
            exchanges["Code"].isin(("binance", "kraken"))
            & (exchanges["Enabled"].astype(int) == 1)
        ]
        active_exchange = database.get_active_exchange(required=False)
        exchange_options = [None, *exchanges["Id"].astype(int).tolist()]
        labels = {
            int(row["Id"]): f"{row['Name']} ({row['Code']})"
            for _, row in exchanges.iterrows()
        }
        selected_exchange = st.selectbox(
            "Active exchange",
            exchange_options,
            index=(
                exchange_options.index(int(active_exchange["id"]))
                if active_exchange and int(active_exchange["id"]) in exchange_options
                else 0
            ),
            format_func=lambda value: "No active exchange" if value is None else labels[int(value)],
            key="active_exchange_selector",
        )
        blockers = database.get_exchange_switch_blockers()
        if blockers["open_positions"] or blockers["unsettled_orders"]:
            st.warning(
                "Exchange switching is blocked while open positions or unsettled "
                f"orders exist ({blockers['open_positions']} positions, "
                f"{blockers['unsettled_orders']} orders)."
            )
        if st.button("Apply exchange"):
            try:
                if selected_exchange is None:
                    database.clear_active_exchange()
                else:
                    database.set_active_exchange(int(selected_exchange))
                st.success("Active exchange updated.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
        if active_exchange is None:
            st.warning(
                "No active exchange is selected. Exchange-dependent jobs and trading "
                "remain disabled."
            )
            return
        st.caption(
            f"Active exchange: {active_exchange['name']} · "
            f"Mode: {active_exchange['trading_mode']}"
        )
        if active_exchange["code"] == "kraken":
            if active_exchange["buy_enabled"] or active_exchange["sell_enabled"]:
                st.error(
                    "Kraken live operations are armed. They still require run_mode=live, "
                    "private credentials and enabled main schedules."
                )
            else:
                st.warning(
                    "Kraken live operations are disabled. Public market data and "
                    "backtesting remain available."
                )
            st.warning(
                "Kraken API keys must have no withdrawal permission. Restrict the key "
                "to Query Funds and Create/Modify Orders and configure an IP allowlist."
            )
            if st.button("Check Kraken Private API", icon=":material/key:"):
                from bec.exchanges.live_execution import private_api_status

                available, message = private_api_status()
                (st.success if available else st.error)(message)
            live_mode_key = "kraken_live_run_mode"
            if live_mode_key not in st.session_state:
                st.session_state[live_mode_key] = (
                    config.read_setting("run_mode") == "live"
                )
            live_mode = st.toggle(
                "Live run mode",
                key=live_mode_key,
                help="Master execution gate. Keep disabled except during controlled live operation.",
            )
            desired_mode = "live" if live_mode else "test"
            if config.read_setting("run_mode") != desired_mode:
                config.update_setting("run_mode", desired_mode)
            if live_mode:
                st.error("Live run mode is enabled. Real Kraken orders are possible.")
            st.markdown("### Trade Execution by Timeframe")
            _render_trade_schedule_toggles(
                exchange_code="kraken",
                disabled=not (
                    active_exchange["buy_enabled"]
                    or active_exchange["sell_enabled"]
                ),
            )
            return
        st.divider()
        st.markdown("### Overview")
        # Compact header card
        ta = config.read_setting("trade_against")
        max_pos = config.read_setting("max_number_of_open_positions")

        def _format_overview_value(value: float) -> str:
            if ta == "BTC":
                if float(value) == 0:
                    return "0"
                return f"{float(value):.8f}".rstrip("0").rstrip(".")
            return millify(value, precision=2)

        total_locked = float(database.get_total_locked_values())
        cur_bal = float(binance.get_symbol_balance(ta))
        avail = max(cur_bal - total_locked, 0)
        open_now = database.get_num_open_positions()
        rem = max(max_pos - open_now, 0)
        next_pos = 0 if rem == 0 else (avail / rem)
        # c1, c2, c3, c4, c5 = st.columns(5)
        # c1.metric("Trade Against", ta)
        # c2.metric("Max Open", max_pos)
        # c3.metric("Locked", f"{total_locked} {ta}")
        # c4.metric(f"Available {ta}", millify(avail, precision=2))
        # c5.metric(f"Next Pos Size {ta}", millify(next_pos, precision=2))

        c6, c7, c8, c9 = st.columns(4)
        c6.metric(f"Balance {ta}", _format_overview_value(avail))
        c7.metric(f"Locked {ta}", _format_overview_value(total_locked))
        c8.metric("Available Positions", rem)
        c9.metric(f"Next Position Size {ta}", _format_overview_value(next_pos))

        # st.space("medium")
        # st.space("small")
        st.divider()

        st.markdown("### Strategies")
        with st.container(border=False):
            st.markdown("Main Strategies")
            st.caption(
                "Primary strategies used by the trading timeframes (1d/4h/1h) for buy/sell decisions. "
                "Also used in the daily refresh to apply approved backtesting results and update the symbols in Positions."
            )

            def _normalize_main_strategies(value):
                if isinstance(value, list):
                    strategy_ids = value
                else:
                    try:
                        parsed = json.loads(str(value))
                        strategy_ids = parsed if isinstance(parsed, list) else [parsed]
                    except Exception:
                        strategy_ids = list(config.DEFAULT_MAIN_STRATEGIES)

                valid_strategy_ids = []
                for strategy_id in strategy_ids:
                    strategy_id = str(strategy_id)
                    if (
                        strategy_id in dict_strategies_main
                        and strategy_id not in valid_strategy_ids
                    ):
                        valid_strategy_ids.append(strategy_id)
                fallback = config.DEFAULT_MAIN_STRATEGIES[0]
                return valid_strategy_ids or (
                    [fallback] if fallback in dict_strategies_main else []
                )

            saved_main_strategies = _normalize_main_strategies(
                config.read_setting("main_strategies")
            )
            if st.session_state.get(MAIN_STRATEGIES_WIDGET_KEY) is None:
                st.session_state[MAIN_STRATEGIES_WIDGET_KEY] = saved_main_strategies

            def _save_main_strategies(selected):
                selected = _normalize_main_strategies(selected)
                patch = {"main_strategies": json.dumps(selected)}
                config.update_settings(patch)

            selected_main_strategies = st.multiselect(
                "Main Strategies",
                list(dict_strategies_main.keys()),
                key=MAIN_STRATEGIES_WIDGET_KEY,
                format_func=format_func_strategies_main,
                label_visibility="collapsed",
            )

            normalized_selected_main_strategies = _normalize_main_strategies(
                selected_main_strategies
            )
            if normalized_selected_main_strategies != saved_main_strategies:
                _save_main_strategies(normalized_selected_main_strategies)

        with st.container(border=False):
            st.markdown("Bitcoin Strategy")
            st.caption(
                "Used by the Auto-switch logic to evaluate BTC market regime (bull/bear) on BTC/stablecoin. "
                "It controls when the app switches trade exposure between stablecoin and BTC."
            )
            btc_strategy_options = list(dict_strategies_btc.keys())
            if not btc_strategy_options:
                st.warning("No approved Bitcoin Strategy is available.")
            else:
                saved_btc_strategy = str(config.read_setting("btc_strategy") or "")
                if saved_btc_strategy not in btc_strategy_options:
                    saved_btc_strategy = btc_strategy_options[0]
                    config.update_setting("btc_strategy", saved_btc_strategy)
                if st.session_state.get("btc_strategy") not in btc_strategy_options:
                    st.session_state.btc_strategy = saved_btc_strategy
                st.selectbox(
                    "BTC Strategy",
                    btc_strategy_options,
                    key="btc_strategy",
                    on_change=lambda: config.update_setting(
                        "btc_strategy", st.session_state.btc_strategy
                    ),
                    format_func=format_func_strategies_btc,
                    label_visibility="collapsed",
                    width=400,
                )

            with st.popover("Auto-switch (Advanced)"):
                st.caption(
                    "When enabled, BEC can close positions and convert balances between stablecoin and BTC "
                    "automatically. This can trigger full portfolio reallocation and materially change risk."
                )
                if "trade_against_switch" not in st.session_state:
                    st.session_state.trade_against_switch = config.read_setting(
                        "trade_against_switch"
                    )
                st.checkbox(
                    "Auto-switch Stablecoin/BTC",
                    key="trade_against_switch",
                    on_change=lambda: config.update_setting(
                        "trade_against_switch", st.session_state.trade_against_switch
                    ),
                )
                if "trade_against_switch_stablecoin" not in st.session_state:
                    st.session_state.trade_against_switch_stablecoin = (
                        config.read_setting("trade_against_switch_stablecoin")
                    )
                st.selectbox(
                    "Stablecoin for auto-switch",
                    ["USDC", "USDT"],
                    key="trade_against_switch_stablecoin",
                    on_change=lambda: config.update_setting(
                        "trade_against_switch_stablecoin",
                        st.session_state.trade_against_switch_stablecoin,
                    ),
                )

        st.space()

        st.markdown("### Trade Execution by Timeframe")
        with st.container():
            _render_trade_schedule_toggles(exchange_code="binance")

        # st.space()
        st.divider()

        st.markdown("### Position Sizing")
        with st.container(horizontal=True):

            # c1, c2 = st.columns(2)
            # Trade Against
            if "trade_against" not in st.session_state:
                st.session_state.trade_against = config.read_setting("trade_against")

            def on_ta():
                config.update_setting("trade_against", st.session_state.trade_against)

            st.selectbox(
                label="Trade Against",
                options=["USDC", "USDT", "BTC"],
                key="trade_against",
                on_change=on_ta,
                width=200,
            )

            # Max open positions
            if "max_number_of_open_positions" not in st.session_state:
                st.session_state.max_number_of_open_positions = config.read_setting(
                    "max_number_of_open_positions"
                )

            def on_max():
                config.update_setting(
                    "max_number_of_open_positions",
                    st.session_state.max_number_of_open_positions,
                )

            st.number_input(
                label="Max Open Positions",
                min_value=1,
                step=1,
                key="max_number_of_open_positions",
                on_change=on_max,
                width=200,
            )

            # Min size
            # c3 = st.columns(1)
            if "min_position_size" not in st.session_state:
                st.session_state.min_position_size = float(
                    config.read_setting("min_position_size")
                )

            def on_min():
                MIN_USD = 20
                ta = st.session_state.trade_against
                if ta in ["USDC", "USDT"]:
                    st.session_state.min_position_size = max(
                        float(st.session_state.min_position_size), MIN_USD
                    )
                else:
                    if float(st.session_state.min_position_size) >= MIN_USD:
                        st.session_state.min_position_size = 0.0001
                config.update_setting(
                    "min_position_size", st.session_state.min_position_size
                )

            min_kwargs = (
                dict(min_value=20.0, step=10.0, format=None)
                if st.session_state.trade_against in ["USDC", "USDT"]
                else dict(min_value=0.0001, step=0.0001, format="%.4f")
            )
            st.number_input(
                label="Minimum Position Size",
                key="min_position_size",
                on_change=on_min,
                width=200,
                **min_kwargs,
            )

        # Top performers & Tradable ratio
        with st.container(horizontal=False):
            if "trade_top_performance" not in st.session_state:
                st.session_state.trade_top_performance = config.read_setting(
                    "trade_top_performance"
                )
            st.slider(
                label="Trade Top Performance Symbols",
                min_value=0,
                max_value=500,
                step=5,
                key="trade_top_performance",
                on_change=lambda: config.update_setting(
                    "trade_top_performance", st.session_state.trade_top_performance
                ),
                width=635,
            )

            if "tradable_balance_ratio" not in st.session_state:
                st.session_state.tradable_balance_ratio = (
                    config.read_setting("tradable_balance_ratio") * 100
                )
            st.slider(
                label="Tradable Balance Ratio",
                min_value=0,
                max_value=100,
                step=1,
                format="%d%%",
                key="tradable_balance_ratio",
                on_change=lambda: config.update_setting(
                    "tradable_balance_ratio",
                    st.session_state.tradable_balance_ratio / 100,
                ),
                width=635,
            )

        st.divider()

        st.markdown("### Locked Values")
        with st.container(border=False):
            if "lock_values" not in st.session_state:
                st.session_state.lock_values = config.read_setting("lock_values")
            st.checkbox(
                "Lock values from partial sales",
                key="lock_values",
                on_change=lambda: (
                    (
                        database.release_all_values()
                        if not st.session_state.lock_values
                        else None
                    ),
                    config.update_setting("lock_values", st.session_state.lock_values),
                ),
                help="""When **enabled**, means that any amount obtained from partially selling a position will be temporarily locked and cannot be used to purchase another position until the entire position is sold. 
                            \nWhen **disabled**, partial sales can be freely reinvested into new positions. It's important to note that this may increase the risk of larger position amounts, as funds from partial sales may be immediately reinvested without reservation.
                        """,
            )
            if st.session_state.lock_values:
                st.caption(
                    "Note that disabling this option will **release all** locked values."
                )

            # with st.expander(label="Current Locked Values", expanded=True):
            df = database.get_all_locked_values()
            df_sel = df.copy()
            df_sel.insert(0, "Select", False)
            edited = st.data_editor(
                df_sel,
                hide_index=True,
                width="content",
                column_config={"Select": st.column_config.CheckboxColumn(), "Id": None},
                disabled=["Bot", "Symbol", "Locked_Amount", "Locked_At"],
            )
            picked = edited[edited.Select]
            if st.button("Unlock Selected", disabled=picked.empty):
                for _id in picked.Id.to_list():
                    if _id and _id > 0:
                        database.release_locked_value_by_id(_id)
                st.rerun()

        st.divider()

        with st.container(border=False):
            st.markdown("### Telegram")
            if "bot_prefix" not in st.session_state:
                st.session_state.bot_prefix = config.read_setting("bot_prefix")
            st.text_input(
                label="Telegram Messages Prefix",
                key="bot_prefix",
                on_change=lambda: config.update_setting(
                    "bot_prefix", st.session_state.bot_prefix
                ),
                width=200,
                help="When there are multiple instances of BEC running, the prefix is useful to distinguish which BEC the telegram message belongs to.",
            )
            if not str(st.session_state.bot_prefix or "").strip():
                st.warning(
                    "Telegram Messages Prefix is empty. Telegram messages will not identify which BEC instance sent them."
                )


def show_main_page():

    global trade_against
    trade_against = config.read_setting("trade_against")

    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2
    active_exchange = database.get_active_exchange(required=False)
    if active_exchange:
        st.caption(f"Exchange: {active_exchange['name']} ({active_exchange['code']})")
    else:
        st.warning("No active exchange selected. Open Trading → Settings to select one.")

    global tab_upnl, tab_rpnl, tab_top_perf, tab_signals, tab_blacklist, tab_settings
    if st.session_state.get(TRADING_ACTIVE_TAB_KEY) not in TRADING_TAB_OPTIONS:
        legacy_tab = st.session_state.get("trading_active_tab")
        st.session_state[TRADING_ACTIVE_TAB_KEY] = (
            legacy_tab if legacy_tab in TRADING_TAB_OPTIONS else TRADING_TAB_OPTIONS[0]
        )

    saved_tab = st.session_state[TRADING_ACTIVE_TAB_KEY]

    active_tab = st.segmented_control(
        "Trading tab",
        TRADING_TAB_OPTIONS,
        default=saved_tab,
        key=TRADING_ACTIVE_TAB_WIDGET_KEY,
        label_visibility="collapsed",
    )
    if active_tab not in TRADING_TAB_OPTIONS:
        active_tab = saved_tab
    st.session_state[TRADING_ACTIVE_TAB_KEY] = active_tab

    tab_container = st.container()
    tab_upnl = tab_container
    tab_rpnl = tab_container
    tab_signals = tab_container
    tab_top_perf = tab_container
    tab_blacklist = tab_container
    tab_settings = tab_container

    if active_tab == "Unrealized PnL":
        unrealized_pnl()
    elif active_tab == "Realized PnL":
        realized_pnl()
    elif active_tab == "Signals":
        signals()
    elif active_tab == "Top Performers":
        top_performers()
    elif active_tab == "Blacklist":
        with tab_blacklist:
            blacklist()
    elif active_tab == "Settings":
        settings()


# Get years from orders
def get_years():
    years = database.get_years_from_orders_by_side("SELL")
    return years


# get months with orders within the year
def get_orders_by_month(year: str):

    months = database.get_months_from_orders_by_year_side(year, "SELL")

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict


def get_realized_strategy_filter_options(
    trades: pd.DataFrame | None = None,
) -> tuple[list[str], dict[str, str]]:
    labels = {ALL_STRATEGIES_FILTER: "All strategies"}
    options = [ALL_STRATEGIES_FILTER]

    if trades is None or trades.empty:
        return options, labels

    trades = _normalize_realized_strategy_columns(trades)
    strategies = (
        trades[["Strategy_Id", "Strategy"]]
        .drop_duplicates()
        .sort_values("Strategy", kind="stable")
    )
    has_missing = False
    for _, strategy in strategies.iterrows():
        strategy_id = str(strategy.get("Strategy_Id", "") or "").strip()
        strategy_label = str(strategy.get("Strategy", "") or "").strip()
        if not strategy_id:
            has_missing = True
            continue
        options.append(strategy_id)
        labels[strategy_id] = strategy_label or strategy_id

    if has_missing:
        options.append(MISSING_STRATEGY_FILTER)
        labels[MISSING_STRATEGY_FILTER] = MISSING_STRATEGY_LABEL
    return options, labels


def _realized_strategy_label(row: pd.Series) -> str:
    strategy_name = str(row.get("Strategy_Name", "") or "").strip()
    strategy_id = str(row.get("Strategy_Id", "") or "").strip()
    return strategy_name or strategy_id or MISSING_STRATEGY_LABEL


def _normalize_realized_strategy_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = df.copy()
    if "Strategy_Id" not in df.columns:
        df["Strategy_Id"] = ""
    if "Strategy_Name" not in df.columns:
        df["Strategy_Name"] = ""
    df["Strategy_Id"] = df["Strategy_Id"].fillna("").astype(str).str.strip()
    df["Strategy_Name"] = df["Strategy_Name"].fillna("").astype(str).str.strip()
    df["Strategy"] = df.apply(_realized_strategy_label, axis=1)
    return df


def _filter_realized_trades_by_strategy(
    df: pd.DataFrame, strategy_filter: str
) -> pd.DataFrame:
    if df.empty or strategy_filter == ALL_STRATEGIES_FILTER:
        return df.copy()
    df = _normalize_realized_strategy_columns(df)
    if strategy_filter == MISSING_STRATEGY_FILTER:
        return df[df["Strategy_Id"] == ""].copy()
    return df[df["Strategy_Id"] == str(strategy_filter)].copy()


def _strategy_filter_label(
    strategy_filter: str, labels: dict[str, str] | None = None
) -> str:
    if labels and strategy_filter in labels:
        return labels[strategy_filter]
    if strategy_filter == ALL_STRATEGIES_FILTER:
        return "All strategies"
    if strategy_filter == MISSING_STRATEGY_FILTER:
        return MISSING_STRATEGY_LABEL
    return str(strategy_filter)


def _numeric_realized_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = df.copy()
    for column in ("PnL_Perc", "PnL_Value", "Sell_Position_Value"):
        df[column] = pd.to_numeric(df.get(column, 0.0), errors="coerce").fillna(0.0)
    return df


def _weighted_realized_pnl_perc(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    df = _numeric_realized_trades(df)
    denominator = df["Sell_Position_Value"].sum()
    if denominator == 0:
        return 0.0
    return float((df["PnL_Perc"] * df["Sell_Position_Value"]).sum() / denominator)


def _realized_period_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"PnL_Perc": 0.0, "PnL_Value": 0.0, "Positions": 0}
    df = _numeric_realized_trades(df)
    return {
        "PnL_Perc": _weighted_realized_pnl_perc(df),
        "PnL_Value": float(df["PnL_Value"].sum()),
        "Positions": int(len(df)),
    }


def _get_realized_trades_for_period(year: str, month: str) -> pd.DataFrame:
    trades = database.get_orders_by_side_year_month("SELL", str(year), str(month))
    return _normalize_realized_strategy_columns(trades)


def _get_realized_trades_for_year(
    year: str, strategy_filter: str = ALL_STRATEGIES_FILTER
) -> pd.DataFrame:
    trades = _get_realized_trades_for_period(str(year), "13")
    return _filter_realized_trades_by_strategy(trades, strategy_filter)


def calculate_realized_rolling_periods(
    strategy_filter: str = ALL_STRATEGIES_FILTER,
) -> pd.DataFrame:
    trades = _get_realized_trades_for_period(ALL_TIME_FILTER, "13")
    trades = _filter_realized_trades_by_strategy(trades, strategy_filter)
    if trades.empty or "Sell_Date" not in trades.columns:
        return pd.DataFrame(
            columns=["Period", "PnL_Perc", "PnL_Value", "Positions"]
        )

    trades = _numeric_realized_trades(trades)
    trades["Sell_Date"] = pd.to_datetime(trades["Sell_Date"], errors="coerce")
    trades = trades.dropna(subset=["Sell_Date"])
    if trades.empty:
        return pd.DataFrame(
            columns=["Period", "PnL_Perc", "PnL_Value", "Positions"]
        )

    now = pd.Timestamp(datetime.now())
    records = []
    for label, offset in REALIZED_PNL_ROLLING_PERIODS:
        start_date = now - offset
        period_trades = trades[trades["Sell_Date"] >= start_date]
        records.append(
            {
                "Period": label,
                **_realized_period_summary(period_trades),
            }
        )
    return pd.DataFrame(records)


def calculate_monthly_realized_returns(
    years: list[str], strategy_filter: str = ALL_STRATEGIES_FILTER
) -> pd.DataFrame:
    records = []

    for year in years:
        trades = _get_realized_trades_for_year(str(year), strategy_filter)
        if not trades.empty and "Sell_Date" in trades.columns:
            trades = trades.copy()
            trades["Sell_Date"] = pd.to_datetime(trades["Sell_Date"], errors="coerce")
            trades = trades.dropna(subset=["Sell_Date"])

        for month in MONTHLY_RETURNS_MONTHS:
            month_number = month["Month_Number"]
            month_trades = pd.DataFrame()
            if not trades.empty and "Sell_Date" in trades.columns:
                month_trades = trades[trades["Sell_Date"].dt.month == month_number]
            summary = _realized_period_summary(month_trades)
            records.append(
                {
                    "Year": str(year),
                    "Month": month["Month"],
                    "Month_Number": month_number,
                    "Period": month["Month"],
                    "Period_Order": month_number,
                    "Strategy": _strategy_filter_label(strategy_filter),
                    **summary,
                }
            )

        annual_summary = _realized_period_summary(trades)
        records.append(
            {
                "Year": str(year),
                "Month": "Total",
                "Month_Number": 13,
                "Period": "Total",
                "Period_Order": 13,
                "Strategy": _strategy_filter_label(strategy_filter),
                **annual_summary,
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        return df
    df["Return_Label"] = df.apply(
        lambda row: f'{float(row["PnL_Perc"]):+.2f}%'
        if int(row["Positions"]) > 0
        else "",
        axis=1,
    )
    df["PnL_Value_Label"] = df["PnL_Value"].apply(
        lambda value: f"{float(value):.{num_decimals}f}"
    )
    return df


def render_realized_monthly_returns(
    years: list[str],
    strategy_filter: str = ALL_STRATEGIES_FILTER,
    strategy_labels: dict[str, str] | None = None,
):
    if not years:
        return

    monthly_returns = calculate_monthly_realized_returns(years, strategy_filter)
    monthly_returns["Strategy"] = _strategy_filter_label(strategy_filter, strategy_labels)
    traded_months = monthly_returns[
        (monthly_returns["Month_Number"] <= 12) & (monthly_returns["Positions"] > 0)
    ]
    if monthly_returns.empty or traded_months.empty:
        return

    max_abs_return = max(float(traded_months["PnL_Perc"].abs().max()), 1.0)

    st.header("Monthly Realized Returns")
    st.caption(
        "Weighted realized PnL% by sell position value. Empty cells mean no closed trades."
    )

    heatmap = (
        alt.Chart(monthly_returns)
        .mark_rect(stroke="white", strokeWidth=1)
        .encode(
            x=alt.X(
                "Period:N",
                sort=list(calendar.month_abbr[1:]) + ["Total"],
                title=None,
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("Year:N", sort="-x", title=None),
            color=alt.condition(
                "datum.Positions == 0",
                alt.value("#F1F3F5"),
                alt.Color(
                    "PnL_Perc:Q",
                    title="Weighted PnL%",
                    scale=alt.Scale(
                        domain=[-max_abs_return, 0, max_abs_return],
                        range=["#D95F5F", "#F7F3C4", "#4F9D69"],
                    ),
                ),
            ),
            tooltip=[
                alt.Tooltip("Year:N"),
                alt.Tooltip("Period:N", title="Month"),
                alt.Tooltip("Strategy:N"),
                alt.Tooltip("PnL_Perc:Q", title="Weighted PnL%", format="+.2f"),
                alt.Tooltip(
                    "PnL_Value:Q",
                    title=f"PnL {trade_against}",
                    format=f".{num_decimals}f",
                ),
                alt.Tooltip("Positions:Q", title="Trades", format=",d"),
            ],
        )
        .properties(height=max(140, 34 * len(years)))
    )

    labels = (
        alt.Chart(monthly_returns)
        .mark_text(fontSize=12)
        .encode(
            x=alt.X("Period:N", sort=list(calendar.month_abbr[1:]) + ["Total"]),
            y=alt.Y("Year:N", sort="-x"),
            text="Return_Label:N",
            color=alt.condition(
                "abs(datum.PnL_Perc) > 0.65 * "
                + str(max_abs_return)
                + " && datum.Positions > 0",
                alt.value("white"),
                alt.value("#1F2933"),
            ),
        )
    )

    st.altair_chart((heatmap + labels).interactive(), use_container_width=True)

    with st.expander("Monthly Returns Distribution"):
        st.caption(
            "This is complementary to the heatmap: it shows how monthly outcomes are distributed, without their calendar order."
        )
        distribution = traded_months.copy()
        distribution["Result"] = distribution["PnL_Perc"].apply(
            lambda value: "Positive" if float(value) >= 0 else "Negative"
        )
        mean_return = float(distribution["PnL_Perc"].mean())
        median_return = float(distribution["PnL_Perc"].median())

        bars = (
            alt.Chart(distribution)
            .mark_bar()
            .encode(
                x=alt.X(
                    "PnL_Perc:Q",
                    bin=alt.Bin(maxbins=16),
                    title="Weighted monthly PnL%",
                ),
                y=alt.Y("count():Q", title="Months"),
                color=alt.Color(
                    "Result:N",
                    scale=alt.Scale(
                        domain=["Negative", "Positive"],
                        range=["#D95F5F", "#4F9D69"],
                    ),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("count():Q", title="Months"),
                    alt.Tooltip("PnL_Perc:Q", title="Weighted PnL%", format="+.2f"),
                ],
            )
            .properties(height=240)
        )
        mean_rule = (
            alt.Chart(pd.DataFrame({"value": [mean_return], "Metric": ["Mean"]}))
            .mark_rule(color="#C62828", strokeDash=[6, 4], size=2)
            .encode(
                x="value:Q",
                tooltip=["Metric:N", alt.Tooltip("value:Q", format="+.2f")],
            )
        )
        median_rule = (
            alt.Chart(pd.DataFrame({"value": [median_return], "Metric": ["Median"]}))
            .mark_rule(color="#F59E0B", strokeDash=[2, 4], size=2)
            .encode(
                x="value:Q",
                tooltip=["Metric:N", alt.Tooltip("value:Q", format="+.2f")],
            )
        )
        st.altair_chart(bars + mean_rule + median_rule, use_container_width=True)


def render_realized_rolling_periods(
    strategy_filter: str = ALL_STRATEGIES_FILTER,
    strategy_labels: dict[str, str] | None = None,
):
    rolling_periods = calculate_realized_rolling_periods(strategy_filter)
    if rolling_periods.empty:
        return

    st.subheader("Realized PnL by Period")
    st.caption(
        "Rolling weighted realized PnL% by sell position value."
        f" Strategy: {_strategy_filter_label(strategy_filter, strategy_labels)}."
    )

    header_cells = []
    value_cells = []
    for row in rolling_periods.to_dict("records"):
        period = str(row["Period"])
        positions = int(row.get("Positions", 0) or 0)
        pnl_perc = float(row.get("PnL_Perc", 0.0) or 0.0)
        header_cells.append(f"<th>{period}</th>")
        if positions == 0:
            value_cells.append('<td><span class="period-empty">No trades</span></td>')
            continue

        direction_class = "positive" if pnl_perc >= 0 else "negative"
        value_cells.append(
            "<td>"
            f'<span class="period-value {direction_class}">'
            f'<span class="period-arrow"></span>{pnl_perc:+.2f}%'
            "</span>"
            f'<span class="period-trades">{positions} trades</span>'
            "</td>"
        )

    st.markdown(
        f"""
        <style>
            .realized-period-table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                border: 1px solid rgba(49, 51, 63, 0.2);
                border-radius: 0.5rem;
                overflow: visible;
                margin: 0.25rem 0 1.5rem;
                table-layout: fixed;
            }}
            .realized-period-table th {{
                background: #F9FAFB;
                color: rgba(49, 51, 63, 0.75);
                font-size: 0.875rem;
                font-weight: 400;
                padding: 0.45rem 0.5rem;
                text-align: center;
                border-right: 1px solid rgba(49, 51, 63, 0.12);
                border-bottom: 1px solid rgba(49, 51, 63, 0.12);
            }}
            .realized-period-table th:first-child {{
                border-top-left-radius: 0.5rem;
            }}
            .realized-period-table th:last-child {{
                border-top-right-radius: 0.5rem;
            }}
            .realized-period-table th:last-child,
            .realized-period-table td:last-child {{
                border-right: 0;
            }}
            .realized-period-table td {{
                background: #FFFFFF;
                padding: 0.85rem 0.5rem;
                text-align: center;
                border-right: 1px solid rgba(49, 51, 63, 0.12);
                vertical-align: middle;
            }}
            .realized-period-table tbody tr:last-child td:first-child {{
                border-bottom-left-radius: 0.5rem;
            }}
            .realized-period-table tbody tr:last-child td:last-child {{
                border-bottom-right-radius: 0.5rem;
            }}
            .period-value {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 0.25rem;
                font-weight: 500;
                line-height: 1.2;
                white-space: nowrap;
            }}
            .period-value.positive {{
                color: #00A65A;
            }}
            .period-value.negative {{
                color: #EF4444;
            }}
            .period-arrow {{
                width: 0;
                height: 0;
                border-left: 4px solid transparent;
                border-right: 4px solid transparent;
            }}
            .period-value.positive .period-arrow {{
                border-bottom: 6px solid #00A65A;
            }}
            .period-value.negative .period-arrow {{
                border-top: 6px solid #EF4444;
            }}
            .period-trades {{
                display: block;
                color: #6B7280;
                font-size: 0.72rem;
                margin-top: 0.15rem;
            }}
            .period-empty {{
                color: #9CA3AF;
                font-size: 0.8rem;
                white-space: nowrap;
            }}
        </style>
        <table class="realized-period-table">
            <thead><tr>{''.join(header_cells)}</tr></thead>
            <tbody><tr>{''.join(value_cells)}</tr></tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )


def realized_detail_column_config():
    return {
        "Id": None,
        "Strategy_Id": None,
        "Strategy_Name": None,
        "Strategy_Params_JSON": None,
        "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
        "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
        "Strategy": st.column_config.TextColumn("Strategy", width="medium"),
        "Exit_Reason": st.column_config.TextColumn(width="large"),
        "Stop_Details": st.column_config.JsonColumn(
            "Stop_Details",
            width="large",
            help="Structured stop metadata saved at trade exit.",
        ),
    }


def render_realized_kpis(kpis: dict):
    st.header("Realized PnL Overview")
    cols = st.columns(6)
    cols[0].metric("Realized PnL", f"{kpis['pnl_value']:.{num_decimals}f}")
    cols[1].metric("Weighted PnL%", f"{kpis['pnl_perc']:.2f}%")
    cols[2].metric("Win Rate", f"{kpis['win_rate']:.2f}%")
    cols[3].metric("Closed Trades", f"{kpis['positions']}")
    cols[4].metric("Best Strategy", kpis["best_strategy"])
    cols[5].metric("Worst Strategy", kpis["worst_strategy"])


def _format_realized_summary_numbers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "PnL_Perc" in df.columns:
        df["PnL_Perc"] = df["PnL_Perc"].apply(lambda x: "{:.2f}".format(float(x)))
    if "PnL_Value" in df.columns:
        df["PnL_Value"] = df["PnL_Value"].apply(
            lambda x: f"{{:.{num_decimals}f}}".format(float(x))
        )
    return df


def _build_realized_bot_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for bot in REALIZED_PNL_BOTS:
        summary = _realized_period_summary(
            trades[trades["Bot"] == bot] if not trades.empty else pd.DataFrame()
        )
        rows.append({"Bot": bot, **summary})

    total_summary = _realized_period_summary(trades)
    rows.append({"Bot": "TOTAL", **total_summary})
    return pd.DataFrame(rows)


def _build_realized_strategy_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "Strategy",
                "PnL_Perc",
                "PnL_Value",
                "Positions",
                "Win_Rate",
                "Best_Trade",
                "Worst_Trade",
                "Avg_Trade",
            ]
        )

    trades = _numeric_realized_trades(_normalize_realized_strategy_columns(trades))
    rows = []
    for strategy, df_strategy in trades.groupby("Strategy", sort=True):
        summary = _realized_period_summary(df_strategy)
        pnl_values = pd.to_numeric(df_strategy["PnL_Value"], errors="coerce").fillna(0.0)
        pnl_perc = pd.to_numeric(df_strategy["PnL_Perc"], errors="coerce").fillna(0.0)
        positions = int(summary["Positions"])
        win_rate = float((pnl_values > 0).sum() / positions * 100) if positions else 0.0
        rows.append(
            {
                "Strategy": strategy,
                **summary,
                "Win_Rate": win_rate,
                "Best_Trade": float(pnl_perc.max()) if positions else 0.0,
                "Worst_Trade": float(pnl_perc.min()) if positions else 0.0,
                "Avg_Trade": float(pnl_perc.mean()) if positions else 0.0,
            }
        )

    return pd.DataFrame(rows).sort_values(
        ["PnL_Value", "PnL_Perc"], ascending=[False, False], kind="stable"
    )


def _build_realized_strategy_timeframe_matrix(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["Strategy", *REALIZED_PNL_BOTS, "TOTAL"])

    trades = _normalize_realized_strategy_columns(trades)
    rows = []
    for strategy, df_strategy in trades.groupby("Strategy", sort=True):
        row = {"Strategy": strategy}
        for bot in REALIZED_PNL_BOTS:
            df_bot = df_strategy[df_strategy["Bot"] == bot]
            row[bot] = None if df_bot.empty else _weighted_realized_pnl_perc(df_bot)
        row["TOTAL"] = _weighted_realized_pnl_perc(df_strategy)
        rows.append(row)

    return pd.DataFrame(rows).sort_values("TOTAL", ascending=False, kind="stable")


def _build_realized_exit_reason_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=["Exit_Type", "Exit_Reason", "PnL_Perc", "PnL_Value", "Positions"]
        )

    trades = trades.copy()
    if "Stop_Type" in trades.columns:
        trades["Exit_Type"] = (
            trades["Stop_Type"].fillna("").astype(str).str.strip().replace("", "unknown")
        )
    else:
        trades["Exit_Type"] = "unknown"
    if "Exit_Reason" in trades.columns:
        trades["Exit_Reason"] = (
            trades["Exit_Reason"]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "No reason")
        )
    else:
        trades["Exit_Reason"] = "No reason"
    rows = []
    for (exit_type, exit_reason), df_exit in trades.groupby(
        ["Exit_Type", "Exit_Reason"], sort=True
    ):
        rows.append(
            {
                "Exit_Type": exit_type,
                "Exit_Reason": exit_reason,
                **_realized_period_summary(df_exit),
            }
        )

    return pd.DataFrame(rows).sort_values(
        ["PnL_Value", "Positions"], ascending=[False, False], kind="stable"
    )


def _build_live_vs_backtest_summary(trades: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "Strategy",
        "Bot",
        "Symbol",
        "Live_PnL_Perc",
        "Live_PnL_Value",
        "Live_Trades",
        "Live_Win_Rate",
        "Backtest_Return_Perc",
        "Backtest_Win_Rate_Perc",
        "Backtest_Trades",
        "Quality_Grade",
        "Quality_Score",
        "Trading_Approved",
    ]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    trades = _numeric_realized_trades(_normalize_realized_strategy_columns(trades))
    rows = []
    for (strategy_id, strategy, bot, symbol), df_group in trades.groupby(
        ["Strategy_Id", "Strategy", "Bot", "Symbol"], sort=True
    ):
        summary = _realized_period_summary(df_group)
        pnl_values = pd.to_numeric(df_group["PnL_Value"], errors="coerce").fillna(0.0)
        positions = int(summary["Positions"])
        rows.append(
            {
                "Strategy_Id": strategy_id,
                "Strategy": strategy,
                "Bot": bot,
                "Symbol": symbol,
                "Live_PnL_Perc": float(summary["PnL_Perc"]),
                "Live_PnL_Value": float(summary["PnL_Value"]),
                "Live_Trades": positions,
                "Live_Win_Rate": float((pnl_values > 0).sum() / positions * 100)
                if positions
                else 0.0,
            }
        )
    live = pd.DataFrame(rows)
    if live.empty:
        return pd.DataFrame(columns=columns)

    try:
        backtests = database.get_all_backtesting_results()
    except Exception:
        backtests = pd.DataFrame()
    if backtests.empty:
        live["Backtest_Return_Perc"] = None
        live["Backtest_Win_Rate_Perc"] = None
        live["Backtest_Trades"] = None
        live["Quality_Grade"] = ""
        live["Quality_Score"] = None
        live["Trading_Approved"] = None
        return live[columns]

    backtest_columns = [
        "Strategy_Id",
        "Symbol",
        "Time_Frame",
        "Return_Perc",
        "Win_Rate_Perc",
        "Trades",
        "Quality_Grade",
        "Quality_Score",
        "Trading_Approved",
    ]
    existing_columns = [column for column in backtest_columns if column in backtests.columns]
    backtests = backtests[existing_columns].copy()
    backtests = backtests.rename(
        columns={
            "Time_Frame": "Bot",
            "Return_Perc": "Backtest_Return_Perc",
            "Win_Rate_Perc": "Backtest_Win_Rate_Perc",
            "Trades": "Backtest_Trades",
        }
    )
    merged = live.merge(
        backtests,
        how="left",
        on=["Strategy_Id", "Bot", "Symbol"],
    )
    merged = merged.sort_values(
        ["Live_PnL_Value", "Live_PnL_Perc"], ascending=[False, False], kind="stable"
    )
    return merged[columns]


def _build_realized_kpis(trades: pd.DataFrame, strategy_summary: pd.DataFrame) -> dict:
    summary = _realized_period_summary(trades)
    if trades.empty:
        win_rate = 0.0
    else:
        pnl_values = pd.to_numeric(trades["PnL_Value"], errors="coerce").fillna(0.0)
        win_rate = float((pnl_values > 0).sum() / len(trades) * 100)

    best_strategy = "NA"
    worst_strategy = "NA"
    if not strategy_summary.empty:
        sorted_summary = strategy_summary.sort_values(
            ["PnL_Value", "PnL_Perc"], ascending=[False, False], kind="stable"
        )
        best_strategy = str(sorted_summary.iloc[0]["Strategy"])
        worst_strategy = str(sorted_summary.iloc[-1]["Strategy"])

    return {
        "pnl_value": float(summary["PnL_Value"]),
        "pnl_perc": float(summary["PnL_Perc"]),
        "win_rate": win_rate,
        "positions": int(summary["Positions"]),
        "best_strategy": best_strategy,
        "worst_strategy": worst_strategy,
    }


def _format_realized_detail(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = _normalize_realized_strategy_columns(df)

    def _fmt8(value) -> str:
        try:
            if pd.isna(value):
                return ""
            val = float(value)
            return f"{val:.8f}"
        except (TypeError, ValueError):
            return ""

    def _fmt_compact(value) -> str:
        raw = _fmt8(value)
        if raw == "":
            return ""
        return raw.rstrip("0").rstrip(".")

    def _parse_atr_params(raw_params: str):
        if not raw_params:
            return None
        try:
            return json.loads(raw_params)
        except Exception:
            return None

    def _build_stop_details(row: pd.Series):
        stop_type = str(row.get("Stop_Type", "") or "").strip().lower()
        trigger = row.get("Stop_Trigger_Price", None)
        trail = row.get("Trail_Stop_ATR_At_Exit", None)
        high = row.get("Highest_Price_Since_Entry_At_Exit", None)
        atr_params = str(row.get("Atr_Params_At_Exit", "") or "").strip()
        atr = _parse_atr_params(atr_params)
        details = {"type": stop_type} if stop_type else {}

        if stop_type == "atr_trailing":
            details["trigger_price"] = _fmt_compact(trigger)
            details["trail_stop_at_exit"] = _fmt_compact(trail)
            details["high_since_entry"] = _fmt_compact(high)
            if atr:
                details["atr"] = atr
            elif atr_params:
                details["atr_raw"] = atr_params
            return details

        if stop_type == "hard_sl":
            details["trigger_price"] = _fmt_compact(trigger)
            if atr:
                details["atr"] = atr
            elif atr_params:
                details["atr_raw"] = atr_params
            return details

        if stop_type == "tp":
            details["label"] = "Take Profit"
            return details

        if stop_type == "forced_sale":
            details["label"] = "Forced Sale"
            return details

        if stop_type == "strategy":
            details["label"] = "Strategy Exit"
            return details

        if stop_type:
            return details
        return None

    for column in ("Buy_Price", "Sell_Price", "Sell_Position_Value", "Buy_Position_Value"):
        if column in df.columns:
            df[column] = df[column].apply(lambda x: f"{float(x):.8f}")
    if "PnL_Perc" in df.columns:
        df["PnL_Perc"] = df["PnL_Perc"].apply(lambda x: "{:.2f}".format(float(x)))
    if "PnL_Value" in df.columns:
        df["PnL_Value"] = df["PnL_Value"].apply(
            lambda x: f"{{:.{num_decimals}f}}".format(float(x))
        )

    if "Stop_Type" in df.columns:
        df["Stop_Details"] = df.apply(_build_stop_details, axis=1)
    else:
        df["Stop_Details"] = None

    preferred_columns = [
        "Id",
        "Bot",
        "Symbol",
        "Strategy",
        "PnL_Perc",
        "PnL_Value",
        "Buy_Date",
        "Buy_Price",
        "Buy_Qty",
        "Buy_Position_Value",
        "Sell_Date",
        "Sell_Price",
        "Sell_Qty",
        "Sell_Position_Value",
        "Exit_Reason",
        "Stop_Details",
        "Strategy_Id",
        "Strategy_Name",
        "Strategy_Params_JSON",
    ]
    ordered_columns = [column for column in preferred_columns if column in df.columns]
    ordered_columns += [column for column in df.columns if column not in ordered_columns]
    df = df[ordered_columns]

    drop_cols = [
        "Stop_Type",
        "Stop_Trigger_Price",
        "Trail_Stop_ATR_At_Exit",
        "Highest_Price_Since_Entry_At_Exit",
        "Atr_Params_At_Exit",
    ]
    existing_drop_cols = [c for c in drop_cols if c in df.columns]
    if existing_drop_cols:
        df = df.drop(columns=existing_drop_cols)
    return df


def calculate_realized_pnl(
    year: str, month: str, strategy_filter: str = ALL_STRATEGIES_FILTER
):
    """
    Aggregates realized PnL by bot (1d, 4h, 1h) and TOTAL.
    Uses WEIGHTED PnL% per bot and TOTAL:
        weight = Sell_Position_Value = (sell_qty * sell_price)

    Args:
        year:  'YYYY' or None (if None, returns empty totals)
        month: '01'..'12' or '13' for 'all months of the given year'

        strategy_filter: selected strategy id, all-strategy sentinel, or missing sentinel
    """
    trades = _get_realized_trades_for_period(str(year), str(month))
    trades = _filter_realized_trades_by_strategy(trades, strategy_filter)

    result_closed_positions = _format_realized_summary_numbers(
        _build_realized_bot_summary(trades)
    )
    strategy_summary = _build_realized_strategy_summary(trades)
    strategy_timeframe_matrix = _build_realized_strategy_timeframe_matrix(trades)
    exit_reason_summary = _build_realized_exit_reason_summary(trades)
    live_vs_backtest = _build_live_vs_backtest_summary(trades)
    kpis = _build_realized_kpis(trades, strategy_summary)

    trades_all = _format_realized_detail(trades)
    trades_month_1d = _format_realized_detail(trades[trades["Bot"] == "1d"])
    trades_month_4h = _format_realized_detail(trades[trades["Bot"] == "4h"])
    trades_month_1h = _format_realized_detail(trades[trades["Bot"] == "1h"])

    return (
        result_closed_positions,
        trades_month_1d,
        trades_month_4h,
        trades_month_1h,
        trades_all,
        strategy_summary,
        strategy_timeframe_matrix,
        exit_reason_summary,
        live_vs_backtest,
        kpis,
    )


def calculate_unrealized_pnl():

    # Load positions by bot
    df_positions_1d = database.get_unrealized_pnl_by_bot(bot="1d")
    df_positions_4h = database.get_unrealized_pnl_by_bot(bot="4h")
    df_positions_1h = database.get_unrealized_pnl_by_bot(bot="1h")
    _refresh_position_durations_for_display(
        df_positions_1d, df_positions_4h, df_positions_1h
    )

    # Build results per bot using WEIGHTED PnL% (weights = Position_Value)
    results_df = pd.DataFrame()
    for timeframe, df_positions in [
        ("1d", df_positions_1d),
        ("4h", df_positions_4h),
        ("1h", df_positions_1h),
    ]:
        # Ensure numeric types for weighting (avoid any stray strings)
        if not df_positions.empty:
            df_bot = df_positions.copy()
            df_bot["Position_Value"] = pd.to_numeric(
                df_bot["Position_Value"], errors="coerce"
            ).fillna(0.0)
            df_bot["PnL_Perc"] = pd.to_numeric(
                df_bot["PnL_Perc"], errors="coerce"
            ).fillna(0.0)
            # Weighted PnL% for this bot
            bot_pos_val_sum = df_bot["Position_Value"].sum()
            if bot_pos_val_sum != 0:
                bot_weighted_pnl_perc = (
                    df_bot["PnL_Perc"] * df_bot["Position_Value"]
                ).sum() / bot_pos_val_sum
            else:
                bot_weighted_pnl_perc = 0.0
            # Sum of PnL value and count of open positions for this bot
            pnl_value_sum = df_bot["PnL_Value"].sum()
            positions = len(df_bot)
        else:
            bot_weighted_pnl_perc = 0.0
            pnl_value_sum = 0.0
            positions = 0

        df_new = pd.DataFrame(
            {
                "Bot": [timeframe],
                "PnL_Perc": [bot_weighted_pnl_perc],  # weighted per-bot PnL%
                "PnL_Value": [pnl_value_sum],
                "Positions": [positions],
            }
        )
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # ---------- TOTAL: weighted PnL% across ALL positions ----------
    df_all = pd.concat(
        [df_positions_1d, df_positions_4h, df_positions_1h], ignore_index=True
    )
    if not df_all.empty:
        df_all["Position_Value"] = pd.to_numeric(
            df_all["Position_Value"], errors="coerce"
        ).fillna(0.0)
        df_all["PnL_Perc"] = pd.to_numeric(df_all["PnL_Perc"], errors="coerce").fillna(
            0.0
        )

        total_position_value = df_all["Position_Value"].sum()
        if total_position_value != 0:
            weighted_pnl_perc_total = (
                df_all["PnL_Perc"] * df_all["Position_Value"]
            ).sum() / total_position_value
        else:
            weighted_pnl_perc_total = 0.0

        sum_pnl_value_total = results_df["PnL_Value"].sum()
        open_positions = len(df_all)
    else:
        weighted_pnl_perc_total = 0.0
        sum_pnl_value_total = 0.0
        open_positions = 0

    # Show "open / max" positions
    max_num_positions = config.read_setting("max_number_of_open_positions")
    positions_info_total = f"{open_positions}/{max_num_positions}"

    # Append TOTAL row (weighted)
    results_df.loc[len(results_df)] = [
        "TOTAL",
        weighted_pnl_perc_total,
        sum_pnl_value_total,
        positions_info_total,
    ]

    # ---------- Display formatting ----------
    # Format PnL% and PnL_Value for results table
    # format the pnl_perc with 2 decimal places
    results_df["PnL_Perc"] = results_df["PnL_Perc"].apply(
        lambda x: "{:.2f}".format(float(x))
    )
    # format the pnl_value decimal places depending on trade against
    results_df["PnL_Value"] = results_df["PnL_Value"].apply(
        lambda x: f"{{:.{num_decimals}f}}".format(float(x))
    )

    # Format detail DataFrames
    for df_ref in (df_positions_1d, df_positions_4h, df_positions_1h):
        if not df_ref.empty:
            df_ref["Strategy"] = df_ref.apply(_format_position_strategy, axis=1)
            df_ref["Signal_Setup"] = df_ref.apply(_format_position_signal_setup, axis=1)
            df_ref["Buy_Price"] = df_ref["Buy_Price"].apply(
                lambda x: f"{{:.{8}f}}".format(x)
            )
            df_ref["Position_Value"] = df_ref["Position_Value"].apply(
                lambda x: f"{{:.{8}f}}".format(x)
            )
            df_ref["PnL_Perc"] = df_ref["PnL_Perc"].apply(
                lambda x: "{:.2f}".format(float(x))
            )
            df_ref["PnL_Value"] = df_ref["PnL_Value"].apply(
                lambda x: f"{{:.{num_decimals}f}}".format(float(x))
            )
            for target_index, column in enumerate(
                [
                    "Id",
                    "Bot",
                    "Symbol",
                    "PnL_Perc",
                    "PnL_Value",
                    "Take_Profits_JSON",
                    "RPQ%",
                    "Qty",
                    "Buy_Price",
                    "Position_Value",
                    "Date",
                    "Duration",
                    "Trail_Stop_ATR",
                    "Highest_Price_Since_Entry",
                    "Strategy",
                    "Signal_Setup",
                    "Strategy_Id",
                    "Strategy_Name",
                    "Strategy_Params_JSON",
                ]
            ):
                if column in df_ref.columns:
                    values = df_ref.pop(column)
                    df_ref.insert(target_index, column, values)

    return results_df, df_positions_1d, df_positions_4h, df_positions_1h


def _refresh_position_durations_for_display(*positions_dfs):
    datetime_now = datetime.now()
    for df in positions_dfs:
        if df.empty or "Date" not in df.columns or "Duration" not in df.columns:
            continue
        df["Duration"] = df.apply(
            lambda row: _format_position_duration_for_display(row, datetime_now),
            axis=1,
        )


def _format_position_duration_for_display(row, datetime_now):
    existing_duration = row.get("Duration", "")
    date_value = row.get("Date")

    if pd.isna(date_value):
        return existing_duration

    date_text = str(date_value).strip()
    if not date_text or date_text.lower() in ("none", "nan", "nat"):
        return existing_duration

    datetime_open_position = None
    for date_format in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            datetime_open_position = datetime.strptime(date_text, date_format)
            break
        except ValueError:
            continue

    if datetime_open_position is None:
        return existing_duration

    diff_seconds = int((datetime_now - datetime_open_position).total_seconds())
    if diff_seconds < 0:
        return existing_duration

    return str(database.calc_duration(diff_seconds))


def _format_position_strategy(row):
    strategy_name = str(row.get("Strategy_Name") or "").strip()
    strategy_id = str(row.get("Strategy_Id") or "").strip()
    return strategy_name or strategy_id


def _prepare_positions_display_grid(
    df: pd.DataFrame, show_trail_stop_atr: bool
) -> pd.DataFrame:
    """Return only user-facing columns for the open positions grid."""
    columns = list(POSITIONS_DISPLAY_COLUMNS)
    if show_trail_stop_atr:
        columns.append("Trail_Stop_ATR")
    return df[[column for column in columns if column in df.columns]].copy()


def _resolve_selected_position(event_positions, positions_df: pd.DataFrame):
    """Resolve a grid selection against the current positions snapshot.

    Streamlit can retain a selected row index for one rerun after the underlying
    position has been deleted. Treat that stale selection as no selection rather
    than indexing beyond the refreshed DataFrame.
    """
    selection = getattr(event_positions, "selection", None)
    selected_rows = getattr(selection, "rows", ())
    if positions_df is None or positions_df.empty or not selected_rows:
        return None

    selected_row_index = selected_rows[0]
    if (
        not isinstance(selected_row_index, int)
        or selected_row_index < 0
        or selected_row_index >= len(positions_df)
    ):
        return None

    return positions_df.iloc[selected_row_index]


def _format_position_signal_setup(row):
    strategy_id = str(row.get("Strategy_Id") or "").strip()
    snapshot = database.parse_strategy_params(row.get("Strategy_Params_JSON", ""))
    params = (
        snapshot.get("parameters")
        if isinstance(snapshot.get("parameters"), dict)
        else {
            key: value
            for key, value in snapshot.items()
            if key not in {"engine", "definition", "risk"}
        }
    )
    definition = (
        snapshot.get("definition")
        if isinstance(snapshot.get("definition"), dict)
        else {}
    )
    if not definition and strategy_id:
        try:
            definition = database.get_strategy_definition(strategy_id)
        except Exception:
            definition = {}

    definition_params = (
        definition.get("parameters", {}) if isinstance(definition, dict) else {}
    )
    if isinstance(definition_params, dict):
        names = [
            name
            for name, spec in definition_params.items()
            if isinstance(spec, dict) and bool(spec.get("optimizable", False))
        ] or list(definition_params.keys())
    else:
        names = list(params.keys())

    parts = []
    for name in names:
        if name in params:
            parts.append(f"{name}={params[name]}")
        if len(parts) >= 4:
            break
    return " | ".join(parts)


# define a function to set the background color of the rows based on pnl_value
def set_pnl_color(val):
    if val is not None:
        val = float(val)
        color = "#E9967A" if val < 0 else "#8FBC8F" if val > 0 else ""
        return f"background-color: {color}"


def format_func_strategies_main(option):
    return dict_strategies_main[option]


def _format_strategy_quality_value(value):
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "NA"
    return f"{float(numeric):.1f}"


def _build_btc_strategy_labels(strategy_names: dict, stablecoin: str) -> dict:
    labels = {
        str(strategy_id): f"{strategy_name} | Quality: NA | Grade: NA"
        for strategy_id, strategy_name in strategy_names.items()
    }
    if not labels:
        return labels

    try:
        results = database.get_all_backtesting_results()
    except Exception:
        return labels
    if results.empty or "Strategy_Id" not in results.columns:
        return labels

    btc_symbol = f"BTC{str(stablecoin or 'USDC').strip().upper()}"
    btc_results = results[
        (results["Symbol"].astype(str).str.upper() == btc_symbol)
        & (results["Strategy_Id"].astype(str).isin(labels.keys()))
    ].copy()
    if btc_results.empty:
        return labels

    btc_results["Quality_Score_Numeric"] = pd.to_numeric(
        btc_results.get("Quality_Score"),
        errors="coerce",
    )
    btc_results = btc_results.sort_values(
        ["Strategy_Id", "Quality_Score_Numeric"],
        ascending=[True, False],
        na_position="last",
    )
    for _, row in btc_results.drop_duplicates("Strategy_Id", keep="first").iterrows():
        strategy_id = str(row.get("Strategy_Id"))
        grade = str(row.get("Quality_Grade") or "").strip().upper() or "NA"
        labels[strategy_id] = (
            f"{strategy_names[strategy_id]} | "
            f"Quality: {_format_strategy_quality_value(row.get('Quality_Score'))} | "
            f"Grade: {grade}"
        )
    return labels


def format_func_strategies_btc(option):
    return dict_strategies_btc_labels.get(option, dict_strategies_btc[option])


def format_func_strategies(option):
    return dict_strategies[option]


def main():

    st.title(f"Trading Dashboard")
    # st.write(f"You are logged in as {st.session_state.role}.")

    # get strategies
    df_strategies_main = database.get_strategies_for_main()
    df_strategies_btc = database.get_strategies_for_btc()
    df_strategies = database.get_all_strategies()
    global dict_strategies_main, dict_strategies_btc, dict_strategies_btc_labels, dict_strategies
    # create a dictionary with code and name columns
    dict_strategies_main = dict(
        zip(df_strategies_main["Id"], df_strategies_main["Name"])
    )
    dict_strategies_btc = dict(zip(df_strategies_btc["Id"], df_strategies_btc["Name"]))
    dict_strategies_btc_labels = _build_btc_strategy_labels(
        dict_strategies_btc,
        config.read_setting("trade_against_switch_stablecoin"),
    )
    dict_strategies = dict(zip(df_strategies["Id"], df_strategies["Name"]))

    show_main_page()


if __name__ in ("__main__", "__page__"):
    main()
