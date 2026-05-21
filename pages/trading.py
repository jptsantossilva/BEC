import pandas as pd
import time
import os
import calendar
import json
from datetime import datetime

import streamlit as st
from millify import millify
import streamlit_authenticator as stauth
import altair as alt

import bec.utils.config as config
import bec.utils.database as database
import bec.exchanges.binance as binance
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

# for testing purposes
# st.session_state


def realized_pnl():
    with tab_rpnl:
        # get years
        years = get_years()

        # Render an empty-state view without crashing the filter widgets.
        if len(years) == 0:
            st.info("There are no closed positions 🤞")
            year = str(datetime.now().year)
            month_number = 13
        else:
            filter_rpnl = st.container(horizontal=True, vertical_alignment="bottom")
            # col1, col2, col3, col4 = st.columns([4, 6, 4, 10], vertical_alignment='bottom')
            # years selectbox
            year = filter_rpnl.selectbox("Year", (years), width=150)

            # get months
            months_dict = get_orders_by_month(year)
            month_names = list(months_dict.values())

            # months selectbox
            month_selected_name = filter_rpnl.selectbox(
                "Month", (month_names), width=200
            )

            disable_full_year = month_selected_name == None
            if month_selected_name == None:
                month_number = 1
            else:  # get month number from month name using months dictionary
                month_number = list(months_dict.keys())[
                    list(months_dict.values()).index(month_selected_name)
                ]

            if filter_rpnl.checkbox("Full Year", disabled=disable_full_year):
                month_number = 13

        result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = (
            calculate_realized_pnl(str(year), str(month_number))
        )
        # print("\nPnL - Total")
        # print(result_closed_positions)

        st.header("Realized PnL - Total")
        result_closed_positions = result_closed_positions.style.map(
            set_pnl_color, subset=["PnL_Perc", "PnL_Value"]
        )
        st.dataframe(result_closed_positions, width="content", hide_index=True)

        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header("Realized PnL - Detail")

        st.subheader("Bot 1d")
        st.dataframe(
            trades_month_1d.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config={
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large"),
                "Stop_Details": st.column_config.JsonColumn(
                    "Stop_Details",
                    width="large",
                    help="Structured stop metadata saved at trade exit.",
                ),
            },
        )

        st.subheader("Bot 4h")
        st.dataframe(
            trades_month_4h.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config={
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large"),
                "Stop_Details": st.column_config.JsonColumn(
                    "Stop_Details",
                    width="large",
                    help="Structured stop metadata saved at trade exit.",
                ),
            },
        )

        st.subheader("Bot 1h")
        st.dataframe(
            trades_month_1h.style.map(set_pnl_color, subset=["PnL_Perc", "PnL_Value"]),
            width="content",
            column_config={
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large"),
                "Stop_Details": st.column_config.JsonColumn(
                    "Stop_Details",
                    width="large",
                    help="Structured stop metadata saved at trade exit.",
                ),
            },
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
            if len(event_positions.selection.rows) == 0:
                return

            selected_row_index = event_positions.selection.rows[0]
            selected_position = positions_df.iloc[selected_row_index]
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
                options=tp_options,
                color=[tp_colors_by_option[option] for option in tp_options],
                format_func=_take_profit_format,
                # width="large",
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


def settings():
    with tab_settings:
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
            for schedule_name, lbl in [
                ("main_1d", "Enable 1d"),
                ("main_4h", "Enable 4h"),
                ("main_1h", "Enable 1h"),
            ]:
                state_key = f"job_{schedule_name}_enabled"
                if state_key not in st.session_state:
                    st.session_state[state_key] = database.get_job_schedule_enabled(
                        schedule_name
                    )

                def _make_toggle(name=schedule_name, key=state_key):
                    database.set_job_schedule_enabled(name, bool(st.session_state[key]))

                st.toggle(
                    lbl,
                    key=state_key,
                    on_change=_make_toggle,
                    help="""
                              **:green[Enabled]**: Buy new positions and sell existing ones based on the daily timeframe.  
                              **:red[Disabled]**: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
                          """,
                )

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
    years = database.get_years_from_orders()
    return years


# get months with orders within the year
def get_orders_by_month(year: str):

    months = database.get_months_from_orders_by_year(year)

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict


def calculate_realized_pnl(year: str, month: str):
    """
    Aggregates realized PnL by bot (1d, 4h, 1h) and TOTAL.
    Uses WEIGHTED PnL% per bot and TOTAL:
        weight = Sell_Position_Value = (sell_qty * sell_price)

    Args:
        year:  'YYYY' or None (if None, returns empty totals)
        month: '01'..'12' or '13' for 'all months of the given year'

    Returns:
        results_df: summary table with weighted PnL% per bot and TOTAL
        df_1d, df_4h, df_1h: detailed DataFrames for each bot
    """

    # Load SELL-side order details per bot (includes Sell_Position_Value)
    df_1d = database.get_orders_by_bot_side_year_month(
        bot="1d", side="SELL", year=year, month=str(month)
    )
    df_4h = database.get_orders_by_bot_side_year_month(
        bot="4h", side="SELL", year=year, month=str(month)
    )
    df_1h = database.get_orders_by_bot_side_year_month(
        bot="1h", side="SELL", year=year, month=str(month)
    )

    # Prepare a helper to compute weighted PnL% for a given dataframe
    def _weighted_pnl_perc(df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0
        # Ensure numeric types for weighting (avoid any stray strings from previous formatting)
        df_num = df.copy()
        df_num["Sell_Position_Value"] = pd.to_numeric(
            df_num.get("Sell_Position_Value", 0.0), errors="coerce"
        ).fillna(0.0)
        df_num["PnL_Perc"] = pd.to_numeric(
            df_num.get("PnL_Perc", 0.0), errors="coerce"
        ).fillna(0.0)
        denom = df_num["Sell_Position_Value"].sum()
        if denom == 0:
            return 0.0
        return float((df_num["PnL_Perc"] * df_num["Sell_Position_Value"]).sum() / denom)

    # Build results per bot with WEIGHTED PnL%
    results_df = pd.DataFrame()
    for label, df_bot in [("1d", df_1d), ("4h", df_4h), ("1h", df_1h)]:
        if df_bot.empty:
            bot_weighted = 0.0
            pnl_value_sum = 0.0
            trades = 0
        else:
            bot_weighted = _weighted_pnl_perc(df_bot)
            pnl_value_sum = float(
                pd.to_numeric(df_bot["PnL_Value"], errors="coerce").fillna(0.0).sum()
            )
            trades = len(df_bot)

        results_df = pd.concat(
            [
                results_df,
                pd.DataFrame(
                    {
                        "Bot": [label],
                        "PnL_Perc": [
                            bot_weighted
                        ],  # Weighted PnL% by Sell_Position_Value
                        "PnL_Value": [pnl_value_sum],  # Sum of realized PnL value
                        "Positions": [trades],  # Number of SELL trades in period
                    }
                ),
            ],
            ignore_index=True,
        )

    # TOTAL row: WEIGHTED across ALL bots (weights = Sell_Position_Value)
    all_frames = [df for df in (df_1d, df_4h, df_1h) if not df.empty]
    df_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if df_all.empty:
        weighted_total = 0.0
        sum_pnl_value_total = 0.0
        trades_total = 0
    else:
        weighted_total = _weighted_pnl_perc(df_all)
        sum_pnl_value_total = float(
            pd.to_numeric(df_all["PnL_Value"], errors="coerce").fillna(0.0).sum()
        )
        trades_total = len(df_all)

    # Append TOTAL row
    results_df.loc[len(results_df)] = [
        "TOTAL",
        weighted_total,
        sum_pnl_value_total,
        trades_total,
    ]

    # ---------- Display formatting ----------
    # Format summary numbers for display (keep numeric precision in detail tables below)
    results_df["PnL_Perc"] = results_df["PnL_Perc"].apply(
        lambda x: "{:.2f}".format(float(x))
    )
    results_df["PnL_Value"] = results_df["PnL_Value"].apply(
        lambda x: f"{{:.{num_decimals}f}}".format(float(x))
    )

    # Detail tables: keep high precision where relevant (8 decimals)
    def _fmt_detail(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()

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
            # Keep precision while reducing visual noise.
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

        # Keep 8-decimal formatting for price/value columns (for readability in UI tables)
        if "Buy_Price" in df.columns:
            df["Buy_Price"] = df["Buy_Price"].apply(lambda x: f"{float(x):.8f}")
        if "Sell_Price" in df.columns:
            df["Sell_Price"] = df["Sell_Price"].apply(lambda x: f"{float(x):.8f}")
        if "Sell_Position_Value" in df.columns:
            df["Sell_Position_Value"] = df["Sell_Position_Value"].apply(
                lambda x: f"{float(x):.8f}"
            )
        if "Buy_Position_Value" in df.columns:
            df["Buy_Position_Value"] = df["Buy_Position_Value"].apply(
                lambda x: f"{float(x):.8f}"
            )
        # Percent and value formatting
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

        if "Exit_Reason" in df.columns and "Stop_Details" in df.columns:
            cols = [c for c in df.columns if c != "Stop_Details"]
            insert_at = cols.index("Exit_Reason") + 1
            cols.insert(insert_at, "Stop_Details")
            df = df[cols]

        # Hide raw stop metadata columns; keep only aggregated Stop_Details in the grid.
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

    df_1d = _fmt_detail(df_1d)
    df_4h = _fmt_detail(df_4h)
    df_1h = _fmt_detail(df_1h)

    return results_df, df_1d, df_4h, df_1h


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
