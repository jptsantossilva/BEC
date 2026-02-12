import os
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

import utils.database as database
from my_backtesting import FOLDER_BACKTEST_RESULTS


st.markdown("## Backtesting Results")

df_strategies = database.get_all_strategies()
dict_strategies = dict(zip(df_strategies["Id"], df_strategies["Name"])) if not df_strategies.empty else {}


def format_func_strategies(option):
    return dict_strategies.get(option, option)


col_br1, col_br2, col_br3 = st.columns(3)

with col_br1:
    search_strategy = st.multiselect(
        "Strategy",
        options=list(dict_strategies.keys()),
        format_func=format_func_strategies,
    )

df_bt_results = database.get_all_backtesting_results()

with col_br3:
    pass

with col_br2:
    list_timeframe = ["1d", "4h", "1h"]
    search_timeframe = st.multiselect(label="Time-Frame", options=list_timeframe)

# search by symbol
list_symbols = df_bt_results["Symbol"].unique().tolist()

col_br_symbol1, col_br_symbol2 = st.columns([0.25, 0.5])
with col_br_symbol2:
    st.write('<div style="height: 35px;"></div>', unsafe_allow_html=True)

    default_symbols = None
    if st.checkbox("Use Top Performers"):
        df_top_perf = database.get_all_symbols_by_market_phase()
        top_perf_symbol_list = df_top_perf["Symbol"].to_list()
        default_symbols = top_perf_symbol_list

with col_br_symbol1:
    search_symbol = st.multiselect(label="Symbol", default=default_symbols, options=list_symbols)

col_br4, col_br5, col_br6 = st.columns(3)

today = datetime.now()
four_years_ago = today.replace(year=today.year - 4)

with col_br4:
    col_br41, col_br42 = st.columns(2)

    with col_br41:
        search_date_ini = st.date_input(
            label="Start date",
            value=four_years_ago,
            min_value=four_years_ago,
            max_value=today,
            format="DD.MM.YYYY",
        )

    with col_br42:
        search_date_end = st.date_input(
            label="End date",
            value=today,
            min_value=search_date_ini,
            max_value=today,
            format="DD.MM.YYYY",
        )

search_return_pct = st.checkbox("Return Percentage > 0", value=True)

df_bt_results["Backtest_Start_Date"] = pd.to_datetime(df_bt_results["Backtest_Start_Date"])
df_bt_results["Backtest_End_Date"] = pd.to_datetime(df_bt_results["Backtest_End_Date"])

if search_strategy:
    df_bt_results = df_bt_results[df_bt_results["Strategy_Id"].isin(search_strategy)]
if search_symbol:
    df_bt_results = df_bt_results[df_bt_results["Symbol"].isin(search_symbol)]
if search_timeframe:
    df_bt_results = df_bt_results[df_bt_results["Time_Frame"].isin(search_timeframe)]
if search_return_pct:
    df_bt_results = df_bt_results[df_bt_results["Return_Perc"] > 0]
if search_date_ini and search_date_end:
    start_date = datetime(search_date_ini.year, search_date_ini.month, search_date_ini.day)
    end_date = datetime(search_date_end.year, search_date_end.month, search_date_end.day)
    df_bt_results = df_bt_results[
        (df_bt_results["Backtest_Start_Date"] <= end_date)
        & (df_bt_results["Backtest_End_Date"] >= start_date)
    ]


def generate_backtest_link(row, file_type):
    strategy_id = str(row["Strategy_Id"])
    time_frame = row["Time_Frame"]
    symbol = row["Symbol"]
    filename = f"{strategy_id} - {time_frame} - {symbol}.{file_type}"

    file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename)
    if os.path.exists(file_path):
        file_path = os.path.join("app", FOLDER_BACKTEST_RESULTS, filename)
        return file_path
    return ""


df_bt_results["Backtest_HTML"] = df_bt_results.apply(
    lambda row: generate_backtest_link(row, "html"), axis=1
)
df_bt_results["Backtest_CSV"] = df_bt_results.apply(
    lambda row: generate_backtest_link(row, "csv"), axis=1
)

st.dataframe(
    df_bt_results,
    width="content",
    column_config={
        "Strategy_Id": None,
        "Backtest_HTML": st.column_config.LinkColumn(
            display_text="Open",
            help="Backtesting results in HTML",
        ),
        "Backtest_CSV": st.column_config.LinkColumn(
            display_text="Open",
            help="Backtesting results in CSV",
        ),
    },
)

st.subheader("Backtesting Trades")
get_trades = st.button("Get Trades", key="get_trades")
if get_trades:
    df_bt_trades = database.get_all_backtesting_trades()

    df_bt_trades["EntryTime"] = pd.to_datetime(df_bt_trades["EntryTime"])
    df_bt_trades["ExitTime"] = pd.to_datetime(df_bt_trades["ExitTime"])

    if search_strategy:
        df_bt_trades = df_bt_trades[df_bt_trades["Strategy_Id"].isin(search_strategy)]
    if search_symbol:
        df_bt_trades = df_bt_trades[df_bt_trades["Symbol"].isin(search_symbol)]
    if search_timeframe:
        df_bt_trades = df_bt_trades[df_bt_trades["Time_Frame"].isin(search_timeframe)]
    if search_date_ini and search_date_end:
        start_date = datetime(search_date_ini.year, search_date_ini.month, search_date_ini.day)
        end_date = datetime(search_date_end.year, search_date_end.month, search_date_end.day)
        df_bt_trades = df_bt_trades[
            (df_bt_trades["EntryTime"] <= end_date)
            & (df_bt_trades["ExitTime"] >= start_date)
        ]

    st.dataframe(df_bt_trades, width="content")

    trades_below_minus20 = df_bt_trades[df_bt_trades["ReturnPct"] < -20].shape[0]
    trades_above_100 = df_bt_trades[df_bt_trades["ReturnPct"] > 100].shape[0]

    trades_minus20_minus10 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= -20) & (df_bt_trades["ReturnPct"] < -10)
    ].shape[0]
    trades_minus10_0 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= -10) & (df_bt_trades["ReturnPct"] < 0)
    ].shape[0]
    trades_0_10 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 0) & (df_bt_trades["ReturnPct"] < 10)
    ].shape[0]
    trades_10_20 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 10) & (df_bt_trades["ReturnPct"] < 20)
    ].shape[0]
    trades_20_30 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 20) & (df_bt_trades["ReturnPct"] < 30)
    ].shape[0]
    trades_30_40 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 30) & (df_bt_trades["ReturnPct"] < 40)
    ].shape[0]
    trades_40_50 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 40) & (df_bt_trades["ReturnPct"] < 50)
    ].shape[0]
    trades_50_60 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 50) & (df_bt_trades["ReturnPct"] < 60)
    ].shape[0]
    trades_60_70 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 60) & (df_bt_trades["ReturnPct"] < 70)
    ].shape[0]
    trades_70_80 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 70) & (df_bt_trades["ReturnPct"] < 80)
    ].shape[0]
    trades_80_90 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 80) & (df_bt_trades["ReturnPct"] < 90)
    ].shape[0]
    trades_90_100 = df_bt_trades[
        (df_bt_trades["ReturnPct"] >= 90) & (df_bt_trades["ReturnPct"] < 100)
    ].shape[0]

    trades_total = (
        trades_below_minus20
        + trades_minus20_minus10
        + trades_minus10_0
        + trades_0_10
        + trades_10_20
        + trades_20_30
        + trades_30_40
        + trades_40_50
        + trades_50_60
        + trades_60_70
        + trades_70_80
        + trades_80_90
        + trades_90_100
        + trades_above_100
    )

    round_num = 2
    if trades_total != 0:
        trades_below_minus20_perc = round(trades_below_minus20 / trades_total, round_num) * 100
        trades_minus20_minus10_perc = round(trades_minus20_minus10 / trades_total, round_num) * 100
        trades_minus10_0_perc = round(trades_minus10_0 / trades_total, round_num) * 100
        trades_0_10_perc = round(trades_0_10 / trades_total, round_num) * 100
        trades_10_20_perc = round(trades_10_20 / trades_total, round_num) * 100
        trades_20_30_perc = round(trades_20_30 / trades_total, round_num) * 100
        trades_30_40_perc = round(trades_30_40 / trades_total, round_num) * 100
        trades_40_50_perc = round(trades_40_50 / trades_total, round_num) * 100
        trades_50_60_perc = round(trades_50_60 / trades_total, round_num) * 100
        trades_60_70_perc = round(trades_60_70 / trades_total, round_num) * 100
        trades_70_80_perc = round(trades_70_80 / trades_total, round_num) * 100
        trades_80_90_perc = round(trades_80_90 / trades_total, round_num) * 100
        trades_90_100_perc = round(trades_90_100 / trades_total, round_num) * 100
        trades_above_100_perc = round(trades_above_100 / trades_total, round_num) * 100
    else:
        trades_below_minus20_perc = 0
        trades_minus20_minus10_perc = 0
        trades_minus10_0_perc = 0
        trades_0_10_perc = 0
        trades_10_20_perc = 0
        trades_20_30_perc = 0
        trades_30_40_perc = 0
        trades_40_50_perc = 0
        trades_50_60_perc = 0
        trades_60_70_perc = 0
        trades_70_80_perc = 0
        trades_80_90_perc = 0
        trades_90_100_perc = 0
        trades_above_100_perc = 0

    trades_by_return_perc = {
        "Category": [
            "< -20%",
            "-20-10%",
            "-10-0%",
            "0-10%",
            "10-20%",
            "20-30%",
            "30-40%",
            "40-50%",
            "50-60%",
            "60-70%",
            "70-80%",
            "80-90%",
            "90-100%",
            "> 100%",
        ],
        "Number of Trades": [
            trades_below_minus20,
            trades_minus20_minus10,
            trades_minus10_0,
            trades_0_10,
            trades_10_20,
            trades_20_30,
            trades_30_40,
            trades_40_50,
            trades_50_60,
            trades_60_70,
            trades_70_80,
            trades_80_90,
            trades_90_100,
            trades_above_100,
        ],
    }
    df_tbrp = pd.DataFrame(trades_by_return_perc)
    df_tbrp["Perc of Trades"] = [
        trades_below_minus20_perc,
        trades_minus20_minus10_perc,
        trades_minus10_0_perc,
        trades_0_10_perc,
        trades_10_20_perc,
        trades_20_30_perc,
        trades_30_40_perc,
        trades_40_50_perc,
        trades_50_60_perc,
        trades_60_70_perc,
        trades_70_80_perc,
        trades_80_90_perc,
        trades_90_100_perc,
        trades_above_100_perc,
    ]

    category_order = [
        "< -20%",
        "-20-10%",
        "-10-0%",
        "0-10%",
        "10-20%",
        "20-30%",
        "30-40%",
        "40-50%",
        "50-60%",
        "60-70%",
        "70-80%",
        "80-90%",
        "90-100%",
        "> 100%",
    ]

    chart_tbrp = alt.Chart(df_tbrp).mark_bar().encode(
        x=alt.X("Category", title="Return %", scale=alt.Scale(domain=category_order)),
        y=alt.Y("Perc of Trades", title="Percentage of Trades"),
        color=alt.Color("Number of Trades"),
    ).properties(
        title="Distribution of Trades",
    )

    st.altair_chart(chart_tbrp)

    st.dataframe(
        df_tbrp,
        width="content",
        hide_index=True,
        height=(len(df_tbrp) + 1) * 35 + 3,
    )
