import pandas as pd
import time
import numpy as np
import os
import yaml
import sys
import calendar
import re
import requests

import streamlit as st
from millify import millify
import streamlit_authenticator as stauth
import altair as alt

import utils.config as config
import utils.database as database
import utils.exchange as exchange
import utils.general as general

from symbol_by_market_phase import main as run_symbol_by_market_phase

import update 

st.set_page_config(
    page_title="Bot Dashboard App",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': 'https://github.com/jptsantossilva/BEC#readme',
        'Report a bug': "https://github.com/jptsantossilva/BEC/issues/new",
        'About': """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
        \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
        """
    }
)

# for testing purposes
# st.session_state

# Initialization
if 'name' not in st.session_state:
    st.session_state.name = ''
if 'username' not in st.session_state:
    st.session_state.username = ''
if 'user_password' not in st.session_state:
    st.session_state.user_password = 'None'
if 'reset_form_open' not in st.session_state:
    st.session_state.reset_form_open = False
if 'reset_password_submitted' not in st.session_state:
    st.session_state.reset_password_submitted = False
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = False
if 'trade_against_switch' not in st.session_state:
    st.session_state.trade_against_switch = config.get_setting("trade_against_switch")
# if 'main_strategy' not in st.session_state:
#     st.session_state.main_strategy = config.get_setting("main_strategy")
# if 'btc_strategy' not in st.session_state:
#     st.session_state.btc_strategy = config.get_setting("btc_strategy")


# im using to find which bots are running
def find_file_paths(filename: str):
    
    # get the current working directory
    cwd = os.getcwd()

    parent_dir_path = os.path.join(cwd, '..')

    # find all folders inside the parent folder
    folder_paths = [os.path.join(parent_dir_path, f) for f in os.listdir(parent_dir_path)
                    if os.path.isdir(os.path.join(parent_dir_path, f))]

    # search for the file in each folder and return the file paths where it exists
    file_paths = []
    for folder_path in folder_paths:
        file_path = os.path.join(folder_path, filename)
        if os.path.exists(file_path):
            file_paths.append(folder_path)

    return file_paths

# def get_bot_names(paths):
#     bot_names = []
#     for path in paths:
#         bot_names.append(os.path.basename(os.path.normpath(path)))
    
#     return bot_names

def get_trade_against():

    # get settings from btc_strategy file
    try:
        file_path = 'config.yaml'
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)

        trade_against = config["trade_against"]

        return trade_against
        
    except FileNotFoundError as e:
        msg = "Error: The file config.yaml could not be found."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # sys.exit(msg) 

    except yaml.YAMLError as e:
        msg = "Error: There was an issue with the YAML file."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # sys.exit(msg)

def get_chart_daily_balance(asset):
    if asset not in ["USD", "BTC"]:
        return

    expander_total_balance = st.expander(label=f"Daily Balance Snapshot - {asset}", expanded=False)
    with expander_total_balance:
        period_selected_balances = st.radio(
            label='Choose Period',
            options=('Last 7 days','Last 30 days', 'Last 90 days', 'YTD', 'All Time'),
            index=1,
            horizontal=True,
            label_visibility='collapsed',
            key=f'period_selected_balances_{asset}')

        if period_selected_balances == 'Last 7 days':
            n_days = 7
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'Last 30 days':
            n_days = 30
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'Last 90 days':
            n_days = 90
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'YTD':
            source = database.get_total_balance_ytd(connection)
        elif period_selected_balances == 'All Time':
            source = database.get_total_balance_all_time(connection)

        if source.empty:
            st.warning('No data on Balances yet! Click Refresh.')
            current_total_balance = 0
        else:
            if asset == "USD":
                current_total_balance = source.Total_Balance_USD.iloc[-1]
            elif asset == "BTC":
                current_total_balance = source.Total_Balance_BTC.iloc[-1]

        col1, col2 = st.columns([10, 1])
        with col1:
            st.caption(f'Last Daily Balance: {current_total_balance}')
        # with col2:
        #     refresh_balance = st.button("Refresh", key=f"refresh_balance_{asset}")

        # if refresh_balance:
        #     with st.spinner("Creating balance snapshot. It can take a few minutes..."):
        #         exchange.create_balance_snapshot(telegram_prefix="")
        #         # dasboard refresh
        #         st.rerun()

        # exit if there is no data to display on chart
        if source.empty:
            return
        
        hover = alt.selection_single(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty="none",
        )
        if asset == "USD":
            lines = (
                alt.Chart(source, 
                        #   title="Total Balance USD Last 30 Days"
                        )
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(f"Total_Balance_{asset}", title=f"Balance_{asset}",scale=alt.Scale(domain=[source.Total_Balance_USD.min(),source.Total_Balance_USD.max()])),
                    # color="Total_Balance_USD",
                )
            )
        elif asset == "BTC":
            lines = (
                alt.Chart(source, 
                        #   title="Total Balance USD Last 30 Days"
                        )
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(f"Total_Balance_{asset}", title=f"Balance_{asset}",scale=alt.Scale(domain=[source.Total_Balance_BTC.min(),source.Total_Balance_BTC.max()])),
                    # color="Total_Balance_USD",
                )
            )

        # Draw points on the line, and highlight based on selection
        points = lines.transform_filter(hover).mark_circle(size=70)

        # Draw a rule at the location of the selection
        tooltips = (
            alt.Chart(source)
            .mark_rule()
            .encode(
                x="Date",
                y=f"Total_Balance_{asset}",
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip("Date", title="Date"),
                    alt.Tooltip(f"Total_Balance_{asset}", title=f"Balance_{asset}"),
                ],
            )
            .add_selection(hover)
        )
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart, use_container_width=True)


def get_chart_daily_asset_balances():
    expander_asset_balances = st.expander(label="Daily Asset Balances", expanded=False)
    with expander_asset_balances:
        period_selected_asset = st.radio(
            label='Choose Period',
            options=('Last 7 days','Last 30 days', 'Last 90 days', 'YTD', 'All Time'),
            index=1,
            horizontal=True,
            label_visibility='collapsed',
            key='period_selected_asset')

        if period_selected_asset == 'Last 7 days':
            n_days = 7
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'Last 30 days':
            n_days = 30
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'Last 90 days':
            n_days = 90
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'YTD':
            source = database.get_asset_balances_ytd(connection)
        elif period_selected_asset == 'All Time':
            source = database.get_asset_balances_all_time(connection)

        if source.empty:
            st.warning('No data on Balances yet!')
            # exit - there is no data to display on chart
            return

        hover = alt.selection_single(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty="none",
        )

        lines = (
            alt.Chart(source, 
                    #   title="Asset Balances Last 30 Days"
                      )
            .mark_line()
            .encode(
                x="Date",
                y=alt.Y("Balance_USD", scale=alt.Scale(domain=[source.Balance_USD.min(),source.Balance_USD.max()])),
                color="Asset",
            )
        )

        # Draw points on the line, and highlight based on selection
        points = lines.transform_filter(hover).mark_circle(size=70)

        # Draw a rule at the location of the selection
        tooltips = (
            alt.Chart(source)
            .mark_rule()
            .encode(
                x="Date",
                y="Balance_USD",
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip("Date", title="Date"),
                    alt.Tooltip("Asset", title="Asset"),
                    alt.Tooltip("Balance_USD", title="Balance_USD"),
                ],
            )
            .add_selection(hover)
        )
        # chart = (lines + points + tooltips).properties(height=800).interactive()
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart, use_container_width=True)

def realized_pnl():
    with tab_rpnl:
        # get years
        years = get_years(bot_selected)

        # years empty list
        if len(years) == 0:
            st.warning('There are no closed positions yet! 🤞')

        col1, col2, col3 = st.columns(3)
        # years selectbox
        year = col1.selectbox(
            'Year',
            (years)
        )
        # get months
        # months_dict = get_orders_by_month(year, bot_selected)
        months_dict = get_orders_by_month(year)
        month_names = list(months_dict.values())

        # months selectbox
        month_selected_name = col2.selectbox(
            'Month',
            (month_names)
        )

        disable_full_year = month_selected_name == None
        if month_selected_name == None:
            month_number = 1
        else: # get month number from month name using months dictionary 
            month_number = list(months_dict.keys())[list(months_dict.values()).index(month_selected_name)]


        if col2.checkbox('Full Year', disabled=disable_full_year):
            month_number = 13

        result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = calculate_realized_pnl(str(year), str(month_number))
        # print("\nPnL - Total")
        # print(result_closed_positions)

        st.header("Realized PnL - Total")
        result_closed_positions = result_closed_positions.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_closed_positions)    
    
        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header(f"Realized PnL - Detail")
        st.subheader("Bot 1d")
        st.dataframe(trades_month_1d.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 4h")
        st.dataframe(trades_month_4h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 1h")
        st.dataframe(trades_month_1h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

        # print('\n----------------------------\n')

def unrealized_pnl():
    with tab_upnl:
        result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = calculate_unrealized_pnl()
        # print("\nUnrealized PnL - Total")
        # print('-------------------------------')
        # print(result_open_positions)

        if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
            st.warning('There are no open positions yet! 🤞') 

        st.header("Unrealized PnL - Total")

        # st.sidebar.subheader('Unrealized PnL %')
        # col1, col2, col3 = st.sidebar.columns(3)
        # currPnL_1d_value = result_open_positions.loc[result_open_positions['Bot'] == '1d', 'PnL_Value'].iloc[0]
        # currPnL_4h_value = result_open_positions.loc[result_open_positions['Bot'] == '4h', 'PnL_Value'].iloc[0]
        # currPnL_1h_value = result_open_positions.loc[result_open_positions['Bot'] == '1h', 'PnL_Value'].iloc[0]
        # currPnL_total_value = float(currPnL_1d_value) + float(currPnL_4h_value) + float(currPnL_1h_value)

        # # Convert long numbers into a human-readable format in Python
        # # 1200 to 1.2k; 12345678 to 12.35M 
        # currPnL_1d_value = millify(currPnL_1d_value, precision=num_decimals)
        # currPnL_4h_value = millify(currPnL_4h_value, precision=num_decimals)
        # currPnL_1h_value = millify(currPnL_1h_value, precision=num_decimals)
        # currPnL_total_value = millify(currPnL_total_value, precision=num_decimals)

        # currPnL_1d_perc = result_open_positions.loc[result_open_positions['Bot'] == '1d', 'PnL_Perc'].iloc[0]
        # currPnL_4h_perc = result_open_positions.loc[result_open_positions['Bot'] == '4h', 'PnL_Perc'].iloc[0]
        # currPnL_1h_perc = result_open_positions.loc[result_open_positions['Bot'] == '1h', 'PnL_Perc'].iloc[0]
        # currPnL_total_perc = float(currPnL_1d_perc) + float(currPnL_4h_perc) + float(currPnL_1h_perc)

        # currPnL_1d_perc = millify(currPnL_1d_perc, precision=2)
        # currPnL_4h_perc = millify(currPnL_4h_perc, precision=2)
        # currPnL_1h_perc = millify(currPnL_1h_perc, precision=2)
        # currPnL_total_perc = millify(currPnL_total_perc, precision=2)

        # col1, col2, col3, col4 = st.columns(4)
        # col1.metric("1d", currPnL_1d_value, str(currPnL_1d_perc)+"%")
        # col2.metric("4h", currPnL_4h_value, str(currPnL_4h_perc)+"%")
        # col3.metric("1h", currPnL_1h_value, str(currPnL_1h_perc)+"%")
        # col4.metric("Total", currPnL_total_value, str(currPnL_total_perc)+"%")

        # st.write("")

        result_open_positions = result_open_positions.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_open_positions)
        
        # print("Unrealized PnL - Detail")
        # print(positions_df_1d)
        # print(positions_df_4h)
        # print(positions_df_1h)

        st.header(f"Unrealized PnL - Detail")
        st.subheader("Bot 1d")
        st.dataframe(positions_df_1d.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
                     column_config = {
                         "PnL_Perc": st.column_config.NumberColumn(
                                                                    # "PnL %",
                                                                    # help="The price of the product in USD",
                                                                    # min_value=0,
                                                                    # max_value=1000,
                                                                    # step=1,
                                                                    format="%.2f",
                                                                ),
                         "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                         "Take_Profit_1": st.column_config.CheckboxColumn(),
                         "Take_Profit_2": st.column_config.CheckboxColumn()
                    }
                )
        st.subheader("Bot 4h")
        st.dataframe(positions_df_4h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
                     column_config = {
                         "PnL_Perc": st.column_config.NumberColumn(
                                                                    # "PnL %",
                                                                    # help="The price of the product in USD",
                                                                    # min_value=0,
                                                                    # max_value=1000,
                                                                    # step=1,
                                                                    format="%.2f",
                                                                ),
                         "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"), 
                         "Take_Profit_1": st.column_config.CheckboxColumn(),
                         "Take_Profit_2": st.column_config.CheckboxColumn()
                    }
                )
        st.subheader("Bot 1h")
        st.dataframe(positions_df_1h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
                     column_config = {
                         "PnL_Perc": st.column_config.NumberColumn(
                                                                    # "PnL %",
                                                                    # help="The price of the product in USD",
                                                                    # min_value=0,
                                                                    # max_value=1000,
                                                                    # step=1,
                                                                    format="%.2f",
                                                                ),
                         "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"), 
                         "Take_Profit_1": st.column_config.CheckboxColumn(),
                         "Take_Profit_2": st.column_config.CheckboxColumn()
                    }
                )        

        #----------------------
        # Force Close Position
        st.header("Force Selling")
        # add expander
        sell_expander = st.expander("Choose position to sell")
        bots = ["1d", "4h", "1h"]
        sell_bot = sell_expander.selectbox(
            label='Bot?',
            options=(bots),
            label_visibility='collapsed')
        # symbols list
        if sell_bot == "1d":
            list_positions = positions_df_1d.Symbol.to_list()
        elif sell_bot == "4h":
            list_positions = positions_df_4h.Symbol.to_list()
        elif sell_bot == "1h":
            list_positions = positions_df_1h.Symbol.to_list()

        sell_symbol = sell_expander.selectbox(
            label='Position?',
            options=(list_positions),
            label_visibility='collapsed')

        disable_sell_confirmation1 = sell_symbol == None
        # get balance
        if not disable_sell_confirmation1:
            # sell_symbol = "INJUSDT" # test
            sell_amount_perc = sell_expander.slider('Amount', 10, 100, 25, 5, format="%d%%", disabled=disable_sell_confirmation1)
            
            # get current position balance
            df_pos = database.get_positions_by_bot_symbol_position(database.conn, bot=sell_bot, symbol=sell_symbol, position=1)
            if not df_pos.empty:
                balance_qty = df_pos['Qty'].iloc[0]
            else:
                balance_qty = 0
            # symbol_only, symbol_stable = general.separate_symbol_and_trade_against(sell_symbol)
            # balance_qty = exchange.get_symbol_balance(symbol=symbol_only, bot=sell_bot) 
            
            sell_amount = balance_qty*(sell_amount_perc/100)
            sell_expander.text_input('Sell Amount / Position Balance', f'{sell_amount} / {balance_qty}', disabled=True)
 
            # sell_expander.write(disable_sell_confirmation1)
            sell_reason = sell_expander.text_input("Reason")
            sell_confirmation1 = sell_expander.checkbox(f"I confirm the Sell of **{sell_amount_perc}%** of **{sell_symbol}** in **{sell_bot}** bot", disabled=disable_sell_confirmation1)
        
            # if button pressed then sell position
            if sell_confirmation1:
                sell_confirmation2 = sell_expander.button("SELL")
                if sell_confirmation2:
                    exchange.create_sell_order(symbol=sell_symbol,
                                            bot=sell_bot,
                                            reason=sell_reason,
                                            percentage=sell_amount_perc
                                            ) 

                    sell_expander.success(f"{sell_symbol} SOLD!")
                    time.sleep(5)
                    # dasboard refresh
                    st.rerun()
            #----------------------

def top_performers():
    with tab_top_perf:
        top_perf = config.get_setting("trade_top_performance")
        st.subheader(f"Top {top_perf} Performers")
        st.caption("The top performers are those in accumulation phase (Price > 50DSMA and Price > 200DSMA and 50DSMA < 200DSMA) and bullish phase (Price > 50DSMA and Price > 200DSMA and 50DSMA > 200DSMA) and then sorted by the price above the 200-day moving average (DSMA) in percentage terms. [Click here for more details](https://twitter.com/jptsantossilva/status/1539976855469428738?s=20).")
        df_mp = database.get_all_symbols_by_market_phase(connection)
        df_mp['Price'] = df_mp['Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA50'] = df_mp['DSMA50'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA200'] = df_mp['DSMA200'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['Perc_Above_DSMA50'] = df_mp['Perc_Above_DSMA50'].apply(lambda x:'{:.2f}'.format(x))
        df_mp['Perc_Above_DSMA200'] = df_mp['Perc_Above_DSMA200'].apply(lambda x:'{:.2f}'.format(x))
        st.dataframe(df_mp)

        filename = "Top_performers_"+trade_against+".txt"
        if os.path.exists(filename):
            with open(filename, "rb") as file:
                st.download_button(
                    label="Download as TradingView List",
                    data=file,
                    file_name=filename,
                    mime='text/csv',
                ) 

        st.subheader(f"Historical Top Performers")
        st.caption("Symbols that spend the most number of days in the bullish or accumulating phases")
        df_symbols_days_at_top = database.symbols_by_market_phase_Historical_get_symbols_days_at_top(connection)
        st.dataframe(df_symbols_days_at_top)

def signals():
    with tab_signals:
        st.subheader(f"Signals Log")
        st.caption("These signals are just informative. They do not automatically trigger buy and sell orders. You can use these to help you make decisions about when to force a manual exit from an unrealized position.")
        expander_signals = st.expander(label="Signals", expanded=False)
        with expander_signals:
            st.write("""**SUPER-RSI** - Triggered when all time-frames are below or above a defined level.
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m <= 25
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m >= 80""")
            # st.divider()  # Draws a horizontal line
        df_s = database.get_all_signals_log(connection, num_rows=100)
        st.dataframe(df_s)

def blacklist():
    with tab_blacklist:
        st.subheader("Blacklist")
        df_blacklist = database.get_symbol_blacklist(connection)
        edited_blacklist = st.data_editor(df_blacklist, num_rows="dynamic")
        blacklist_apply_changes = st.button("Save")

        if blacklist_apply_changes:
            edited_blacklist.to_sql(name='Blacklist',con=connection, index=True, if_exists="replace")
            st.success("Blacklist changes saved")

def backtesting_results():
    with tab_backtesting_results:
        st.subheader("Backtesting Results")

        # search by strategy
        search_strategy = st.multiselect(
                    'Strategy',
                    options=list(dict_strategies.keys()),
                    format_func=format_func_strategies
                    )
        # st.write('You selected:', search_strategy)

        df_bt_results = database.get_all_backtesting_results(connection)

        # search by symbol
        # get distinct symbols
        list_symbols = df_bt_results['Symbol'].unique().tolist()
        search_symbol = st.multiselect(
                    'Symbol',
                    options=list_symbols
                    )
        # st.write('You selected:', search_symbol)
        
        # if (not search_strategy) and (not search_symbol):
        #     st.dataframe(df_bt_results)
        if search_strategy:
            df_bt_results = df_bt_results[df_bt_results['Strategy_Id'].isin(search_strategy)]
        if search_symbol:
            df_bt_results = df_bt_results[df_bt_results['Symbol'].isin(search_symbol)]

        # remove strategy_id column
        df_bt_results = df_bt_results.drop(columns=['Strategy_Id'])

        st.dataframe(df_bt_results)

def manage_config():

    # try:
    #     # Read the YAML file
    #     with open('config.yaml', 'r') as f:
    #         config_file = yaml.safe_load(f)
    # except FileNotFoundError:
    #     st.warning('Config file not found!')
    #     st.stop()

    # Create Streamlit widgets for each configuration option
    col1_cfg, col2_cfg = tab_settings.columns(2)
    if tab_settings:
        
        with col2_cfg:
            st.write("**Main Strategy**")

            try:
                # ms = st.session_state.main_strategy
                prev_main_strategy = config.get_setting('main_strategy')
                main_strategy = st.selectbox('Main Strategy', 
                                            key="main_strategy",
                                            options=list(dict_strategies_main.keys()),
                                            index=list(dict_strategies_main).index(prev_main_strategy),
                                            format_func=format_func_strategies_main, 
                                            label_visibility="collapsed"
                                            )
                if main_strategy != prev_main_strategy: 
                    config.set_setting("main_strategy", main_strategy)
                # st.write(f"You selected option {main_strategy} called {format_func_strategies(main_strategy)}")       
            except KeyError:
                st.warning('Invalid or missing configuration: main_strategy')
                st.stop()

            
            st.write("**BTC Strategy**")

            try:
                # bs = config['btc_strategy']
                # bs = st.session_state.btc_strategy
                prev_btc_strategy = config.get_setting('btc_strategy')
                btc_strategy = st.selectbox('BTC Strategy', 
                                            key="btc_strategy",
                                            options=list(dict_strategies_btc.keys()),
                                            index=list(dict_strategies_btc).index(prev_btc_strategy),
                                            format_func=format_func_strategies_btc,
                                            label_visibility="collapsed",
                                            # disabled=not st.session_state.trade_against_switch,
                                            help=""""""
                                            )
                if btc_strategy != prev_btc_strategy: 
                    config.set_setting("btc_strategy", btc_strategy)
            except KeyError:
                st.warning('Invalid or missing configuration: btc_strategy')
                st.stop()

            prev_trade_against_switch = config.get_setting("trade_against_switch")
            trade_against_switch = st.checkbox("Automatically switch between trade against USDT or BTC",
                                                key="trade_against_switch",
                                                help="""Considering the chosen Bitcoin strategy will decide whether it is a Bull or Bear market. If Bull then will convert USDT to BTC and trade against BTC. If Bear will convert BTC into USDT and trade against USDT.""")
            if trade_against_switch != prev_trade_against_switch: 
                config.set_setting("trade_against_switch", trade_against_switch)
            

            # col1_stra, col2_stra = st.columns(2)
            # with col1_stra:
            run_backtesting = st.button("Run Backtesting", help="Please be patient, as it could take around 1 hour to complete.")
            if run_backtesting:
                with st.spinner('This task is taking a leisurely stroll through the digital landscape (+/- 1h). Why not do the same? Stretch those legs, grab a snack, or contemplate the meaning of life.'):
                    trade_against = get_trade_against() 
                    run_symbol_by_market_phase(time_frame="1d", trade_against=trade_against)

        with col1_cfg:
            st.write("**Settings**")

            try:
                # prev_bot_1d = config_file['bot_1d']
                prev_bot_1d = config.get_setting("bot_1d")
                bot_1d = st.checkbox(label='Bot 1D', value=prev_bot_1d,
                                     help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_1d and not bot_1d:
                    check_open_positions("1d")
                if bot_1d != prev_bot_1d: 
                    config.set_setting("bot_1d", bot_1d)
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 1D')
                st.stop()
            
            try:
                # prev_bot_4h = config_file['bot_4h']
                prev_bot_4h = config.get_setting("bot_4h")
                bot_4h = st.checkbox(label='Bot 4H', value=prev_bot_4h,
                                     help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_4h and not bot_4h:
                    check_open_positions("4h")
                if bot_4h != prev_bot_4h:
                    config.set_setting("bot_4h", bot_4h)
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 4h')
                st.stop()
            
            try:
                # prev_bot_1h = config_file['bot_1h']
                prev_bot_1h = config.get_setting("bot_1h")
                bot_1h = st.checkbox(label='Bot 1h', value=prev_bot_1h,
                                    help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_1h and not bot_1h:
                    check_open_positions("1h")
                if bot_1h != prev_bot_1h:
                    config.set_setting("bot_1h", bot_1h)
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 1h')
                st.stop()
            
            try:
                prev_stake_amount_type = config.get_setting("stake_amount_type")
                stake_amount_type = st.selectbox('Stake Amount Type', ['unlimited'], 
                                                help="""Stake_amount is the amount of stake the bot will use for each trade. 
                                                    \nIf stake_amount = "unlimited" the increasing/decreasing of stakes will depend on the performance of the bot. Lower stakes if the bot is losing, higher stakes if the bot has a winning record since higher balances are available and will result in profit compounding.
                                                    \nIf stake amount = static number, that is the amount per trade
                                                """)
                if stake_amount_type != prev_stake_amount_type:
                    config.set_setting("stake_amount_type", stake_amount_type)
            except KeyError:
                st.warning('Invalid or missing configuration: stake_amount_type')
                st.stop()
            
            try:
                prev_max_number_of_open_positions = config.get_setting("max_number_of_open_positions")
                max_number_of_open_positions = st.number_input(label="Max Number of Open Positions", 
                                                            min_value=1,
                                                            value=int(prev_max_number_of_open_positions),
                                                            max_value=50,
                                                            step=1,
                                                            help="""
                                                            If tradable balance = 1000 and max_number_of_open_positions = 10, the stake_amount = 1000/10 = 100
                                                            """)
                if max_number_of_open_positions != prev_max_number_of_open_positions:
                    config.set_setting("max_number_of_open_positions", max_number_of_open_positions)
            except KeyError:
                st.warning('Invalid or missing configuration: max_number_of_open_positions')
                st.stop()
            
            try:
                prev_tradable_balance_ratio = config.get_setting("tradable_balance_ratio")
                tradable_balance_ratio = st.slider(label='Tradable Balance Ratio', 
                                                min_value=0.0, 
                                                max_value=1.0, 
                                                value=float(prev_tradable_balance_ratio), 
                                                step=0.01,
                                                help="""Tradable percentage of the balance
                                                """)
                if tradable_balance_ratio != prev_tradable_balance_ratio:
                    config.set_setting("tradable_balance_ratio", tradable_balance_ratio)
            except KeyError:
                st.warning('Invalid or missing configuration: tradable_balance_ratio')
                st.stop()
            
            try:
                prev_trade_against = config.get_setting("trade_against")
                trade_against = st.selectbox('Trade Against', ['USDT', 'BTC'], index=['USDT', 'BTC'].index(prev_trade_against),
                                            help="""Trade against USDT or BTC
                                            """)
                if trade_against != prev_trade_against:
                    config.set_setting("trade_against", trade_against)
            except KeyError:
                st.warning('Invalid or missing configuration: trade_against')
                st.stop()

            try:
                prev_min_position_size = config.get_setting("min_position_size")
                if trade_against in ["USDT"]:
                    trade_min_val = 0
                    trade_step = 1
                    trade_format = None
                    if int(prev_min_position_size) < 20:
                        trade_min_pos_size = 20
                    else:
                        trade_min_pos_size = int(prev_min_position_size)
                elif trade_against == "BTC":
                    trade_min_val = 0.0
                    trade_step = 0.0001
                    trade_format = "%.4f"
                    if float(prev_min_position_size) > 0.0001:
                        trade_min_pos_size = 0.0001
                    else:
                        trade_min_pos_size = float(prev_min_position_size)

                min_position_size = st.number_input(label='Minimum Position Size', 
                                                    min_value=trade_min_val, 
                                                    value=trade_min_pos_size, 
                                                    step=trade_step,
                                                    format=trade_format,
                                                    help="""If trade_against = USDT => min_position_size = 20
                                                        \nIf trade_against = BTC => min_position_size = 0.001
                                                    """)
                if min_position_size != prev_min_position_size:
                    config.set_setting("min_position_size", min_position_size)
            except KeyError:
                st.warning('Invalid or missing configuration: min_position_size')
                st.stop()

            try:
                prev_trade_top_performance = config.get_setting("trade_top_performance")
                trade_top_performance = st.slider('Trade Top Performance Symbols', 1, 50, prev_trade_top_performance,
                                                help="""
                                                    Trade top X performance symbols                                              
                                                """)
                if trade_top_performance != prev_trade_top_performance:
                    config.set_setting("trade_top_performance", trade_top_performance)
            except KeyError:
                st.warning('Invalid or missing configuration: trade_top_performance')
                st.stop()

            try:
                prev_stop_loss = config.get_setting("stop_loss")
                stop_loss = st.number_input(label='Stop Loss %', 
                                            min_value=0, 
                                            value=int(prev_stop_loss), 
                                            step=1,
                                            # key="stop_loss",
                                            help="""Set stop loss to automatically sell if its price falls below a certain percentage.
                                                \nExamples:
                                                \n stop_loss = 0 => will not use stop loss. The stop loss used will be triggered when slow_ema > fast_ema
                                                \n stop_loss = 10 => 10%   
                                            """)
                if stop_loss != prev_stop_loss:
                    config.set_setting("stop_loss", stop_loss)
            except KeyError:
                st.warning('Invalid or missing configuration: stop_loss')
                st.stop()

            col1_tp1, col2_tp1 = col1_cfg.columns(2)

            with col1_tp1:

                try:
                    prev_take_profit_1 = config.get_setting("take_profit_1")
                    take_profit_1 = st.number_input(label="Take-Profit Level 1 %", 
                                                    min_value=0, 
                                                    value=int(prev_take_profit_1), 
                                                    step=1,
                                                    key="take_profit_1",
                                                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                                                    )
                    if take_profit_1 != prev_take_profit_1:
                        config.set_setting("take_profit_1", take_profit_1)
                except KeyError:
                    st.warning('Invalid or missing configuration: take_profit_1')
                    st.stop()

            with col2_tp1:
                try:
                    prev_take_profit_1_amount = config.get_setting("take_profit_1_amount")
                    take_profit_1_amount = st.number_input(
                                                            label="Amount %", 
                                                            min_value=5, 
                                                            max_value=100,
                                                            value=int(prev_take_profit_1_amount), 
                                                            step=5,
                                                            key="take_profit_1_amount",
                                                            help="The percentage to be sold when the take profits level 1 is achieved."
                                                            )
                    if take_profit_1_amount != prev_take_profit_1_amount:
                        config.set_setting("take_profit_1_amount", take_profit_1_amount)
                except KeyError:
                    st.warning('Invalid or missing configuration: take_profit_1_amount')
                    st.stop()
            
            col1_tp2, col2_tp2 = col1_cfg.columns(2)

            with col1_tp2:    
                try:
                    prev_take_profit_2 = config.get_setting("take_profit_2")
                    take_profit_2 = st.number_input(label="Take-Profit Level 2 %", 
                                                    min_value=0, 
                                                    value=int(prev_take_profit_2), 
                                                    step=1,
                                                    key="take_profit_2",
                                                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                                                    )
                    if take_profit_2 != prev_take_profit_2:
                        config.set_setting("take_profit_2", take_profit_2)
                except KeyError:
                    st.warning('Invalid or missing configuration: take_profit_2')
                    st.stop()

            with col2_tp2:
                try:
                    prev_take_profit_2_amount = config.get_setting("take_profit_2_amount")
                    take_profit_2_amount = st.number_input(
                                                            label="Amount %", 
                                                            min_value=5, 
                                                            max_value=100,
                                                            value=int(prev_take_profit_2_amount), 
                                                            step=5,
                                                            key="take_profit_2_amount",
                                                            help="The percentage to be sold when the take profits level 2 is achieved."
                                                            )
                    if take_profit_2_amount != prev_take_profit_2_amount:
                        config.set_setting("take_profit_2_amount", take_profit_2_amount)
                except KeyError:
                    st.warning('Invalid or missing configuration: take_profit_2_amount')
                    st.stop()

    # Update the configuration dictionary with the modified values
    # config_file['stake_amount_type'] = stake_amount_type
    # config_file['max_number_of_open_positions'] = max_number_of_open_positions
    # config_file['tradable_balance_ratio'] = tradable_balance_ratio
    # config_file['min_position_size'] = min_position_size
    # config_file['trade_top_performance'] = trade_top_performance
    # config_file['trade_against'] = trade_against
    # config_file['stop_loss'] = stop_loss
    # config_file['bot_1d'] = bot_1d
    # config_file['bot_4h'] = bot_4h
    # config_file['bot_1h'] = bot_1h
    # config_file['main_strategy'] = main_strategy
    # config_file['btc_strategy'] = btc_strategy
    # config_file['trade_against_switch'] = trade_against_switch    
    # config_file['take_profit_1'] = take_profit_1 
    # config_file['take_profit_1_amount'] = take_profit_1_amount   
    # config_file['take_profit_2'] = take_profit_2    
    # config_file['take_profit_2_amount'] = take_profit_2_amount

    # # Write the modified configuration dictionary back to the YAML file
    # try:
    #     with open('config.yaml', 'w') as f:
    #         yaml.dump(config_file, f)
    # except PermissionError:
    #     st.warning('Permission denied: could not write to config file!')
    #     st.stop()
    
def check_app_version():
    last_date = general.extract_date_from_local_changelog()
    if last_date:
        app_version = last_date
    else:
        app_version = "App version not found"
    st.caption(f'**{bot_selected}** - {trade_against} - App Version {app_version}')

    github_last_date = general.extract_date_from_github_changelog()
    if github_last_date != last_date:
        st.warning("Update Available! A new version of the BEC is available. Click UPDATE to get the latest features and improvements. Check the [Change Log](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md) for more details.")
        update_version = st.button('UPDATE', key="update_version")
        if update_version:
            with st.spinner('🎉 Hold on tight! 🎉 Our elves are sprinkling magic dust on the app to make it even better.'):
                result = update.main() 
                st.code(result)

                restart_time = 5
                progress_text = f"App will restart in {restart_time} seconds."
                my_bar = st.progress(0, text=progress_text)

                for step in range(6):
                    progress_percent = step * 20
                    if progress_percent != 0:
                        restart_time -= 1
                        progress_text = f"App will restart in {restart_time} seconds."
                    my_bar.progress(progress_percent, text=progress_text)
                    time.sleep(1)  

                st.rerun()

def show_main_page():
    
    global trade_against
    trade_against = get_trade_against()

    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2
    # num_decimals = 2  

    # Get the current directory
    current_dir = os.getcwd()
    # Get the parent directory
    parent_dir = os.path.basename(current_dir)

    global bot_selected
    bot_selected = parent_dir  

    check_app_version()      

    get_chart_daily_balance(asset="USD")
    get_chart_daily_balance(asset="BTC")
    get_chart_daily_asset_balances()

    global tab_upnl, tab_rpnl, tab_top_perf, tab_signals, tab_blacklist, tab_backtesting_results, tab_settings
    tab_upnl, tab_rpnl, tab_signals, tab_top_perf, tab_blacklist, tab_backtesting_results, tab_settings = st.tabs(["Unrealized PnL", "Realized PnL", "Signals", "Top Performers", "Blacklist", "Backtesting Results", "Settings"])

    realized_pnl()
    unrealized_pnl()
    signals()
    top_performers()
    blacklist()
    backtesting_results()
    manage_config()

def check_open_positions(bot: str):
    num = database.get_num_open_positions_by_bot(connection=connection, bot=bot)
    if num > 0:
        msg = f"There are {num} open position on Bot_{bot}. If you turn the bot OFF this positions will remain open. Make sure you close them."
        st.warning(msg) 

def show_form_reset_password():
    if st.session_state.authentication_status and st.session_state.reset_form_open:
        try:
            with st.form(key="reset_password"):
                st.subheader("Reset password")
                # password = st.text_input('Current password', type='password')
                new_password = st.text_input('New password', type='password', key="new_password")
                new_password_repeat = st.text_input('Repeat password', type='password', key="new_password_repeat")

                # st.form_submit_button(label='Reset Password', on_click=reset_password_submitted(True))
                submitted = st.form_submit_button(label='Reset Password')
                if submitted:
                    reset_password_submitted(True)

            if 'reset_password_submitted' in  st.session_state:
                if st.session_state.reset_password_submitted == True:
                    if len(new_password) > 0:
                        if new_password == new_password_repeat:
                            if 1 == 1: #password != new_password: 
                                new_password_hashed = stauth.Hasher([new_password]).generate()[0]
                                database.update_user_password(connection, username=st.session_state.username, password=new_password_hashed)
                                st.success('Password updated!')
                                time.sleep(3)
                                reset_password_submitted(False)
                                reset_form_open(False)
                                st.rerun()
                            else:
                                st.error('New and current passwords are the same')
                        else:
                            st.error('Passwords do not match')
                    else:
                        st.error('No new password provided')

        except Exception as e:
            st.error(e)

def create_user():
    try:
        if authenticator.register_user(form_name='Register user', preauthorization=True):
            st.success('User registered successfully')

        # authenticator.credentials
    except Exception as e:
        st.error(e)

def forgot_password():
    try:
        username_forgot_pw, email_forgot_password, random_password = authenticator.forgot_password('Forgot password')
        if username_forgot_pw:
            st.success('New password sent securely')
            # Random password to be transferred to user securely
        elif username_forgot_pw == False:
            st.error('Username not found')
    except Exception as e:
        st.error(e)

# Get years from orders
def get_years(bot):
    years = database.get_years_from_orders(connection)
    return years

# get months with orders within the year
def get_orders_by_month(year: str):

    months = database.get_months_from_orders_by_year(connection, year)

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict
    
# Define a function to get the year and month from a datetime object
def get_year_month(date):
    return date.year, date.month

def calculate_realized_pnl(year: str, month: str):

    # print(f'Year = {year}')
    # if month == '13':
    #     print(f'Month = ALL')
    # else:
    #     print(f'Month = {month}')

    # print('\n Realized PnL')
    # print('---------------------')
    
    df_month_1d = database.get_orders_by_bot_side_year_month(connection, bot="1d", side="SELL", year=year, month=month)
    df_month_4h = database.get_orders_by_bot_side_year_month(connection, bot="4h", side="SELL", year=year, month=month)
    df_month_1h = database.get_orders_by_bot_side_year_month(connection, bot="1h", side="SELL", year=year, month=month)
    
    # set decimal precision 
    df_month_1d['Buy_Price'] = df_month_1d['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1d['Sell_Price'] = df_month_1d['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    
    df_month_4h['Buy_Price'] = df_month_4h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_4h['Sell_Price'] = df_month_4h['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    
    df_month_1h['Buy_Price'] = df_month_1h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1h['Sell_Price'] = df_month_1h['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    # print('')              
    # print(df_month_1d)
    # print(df_month_4h)
    # print(df_month_1h)
    
    results_df = pd.DataFrame()
    # results_df = pd.DataFrame(columns=['bot','Year','Month','pnl_%','pnl_value','trades'])
    for timeframe, df_month in [('1d', df_month_1d), ('4h', df_month_4h), ('1h', df_month_1h)]:
        if df_month.empty:
            continue
        pnl_perc_sum = df_month.PnL_Perc.sum()
        pnl_value_sum = round(df_month.PnL_Value.sum(), num_decimals)
        trades = len(df_month) 
        df_new = pd.DataFrame({
                'Bot': [timeframe],
                'Year': [year],
                'Month': [month],
                'PnL_Perc': [pnl_perc_sum],
                'PnL_Value': [pnl_value_sum],
                'Trades': [trades]})
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    if not results_df.empty:
        sum_pnl_perc = round(results_df['PnL_Perc'].sum(), 2)
        sum_pnl_value = round(results_df['PnL_Value'].sum(), num_decimals)
        sum_trades = results_df['Trades'].sum()
    else:
        sum_pnl_perc = 0
        sum_pnl_value = 0
        sum_trades = 0

    # Add a new row at the end of the dataframe with the sum values
    if not results_df.empty:
        results_df.loc[len(results_df)] = ['TOTAL','', '', sum_pnl_perc, sum_pnl_value, sum_trades]
    else:
        df_data = [['TOTAL','', '', sum_pnl_perc, sum_pnl_value, sum_trades]]
        results_df = pd.DataFrame(df_data, columns=['Bot', 'Year', 'Month','PnL_Perc','PnL_Value','Trades'])

    # format the pnl_perc and pnl_value decimal places
    # format the pnl_value decimal places depending on trade against
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))

    df_month_1d['PnL_Perc'] = df_month_1d['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_1d['PnL_Value'] = df_month_1d['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_month_4h['PnL_Perc'] = df_month_4h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_4h['PnL_Value'] = df_month_4h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_month_1h['PnL_Perc'] = df_month_1h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_1h['PnL_Value'] = df_month_1h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))

    return results_df, df_month_1d, df_month_4h, df_month_1h

def calculate_unrealized_pnl():
    
    # print('\nUnrealized PnL')
    # print('---------------------')

    # results_df = pd.DataFrame(columns=['bot','pnl_%','pnl_value','positions'])

    df_positions_1d = database.get_unrealized_pnl_by_bot(connection, bot="1d")
    df_positions_4h = database.get_unrealized_pnl_by_bot(connection, bot="4h")
    df_positions_1h = database.get_unrealized_pnl_by_bot(connection, bot="1h")

    # set decimal precision 
    df_positions_1d['Buy_Price'] = df_positions_1d['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_4h['Buy_Price'] = df_positions_4h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_1h['Buy_Price'] = df_positions_1h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    
    # print(df_positions_1d)
    # print(df_positions_4h)
    # print(df_positions_1h)

    # dataframe with totals
    results_df = pd.DataFrame()
    for timeframe, df_positions in [('1d', df_positions_1d), ('4h', df_positions_4h), ('1h', df_positions_1h)]:
        pnl_perc_sum = df_positions.PnL_Perc.sum()
        pnl_value_sum = df_positions.PnL_Value.sum()
        positions = len(df_positions) 
        df_new = pd.DataFrame({
                'Bot': [timeframe],
                'PnL_Perc': [pnl_perc_sum],
                'PnL_Value': [pnl_value_sum],
                'Positions': [positions]})
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sums of the PnLs and positions
    sum_pnl_perc = results_df['PnL_Perc'].sum()
    sum_pnl_value = results_df['PnL_Value'].sum()
    sum_positions = results_df['Positions'].sum()
    
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL', sum_pnl_perc, sum_pnl_value, sum_positions]

    # format the pnl_perc with 2 decimal places
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    # format the pnl_value decimal places depending on trade against
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    # format the pnl_perc and pnl_value decimal places
    df_positions_1d['PnL_Perc'] = df_positions_1d['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_1d['PnL_Value'] = df_positions_1d['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    df_positions_4h['PnL_Perc'] = df_positions_4h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_4h['PnL_Value'] = df_positions_4h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    df_positions_1h['PnL_Perc'] = df_positions_1h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_1h['PnL_Value'] = df_positions_1h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    
    return results_df, df_positions_1d, df_positions_4h, df_positions_1h

# define a function to set the background color of the rows based on pnl_value
def set_pnl_color(val):
    if val is not None:
        val = float(val)
        color = '#E9967A' if val < 0 else '#8FBC8F' if val > 0 else ''
        return f'background-color: {color}'

def reset_form_open(state):
    if 'reset_form_open' in  st.session_state:
        st.session_state.reset_form_open = state

def reset_password_submitted(state):
    if 'reset_password_submitted' in  st.session_state:
        st.session_state.reset_password_submitted = state

def format_func_strategies_main(option):
        return dict_strategies_main[option]

def format_func_strategies_btc(option):
        return dict_strategies_btc[option]

def format_func_strategies(option):
        return dict_strategies[option]

def main():

    # Initialization
    # if 'name' not in  st.session_state:
    #     st.session_state.name = ''
    # if 'username' not in  st.session_state:
    #     st.session_state.username = ''
    # if 'user_password' not in  st.session_state:
    #     st.session_state.user_password = 'None'
    # if 'reset_form_open' not in st.session_state:
    #     st.session_state.reset_form_open = False
    # if 'reset_password_submitted' not in  st.session_state:
    #     st.session_state.reset_password_submitted = False
    # if 'authentication_status' not in  st.session_state:
    #     st.session_state.authentication_status = None

    # connect to database
    global connection
    connection = database.connect()

    df_users = database.get_all_users(connection)
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict('index')
    formatted_credentials = {'usernames': {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials['usernames'][username] = user_info

    
    # get strategies
    df_strategies_main = database.get_strategies_for_main(connection)
    df_strategies_btc = database.get_strategies_for_btc(connection)
    df_strategies = database.get_all_strategies(connection)
    # Convert the DataFrame to a dictionary with 'Id' as keys and 'Name' as values
    # dict_strategies = df_strategies.set_index('Id')['Name'].to_dict()
    global dict_strategies_main, dict_strategies_btc, dict_strategies
    dict_strategies_main = df_strategies_main['Name'].to_dict()
    dict_strategies_btc = df_strategies_btc['Name'].to_dict()
    dict_strategies = df_strategies['Name'].to_dict()

    global authenticator

    st.title(f'BEC Dashboard')

    authenticator = stauth.Authenticate(
        credentials=formatted_credentials,
        cookie_name="dashboard_cookie_name",
        key="dashboard_cookie_key",
        cookie_expiry_days=30
    )

    name, authentication_status, username = authenticator.login('Login', 'main')
    st.session_state.name = name
    st.session_state.username = username
    # st.session_state.user_password = authenticator.credentials['usernames'][username]['password']

    if authentication_status:
        authenticator.logout('Logout', 'sidebar')
        
        # reset_clicked = st.sidebar.button("Reset", on_click=reset_form_open(True))
        reset_clicked = st.sidebar.button("Reset", key="reset_clicked")
        if reset_clicked:
            reset_form_open(True)
        show_form_reset_password()

        # create_user_clicked = st.sidebar.button("Create User")
        # if create_user_clicked:
        #     create_user()

        st.sidebar.title(f'Welcome *{st.session_state.name}*')
        show_main_page()
    elif authentication_status == False:
        st.error('Username or password is incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password')

if __name__ == "__main__":
    main()










