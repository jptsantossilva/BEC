import pandas as pd
import time
import numpy as np
import os
import yaml
import sys
import calendar

import streamlit as st
from millify import millify
import streamlit_authenticator as stauth
import altair as alt

import utils.database as database
import utils.config as config
import utils.exchange as exchange

st.set_page_config(
    page_title="Bot Dashboard App",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': 'https://github.com/jptsantossilva/Binance-Trading-bot-EMA-Cross#readme',
        'Report a bug': "https://github.com/jptsantossilva/Binance-Trading-bot-EMA-Cross/issues/new",
        'About': "# I am a Trading Bot \nI do not have a name yet but I'm trying to be an *extremely* cool app!"
    }
)

# for testing purposes
# st.session_state

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

    # get settings from config file
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

def get_chart_daily_balance():
    expander_total_balance = st.expander(label=f"Daily Balance Snapshot", expanded=True)
    with expander_total_balance:
        period_selected_balances = st.radio(
            label='Choose Period',
            options=('Last 7 days','Last 30 days', 'Last 90 days', 'YTD', 'All Time'),
            index=1,
            horizontal=True,
            label_visibility='collapsed',
            key='period_selected_balances')

        if period_selected_balances == 'Last 7 days':
            n_days = 7
            source = database.get_total_balance_usd_last_n_days(connection, n_days)
        elif period_selected_balances == 'Last 30 days':
            n_days = 30
            source = database.get_total_balance_usd_last_n_days(connection, n_days)
        elif period_selected_balances == 'Last 90 days':
            n_days = 90
            source = database.get_total_balance_usd_last_n_days(connection, n_days)
        elif period_selected_balances == 'YTD':
            source = database.get_total_balance_usd_ytd(connection)
        elif period_selected_balances == 'All Time':
            source = database.get_total_balance_usd_all_time(connection)

        if source.empty:
            st.warning('No data on Balances yet! Click Refresh.')
            current_total_balance = 0
        else:
            current_total_balance = source.Total_Balance_USD.iloc[-1]
        col1, col2 = st.columns([10, 1])
        with col1:
            st.caption(f'Last Daily Balance: {current_total_balance}')
        with col2:
            refresh_balance = st.button("Refresh")

        if refresh_balance:
            with st.spinner('Creating balance snapshot...'):
                exchange.create_balance_snapshot(telegram_prefix="")
                # dasboard refresh
                st.experimental_rerun()

        # exit if there is no data to display on chart
        if source.empty:
            return
        
        hover = alt.selection_single(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty="none",
        )
        lines = (
            alt.Chart(source, 
                    #   title="Total Balance USD Last 30 Days"
                      )
            .mark_line()
            .encode(
                x="Date",
                y=alt.Y("Total_Balance_USD", title="Balance_BUSD",scale=alt.Scale(domain=[source.Total_Balance_USD.min(),source.Total_Balance_USD.max()])),
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
                y="Total_Balance_USD",
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip("Date", title="Date"),
                    alt.Tooltip("Total_Balance_USD", title="Balance_USD"),
                ],
            )
            .add_selection(hover)
        )
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart, use_container_width=True)


def get_chart_daily_asset_balances():
    expander_asset_balances = st.expander(label="Daily Asset Balances", expanded=True)
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
        # apply the lambda function to make the last row bold
        # result_closed_positions = result_closed_positions.apply(lambda x: ['font-weight: bold' if i == len(x)-1 else '' for i in range(len(x))], axis=1)

        # print(result_closed_positions)

        st.header("Realized PnL - Total")
        # tab_rpnl.dataframe(result_closed_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['Pnl_Perc','Pnl_Value']))
        st.dataframe(result_closed_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header(f"Realized PnL - Detail")
        st.subheader("Bot 1d")
        st.dataframe(trades_month_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 4h")
        st.dataframe(trades_month_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 1h")
        st.dataframe(trades_month_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

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
        currPnL_1d_value = result_open_positions.loc[result_open_positions['Bot'] == '1d', 'PnL_Value'].iloc[0]
        currPnL_4h_value = result_open_positions.loc[result_open_positions['Bot'] == '4h', 'PnL_Value'].iloc[0]
        currPnL_1h_value = result_open_positions.loc[result_open_positions['Bot'] == '1h', 'PnL_Value'].iloc[0]
        currPnL_total_value = float(currPnL_1d_value) + float(currPnL_4h_value) + float(currPnL_1h_value)

        # Convert long numbers into a human-readable format in Python
        # 1200 to 1.2k; 12345678 to 12.35M 
        currPnL_1d_value = millify(currPnL_1d_value, precision=num_decimals)
        currPnL_4h_value = millify(currPnL_4h_value, precision=num_decimals)
        currPnL_1h_value = millify(currPnL_1h_value, precision=num_decimals)
        currPnL_total_value = millify(currPnL_total_value, precision=num_decimals)

        currPnL_1d_perc = result_open_positions.loc[result_open_positions['Bot'] == '1d', 'PnL_Perc'].iloc[0]
        currPnL_4h_perc = result_open_positions.loc[result_open_positions['Bot'] == '4h', 'PnL_Perc'].iloc[0]
        currPnL_1h_perc = result_open_positions.loc[result_open_positions['Bot'] == '1h', 'PnL_Perc'].iloc[0]
        currPnL_total_perc = float(currPnL_1d_perc) + float(currPnL_4h_perc) + float(currPnL_1h_perc)

        currPnL_1d_perc = millify(currPnL_1d_perc, precision=2)
        currPnL_4h_perc = millify(currPnL_4h_perc, precision=2)
        currPnL_1h_perc = millify(currPnL_1h_perc, precision=2)
        currPnL_total_perc = millify(currPnL_total_perc, precision=2)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("1d", currPnL_1d_value, str(currPnL_1d_perc)+"%")
        col2.metric("4h", currPnL_4h_value, str(currPnL_4h_perc)+"%")
        col3.metric("1h", currPnL_1h_value, str(currPnL_1h_perc)+"%")
        col4.metric("Total", currPnL_total_value, str(currPnL_total_perc)+"%")

        st.write("")

        st.dataframe(data=result_open_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

        # print("Unrealized PnL - Detail")
        # print(positions_df_1d)
        # print(positions_df_4h)
        # print(positions_df_1h)

        st.header(f"Unrealized PnL - Detail")
        st.subheader("Bot 1d")
        st.dataframe(positions_df_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 4h")
        st.dataframe(positions_df_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
        st.subheader("Bot 1h")
        st.dataframe(positions_df_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

        #----------------------
        # Force Close Position
        st.header("Forced Selling")
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
        # sell_expander.write(sell_symbol)
        disable_sell_confirmation1 = sell_symbol == None 
        # sell_expander.write(disable_sell_confirmation1)
        sell_reason = sell_expander.text_input("Reason")
        sell_confirmation1 = sell_expander.checkbox(f"I confirm the Sell of **{sell_symbol}** in **{sell_bot}** bot", disabled=disable_sell_confirmation1)
        # if button pressed then sell position
        if sell_confirmation1:
            sell_confirmation2 = sell_expander.button("SELL")
            if sell_confirmation2:
                exchange.create_sell_order(symbol=sell_symbol,
                                        bot=sell_bot,
                                        reason=sell_reason) 

                sell_expander.success(f"{sell_symbol} SOLD!")
                time.sleep(3)
                # dasboard refresh
                st.experimental_rerun()
        #----------------------

def top_performers():
    with tab_top_perf:
        st.subheader(f"Top {config.trade_top_performance} Performers")
        st.caption("The ranking order is determined by considering the price above the 200-day moving average (DSMA) in percentage terms.")
        df_mp = database.get_all_symbols_by_market_phase(connection)
        df_mp['Price'] = df_mp['Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA50'] = df_mp['DSMA50'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA200'] = df_mp['DSMA200'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['Perc_Above_DSMA50'] = df_mp['Perc_Above_DSMA50'].apply(lambda x:'{:.2f}'.format(x))
        df_mp['Perc_Above_DSMA200'] = df_mp['Perc_Above_DSMA200'].apply(lambda x:'{:.2f}'.format(x))
        st.dataframe(df_mp)

        filename = "Top_performers_"+config.trade_against+".txt"
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
            st.write("""**SUPER-RSI** - Triggered when all time-frames are below or above the defined level.
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m <= 25
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m >= 80""")
            # st.divider()  # 👈 Draws a horizontal rule
        df_s = database.get_all_signals_log(connection, num_rows=100)
        st.dataframe(df_s)

def blacklist():
    with tab_blacklist:
        st.subheader("Blacklist")
        df_blacklist = database.get_symbol_blacklist(connection)
        edited_blacklist = st.experimental_data_editor(df_blacklist, num_rows="dynamic")
        blacklist_apply_changes = st.button("Save")

        if blacklist_apply_changes:
            edited_blacklist.to_sql(name='Blacklist',con=connection, index=True, if_exists="replace")
            st.success("Blacklist changes saved")

def best_ema():
    with tab_best_ema:
        st.subheader("Best EMA")
        df_bema = database.get_all_best_ema(connection)
        st.dataframe(df_bema)

def manage_config():

    try:
        # Read the YAML file
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        st.warning('Config file not found!')
        st.stop()

    # Create Streamlit widgets for each configuration option
    col1_cfg, col2_cfg, col3_cfg = tab_settings.columns(3)
    if tab_settings: 
        with col1_cfg:
            try:
                prev_bot_1d = config['bot_1d']
                bot_1d = st.checkbox(label='Bot 1D', value=prev_bot_1d,
                                     help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_1d and not bot_1d:
                    check_open_positions("1d")
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 1D')
                st.stop()
            try:
                prev_bot_4h = config['bot_4h']
                bot_4h = st.checkbox(label='Bot 4H', value=prev_bot_4h,
                                     help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_4h and not bot_4h:
                    check_open_positions("4h")
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 4h')
                st.stop()
            try:
                prev_bot_1h = config['bot_1h']
                bot_1h = st.checkbox(label='Bot 1h', value=config['bot_1h'],
                                    help="""Turn the Bot ON or OFF""")
                # Check if the value of bot has changed
                if prev_bot_1h and not bot_1h:
                    check_open_positions("1h")
            except KeyError:
                st.warning('Invalid or missing configuration: Bot 1h')
                st.stop()
            try:
                stake_amount_type = st.selectbox('Stake Amount Type', ['unlimited'], 
                                                help="""Stake_amount is the amount of stake the bot will use for each trade. 
                                                    \nIf stake_amount = "unlimited" the increasing/decreasing of stakes will depend on the performance of the bot. Lower stakes if the bot is losing, higher stakes if the bot has a winning record since higher balances are available and will result in profit compounding.
                                                    \nIf stake amount = static number, that is the amount per trade
                                                """)
            except KeyError:
                st.warning('Invalid or missing configuration: stake_amount_type')
                st.stop()
            try:
                max_number_of_open_positions = st.number_input(label="Max Number of Open Positions", 
                                                            min_value=1,
                                                            value=int(config['max_number_of_open_positions']),
                                                            max_value=50,
                                                            step=1,
                                                            help="""
                                                            If tradable balance = 1000 and max_number_of_open_positions = 10, the stake_amount = 1000/10 = 100
                                                            """)
            except KeyError:
                st.warning('Invalid or missing configuration: max_number_of_open_positions')
                st.stop()
            try:
                tradable_balance_ratio = st.slider(label='Tradable Balance Ratio', 
                                                min_value=0.0, 
                                                max_value=1.0, 
                                                value=float(config['tradable_balance_ratio']), 
                                                step=0.01,
                                                help="""Tradable percentage of the balance
                                                """)
            except KeyError:
                st.warning('Invalid or missing configuration: tradable_balance_ratio')
                st.stop()
            try:
                trade_against = st.selectbox('Trade Against', ['BUSD', 'USDT', 'BTC'], index=['BUSD', 'USDT', 'BTC'].index(config['trade_against']),
                                            help="""Trade against BUSD, USDT or BTC
                                            """)
            except KeyError:
                st.warning('Invalid or missing configuration: trade_against')
                st.stop()
            try:
                if trade_against in ["BUSD","USDT"]:
                    trade_min_val = 0
                    trade_step = 1
                    trade_format = None
                    if int(config['min_position_size']) < 20:
                        trade_min_pos_size = 20
                    else:
                        trade_min_pos_size = int(config['min_position_size'])
                elif trade_against == "BTC":
                    trade_min_val = 0.0
                    trade_step = 0.0001
                    trade_format = "%.4f"
                    if float(config['min_position_size']) > 0.0001:
                        trade_min_pos_size = 0.0001
                    else:
                        trade_min_pos_size = float(config['min_position_size'])

                min_position_size = st.number_input(label='Minimum Position Size', 
                                                    min_value=trade_min_val, 
                                                    value=trade_min_pos_size, 
                                                    step=trade_step,
                                                    format=trade_format,
                                                    help="""If trade_against = BUSD or USDT => min_position_size = 20
                                                        \nIf trade_against = BTC => min_position_size = 0.001
                                                    """)
            except KeyError:
                st.warning('Invalid or missing configuration: min_position_size')
                st.stop()
            try:
                trade_top_performance = st.slider('Trade Top Performance Coins', 1, 50, config['trade_top_performance'],
                                                help="""
                                                    Trade top X performance coins                                              
                                                """)
            except KeyError:
                st.warning('Invalid or missing configuration: trade_top_performance')
                st.stop()
            try:
                stop_loss = st.number_input(label='Stop Loss %', 
                                            min_value=0.0, 
                                            value=float(config['stop_loss']), 
                                            step=0.01,
                                            help="""Set stop loss to automatically sell if its price falls below a certain percentage.
                                                \nExamples:
                                                \n stop_loss = 0 => will not use stop loss. The stop loss used will be triggered when slow_ema > fast_ema
                                                \n stop_loss = 10 => 10%   
                                            """)
            except KeyError:
                st.warning('Invalid or missing configuration: stop_loss')
                st.stop()

    # Update the configuration dictionary with the modified values
    config['stake_amount_type'] = stake_amount_type
    config['max_number_of_open_positions'] = max_number_of_open_positions
    config['tradable_balance_ratio'] = tradable_balance_ratio
    config['min_position_size'] = min_position_size
    config['trade_top_performance'] = trade_top_performance
    config['trade_against'] = trade_against
    config['stop_loss'] = stop_loss
    config['bot_1d'] = bot_1d
    config['bot_4h'] = bot_4h
    config['bot_1h'] = bot_1h

    # Write the modified configuration dictionary back to the YAML file
    try:
        with open('config.yaml', 'w') as f:
            yaml.dump(config, f)
    except PermissionError:
        st.warning('Permission denied: could not write to config file!')
        st.stop()

def show_main_page():
    
    # paths = find_file_paths('data.db')
    # bot_names = get_bot_names(paths)

    #sidebar with available bots
    # with st.sidebar:
    #     bot_selected = st.radio("Choose Bot:",(bot_names))

    # global connection
    # connection = database.connect_to_bot(bot_selected)
    
    # trade_against = get_trade_against(bot_selected)
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
    st.caption(f'**{bot_selected}** - {trade_against}')

    get_chart_daily_balance()
    get_chart_daily_asset_balances()

    global tab_upnl, tab_rpnl, tab_top_perf, tab_signals, tab_blacklist, tab_best_ema, tab_settings
    tab_upnl, tab_rpnl, tab_signals, tab_top_perf, tab_blacklist, tab_best_ema, tab_settings = st.tabs(["Unrealized PnL", "Realized PnL", "Signals", "Top Performers", "Blacklist", "Best EMA", "Settings"])

    realized_pnl()
    unrealized_pnl()
    signals()
    top_performers()
    blacklist()
    best_ema()
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
                new_password = st.text_input('New password', type='password')
                new_password_repeat = st.text_input('Repeat password', type='password')

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
                                st.experimental_rerun()
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

    # print('')              
    
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

# # define the table style where the last row is bold
def last_row_bold(row):
    return ['font-weight: bold']*len(row)
    if row.name == result_closed_positions.index[-1]:
        # return ['font-weight: bold']*len(row)
        return f'background-color: black'
    return ['']*len(row)

def reset_form_open(state):
    if 'reset_form_open' in  st.session_state:
        st.session_state.reset_form_open = state

def reset_password_submitted(state):
    if 'reset_password_submitted' in  st.session_state:
        st.session_state.reset_password_submitted = state

def main():

    # Initialization
    if 'name' not in  st.session_state:
        st.session_state.name = ''
    if 'username' not in  st.session_state:
        st.session_state.username = ''
    if 'user_password' not in  st.session_state:
        st.session_state.user_password = 'None'
    if 'reset_form_open' not in st.session_state:
        st.session_state.reset_form_open = False
    if 'reset_password_submitted' not in  st.session_state:
        st.session_state.reset_password_submitted = False
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
        reset_clicked = st.sidebar.button("Reset")
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











