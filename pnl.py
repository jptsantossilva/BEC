import pandas as pd
import streamlit as st
import time
import numpy as np
from millify import millify
import os
import yaml
import sys
import database
import calendar
import config
import exchange
import streamlit_authenticator as stauth

if "authentication_status" not in st.session_state:
        st.session_state["authentication_status"] = None

# st.session_state

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

# im using to find which bots are running
def find_file_paths(filename):
    
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

def get_bot_names(paths):
    bot_names = []
    for path in paths:
        bot_names.append(os.path.basename(os.path.normpath(path)))
    
    return bot_names
    
def set_database_connection(bot):
    # get the current working directory
    cwd = os.getcwd()
    file_path = os.path.join(cwd, '..', bot)    
    database.set_connection(file_path)
    
def get_trade_against(bot):

    # get settings from config file
    try:
        # get the current working directory
        cwd = os.getcwd()
        file_path = os.path.join(cwd, '..', bot, 'config.yaml')
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)

        trade_against = config["trade_against"]

        return trade_against
        
    except FileNotFoundError as e:
        msg = "Error: The file config.yaml could not be found."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        # telegram.send_telegram_message(telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg)
        # sys.exit(msg) 

    except yaml.YAMLError as e:
        msg = "Error: There was an issue with the YAML file."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        # telegram.send_telegram_message(telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg)
        # sys.exit(msg)

def show_main_page():

    paths = find_file_paths('data.db')
    bot_names = get_bot_names(paths)

    #sidebar with available bots
    with st.sidebar:
        bot_selected = st.radio(
            "Choose Bot:",
            (bot_names)
            )

    set_database_connection(bot_selected)
    trade_against = get_trade_against(bot_selected)
    
    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2  

    st.caption(f'**{bot_selected}** - {trade_against}')

    tab_upnl, tab_rpnl, tab_top_perf, tab_blacklist, tab_best_ema = st.tabs(["Unrealized PnL", "Realized PnL", "Top Performers", "Blacklist", "Best EMA"])

    # get years
    years = get_years(bot_selected)

    # years empty list
    if len(years) == 0:
        tab_rpnl.warning('There are no closed positions yet! 🤞')

    col1, col2, col3 = tab_rpnl.columns(3)
    # years selectbox
    year = col1.selectbox(
        'Year',
        (years)
    )
    # get months
    months_dict = get_orders_by_month(year, bot_selected)
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

    result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = calculate_realized_pnl(year, month_number)
    print("\nPnL - Total")
    # apply the lambda function to make the last row bold
    # result_closed_positions = result_closed_positions.apply(lambda x: ['font-weight: bold' if i == len(x)-1 else '' for i in range(len(x))], axis=1)

    print(result_closed_positions)

    tab_rpnl.header("Realized PnL - Total")
    # tab_rpnl.dataframe(result_closed_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['Pnl_Perc','Pnl_Value']))
    tab_rpnl.dataframe(result_closed_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

    print("Realized PnL - Detail")
    print(trades_month_1d)
    print(trades_month_4h)
    print(trades_month_1h)

    tab_rpnl.header(f"Realized PnL - Detail")
    tab_rpnl.subheader("Bot 1d")
    tab_rpnl.dataframe(trades_month_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
    tab_rpnl.subheader("Bot 4h")
    tab_rpnl.dataframe(trades_month_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
    tab_rpnl.subheader("Bot 1h")
    tab_rpnl.dataframe(trades_month_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))


    # print('\n----------------------------\n')

    result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = calculate_unrealized_pnl()
    print("\nUnrealized PnL - Total")
    print('-------------------------------')
    print(result_open_positions)

    if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
        tab_upnl.warning('There are no open positions yet! 🤞') 

    tab_upnl.header("Unrealized PnL - Total")

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

    col1, col2, col3, col4 = tab_upnl.columns(4)
    col1.metric("1d", currPnL_1d_value, str(currPnL_1d_perc)+"%")
    col2.metric("4h", currPnL_4h_value, str(currPnL_4h_perc)+"%")
    col3.metric("1h", currPnL_1h_value, str(currPnL_1h_perc)+"%")
    col4.metric("Total", currPnL_total_value, str(currPnL_total_perc)+"%")

    tab_upnl.write("")

    tab_upnl.dataframe(data=result_open_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

    print("Unrealized PnL - Detail")
    print(positions_df_1d)
    print(positions_df_4h)
    print(positions_df_1h)

    tab_upnl.header(f"Unrealized PnL - Detail")
    tab_upnl.subheader("Bot 1d")
    tab_upnl.dataframe(positions_df_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
    tab_upnl.subheader("Bot 4h")
    tab_upnl.dataframe(positions_df_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))
    tab_upnl.subheader("Bot 1h")
    tab_upnl.dataframe(positions_df_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']))

    #----------------------
    # Force Close Position
    tab_upnl.header("Forced Selling")
    # add expander
    sell_expander = tab_upnl.expander("Choose position to sell")
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
            # sell
            symbol_only, symbol_stable = exchange.separate_symbol_and_trade_against(sell_symbol)
            # get balance
            balance_qty = exchange.get_symbol_balance(symbol_only, sell_bot)  
            # verify sell quantity
            df_pos = database.get_positions_by_bot_symbol_position(bot=sell_bot, symbol=sell_symbol, position=1)
            if not df_pos.empty:
                buy_order_qty = df_pos['Qty'].iloc[0]
            
            sell_qty = buy_order_qty
            if balance_qty < buy_order_qty:
                sell_qty = balance_qty
            sell_qty = exchange.adjust_size(sell_symbol, sell_qty)
            exchange.create_sell_order(symbol=sell_symbol,
                                    bot=sell_bot,
                                    reason=sell_reason) 

            sell_expander.success(f"{sell_symbol} SOLD!")
            time.sleep(3)
            # dasboard refresh
            st.experimental_rerun()
    #----------------------

    tab_top_perf.subheader(f"Top {config.trade_top_performance} Performers")
    df_mp = database.get_all_symbols_by_market_phase()
    df_mp['Price'] = df_mp['Price'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_mp['DSMA50'] = df_mp['DSMA50'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_mp['DSMA200'] = df_mp['DSMA200'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_mp['Perc_Above_DSMA50'] = df_mp['Perc_Above_DSMA50'].apply(lambda x:'{:.2f}'.format(x))
    df_mp['Perc_Above_DSMA200'] = df_mp['Perc_Above_DSMA200'].apply(lambda x:'{:.2f}'.format(x))
    tab_top_perf.dataframe(df_mp)


    #----------------------

    tab_blacklist.subheader("Blacklist")
    df_blacklist = database.get_symbol_blacklist()
    edited_blacklist = tab_blacklist.experimental_data_editor(df_blacklist, num_rows="dynamic")
    blacklist_apply_changes = tab_blacklist.button("Save")

    if blacklist_apply_changes:
        edited_blacklist.to_sql(name='Blacklist',con=database.connection, index=True, if_exists="replace")
        tab_blacklist.success("Blacklist changes saved")


    tab_best_ema.subheader("Best EMA")
    df_bema = database.get_all_best_ema()
    tab_best_ema.dataframe(df_bema)


def reset_password():
    # if authentication_status:
    if st.session_state.authentication_status:
        try:
            if authenticator.reset_password(st.session_state.username, 'Reset password'):
                st.success('Password modified successfully')
                new_passw = authenticator.credentials['usernames'][st.session_state.username]['password']
                database.update_user_password(username=st.session_state.username, password=new_passw)
                # time.sleep(5)  # pause for 5 seconds
        except Exception as e:
            st.error(e)

def create_new_user():
    try:
        if authenticator.register_user('Register user'):
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
    years = database.get_years_from_orders()
    return years

# get months with orders within the year
def get_orders_by_month(year, bot):

    months = database.get_months_from_orders_by_year(year)

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict
    
# Define a function to get the year and month from a datetime object
def get_year_month(date):
    return date.year, date.month

def calculate_realized_pnl(year, month):

    print(f'Year = {year}')
    if month == 13:
        print(f'Month = ALL')
    else:
        print(f'Month = {month}')

    print('\n Realized PnL')
    print('---------------------')
    
    
    df_month_1d = database.get_orders_by_bot_side_year_month(bot="1d", side="SELL", year=year, month=str(month))
    df_month_4h = database.get_orders_by_bot_side_year_month(bot="4h", side="SELL", year=year, month=str(month))
    df_month_1h = database.get_orders_by_bot_side_year_month(bot="1h", side="SELL", year=year, month=str(month))
    
    print('')              
    print(df_month_1d)
    print(df_month_4h)
    print(df_month_1h)
    
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

    return results_df, df_month_1d, df_month_4h, df_month_1h

def calculate_unrealized_pnl():
    
    print('\nUnrealized PnL')
    print('---------------------')

    # results_df = pd.DataFrame(columns=['bot','pnl_%','pnl_value','positions'])

    df_positions_1d = database.get_unrealized_pnl_by_bot(bot="1d")
    df_positions_4h = database.get_unrealized_pnl_by_bot(bot="4h")
    df_positions_1h = database.get_unrealized_pnl_by_bot(bot="1h")

    print('')              
    
    print(df_positions_1d)
    print(df_positions_4h)
    print(df_positions_1h)

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

def show_login_page():
    # connect to database
    database.connect()
    df_users = database.get_all_users()
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict('index')
    formatted_credentials = {'usernames': {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials['usernames'][username] = user_info

    global authenticator
    st.title(f'Dashboard')

    authenticator = stauth.Authenticate(
        credentials=formatted_credentials,
        cookie_name="dashboard_cookie_name",
        key="dashboard_cookie_key",
        cookie_expiry_days=0
    )

    name, authentication_status, username = authenticator.login('Login', 'main')

    st.session_state.name = name
    st.session_state.username = username

    if authentication_status:
        authenticator.logout('Logout', 'sidebar')
        st.sidebar.button("Reset", on_click=reset_password)
        st.sidebar.title(f'Welcome *{st.session_state.name}*')
        show_main_page()
    elif authentication_status == False:
        st.error('Username or password is incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password')


# st.session_state

show_login_page()


# Close the database connection
# database.connection.close()







