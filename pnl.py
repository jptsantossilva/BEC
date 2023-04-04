import pandas as pd
import streamlit as st
import datetime
import numpy as np
from millify import millify
import os
import yaml
import sys

st.set_page_config(
    page_title="Bot Dashboard App",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        # 'Get Help': 'https://www.extremelycoolapp.com/help',
        # 'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

# im using to find which bots are running using the positions files
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

paths = find_file_paths('positions1d.csv')

def get_bot_names(paths):
    bot_names = []
    for path in paths:
        bot_names.append(os.path.basename(os.path.normpath(path)))
    
    return bot_names

bot_names = get_bot_names(paths)

#sidebar with available bots
with st.sidebar:
    bot_selected = st.radio(
        "Choose Bot:",
        (bot_names)
        )
    
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
        # telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
        # sys.exit(msg) 

    except yaml.YAMLError as e:
        msg = "Error: There was an issue with the YAML file."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        # telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
        # sys.exit(msg)

st.title(f'Dashboard')
trade_against = get_trade_against(bot_selected)
num_decimals = 8 if trade_against == "BTC" else 2  
st.caption(f'**{bot_selected}** - {trade_against}')

tab_upnl, tab_rpnl = st.tabs(["Unrealized PnL","Realized PnL"])

# Get years with orders
def get_orders_by_year(bot):
    
    time_frame = ["1d", "4h", "1h"]

    years = []

    for tf in time_frame: 
        # get the current working directory
        cwd = os.getcwd()
        bot_dir_path = os.path.join(cwd, '..', bot, 'orders'+tf+'.csv')
        df = pd.read_csv(bot_dir_path)
        
        # df = pd.read_csv('orders'+tf+'.csv')

        # convert 'date' column to datetime format
        df['time'] = pd.to_datetime(df['time'])

        # get unique years 
        years.append(df['time'].dt.year.unique())
    
    # flatten list and eliminate duplicates
    years = set([year for sublist in years for year in sublist])
        
    # sort list in descending order
    years = sorted(years, reverse=True)

    # print resulting list of years
    # print(years)

    return years

# get months with orders within the year
def get_orders_by_month(year, bot):
    
    time_frame = ["1d", "4h", "1h"]

    months = {}

    for tf in time_frame: 
        # get the current working directory
        cwd = os.getcwd()
        bot_dir_path = os.path.join(cwd, '..', bot, 'orders'+tf+'.csv')
        df = pd.read_csv(bot_dir_path)
        # df = pd.read_csv('orders'+tf+'.csv')
        
        # convert 'date' column to datetime format
        df['time'] = pd.to_datetime(df['time'])

        # Filter dataframe to include only the specified year
        filtered_df = df[df['time'].dt.year == year]

        # get unique months
        unique_months = filtered_df['time'].dt.month_name().unique()
        
        # add each month to the dictionary with its corresponding value
        for month_name in unique_months:
            month_num = filtered_df['time'].dt.month[filtered_df['time'].dt.month_name() == month_name].iloc[0]
            months[month_name] = month_num
    
    # sort
    months = sorted(months, key=months.get)

    # insert ALL as first item
    # months["ALL"] = 0

    return months

# get years
years = get_orders_by_year(bot_selected)

# years empty list
if len(years) == 0:
    tab_rpnl.warning('There are no closed positions yet! Looks like you just started 🤞')

col1, col2, col3 = tab_rpnl.columns(3)

# years selectbox
year = col1.selectbox(
    'Year',
    (years)
)
# get months
months = get_orders_by_month(year, bot_selected)

# months selectbox
month = col2.selectbox(
    'Month',
    (months)
)

# get month 
if month == None:
    #get current month name
    month = datetime.date.today().strftime('%B')
    
month_number = datetime.datetime.strptime(month, '%B').month
if col2.checkbox('Full Year'):
    month_number = 13

# st.write('month_number: ', month_number)

# Define a function to get the year and month from a datetime object
def get_year_month(date):
    return date.year, date.month

def calculate_realized_pnl(year, month, bot):

    # Get user input for the year and month
    # print('Choose period for PnL analysis of closed positions')
    # year = int(input('Enter year (YYYY): '))
    # month = int(input('Enter month (MM): '))

    print(f'Year = {year}')
    if month == 13:
        print(f'Month = ALL')
    else:
        print(f'Month = {month}')

    print('\n Realized PnL')
    print('---------------------')
    
    positionsTimeframe = ["1d", "4h", "1h"] 

    results_df = pd.DataFrame(columns=['bot','Year','Month','pnl_%','pnl_value','trades'])

    for tf in positionsTimeframe: 
        # get the current working directory
        cwd = os.getcwd()
        bot_dir_path = os.path.join(cwd, '..', bot, 'orders'+tf+'.csv')
        
        df = pd.read_csv(bot_dir_path)
        # df = pd.read_csv('orders'+tf+'.csv')
    
        # Convert the time column to a Pandas datetime object
        df['time'] = pd.to_datetime(df['time'])
    
        # Filter the dataframe by the year and by the 'SELL' side
        year_filter = df['time'].dt.year == year
        side_filter = df['side'] == 'SELL'
        month_filter = df['time'].dt.month == month

        if month <= 12:
            df = df[year_filter & month_filter & side_filter]
        elif month == 13: # full year
            df = df[year_filter & side_filter]

        df['bot'] = tf
        # Get the total number of rows in the filtered dataframe
        trades = len(df)

        # remove miliseconds
        df['time'] = df['time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        month_df = df[['bot','time','symbol','executedQty','price','pnlusd','pnlperc']].copy()
        month_df = month_df.rename(columns={'time':'date','executedQty':'quantity','price':'sellPrice','pnlusd':'pnl_value','pnlperc':'pnl_%'})

        print('')              
        print(month_df)

        # drop the 'bot' column
        month_df = month_df.drop('bot', axis=1)

        if tf == "1h":
            month_df_1h = month_df
        elif tf == "4h":
            month_df_4h = month_df
        elif tf == "1d":
            month_df_1d = month_df

        # Calculate the sums of the 'pnlperc' and 'pnlvalue' columns
        pnl_perc_sum = month_df['pnl_%'].sum()
        pnl_value_sum = round(month_df['pnl_value'].sum(), num_decimals)

        # Create a new dataframe with the results
        df_new = pd.DataFrame({
                'bot': [tf],
                'Year': [year],
                'Month': [month],
                'pnl_%': [pnl_perc_sum],
                'pnl_value': [pnl_value_sum],
                'trades': [trades]})
        # append the new data to the existing DataFrame
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    sum_pnl_perc = results_df['pnl_%'].sum()
    sum_pnl_value = results_df['pnl_value'].sum()
    sum_trades = results_df['trades'].sum()
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL','', '', sum_pnl_perc, sum_pnl_value, sum_trades]
    
    return results_df, month_df_1d, month_df_4h, month_df_1h

def calculate_unrealized_pnl(bot):
    
    print('\nUnrealized PnL')
    print('---------------------')

    positionsTimeframe = ["1d", "4h", "1h"] 

    results_df = pd.DataFrame(columns=['bot','pnl_%','pnl_value','positions'])

    for tf in positionsTimeframe: 
        # get the current working directory
        cwd = os.getcwd()
        bot_dir_path = os.path.join(cwd, '..', bot, 'positions'+tf+'.csv')
        
        df = pd.read_csv(bot_dir_path)
        # df = pd.read_csv('positions'+tf+'.csv')
    
        # Convert the time column to a Pandas datetime object
        # df['time'] = pd.to_datetime(df['time'])
    
        # Filter the dataframe by the year and month provided by the user, and by the 'SELL' side
        # month_df = df[(df['time'].dt.year == year) & (df['time'].dt.month == month) & (df['position'] == '1')]
        df = df[(df['position'] == 1)]

        # Get the total number of rows in the filtered dataframe
        positions = len(df)
        df['bot'] = tf

        df['pnlusd'] = (df['currentPrice']*df['quantity'])-(df['buyPrice']*df['quantity']) 
        # calc pnlperc2 to avoid the round from the original pnlperc
        df['pnlperc2'] = (((df['currentPrice']*df['quantity'])-(df['buyPrice']*df['quantity']))/(df['buyPrice']*df['quantity']))*100

        positions_df = df[['bot','Currency','quantity','buyPrice','pnlusd','pnlperc2']].copy()
        positions_df = positions_df.rename(columns={'Currency':'symbol','pnlusd':'pnl_value','pnlperc2':'pnl_%'})

        # drop the 'bot' column
        positions_df = positions_df.drop('bot', axis=1)

        if tf == "1h":
            positions_df_1h = positions_df
        elif tf == "4h":
            positions_df_4h = positions_df
        elif tf == "1d":
            positions_df_1d = positions_df

        # Calculate the sums of the 'pnlperc' and 'pnlvalue' columns
        pnl_perc_sum = round(positions_df['pnl_%'].sum(),2)
        pnl_value_sum = round(positions_df['pnl_value'].sum(), num_decimals)

        print('')              
        print(positions_df)

        # Create a new dataframe with the results
        df_new = pd.DataFrame({
                'bot': [tf],
                'pnl_%': [pnl_perc_sum],
                'pnl_value': [pnl_value_sum],
                'positions': [positions]})
        # append the new data to the existing DataFrame
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    sum_pnl_perc = results_df['pnl_%'].sum()
    sum_pnl_value = results_df['pnl_value'].sum()
    sum_positions = results_df['positions'].sum()
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL', sum_pnl_perc, sum_pnl_value, sum_positions]
    
    return results_df, positions_df_1d, positions_df_4h, positions_df_1h


# define a function to set the background color of the rows based on pnl_value
def set_pnl_color(val):
    color = '#E9967A' if val < 0 else '#8FBC8F' if val > 0 else ''
    return f'background-color: {color}'

# define the table style where the last row is bold
def last_row_bold(row):
    return ['font-weight: bold']*len(row)
    if row.name == result_closed_positions.index[-1]:
        # return ['font-weight: bold']*len(row)
        return f'background-color: black'
    return ['']*len(row)

result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = calculate_realized_pnl(year, month_number, bot_selected)
print("\nPnL - Total")
# apply the lambda function to make the last row bold
# result_closed_positions = result_closed_positions.apply(lambda x: ['font-weight: bold' if i == len(x)-1 else '' for i in range(len(x))], axis=1)

print(result_closed_positions)

tab_rpnl.header("Realized PnL - Total")
tab_rpnl.dataframe(result_closed_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))

print("Realized PnL - Detail")
print(trades_month_1d)
print(trades_month_4h)
print(trades_month_1h)

tab_rpnl.header(f"Realized PnL - Detail")
tab_rpnl.subheader("Bot 1d")
tab_rpnl.dataframe(trades_month_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))
tab_rpnl.subheader("Bot 4h")
tab_rpnl.dataframe(trades_month_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))
tab_rpnl.subheader("Bot 1h")
tab_rpnl.dataframe(trades_month_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))


# print('\n----------------------------\n')

result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = calculate_unrealized_pnl(bot_selected)
print("\nUnrealized PnL - Total")
print('-------------------------------')
print(result_open_positions)

if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
    tab_upnl.warning('There are no open positions yet! Looks like you just started 🤞') 

tab_upnl.header("Unrealized PnL - Total")

# st.sidebar.subheader('Unrealized PnL %')
# col1, col2, col3 = st.sidebar.columns(3)
currPnL_1d_value = result_open_positions.loc[result_open_positions['bot'] == '1d', 'pnl_value'].iloc[0]
currPnL_4h_value = result_open_positions.loc[result_open_positions['bot'] == '4h', 'pnl_value'].iloc[0]
currPnL_1h_value = result_open_positions.loc[result_open_positions['bot'] == '1h', 'pnl_value'].iloc[0]
currPnL_total_value = currPnL_1d_value + currPnL_4h_value + currPnL_1h_value

# Convert long numbers into a human-readable format in Python
# 1200 to 1.2k; 12345678 to 12.35M 
currPnL_1d_value = millify(currPnL_1d_value, precision=num_decimals)
currPnL_4h_value = millify(currPnL_4h_value, precision=num_decimals)
currPnL_1h_value = millify(currPnL_1h_value, precision=num_decimals)
currPnL_total_value = millify(currPnL_total_value, precision=num_decimals)

currPnL_1d_perc = result_open_positions.loc[result_open_positions['bot'] == '1d', 'pnl_%'].iloc[0]
currPnL_4h_perc = result_open_positions.loc[result_open_positions['bot'] == '4h', 'pnl_%'].iloc[0]
currPnL_1h_perc = result_open_positions.loc[result_open_positions['bot'] == '1h', 'pnl_%'].iloc[0]
currPnL_total_perc = currPnL_1d_perc + currPnL_4h_perc + currPnL_1h_perc

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

tab_upnl.dataframe(result_open_positions.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))

print("Unrealized PnL - Detail")
print(positions_df_1d)
print(positions_df_4h)
print(positions_df_1h)

tab_upnl.header(f"Unrealized PnL - Detail")
tab_upnl.subheader("Bot 1d")
tab_upnl.dataframe(positions_df_1d.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))
tab_upnl.subheader("Bot 4h")
tab_upnl.dataframe(positions_df_4h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))
tab_upnl.subheader("Bot 1h")
tab_upnl.dataframe(positions_df_1h.style.apply(last_row_bold, axis=0).applymap(set_pnl_color, subset=['pnl_%','pnl_value']))







