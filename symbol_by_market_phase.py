"""
Gets all coin pairs from Binance, calculate market phase for each and store results in coinpairByMarketPhase_USD_1d.csv 
Removes coins from positions files that are not top performers in the accumulation or bullish phase.
Adds the coins in the accumulation or bullish phase to addCoinPair.csv and calc BestEMA 
for each coin pair on 1d,4h,1h time frame and save on positions files
"""

# %%
import os
import config
from binance.client import Client
import requests
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np
import sys
import timeit
# import add_symbol
import telegram
import logging
import yaml
import sqlite3
import database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# calculate program run time
start = timeit.default_timer() 

# inform start
telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eStart, "Market Phases - Start")

# log file to store error messages
log_filename = "coinpairByMarketPhase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

connection = database.connect()
database.create_tables(connection)

# # get settings from config file
# try:
#     with open("config.yaml", "r") as file:
#         config = yaml.safe_load(file)

#     trade_top_performance = config. ["trade_top_performance"]

# except FileNotFoundError as e:
#     msg = "Error: The file config.yaml could not be found."
#     msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
#     sys.exit(msg) 

# except yaml.YAMLError as e:
#     msg = "Error: There was an issue with the YAML file."
#     msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
#     sys.exit(msg) 

# Binance
# environment variables
# try:
    # Binance
    # api_key = os.environ.get('binance_api')
    # api_secret = os.environ.get('binance_secret')

# except KeyError as e: 
#     msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)

# telegram
# telegramToken_MarketPhases = os.environ.get('telegramToken_MarketPhases')
# telegram_chat_id = os.environ.get('telegram_chat_id')

# Binance Client
# try:
#     client = Client(api_key, api_secret)
# except Exception as e:
#         msg = "Error connecting to Binance. "+ repr(e)
#         print(msg)
#         logging.exception(msg)
#         telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
#         sys.exit(msg) 

def connect_binance():
    api_key = config.get_env_var('binance_api')
    api_secret = config.get_env_var('binance_secret')

    # Binance Client
    try:
        global binance_client
        binance_client = Client(api_key, api_secret)
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
            sys.exit(msg) 

connect_binance()

# Check the program has been called with the timeframe
# total arguments
n = len(sys.argv)
# print("Total arguments passed:", n)
if n < 2:
    print("Argument is missing")
    timeframe = input('Enter timeframe (1d, 8h, 4h):')
    trade_against = input('Trade against USDT, BUSD or BTC:')
else:
    # argv[0] in Python is always the name of the script.
    timeframe = sys.argv[1]
    trade_against = sys.argv[2]

if timeframe == "1d": startdate = "200 day ago UTC"
elif timeframe == "8h": startdate = str(8*200)+" hour ago UTC"
elif timeframe == "4h": startdate = str(4*200)+" hour ago UTC"

# read coins in blacklist to not trade
# df_blacklist = pd.read_csv('blacklist.csv')

df_blacklist = pd.DataFrame(database.get_all_blacklist(connection))
df_blacklist['Symbol'] = df_blacklist['Symbol'].astype(str)+trade_against
# put the blacklist in a set
blacklist = set(df_blacklist["Symbol"].unique())

try:
    exchange_info = client.get_exchange_info()
except Exception as e:
        msg = "Error connecting to Binance. "+ repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        sys.exit(msg) 
        
symbols = set()

for s in exchange_info['symbols']:
    if (s['symbol'].endswith(trade_against)
        and not(s['symbol'].endswith('DOWN'+trade_against))
        and not(s['symbol'].endswith('UP'+trade_against))
        and not(s['symbol'] == "AUD"+trade_against) # Australian Dollar
        and not(s['symbol'] == "EUR"+trade_against) # Euro
        and not(s['symbol'] == "GBP"+trade_against) # British pound
        and s['status'] == 'TRADING'):
            symbols.add(s['symbol'])

# from the coinPairs to trade, exclude coins from Blacklist
symbols -= blacklist

symbols = sorted(symbols)
msg = str(len(symbols))+" symbols found. Calculating..."
print(msg)
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)

def applytechnicals(df):
        
        df['DSMA50'] = df['Price'].rolling(50).mean()
        df['DSMA200'] = df['Price'].rolling(200).mean()

        df['Perc_Above_DSMA50'] = ((df['Close']-df['DSMA50'])/df['DSMA50'])*100
        df['Perc_Above_DSMA200'] = ((df['Close']-df['DSMA200'])/df['DSMA200'])*100        

def getdata(Symbol):
    try:
        frame = pd.DataFrame(client.get_historical_klines(Symbol
                                                        ,timeframe                                        
                                                        
                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        # ,startDate

                                                        ))
        
        frame = frame.iloc[:,[0,4]] # columns selection
        frame.columns = ['Time','Price'] #rename columns
        # new dataframe with price only
        frame[['Price']] = frame[['Price']].astype(float) #cast to float
        # frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        frame['Symbol'] = Symbol
        frame.index = [datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        
        frame = frame[['Symbol','Price']]
        return frame
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+coinPair+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)

        # return empty dataframe
        frame = pd.DataFrame()
        return frame 

# empty dataframe
dfResult = pd.DataFrame()

for symbol in symbols:

    print("calculating "+symbol)
    df = getdata(symbol)
    applytechnicals(df)
    # last one is the one with 200dsma value
    df = df.tail(1)

    if dfResult.empty:
        dfResult = df
    else:
        dfResult = pd.concat([dfResult, df])

# Coins in accumulation and Bullish phases
conditions = [
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200']), # recovery phase
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200']), # accumulation phase
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # bullish phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # warning phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # distribution phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200'])  # bearish phase
]
# set marketphase to each coin
values = ['recovery', 'accumulation', 'bullish', 'warning','distribution','bearish']
dfResult['Market_Phase'] = np.select(conditions, values)
# print(dfResult)

# currentDate = date.today().strftime('%Y%m%d')
# dfResult.to_csv("coinPairByMarketPhase/coinpairByMarketPhase_"+trade_against+"_"+timeframe+"_"+currentDate+".csv")

dfBullish = dfResult.query("Market_Phase == 'bullish'")
dfAccumulation= dfResult.query("Market_Phase == 'accumulation'")

# union accumulation and bullish results
dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)

df_top = dfUnion.sort_values(by=['Perc_Above_DSMA200'], ascending=False)
df_top = df_top.head(config.trade_top_performance)

# set rank for highest strength
df_top['Rank'] = np.arange(len(df_top))+1

# delete existing data
database.delete_all_symbols_by_market_phase(connection)

# insert new symbols
for index, row in df_top.iterrows():
    database.insert_symbols_by_market_phase(connection, 
                                            row['Symbol'],
                                            row['Price'],
                                            row['Volume'],
                                            row['DSMA50'],
                                            row['DSMA200'],
                                            row['Market_Phase'],
                                            row['Perc_Above_DSMA50'],
                                            row['Perc_Above_DSMA200'],
                                            row['Rank']
                                            )

# df_top.to_csv("coinpairByMarketPhase_"+trade_against+"_"+timeframe+".csv", index=False)

selected_columns = df_top[["Symbol","Price","Market_Phase"]]
df_top_print = selected_columns.copy()
# df_top_print = df_top_print.rename(columns={"Coinpair": "Symbol", "Close": "Price"})
# reset the index and set number beginning from 1
df_top_print = df_top_print.reset_index(drop=True)
df_top_print.index += 1

msg = f"Top {str(config.trade_top_performance)} performance coins:"
print(msg)
print(df_top_print.to_string(index=True))

telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", df_top_print.to_string(index=True))

#---------------------------------------------
# create file list of top performers and coins in position to import to TradingView 
#---------------------------------------------
# Read CSV file and select only the 'coinpair' column
# df_tv_list = pd.read_csv('coinpairByMarketPhase_'+trade_against+'_1d.csv', usecols=['Coinpair'])
df_tv_list = pd.DataFrame(database.get_symbols_from_symbols_by_market_phase(connection))

# df_pos_1h = pd.read_csv("positions1h.csv")
# df_pos_4h = pd.read_csv("positions4h.csv")
# df_pos_1d = pd.read_csv("positions1d.csv")

df_pos_1h = pd.DataFrame(database.get_symbol_from_position_by_bot(connection, "1h"))
df_pos_4h = pd.DataFrame(database.get_symbol_from_position_by_bot(connection, "4h"))
df_pos_1d = pd.DataFrame(database.get_symbol_from_position_by_bot(connection, "1d"))

# Rename the column to 'symbol'
# df_tv_list = df_tv_list.rename(columns={'Coinpair': 'symbol'})

# Rename the 'symbol' column to 'Currency' in the 'df_pos1h', 'df_pos4h', and 'df_pos1d' dataframes
# df_pos_1h = df_pos_1h.rename(columns={'Currency': 'symbol'})
# df_pos_4h = df_pos_4h.rename(columns={'Currency': 'symbol'})
# df_pos_1d = df_pos_1d.rename(columns={'Currency': 'symbol'})

# Filter the open positions
# df_pos_1h = df_pos_1h.query('position == 1')[['symbol']]
# df_pos_4h = df_pos_4h.query('position == 1')[['symbol']]
# df_pos_1d = df_pos_1d.query('position == 1')[['symbol']]

# Merge the dataframes using an outer join on the 'symbol' column
merged_df = pd.merge(df_tv_list, df_pos_1h, on='symbol', how='outer')
merged_df = pd.merge(merged_df, df_pos_4h, on='symbol', how='outer')
merged_df = pd.merge(merged_df, df_pos_1d, on='symbol', how='outer')

df_top = merged_df

df_tv_list = merged_df
df_tv_list['symbol'] = "BINANCE:"+df_tv_list['symbol']
# Write DataFrame to CSV file
filename = "Top_performers_"+trade_against+".txt" 
df_tv_list.to_csv(filename, header=False, index=False)
msg = "TradingView List:"
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)
telegram.send_telegram_file(telegram.telegramToken_market_phases, filename)
#---------------------------------------------

# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfBullish.to_string(index=False))
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfBullish))} in bullish phase")
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfAccumulation.to_string(index=False))
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfAccumulation))} in accumulation phase")

positionsTimeframe = ["1d", "4h", "1h"]

if not df_top.empty:

    #---------------------------------------------
    # remove coins from position files that are not top perfomers in accumulation or bullish phase
    #---------------------------------------------
    # top_coins = df_top.symbol.to_list()

    # for tf in positionsTimeframe: 
    #     positionsfile = pd.read_csv('positions'+tf+'.csv')

    #     filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
    #     filter2 = positionsfile['Currency'].isin(top_coins)
    #     positionsfile = positionsfile[filter1 | filter2]  
        
    #     positionsfile.to_csv('positions'+tf+'.csv', index=False)

    
    database.delete_positions_not_top_rank(connection)
    #---------------------------------------------

    #---------------------------------------------
    # add top rank coins with positive returns to positions files
    #---------------------------------------------


    database.add_top_rank_to_position(connection)


    # df_best_ema = pd.read_csv('coinpairBestEma.csv')
    # for tf in positionsTimeframe: 
    #     for coinPair in top_coins:
            
    #         # read position file
    #         df_pos = pd.read_csv('positions'+tf+'.csv')

    #         # check if coin is already in position file
    #         exists = coinPair in df_pos['Currency'].values
    #         if not exists:
                
    #             # get return percentage
    #             values = df_best_ema.loc[(df_best_ema['coinPair'] == coinPair) & (df_best_ema['timeFrame'] == tf), ['returnPerc']].values
    #             if len(values) > 0:
    #                 return_perc = values[0][0]
                    
    #                 #if return percentage > 0 add coin to positions file
    #                 if return_perc > 0:            
    #                     df_add = pd.DataFrame({'Currency': [coinPair],
    #                                             'performance_rank': [0],
    #                                             'position': [0],
    #                                             'quantity': [0],
    #                                             'buyPrice': [0],
    #                                             'currentPrice': [0],
    #                                             'PnLperc': [0]})
    #                     df_pos = pd.concat([df_pos, df_add], ignore_index = True, axis = 0)

    #                     try:
    #                         df_pos.to_csv('positions'+tf+'.csv', index=False)
    #                     except Exception as e:
    #                         msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
    #                         print(msg)
    #                         # telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
    #---------------------------------------------

    #---------------------------------------------
    # add symbols top performers in accumulation or bullish phase to calc best ema
    # add symbols with open positions to calc best ema
    #---------------------------------------------

    # delete rows with calc completed and keep only symbols with calc not completed
    database.delete_symbols_to_calc_completed(connection)

    # add the symbols with open positions to calc 
    database.add_symbols_with_open_positions_to_calc(connection)
    
    # add the symbols in top rank to calc
    database.add_symbols_top_rank_to_calc(connection)
     


    # try:
    #     filename = 'addcoinpair.csv'
    #     fileAddcoinpair = pd.read_csv(filename)
    # except Exception as e:
    #     msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
    #     print(msg)
    #     logging.exception(msg)
    #     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 

    # remove the coins that are not anymore top performers on the accumulation or bullish phases 
    # and next time the coin goes into these phases will calc again the best ema

    #---------------------------------------------
    #  we want to calculate the best ema for top performers, and also for those where we have positions and are no longer top performers
    #---------------------------------------------
    # df_mp = pd.read_csv("coinpairByMarketPhase_"+trade_against+"_1d.csv", usecols=['Coinpair'])
    # df_pos_1h = pd.read_csv("positions1h.csv")
    # df_pos_4h = pd.read_csv("positions4h.csv")
    # df_pos_1d = pd.read_csv("positions1d.csv")

    # Rename the column to 'symbol'
    # df_mp = df_mp.rename(columns={'Coinpair': 'symbol'})

    # Rename the 'symbol' column to 'Currency' in the 'df_pos1h', 'df_pos4h', and 'df_pos1d' dataframes
    # df_pos_1h = df_pos_1h.rename(columns={'Currency': 'symbol'})
    # df_pos_4h = df_pos_4h.rename(columns={'Currency': 'symbol'})
    # df_pos_1d = df_pos_1d.rename(columns={'Currency': 'symbol'})

    # Filter the open positions
    # df_pos_1h = df_pos_1h.query('position == 1')[['symbol']]
    # df_pos_4h = df_pos_4h.query('position == 1')[['symbol']]
    # df_pos_1d = df_pos_1d.query('position == 1')[['symbol']]

    # Merge the dataframes using an outer join on the 'symbol' column
    # merged_df = pd.merge(df_mp, df_pos_1h, on='symbol', how='outer')
    # merged_df = pd.merge(merged_df, df_pos_4h, on='symbol', how='outer')
    # merged_df = pd.merge(merged_df, df_pos_1d, on='symbol', how='outer')

    # df_top = merged_df
    #---------------------------------------------

    # keep only coins with calc not completed. Now I want to make sure best ema is calculated everyday
    # filter1 = fileAddcoinpair['Completed'] == 0 
    # # filter2 = fileAddcoinpair['Currency'].isin(top_coins)
    # # fileAddcoinpair = fileAddcoinpair[filter1 | filter2]  
    # fileAddcoinpair = fileAddcoinpair[filter1]  
    # # add coin pairs
    # for coinPair in df_top.symbol:
    #     # check if coin already exists (completed = 0)
    #     exists = coinPair in fileAddcoinpair['Currency'].values
    #     if not exists:
    #         dfAdd = pd.DataFrame({'Currency': [coinPair],
    #                               'Completed' : [0],
    #                               'Date' : [str(date.today())]
    #                             })
    #         fileAddcoinpair = pd.concat([fileAddcoinpair, dfAdd], ignore_index = True, axis = 0)

    # try:
    #     fileAddcoinpair.to_csv('addcoinpair.csv', index=False)
    # except Exception as e:
    #     msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
    #     print(msg)
    #     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
    #------------------------

    # read addcoinpair file and calc BestEMA for each coin pair on 1d, 4h and 1h time frame and save on positions table
    add_symbol.main()    

else:

    # if there are no symbols in accumulation or bullish phase remove all not open from positions
    database.delete_all_positions_not_open(connection)

    # # if there are no coins in accumulation or bullish phase remove all from positions
    # for tf in positionsTimeframe: 
    #     try:
    #         filename = 'positions'+tf+'.csv'
    #         positionsfile = pd.read_csv(filename)

    #         positionsfile = positionsfile[(positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)]  
    #         positionsfile.to_csv(filename, index=False)
    #     except Exception as e:
    #         msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
    #         print(msg)
    #         logging.exception(msg)
    #         telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 
    #         continue


# # %%
# dfRecovery = dfResult.query("MarketPhase == 'recovery'")
# print("\nCoins in Recovery Market Phase")
# print(dfRecovery)

# dfAccumulation= dfResult.query("MarketPhase == 'accumulation'")
# print("\nCoins in Accumulation Market Phase")
# print(dfAccumulation)

# dfBullish = dfResult.query("MarketPhase == 'bullish'")
# print("\nCoins in Bullish Market Phase")
# print(dfBullish)

# dfWarning = dfResult.query("MarketPhase == 'warning'")
# print("\nCoins in Warning Market Phase")
# print(dfWarning)

# dfDistribution = dfResult.query("MarketPhase == 'distribution'")
# print("\nCoins in Distribution Market Phase")
# print(dfDistribution)

# dfBearish = dfResult.query("MarketPhase == 'bearish'")
# print("\nCoins in Bearish Market Phase")
# print(dfBearish)

# inform that ended
telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eStop, "Market Phases - End")

# calculate execution time
stop = timeit.default_timer()
total_seconds = stop - start

duration = database.duration(total_seconds)

# days, remainder = divmod(total_seconds, 3600*24)
# hours, remainder = divmod(remainder, 3600)
# minutes, seconds = divmod(remainder, 60)

# # Creating a string that displays the time in the hms format
# time_format = ""
# if days > 0:
#     time_format += "{:2d}d ".format(int(days))
# if hours > 0 or (days > 0 and (minutes > 0 or seconds > 0)):
#     time_format += "{:2d}h ".format(int(hours))
# if minutes > 0 or (hours > 0 and seconds > 0) or (days > 0 and seconds > 0):
#     time_format += "{:2d}m ".format(int(minutes))
# if seconds > 0 or (days == 0 and hours == 0 and minutes == 0):
#     time_format += "{:2d}s".format(int(seconds))

msg = f'Execution Time: {duration}'
print(msg)
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)


