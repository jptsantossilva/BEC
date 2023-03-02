"""
Gets all coin pairs from Binance, calculate market phase for each and store results in coinpairByMarketPhase_USD_1d.csv 
Removes coins from positions files that are not top performers in the accumulation or bullish phase.
Adds the coins in the accumulation or bullish phase to addCoinPair.csv and calc BestEMA 
for each coin pair on 1d,4h,1h time frame and save on positions files
"""

# %%
import os
from binance.client import Client
import requests
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np
import sys
import timeit
import addCoinPair
import telegram
import logging
import yaml

# calculate program run time
start = timeit.default_timer() 

# inform start
telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eStart, "Market Phases - Start")

# log file to store error messages
log_filename = "coinpairByMarketPhase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

# get settings from config file
try:
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    trade_top_performance = config["trade_top_performance"]

except FileNotFoundError as e:
    msg = "Error: The file config.yaml could not be found."
    msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
    sys.exit(msg) 

except yaml.YAMLError as e:
    msg = "Error: There was an issue with the YAML file."
    msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
    sys.exit(msg) 

# Binance
# environment variables
try:
    # Binance
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')

except KeyError as e: 
    msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)

# telegram
# telegramToken_MarketPhases = os.environ.get('telegramToken_MarketPhases')
# telegram_chat_id = os.environ.get('telegram_chat_id')

# Binance Client
try:
    client = Client(api_key, api_secret)
except Exception as e:
        msg = "Error connecting to Binance. "+ repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        sys.exit(msg) 

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
dfBlacklist = pd.read_csv('blacklist.csv')
dfBlacklist['Currency'] = dfBlacklist['Currency'].astype(str)+trade_against
# put the blacklist in a set
blacklist = set(dfBlacklist["Currency"].unique())

try:
    exchange_info = client.get_exchange_info()
except Exception as e:
        msg = "Error connecting to Binance. "+ repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        sys.exit(msg) 
        
coinPairs = set()

for s in exchange_info['symbols']:
    if (s['symbol'].endswith(trade_against)
        and not(s['symbol'].endswith('DOWN'+trade_against))
        and not(s['symbol'].endswith('UP'+trade_against))
        and not(s['symbol'] == "AUD"+trade_against) # Australian Dollar
        and not(s['symbol'] == "EUR"+trade_against) # Euro
        and not(s['symbol'] == "GBP"+trade_against) # British pound
        and s['status'] == 'TRADING'):
            coinPairs.add(s['symbol'])

# from the coinPairs to trade, exclude coins from Blacklist
coinPairs -= blacklist

coinPairs = sorted(coinPairs)
msg = str(len(coinPairs))+" symbols found. Calculating..."
print(msg)
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)

def applytechnicals(df):
        
        df['50DSMA'] = df['Close'].rolling(50).mean()
        df['200DSMA'] = df['Close'].rolling(200).mean()

        df['perc_above_50DSMA'] = ((df['Close']-df['50DSMA'])/df['50DSMA'])*100
        df['perc_above_200DSMA'] = ((df['Close']-df['200DSMA'])/df['200DSMA'])*100        

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
        frame.columns = ['Time','Close'] #rename columns
        frame[['Close']] = frame[['Close']].astype(float) #cast to float
        # frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        frame['Coinpair'] = Symbol
        frame.index = [datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        
        frame = frame[['Coinpair','Close']]
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

for coinPair in coinPairs:

    print("calculating "+coinPair)
    df = getdata(coinPair)
    applytechnicals(df)
    # last one is the one with 200dsma value
    df = df.tail(1)

    if dfResult.empty:
        dfResult = df
    else:
        dfResult = pd.concat([dfResult, df])

# Coins in accumulation and Bullish phases
conditions = [
    (dfResult['Close'] > dfResult['50DSMA']) & (dfResult['Close'] < dfResult['200DSMA']) & (dfResult['50DSMA'] < dfResult['200DSMA']), # recovery phase
    (dfResult['Close'] > dfResult['50DSMA']) & (dfResult['Close'] > dfResult['200DSMA']) & (dfResult['50DSMA'] < dfResult['200DSMA']), # accumulation phase
    (dfResult['Close'] > dfResult['50DSMA']) & (dfResult['Close'] > dfResult['200DSMA']) & (dfResult['50DSMA'] > dfResult['200DSMA']), # bullish phase
    (dfResult['Close'] < dfResult['50DSMA']) & (dfResult['Close'] > dfResult['200DSMA']) & (dfResult['50DSMA'] > dfResult['200DSMA']), # warning phase
    (dfResult['Close'] < dfResult['50DSMA']) & (dfResult['Close'] < dfResult['200DSMA']) & (dfResult['50DSMA'] > dfResult['200DSMA']), # distribution phase
    (dfResult['Close'] < dfResult['50DSMA']) & (dfResult['Close'] < dfResult['200DSMA']) & (dfResult['50DSMA'] < dfResult['200DSMA'])  # bearish phase
]
# set marketphase to each coin
values = ['recovery', 'accumulation', 'bullish', 'warning','distribution','bearish']
dfResult['MarketPhase'] = np.select(conditions, values)
# print(dfResult)

# currentDate = date.today().strftime('%Y%m%d')
# dfResult.to_csv("coinPairByMarketPhase/coinpairByMarketPhase_"+trade_against+"_"+timeframe+"_"+currentDate+".csv")

dfBullish = dfResult.query("MarketPhase == 'bullish'")
dfAccumulation= dfResult.query("MarketPhase == 'accumulation'")

# union accumulation and bullish results
dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)

df_top = dfUnion.sort_values(by=['perc_above_200DSMA'], ascending=False)
df_top = df_top.head(trade_top_performance)

# set rank for highest strength
df_top['performance_rank'] = np.arange(len(df_top))+1

df_top.to_csv("coinpairByMarketPhase_"+trade_against+"_"+timeframe+".csv", index=False)

selected_columns = df_top[["Coinpair","Close","MarketPhase",'performance_rank']]
df_top_print = selected_columns.copy()
df_top_print.rename(columns={"Coinpair": "Symbol", "Close": "Price", "performance_rank": "BestPerf" }, inplace=True)

telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"Top {str(trade_top_performance)} performance coins:")
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", df_top_print.to_string(index=False))

# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfBullish.to_string(index=False))
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfBullish))} in bullish phase")
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfAccumulation.to_string(index=False))
# telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfAccumulation))} in accumulation phase")

positionsTimeframe = ["1d", "4h", "1h"]

if not df_top.empty:

    #------------------------
    # remove coins from position files that are not top perfomers in accumulation or bullish phase
    #------------------------
    top_coins = df_top.Coinpair.to_list()

    for tf in positionsTimeframe: 
        positionsfile = pd.read_csv('positions'+tf+'.csv')

        filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
        filter2 = positionsfile['Currency'].isin(top_coins)
        positionsfile = positionsfile[filter1 | filter2]  
        
        positionsfile.to_csv('positions'+tf+'.csv', index=False)
    #------------------------

    #------------------------
    # add top rank coins with positive returns to positions files
    #------------------------
    df_best_ema = pd.read_csv('coinpairBestEma.csv')
    for tf in positionsTimeframe: 
        for coinPair in top_coins:
            
            # read position file
            df_pos = pd.read_csv('positions'+tf+'.csv')

            # check if coin is already in position file
            exists = coinPair in df_pos['Currency'].values
            if not exists:
                
                # get return percentage
                values = df_best_ema.loc[(df_best_ema['coinPair'] == coinPair) & (df_best_ema['timeFrame'] == tf), ['returnPerc']].values
                if len(values) > 0:
                    return_perc = values[0][0]
                    
                    #if return percentage > 0 add coin to positions file
                    if return_perc > 0:            
                        df_add = pd.DataFrame({'Currency': [coinPair],
                                                'performance_rank': [0],
                                                'position': [0],
                                                'quantity': [0],
                                                'buyPrice': [0],
                                                'currentPrice': [0],
                                                'PnLperc': [0]})
                        df_pos = pd.concat([df_pos, df_add], ignore_index = True, axis = 0)

                        try:
                            df_pos.to_csv('positions'+tf+'.csv', index=False)
                        except Exception as e:
                            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                            print(msg)
                            # telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
    #------------------------

    #------------------------
    # add coin pairs top performers in accumulation or bullish phase to calc best ema if not exist 
    #------------------------
    try:
        filename = 'addcoinpair.csv'
        fileAddcoinpair = pd.read_csv(filename)
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 

    # remove the coins that are not anymore top performers on the accumulation or bullish phases 
    # and next time the coin goes into these phases will calc again the best ema

    # keep only coins with calc not completed. Now I want to make sure best ema is calculated everyday
    filter1 = fileAddcoinpair['Completed'] == 0 
    # filter2 = fileAddcoinpair['Currency'].isin(top_coins)
    # fileAddcoinpair = fileAddcoinpair[filter1 | filter2]  
    fileAddcoinpair = fileAddcoinpair[filter1
                                      ]  
    # add coin pairs
    for coinPair in df_top.Coinpair:
        # check if coin already exists (completed = 0)
        exists = coinPair in fileAddcoinpair['Currency'].values
        if not exists:
            dfAdd = pd.DataFrame({'Currency': [coinPair],
                                  'Completed' : [0],
                                  'Date' : [str(date.today())]
                                })
            fileAddcoinpair = pd.concat([fileAddcoinpair, dfAdd], ignore_index = True, axis = 0)

    try:
        fileAddcoinpair.to_csv('addcoinpair.csv', index=False)
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
    #------------------------

    # read addcoinpair file and calc BestEMA for each coin pair on 1d,4h,1h time frame and save on positions files
    addCoinPair.main()    

else:
    # if there are no coins in accumulation or bullish phase remove all from positions
    for tf in positionsTimeframe: 
        try:
            filename = 'positions'+tf+'.csv'
            positionsfile = pd.read_csv(filename)

            positionsfile = positionsfile[(positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)]  
            positionsfile.to_csv(filename, index=False)
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 
            continue


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

stop = timeit.default_timer()
msg = "Execution Time (s): "+str(round(stop - start,1))
print(msg) 
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)


