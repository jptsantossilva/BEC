"""
Gets all coin pairs from Binance, calculate market phase for each and store results in coinpairByMarketPhase_USD_1d.csv 
Removes coins from positions files that are not in the accumulation or bullish phase.
Adds the coins in the accumulation or bullish phase to addCoinPair.csv and calc BestEMA 
for each coin pair on 1d,4h,1h time frame and save on positions files
"""

# %%
import os
from binance.client import Client
import requests
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import numpy as np
import sys
import timeit
import addCoinPair
import telegram
import logging

# %%
# %%

# calculate program run time
start = timeit.default_timer() 

# inform start
telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eStart, "Market Phases - Start")

# log file to store error messages
log_filename = "coinpairByMarketPhase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

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
    stablecoin = input('Trade against USDT, BUSD or BTC:')
else:
    # argv[0] in Python is always the name of the script.
    timeframe = sys.argv[1]
    stablecoin = sys.argv[2]

if timeframe == "1d": startdate = "200 day ago UTC"
elif timeframe == "8h": startdate = str(8*200)+" hour ago UTC"
elif timeframe == "4h": startdate = str(4*200)+" hour ago UTC"

# read coins in blacklist to not trade
dfBlacklist = pd.read_csv('blacklist.csv')
dfBlacklist['Currency'] = dfBlacklist['Currency'].astype(str)+stablecoin
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
    if (s['symbol'].endswith(stablecoin)
        and not(s['symbol'].endswith('DOWN'+stablecoin))
        and not(s['symbol'].endswith('UP'+stablecoin))
        and not(s['symbol'] == "AUD"+stablecoin) # Australian Dollar
        and not(s['symbol'] == "EUR"+stablecoin) # Euro
        and not(s['symbol'] == "GBP"+stablecoin) # British pound
        and s['status'] == 'TRADING'):
            coinPairs.add(s['symbol'])

# from the coinPairs to trade, exclude coins from Blacklist
coinPairs -= blacklist

coinPairs = sorted(coinPairs)
msg = str(len(coinPairs))+" coins found. Calculating..."
print(msg)
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", msg)

def applytechnicals(df):
        df['50DSMA'] = df['Close'].rolling(50).mean()
        df['200DSMA'] = df['Close'].rolling(200).mean()

def getdata(Symbol):
    try:
        frame = pd.DataFrame(client.get_historical_klines(Symbol,
                                                        timeframe,                                        
                                                        startdate))
        
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
        frame = pd.DataFrame()
        return frame 

# %%
dfResult = pd.DataFrame()
# i = 0

for coinPair in coinPairs:
    # if coinPair == "VGXUSDT":
    #     print(dfResult)
    #     break    
    
    # i = i+1
    # if i == 2:
    #     break

    print("calculating "+coinPair)
    # df = pd.DataFrame()
    # print(len(df.index))
    df = getdata(coinPair)
    applytechnicals(df)
    df.dropna(inplace=True)

    if dfResult.empty:
        dfResult = df
    else:
        dfResult = pd.concat([dfResult, df])
    
    # print(dfResult)

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
# dfResult.to_csv("coinPairByMarketPhase/coinpairByMarketPhase_"+stablecoin+"_"+timeframe+"_"+currentDate+".csv")

# add coin pairs in accumulation or bullish phase to addcoinpair file
dfBullish = dfResult.query("MarketPhase == 'bullish'")
dfAccumulation= dfResult.query("MarketPhase == 'accumulation'")
# union accumulation and bullish results
dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)
dfUnion.to_csv("coinpairByMarketPhase_"+stablecoin+"_"+timeframe+".csv")

telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfBullish.to_string(index=False))
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfBullish))} in bullish phase")
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", dfAccumulation.to_string(index=False))
telegram.send_telegram_message(telegram.telegramToken_market_phases, "", f"{str(len(dfAccumulation))} in accumulation phase")

positionsTimeframe = ["1d", "4h", "1h"]

if not dfUnion.empty:

    # remove coin pairs from position files not in accumulation or bullish phase
    accuBullishCoinPairs = dfUnion.Coinpair.to_list()

    for tf in positionsTimeframe: 
        positionsfile = pd.read_csv('positions'+tf+'.csv')

        filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
        filter2 = positionsfile['Currency'].isin(accuBullishCoinPairs)
        positionsfile = positionsfile[filter1 | filter2]  
        
        positionsfile.to_csv('positions'+tf+'.csv', index=False)
    #------------------------


    # add coin pairs in accumulation or bullish phase
    try:
        filename = 'addcoinpair.csv'
        fileAddcoinpair = pd.read_csv(filename)
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 

    # remove the coins that are not anymore on the accumulation or bullish phases 
    # and next time the coin goes into these phases will calc again the best ema
    filter1 = fileAddcoinpair['Completed'] == 0
    filter2 = fileAddcoinpair['Currency'].isin(accuBullishCoinPairs)
    fileAddcoinpair = fileAddcoinpair[filter1 | filter2]  

    # add coin pairs
    for coinPair in dfUnion.Coinpair:
        # line = fileAddcoinpair.index[(fileAddcoinpair['Currency'] == coinPair)].to_list()
        exists = coinPair in fileAddcoinpair['Currency'].values
        if not exists:
            dfAdd = pd.DataFrame({'Currency': [coinPair],
                                    'Completed' : [0],
                                    'Date' : [str(date.today())]})
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
telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eStop, msg)


