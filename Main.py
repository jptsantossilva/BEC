"""
Read position file, get coin pairs in position and check if is time to sell, get coin pairs not in position and check if its time to buy.  

Gets all coin pairs from Binance, calculate market phase for each and store results in coinpairByMarketPhase_USD_1d.csv 
Removes coins from positions files that are not in the accumulation or bullish phase.
Adds the coins in the accumulation or bullish phase to addCoinPair.csv and calc BestEMA 
for each coin pair on 1d,4h,1h time frame and save on positions files
"""


import os
import re
from xml.dom import ValidationErr
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance import BinanceSocketManager
from binance.helpers import round_step_size
import requests
from datetime import datetime
import time
import sys
import math
import numpy as np
#import dataframe_image as dfi
from numbers import Number
from typing import Sequence
from backtesting.lib import crossover
import logging
import telegram
import timeit
import yaml
from addCoinPair import get_performance_rank

# calculate program run time
start = timeit.default_timer()

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

# Check the program has been called with the timeframe
# total arguments
n = len(sys.argv)
# print("Total arguments passed:", n)
if n < 2:
    print("Argument is missing")
    timeframe = input('Enter timeframe (1d, 4h or 1h):')
    runMode = input('Enter run mode (test, prod):')
else:
    # argv[0] in Python is always the name of the script.
    timeframe = sys.argv[1]

    # run modes 
    # test - does not execute orders on the exchange
    # prod - execute orders on the exchange
    runMode = sys.argv[2]

if timeframe == "1h":
    gTimeFrameNum = int("1")
    gtimeframeTypeShort = "h" # h, d
    gtimeframeTypeLong = "hour" # hour, day

    telegramToken = telegram.telegramToken_1h

elif timeframe == "4h":
    gTimeFrameNum = int("4")
    gtimeframeTypeShort = "h" # h, d
    gtimeframeTypeLong = "hour" # hour, day

    telegramToken = telegram.telegramToken_4h

elif timeframe == "1d":
    gTimeFrameNum = int("1")
    gtimeframeTypeShort = "d" # h, d
    gtimeframeTypeLong = "day" # hour, day

    telegramToken = telegram.telegramToken_1d

# inform that started
telegram.send_telegram_message(telegramToken, telegram.eStart, "Binance Trader Bot - Start")

# get settings from config file
try:
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    stake_amount_type               = config["stake_amount_type"]
    max_number_of_open_positions    = config["max_number_of_open_positions"]
    tradable_balance_ratio          = config["tradable_balance_ratio"]
    min_position_size               = config["min_position_size"]
    trade_against                   = config["trade_against"]

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

# environment variables
try:
    # Binance
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')

except KeyError as e: 
    msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
    sys.exit(msg) 

# constants

# strategy
# gTimeframe = client.KLINE_INTERVAL_1HOUR # "1h"
gFastMA = int("8")
gSlowMA = int("34")
gStrategyName = str(gFastMA)+"/"+str(gSlowMA)+" EMA CROSS"

# create empty dataframes
df_positions = pd.DataFrame()
df_orders    = pd.DataFrame()
df_best_ema  = pd.DataFrame()

def read_csv_files():

    global df_positions
    global df_orders
    global df_best_ema

    try:
        # read positions
        # make sure performance rank is fulfilled
        set_performance_rank()

        filename = 'positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
        df_positions = pd.read_csv(filename)

        # read orders csv
        # we just want the header, there is no need to get all the existing orders.
        # at the end we will append the orders to the csv
        filename = 'orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
        df_orders = pd.read_csv(filename, nrows=0)

        # read best ema cross
        filename = 'coinpairBestEma.csv'
        df_best_ema = pd.read_csv(filename)

    except FileNotFoundError as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)         
    except PermissionError as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg) 
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg) 

# Binance Client
try:
    client = Client(api_key, api_secret)
except Exception as e:
        msg = "Error connecting to Binance. "+ repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        sys.exit(msg) 

def get_num_open_positions():
    try:
        # df_open_positions_1h = pd.read_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv')
        df_open_positions_1h = pd.read_csv('positions1h.csv')
        df_open_positions_1h = df_open_positions_1h[df_open_positions_1h.position == 1].Currency

        df_open_positions_4h = pd.read_csv('positions4h.csv')
        df_open_positions_4h = df_open_positions_4h[df_open_positions_4h.position == 1].Currency

        df_open_positions_1d = pd.read_csv('positions1d.csv')
        df_open_positions_1d = df_open_positions_1d[df_open_positions_1d.position == 1].Currency

        total_open_positions = len(df_open_positions_1h) +len(df_open_positions_4h) +len(df_open_positions_1d)
        return total_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        return -1
    
def calc_stake_amount(coin):
    if stake_amount_type == "unlimited":
        num_open_positions = get_num_open_positions()

        # if error occurred
        if num_open_positions == -1:
            return 0
        if num_open_positions >= max_number_of_open_positions:
            return -2 

        try:
            balance = float(client.get_asset_balance(asset=coin)['free'])
            
        except BinanceAPIException as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
    
        tradable_balance = balance*tradable_balance_ratio 
        stake_amount = tradable_balance/(max_number_of_open_positions-num_open_positions)
        
        # make sure the size is >= the minimum size
        if stake_amount < min_position_size:
            stake_amount = min_position_size

        # make sure there are enough funds otherwise abort the buy position
        if balance < stake_amount:
            stake_amount = 0

        return stake_amount
    elif int(stake_amount_type) >= 0:
        return stake_amount_type
    else:
        return 0
    

def get_data(coinPair, aTimeframeNum, aTimeframeTypeShort, aFastMA=0, aSlowMA=0):

    try:
        # update EMAs from the best EMA return ratio
        global gFastMA
        global gSlowMA
        global gStrategyName

        lTimeFrame = str(aTimeframeNum)+aTimeframeTypeShort
        if aTimeframeTypeShort == "h":
            lTimeframeTypeLong = "hour"
        elif aTimeframeTypeShort == "d":
            lTimeframeTypeLong = "day"
        
        if aSlowMA > 0 and aFastMA > 0:
            gFastMA = aFastMA
            gSlowMA = aSlowMA
        else:
            listEMAvalues = df_best_ema[(df_best_ema.coinPair == coinPair) & (df_best_ema.timeFrame == lTimeFrame)]

            if not listEMAvalues.empty:
                gFastMA = int(listEMAvalues.fastEMA.values[0])
                gSlowMA = int(listEMAvalues.slowEMA.values[0])
            else:
                gFastMA = int("0")
                gSlowMA = int("0")

        gStrategyName = str(gFastMA)+"/"+str(gSlowMA)+" EMA cross"

        # if bestEMA does not exist return empty dataframe in order to no use that trading pair
        if gFastMA == 0:
            frame = pd.DataFrame()
            return frame
        
        # if best Ema exist get price data 
        # lstartDate = str(1+gSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC"
        sma200 = 200
        lstartDate = str(sma200*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
        ltimeframe = str(aTimeframeNum)+aTimeframeTypeShort
        frame = pd.DataFrame(client.get_historical_klines(coinPair
                                                        ,ltimeframe
    
                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        # ,lstartDate
                                                        
                                                        ))

        frame = frame[[0,4]]
        frame.columns = ['Time','Close']
        frame.Close = frame.Close.astype(float)
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        return frame
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+coinPair+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        frame = pd.DataFrame()
        return frame 

#-----------------------------------------------------------------------
# calculates moving averages 
#-----------------------------------------------------------------------
def apply_technicals(df, aFastMA, aSlowMA):
    
    if aFastMA > 0: 
        df['FastEMA'] = df['Close'].ewm(span=aFastMA, adjust=False).mean()
        df['SlowEMA'] = df['Close'].ewm(span=aSlowMA, adjust=False).mean()
        df['SMA50'] = df['Close'].rolling(50).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# Update positions files 
#-----------------------------------------------------------------------
def change_pos(dfPos, curr, order, typePos, buyPrice=0, currentPrice=0):

    # type = buy, sell or updatePnL

    try:

        if typePos == "buy":
            dfPos.loc[dfPos['Currency'] == curr, ['position','quantity','buyPrice']] = [1,float(order['executedQty']),float(buyPrice)]
        elif typePos == "sell":
            dfPos.loc[dfPos['Currency'] == curr, ['position','quantity','buyPrice','currentPrice','PnLperc']] = [0,0,0,0,0]
        elif typePos == "updatePnL":
            pos = dfPos.loc[dfPos['Currency'] == curr]
            if len(pos) > 0:
                lBuyPrice = pos['buyPrice'].values[0]
                if not math.isnan(lBuyPrice) and (lBuyPrice > 0):
                    PnLperc = ((currentPrice-lBuyPrice)/lBuyPrice)*100
                    PnLperc = round(PnLperc, 2)
                    dfPos.loc[dfPos['Currency'] == curr, ['currentPrice','PnLperc']] = [currentPrice,PnLperc]

        dfPos.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        
#-----------------------------------------------------------------------

# Make sure we are only trying to buy positions on coins included on market phases file.  
# this is needed here specially for 1h/4h time frames when coin is no longer on bullish or accumulation and a close position occurred
# and we dont want to back in position during the same day
def remove_coins_position():
    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv

    dfAllByMarketPhase = pd.read_csv(f'coinpairByMarketPhase_{trade_against}_1d.csv')
    # dfBullish = dfAllByMarketPhase.query("MarketPhase == 'bullish'")
    # dfAccumulation= dfAllByMarketPhase.query("MarketPhase == 'accumulation'")
    # # union accumulation and bullish results
    # dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)
    # accuBullishCoinPairs = dfUnion.Coinpair.to_list()

    positionsfile = pd.read_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv')

    filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
    filter2 = positionsfile['Currency'].isin(dfAllByMarketPhase['Coinpair'])
    positionsfile = positionsfile[filter1 | filter2]

    # order by name
    positionsfile.sort_values(by=['Currency'], inplace=True)

    positionsfile.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

# %%
def adjust_size(coin, amount):

    # sendTelegramMessage("", "adjust size")
    
    for filt in client.get_symbol_info(coin)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            stepSize = float(filt['stepSize'])
            minQty = float(filt['minQty'])
            break

    order_quantity = round_step_size(amount, stepSize)
    return order_quantity


def calc_pnl(symbol, sellprice: float, sellqty: float):
    try:

        # open orders file to search last buy order for the coin and time frame provided on the argument.
        with open(r"orders"+str(gTimeFrameNum)+gtimeframeTypeShort+".csv", 'r') as fp:
            for l_no, line in reversed(list(enumerate(fp))):
                # search string
                if (symbol in line) and ("BUY" in line):

                    # print('string found in a file')
                    # print('Line Number:', l_no)
                    # print('Line:', line)
                    
                    # sellprice = 300
                    orderid = line.split(',')[0]
                    # print('orderid:', orderid)
                    buyprice = float(line.split(',')[4])
                    # print('Buy Price:', buyprice)
                    buyqty = float(line.split(',')[5])
                    # print('Buy qty:', buyqty)
                    # print('Sell price:', sellprice)
                    # sellqty = buyqty
                    # PnLperc = ((sellprice-buyprice)/buyprice)*100
                    PnLperc = (((sellprice*sellqty)-(buyprice*buyqty))/(buyprice*buyqty))*100
                    PnLperc = round(PnLperc, 2)
                    PnLvalue = (sellprice*sellqty)-(buyprice*buyqty) # erro!
                    PnLValue = round(PnLvalue, 2)
                    # print('Buy USD =', round(buyprice*buyqty,2))
                    # print('Sell USD =', round(sellprice*sellqty,2))
                    # print('PnL% =', PnLperc)
                    # print('PnL USD =', PnLvalue)
                    
                    
                    lista = [orderid, PnLperc, PnLvalue]
                    return lista
                    
                    # terminate the loop
                    break

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        return []

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        return -1


# get performance rank values from coinpairByMarketPhase and set to position file
def set_performance_rank():

    filename = 'positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
    df_pos = pd.read_csv(filename)  

    filename = f'coinpairByMarketPhase_{trade_against}_1d.csv'
    df_mp = pd.read_csv(filename)
    
    df_merged = df_pos.merge(df_mp, left_on='Currency', right_on='Coinpair', how='left')
    df_pos['performance_rank'] = df_merged['performance_rank_y']

    # those that dont have performance rank number will set rank num to 1000 to make sure that they are at the end of the list
    df_pos['performance_rank'].fillna(1000, inplace=True)

    df_pos.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

def trader():

    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv
    remove_coins_position()

    # read position, orders and bestEma files
    read_csv_files()

    # sort positions by performance rank
    df_positions.sort_values(by=['performance_rank'], inplace=True)

    # list of coins in position - SELL
    list_to_sell = df_positions[df_positions.position == 1].Currency
    
    # list of coins in position - BUY
    list_to_buy = df_positions[df_positions.position == 0].Currency
    
    # ------------------------------------------------------------
    # check open positions and SELL if conditions are fulfilled 
    # ------------------------------------------------------------
    for coinPair in list_to_sell:
        # sendTelegramMessage("",coinPair) 
        df = get_data(coinPair, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            msg = f'{coinPair} - {gStrategyName} - Best EMA values missing'
            print(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            continue

        apply_technicals(df, gFastMA, gSlowMA)
        lastrow = df.iloc[-1]

        # separate coin from stable. example coinPair=BTCUSDT coinOnly=BTC coinStable=USDT
        if coinPair.endswith("BTC"):
            coinOnly = coinPair[:-3]
            coinStable = coinPair[-3:]
        elif coinPair.endswith(("BUSD","USDT")):    
            coinOnly = coinPair[:-4]
            coinStable = coinPair[-4:]

        if lastrow.SlowEMA > lastrow.FastEMA:
        # if crossover(df.SlowEMA, df.FastEMA): 
            try:
                balanceQty = float(client.get_asset_balance(asset=coinOnly)['free'])  
            except BinanceAPIException as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                continue
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                continue            

            buyOrderQty = float(df_positions[df_positions.Currency == coinPair].quantity.values[0])
            sellQty = buyOrderQty
            if balanceQty < buyOrderQty:
                sellQty = balanceQty
            sellQty = adjust_size(coinPair, sellQty)

            if sellQty > 0: 
                
                try:        
                    if runMode == "prod":
                        order = client.create_order(symbol=coinPair,
                                                side=client.SIDE_SELL,
                                                type=client.ORDER_TYPE_MARKET,
                                                quantity = sellQty
                                                )
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)

                        # update position file with the sell order
                        # changepos(df_positions, coinPair,'',buy=False)
                        change_pos(df_positions, coinPair, '', typePos="sell")

                except BinanceAPIException as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                except BinanceOrderException as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                except Exception as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)

                #add new row to end of DataFrame
                if runMode == "prod":
                    addPnL = calc_pnl(coinPair, float(avg_price), float(order['executedQty']))
                    if addPnL:
                        df_orders.loc[len(df_orders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
                                                            order['side'], avg_price, order['executedQty'],
                                                            addPnL[0], # buyorderid 
                                                            addPnL[1], # PnL%
                                                            addPnL[2]  # PnL USD
                                                            ]
                

                        # print(order)
                
                        if addPnL[2] > 0: 
                            # trade with profit
                            telegram.send_telegram_alert(telegramToken, telegram.eTradeWithProfit,
                                        # order['transactTime']
                                        pd.to_datetime(order['transactTime'], unit='ms'), 
                                        order['symbol'], 
                                        str(gTimeFrameNum)+gtimeframeTypeShort, 
                                        gStrategyName,
                                        order['side'],
                                        avg_price,
                                        order['executedQty'],
                                        avg_price*float(order['executedQty']),
                                        addPnL[1], # PnL%
                                        addPnL[2]  # PnL USD
                                        )
                        else:
                            # trade with loss
                            telegram.send_telegram_alert(telegramToken, telegram.eTradeWithLoss,
                                        # order['transactTime']
                                        pd.to_datetime(order['transactTime'], unit='ms'), 
                                        order['symbol'], 
                                        str(gTimeFrameNum)+gtimeframeTypeShort, 
                                        gStrategyName,
                                        order['side'],
                                        avg_price,
                                        order['executedQty'],
                                        avg_price*float(order['executedQty']),
                                        addPnL[1], # PnL%
                                        addPnL[2]  # PnL USD
                                        )

                        
            else:
                if runMode == "prod":
                    # if there is no qty on balance to sell we set the qty on positions file to zero
                    # this can happen if we sell on the exchange (for example, due to a pump) before the bot sells it. 
                    # changepos(df_positions, coinPair,'',buy=False)
                    change_pos(df_positions, coinPair, '', typePos="sell")
        else:
            msg = f'{coinPair} - {gStrategyName} - Sell condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)
            
            # set current PnL
            lastrow = df.iloc[-1]
            currentPrice = lastrow.Close
            # changepos(df_positions, coinPair,'',buy=False)
            change_pos(df_positions, coinPair, '', typePos="updatePnL", currentPrice=currentPrice)


    # ------------------------------------------------------------------
    # check coins not in positions and BUY if conditions are fulfilled
    # ------------------------------------------------------------------
    for coinPair in list_to_buy:
        # sendTelegramMessage("",coinPair) 
        df = get_data(coinPair, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            msg = f'{coinPair} - {gStrategyName} - Best EMA values missing'
            print(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            continue

        apply_technicals(df, gFastMA, gSlowMA)
        lastrow = df.iloc[-1]

        # separate coin from stable. example coinPair=BTCUSDT coinOnly=BTC coinStable=USDT 
        if coinPair.endswith("BTC"):
            coinOnly = coinPair[:-3]
            coinStable = coinPair[-3:]
        elif coinPair.endswith(("BUSD","USDT")):    
            coinOnly = coinPair[:-4]
            coinStable = coinPair[-4:]

        # if we wanna be more agressive we can use the following approach:
        # since the coin pair by marketphase is already choosing the coins in bullish and accumulation phase on daily time frame 
        # we can pass the verification of those market phases in lower timeframes, 4h and 1h, otherwise we will loose some oportunities
        # to be more conservative = use the same approach as the backtesting and keep those market phase verification in lower timeframes
        accumulationPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
        bullishPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)
        
        if (accumulationPhase or bullishPhase) and crossover(df.FastEMA, df.SlowEMA):
        # if crossover(df.FastEMA, df.SlowEMA):
            positionSize = calc_stake_amount(coin=coinStable)
            # sendTelegramMessage("", "calc position size 5")
            # print("positionSize: ", positionSize)
            # sendTelegramMessage('',client.SIDE_BUY+" "+coinPair+" BuyStableQty="+str(positionSize))  
            if positionSize > 0:
                
                if runMode == "prod":
                    try:
                        order = client.create_order(symbol=coinPair,
                                                    side=client.SIDE_BUY,
                                                    type=client.ORDER_TYPE_MARKET,
                                                    quoteOrderQty = positionSize,
                                                    newOrderRespType = 'FULL') 
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)
                        # print('avg_price=',avg_price)

                        # update positions file with the buy order
                        # changepos(df_positions, coinPair,order,buy=True,buyPrice=avg_price)
                        change_pos(df_positions, coinPair, order, typePos="buy", buyPrice=avg_price)
                    
                    except BinanceAPIException as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                    except BinanceOrderException as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                    except Exception as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                
                #add new row to end of DataFrame
                if runMode == "prod":
                    df_orders.loc[len(df_orders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
                                                        order['side'], avg_price, order['executedQty'],
                                                        0,0,0]
                            
                    
                    telegram.send_telegram_alert(telegramToken, telegram.eEnterTrade,
                                    # order['transactTime'], 
                                    pd.to_datetime(order['transactTime'], unit='ms'),
                                    order['symbol'], 
                                    str(gTimeFrameNum)+gtimeframeTypeShort, 
                                    gStrategyName,
                                    order['side'],
                                    avg_price,
                                    order['executedQty'],
                                    positionSize)
            
            elif positionSize == -2:
                num_open_positions = get_num_open_positions()
                telegram.send_telegram_message(telegramToken, telegram.eInformation, client.SIDE_BUY+" "+coinPair+" - Max open positions ("+str(num_open_positions)+"/"+str(max_number_of_open_positions)+") already occupied!")
            else:
                telegram.send_telegram_message(telegramToken, telegram.eInformation, client.SIDE_BUY+" "+coinPair+" - Not enough "+coinStable+" funds!")
                
        else:
            msg = f'{coinPair} - {gStrategyName} - Buy condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)


def main():

    trader()

    # add orders to csv file
    df_orders.to_csv('orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', mode='a', index=False, header=False)
     
    # positions summary
    df_current_positions = get_open_positions(df_positions)
    df_cp_to_print = df_current_positions.drop(columns=['position','performance_rank'])
    df_cp_to_print.sort_values(by=['Currency'], inplace=True)
    df_cp_to_print.rename(columns={"Currency": "Symbol", "Close": "Price", }, inplace=True)
    df_cp_to_print.reset_index(drop=True, inplace=True) # gives consecutive numbers to each row
    if df_cp_to_print.empty:
        print("Result: no open positions yet")
        telegram.send_telegram_message(telegramToken, "", "Result: no open positions")
    else:
        print(df_cp_to_print)
        telegram.send_telegram_message(telegramToken, "", df_cp_to_print.to_string())

    if stake_amount_type == "unlimited":
        num_open_positions = get_num_open_positions()
        msg = f"{str(num_open_positions)}/{str(max_number_of_open_positions)} positions occupied"
        print(msg)
        telegram.send_telegram_message(telegramToken, "", msg=msg)

    # dfi.export(posframe, 'balance.png', fontsize=8, table_conversion='matplotlib')
    # sendTelegramPhoto() 

    # inform that ended
    telegram.send_telegram_message(telegramToken, telegram.eStop, "Binance Trader Bot - End")

    stop = timeit.default_timer()
    msg = 'Execution Time (s): '+str(round(stop - start,1))
    print(msg)
    telegram.send_telegram_message(telegramToken, "", msg)

if __name__ == "__main__":
    main()



