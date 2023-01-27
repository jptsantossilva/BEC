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

# %%
# environment variables
try:
    # Binance
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    telegram_chat_id = os.environ.get('telegram_chat_id')
except KeyError: 
    print("Environment variable does not exist")

# Binance Client
client = Client(api_key, api_secret)

# %%
# constants

# strategy
# gTimeframe = client.KLINE_INTERVAL_1HOUR # "1h"
gFastMA = int("8")
gSlowMA = int("34")
gStrategyName = str(gFastMA)+"/"+str(gSlowMA)+" CROSS"

# emoji
eStart   = u'\U000025B6'
eStop    = u'\U000023F9'
eWarning = u'\U000026A0'
eEnterTrade = u'\U0001F91E' #crossfingers
eExitTrade  = u'\U0001F91E' #crossfingers
eTradeWithProfit = u'\U0001F44D' # thumbs up
eTradeWithLoss   = u'\U0001F44E' # thumbs down
eInformation = u'\U00002139'

# run modes 
# test - does not execute orders on the exchange
# prod - execute orders on the exchange

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
    runMode = sys.argv[2]

if timeframe == "1h":
    gTimeFrameNum = int("1")
    gtimeframeTypeShort = "h" # h, d
    gtimeframeTypeLong = "hour" # hour, day
elif timeframe == "4h":
    gTimeFrameNum = int("4")
    gtimeframeTypeShort = "h" # h, d
    gtimeframeTypeLong = "hour" # hour, day
elif timeframe == "1d":
    gTimeFrameNum = int("1")
    gtimeframeTypeShort = "d" # h, d
    gtimeframeTypeLong = "day" # hour, day

# try:
#     if timeframe == "1h":
#         telegram_chat_id = os.environ.get('telegram_chat_id_1h')
#     elif timeframe == "4h":
#         telegram_chat_id = os.environ.get('telegram_chat_id_4h')
#     elif timeframe == "1d":
#         telegram_chat_id = os.environ.get('telegram_chat_id_1d')

#     telegram_chat_ClosedPositions = os.environ.get('telegram_chat_ClosedPositions')
# except KeyError: 
#     print("Environment variable does not exist - telegram_chat_id")


# Telegram
telegramToken = os.environ.get('telegramToken'+str(gTimeFrameNum)+gtimeframeTypeShort) 
telegramToken_ClosedPosition = os.environ.get('telegramToken_ClosedPositions') 

# stake_amount = amount of stake the bot will use for each trade 
# if stake_amount = "unlimited", this configuration will allow increasing/decreasing stakes depending on the performance
# of the bot (lower stake if the bot is losing, higher stakes if the bot has a winning record since higher balances are available), 
# and will result in profit compounding.
stake_amount_type = "unlimited"
# or if a static number is defined, that is the amount per trade
# stake_amount_type = 500

# tradable percentage of the balance
# for example: if you want to run 3 bot instances (1h, 4h and 1D), you can set the percentage of the total balance to be allocated to each of the bots.
tradable_balance_ratio = 1 # 1=100% ; 0.5=50%

# max number of open trades
# if tradable balance = 1000 and max_open_positions = 10, the stake_amount = 1000/10 = 100 
max_open_positions = 34

# minimum position size in usd
minPositionSize = float("20.0") 

# create empty dataframes
dfPositions = pd.DataFrame()
dfOrders = pd.DataFrame()
dfBestEMA = pd.DataFrame()

def read_csv_files():

    global dfPositions
    global dfOrders
    global dfBestEMA

    # read positions
    dfPositions = pd.read_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv')
    # posframe

    # read orders csv
    # we just want the header, there is no need to get all the existing orders.
    # at the end we will append the orders to the csv
    dfOrders = pd.read_csv('orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', nrows=0)

    # read best ema cross
    dfBestEMA = pd.read_csv('coinpairBestEma.csv')


# %%
def send_telegram_message(emoji, msg):

    if not emoji:
        lmsg = msg
    else:
        lmsg = emoji+" "+msg

    # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
    # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
    lmsg = "<pre>"+lmsg+"</pre>"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML",
    }
    
    try:
        resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken), params=params).json()
    except Exception as e:
        msg = "sendTelegramMessage - There was an error: "
        print(msg, e)
        # send_telegram_message(eWarning, msg+e)
        pass 


def send_telegram_alert(emoji, date, coin, timeframe, strategy, ordertype, unitValue, amount, USDValue, pnlPerc = '', pnlUSD = ''):
    lmsg = emoji + " " + str(date) + "\n" + coin + "\n" + strategy + "\n" + timeframe + "\n" + ordertype + "\n" + "UnitPrice: " + str(unitValue) + "\n" + "Qty: " + str(amount)+ "\n" + "USD: " + str(USDValue)
    if pnlPerc != '':
        lmsg = lmsg + "\n"+"PnL%: "+str(round(float(pnlPerc),2)) + "\n"+"PnL USD: "+str(round(float(pnlUSD),2))

    # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
    # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
    lmsg = "<pre>"+lmsg+"</pre>"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML",
    }
    
    resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken), params=params).json()

    # if is a closed position send also to telegram of closed positions
    if emoji in [eTradeWithProfit, eTradeWithLoss]:
        params = {
        "chat_id": telegram_chat_id,
        "text": lmsg,
        "parse_mode": "HTML",
        }
        resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken_ClosedPosition), params=params).json()

def send_telegram_photo(photoName='balance.png'):
    # get current dir
    cwd = os.getcwd()
    limg = cwd+"/"+photoName
    # print(limg)
    oimg = open(limg, 'rb')
    url = f"https://api.telegram.org/bot{telegramToken}/sendPhoto?chat_id={telegram_chat_id}"
    requests.post(url, files={'photo':oimg}) # this sends the message

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
        msg = "get_num_open_positions - There was an error: "
        print(msg, e)
        send_telegram_message(eWarning, msg+str(e))
        return -1
    
def calc_stake_amount(coin = 'BUSD'):
    if stake_amount_type == "unlimited":
        num_open_positions = get_num_open_positions()

        # if error occurred
        if num_open_positions == -1:
            return 0
        if num_open_positions >= max_open_positions:
            return -2 

        try:
            balance = float(client.get_asset_balance(asset=coin)['free'])
        except BinanceAPIException as e:
            msg = "calc_stake_amount - There was an error: "
            print(msg, e)
            send_telegram_message(eWarning, msg+str(e))
        except Exception as e:
            msg = "calc_stake_amount - There was an error: "
            print(msg, e)
            send_telegram_message(eWarning, msg+str(e))
    
        tradable_balance = balance*tradable_balance_ratio 
        stake_amount = int(tradable_balance/(max_open_positions-num_open_positions))
        
        # make sure the size is >= the minimum size
        if stake_amount < minPositionSize:
            stake_amount = minPositionSize

        # make sure there are enough funds otherwise abort the buy position
        if balance < stake_amount:
            stake_amount = 0

        return stake_amount
    elif int(stake_amount_type) >= 0:
        return stake_amount_type
    else:
        return 0

# def calcPositionSize(pStablecoin = 'BUSD'):

#     try:
        
#         # get balance from BUSD
#         stablecoin = client.get_asset_balance(asset=pStablecoin)
#         stablecoinBalance = float(stablecoin['free'])
#         # print(stableBalance)
#     except BinanceAPIException as e:
#         msg = "calcPositionSize - There was an error: "
#         print(msg, e)
#         sendTelegramMessage(eWarning, msg+e)
#     except Exception as e:
#         msg = "calcPositionSize - There was an error: "
#         print(msg, e)
#         sendTelegramMessage(eWarning, msg+e)
        
#     # calculate position size based on the percentage per trade
#     # resultado = stablecoinBalance*tradepercentage 
#     # resultado = round(resultado, 5)

#     if resultado < minPositionSize:
#         resultado = minPositionSize

#     # make sure there are enough funds otherwise abort the buy position
#     if stablecoinBalance < resultado:
#         resultado = 0

#     return resultado
    
# %%
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
            listEMAvalues = dfBestEMA[(dfBestEMA.coinPair == coinPair) & (dfBestEMA.timeFrame == lTimeFrame)]

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
        frame = pd.DataFrame(client.get_historical_klines(coinPair,
                                                        ltimeframe,
                                                        lstartDate))

        frame = frame[[0,4]]
        frame.columns = ['Time','Close']
        frame.Close = frame.Close.astype(float)
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        return frame
    except Exception as e:
        msg = f"getdata - {coinPair} -  There was an error: "
        print(msg, e)
        send_telegram_message(eWarning, msg+str(e))
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
        msg = "change_pos - There was an error: "
        print(msg, e)
        send_telegram_message(eWarning, msg+str(e))
        pass
#-----------------------------------------------------------------------

def remove_coins_position():
    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv
    
    dfAllByMarketPhase = pd.read_csv('coinpairByMarketPhase_BUSD_1d.csv')
    dfBullish = dfAllByMarketPhase.query("MarketPhase == 'bullish'")
    dfAccumulation= dfAllByMarketPhase.query("MarketPhase == 'accumulation'")
    # union accumulation and bullish results
    dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)
    accuBullishCoinPairs = dfUnion.Coinpair.to_list()

    positionsfile = pd.read_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv')

    filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
    filter2 = positionsfile['Currency'].isin(accuBullishCoinPairs)
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
        msg = "calc_pnl - There was an error: "
        print(msg, e)
        send_telegram_message(eWarning, msg+str(e))
        return []

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = "get_open_positions - There was an error: "
        print(msg, str(e))
        # sendTelegramMessage(eWarning, msg+e)
        return -1

# %%
def trader():

    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv
    remove_coins_position()

    # read position, orders and bestEma files
    read_csv_files()

    # list of coins in position - SELL
    listPosition1 = dfPositions[dfPositions.position == 1].Currency
    # list of coins in position - BUY
    listPosition0 = dfPositions[dfPositions.position == 0].Currency

    # ------------------------------------------------------------
    # check open positions and SELL if conditions are fulfilled 
    # ------------------------------------------------------------
    for coinPair in listPosition1:
        # sendTelegramMessage("",coinPair) 
        df = get_data(coinPair, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            print(f'{coinPair} - {gStrategyName} - Best EMA values missing')
            send_telegram_message(eWarning,f'{coinPair} - {gStrategyName} - Best EMA values missing')
            continue

        apply_technicals(df, gFastMA, gSlowMA)
        # lastrow = df.iloc[-1]

        # separate coin from stable. example coinPair=BTCUSDT coinOnly=BTC coinStable=USDT 
        coinOnly = coinPair[:-4]
        coinStable = coinPair[-4:]

        # if lastrow.SlowMA > lastrow.FastMA:
        if crossover(df.SlowEMA, df.FastEMA): 
            try:
                balanceQty = float(client.get_asset_balance(asset=coinOnly)['free'])  
            except BinanceAPIException as e:
                msg = "balanceQty - There was an error: "
                print(msg, e)
                send_telegram_message(eWarning, msg+str(e))
                continue
            except Exception as e:
                msg = "balanceQty - There was an error: "
                print(msg, e)
                send_telegram_message(eWarning, msg+str(e))
                continue            

            buyOrderQty = float(dfPositions[dfPositions.Currency == coinPair].quantity.values[0])
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
                        # changepos(dfPositions, coinPair,'',buy=False)
                        change_pos(dfPositions, coinPair, '', typePos="sell")

                except BinanceAPIException as e:
                    msg = "SELL create_order - There was an error: "
                    print(msg, e)
                    send_telegram_message(eWarning, msg+str(e))
                except BinanceOrderException as e:
                    msg = "SELL create_order - There was an error: "
                    print(msg, e)
                    send_telegram_message(eWarning, msg+str(e))
                except Exception as e:
                    msg = "SELL create_order - There was an error: "
                    print(msg, e)
                    send_telegram_message(eWarning, msg+str(e))

                #add new row to end of DataFrame
                if runMode == "prod":
                    addPnL = calc_pnl(coinPair, float(avg_price), float(order['executedQty']))
                    if addPnL:
                        dfOrders.loc[len(dfOrders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
                                                            order['side'], avg_price, order['executedQty'],
                                                            addPnL[0], # buyorderid 
                                                            addPnL[1], # PnL%
                                                            addPnL[2]  # PnL USD
                                                            ]
                

                        # print(order)
                        # sendTelegramMessage(eExitTrade, order)
                        if addPnL[2] > 0: 
                            emojiTradeResult = eTradeWithProfit
                        else:
                            emojiTradeResult = eTradeWithLoss

                        send_telegram_alert(emojiTradeResult,
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
                    # changepos(dfPositions, coinPair,'',buy=False)
                    change_pos(dfPositions, coinPair, '', typePos="sell")
        else:
            print(f'{coinPair} - {gStrategyName} - Sell condition not fulfilled')
            send_telegram_message("",f'{coinPair} - {gStrategyName} - Sell condition not fulfilled')
            
            # set current PnL
            lastrow = df.iloc[-1]
            currentPrice = lastrow.Close
            # changepos(dfPositions, coinPair,'',buy=False)
            change_pos(dfPositions, coinPair, '', typePos="updatePnL", currentPrice=currentPrice)


    # ------------------------------------------------------------------
    # check coins not in positions and BUY if conditions are fulfilled
    # ------------------------------------------------------------------
    for coinPair in listPosition0:
        # sendTelegramMessage("",coinPair) 
        df = get_data(coinPair, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            print(f'{coinPair} - {gStrategyName} - Best EMA values missing')
            send_telegram_message(eWarning,f'{coinPair} - {gStrategyName} - Best EMA values missing')
            continue

        apply_technicals(df, gFastMA, gSlowMA)
        lastrow = df.iloc[-1]

        # separate coin from stable. example coinPair=BTCUSDT coinOnly=BTC coinStable=USDT 
        coinOnly = coinPair[:-4]
        # print('coinOnly=',coinOnly)
        coinStable = coinPair[-4:]
        # print('coinStable=',coinStable)

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
                try:
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
                            # changepos(dfPositions, coinPair,order,buy=True,buyPrice=avg_price)
                            change_pos(dfPositions, coinPair, order, typePos="buy", buyPrice=avg_price)
                        
                        except BinanceAPIException as e:
                            msg = "BUY create_order - There was an error: "
                            print(msg, e)
                            send_telegram_message(eWarning, msg+str(e))
                        except BinanceOrderException as e:
                            msg = "BUY create_order - There was an error: "
                            print(msg, e)
                            send_telegram_message(eWarning, msg+str(e))
                        except Exception as e:
                            msg = "BUY create_order - There was an error: "
                            print(msg, e)
                            send_telegram_message(eWarning, msg+str(e))

                except BinanceAPIException as e:
                    send_telegram_message(eWarning, str(e))
                except BinanceOrderException as e:
                    send_telegram_message(eWarning, str(e))
                
                #add new row to end of DataFrame
                if runMode == "prod":
                    dfOrders.loc[len(dfOrders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
                                                        order['side'], avg_price, order['executedQty'],
                                                        0,0,0]
                            
                    
                    send_telegram_alert(eEnterTrade,
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
                send_telegram_message(eWarning,client.SIDE_BUY+" "+coinPair+" - Max open positions ("+str(num_open_positions)+"/"+str(max_open_positions)+") already occupied!")
            else:
                send_telegram_message(eWarning,client.SIDE_BUY+" "+coinPair+" - Not enough "+coinStable+" funds!")
                
        else:
            print(f'{coinPair} - {gStrategyName} - Buy condition not fulfilled')
            send_telegram_message("",f'{coinPair} - {gStrategyName} - Buy condition not fulfilled')

    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv
    # this is needed here specially for 1h/4h time frames when coin is no longer on bullish or accumulation and a close position occurred
    # and we dont want to back in position during the same day
    remove_coins_position()


def main():
    # inform that is running
    send_telegram_message(eStart,"Binance Trader Bot - Start")

    try:
        trader()
    except Exception as e:
        msg = "Trader - There was an error: "
        print(msg, e)
        send_telegram_message(eWarning, msg+str(e))
        pass


    # add orders to csv file
    dfOrders.to_csv('orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', mode='a', index=False, header=False)


    # posframe.drop('position', axis=1, inplace=True)
    # posframe.style.applymap(custom_style)
     
    # positions summary
    df_current_positions = get_open_positions(dfPositions)
    if df_current_positions.empty:
        print("Result: no open positions yet")
        send_telegram_message("","Result: no open positions")
    else:
        print(df_current_positions)
        send_telegram_message("",df_current_positions.to_string())

    # dfi.export(posframe, 'balance.png', fontsize=8, table_conversion='matplotlib')
    # sendTelegramPhoto()

    # inform that ended
    send_telegram_message(eStop, "Binance Trader Bot - End")

if __name__ == "__main__":
    main()



