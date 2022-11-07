# %%
import os
import re
from turtle import left
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
import dataframe_image as dfi
from numbers import Number
from typing import Sequence

# %%
# environment variables
try:
    # Binance
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    
    # Telegram
    telegramToken = os.environ.get('telegramToken') 
    telegram_chat_id = os.environ.get('telegram_chat_id')
except KeyError: 
    print("Environment variable does not exist")

# Binance Client
client = Client(api_key, api_secret)

# %%
# constants

# positionscheck file example
# Currency,position,quantity
# BTCBUSD,0,0.0

# strategy
# gTimeframe = client.KLINE_INTERVAL_1HOUR # "1h"
gFastMA = int("8")
gSlowMA = int("34")
gTimeFrameNum = int("1")
gtimeframeTypeShort = "h" # h, D
gtimeframeTypeLong = "hour" # hour, day
gStrategyName = str(gFastMA)+"/"+str(gSlowMA)+" CROSS"

# percentage of balance to open position for each trade - example 0.1 = 10%
tradepercentage = float("0.05") #0.2%
minPositionSize = float("15.0") # minimum position size in usd
# risk percentage per trade - example 0.01 = 1%
risk = float("0.01")

# Telegram
url = f"https://api.telegram.org/bot{telegramToken}/getUpdates"
# print(requests.get(url).json())

# emoji
eStart   = u'\U000025B6'
eStop    = u'\U000023F9'
eWarning = u'\U000026A0'
eEnterTrade = u'\U0001F91E' #crossfingers
eExitTrade  = u'\U0001F91E' #crossfingers
eTradeWithProfit = u'\U0001F44D' # thumbs up
eTradeWithLoss   = u'\U0001F44E' # thumbs down
eInformation = u'\U00002139'


# %%
def sendTelegramMessage(emoji, msg):
    if not emoji:
        lmsg = msg
    else:
        lmsg = emoji+" "+msg
    url = f"https://api.telegram.org/bot{telegramToken}/sendMessage?chat_id={telegram_chat_id}&text={lmsg}"
    requests.get(url).json() # this sends the message

def sendTelegramAlert(emoji, date, coin, timeframe, strategy, ordertype, unitValue, amount, USDValue, pnlPerc = '', pnlUSD = ''):
    lmsg = emoji + " " + str(date) + "\n" + coin + "\n" + strategy + "\n" + timeframe + "\n" + ordertype + "\n" + "UnitPrice: " + str(unitValue) + "\n" + "Qty: " + str(amount)+ "\n" + "USD: " + str(USDValue)
    if pnlPerc != '':
        lmsg = lmsg + "\n"+"PnL%: "+str(round(float(pnlPerc),2)) + "\n"+"PnL USD: "+str(round(float(pnlUSD),2))
    
    url = f"https://api.telegram.org/bot{telegramToken}/sendMessage?chat_id={telegram_chat_id}&text={lmsg}"
    requests.get(url).json() # this sends the message

def sendTelegramPhoto(photoName='balance.png'):
    # get current dir
    cwd = os.getcwd()
    limg = cwd+"/"+photoName
    # print(limg)
    oimg = open(limg, 'rb')
    url = f"https://api.telegram.org/bot{telegramToken}/sendPhoto?chat_id={telegram_chat_id}"
    requests.post(url, files={'photo':oimg}) # this sends the message

# %%
# read positions csv
posframe = pd.read_csv('positioncheck')
# posframe

# Todo
# get top 10 relative to BTC or USD strongest coins and trade those.
# the coins must have liquidity- choose from the top50 or 100 max from the marketcap rank 
# Add them automatically to positioncheck.
# remove the coins that are not so strong = not in an uptrend. example below 4H/1D 200MA
# 

# read orders csv
# we just want the header, there is no need to get all the existing orders.
# at the end we will append the orders to the csv
# sendTelegramMessage("", "read orders csv")
dforders = pd.read_csv('orders', nrows=0)
# dforders

# best ema cross return
dfBestEMA = pd.read_csv('coinpairBestEma')

# %%
# Not working properly yet
def spot_balance():
        sum_btc = 0.0
        balances = client.get_account()
        for _balance in balances["balances"]:
            asset = _balance["asset"]
            if True: #float(_balance["free"]) != 0.0 or float(_balance["locked"]) != 0.0:
                try:
                    btc_quantity = float(_balance["free"]) + float(_balance["locked"])
                    if asset == "BTC":
                        sum_btc += btc_quantity
                    else:
                        _price = client.get_symbol_ticker(symbol=asset + "BTC")
                        sum_btc += btc_quantity * float(_price["price"])
                except:
                    pass

        current_btc_price_USD = client.get_symbol_ticker(symbol="BTCUSDT")["price"]
        own_usd = sum_btc * float(current_btc_price_USD)
        print(" * Spot => %.8f BTC == " % sum_btc, end="")
        print("%.8f USDT" % own_usd)
# spot_balance()

# %%
def calcPositionSize(pStablecoin = 'BUSD'):
    # sendTelegramMessage("", "calc position size")

    try:
        
        # get balance from BUSD
        stablecoin = client.get_asset_balance(asset=pStablecoin)
        stablecoin = float(stablecoin['free'])
        # print(stableBalance)

        # calculate position size based on the percentage per trade
        resultado = stablecoin*tradepercentage 
        resultado = round(resultado, 5)

        if resultado < minPositionSize:
            resultado = minPositionSize


        return resultado
    except BinanceAPIException as e:
        sendTelegramMessage(eWarning, e)
    
    

# %%
def getdata(coinPair):

    lstartDate = str(1+gSlowMA*gTimeFrameNum)+" "+gtimeframeTypeLong+" ago UTC" 
    ltimeframe = str(gTimeFrameNum)+gtimeframeTypeShort
    frame = pd.DataFrame(client.get_historical_klines(coinPair,
                                                    ltimeframe,
                                                    lstartDate))

    frame = frame[[0,4]]
    frame.columns = ['Time','Close']
    frame.Close = frame.Close.astype(float)
    frame.Time = pd.to_datetime(frame.Time, unit='ms')
    return frame

# %%
def applytechnicals(df, coinPair):
    
    # update EMAs from the best EMA return ratio
    global gFastMA
    global gSlowMA
    global gStrategyName

    lTimeFrame = str(gTimeFrameNum)+gtimeframeTypeShort
    
    listEMAvalues = dfBestEMA[(dfBestEMA.coinPair == coinPair) & (dfBestEMA.timeFrame == lTimeFrame)]
    
    if not listEMAvalues.empty:
        gFastMA = listEMAvalues.fastEMA.values[0]
        gSlowMA = listEMAvalues.slowEMA.values[0]
        

    gStrategyName = str(gFastMA)+"/"+str(gSlowMA)+" EMA cross"
    

    df['FastMA'] = df['Close'].ewm(span=gFastMA, adjust=False).mean()
    df['SlowMA'] = df['Close'].ewm(span=gSlowMA, adjust=False).mean()


# %%
def changepos(curr, order, buy=True):
    # sendTelegramMessage("", "change pos")
    if buy:
        posframe.loc[posframe.Currency == curr, 'position'] = 1
        posframe.loc[posframe.Currency == curr, 'quantity'] = float(order['executedQty'])
    else:
        posframe.loc[posframe.Currency == curr, 'position'] = 0
        posframe.loc[posframe.Currency == curr, 'quantity'] = 0

    posframe.to_csv('positioncheck', index=False)


# %%
def adjustSize(coin, amount):

    # sendTelegramMessage("", "adjust size")
    
    for filt in client.get_symbol_info(coin)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            stepSize = float(filt['stepSize'])
            minQty = float(filt['minQty'])
            break

    order_quantity = round_step_size(amount, stepSize)
    return order_quantity


def calcPnL(symbol, sellprice: float, sellqty: float):
    with open(r"orders", 'r') as fp:
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
                PnLperc = ((sellprice-buyprice)/buyprice)*100
                PnLperc = round(PnLperc, 2)
                PnLvalue = (sellprice*sellqty)-(buyprice*buyqty) # erro!
                PnLValue = round(PnLvalue, 2)
                # print('Buy USD =', round(buyprice*buyqty,2))
                # print('Sell USD =', round(sellprice*sellqty,2))
                # print('PnL% =', PnLperc)
                # print('PnL USD =', PnLvalue)
                # don't look for next lines
                
                lista = [orderid, PnLperc, PnLvalue]
                return lista
                
                break

# %%
def trader():
    # sendTelegramMessage("", "trader")

    listPosition1 = posframe[posframe.position == 1].Currency
    listPosition0 = posframe[posframe.position == 0].Currency

    # check open positions and SELL if conditions are fulfilled 
    for coinPair in listPosition1:
        # sendTelegramMessage("",coinPair) 
        df = getdata(coinPair)
        applytechnicals(df, coinPair)
        lastrow = df.iloc[-1]

        # separate coin from stable. example coinPair=BTCUSDT coinOnly=BTC coinStable=USDT 
        coinOnly = coinPair[:-4]
        # print('coinOnly=',coinOnly)
        coinStable = coinPair[-4:]
        # print('coinStable=',coinStable)

        if lastrow.SlowMA > lastrow.FastMA:
            # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair)
            # print('coinStable=',coinStable) 
            # was not selling because the buy order amount is <> from the balance => fees were applied and we get less than the buy order
            # thats why we need to get the current balance 
            # sendTelegramMessage("",client.SIDE_SELL+" coinOnly:"+coinOnly) 
            # balanceQty = client.get_asset_balance(asset=coinOnly)['free']
            try:
                balanceQty = float(client.get_asset_balance(asset=coinOnly)['free'])  
            except BinanceAPIException as ea:
                sendTelegramMessage(eWarning, ea)

            # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair+" balanceQty:"+str(balanceQty))

            # print("balanceQty: ",balanceQty)
            buyOrderQty = float(posframe[posframe.Currency == coinPair].quantity.values[0])
            # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair+" buyOrderQty:"+str(buyOrderQty))
            # print("buyOrderQty: ",buyOrderQty)
            sellQty = buyOrderQty
            # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair+" sellQty: "+str(sellQty))
            if balanceQty < buyOrderQty:
                sellQty = balanceQty
                # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair+" sellQty:"+str(sellQty))
            sellQty = adjustSize(coinPair, sellQty)
            # sendTelegramMessage("",client.SIDE_SELL+" "+coinPair+" sellQty="+str(sellQty))
            if sellQty > 0: 
                
                try:        
                    order = client.create_order(symbol=coinPair,
                                            side=client.SIDE_SELL,
                                            type=client.ORDER_TYPE_MARKET,
                                            # quantity = posframe[posframe.Currency == coinPair].quantity.values[0]
                                            quantity = sellQty
                                            )
                    
                    fills = order['fills']
                    avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                    avg_price = round(avg_price,8)
                    # print('avg_price=',avg_price)

                    changepos(coinPair,order,buy=False)
                except BinanceAPIException as ea:
                    sendTelegramMessage(eWarning, ea)
                except BinanceOrderException as eo:
                    sendTelegramMessage(eWarning, eo)

                #add new row to end of DataFrame
                addPnL = calcPnL(coinPair, float(avg_price), float(order['executedQty']))
                dforders.loc[len(dforders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
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

                sendTelegramAlert(emojiTradeResult,
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
                changepos(coinPair,'',buy=False)
        else:
            print(f'{coinPair} - Sell condition not fulfilled')
            sendTelegramMessage("",f'{coinPair} - Sell condition not fulfilled')

    # check coins not in positions and BUY if conditions are fulfilled
    for coinPair in listPosition0:
        # sendTelegramMessage("",coinPair) 
        df = getdata(coinPair)
        applytechnicals(df, coinPair)
        lastrow = df.iloc[-1]
        
        if (lastrow.Close > lastrow.FastMA) and (lastrow.FastMA > lastrow.SlowMA):
            positionSize = calcPositionSize(pStablecoin=coinStable)
            # sendTelegramMessage("", "calc position size 5")
            # print("positionSize: ", positionSize)
            # sendTelegramMessage('',client.SIDE_BUY+" "+coinPair+" BuyStableQty="+str(positionSize))  
            if positionSize > 0:
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

                    changepos(coinPair,order,buy=True)
                except BinanceAPIException as ea:
                    sendTelegramMessage(eWarning, ea)
                except BinanceOrderException as eo:
                    sendTelegramMessage(eWarning, eo)
                
                #add new row to end of DataFrame
                dforders.loc[len(dforders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), coinPair, 
                                                    order['side'], avg_price, order['executedQty'],
                                                    0,0,0]
                        
                
                sendTelegramAlert(eEnterTrade,
                                # order['transactTime'], 
                                pd.to_datetime(order['transactTime'], unit='ms'),
                                order['symbol'], 
                                str(gTimeFrameNum)+gtimeframeTypeShort, 
                                gStrategyName,
                                order['side'],
                                avg_price,
                                order['executedQty'],
                                positionSize)
            else:
                sendTelegramMessage(eWarning,client.SIDE_BUY+" "+coinPair+" - Not enough "+coinStable+" funds!")
                
        else:
            print(f'{coinPair} - Buy condition not fulfilled')
            sendTelegramMessage("",f'{coinPair} - Buy condition not fulfilled')


def main():
    # inform that is running
    sendTelegramMessage(eStart,"Binance Trader Bot - Start")

    trader()

    # add orders to csv file
    dforders.to_csv('orders', mode='a', index=False, header=False)


    # posframe.drop('position', axis=1, inplace=True)
    # posframe.style.applymap(custom_style)
     
    # send balance
    print(posframe)

    sendTelegramMessage("",posframe.to_string())

    # dfi.export(posframe, 'balance.png', fontsize=8, table_conversion='matplotlib')
    # sendTelegramPhoto()

    # Todo
    # set orders filled buy value
    # get orders where price = 0 
    # get filled price
    # set price and save to csv file

    # inform that ended
    sendTelegramMessage(eStop, "Binance Trader Bot - End")

main()


