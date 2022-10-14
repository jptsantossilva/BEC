# %%
import os
import pandas as pd
from binance.client import Client
import requests
from datetime import datetime
import sys

# %%
# features list
""" 
Telegram notifications - enter trade, 
                         exit trade, 
                         current balance, 
                         pnl balance since begining and last 30 days,
                         error ocurred,
                         Bollinger bands width < X value
                         RSI em multi time frames
                         ver os alertas do TradrPro do the chart guys
                         ver que moedas satisfazem a estrategia do top 100
                         https://www.blockchaincenter.net/en/trending-coins/
                         get top 100 with price close > DSMA200
"""

# %%
# constants

# coins to trade
symbols = ['BTCBUSD','ETHBUSD','BNBBUSD','SOLBUSD','MATICBUSD','FTTBUSD']

# strategy
timeframe = "1h"

# percentage of balance to open position for each trade - example 0.1 = 10%
tradepercentage = float("0.002")
# risk percentage per trade - example 0.01 = 1%
risk = float("0.01")

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
    lmsg = emoji+" "+msg
    url = f"https://api.telegram.org/bot{telegramToken}/sendMessage?chat_id={telegram_chat_id}&text={lmsg}"
    requests.get(url).json() # this sends the message

def sendTelegramAlert(emoji, date, coin, timeframe, strategy, ordertype, value, amount):
    lmsg = emoji + " " + date + " - " + coin + " - " + strategy + " - " + timeframe + " - " + ordertype + " - " + "Value: " + value + " - " + "Amount: " + amount
    url = f"https://api.telegram.org/bot{telegramToken}/sendMessage?chat_id={telegram_chat_id}&text={lmsg}"
    requests.get(url).json() # this sends the message

# %%
# create initial csv with positions
# posframe = pd.DataFrame(symbols)
# posframe.columns = ['Currency']
# posframe['position'] = 0
# posframe['quantity'] = 0
# posframe.to_csv('positioncheck', index=False)

# read positions csv
posframe = pd.read_csv('positioncheck')
# posframe

# read orders csv
# we just want the header, there is no need to get all the existing orders.
# at the end we will append the orders to the csv
# 
dforders = pd.read_csv('orders', nrows=0)
# dforders



# %%
# environment variables
try:
    api_key = os.environ.get('binance_api')
    # print("api_key: ", api_key)
    api_secret = os.environ.get('binance_secret')
    telegramToken = os.environ.get('telegramToken') 
    telegram_chat_id = os.environ.get('telegram_chat_id')
except KeyError: 
    sendTelegramMessage(eWarning,"Environment variable does not exist")
    print("Environment variable does not exist")

# %%
client = Client(api_key, api_secret)


# %%
# def testTelegramMessages():
    # sendTelegramMessage(eInformation," Environment variable does not exist")
# testTelegramMessages()

# %%
#criar csv das orders
""" orders = client.get_all_orders(symbol='BTCBUSD', limit=1)
dforders = pd.DataFrame(orders)
# colunas a manter
col_keep = ['symbol','price','executedQty','side','time']
dforders = dforders[col_keep]
dforders.time = pd.to_datetime(dforders.time, unit='ms')
dforders.to_csv('orders.csv', mode='a', index=False, header=False) """

# %%
def calcPositionSize():

    # get balance from BUSD
    stableBalance = client.get_asset_balance(asset='BUSD')['free']
    stableBalance = float(stableBalance)
    # print(stableBalance)

    # calculate position size based on the percentage per trade
    positionSize = stableBalance*tradepercentage 
    
    # positionAmount = 10
    return positionSize

# %%
def gethourlydata(symbol):
    from_time = int(datetime.strptime("2022-07-17", "%Y-%m-%d").timestamp()*1000)
    to_time = int(datetime.strptime("2022-07-18", "%Y-%m-%d").timestamp()*1000)
    frame = pd.DataFrame(client.get_historical_klines(symbol,
                                                        timeframe,
                                                        start_str=from_time, end_str=to_time, limit=1000)
                                                        # '200 hour ago UTC'))
    frame
    frame = frame[[0,4]]
    frame.columns = ['Time','Close']
    frame.Close = frame.Close.astype(float)
    frame.Time = pd.to_datetime(frame.Time, unit='ms')
    return frame

# %%
def applytechnicals(df):
    df['FastSMA'] = df.Close.rolling(50).mean()
    df['SlowSMA'] = df.Close.rolling(200).mean()

# %%
def changepos(curr, order, buy=True):
    if buy:
        posframe.loc[posframe.Currency == curr, 'position'] = 1
        posframe.loc[posframe.Currency == curr, 'quantity'] = float(order['executedQty'])
    else:
        posframe.loc[posframe.Currency == curr, 'position'] = 0
        posframe.loc[posframe.Currency == curr, 'quantity'] = 0

    posframe.to_csv('positioncheck', index=False)


# %%
def trader():

    # check open positions and SELL if conditions are fulfilled 
    for coin in posframe[posframe.position == 1].Currency:
        df = gethourlydata(coin)
        applytechnicals(df)
        lastrow = df.iloc[-1]
        if lastrow.SlowSMA > lastrow.FastSMA:
            order = client.create_order(symbol=coin,
                                        side='SELL',
                                        type='MARKET',
                                        quantity = posframe[posframe.Currency == coin].quantity.values[0])
            changepos(coin,order,buy=False)
            
            #add new row to end of DataFrame
            dforders.loc[len(dforders.index)] = [coin, order['price'], order['executedQty'], order['side'], order['transactTime']]
            
            print(order)
            sendTelegramMessage(eExitTrade, order)
            sendTelegramAlert(eExitTrade,
                            order['transactTime'], 
                            order['symbol'], 
                            timeframe, 
                            "SMA 50-200 CROSS",
                            order['side'],
                            order['price'],
                            order['executedQty'])

    # check coins not in positions and BUY if conditions are fulfilled
    for coin in posframe[posframe.position == 0].Currency:
        df = gethourlydata(coin)
        applytechnicals(df)
        lastrow = df.iloc[-1]
        if lastrow.FastSMA > lastrow.SlowSMA:
            positionSize = calcPositionSize()
            # print("positionSize: ", positionSize)
            order = client.create_order(symbol=coin,
                                        side='BUY',
                                        type='MARKET',
                                        quoteOrderQty = positionSize)
            changepos(coin,order,buy=True)
            
            #add new row to end of DataFrame
            dforders.loc[len(dforders.index)] = [coin, order['price'], order['executedQty'], order['side'], order['transactTime']]
                      
            print(order)
            sendTelegramMessage(eEnterTrade, order)
            sendTelegramAlert(eEnterTrade,
                            order['transactTime'], 
                            order['symbol'], 
                            timeframe, 
                            "SMA 50-200 CROSS",
                            order['side'],
                            order['price'],
                            order['executedQty'])
        else:
            print(f'Buying condition for {coin} is not fulfilled')


# %%
try:
    # inform that is running
    # now = datetime.now()
    # dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
    sendTelegramMessage(eStart,"Binance Trader Bot - Started")

    trader()

    # add orders to csv file
    dforders.time = pd.to_datetime(dforders.time, unit='ms')
    dforders.to_csv('orders', mode='a', index=False, header=False)

    # inform that ended
    sendTelegramMessage(eStop, "Binance Trader Bot - Ended")
    
except:
    sendTelegramMessage(eWarning, "Oops! "+ str(sys.exc_info()[0])+ " occurred.")
    print("Oops!", sys.exc_info()[0], "occurred.")



