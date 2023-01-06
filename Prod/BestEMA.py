
"""
calculates best ema for the coinpair and time frame provided and store results on coinpairBestEma.csv
"""

import os
from binance.client import Client
import pandas as pd
import datetime
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import sys
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import time
from binance.exceptions import BinanceAPIException
import requests

# %%
# Binance API
api_key = os.environ.get('binance_api')
api_secret = os.environ.get('binance_secret')

# %%
client = Client(api_key, api_secret)

# backtest with 4 years of price data 
#-------------------------------------
today = date.today() 
# today - 4 years - 200 days
pastdate = today - relativedelta(years=4) - relativedelta(days=200)
# print(pastdate)
tuple = pastdate.timetuple()
timestamp = time.mktime(tuple)

startdate = str(timestamp)
# startdate = "15 Dec, 2018 UTC"
# startdate = "12 May, 2022 UTC"
# startdate = "4 year ago UTC"
# startdate = "10 day ago UTC"
#-------------------------------------
timeframe = ""

# telegram
telegramToken_errors = os.environ.get('telegramToken_errors')
telegram_chat_id = os.environ.get('telegram_chat_id')

# %%
def sendTelegramMessage(msg):
    
    # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
    # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
    lmsg = "<pre>"+msg+"</pre>"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML",
    }
    
    resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken_errors), params=params).json()

def EMA(values, n):
    """
    Return exp moving average of `values`, at
    each step taking into account `n` previous values.
    """
    return pd.Series(values).ewm(span=n, adjust=False).mean()

def SMA(values, n):
    """
    Return simple moving average of `values`, at
    each step taking into account `n` previous values.
    """
    return pd.Series(values).rolling(n).mean()

#-------------------------------------
# we will use 2 exponencial moving averages:
# BUY when fast ema > slow ema
# Close position when slow ema > fast ema  
#-------------------------------------
class EmaCross(Strategy):
    n1 = 7
    n2 = 8
    nFastSMA = 50
    nSlowSMA = 200    
    
    def init(self):
        self.emaFast = self.I(EMA, self.data.Close, self.n1)
        self.emaSlow = self.I(EMA, self.data.Close, self.n2)
        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)

    def next(self):
        fastEMA = self.emaFast
        slowEMA = self.emaSlow
        SMA50 = self.sma50
        SMA200 = self.sma200
        priceClose = self.data.Close
        
        # accumulationPhase = False
        # bullishPhase = False

        accumulationPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        bullishPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)

        if not self.position:
            if (accumulationPhase or bullishPhase) and crossover(fastEMA, slowEMA):
            # if crossover(fastEMA, slowEMA):
                self.buy()
        
        else: 
            if crossover(slowEMA, fastEMA): 
                self.position.close()
            


# %%
def getdata(pSymbol, pTimeframe):
    frame = pd.DataFrame(client.get_historical_klines(pSymbol,
                                                      pTimeframe,
                                                      startdate
                                                      ))
    
    frame = frame.iloc[:,:6] # use the first 5 columns
    frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
    frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
    frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
    # frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
    frame = frame.set_index(pd.DatetimeIndex(frame['Time']))
    frame = frame.drop(['Time'], axis=1)

    return frame

# %%
def runBackTest(coinPair):

    coinOnly = coinPair[:-4]
    coinStable = coinPair[-4:]

    # print("coinPair = ",coinPair)
    # df = getdata(coinPair, timeframe)

    # get historical data from BUSD and USDT and use the one with more data 
    dfStable1 = pd.DataFrame()
    dfStable2 = pd.DataFrame()

    iniBUSD = 0
    iniUSDT = 0
    
    try:
        dfStableBUSD = getdata(coinOnly+"BUSD", timeframe)
    except BinanceAPIException as e:
        msg = "BinanceAPIException - " + str(e.status_code) + " - " + e.message+ " - " +coinOnly+"BUSD"
        print(msg)
        sendTelegramMessage(msg)
    
    if not dfStableBUSD.empty:
        ini1 = dfStableBUSD.index[0]

    try:
        dfStableUSDT = getdata(coinOnly+"USDT", timeframe) 
    except BinanceAPIException as e:
        msg = "BinanceAPIException - " + str(e.status_code) + " - " + e.message + " - " +coinOnly+"USDT"
        print(msg)
        sendTelegramMessage(msg)

    if not dfStableUSDT.empty:
        ini2 = dfStableUSDT.index[0]

    # get start date and use the older one
    if dfStableBUSD.empty and dfStableUSDT.empty:
        # print("Both wrong")
        return
    elif dfStableBUSD.empty and not dfStableUSDT.empty:
        # print("choose ini2")
        df = dfStableUSDT.copy()
    elif not dfStableBUSD.empty and dfStableUSDT.empty:
        # print("choose ini1")
        df = dfStableBUSD.copy()
    elif ini1 > ini2:
        # USDT has more history
        print("USDT pair has more history data")
        df = dfStableUSDT.copy()
    else:
        # BUSD has more history
        print("BUSD pair has more history data")
        df = dfStableBUSD.copy()

    # df = df.drop(['Time'], axis=1)
    # print(df)

    bt = Backtest(df, EmaCross, cash=100000, commission=0.001)
    stats = bt.run()
    # print(stats)
    # bt.plot()

    stats, heatmap = bt.optimize(
    n1=range(5, 100, 5),
    n2=range(10, 200, 5),
    constraint=lambda param: param.n1 < param.n2,
    maximize='Equity Final [$]',
    return_heatmap=True
    )

    dfbema = pd.DataFrame(heatmap.sort_values().iloc[-1:])
    n1 = dfbema.index.get_level_values(0)[0]
    n2 = dfbema.index.get_level_values(1)[0]
    returnPerc = round(stats['Return [%]'],2)
    BuyHoldReturnPerc = round(stats['Buy & Hold Return [%]'],2)
    BacktestStartDate = str(df.index[0])

    # lista
    print("n1=",n1)
    print("n2=",n2)
    print("Return [%] = ",round(returnPerc,2))
    print("Buy & Hold Return [%] = ",round(BuyHoldReturnPerc,2))
    print("Backtest start date =", BacktestStartDate)

    coinpairBestEma = pd.read_csv('coinpairBestEma.csv')
    # coinpairBestEma
    # add to file coinpair Best Ema 
    # if exist then update else add
    linha = coinpairBestEma.index[(coinpairBestEma.coinPair == coinPair) & (coinpairBestEma.timeFrame == timeframe)].to_list()

    if not linha:
        # print("There is no line in coinpairBestEma file with coinPair "+str(coinPair)+ " and timeframe "+str(timeframe)+". New line will be added.")
        # add line
        coinpairBestEma.loc[len(coinpairBestEma.index)] = [coinPair, 
                                                            n1,
                                                            n2,
                                                            timeframe,
                                                            returnPerc,
                                                            BuyHoldReturnPerc,
                                                            BacktestStartDate
                                                            ]
    else:
        # print("linha=",linha[0])
        # update line
        coinpairBestEma.loc[linha[0],['fastEMA','slowEMA','returnPerc','BuyHoldReturnPerc','BacktestStartDate']] = [n1, n2, returnPerc,BuyHoldReturnPerc,BacktestStartDate]

    # coinpairBestEma
    # print("Saving Coin Pair to coinpairBestEma file")

    #order by coinpair and timeframe
    coinpairBestEma.sort_values(by=['coinPair','timeFrame'], inplace=True)
    coinpairBestEma.to_csv('coinpairBestEma.csv', index=False, header=True)


def addcoinpair(coinPair, lTimeframe):

    result = False
    
    global timeframe 
    timeframe = str(lTimeframe)

    print("Backtest - "+coinPair+" - "+timeframe+" - Start")
    runBackTest(coinPair)
    print("Backtest "+coinPair+" - "+timeframe+" - End")

    result = True
    return result



