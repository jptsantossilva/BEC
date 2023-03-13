
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
import telegram
import logging
import timeit

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

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

# Binance Client
try:
    client = Client(api_key, api_secret)
except Exception as e:
    msg = "Error connecting to Binance. "+ repr(e)
    print(msg)
    logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
    sys.exit(msg) 

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
# SELL when slow ema > fast ema  
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
    try:
        frame = pd.DataFrame(client.get_historical_klines(pSymbol
                                                        ,pTimeframe

                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        ,startdate
                                                        ))
        
        frame = frame.iloc[:,:6] # use the first 5 columns
        frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        # frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        frame = frame.set_index(pd.DatetimeIndex(frame['Time']))
        frame = frame.drop(['Time'], axis=1)

        return frame
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+pSymbol+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        frame = pd.DataFrame()
        return frame 

def runBackTest(coin_pair):
    
    if coin_pair.endswith("BTC"):
        coinOnly = coin_pair[:-3]
        coinStable = coin_pair[-3:]
    elif coin_pair.endswith(("BUSD","USDT")):    
        coinOnly = coin_pair[:-4]
        coinStable = coin_pair[-4:]

    # print("coinPair = ",coinPair)
    # df = getdata(coinPair, timeframe)

    if coin_pair.endswith("BTC"):
        df = getdata(coin_pair, timeframe)

    # get historical data from BUSD and USDT and use the one with more data 
    elif coin_pair.endswith(("BUSD","USDT")):
        dfStableBUSD = pd.DataFrame()
        dfStableUSDT = pd.DataFrame()
    
        iniBUSD = 0
        iniUSDT = 0
        
        dfStableBUSD = getdata(coinOnly+"BUSD", timeframe)
            
        if not dfStableBUSD.empty:
            ini1 = dfStableBUSD.index[0]

        dfStableUSDT = getdata(coinOnly+"USDT", timeframe) 

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
            print("USDT pair has more historical data")
            df = dfStableUSDT.copy()
        else:
            # BUSD has more history
            print("BUSD pair has more historical data")
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

    try:
        filename = 'coinpairBestEma.csv'
        coinpairBestEma = pd.read_csv(filename)
        # coinpairBestEma
        # add to file coinpair Best Ema 
        # if exist then update else add
        linha = coinpairBestEma.index[(coinpairBestEma.coinPair == coin_pair) & (coinpairBestEma.timeFrame == timeframe)].to_list()

        if not linha:
            # print("There is no line in coinpairBestEma file with coinPair "+str(coinPair)+ " and timeframe "+str(timeframe)+". New line will be added.")
            # add line
            coinpairBestEma.loc[len(coinpairBestEma.index)] = [coin_pair, 
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
        coinpairBestEma.to_csv(filename, index=False, header=True)

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)


def addcoinpair(coinPair, lTimeframe):

    result = False
    
    global timeframe 
    timeframe = str(lTimeframe)

    try:
        # calculate program run time
        start = timeit.default_timer()
        
        print("")
        print("Backtest - "+coinPair+" - "+timeframe+" - Start")
        runBackTest(coinPair)
        print("Backtest "+coinPair+" - "+timeframe+" - End")
        
        stop = timeit.default_timer()
        msg = 'Execution Time (s): '+str(round(stop - start,1))
        print(msg)
        
        result = True
        return result
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        
        return False
    



