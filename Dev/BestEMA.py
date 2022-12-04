"""
calculates best ema for the coinpair and time frame provided and store results on coinpairBestEma.csv
"""

import os
from binance.client import Client
import pandas as pd
import datetime as dt
from backtesting import Backtest
from backtesting import Strategy
import sys
 

# %%
# Binance API
api_key = os.environ.get('binance_api')
api_secret = os.environ.get('binance_secret')


# %%
client = Client(api_key, api_secret)



# startdate = "10 Nov, 2018 UTC"
# startdate = "12 May, 2022 UTC"
startdate = "4 year ago UTC"
# startdate = "10 day ago UTC"
timeframe = "1h"



def EMA(values, n):
    """
    Return exp moving average of `values`, at
    each step taking into account `n` previous values.
    """
    
    return pd.Series(values).ewm(span=n, adjust=False).mean()


# %%



# we will use 2 exponencial moving averages:
# BUY when fast ema > slow ema
# Close position when slow ema > fast ema  
class EmaCross(Strategy):
    n1 = 8
    n2 = 34

    
    def init(self):
        
        self.ma1 = self.I(EMA, self.data.Close, self.n1)
        self.ma2 = self.I(EMA, self.data.Close, self.n2)
        
    def next(self):

        fastMA = self.ma1[-1]
        slowMA = self.ma2[-1]
        priceClose = self.data.Close[-1]

        if not self.position:
            
            if fastMA > slowMA:
                self.buy()
        
        else:
            if slowMA > fastMA:   
                self.position.close()
            


# %%
def getdata(Symbol):
    frame = pd.DataFrame(client.get_historical_klines(Symbol,
                                                      timeframe,
                                                      startdate
                                                      ))
    
    frame = frame.iloc[:,:6] # use the first 5 columns
    frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
    frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
    # frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
    frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
    return frame

# %%
def runBackTest(coinPair):

    print("coinPair = ",coinPair)
    df = getdata(coinPair)
    df = df.drop(['Time'], axis=1)

    bt = Backtest(df, EmaCross, cash=100000, commission=0.001)
    stats = bt.run()
    stats
    # bt.plot() 

    stats, heatmap = bt.optimize(
    n1=range(1, 100, 2),
    n2=range(2, 200, 2),
    constraint=lambda param: param.n1 < param.n2,
    maximize='Equity Final [$]',
    return_heatmap=True
    )

    dfbema = pd.DataFrame(heatmap.sort_values().iloc[-1:])
    n1 = dfbema.index.get_level_values(0)[0]
    n2 = dfbema.index.get_level_values(1)[0]
    returnPerc = round(stats['Return [%]'],2)
    BuyHoldReturnPerc = round(stats['Buy & Hold Return [%]'],2)

    # lista
    print("n1=",n1)
    print("n2=",n2)
    print("Return [%] = ",round(returnPerc,2))
    print("Buy & Hold Return [%] = ",round(BuyHoldReturnPerc,2))

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
                                                            BuyHoldReturnPerc
                                                        ]
    else:
        print("linha=",linha[0])
        # update line
        coinpairBestEma.loc[linha[0],['fastEMA','slowEMA','returnPerc','BuyHoldReturnPerc']] = [n1, n2, returnPerc,BuyHoldReturnPerc]

    # coinpairBestEma
    print("Saving Coin Pair to coinpairBestEma file")

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



