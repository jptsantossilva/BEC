# %%

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

# %%


# startdate = "10 Nov, 2018 UTC"
# startdate = "12 May, 2022 UTC"
startdate = "4 year ago UTC"
# startdate = "10 day ago UTC"
timeframe = "1h"

# Check the program has been called with the timeframe
# total arguments
n = len(sys.argv)
# print("Total arguments passed:", n)
if n < 2:
    print("Argument is missing")
    timeframe = input('Enter timeframe (1d, 4h or 1h):')
else:
    # argv[0] in Python is always the name of the script.
    timeframe = sys.argv[1]


    
# coinPair = "BTCBUSD"

# %%
# client.get_account()

# %%
# from optparse import Values
def EMA(values, n):
    """
    Return exp moving average of `values`, at
    each step taking into account `n` previous values.
    """
    
    return pd.Series(values).ewm(span=n, adjust=False).mean()


# %%



# we will use four moving averages in total: 
# two moving averages whose relationship determines a general trend (we only trade long when the shorter MA is above the longer one, and vice versa), 
# and two moving averages whose cross-over with daily close prices determine the signal to enter or exit the position.
class EmaCross(Strategy):
    n1 = 8
    n2 = 34
    # n_enter = 20
    # n_exit = 10
    
    def init(self):
        # self.sma1 = self.I(EMA, self.data.Close, self.n1)
        # self.sma2 = self.I(EMA, self.data.Close, self.n2)
        
        self.ma1 = self.I(EMA, self.data.Close, self.n1)
        self.ma2 = self.I(EMA, self.data.Close, self.n2)
        
    def next(self):

        fastMA = self.ma1[-1]
        slowMA = self.ma2[-1]
        priceClose = self.data.Close[-1]

        if not self.position:
            
            # On upwards trend, if price closes above
            # "entry" MA, go long
            
            # Here, even though the operands are arrays, this
            # works by implicitly comparing the two last values
            # if (priceClose > fastMA) and (fastMA > slowMA):
            if fastMA > slowMA:
                # if crossover(self.data.Close, self.sma_enter):
                self.buy()
                    
            # On downwards trend, if price closes below
            # "entry" MA, go short
            
            # else:
            #     if crossover(self.sma_enter, self.data.Close):
            #         self.sell()
        
        # But if we already hold a position and the price
        # closes back below (above) "exit" MA, close the position
        
        else:
            # if (self.position.is_long and
            #     crossover(self.sma_exit, self.data.Close)
            #     or
            #     self.position.is_short and
            #     crossover(self.data.Close, self.sma_exit)):
            if slowMA > fastMA:   
                self.position.close()
            


# %%
def getdata(Symbol):
    frame = pd.DataFrame(client.get_historical_klines(Symbol,
                                                      timeframe,
                                                      # client.KLINE_INTERVAL_1HOUR,
                                                    #  '3 years ago UTC')
                                                      # '1 Feb, 2019 UTC', # bear market anterior
                                                      # '16 Nov, 2021 UTC' # inicio bear market 
                                                      # '14 Jun, 2022 UTC'   # 20k suporte
                                                      # '90 day ago UTC' 
                                                      startdate
                                                      # '4000 hour ago UTC' # 4hour
                                                      ))
    
    frame = frame.iloc[:,:6] # use the first 5 columns
    frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
    frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
    # frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
    frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
    return frame

# %%
def runBackTest(coinPair):

    print("coinPair=",coinPair)
    df = getdata(coinPair)
    df = df.drop(['Time'], axis=1)
    # df
    # df = bollinger_bands(df)


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

    # heatmap.dropna(inplace=True)
    # heatmap.droplevel
    # drop(labels inplace=True)
    # heatmap = heatmap[heatmap[0] > heatmap[1]]
    # from ast import Break
    # from doctest import BLANKLINE_MARKER


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

    coinpairBestEma = pd.read_csv('coinpairBestEma')
    # coinpairBestEma
    # add to file coinpair Best Ema 
    # if exist then update else add
    linha = coinpairBestEma.index[(coinpairBestEma.coinPair == coinPair) & (coinpairBestEma.timeFrame == timeframe)].to_list()

    if not linha:
        print("There is no line with coinPair "+str(coinPair)+ " and timeframe "+str(timeframe)+". New line will be added.")
        #add linha
        coinpairBestEma.loc[len(coinpairBestEma.index)] = [coinPair, 
                                                            n1,
                                                            n2,
                                                            timeframe,
                                                            returnPerc,
                                                            BuyHoldReturnPerc
                                                        ]
    else:
        print("linha=",linha[0])
        # update linha
        coinpairBestEma.loc[linha[0],['fastEMA','slowEMA','returnPerc','BuyHoldReturnPerc']] = [n1, n2, returnPerc,BuyHoldReturnPerc]

    # coinpairBestEma
    print("Saving Coin Pair to csv")
    coinpairBestEma.to_csv('coinpairBestEma', index=False, header=True)


# %%
# heatmap.sort_values().iloc[-20:]

# %%

# stats._strategy

# coinpairBestEma = pd.read_csv('coinpairBestEma')
# coinpairBestEma.loc[len(coinpairBestEma.index)] = ["START DATE = "+startdate+" TIMEFRAME="+timeframe, 
#                                                         "",
#                                                         "",
#                                                         "",
#                                                         "",
#                                                         ""
#                                                     ]
# coinpairBestEma.to_csv('coinpairBestEma', index=False, header=True)

# %%
Listcoinpair = pd.read_csv('coinpair')
# get coin pairs only
Listcoinpair = Listcoinpair.Currency
# Listcoinpair

# %%
# run backtest for each coin pair
for coinPair in Listcoinpair:
    print("Backtest - Start")
    runBackTest(coinPair)

print("Backtest - End")


