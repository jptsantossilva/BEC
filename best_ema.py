import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import sys
from datetime import date
from dateutil.relativedelta import relativedelta
import time
import utils.telegram as telegram
import logging
import timeit
import utils.database as database
from utils.exchange import client

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "coinpairByMarketPhase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

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

        accumulationPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        bullishPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)

        if not self.position:
            if (accumulationPhase or bullishPhase) and crossover(fastEMA, slowEMA):
            # if crossover(fastEMA, slowEMA):
                self.buy()
        
        else: 
            if crossover(slowEMA, fastEMA): 
                self.position.close()
            
def get_data(pSymbol, pTimeframe):
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
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)

        # avoid error message in telegram if error is related to non-existing trading pair
        # example: CREAMUSDT - BinanceAPIException(Response [400], 400, code:-1121,msg:Invalid symbol.)
        invalid_symbol_error = '"code":-1121,"msg":"Invalid symbol.'
        if invalid_symbol_error not in msg:             
            telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)

        frame = pd.DataFrame()
        return frame 

def run_backtest(symbol, timeframe):
    
    if symbol.endswith("BTC"):
        symbol_only = symbol[:-3]
        symbol_stable = symbol[-3:]
    elif symbol.endswith(("BUSD","USDT")):    
        symbol_only = symbol[:-4]
        symbol_stable = symbol[-4:]

    if symbol.endswith("BTC"):
        df = get_data(symbol, timeframe)

    # get historical data from BUSD and USDT and use the one with more data 
    elif symbol.endswith(("BUSD","USDT")):
        dfStableBUSD = pd.DataFrame()
        dfStableUSDT = pd.DataFrame()
    
        iniBUSD = 0
        iniUSDT = 0
        
        dfStableBUSD = get_data(symbol_only+"BUSD", timeframe)
            
        if not dfStableBUSD.empty:
            ini1 = dfStableBUSD.index[0]

        dfStableUSDT = get_data(symbol_only+"USDT", timeframe) 

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

    database.add_best_ema(database.conn,
                          timeframe=timeframe,
                          symbol=symbol,
                          ema_fast=n1,
                          ema_slow=n2,
                          return_perc=returnPerc,
                          buy_hold_return_perc=BuyHoldReturnPerc,
                          backtest_start_date=BacktestStartDate
                          )

def calc_best_ema(symbol, timeframe):

    result = False

    try:
        # calculate run time
        start = timeit.default_timer()
        
        print("")
        print("Backtest - "+symbol+" - "+timeframe+" - Start")
        run_backtest(symbol, timeframe)
        print("Backtest "+symbol+" - "+timeframe+" - End")
        
        stop = timeit.default_timer()
        total_seconds = stop - start
        duration = database.calc_duration(total_seconds)
        msg = f'Execution Time: {duration}'
        print(msg)
        
        result = True
        return result
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - " + repr(e)
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        
        return False
    



