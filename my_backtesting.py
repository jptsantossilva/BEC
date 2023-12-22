import pandas as pd
import sys
from datetime import date
from dateutil.relativedelta import relativedelta
import time
import logging
import timeit

from backtesting import Backtest, Strategy
from backtesting.lib import crossover

import utils.telegram as telegram
import utils.database as database
# import utils.config as config
from utils.exchange import client


# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "symbol_by_market_phase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

# backtest with 4 years of price data 
#-------------------------------------
today = date.today() 
# today - 4 years - 200 days (DSMA200)
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
# Use SMA50 and SMA200
# BUY when price close > SMA50 and price close > SMA200 and SMA50<SMA200 (Accumulation Phase)
# BUY when price close > SMA50 and price close > SMA200 and SMA50 > SMA200
# SELL when price close < SMA50 or SMA200 (whatever happens first)
#-------------------------------------
class market_phases(Strategy):
    nFastSMA = 50
    nSlowSMA = 200    
    
    def init(self):
        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)

    def next(self):
        SMA50 = self.sma50
        SMA200 = self.sma200
        priceClose = self.data.Close

        accumulationPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        bullishPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)

        
        if not self.position:
            if (accumulationPhase or bullishPhase): 
            # if crossover(fastEMA, slowEMA):
                self.buy()
        
        else: 
            if not(accumulationPhase or bullishPhase):
                self.position.close()

#-------------------------------------
# we will use 2 exponencial moving averages:
# BUY when fast ema > slow ema
# SELL when slow ema > fast ema  
#-------------------------------------
class ema_cross(Strategy):
    n1 = 2
    n2 = 14 
    
    def init(self):
        self.emaFast = self.I(EMA, self.data.Close, self.n1)
        self.emaSlow = self.I(EMA, self.data.Close, self.n2)

    def next(self):
        fastEMA = self.emaFast
        slowEMA = self.emaSlow
        
        if not self.position:
            if crossover(fastEMA, slowEMA): 
                self.buy()
        
        else: 
            if crossover(slowEMA, fastEMA): 
                self.position.close()

class ema_cross_with_market_phases(Strategy):
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
            
def get_data(symbol, timeframe):
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            frame = pd.DataFrame(client.get_historical_klines(symbol
                                                            ,timeframe

                                                            # better get all historical data. 
                                                            # Using a defined start date will affect ema values. 
                                                            # To get same ema and sma values of tradingview all historical data must be used. 
                                                            ,startdate
                                                            ))
            success = True
        except Exception as e:
            # avoid error message in telegram if error is related to non-existing trading pair
            # example: CREAMUSDT - BinanceAPIException(Response [400], 400, code:-1121,msg:Invalid symbol.)
            msg = repr(e)
            print(msg)
            invalid_symbol_error = '"code":-1121,"msg":"Invalid symbol.'
            if invalid_symbol_error in msg:             
                frame = pd.DataFrame()
                return frame 

            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+pSymbol+" - "+repr(e)
            print(msg)

    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+pSymbol
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)

        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        frame = pd.DataFrame()
        return frame
    else:            
        frame = frame.iloc[:,:6] # use the first 5 columns
        frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        # frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        frame = frame.set_index(pd.DatetimeIndex(frame['Time']))
        frame = frame.drop(['Time'], axis=1)
        return frame

def run_backtest(symbol, timeframe, strategy, optimize):

    # vars initialization
    n1 = 0
    n2 = 0

    df = get_data(symbol, timeframe)

    if df.empty:
        return # exit function
    
    commission_value = float(0.005)
    cash_value = float(100000)

    # Checking the value of strategy
    bt = Backtest(df, strategy=strategy, cash=cash_value, commission=commission_value)
    
    stats = bt.run()
    # print(stats)
    # bt.plot()

    if optimize:
        stats, heatmap = bt.optimize(
            n1=range(10, 100, 10),
            n2=range(20, 200, 10),
            constraint=lambda param: param.n1 < param.n2,
            maximize='Equity Final [$]',
            return_heatmap=True
        )   

        dfbema = pd.DataFrame(heatmap.sort_values().iloc[-1:])
        n1 = dfbema.index.get_level_values(0)[0]
        n2 = dfbema.index.get_level_values(1)[0]
    
    
    return_perc = round(stats['Return [%]'],2)
    buy_hold_return_Perc = round(stats['Buy & Hold Return [%]'],2)
    backtest_start_date = str(df.index[0])
    backtest_end_date = str(df.index[-1])

    # get strategy name from strategy class
    strategy_name = str(strategy).split('.')[-1][:-2]
    
    # lista
    print(f"Strategy = {strategy_name}")
    
    if optimize:
        print("n1 = ",n1)
        print("n2 = ",n2)
    
    print("Return [%] = ",return_perc)
    print("Buy & Hold Return [%] = ",buy_hold_return_Perc)
    print("Backtest start date = ", backtest_start_date)
    print("Backtest end date =" , backtest_end_date)

    database.add_backtesting_results(database.conn,
                                    timeframe=timeframe,
                                    symbol=symbol,
                                    ema_fast=n1,
                                    ema_slow=n2,
                                    return_perc=return_perc,
                                    buy_hold_return_perc=buy_hold_return_Perc,
                                    backtest_start_date=backtest_start_date,
                                    backtest_end_date=backtest_end_date,
                                    strategy_Id=strategy_name
                                    )
    
def get_backtesting_results(strategy_id, symbol, time_frame):
    
    # get best ema
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(connection=database.conn, 
                                                                        symbol=symbol, 
                                                                        time_frame=time_frame, 
                                                                        strategy_id=strategy_id)

    if not df.empty:
        fast_ema = int(df.Ema_Fast.values[0])
        slow_ema = int(df.Ema_Slow.values[0])
        strategy_name = df.Name.values[0]
    else:
        fast_ema = int("0")
        slow_ema = int("0")

    # strategy_name
    # strategy_name = str(fast_ema)+"/"+str(slow_ema)+" "+strategy_name
        
    return fast_ema, slow_ema

def calc_backtesting(symbol, timeframe, strategy, optimize):

    result = False

    try:
        # calculate run time
        start = timeit.default_timer()
        
        print("")
        print("Backtest - "+symbol+" - "+timeframe+" - Start")
        
        run_backtest(symbol, timeframe, strategy, optimize)
        
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
    



