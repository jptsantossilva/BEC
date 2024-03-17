# %%
import sys 
import os
from datetime import date
import pandas as pd
import webbrowser
from dateutil.relativedelta import relativedelta
import time
from enum import Enum
import ta

from exchanges.binance import client

from backtesting import Backtest, Strategy 
from backtesting.lib import crossover



# from my_backtesting import FOLDER_BACKTEST_RESULTS
FOLDER_BACKTEST_RESULTS = "static/backtest_results"

class strategy(Enum):
    EMA_CROSS = "ema_cross"
    EMA_CROSS_WITH_MARKET_PHASES = "ema_cross_with_market_phases"
    MARKET_PHASES = "market_phases"
    CONSECUTIVE_CANDLES = "consecutive_candles"
    CONSECUTIVE_CANDLES_WITH_MARKET_PHASES = "consecutive_candles_with_marketphases"
    MULTI_EMA_CROSS = "multi_ema_cross"
    RSI_UPTREND = "rsi_uptrend"
    BREAKOUT = "breakout"

def get_start_date(years: int, days: int):
    # backtest with 4 years of price data 
    #-------------------------------------
    today = date.today() 
    # today - 4 years - 200 days
    past_date = today - relativedelta(years=years) - relativedelta(days=days)
    # print(pastdate)
    tuple = past_date.timetuple()
    timestamp = time.mktime(tuple)

    start_date = str(timestamp)

    return start_date

# %%
# test_emas = True
# test_market_phases = False

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

# def RSI(df, n=14):
#     """Relative strength index"""
#     rsi = ta.momentum.RSIIndicator(df['Close'], window=n)
#     # df['RSI'] = rsi.rsi()
#     return rsi

def RSI(array, n):
    """Relative strength index"""
    # Approximate; good enough
    gain = pd.Series(array).diff()
    loss = gain.copy()
    gain[gain < 0] = 0
    loss[loss > 0] = 0
    rs = gain.ewm(n).mean() / loss.abs().ewm(n).mean()
    return 100 - 100 / (1 + rs)


def get_data(symbol, time_frame, start_date):
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            frame = pd.DataFrame(client.get_historical_klines(
                symbol, 
                time_frame,                                                           
                # better get all historical data. 
                # Using a defined start date will affect ema values. 
                # To get same ema and sma values of tradingview default historical data must be used.
                start_date
                ))
            success = True
        except Exception as e:
            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
            print(msg)

    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        # msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)
        # telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        frame = pd.DataFrame()
        return frame()
    else:
        frame = frame.iloc[:,:6] # use the first 5 columns
        frame.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        frame[['Open','High','Low','Close','Volume']] = frame[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        # frame.index = [dt.datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        
        frame['Symbol'] = symbol
        # frame.index = [datetime.fromtimestamp(x / 1000.0) for x in frame.Time]
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        frame.index = frame.Time
        # frame = frame[['Symbol', 'Price']]
        return frame

class Breakout(Strategy):
    # BTC
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

class EmaCross(Strategy):
    # BTC
    n1 = 2
    n2 = 14 
    
    # ETH
    n1 = 2
    n2 = 18

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

class MultiEmaCross(Strategy):
    # BTC
    n13 = 13
    n25 = 25
    n100 = 100
    n200 = 200
    n300 = 300

    def init(self):
        self.ema13 = self.I(EMA, self.data.Close, self.n13)
        self.ema25 = self.I(EMA, self.data.Close, self.n25)
        self.ema100 = self.I(EMA, self.data.Close, self.n100)
        self.ema200 = self.I(EMA, self.data.Close, self.n200)
        self.ema300 = self.I(EMA, self.data.Close, self.n300)

    def next(self):
        
        if not self.position:
            
            # price above all emas AND ema13 > ema300
            # if (self.data.Close > self.ema13 
            #     and self.ema13 > self.ema300
            #     and self.data.Close > self.ema25  
            #     and self.data.Close > self.ema100 
            #     and self.data.Close > self.ema200 
            #     and self.data.Close > self.ema300): 
            
            # price above all emas
            if (self.data.Close > self.ema13 
                and self.data.Close > self.ema25  
                and self.data.Close > self.ema100 
                and self.data.Close > self.ema200 
                and self.data.Close > self.ema300): 
                self.buy()
        
        else: 
            # if self.ema1 < self.ema2 :
            if self.data.Close < self.ema13 :
                self.position.close()

class RSI_Uptrend(Strategy):
    nFastSMA = 50
    nSlowSMA = 200   
    rsi_level_low = 30
    rsi_level_high = 70 
    rsi_lookback_periods = 14
    
    def init(self):
        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)
        self.actual_rsi = self.I(RSI, self.data.Close, self.rsi_lookback_periods)
        
    def next(self):
        SMA50 = self.sma50[-1]
        SMA200 = self.sma200[-1]
        price = self.data.Close[-1]        

        accumulationPhase = (price > SMA50) and (price > SMA200) and (SMA50 < SMA200)
        bullishPhase = (price > SMA50) and (price > SMA200) and (SMA50 > SMA200)
        
        if not self.position:
            # if (accumulationPhase or bullishPhase) and self.actual_rsi[-1] <= self.rsi_level_low: 
            # if (price > SMA200) and self.actual_rsi[-1] <= self.rsi_level_low: 
            if self.actual_rsi[-1] <= self.rsi_level_low:
            # if crossover(fastEMA, slowEMA):
                self.buy()
        
        else: 
            # if not(accumulationPhase or bullishPhase):
            if self.actual_rsi[-1] >= self.rsi_level_high:
                self.position.close()

class MarketPhases(Strategy):
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

class EmaCross_MarketPhases(Strategy):
    n1 = 30
    n2 = 40
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

        condition_phase = accumulationPhase or bullishPhase
        condition_buy = crossover(fastEMA, slowEMA)
        condition_sell = crossover(slowEMA, fastEMA) 

        if not self.position:
            if condition_phase and condition_buy:
                self.buy()
        
        else: 
            if condition_sell: 
                self.position.close()

class consecutive_candles(Strategy):
    consecutive_candles = 3
    stop_loss_candles = 3
    stop_loss_percent = 1.0  # Adjust this value to set your stop loss percentage

    def init(self):
        self.consecutive_red_candles_count = 0
        self.consecutive_green_candles_count = 0
        self.stop_loss_count = 0
        self.stop_loss_price = None

    def next(self):

        # initial stop loss value
        if self.stop_loss_price == None:
            self.stop_loss_price = self.data.Close[-1]

        if not self.position:
            # if len(self.data) >= self.consecutive_candles + 1 and crossover(self.data.Close, self.data.Close[-self.consecutive_candles]):
            
            # if green candle
            if self.data.Close[-1] > self.data.Close[-2]:
                self.consecutive_green_candles_count += 1
                if self.consecutive_green_candles_count == self.consecutive_candles:
                    self.buy()
                    self.consecutive_green_candles_count = 0
            else:
                self.consecutive_green_candles_count = 0
        
        # red candles counter
        if self.data.Close[-1] < self.data.Close[-2]:
            self.stop_loss_count += 1
            if self.stop_loss_count >= self.stop_loss_candles:
                self.stop_loss_price = self.data.Close[-1]
                # self.stop_loss_count = 0
        else:
            self.stop_loss_count = 0

        if self.position:
            if self.data.Close[-1] <= self.stop_loss_price:
                self.position.close()

class consecutive_candles_marketphases(Strategy):
    consecutive_candles = 3
    stop_loss_candles = 3
    stop_loss_percent = 1.0  # Adjust this value to set your stop loss percentage

    nFastSMA = 50
    nSlowSMA = 200  

    def init(self):
        self.consecutive_red_candles_count = 0
        self.consecutive_green_candles_count = 0
        self.stop_loss_count = 0
        self.stop_loss_price = None

        self.sma50 = self.I(SMA, self.data.Close, self.nFastSMA)
        self.sma200 = self.I(SMA, self.data.Close, self.nSlowSMA)

    def next(self):

        SMA50 = self.sma50
        SMA200 = self.sma200
        priceClose = self.data.Close

        accumulationPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 < SMA200)
        bullishPhase = (priceClose > SMA50) and (priceClose > SMA200) and (SMA50 > SMA200)
        # bullish50sma = (priceClose > SMA50)

        condition_phase = accumulationPhase or bullishPhase
        # condition_phase = bullish50sma

        # initial stop loss value
        if self.stop_loss_price == None:
            self.stop_loss_price = self.data.Close[-1]

        if not self.position:
            # if len(self.data) >= self.consecutive_candles + 1 and crossover(self.data.Close, self.data.Close[-self.consecutive_candles]):
            
            # if green candle
            if self.data.Close[-1] > self.data.Close[-2]:
                self.consecutive_green_candles_count += 1
                if condition_phase and self.consecutive_green_candles_count >= self.consecutive_candles:
                    self.buy()
                    self.consecutive_green_candles_count = 0
            else:
                self.consecutive_green_candles_count = 0
        
        # red candles counter
        if self.data.Close[-1] < self.data.Close[-2]:
            self.stop_loss_count += 1
            if self.stop_loss_count >= self.stop_loss_candles:
                self.stop_loss_price = self.data.Close[-1]
                # self.stop_loss_count = 0
        else:
            self.stop_loss_count = 0

        if self.position:
            if self.data.Close[-1] <= self.stop_loss_price:
                self.position.close()

# %%
def run_backtest(symbol, timeframe, strategy, start_date):
    
    # if symbol.endswith("BTC"):
    #     symbol_only = symbol[:-3]
    #     symbol_stable = symbol[-3:]
    # elif symbol.endswith("USDT"):    
    #     symbol_only = symbol[:-4]
    #     symbol_stable = symbol[-4:]

    df = get_data(symbol, timeframe, start_date)

    if df.empty:
        return # exit function

    if strategy == strategy.EMA_CROSS:
        bt = Backtest(df, EmaCross, cash=100000, commission=0.005)
    if strategy == strategy.MARKET_PHASES:
        bt = Backtest(df, MarketPhases, cash=100000, commission=0.005)
    if strategy == strategy.EMA_CROSS_WITH_MARKET_PHASES:
        bt = Backtest(df, EmaCross_MarketPhases, cash=100000, commission=0.005)
    if strategy == strategy.CONSECUTIVE_CANDLES:
        bt = Backtest(df, consecutive_candles, cash=100000, commission=0.005)
    if strategy == strategy.CONSECUTIVE_CANDLES_WITH_MARKET_PHASES:
        bt = Backtest(df, consecutive_candles_marketphases, cash=100000, commission=0.005)
    if strategy == strategy.MULTI_EMA_CROSS:
        bt = Backtest(df, MultiEmaCross, cash=100000, commission=0.005, trade_on_close=True)
    if strategy == strategy.RSI_UPTREND:
        bt = Backtest(df, RSI_Uptrend, cash=100000, commission=0.005)
    if strategy == strategy.BREAKOUT:
        bt = Backtest(df, Breakout, cash=100000, commission=0.005)
        
    # run backtesting
    stats = bt.run()

    # print(stats)
    df_stats = pd.DataFrame(stats)
    print (df_stats)
    # Access and print trades
    df_trades = pd.DataFrame(stats._trades)
    # remove Size column
    df_trades = df_trades.drop(columns=['Size'])

    # Insert the new columns at the beginning of the DataFrame
    df_trades.insert(0, "Symbol", symbol)
    df_trades.insert(1, "Time_Frame", time_frame)
    df_trades.insert(2, "Strategy_Id", strategy)

    print(df_trades)

    filename=f"{strategy} - {time_frame} - {symbol}"

    # Create the folder if it doesn't exist
    if not os.path.exists(FOLDER_BACKTEST_RESULTS):
        os.makedirs(FOLDER_BACKTEST_RESULTS)

    # Specify the CSV file path
    csv_file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename+".csv")

    # Export both DataFrames to the same CSV file
    df_stats.to_csv(csv_file_path, index=True)
    df_trades.to_csv(csv_file_path, mode='a', index=False, header=True)

    filename_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename)

    bt.plot(
            # plot_return = True,
            # plot_drawdown = True,
            filename = filename_path,
            open_browser=False)    
    
    #-----
    # add style
    html_file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename+".html")
    with open(html_file_path, 'r') as file:
        html_content = file.read()

    # Locate the style tag in the HTML content
    style_tag_start = html_content.find('<style>')
    style_tag_end = html_content.find('</style>', style_tag_start)

    # Append or modify the content of the style tag
    style_content_to_add = """
    table {
        font-family: arial, sans-serif;
        border-collapse: collapse;
        /* padding-left: 8px; */
        /* width: 100%; */
    }
    
    td, th {
      padding: 8px;
      text-align: left;
      border-bottom: 1px solid #ddd;
        }

    tr:hover {background-color: MediumSeaGreen;}
    """
    modified_html_content = (
        html_content[:style_tag_end]
        + style_content_to_add
        + html_content[style_tag_end:]
    )

    with open(html_file_path, 'w') as file:
        file.write(modified_html_content)
    #-----

    # Convert the DataFrame to an HTML table
    stats_html_table = df_stats.to_html(index=True, border="0")
    trades_html_table = df_trades.to_html(index=False, border="0")

    #-----
    # add style
    # html_file_path = filename+".html"
    with open(html_file_path, 'r') as file:
        html_content = file.read()

    # Locate the style tag in the HTML content
    body_tag_start = html_content.find('<body>')
    body_tag_end = html_content.find('</body>', body_tag_start)

    # Append or modify the content of the style tag
    stats_table_title = """<h3 style="font-family: arial, sans-serif; text-align: left; padding: 8px"><br>STATS</h3>  """
    stats_content_to_add = stats_table_title + stats_html_table
    trades_table_title = """<h3 style="font-family: arial, sans-serif; text-align: left; padding: 8px"><br>TRADES</h3>  """
    trades_content_to_add = trades_table_title + trades_html_table
    body_content_to_add = stats_content_to_add + trades_content_to_add

    modified_html_content = (
        html_content[:body_tag_end]
        + body_content_to_add
        + html_content[body_tag_end:]
    )

    with open(html_file_path, 'w') as file:
        file.write(modified_html_content)
    #-----

    # # Specify the existing HTML file path
    # existing_html_file = f"{filename}.html"

    # # Open the existing HTML file in append mode and write the HTML table
    # with open(existing_html_file, 'a') as f:
    #     f.write(trades_html_table)

    # Open the HTML file in the default web browser
    webbrowser.open(html_file_path)

    return

    if strategy == strategy.EMA_CROSS:
        if timeframe == "1d":
            n1_range = range(10, 100, 10)
            n2_range = range(20, 200, 10)
        elif timeframe == "1w":
            n1_range = range(2, 30, 2)
            n2_range = range(2, 30, 2)
        
        stats, heatmap = bt.optimize(
            n1=n1_range,
            n2=n2_range,
            constraint=lambda param: param.n1 < param.n2,
            maximize='Equity Final [$]',
            # maximize='Win Rate [%]',
            return_heatmap=True
    )
    elif strategy == strategy.MARKET_PHASES:
        stats, heatmap = bt.optimize(
            nFastSMA = 50,
            nSlowSMA = 200,   
            # constraint=lambda param: param.n1 < param.n2,
            maximize='Equity Final [$]',
            # maximize='Win Rate [%]',
            return_heatmap=True
        )
    elif strategy == strategy.EMA_CROSS_WITH_MARKET_PHASES:
        stats, heatmap = bt.optimize(
            n1=range(10, 100, 10),
            n2=range(20, 200, 10),
            nFastSMA = 50,
            nSlowSMA = 200,   
            constraint=lambda param: param.n1 < param.n2,
            maximize='Equity Final [$]',
            # maximize='Win Rate [%]',
            return_heatmap=True
        )

    dfbema = pd.DataFrame(heatmap.sort_values().iloc[-1:])
    if strategy in [strategy.MARKET_PHASES, strategy.EMA_CROSS]:
        n1 = dfbema.index.get_level_values(0)[0]
        n2 = dfbema.index.get_level_values(1)[0]
    elif strategy == strategy.EMA_CROSS_WITH_MARKET_PHASES:
        n1 = dfbema.index.get_level_values(0)[0]
        n2 = dfbema.index.get_level_values(1)[0]
        n3 = dfbema.index.get_level_values(2)[0]
        n4 = dfbema.index.get_level_values(3)[0]
    returnPerc = round(stats['Return [%]'],2)
    winrate = round(stats['Win Rate [%]'],2)
    BuyHoldReturnPerc = round(stats['Buy & Hold Return [%]'],2)
    BacktestStartDate = str(df.index[0])

    # lista
    if strategy == strategy.EMA_CROSS:
        print("n1=",n1)
        print("n2=",n2)
    if strategy == strategy.MARKET_PHASES:
        print("nFastSMA=",n1)
        print("nSlowSMA=",n2)
    if strategy == strategy.EMA_CROSS_WITH_MARKET_PHASES:
        print("n1=",n1)
        print("n2=",n2)
        print("nFastSMA=",n3)
        print("nSlowSMA=",n4)
    print("Return [%] = ",round(returnPerc,2))
    print(f"Win Rate [%] = {winrate}")
    print("Buy & Hold Return [%] = ",round(BuyHoldReturnPerc,2))
    print("Backtest start date =", BacktestStartDate)

    # database.add_best_ema(database.conn,
    #                       timeframe=timeframe,
    #                       symbol=symbol,
    #                       ema_fast=n1,
    #                       ema_slow=n2,
    #                       return_perc=returnPerc,
    #                       buy_hold_return_perc=BuyHoldReturnPerc,
    #                       backtest_start_date=BacktestStartDate
    #                       )

#---------------------------------------------------------------------
# MANAGE SETTINGS

# strategy = strategy.EMA_CROSS # best ema
strategy = strategy.MARKET_PHASES 
# strategy = strategy.MULTI_EMA_CROSS
# strategy = Strategy.EMA_CROSS_WITH_MARKET_PHASES # best ema + market phases
# strategy = strategy.CONSECUTIVE_CANDLES
# strategy = strategy.CONSECUTIVE_CANDLES_WITH_MARKET_PHASES
# strategy = strategy.RSI_UPTREND
# strategy = strategy.BREAKOUT

# symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT", "MATICUSDT", "LINKUSDT", "AAVEUSDT", "SUSHIUSDT", "YFIUSDT", "NEARUSDT", "FTMUSDT", "CAKEUSDT", "CELRUSDT", "PHBUSDT"]
symbols = ["BTCUSDT"]
# time_frame="1w"
time_frame="1d"
# time_frame="4h"
# time_frame="1h"

start_date = get_start_date(years=4, days=400)

for symbol in symbols:
    # get_data(symbol, time_frame, start_date)
    print(f"strategy: {strategy}")
    print(f"symbol: {symbol}")
    print(f"time-frame: {time_frame}")
    run_backtest(symbol, time_frame, strategy, start_date)
    


