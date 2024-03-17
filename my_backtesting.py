import pandas as pd
import sys
import os
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
from exchanges.binance import client


# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "symbol_by_market_phase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

FOLDER_BACKTEST_RESULTS = "static/backtest_results"

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
            df = pd.DataFrame(client.get_historical_klines(symbol
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
                df = pd.DataFrame()
                return df 

            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
            print(msg)

    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)

        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        df = pd.DataFrame()
        return df
    else:            
        df = df.iloc[:,:6] # use the first 5 columns
        df.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        df.Time = pd.to_datetime(df.Time, unit='ms') #make human readable timestamp
        df = df.set_index(pd.DatetimeIndex(df['Time']))
        df = df.drop(['Time'], axis=1)

        # Remove the last row
        # This functionality is valuable because our data collection doesn't always coincide precisely with the closing time of a candle. 
        # As a result, the last row in our dataset represents the most current price information. 
        # This becomes significant when applying technical analysis, as it directly influences the accuracy of metrics and indicators. 
        # The implications extend to the decision-making process for buying or selling, making it essential to account for the real-time nature of the last row in our data.
        df = df.drop(df.index[-1])

        return df
    
def get_strategy_name(strategy):
    # get strategy name from strategy class
    strategy_name = str(strategy).split('.')[-1][:-2]
    return strategy_name

def save_backtesting_to_html(bt, stats, strategy, timeframe, symbol):
        # stats
        df_stats = pd.DataFrame(stats)
        # trades
        df_trades = pd.DataFrame(stats._trades)
        # remove Size column
        df_trades = df_trades.drop(columns=['Size'])

        strategy_name = get_strategy_name(strategy)
        filename=f"{strategy_name} - {timeframe} - {symbol}"

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

        # add stats and trade to html file

        # add style
        html_file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename+".html")
        with open(html_file_path, 'r') as file:
            html_content = file.read()

        # Locate the style tag in the HTML content
        style_tag_start = html_content.find('<style>')
        if style_tag_start == -1:
            head_tag_end = html_content.find('</head>')

            style_content_to_add = """<style>\n</style>"""
            modified_html_content = (
                html_content[:head_tag_end-1]
                + style_content_to_add
                + html_content[head_tag_end-1:]
            )
            with open(html_file_path, 'w') as file:
                file.write(modified_html_content)
        #-----

        with open(html_file_path, 'r') as file:
            html_content = file.read()
        # Locate the style tag in the HTML content
        style_tag_start = html_content.find('<style>')
        style_tag_end = html_content.find('</style>', style_tag_start)

        # Append or modify the content of the style tag
        
        # dataframe {
        #     text-align: left;
        # }
        style_content_to_add = """
        h2 {
            text-align: center;
            font-family: Helvetica, Arial, sans-serif;
        }
        table { 
            margin-left: auto;
            margin-right: auto;
        }
        table, th, td {
            border: 1px solid black;
            border-collapse: collapse;
        }
        th, td {
            padding: 5px;
            text-align: left;
            font-family: Helvetica, Arial, sans-serif;
            font-size: 90%;
        }
        table tbody tr:hover {
            background-color: #dddddd;
        }
        .wide {
            width: 90%; 
        }
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
        stats_html_table = df_stats.to_html(index=True, header=False)
        trades_html_table = df_trades.to_html(index=False)

        #-----
        # add style
        # html_file_path = filename+".html"
        with open(html_file_path, 'r') as file:
            html_content = file.read()

        # Locate the style tag in the HTML content
        body_tag_start = html_content.find('<body>')
        body_tag_end = html_content.find('</body>', body_tag_start)

        # Append or modify the content of the style tag
        stats_table_title = "<h2> STATS </h2>\n"
        stats_content_to_add = stats_table_title + stats_html_table
        trades_table_title = "<h2> TRADES </h2>\n"
        trades_content_to_add = trades_table_title + trades_html_table
        body_content_to_add = stats_content_to_add + trades_content_to_add

        modified_html_content = (
            html_content[:body_tag_end]
            + body_content_to_add
            + html_content[body_tag_end:]
        )

        with open(html_file_path, 'w') as file:
            file.write(modified_html_content)
        #------


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
    strategy_name = get_strategy_name(strategy)
    
    # lista
    print(f"Strategy = {strategy_name}")
    
    if optimize:
        print("n1 = ",n1)
        print("n2 = ",n2)
    
    print("Return [%] = ",return_perc)
    print("Buy & Hold Return [%] = ",buy_hold_return_Perc)
    print("Backtest start date = ", backtest_start_date)
    print("Backtest end date =" , backtest_end_date)

    
    # save results as html file
    save_backtesting_to_html(bt, stats, strategy, timeframe, symbol)

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
    
    # trades
    df_trades = pd.DataFrame(stats._trades)
    # remove Size column
    df_trades = df_trades.drop(columns=['Size'])

    # Insert the new columns at the beginning of the DataFrame
    df_trades.insert(0, "Symbol", symbol)
    df_trades.insert(1, "Time_Frame", timeframe)
    df_trades.insert(2, "Strategy_Id", strategy_name)

    # convert data type to string
    df_trades['Duration'] = df_trades['Duration'].astype(str)

    df_trades['ReturnPct'] = df_trades['ReturnPct']*100
    
    # Save trades to database
    # df_trades.to_sql('Backtesting_Trades', database.conn, index=False, if_exists='append')
    for index, row in df_trades.iterrows():
        database.add_backtesting_trade(
            connection=database.conn,
            symbol=row['Symbol'],
            timeframe=row['Time_Frame'],
            strategy_id=row['Strategy_Id'],
            entry_bar=row['EntryBar'],
            exit_bar=row['ExitBar'],
            entry_price=row['EntryPrice'],
            exit_price=row['ExitPrice'],
            pnl=row['PnL'],
            return_pct=row['ReturnPct'],
            entry_time=row['EntryTime'],
            exit_time=row['ExitTime'],
            duration=row['Duration']
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
        # get strategy name from strategy class
        strategy_name = get_strategy_name(strategy)
        print(f"Backtest strategy {strategy_name} - {symbol} - {timeframe} - Start")
        
        run_backtest(symbol, timeframe, strategy, optimize)

        print(f"Backtest strategy {strategy_name} - {symbol} - {timeframe} - End")
        
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
    



