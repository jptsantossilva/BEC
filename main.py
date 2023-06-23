
import pandas as pd
import sys
import math
import logging
import timeit
import datetime
import pytz
import schedule
import time

from binance.exceptions import BinanceAPIException
from backtesting.lib import crossover

import utils.config as config
import utils.database as database
import utils.exchange as exchange
import utils.telegram as telegram

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')


# Global Vars
telegram_token = telegram.telegram_token_main

# sl = single line message; ml = multi line message
telegram_prefix_sl = ''
telegram_prefix_ml = ''

# strategy
strategy_name = ''

def read_arguments():
    # total arguments
    n = len(sys.argv)
    
    if n < 3:
        print("Argument is missing")
        time_frame = input('Enter time frame (1d, 4h or 1h):')
        run_mode = input('Enter run mode (test, prod):')
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]

        # run modes 
        # test - does not execute orders on the exchange
        # prod - execute orders on the exchange
        run_mode = sys.argv[2]

    return time_frame, run_mode

def apply_arguments(time_frame):

    global telegram_token, telegram_prefix_ml, telegram_prefix_sl

    if time_frame == "1h":
        telegram_prefix_sl = telegram.telegram_prefix_bot_1h_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_1h_ml
    elif time_frame == "4h":
        telegram_prefix_sl = telegram.telegram_prefix_bot_4h_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_4h_ml
    elif time_frame == "1d":
        telegram_prefix_sl = telegram.telegram_prefix_bot_1d_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_1d_ml
    else:
        msg = "Incorrect time frame. Bye"

def get_data(symbol, time_frame):

    try:
        # get best ema
        df_best_ema = database.get_best_ema_by_symbol_timeframe(connection=database.conn, symbol=symbol, time_frame=time_frame)

        if not df_best_ema.empty:
            fast_ema = int(df_best_ema.Ema_Fast.values[0])
            slow_ema = int(df_best_ema.Ema_Slow.values[0])
        else:
            fast_ema = int("0")
            slow_ema = int("0")

        global strategy_name
        strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

        # if bestEMA does not exist return empty dataframe in order to no use that trading pair
        if fast_ema == 0:
            frame = pd.DataFrame()
            return frame, fast_ema, slow_ema
        
        # if best Ema exist get price data 
        # lstartDate = str(1+gSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC"
        # sma200 = 200
        # lstartDate = str(sma200*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
        # time_frame = str(time_frame_num)+time_frame_type_short
        frame = pd.DataFrame(exchange.client.get_historical_klines(symbol,
                                                                   time_frame    
                                                                   # better get all historical data. 
                                                                   # Using a defined start date will affect ema values. 
                                                                   # To get same ema and sma values of tradingview all historical data must be used. 
                                                                   # ,lstartDate)
                                                                   ))

        frame = frame[[0,4]]
        frame.columns = ['Time','Close']
        frame.Close = frame.Close.astype(float)
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        return frame, fast_ema, slow_ema
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        frame = pd.DataFrame()
        return frame 

# calculates moving averages 
def apply_technicals(df, fast_ema, slow_ema): 
    df['FastEMA'] = df['Close'].ewm(span=fast_ema, adjust=False).mean()
    df['SlowEMA'] = df['Close'].ewm(span=slow_ema, adjust=False).mean()
    df['SMA50']   = df['Close'].rolling(50).mean()
    df['SMA200']  = df['Close'].rolling(200).mean()

# calc current pnl  
def get_current_pnl(symbol, current_price):

    try:
        # get buy price
        df_buy_price = database.get_positions_by_bot_symbol_position(database.conn, bot=time_frame, symbol=symbol, position=1)
        buy_price = 0
        pnl_perc = 0
        
        if not df_buy_price.empty:
            # get buy price
            buy_price = df_buy_price['Buy_Price'].iloc[0]
            # check if buy price is fulfilled 
            if not math.isnan(buy_price) and buy_price > 0:
                # calc pnl percentage
                pnl_perc = ((current_price - buy_price) / buy_price) * 100
                pnl_perc = round(pnl_perc, 2)
        
        return pnl_perc
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        return -1

def trade(time_frame, run_mode):
    # Make sure we are only trying to buy positions on symbols included on market phases table
    database.delete_positions_not_top_rank(database.conn)

    # list of symbols in position - SELL
    df_sell = database.get_positions_by_bot_position(database.conn, bot=time_frame, position=1)
    list_to_sell = df_sell.Symbol.tolist()
    
    
    # list of symbols in position - BUY
    df_buy = database.get_positions_by_bot_position(database.conn, bot=time_frame, position=0)
    list_to_buy = df_buy.Symbol.tolist()
    
    # check open positions and SELL if conditions are fulfilled 
    for symbol in list_to_sell:
        df, fast_ema, slow_ema = get_data(symbol=symbol, time_frame=time_frame)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)
        lastrow = df.iloc[-1]

        # if using stop loss
        sell_stop_loss = False
        if config.stop_loss > 0:
            # check current price
            current_price = lastrow.Close
            # check current pnl
            current_pnl = get_current_pnl(symbol, current_price)
            sell_stop_loss = current_pnl <= -config.stop_loss

        condition_crossover = (lastrow.SlowEMA > lastrow.FastEMA) 

        if condition_crossover or sell_stop_loss:
            if run_mode == 'prod': 
                exchange.create_sell_order(symbol=symbol,
                                           bot=time_frame,
                                           fast_ema=fast_ema,
                                           slow_ema=slow_ema)                        
            
        else:
            msg = f'{symbol} - {strategy_name} - Sell condition not fulfilled'
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)
            
            # set current PnL
            lastrow = df.iloc[-1]
            current_price = lastrow.Close
            database.update_position_pnl(database.conn,
                                         bot=time_frame,
                                         symbol=symbol, 
                                         curr_price=current_price)


    # check coins not in positions and BUY if conditions are fulfilled
    for symbol in list_to_buy:
        df, fast_ema, slow_ema = get_data(symbol=symbol, time_frame=time_frame)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)
        lastrow = df.iloc[-1]

        accumulationPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
        bullishPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)
        
        condition_phase = accumulationPhase or bullishPhase
        condition_crossover = crossover(df.FastEMA, df.SlowEMA)

        if condition_phase and condition_crossover:
            if run_mode == 'prod': 
                exchange.create_buy_order(symbol=symbol, bot=time_frame, fast_ema=fast_ema, slow_ema=slow_ema)    
        else:
            msg = f'{symbol} - {strategy_name} - Buy condition not fulfilled'
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)

def positions_summary(time_frame):
    df_summary = database.get_positions_by_bot_position(database.conn,
                                                        bot=time_frame, 
                                                        position=1)
    
    # remove unwanted columns
    df_dropped = df_summary.drop(columns=['Id','Date','Bot','Position','Rank','Qty','Ema_Fast','Ema_Slow','Buy_Order_Id','Duration'])
    
    # sort by symbol
    df_sorted = df_dropped.sort_values("Symbol")
    
    # df_cp_to_print.rename(columns={"Currency": "Symbol", "Close": "Price", }, inplace=True)
    df_sorted.reset_index(drop=True, inplace=True) # gives consecutive numbers to each row
    if df_sorted.empty:
        msg = "Positions Summary: no open positions"
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, "", msg)
    else:
        msg = df_sorted.to_string()
        msg = telegram_prefix_sl + "Positions Summary:\n" + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, "", msg)

    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(database.conn)
        msg = f"{str(num_open_positions)}/{str(config.max_number_of_open_positions)} positions occupied"
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, "", msg)


def run(time_frame, run_mode):

    if time_frame == "1h" and not config.bot_1h:
        msg = f"Bot {time_frame} is inactive. Check Config file. Bye"
        print(msg)
        return
    elif time_frame == "4h" and not config.bot_4h:
        msg = f"Bot {time_frame} is inactive. Check Config file. Bye"
        print(msg)
        return
    elif time_frame == "1d" and not config.bot_1d:
        msg = f"Bot {time_frame} is inactive. Check Config file. Bye"
        print(msg)
        return           

    # calculate program run time
    start = timeit.default_timer()
    
    # inform that bot has started
    msg = "Start"
    msg = telegram_prefix_sl + msg
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_START, msg)

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()
    
    trade(time_frame, run_mode)

    positions_summary(time_frame)

    # Close the database connection
    database.conn.close()

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.calc_duration(total_seconds)

    msg = f'Execution Time: {duration}'
    msg = telegram_prefix_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)

    # inform that bot has finished
    msg = "End"
    msg = telegram_prefix_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_STOP, msg)

def scheduled_run(time_frame, run_mode):
    apply_arguments(time_frame)
    run(time_frame=time_frame, run_mode=run_mode)

if __name__ == "__main__":
    time_frame, run_mode = read_arguments()
    apply_arguments(time_frame)            
    run(time_frame=time_frame, run_mode=run_mode)



