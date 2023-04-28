
import pandas as pd
import config
import exchange
from binance.exceptions import BinanceAPIException
import sys
import math
import numpy as np
from backtesting.lib import crossover
import logging
import telegram
import timeit
import config
import database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# calculate program run time
start = timeit.default_timer()

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')


# Global Vars
time_frame = ''
run_mode = ''
time_frame_num = ''
time_frame_type_short = ''
time_frame_type_long = ''
telegram_token = ''

# strategy
strategy_name = ''

def read_arguments():
    # Check for the time_frame and run_mode arguments
    
    # total arguments
    n = len(sys.argv)
    
    global time_frame
    global run_mode
    global time_frame_num
    global time_frame_type_short
    global time_frame_type_long
    global telegram_token

    if n < 2:
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

    if time_frame == "1h":
        time_frame_num = int("1")
        time_frame_type_short = "h" # h, d
        time_frame_type_long = "hour" # hour, day
        telegram_token = telegram.telegram_token_1h

    elif time_frame == "4h":
        time_frame_num = int("4")
        time_frame_type_short = "h" # h, d
        time_frame_type_long = "hour" # hour, day
        telegram_token = telegram.telegram_token_4h

    elif time_frame == "1d":
        time_frame_num = int("1")
        time_frame_type_short = "d" # h, d
        time_frame_type_long = "day" # hour, day
        telegram_token = telegram.telegram_token_1d
    else:
        msg = "Incorrect time frame. Bye"
        sys.exit(msg)

def calc_stake_amount(symbol):
    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(database.conn)

        if num_open_positions >= config.max_number_of_open_positions:
            return -2 

        try:
            balance = float(exchange.client.get_asset_balance(asset = symbol)['free'])
            
        except BinanceAPIException as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            return 0
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            return 0
    
        tradable_balance = balance*config.tradable_balance_ratio 
        
        stake_amount = tradable_balance/(config.max_number_of_open_positions-num_open_positions)
        
        if symbol == "BTC":
            stake_amount = round(stake_amount, 8)
        elif symbol in ("BUSD", "USDT"):
            stake_amount = int(stake_amount)
        
        # make sure the size is >= the minimum size
        if stake_amount < config.min_position_size:
            stake_amount = config.min_position_size

        # make sure there are enough funds otherwise abort the buy position
        if balance < stake_amount:
            stake_amount = 0

        return stake_amount
    
    elif int(config.stake_amount_type) >= 0:
        return config.stake_amount_type
    else:
        return 0
    

def get_data(symbol, time_frame_num, time_frame_type_short):

    try:
        # update EMAs from the best EMA return ratio
        global strategy_name

        time_frame = str(time_frame_num)+time_frame_type_short
        if time_frame_type_short == "h":
            time_frame_type_long = "hour"
        elif time_frame_type_short == "d":
            time_frame_type_long = "day"
        
        # get best ema
        df_best_ema = database.get_best_ema_by_symbol_timeframe(database.conn, symbol=symbol, time_frame=time_frame)

        if not df_best_ema.empty:
            fast_ema = int(df_best_ema.Ema_Fast.values[0])
            slow_ema = int(df_best_ema.Ema_Slow.values[0])
        else:
            fast_ema = int("0")
            slow_ema = int("0")

        strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

        # if bestEMA does not exist return empty dataframe in order to no use that trading pair
        if fast_ema == 0:
            frame = pd.DataFrame()
            return frame, fast_ema, slow_ema
        
        # if best Ema exist get price data 
        # lstartDate = str(1+gSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC"
        sma200 = 200
        # lstartDate = str(sma200*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
        # time_frame = str(time_frame_num)+time_frame_type_short
        frame = pd.DataFrame(exchange.client.get_historical_klines(symbol
                                                        ,time_frame    
                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        # ,lstartDate
                                                        
                                                        ))

        frame = frame[[0,4]]
        frame.columns = ['Time','Close']
        frame.Close = frame.Close.astype(float)
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        return frame, fast_ema, slow_ema
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
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
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        return -1

def trade():
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
        df, fast_ema, slow_ema = get_data(symbol, time_frame_num, time_frame_type_short)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
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
        df, fast_ema, slow_ema = get_data(symbol, time_frame_num, time_frame_type_short)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
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
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)

def positions_summary():
    df_summary = database.get_positions_by_bot_position(database.conn,
                                                        bot=time_frame, position=1)
    
    # remove unwanted columns
    df_dropped = df_summary.drop(columns=['Id','Date','Bot','Position','Rank','Qty','Ema_Fast','Ema_Slow','Buy_Order_Id','Duration'])
    
    # sort by symbol
    df_sorted = df_dropped.sort_values("Symbol")
    
    # df_cp_to_print.rename(columns={"Currency": "Symbol", "Close": "Price", }, inplace=True)
    df_sorted.reset_index(drop=True, inplace=True) # gives consecutive numbers to each row
    if df_sorted.empty:
        print("Result: no open positions")
        telegram.send_telegram_message(telegram_token, "", "Result: no open positions")
    else:
        print(df_sorted)
        telegram.send_telegram_message(telegram_token, "", df_sorted.to_string())

    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(database.conn)
        msg = f"{str(num_open_positions)}/{str(config.max_number_of_open_positions)} positions occupied"
        print(msg)
        telegram.send_telegram_message(telegram_token, "", msg=msg)


def main():
    read_arguments()

    # inform that bot has started
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_START, "Start")
    
    trade()

    positions_summary()

    # Close the database connection
    database.conn.close()

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.calc_duration(total_seconds)

    msg = f'Execution Time: {duration}'
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)

    # inform that bot has finished
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_STOP, "Binance Trader Bot - End")

if __name__ == "__main__":
    main()



