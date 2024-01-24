
import pandas as pd
import sys
import math
import logging
import timeit

from backtesting.lib import crossover

import utils.config as config
import utils.database as database
import exchanges.binance as binance
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
# strategy_name = ''

def read_arguments():
    # total arguments
    n = len(sys.argv)
    
    if n < 2:
        print("Argument is missing")
        time_frame = input('Enter time frame (1d, 4h or 1h):')
        # run_mode = input('Enter run mode (test, prod):')
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]

        # run modes 
        # test - does not execute orders on the exchange
        # prod - execute orders on the exchange
        # run_mode = sys.argv[2]

    return time_frame #, run_mode

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

def get_backtesting_results(strategy_id, symbol, time_frame):
    
    # get best ema
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(connection=database.conn, 
                                                                        symbol=symbol, 
                                                                        time_frame=time_frame, 
                                                                        strategy_id=strategy_id)

    if not df.empty:
        fast_ema = int(df.Ema_Fast.values[0])
        slow_ema = int(df.Ema_Slow.values[0])
    else:
        fast_ema = int("0")
        slow_ema = int("0")

    # if bestEMA does not exist return empty dataframe in order to no use that trading pair
    return fast_ema, slow_ema

def get_data(symbol, time_frame):

    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            df = pd.DataFrame(binance.client.get_historical_klines(symbol,
                                                                    time_frame,    
                                                                    # better get all historical data. 
                                                                    # Using a defined start date will affect ema values. 
                                                                    ))

            success = True
        except Exception as e:
            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
            print(msg)
            
    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        df = pd.DataFrame()
        return df()
    else:
        df = df[[0,4]]
        df.columns = ['Time','Close']
        # using dictionary to convert specific columns
        convert_dict = {'Close': float}
        df = df.astype(convert_dict)
        df.Time = pd.to_datetime(df.Time, unit='ms')

        # Remove the last row
        # This functionality is valuable because our data collection doesn't always coincide precisely with the closing time of a candle. 
        # As a result, the last row in our dataset represents the most current price information. 
        # This becomes significant when applying technical analysis, as it directly influences the accuracy of metrics and indicators. 
        # The implications extend to the decision-making process for buying or selling, making it essential to account for the real-time nature of the last row in our data.
        df = df.drop(df.index[-1])

        return df

# calculates moving averages 
def apply_technicals(df, fast_ema=0, slow_ema=0): 
    df['FastEMA'] = df['Close'].ewm(span=fast_ema, adjust=False).mean()
    df['SlowEMA'] = df['Close'].ewm(span=slow_ema, adjust=False).mean()
    df['SMA50']   = df['Close'].rolling(50).mean()
    df['SMA200']  = df['Close'].rolling(200).mean()

# calc current pnl  
def get_current_pnl(symbol, current_price, time_frame):

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
        
        # initialize vars
        fast_ema = 0 
        slow_ema = 0

        # get best backtesting results for the strategy
        if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            fast_ema, slow_ema = get_backtesting_results(strategy_id=config.strategy_id, symbol=symbol, time_frame=time_frame)

            if fast_ema == 0 or slow_ema == 0:
                msg = f'{symbol} - {config.strategy_name} - Best EMA values missing'
                msg = telegram_prefix_sl + msg
                print(msg)
                telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
                continue

        # get latest price data 
        df = get_data(symbol=symbol, time_frame=time_frame)

        apply_technicals(df, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        # Current price
        current_price = lastrow.Close
        
        # Current PnL
        current_pnl = get_current_pnl(symbol, current_price, time_frame)

        # if using stop loss
        sell_stop_loss = False
        if config.stop_loss > 0:
            sell_stop_loss = current_pnl <= -config.stop_loss

        # if using take profit 1
        sell_tp_1 = False
        if config.take_profit_1_pnl_perc > 0:
            # check if tp1 occurred already
            # Filter
            df_tp1 = df_sell.loc[df_sell['Symbol'] == symbol, 'Take_Profit_1']
            # Extract the single value from the result (assuming only one row matches)
            tp1_occurred = df_tp1.values[0]
            # if not occurred
            if tp1_occurred == 0:
                sell_tp_1 = current_pnl >= config.take_profit_1_pnl_perc

        # if using take profit 1
        sell_tp_2 = False
        if config.take_profit_2_pnl_perc > 0:
            # check if tp1 occurred already
            # Filter
            df_tp2 = df_sell.loc[df_sell['Symbol'] == symbol, 'Take_Profit_2']
            # Extract the single value from the result (assuming only one row matches)
            tp2_occurred = df_tp2.values[0]
            # if not occurred
            if tp2_occurred == 0:
                sell_tp_2 = current_pnl >= config.take_profit_2_pnl_perc

        # check sell condition for the strategy
        if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            condition_crossover = (lastrow.SlowEMA > lastrow.FastEMA)
            sell_condition = condition_crossover 
        elif config.strategy_id in ["market_phases"]:
            sell_condition = (lastrow.Close < lastrow.SMA50) or (lastrow.Close < lastrow.SMA200) 

        if sell_condition or sell_stop_loss or sell_tp_1 or sell_tp_2:
            
            # stop loss
            if sell_stop_loss:
                binance.create_sell_order(symbol=symbol,
                                            bot=time_frame,
                                            fast_ema=fast_ema,
                                            slow_ema=slow_ema,
                                            reason=f"Stop loss {config.stop_loss}%"
                                            )  
                
            # sell_codition ema crossover
            elif sell_condition:
                binance.create_sell_order(symbol=symbol,
                                            bot=time_frame,
                                            fast_ema=fast_ema,
                                            slow_ema=slow_ema,
                                            )  

            # sell take profit 1
            if sell_tp_1:
                binance.create_sell_order(symbol=symbol,
                                            bot=time_frame,
                                            fast_ema=fast_ema,
                                            slow_ema=slow_ema,
                                            reason=f"Take-Profit Level 1 - {config.take_profit_1_pnl_perc}% PnL - {config.take_profit_1_amount_perc}% Amount",
                                            percentage=config.take_profit_1_amount_perc,
                                            take_profit_num=1
                                            )  
            # sell take profit 2
            if sell_tp_2:
                binance.create_sell_order(symbol=symbol,
                                            bot=time_frame,
                                            fast_ema=fast_ema,
                                            slow_ema=slow_ema,
                                            reason=f"Take-Profit Level 2 - {config.take_profit_2_pnl_perc}% PnL - {config.take_profit_2_amount_perc}% Amount",
                                            percentage=config.take_profit_2_amount_perc,
                                            take_profit_num=2
                                            )                       
        
        else:
            msg = f'{symbol} - {config.strategy_name} - Sell condition not fulfilled'
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)
            
            # set current PnL
            current_price = lastrow.Close
            database.update_position_pnl(database.conn,
                                         bot=time_frame,
                                         symbol=symbol, 
                                         curr_price=current_price)


    # check symbols not in positions and BUY if conditions are fulfilled
    for symbol in list_to_buy:

        # initialize vars
        fast_ema = 0 
        slow_ema = 0

        # get best backtesting results for the strategy
        if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            fast_ema, slow_ema = get_backtesting_results(strategy_id=config.strategy_id, symbol=symbol, time_frame=time_frame)

            if fast_ema == 0 or slow_ema == 0:
                msg = f'{symbol} - {config.strategy_name} - Best EMA values missing'
                msg = telegram_prefix_sl + msg
                print(msg)
                telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
                continue

        df = get_data(symbol=symbol, time_frame=time_frame)

        apply_technicals(df, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        # check buy condition for the strategy
        if config.strategy_id in ["ema_cross_with_market_phases"]:
            
            accumulation_phase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
            bullish_phase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)
        
            condition_phase = accumulation_phase or bullish_phase
            condition_crossover = crossover(df.FastEMA, df.SlowEMA)
            buy_condition = condition_phase and condition_crossover

        elif config.strategy_id in ["market_phases"]:
            
            accumulation_phase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
            bullish_phase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)
        
            buy_condition = accumulation_phase or bullish_phase            

        if buy_condition:
                binance.create_buy_order(symbol=symbol, bot=time_frame, fast_ema=fast_ema, slow_ema=slow_ema)    
        else:
            msg = f'{symbol} - {config.strategy_name} - Buy condition not fulfilled'
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
        msg = f"Bot {time_frame} is inactive. Check Dashboard - Settings. Bye"
        print(msg)
        return
    elif time_frame == "4h" and not config.bot_4h:
        msg = f"Bot {time_frame} is inactive. Check Dashboard - Settings. Bye"
        print(msg)
        return
    elif time_frame == "1d" and not config.bot_1d:
        msg = f"Bot {time_frame} is inactive. Check Dashboard - Settings. Bye"
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

    # exchange.create_balance_snapshot(telegram_prefix="")

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
    time_frame = read_arguments()
    run_mode = config.get_setting("run_mode")
    apply_arguments(time_frame)            
    run(time_frame=time_frame, run_mode=run_mode)



