import pandas as pd
import datetime
import numpy as np
import sys
import logging
import timeit
import pytz

from backtesting.lib import crossover

import utils.config as config
import utils.database as database
import utils.exchange as exchange
import utils.telegram as telegram
import add_symbol
from my_backtesting import calc_backtesting, get_data, get_backtesting_results

def get_blacklist():
    # Read symbols from blacklist to not trade
    df_blacklist = database.get_symbol_blacklist(database.conn)
    blacklist = set()
    if not df_blacklist.empty:
        trade_against = config.trade_against
        df_blacklist['Symbol'] = df_blacklist['Symbol'].astype(str) + trade_against
        # Convert blacklist to set
        blacklist = set(df_blacklist["Symbol"].unique())
    return blacklist


def apply_technicals(df):
    if df.empty:
        return
    
    df['DSMA50'] = df['Price'].rolling(50).mean()
    df['DSMA200'] = df['Price'].rolling(200).mean()

    df['Perc_Above_DSMA50'] = ((df['Price'] - df['DSMA50']) / df['DSMA50']) * 100
    df['Perc_Above_DSMA200'] = ((df['Price'] - df['DSMA200']) / df['DSMA200']) * 100


def get_data(symbol, time_frame):
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            frame = pd.DataFrame(exchange.client.get_historical_klines(symbol, time_frame,                                                           
                                                            # better get all historical data. 
                                                            # Using a defined start date will affect ema values. 
                                                            # To get same ema and sma values of tradingview default historical data must be used.
                                                            ))
            success = True
        except Exception as e:
            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
            print(msg)

    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        frame = pd.DataFrame()
        return frame()
    else:
        frame = frame.iloc[:, [0, 4]]  # Column selection
        frame.columns = ['Time', 'Price']  # Rename columns
        
        # using dictionary to convert specific columns
        convert_dict = {'Price': float} # Cast to float
        frame = frame.astype(convert_dict)

        frame['Symbol'] = symbol
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        frame.index = frame.Time
        frame = frame[['Symbol', 'Price']]
        return frame
    
def read_arguments():    
    # Arguments
    n = len(sys.argv)

    if n < 2:
        print("Argument is missing")
        time_frame = input('Enter timeframe (1d, 8h, 4h):')
        # trade_against = input('Trade against USDT or BTC:')
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]
        # trade_against = sys.argv[2]

    trade_against = config.trade_against

    return time_frame, trade_against

def get_symbols(trade_against):    
    # Get blacklist
    blacklist = get_blacklist()

    exchange_info = exchange.get_exchange_info()

    symbols = set()

    # Get symbols
    for s in exchange_info['symbols']:
        if (
            s['symbol'].endswith(trade_against)
            and not (s['symbol'].endswith('DOWN' + trade_against))
            and not (s['symbol'].endswith('UP' + trade_against))
            and not (s['symbol'] == "AUD" + trade_against)  # Australian Dollar
            and not (s['symbol'] == "EUR" + trade_against)  # Euro
            and not (s['symbol'] == "GBP" + trade_against)  # British pound
            and s['status'] == 'TRADING'
        ):
            symbols.add(s['symbol'])

    # From the symbols to trade, exclude symbols from blacklist
    symbols -= blacklist

    symbols = sorted(symbols)
    return symbols

def set_market_phases_to_symbols(symbols, time_frame):
    # Empty dataframe
    df_result = pd.DataFrame()

    for symbol in symbols:
        print("Calculating " + symbol)
        df = get_data(symbol, time_frame)

        if df.empty:
            continue
        
        apply_technicals(df)
        # Last one is the one with 200dsma value
        df = df.tail(1)

        if df_result.empty:
            df_result = df
        else:
            df_result = pd.concat([df_result, df])

    # Market phases conditions
    conditions = [
        (df_result['Price'] > df_result['DSMA50']) & (df_result['Price'] < df_result['DSMA200']) & (df_result['DSMA50'] < df_result['DSMA200']), # recovery phase
        (df_result['Price'] > df_result['DSMA50']) & (df_result['Price'] > df_result['DSMA200']) & (df_result['DSMA50'] < df_result['DSMA200']), # accumulation phase
        (df_result['Price'] > df_result['DSMA50']) & (df_result['Price'] > df_result['DSMA200']) & (df_result['DSMA50'] > df_result['DSMA200']), # bullish phase
        (df_result['Price'] < df_result['DSMA50']) & (df_result['Price'] > df_result['DSMA200']) & (df_result['DSMA50'] > df_result['DSMA200']), # warning phase
        (df_result['Price'] < df_result['DSMA50']) & (df_result['Price'] < df_result['DSMA200']) & (df_result['DSMA50'] > df_result['DSMA200']), # distribution phase
        (df_result['Price'] < df_result['DSMA50']) & (df_result['Price'] < df_result['DSMA200']) & (df_result['DSMA50'] < df_result['DSMA200'])  # bearish phase
    ]
    # Set market phase for each symbol
    values = ['recovery', 'accumulation', 'bullish', 'warning', 'distribution', 'bearish']
    df_result['Market_Phase'] = np.select(conditions, values)

    return df_result

def trade_against_auto_switch():

    if config.trade_against_switch:
        btc_pair = "BTCUSDT"
        btc_timeframe = "1d"
        sell_timeframes = ["1d", "4h", "1h"]
        sell_message = "Trade against auto switch"

        df_btc = get_data(btc_pair, btc_timeframe)

        btc_strategy_id = config.get_setting("btc_strategy")

        # get buy and sell conditions
        if btc_strategy_id in ['ema_cross']:

            fast_ema, slow_ema = get_backtesting_results(strategy_id=btc_strategy_id, symbol=btc_pair, time_frame=btc_timeframe)
            
            # technical indicators
            df_btc['FastEma'] = df_btc['Price'].ewm(span=fast_ema, adjust=False).mean()
            df_btc['SlowEma'] = df_btc['Price'].ewm(span=slow_ema, adjust=False).mean()

            buy_condition = crossover(df_btc.FastEMA, df_btc.SlowEMA)
            sell_condition = crossover(df_btc.SlowEMA, df_btc.FastEMA)

        elif btc_strategy_id in ["market_phases"]:

            # technical indicators
            df_btc['SMA50'] = df_btc['Price'].rolling(50).mean()
            df_btc['SMA200'] = df_btc['Price'].rolling(200).mean()
            
            # last row
            lastrow = df_btc.iloc[-1]
            # second-to-last row 
            second_to_last_row = df_btc.iloc[-2]
            
            accumulation_phase = (lastrow.Price > lastrow.SMA50) and (lastrow.Price > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
            bullish_phase = (lastrow.Price > lastrow.SMA50) and (lastrow.Price > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)

            accumulation_phase_previous = (second_to_last_row.Price > second_to_last_row.SMA50) and (second_to_last_row.Price > second_to_last_row.SMA200) and (second_to_last_row.SMA50 < second_to_last_row.SMA200)
            bullish_phase_previous = (second_to_last_row.Price > second_to_last_row.SMA50) and (second_to_last_row.Price > second_to_last_row.SMA200) and (second_to_last_row.SMA50 > second_to_last_row.SMA200)
        
            buy_condition_curr = accumulation_phase or bullish_phase    
            buy_condition_previous = accumulation_phase_previous or bullish_phase_previous 
            
            buy_condition = buy_condition_curr and not buy_condition_previous
            sell_condition = buy_condition_previous and not buy_condition_curr

        # convert USDT to BTC
        if config.trade_against == "USDT" and buy_condition:
            # sell all positions to USDT
            for tf in sell_timeframes:
                df_sell = database.get_positions_by_bot_position(database.conn, bot=tf, position=1)
                list_to_sell = df_sell.Symbol.tolist()
                for symbol in list_to_sell:
                    exchange.create_sell_order(symbol=symbol,
                                                bot=tf,
                                                reason=f"{sell_message}"
                                                )  

            # convert all USDT to BTC
            exchange.create_buy_order(symbol=btc_pair, bot=btc_timeframe, convert_all_balance=True)
                
            # change trade against to BTC
            config.set_trade_against("BTC")

        elif config.trade_against == "BTC" and sell_condition:
            # sell all positions to BTC
            for tf in sell_timeframes:
                df_sell = database.get_positions_by_bot_position(database.conn, bot=tf, position=1)
                list_to_sell = df_sell.Symbol.tolist()
                for symbol in list_to_sell:
                    exchange.create_sell_order(symbol=symbol,
                                                bot=tf,
                                                reason=f"{sell_message}"
                                                )  

            # convert all BTC to USDT
            exchange.create_sell_order(symbol=btc_pair, bot=btc_timeframe, convert_all_balance=True)

            # change trade against to USDT
            config.set_trade_against("USDT")

def main(time_frame, trade_against):
    # Calculate program run time
    start = timeit.default_timer()

    # Inform start
    msg = "Start"
    msg = telegram.telegram_prefix_market_phases_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_START, msg)

    # Log file to store error messages
    log_filename = "symbol_by_market_phase.log"
    logging.basicConfig(filename=log_filename, level=logging.INFO,
                        format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    # create daily balance snapshot
    exchange.create_balance_snapshot(telegram_prefix="")

    # backtest BTC Strategy
    btc_pair = "BTCUSDT"
    calc_backtesting(symbol=btc_pair, timeframe=time_frame, strategy=config.btc_strategy, optimize=config.btc_strategy_backtest_optimize)
    
    # Automatically switch trade against
    trade_against_auto_switch()

    trade_against = config.get_setting("trade_against")

    symbols = get_symbols(trade_against=trade_against)
    msg = str(len(symbols)) + " symbols found. Calculating market phases..."
    msg = telegram.telegram_prefix_market_phases_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    df_result = set_market_phases_to_symbols(symbols, time_frame)

    # Keep those in accumulation or bullish phases
    dfUnion = df_result.query("Market_Phase in ['bullish', 'accumulation']")

    df_top = dfUnion.sort_values(by=['Perc_Above_DSMA200'], ascending=False)
    df_top = df_top.head(config.trade_top_performance)

    # Set rank for highest strength
    df_top['Rank'] = np.arange(len(df_top)) + 1

    # Delete existing data
    database.delete_all_symbols_by_market_phase(database.conn)

    # Insert new symbols
    for index, row in df_top.iterrows():
        database.insert_symbols_by_market_phase(
            database.conn,
            row['Symbol'],
            row['Price'],
            row['DSMA50'],
            row['DSMA200'],
            row['Market_Phase'],
            row['Perc_Above_DSMA50'],
            row['Perc_Above_DSMA200'],
            row['Rank']
        )

    utc_time = datetime.datetime.now(pytz.timezone('UTC'))
    formatted_date = utc_time.strftime('%Y-%m-%d')
    database.insert_symbols_by_market_phase_historical(database.conn, formatted_date)

    selected_columns = df_top[["Symbol", "Price", "Market_Phase"]]
    df_top_print = selected_columns.copy()
    # Reset the index and set number beginning from 1
    df_top_print = df_top_print.reset_index(drop=True)
    df_top_print.index += 1

    msg = f"Top {str(config.trade_top_performance)} performance symbols:"
    msg = telegram.telegram_prefix_market_phases_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    msg = df_top_print.to_string(index=True)
    msg = telegram.telegram_prefix_market_phases_ml + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    # Create file to import to TradingView with the list of top performers and symbols in position
    df_tv_list = database.get_distinct_symbol_by_market_phase_and_positions(database.conn)
    df_top = df_tv_list
    df_tv_list['symbol'] = "BINANCE:" + df_tv_list['symbol']
    # Write DataFrame to CSV file
    filename = "Top_performers_" + trade_against + ".txt"
    df_tv_list.to_csv(filename, header=False, index=False)
    msg = "TradingView List:"
    msg = telegram.telegram_prefix_market_phases_sl + msg
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
    telegram.send_telegram_file(telegram.telegram_token_main, filename)

    if not df_top.empty:
        # Remove symbols from positions table that are not top performers in accumulation or bullish phase
        database.delete_positions_not_top_rank(database.conn)

        # Add top rank symbols with positive returns to positions files
        database.add_top_rank_to_positions(database.conn)

        # Delete rows with calc completed and keep only symbols with calc not completed
        database.delete_symbols_to_calc_completed(database.conn)

        # Add the symbols with open positions to calc
        database.add_symbols_with_open_positions_to_calc(database.conn)

        # Add the symbols in top rank to calc
        database.add_symbols_top_rank_to_calc(database.conn)

        # Calc best ema for each symbol on 1d, 4h and 1h time frame and save on positions table
        add_symbol.run()

    else:
        # if there are no symbols in accumulation or bullish phase remove all not open from positions
        database.delete_all_positions_not_open(database.conn)

    # Close the database connection
    database.conn.close()

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.calc_duration(total_seconds)
    msg = f'Execution Time: {duration}'
    msg = telegram.telegram_prefix_market_phases_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    # inform that ended
    msg = "End"
    msg = telegram.telegram_prefix_market_phases_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_STOP, msg)

def scheduled_run(time_frame, trade_against):
    main(time_frame=time_frame, trade_against=trade_against)

if __name__ == "__main__":
    time_frame, trade_against = read_arguments()
    main(time_frame=time_frame, trade_against=trade_against)
