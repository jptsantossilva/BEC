"""
Multi EMAs

The strategy goes like this:

send alerts when:
    - price close crosses above all the EMAs 13, 25 100, 200 and 300
    - price close crosses below EMA13 and was above all EMAs before
"""

import sys
import pandas as pd
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import time

# import ta

import utils.config as config
import utils.telegram as telegram
import utils.database as database 
import exchanges.binance as binance

TELEGRAM_PREFIX_SIGNAL = "MULTI-EMAs" 

def get_data(symbol, time_frame, start_date):
    print(f"{symbol} - getting data...")
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            df = pd.DataFrame(binance.client.get_historical_klines(symbol,
                                                                    time_frame,
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
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        frame = pd.DataFrame()
        return frame
    else:
        df = df.iloc[:,:6] # use the first 5 columns
        df.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        df['Date'] = df['Time'].astype(str) 
        # set the 'date' column as the DataFrame index
        df.set_index(pd.to_datetime(df['Date'], unit='ms'), inplace=True) # make human readable timestamp)
        df = df.drop(['Date'], axis=1)
        return df


def EMA(values, n):
    """
    Return exp moving average of `values`, at
    each step taking into account `n` previous values.
    """
    return pd.Series(values).ewm(span=n, adjust=False).mean()

def apply_technicals(df): 
    df['EMA13']  = df['Close'].ewm(span=13, adjust=False).mean()
    df['EMA25']  = df['Close'].ewm(span=25, adjust=False).mean()
    df['EMA100'] = df['Close'].ewm(span=100, adjust=False).mean()
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
    df['EMA300'] = df['Close'].ewm(span=300, adjust=False).mean()

def main(symbol):
    # 1D timeframe
    time_frame = binance.client.KLINE_INTERVAL_1DAY

    # get start date
    # get max data as possible to make sure the slowest emas 200 and 300 get the same values as tradingview 
    #-------------------------------------
    today = date.today() 
    # today - 300 days
    pastdate = today - relativedelta(years=4)
    # print(pastdate)
    tuple = pastdate.timetuple()
    timestamp = time.mktime(tuple)
    start_date = str(timestamp)

    # default return value
    return_value = ""

    # get data from the 15m timeframe
    df = get_data(symbol, time_frame, start_date)
    
    # avoid symbols with less that 300 days
    if len(df) < 300:
        return return_value

    # apply multi emas
    apply_technicals(df)

    # check alert conditions
    last_close_value = df['Close'].iloc[-1]
    before_last_close_value = df['Close'].iloc[-2]

    last_ema13_value = df['EMA13'].iloc[-1]
    before_last_ema13_value = df['EMA13'].iloc[-2]

    last_ema25_value = df['EMA25'].iloc[-1]

    last_ema100_value = df['EMA100'].iloc[-1]

    last_ema200_value = df['EMA200'].iloc[-1]

    last_ema300_value = df['EMA300'].iloc[-1]
    before_last_ema300_value = df['EMA300'].iloc[-2]

    # BUY Signals
    # check if price crossed over ema300 and price > all emas
    buy_crossover_ema13 = (before_last_close_value <= before_last_ema13_value 
                            and last_close_value >= last_ema13_value
                            and last_ema13_value >= last_ema300_value
                            and last_close_value >= last_ema25_value
                            and last_close_value >= last_ema100_value
                            and last_close_value >= last_ema200_value
                            and last_close_value >= last_ema300_value)
    
    buy_crossover_ema300 = (before_last_close_value <= before_last_ema300_value 
                            and last_close_value >= last_ema300_value
                            and last_ema13_value <= last_ema300_value
                            and last_close_value >= last_ema25_value
                            and last_close_value >= last_ema100_value
                            and last_close_value >= last_ema200_value
                            and last_close_value >= last_ema300_value)
    
    if buy_crossover_ema13:
        return_value = "EMA13"
        signal_message = "Price cross over DEMA 13"
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Buying"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)

    if buy_crossover_ema300:
        return_value = "EMA300"
        signal_message = "Price cross over DEMA 300"
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Buying"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)

    # SELL Signals
    sell_crossunder_ema13 = (before_last_close_value >= before_last_ema13_value 
                        and last_close_value <= last_ema13_value
                        and last_ema13_value >= last_ema300_value)
    
    sell_crossunder_ema300 = (before_last_close_value >= before_last_ema300_value 
                         and last_close_value <= last_ema300_value 
                         and last_ema13_value <= last_ema300_value)
    
    if sell_crossunder_ema13:
        return_value = "EMA13"
        signal_message = "Price cross under DEMA 13"
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Selling"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)

    if sell_crossunder_ema300:
        return_value = "EMA300"
        signal_message = "Price cross under DEMA 300"
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Selling"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)  

    # get current date and time
    now = datetime.datetime.now()  

    # add signal to database
    if (buy_crossover_ema13
        or buy_crossover_ema300
        or sell_crossunder_ema13
        or sell_crossunder_ema300):
        database.add_signal_log(database.conn, date=now, signal="Multi-EMAs", signal_message=signal_message, symbol=symbol, notes=add_note)

    return return_value

def get_symbols(trade_against):    
    # Get blacklist
    # blacklist = get_blacklist()

    exchange_info = binance.get_exchange_info()

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
            and not (s['symbol'] == "BUSD" + trade_against)  # British pound
            and not (s['symbol'] == "USDC" + trade_against)  # British pound
            and s['status'] == 'TRADING'
        ):
            symbols.add(s['symbol'])

    # From the symbols to trade, exclude symbols from blacklist
    # symbols -= blacklist

    symbols = sorted(symbols)
    return symbols

def run():
    
    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    # trade_against = config.trade_against
    trade_against = "USDC"
    symbols = get_symbols(trade_against=trade_against)

    # test
    # main("CHESSUSDT")

    # Creating two sections
    EMA13_section = "# EMA13 cross\n"
    EMA300_section = "# EMA300 cross\n"

    # Create an empty DataFrame with column names
    columns = ['Symbol', 'Signal']
    df_signals = pd.DataFrame(columns=columns)

   # check if the symbols list is empty
    if not symbols:
        print("There are no symbols to evaluate.")
    else:
        # iterate over the symbols
        for symbol in symbols:
            result_signal = main(symbol)
            if result_signal:
                # Add the signal to the DataFrame
                # new_row = {'Symbol': symbol, 'Signal': result_signal}
                # df_signals = df_signals.append(new_row, ignore_index=True)
                new_row = pd.DataFrame({'Symbol': [symbol], 'Signal': [result_signal]})
                # Concatenate the original DataFrame with the new row
                df_signals = pd.concat([df_signals, new_row], ignore_index=True)
                

    if not df_signals.empty:
        # get TV prices from binance  
        df_signals['Symbol'] = "BINANCE:" + df_signals['Symbol']

        # Filter rows where Signal is "EMA13"
        df_ema13 = df_signals[df_signals['Signal'] == 'EMA13']
        # Filter rows where Signal is "EMA13"
        df_ema300 = df_signals[df_signals['Signal'] == 'EMA300']

        # Writing DataFrame to CSV file with sections
        filename = "Multi_EMAs.txt"
        with open(filename, 'w') as file:
            # Write the EMA13 section
            file.write(EMA13_section)
            df_ema13.to_csv(file, columns=['Symbol'], header=False, index=False)

            # Separate sections with a line
            file.write("\n\n")

            # Write the EMA300
            file.write(EMA300_section)
            df_ema300.to_csv(file, columns=['Symbol'], header=False, index=False)

        msg = "TradingView List:"
        msg = telegram.telegram_prefix_signals_sl + msg
        telegram.send_telegram_message(telegram.telegram_token_signals, "", msg)
        telegram.send_telegram_file(telegram.telegram_token_signals, filename)


    # msg = 'MULTI-EMA - End'
    # print(msg)
    # telegram.send_telegram_message(telegram.eStop, msg)

if __name__ == "__main__":
    run()
