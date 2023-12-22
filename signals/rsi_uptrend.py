"""
RSI on Uptrend

The strategy goes like this:

send alerts when:
    - Symbol is in a uptrend - Use top performers (price > DSMA200 and 50 DSMA)
    - Use 4h timeframe
    - RSI(14) < 30 or RSI > 70
"""

import sys
import pandas as pd
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import time

import ta

import utils.telegram as telegram
import utils.database as database 
import utils.exchange as exchange
import utils.config as config

TELEGRAM_PREFIX_SIGNAL = "RSI-Uptrend" 
RSI_LOOKBACK_PERIODS = 14 # 14 days

def get_data(symbol, time_frame, start_date):
    print(f"{symbol} - getting data...")
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            df = pd.DataFrame(exchange.client.get_historical_klines(symbol,
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
        # symbol delisted
        num_columns = len(df.columns)
        if num_columns < 6:
            return pd.DataFrame()
        
        df = df.iloc[:,:6] # use the first 5 columns
        df.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
        df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].astype(float) #cast to float
        df['Date'] = df['Time'].astype(str) 
        # set the 'date' column as the DataFrame index
        df.set_index(pd.to_datetime(df['Date'], unit='ms'), inplace=True) # make human readable timestamp)
        df = df.drop(['Date'], axis=1)
        return df


def RSI(df, n=14):
    """Relative strength index"""
    rsi = ta.momentum.RSIIndicator(df['Close'], window=n)
    df['RSI'] = rsi.rsi()

def apply_technicals(df): 
    RSI(df, RSI_LOOKBACK_PERIODS)

def main(symbol):
    # 4H timeframe
    time_frame = exchange.client.KLINE_INTERVAL_4HOUR

    # get start date
    today = date.today() 
    pastdate = today - relativedelta(days=(RSI_LOOKBACK_PERIODS*1)) # used *10 to guarantee the qty of data to calculate rsi correctly 
    tuple = pastdate.timetuple()
    timestamp = time.mktime(tuple)
    start_date = str(timestamp)

    # default return value
    return_value = ""

    # get data
    df = get_data(symbol, time_frame, start_date)
    
    # avoid symbols with less data
    if len(df) < RSI_LOOKBACK_PERIODS:
        return return_value

    # apply rsi
    apply_technicals(df)

    # check alert conditions
    rsi_value = df['RSI'].iloc[-1]
    
    # BUY Signals
    if rsi_value < 30:
        return_value = "RSI<30"
        signal_message = return_value
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Buying"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)

    # SELL Signals
    if rsi_value > 70:
        return_value = "RSI>70"
        signal_message = return_value
        msg = f"{TELEGRAM_PREFIX_SIGNAL} - {symbol} - {signal_message}"
        add_note = "Consider Selling"
        msg = msg + f"\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_signals, '', msg)

    # get current date and time
    now = datetime.datetime.now()  

    # add signal to database
    if (rsi_value < 30 or rsi_value > 70):
        database.add_signal_log(database.conn, date=now, signal="RSI-Uptrend", signal_message=signal_message, symbol=symbol, notes=add_note)

    return return_value

def get_symbols(trade_against):    
    # Get blacklist
    # blacklist = get_blacklist()

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

    df_symbols = database.get_symbols_from_symbols_by_market_phase(database.conn)
    symbols = df_symbols['Symbol'].tolist()

    # Creating two sections
    RSI30_section = "# RSI<30\n"
    RSI70_section = "# RSI>70\n"

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

        # Filter rows where Signal is "RSI<30"
        df_rsi30 = df_signals[df_signals['Signal'] == 'RSI<30']
        # Filter rows where Signal is "RSI>70"
        df_rsi70 = df_signals[df_signals['Signal'] == 'RSI>70']

        # Writing DataFrame to CSV file with sections
        filename = "RSI_Uptrend.txt"
        with open(filename, 'w') as file:
            # Write the RSI30 section
            file.write(RSI30_section)
            df_rsi30.to_csv(file, columns=['Symbol'], header=False, index=False)

            # Separate sections with a line
            file.write("\n\n")

            # Write the RSI70 section
            file.write(RSI70_section)
            df_rsi70.to_csv(file, columns=['Symbol'], header=False, index=False)

        msg = "TradingView List:"
        msg = telegram.telegram_prefix_signals_sl + msg
        telegram.send_telegram_message(telegram.telegram_token_signals, "", msg)
        telegram.send_telegram_file(telegram.telegram_token_signals, filename)


    # msg = 'RSI-Uptrend - End'
    # print(msg)
    # telegram.send_telegram_message(telegram.eStop, msg)

if __name__ == "__main__":
    run()
