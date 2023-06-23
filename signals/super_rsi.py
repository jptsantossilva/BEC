"""
SUPER-RSI

The strategy roughly goes like this:

send alerts when:
    RSI 1d / 4h / 1h / 30m / 15m <= 25
    RSI 1d / 4h / 1h / 30m / 15m >= 80
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


def get_data(Symbol, time_frame, start_date):
    print(f"{Symbol} - getting data...")
    df = pd.DataFrame(exchange.client.get_historical_klines(Symbol,
                                                            time_frame,
                                                            start_date
                                                            ))
    
    df = df.iloc[:,:6] # use the first 5 columns
    df.columns = ['Time','Open','High','Low','Close','Volume'] #rename columns
    df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].astype(float) #cast to float
    df['Date'] = df['Time'].astype(str) 
    # set the 'date' column as the DataFrame index
    df.set_index(pd.to_datetime(df['Date'], unit='ms'), inplace=True) # make human readable timestamp)
    df = df.drop(['Date'], axis=1)
    return df

#-----------------------------------------------------------------------
# calculate RSI 
#-----------------------------------------------------------------------
def RSI(df, n):
    """Relative strength index"""
    rsi = ta.momentum.RSIIndicator(df['Close'], window=n)
    df['rsi'] = rsi.rsi()

def apply_technicals(df, rsi_length):
    # calc RSI
    RSI(df, rsi_length)

def super_rsi(symbol):
    # 15min timeframe
    time_frame = exchange.client.KLINE_INTERVAL_15MINUTE
    rsi_lookback_periods = 14 # 14 days

    # get start date
    today = date.today() 
    pastdate = today - relativedelta(days=(rsi_lookback_periods*10)) # used *10 to guarantee the qty of data to calculate rsi on 1D timeframe correctly 
    tuple = pastdate.timetuple()
    timestamp = time.mktime(tuple)
    start_date = str(timestamp)

    rsi_1d = 14
    rsi_4h = 14
    rsi_1h = 14
    rsi_30m = 14
    rsi_15m = 14

    rsi_low = 25
    rsi_high = 80

    # get data from the 15m timeframe
    df_15m = get_data(symbol, time_frame, start_date)
    # Compute RSI
    apply_technicals(df_15m, rsi_15m)

    # check rsi value
    # we want the value before the last that corresponded to the last closed candle. 
    # The last one is the current and the candle is not yet closed
    value = round(df_15m['rsi'].iloc[-2],1) 
    result_low = value <= rsi_low
    result_high = value >= rsi_high

    msg_15m = f"{symbol} - RSI({rsi_15m}) 15m = {value}"
    msg_15m = telegram.telegram_prefix_signals_sl + msg_15m
    print(msg_15m)
    telegram.send_telegram_message(telegram.telegram_token_main, '', msg_15m)
    
    if not result_low:
        msg = f"{symbol} - RSI({rsi_15m}) 15m ≤ {rsi_low} - condition not fulfilled"
        # print(msg)
        # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
    if not result_high:
        msg = f"{symbol} - RSI({rsi_15m}) 15m ≥ {rsi_high} - condition not fulfilled"
        # print(msg) 
        # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
    
    # result_low = True # tests
    if not result_low and not result_high:
        return  # Exit the function
    
    if result_low or result_high:
        df_30m = df_15m.resample('30min').last()
        apply_technicals(df_30m, rsi_30m)
        # print(df_30m)

        value = round(df_30m['rsi'].iloc[-2],1)
        
        # if previous was below then test the current
        if result_low:
            result_low = value <= rsi_low
        # if previous was above then test the current
        if result_high:
            result_high = value >= rsi_high
        
        msg_30m = f"{symbol} - RSI({rsi_30m}) 30m = {value}"
        msg_30m = telegram.telegram_prefix_signals_sl + msg_30m
        print(msg_30m)
        telegram.send_telegram_message(telegram.telegram_token_main,'', msg_30m)

        if not result_low:
            msg = f"{symbol} - RSI({rsi_30m}) 30m ≤ {rsi_low} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
    
        if not result_high:
            msg = f"{symbol} - RSI({rsi_30m}) 30m ≥ {rsi_high} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
        
        # result_low = True # tests
        if not result_low and not result_high:
            return  # Exit the function
        
    if result_low or result_high:
        df_1h = df_15m.resample('1H').last()
        apply_technicals(df_1h, rsi_1h)
        
        value = round(df_1h['rsi'].iloc[-2],1)
        result_low = value <= rsi_low
        result_high = value >= rsi_high
        
        msg_1h = f"{symbol} - RSI({rsi_1h}) 1H = {value}"
        msg_1h = telegram.telegram_prefix_signals_sl + msg_1h
        print(msg_1h)
        telegram.send_telegram_message(telegram.telegram_token_main,'', msg_1h)
        
        if not result_low:
            msg = f"{symbol} - RSI({rsi_1h}) 1H ≤ {rsi_low} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
    
        if not result_high:
            msg = f"{symbol} - RSI({rsi_1h}) 1H ≥ {rsi_high} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
        
        # result_low = True # tests
        if not result_low and not result_high:
            return  # Exit the function
    
    if result_low or result_high:
        df_4h = df_15m.resample('4H').last()
        apply_technicals(df_4h, rsi_4h)
        
        value = round(df_4h['rsi'].iloc[-2],1)
        result_low_4h = value <= rsi_low
        result_high_4h = value >= rsi_high
        
        msg_4h = f"{symbol} - RSI({rsi_4h}) 4H = {value}"
        msg_4h = telegram.telegram_prefix_signals_sl + msg_4h
        print(msg_4h)
        telegram.send_telegram_message(telegram.telegram_token_main,'', msg_4h)
         
        if not result_low_4h:
            msg = f"{symbol} - RSI({rsi_4h}) 4H ≤ {rsi_low} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)

        if not result_high_4h:
            msg = f"{symbol} - RSI({rsi_4h}) 4H ≥ {rsi_high} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)

        # result_low_4h = True # tests
        if not result_low_4h and not result_high_4h:
            return  # Exit the function
        
    if result_low_4h or result_high_4h:
        df_1d = df_15m.resample('D').last()
        apply_technicals(df_1d, rsi_1d)
        
        value = round(df_1d['rsi'].iloc[-2],1)
        result_low_1d = value <= rsi_low
        result_high_1d = value >= rsi_high
        
        msg_1d = f"{symbol} - RSI({rsi_1d}) 1D = {value}"
        msg_1d = telegram.telegram_prefix_signals_sl + msg_1d
        print(msg_1d)
        telegram.send_telegram_message(telegram.telegram_token_main,'', msg_1d)
         
        if not result_low_1d:
            msg = f"{symbol} - RSI({rsi_1d}) 1D ≤ {rsi_low} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)
        
        if not result_high_1d:
            msg = f"{symbol} - RSI({rsi_1d}) 1D ≥ {rsi_high} - condition not fulfilled"
            # print(msg) 
            # telegram.send_telegram_message(telegram.telegram_token_main,'', msg)

        # result_low_1d = True # tests
        # if not result_low_1d and not result_high_1d:
        #     return  # Exit the function

    # if rsi is below min level or above max level in all timeframes we have a super rsi alert!
    if result_low_4h or result_high_4h or result_low_1d or result_high_1d:
        # get current date and time
        now = datetime.datetime.now()
        # format the current date and time
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

        msg = f"SUPER-RSI alert!\n{formatted_now}\n{symbol}\n{msg_15m}\n{msg_30m}\n{msg_1h}\n{msg_4h}"
        if result_high_1d or result_low_1d:
            msg = msg + f"\n{msg_1d}"        
        
        if result_low_4h or result_low_1d:
            add_note = "Consider Buying"
            msg = msg + f"\nNote: {add_note}"
            msg = telegram.telegram_prefix_signals_sl + msg
            telegram.send_telegram_message(telegram.telegram_token_signals,telegram.EMOJI_ENTER_TRADE, msg)
            if result_low_1d:
                signal_message = f"RSI(14) 1d,4h,1h,30m,15m < {rsi_low}"
            else:
                signal_message = f"RSI(14) 4h,1h,30m,15m < {rsi_low}"
            
        elif result_high_4h or result_high_1d:
            add_note = "Consider Selling"
            msg = msg + f"\nNote: {add_note}"
            msg = telegram.telegram_prefix_signals_sl + msg
            telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_EXIT_TRADE, msg)
            if result_high_1d:
                signal_message = f"RSI(14) 1d,4h,1h,30m,15m > {rsi_high}"
            else:
                signal_message = f"RSI(14) 4h,1h,30m,15m > {rsi_high}"

        # add signal to database
        database.add_signal_log(database.conn, date=now, signal="Super-RSI", signal_message=signal_message, symbol=symbol, notes=add_note)

def run():
    # msg = 'SUPER-RSI - Start'
    # print(msg)
    # telegram.send_telegram_message(telegram.telegram_token_main, telegram.eStart, msg)

    # local_time = datetime.datetime.now()
    # print(f"Run Super-RSI: {local_time}")

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    df_symbols = database.get_distinct_symbol_from_positions_where_position1(database.conn)
    # check if the symbols list is empty
    if df_symbols.empty:
        print("There are no symbols to calculate.")
    else:
        symbols = df_symbols["Symbol"].to_list()
        # iterate over the symbols and call current_super_rsi() for each one
        for symbol in symbols:
            super_rsi(symbol)

    # msg = 'SUPER-RSI - End'
    # print(msg)
    # telegram.send_telegram_message(telegram.eStop, msg)

if __name__ == "__main__":
    run()
