"""
Bollinger-Bands Width

Send alerts when the BB width is unusually low (squeeze) or high (expansion).
"""

import sys
import os

# Allow running this file directly from the signals/ folder.
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import pandas as pd
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import time

import ta

import utils.telegram as telegram
import utils.database as database
import exchanges.binance as binance

TELEGRAM_PREFIX_SIGNAL = "Bollinger Bands Width"
BB_LENGTH = 20
BB_STD = 2.0
BB_WIDTH_LOW_Q = 0.10
BB_WIDTH_HIGH_Q = 0.90
LOOKBACK_DAYS_1D = 1460
REGIME_MA = 200
REGIME_SLOPE_PERIOD = 10
REGIME_MIN_POINTS = 100

def get_data(symbol, time_frame, start_date):
    print(f"{symbol} - getting data...")
    max_retry = 3
    df = binance.get_ohlcv(
        symbol=symbol,
        interval=time_frame,
        start_date=start_date,
        max_retries=max_retry,
        drop_last=False,
        drop_incomplete=False,
        include_symbol=False,
        set_index=True,
        keep_time_col=True,
    )

    if df.empty:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        msg = telegram.telegram_prefix_signals_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        return pd.DataFrame()
    return df

def apply_technicals(df):
    bb = ta.volatility.BollingerBands(df['Close'], window=BB_LENGTH, window_dev=BB_STD)
    df['bb_width'] = (bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg() * 100.0
    df['sma_regime'] = df['Close'].rolling(REGIME_MA).mean()
    df['sma_regime_slope'] = df['sma_regime'] - df['sma_regime'].shift(REGIME_SLOPE_PERIOD)

def _get_regime(df):
    close = df['Close']
    sma = df['sma_regime']
    slope = df['sma_regime_slope']
    bull = (close > sma) & (slope > 0)
    bear = (close < sma) & (slope < 0)
    return pd.Series(pd.NA, index=df.index).mask(bull, "bull").mask(bear, "bear").fillna("neutral")

def _start_date_from_days(days):
    today = date.today()
    pastdate = today - relativedelta(days=days)
    tuple = pastdate.timetuple()
    timestamp = time.mktime(tuple)
    return str(timestamp)

def _check_bb_width(symbol, time_frame, timeframe_label, lookback_days):
    start_date = _start_date_from_days(lookback_days)
    df = get_data(symbol, time_frame, start_date)
    if df.empty or len(df) < BB_LENGTH + 2:
        return

    apply_technicals(df)
    width_series = df['bb_width'].dropna()
    if len(width_series) < BB_LENGTH + 2:
        return
    width = round(width_series.iloc[-2], 2)

    regime_series = _get_regime(df).reindex(width_series.index)
    current_regime = regime_series.iloc[-2]
    regime_mask = regime_series == current_regime
    regime_width = width_series[regime_mask]

    if len(regime_width) >= REGIME_MIN_POINTS:
        low_thr = round(regime_width.quantile(BB_WIDTH_LOW_Q), 2)
        high_thr = round(regime_width.quantile(BB_WIDTH_HIGH_Q), 2)
        regime_note = f"regime={current_regime}"
    else:
        low_thr = round(width_series.quantile(BB_WIDTH_LOW_Q), 2)
        high_thr = round(width_series.quantile(BB_WIDTH_HIGH_Q), 2)
        regime_note = f"regime={current_regime} (fallback)"

    msg_tf = (f"{symbol} - BB Width({BB_LENGTH},{BB_STD}) {timeframe_label} = {width}% "
              f"[p{int(BB_WIDTH_LOW_Q*100)}={low_thr}% p{int(BB_WIDTH_HIGH_Q*100)}={high_thr}% {regime_note}]")
    msg_tf = telegram.telegram_prefix_signals_sl + msg_tf
    print(msg_tf)
    telegram.send_telegram_message(telegram.telegram_token_main, '', msg_tf)

    if width <= low_thr or width >= high_thr:
        now = datetime.datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

        if width <= low_thr:
            add_note = f"Squeeze (<= p{int(BB_WIDTH_LOW_Q*100)} {low_thr}%)"
        else:
            add_note = f"Expansion (>= p{int(BB_WIDTH_HIGH_Q*100)} {high_thr}%)"

        msg = f"{TELEGRAM_PREFIX_SIGNAL} alert!\n{formatted_now}\n{symbol}\n{msg_tf}\nNote: {add_note}"
        msg = telegram.telegram_prefix_signals_sl + msg
        telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_INFORMATION, msg)

        signal_message = f"BB width {timeframe_label} {width}% ({regime_note})"
        database.add_signal_log(date=now,
                                signal="BB-Width",
                                signal_message=signal_message,
                                symbol=symbol,
                                notes=add_note)

def bb_width(symbol):
    _check_bb_width(symbol,
                    binance.get_client().KLINE_INTERVAL_1DAY,
                    "1D",
                    LOOKBACK_DAYS_1D)

def run():
    df_symbols = database.get_distinct_symbol_from_positions_where_position1()
    symbols_to_add = ['BTCUSDC', 'ETHUSDC']
    df_to_add = pd.DataFrame({'Symbol': symbols_to_add})
    df_symbols = pd.concat([df_symbols, df_to_add[~df_to_add['Symbol'].isin(df_symbols['Symbol'].values)]], ignore_index=True)
    if df_symbols.empty:
        print("There are no symbols to calculate.")
    else:
        symbols = df_symbols["Symbol"].to_list()
        for symbol in symbols:
            bb_width(symbol)

if __name__ == "__main__":
    run()
