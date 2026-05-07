import logging
import math
import sys
import time
import timeit
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from backtesting.lib import crossover
from dateutil.relativedelta import relativedelta

import exchanges.binance as binance
import utils.config as config
import utils.database as database
import utils.telegram as telegram

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p -",
)


# Global Vars
telegram_token = telegram.telegram_token_main

# sl = single line message; ml = multi line message
telegram_prefix_sl = ""
telegram_prefix_ml = ""

# strategy
# strategy_name = ''


def read_arguments():
    # total arguments
    n = len(sys.argv)

    if n < 2:
        print("Argument is missing")
        time_frame = input("Enter time frame (1d, 4h or 1h):")
        # run_mode = input('Enter run mode (test, prod):')
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]

        # run modes
        # test - does not execute orders on the exchange
        # prod - execute orders on the exchange
        # run_mode = sys.argv[2]

    return time_frame  # , run_mode


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
        # Invalid timeframe
        raise ValueError("Incorrect time frame. Use one of: 1h, 4h, 1d")


def get_backtesting_results(strategy_id, symbol, time_frame):

    # get best ema
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(
        symbol=symbol, time_frame=time_frame, strategy_id=strategy_id
    )

    if not df.empty:
        fast_ema = int(df.Ema_Fast.values[0])
        slow_ema = int(df.Ema_Slow.values[0])
    else:
        fast_ema = 0
        slow_ema = 0

    # if bestEMA does not exist return empty dataframe in order to no use that trading pair
    return fast_ema, slow_ema


def get_data(symbol, timeframe):

    # calc start date from were we need historical data candles
    # we do this to make sure we get same ema/sma values as those at tradingview
    # -------------------------------------
    today = datetime.now(timezone.utc)

    pastdate = None
    if timeframe == "1h":
        pastdate = today - relativedelta(hours=200 * 8)
    elif timeframe == "4h":
        # Use 4x hours to match 4h candles (approx. 1600 bars for 200*8)
        pastdate = today - relativedelta(hours=4 * 200 * 8)
    elif timeframe == "1d":
        pastdate = today - relativedelta(days=200 * 8)
    else:
        raise ValueError(
            f"Invalid timeframe '{timeframe}'. Expected one of: 1h, 4h, 1d."
        )
    
    start_ms = int(pastdate.timestamp() * 1000)

    # Trading uses only fully closed candles; drop the in-progress bar.
    df = binance.get_ohlcv(
        symbol=symbol,
        interval=timeframe,
        start_date=start_ms,
        drop_incomplete=True,
        keep_time_col=False,
    )

    if df.empty:
        msg = f"Failed after max tries to get historical data for {symbol} ({timeframe}). "
        msg = msg + sys._getframe().f_code.co_name + " - " + symbol
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        return pd.DataFrame()

    return df


# calculates moving averages
def apply_technicals(df, fast_ema=0, slow_ema=0):
    df["FastEMA"] = df["Close"].ewm(span=fast_ema, adjust=False).mean() if fast_ema > 0 else np.nan
    df["SlowEMA"] = df["Close"].ewm(span=slow_ema, adjust=False).mean() if slow_ema > 0 else np.nan
    df["SMA50"] = df["Close"].rolling(50).mean()
    df["SMA200"] = df["Close"].rolling(200).mean()


def calculate_wma(values, period):
    period = max(int(period), 1)
    weights = np.arange(1, period + 1)
    return pd.Series(values).rolling(period).apply(
        lambda prices: np.dot(prices, weights) / weights.sum(),
        raw=True,
    )


def calculate_hma(values, period):
    period = max(int(period), 1)
    half_period = max(int(period / 2), 1)
    sqrt_period = max(int(np.sqrt(period)), 1)
    values = pd.Series(values)
    return calculate_wma(
        2 * calculate_wma(values, half_period) - calculate_wma(values, period),
        sqrt_period,
    )


def calculate_rsi(values, period):
    period = max(int(period), 1)
    close = pd.Series(values)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(100).where(avg_loss != 0, 100)


def calculate_linreg(values, period):
    period = max(int(period), 1)
    x = np.arange(period, dtype=float)
    x_mean = x.mean()
    denominator = ((x - x_mean) ** 2).sum()

    def _linreg_endpoint(y):
        y_mean = y.mean()
        slope = ((x - x_mean) * (y - y_mean)).sum() / denominator if denominator else 0.0
        intercept = y_mean - slope * x_mean
        return intercept + slope * (period - 1)

    return pd.Series(values).rolling(period).apply(_linreg_endpoint, raw=True)


def apply_hma_rsi_linreg_technicals(df, fast_hma=16, slow_hma=65):
    df["HMAFast"] = calculate_hma(df["Close"], fast_hma)
    df["HMASlow"] = calculate_hma(df["Close"], slow_hma)
    df["RSI14"] = calculate_rsi(df["Close"], 14)


def get_daily_linreg_condition(symbol):
    df_daily = get_data(symbol=symbol, timeframe="1d")
    if df_daily.empty:
        return False, "Daily dataframe empty"

    df_daily["LINREG50"] = calculate_linreg(df_daily["Close"], 50)
    last_daily = df_daily.iloc[-1]
    if pd.isna(last_daily.LINREG50):
        return False, "Daily LINREG50 not ready"

    condition = float(last_daily.Close) > float(last_daily.LINREG50)
    detail = (
        f"Daily Close: {float(last_daily.Close):.8f} | "
        f"LINREG50: {float(last_daily.LINREG50):.8f} | Close>LINREG50: {condition}"
    )
    return condition, detail


def calculate_atr(df: pd.DataFrame, period: int) -> float:
    if period <= 0 or df.empty:
        return 0.0
    if not {"High", "Low", "Close"}.issubset(df.columns):
        return 0.0

    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [
            (df["High"] - df["Low"]).abs(),
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_series = tr.rolling(period, min_periods=period).mean()
    if atr_series.empty or pd.isna(atr_series.iloc[-1]):
        return 0.0
    return float(atr_series.iloc[-1])


# calc current pnl
def get_current_pnl(symbol, current_price, timeframe, position_id=None):

    try:
        # get buy price
        if position_id is not None:
            df_buy_price = database.get_position_by_id(position_id)
        else:
            df_buy_price = database.get_positions_by_bot_symbol_position(
                bot=timeframe, symbol=symbol, position=1
            )
        buy_price = 0
        pnl_perc = 0

        if not df_buy_price.empty:
            # get buy price
            buy_price = df_buy_price["Buy_Price"].iloc[0]
            # check if buy price is fulfilled
            if not math.isnan(buy_price) and buy_price > 0:
                # calc pnl percentage
                pnl_perc = ((current_price - buy_price) / buy_price) * 100
                pnl_perc = round(pnl_perc, 2)

        return pnl_perc

    except Exception as e:
        msg = sys._getframe().f_code.co_name + " - " + repr(e)
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)


def get_strategy_display_name(strategy_id: str) -> str:
    strategy_name = database.get_strategy_name(strategy_id)
    return strategy_name or strategy_id


def get_default_main_strategy_id(settings) -> str:
    return settings.main_strategies[0] if settings.main_strategies else ""


def strategy_uses_tuned_parameters(strategy_id: str) -> bool:
    return strategy_id in ["ema_cross_with_market_phases", "ema_cross", "hma_rsi_linreg"]


def get_strategy_parameters(strategy_id: str, symbol: str, timeframe: str, pos_row=None, prefer_position_params: bool = False):
    fast_ema = 0
    slow_ema = 0
    if strategy_uses_tuned_parameters(strategy_id):
        if prefer_position_params and pos_row is not None:
            params = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", ""))
            if strategy_id in ["ema_cross", "ema_cross_with_market_phases"]:
                fast_raw = params.get("ema_fast", 0)
                slow_raw = params.get("ema_slow", 0)
            elif strategy_id == "hma_rsi_linreg":
                fast_raw = params.get("hma_fast", 0)
                slow_raw = params.get("hma_slow", 0)
            else:
                fast_raw = params.get("fast", 0)
                slow_raw = params.get("slow", 0)
            fast_ema = 0 if pd.isna(fast_raw) else int(fast_raw)
            slow_ema = 0 if pd.isna(slow_raw) else int(slow_raw)
        if fast_ema == 0 or slow_ema == 0:
            fast_ema, slow_ema = get_backtesting_results(
                strategy_id=strategy_id, symbol=symbol, time_frame=timeframe
            )
        if (fast_ema == 0 or slow_ema == 0) and pos_row is not None:
            fast_raw = pos_row.get("Ema_Fast", 0)
            slow_raw = pos_row.get("Ema_Slow", 0)
            fast_ema = 0 if pd.isna(fast_raw) else int(fast_raw)
            slow_ema = 0 if pd.isna(slow_raw) else int(slow_raw)
    return fast_ema, slow_ema


def apply_strategy_technicals(df, strategy_id: str, fast_ema: int, slow_ema: int):
    apply_technicals(df, fast_ema, slow_ema)
    if strategy_id == "hma_rsi_linreg":
        apply_hma_rsi_linreg_technicals(df, fast_hma=fast_ema, slow_hma=slow_ema)


def get_strategy_sell_condition(strategy_id: str, df, lastrow):
    if strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
        return lastrow.SlowEMA > lastrow.FastEMA
    if strategy_id in ["market_phases"]:
        return (lastrow.Close < lastrow.SMA50) or (lastrow.Close < lastrow.SMA200)
    if strategy_id == "hma_rsi_linreg":
        return crossover(df.HMASlow, df.HMAFast)
    return False


def get_strategy_buy_condition(strategy_id: str, symbol: str, df, lastrow):
    detail = ""
    if strategy_id in ["ema_cross_with_market_phases"]:
        accumulation_phase = (
            (lastrow.Close > lastrow.SMA50)
            and (lastrow.Close > lastrow.SMA200)
            and (lastrow.SMA50 < lastrow.SMA200)
        )
        bullish_phase = (
            (lastrow.Close > lastrow.SMA50)
            and (lastrow.Close > lastrow.SMA200)
            and (lastrow.SMA50 > lastrow.SMA200)
        )
        condition_phase = accumulation_phase or bullish_phase
        return condition_phase and crossover(df.FastEMA, df.SlowEMA), detail

    if strategy_id in ["market_phases"]:
        accumulation_phase = (
            (lastrow.Close > lastrow.SMA50)
            and (lastrow.Close > lastrow.SMA200)
            and (lastrow.SMA50 < lastrow.SMA200)
        )
        bullish_phase = (
            (lastrow.Close > lastrow.SMA50)
            and (lastrow.Close > lastrow.SMA200)
            and (lastrow.SMA50 > lastrow.SMA200)
        )
        return accumulation_phase or bullish_phase, detail

    if strategy_id in ["ema_cross"]:
        return crossover(df.FastEMA, df.SlowEMA), detail

    if strategy_id == "hma_rsi_linreg":
        daily_linreg_condition, daily_linreg_detail = get_daily_linreg_condition(symbol)
        return (
            crossover(df.HMAFast, df.HMASlow)
            and float(lastrow.RSI14) > 52
            and daily_linreg_condition
        ), daily_linreg_detail

    return False, detail


def trade(timeframe, run_mode, settings=None):
    if settings is None:
        settings = config.load_settings(refresh=True)

    # Make sure we are only trying to buy positions on symbols included on market phases table
    database.delete_positions_not_top_rank()

    # list of symbols in position - SELL
    df_sell = database.get_positions_by_bot_position(bot=timeframe, position=1)

    # list of symbols in position - BUY
    df_buy = database.get_positions_by_bot_position(bot=timeframe, position=0)

    # if trading by time frame is
    # Enabled: Buy new positions and sell existing ones.
    # Disabled: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.disabled => Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
    if not database.is_trade_main_timeframe_enabled(timeframe):
        df_buy = df_buy.iloc[0:0].copy()

    # check open positions and SELL if conditions are fulfilled
    for _, pos_row in df_sell.iterrows():
        symbol = str(pos_row["Symbol"])
        position_id = int(pos_row["Id"])
        strategy_id = str(pos_row.get("Strategy_Id") or get_default_main_strategy_id(settings))
        strategy_name = str(pos_row.get("Strategy_Name") or get_strategy_display_name(strategy_id))

        # initialize vars
        fast_ema = 0
        slow_ema = 0

        # get best backtesting results for the strategy
        if strategy_uses_tuned_parameters(strategy_id):
            fast_ema, slow_ema = get_strategy_parameters(
                strategy_id,
                symbol,
                timeframe,
                pos_row=pos_row,
                prefer_position_params=True,
            )

            if fast_ema == 0 or slow_ema == 0:
                parameter_name = "HMA" if strategy_id == "hma_rsi_linreg" else "EMA"
                msg = f"{symbol} - {strategy_name} - Best {parameter_name} values missing"
                msg = telegram_prefix_sl + msg
                print(msg)
                telegram.send_telegram_message(
                    telegram_token, telegram.EMOJI_WARNING, msg
                )
                continue

        # get latest price data
        df = get_data(symbol=symbol, timeframe=timeframe)

        # Check if df is empty
        if df.empty:
            continue  # Skip to the next iteration

        apply_strategy_technicals(df, strategy_id, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        # Current price
        current_price = lastrow.Close

        # Current PnL
        current_pnl = get_current_pnl(symbol, current_price, timeframe, position_id=position_id)

        buy_price_raw = pos_row.get("Buy_Price", 0)
        highest_raw = pos_row.get("Highest_Price_Since_Entry", 0)
        trail_stop_raw = pos_row.get("Trail_Stop_ATR", 0)

        buy_price = 0.0 if pd.isna(buy_price_raw) else float(buy_price_raw)
        existing_highest = 0.0 if pd.isna(highest_raw) else float(highest_raw)
        existing_trail_stop = 0.0 if pd.isna(trail_stop_raw) else float(trail_stop_raw)

        atr_trailing_enabled = bool(settings.atr_trailing_enabled)
        atr_period = int(settings.atr_period)
        atr_multiplier = float(settings.atr_multiplier)
        atr_activation_pnl = float(settings.atr_activation_pnl)
        atr_value = calculate_atr(df, period=atr_period)

        base_highest = existing_highest if existing_highest > 0 else buy_price
        highest_price_since_entry = max(base_highest, float(current_price))

        hard_stop_price = (
            float(buy_price) * (1 - (float(settings.stop_loss) / 100))
            if float(settings.stop_loss) > 0 and float(buy_price) > 0
            else 0.0
        )

        trailing_started = existing_trail_stop > 0
        can_enable_trailing = (
            atr_trailing_enabled
            and atr_multiplier > 0
            and atr_value > 0
            and (trailing_started or current_pnl >= atr_activation_pnl)
        )

        trail_stop_atr = existing_trail_stop if can_enable_trailing else 0.0
        if can_enable_trailing:
            calc_trail_stop = highest_price_since_entry - (atr_multiplier * atr_value)
            calc_trail_stop = max(0.0, float(calc_trail_stop))
            trail_stop_atr = max(float(trail_stop_atr), float(calc_trail_stop))

        active_stop_price = hard_stop_price
        if trail_stop_atr > 0:
            active_stop_price = max(float(active_stop_price), float(trail_stop_atr))

        database.update_position_risk(
            bot=timeframe,
            symbol=symbol,
            highest_price_since_entry=highest_price_since_entry,
            trail_stop_atr=trail_stop_atr,
            position_id=position_id,
        )

        # if using stop loss (hard SL and/or ATR trailing stop)
        sell_stop_loss = False
        stop_loss_reason = f"Stop loss {settings.stop_loss}%"
        if active_stop_price > 0:
            sell_stop_loss = float(current_price) <= float(active_stop_price)
            if sell_stop_loss and trail_stop_atr > 0 and abs(active_stop_price - trail_stop_atr) < 1e-12:
                stop_loss_reason = (
                    f"ATR Trailing Stop - ATR({atr_period}) x {atr_multiplier:.2f} "
                    f"| Trigger: {active_stop_price:.8f}"
                )

        # if using take profit 1
        sell_tp_1 = False
        if settings.take_profit_1 > 0:
            tp1_raw = pos_row.get("Take_Profit_1", 0)
            tp1_occurred = 0 if pd.isna(tp1_raw) else int(tp1_raw)
            # if not occurred
            if tp1_occurred == 0:
                sell_tp_1 = current_pnl >= settings.take_profit_1

        # if using take profit 2
        sell_tp_2 = False
        if settings.take_profit_2 > 0:
            tp2_raw = pos_row.get("Take_Profit_2", 0)
            tp2_occurred = 0 if pd.isna(tp2_raw) else int(tp2_raw)
            # if not occurred
            if tp2_occurred == 0:
                sell_tp_2 = current_pnl >= settings.take_profit_2

        # if using take profit 3
        sell_tp_3 = False
        if settings.take_profit_3 > 0:
            tp3_raw = pos_row.get("Take_Profit_3", 0)
            tp3_occurred = 0 if pd.isna(tp3_raw) else int(tp3_raw)
            # if not occurred
            if tp3_occurred == 0:
                sell_tp_3 = current_pnl >= settings.take_profit_3

        # if using take profit 3
        sell_tp_4 = False
        if settings.take_profit_4 > 0:
            tp4_raw = pos_row.get("Take_Profit_4", 0)
            tp4_occurred = 0 if pd.isna(tp4_raw) else int(tp4_raw)
            # if not occurred
            if tp4_occurred == 0:
                sell_tp_4 = current_pnl >= settings.take_profit_4

        # check sell condition for the strategy
        sell_condition = get_strategy_sell_condition(strategy_id, df, lastrow)

        # set current PnL
        current_price = lastrow.Close
        database.update_position_pnl(
            bot=timeframe, symbol=symbol, curr_price=current_price, position_id=position_id
        )

        # Execute exactly one sell action by priority
        if (
            sell_stop_loss
            or sell_condition
            or sell_tp_1
            or sell_tp_2
            or sell_tp_3
            or sell_tp_4
        ):

            # stop loss
            if sell_stop_loss:
                binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    fast_ema=fast_ema,
                    slow_ema=slow_ema,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                    reason=stop_loss_reason,
                )
                continue

            # sell_codition
            if sell_condition:
                binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    fast_ema=fast_ema,
                    slow_ema=slow_ema,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                )
                continue

            # Take-Profits in cascade (TP1 -> TP2 -> TP3 -> TP4)
            # Each 'percentage' is applied to the remaining open position.
            # If any TP sells 100% of the position, stop immediately (skip further TPs).
            tp_steps = [
                (
                    1,
                    sell_tp_1,
                    settings.take_profit_1,
                    float(settings.take_profit_1_amount),
                ),
                (
                    2,
                    sell_tp_2,
                    settings.take_profit_2,
                    float(settings.take_profit_2_amount),
                ),
                (
                    3,
                    sell_tp_3,
                    settings.take_profit_3,
                    float(settings.take_profit_3_amount),
                ),
                (
                    4,
                    sell_tp_4,
                    settings.take_profit_4,
                    float(settings.take_profit_4_amount),
                ),
            ]

            tp_executed = False
            for tp_num, tp_flag, tp_pnl, tp_amount in tp_steps:
                if not tp_flag:
                    continue
                if tp_amount <= 0.0:
                    continue

                binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    fast_ema=fast_ema,
                    slow_ema=slow_ema,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                    reason=f"Take-Profit Level {tp_num} - {tp_pnl}% PnL - {tp_amount}% Amount",
                    percentage=tp_amount,
                    take_profit_num=tp_num,
                )

                tp_executed = True
                # If this level sold 100%, there's no remaining position; stop chaining TPs
                if tp_amount >= 100.0:
                    break

            # If any TP executed, skip the rest of this loop iteration (avoid redundant actions)
            if tp_executed:
                continue

        else:
            best_emas = (
                "" if fast_ema == 0 or slow_ema == 0 else f"{fast_ema}/{slow_ema}"
            )
            ema_debug = ""
            if strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                fast_ema_value = float(lastrow.FastEMA)
                slow_ema_value = float(lastrow.SlowEMA)
                ema_debug = (
                    f" | FastEMA: {fast_ema_value:.8f} | SlowEMA: {slow_ema_value:.8f} "
                    f"| Slow>Fast: {slow_ema_value > fast_ema_value}"
                )
            elif strategy_id == "hma_rsi_linreg":
                ema_debug = (
                    f" | HMA{fast_ema}: {float(lastrow.HMAFast):.8f} | HMA{slow_ema}: {float(lastrow.HMASlow):.8f} "
                    f"| HMA{fast_ema}<HMA{slow_ema} cross: {crossover(df.HMASlow, df.HMAFast)}"
                )

            msg = (
                f"{symbol} - {best_emas} {strategy_name} - Sell condition not fulfilled"
                f"{ema_debug}"
            )
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)

    # check symbols not in positions and BUY if conditions are fulfilled
    for _, pos_row in df_buy.iterrows():
        symbol = str(pos_row["Symbol"])
        position_id = int(pos_row["Id"])
        strategy_id = str(pos_row.get("Strategy_Id") or get_default_main_strategy_id(settings))
        strategy_name = str(pos_row.get("Strategy_Name") or get_strategy_display_name(strategy_id))

        # initialize vars
        fast_ema = 0
        slow_ema = 0

        # get best backtesting results for the strategy
        if strategy_uses_tuned_parameters(strategy_id):
            fast_ema, slow_ema = get_strategy_parameters(strategy_id, symbol, timeframe, pos_row=pos_row)

            if fast_ema == 0 or slow_ema == 0:
                parameter_name = "HMA" if strategy_id == "hma_rsi_linreg" else "EMA"
                msg = f"{symbol} - {strategy_name} - Best {parameter_name} values missing"
                msg = telegram_prefix_sl + msg
                print(msg)
                telegram.send_telegram_message(
                    telegram_token, telegram.EMOJI_WARNING, msg
                )
                continue

        df = get_data(symbol=symbol, timeframe=timeframe)

        # skip if no data
        if df.empty:
            msg = f"{symbol} - {strategy_name} - Empty dataframe on BUY loop"
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            continue

        apply_strategy_technicals(df, strategy_id, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        buy_condition, buy_detail = get_strategy_buy_condition(strategy_id, symbol, df, lastrow)

        if buy_condition:
            binance.create_buy_order(
                symbol=symbol,
                bot=timeframe,
                fast_ema=fast_ema,
                slow_ema=slow_ema,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                position_id=position_id,
            )
        else:
            best_emas = (
                "" if fast_ema == 0 or slow_ema == 0 else f"{fast_ema}/{slow_ema}"
            )

            msg = f"{symbol} - {best_emas} {strategy_name} - Buy condition not fulfilled"
            if strategy_id == "hma_rsi_linreg":
                msg = (
                    f"{symbol} - {fast_ema}/{slow_ema} {strategy_name} - Buy condition not fulfilled"
                    f" | HMA{fast_ema}>HMA{slow_ema} cross: {crossover(df.HMAFast, df.HMASlow)}"
                    f" | RSI14: {float(lastrow.RSI14):.2f} > 52: {float(lastrow.RSI14) > 52}"
                    f" | {buy_detail}"
                )
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)


def positions_summary(timeframe, settings=None):
    if settings is None:
        settings = config.load_settings(refresh=True)

    df_summary = database.get_positions_by_bot_position(bot=timeframe, position=1)

    # remove unwanted columns
    df_dropped = df_summary.drop(
        columns=[
            "Id",
            "Date",
            "Bot",
            "Position",
            "Rank",
            "Qty",
            "Ema_Fast",
            "Ema_Slow",
            "Buy_Order_Id",
            "Duration",
        ]
    )

    # sort by symbol
    df_sorted = df_dropped.sort_values("Symbol")

    # df_cp_to_print.rename(columns={"Currency": "Symbol", "Close": "Price", }, inplace=True)
    df_sorted.reset_index(
        drop=True, inplace=True
    )  # gives consecutive numbers to each row
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

    if settings.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions()
        msg = f"{str(num_open_positions)}/{str(settings.max_number_of_open_positions)} positions occupied"
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram_token, "", msg)


def run(timeframe, run_mode):
    settings = config.load_settings(refresh=True)

    # if timeframe == "1h" and not config.bot_1h:
    #     msg = f"Bot {timeframe} is inactive. Check Dashboard - Settings. Bye"
    #     print(msg)
    #     return
    # elif timeframe == "4h" and not config.bot_4h:
    #     msg = f"Bot {timeframe} is inactive. Check Dashboard - Settings. Bye"
    #     print(msg)
    #     return
    # elif timeframe == "1d" and not config.bot_1d:
    #     msg = f"Bot {timeframe} is inactive. Check Dashboard - Settings. Bye"
    #     print(msg)
    #     return

    # calculate program run time
    start = timeit.default_timer()

    # inform that bot has started
    msg = "Start"
    msg = telegram_prefix_sl + msg
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_START, msg)

    trade(timeframe, run_mode, settings=settings)

    positions_summary(timeframe, settings=settings)

    # exchange.create_balance_snapshot(telegram_prefix="")

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.calc_duration(total_seconds)

    msg = f"Execution Time: {duration}"
    msg = telegram_prefix_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)

    # inform that bot has finished
    msg = "End"
    msg = telegram_prefix_sl + msg
    print(msg)
    telegram.send_telegram_message(telegram_token, telegram.EMOJI_STOP, msg)


if __name__ == "__main__":
    time_frame = read_arguments()
    run_mode = config.load_settings(refresh=True).run_mode

    try:
        # Validate timeframe and prefixes first
        apply_arguments(time_frame)
    except ValueError as e:
        # Cron-friendly: log error and exit with code 2 (usage/config error)
        logging.error(str(e))
        try:
            telegram.send_telegram_message(
                telegram_token, telegram.EMOJI_WARNING, f"[FATAL] {e}"
            )
        except Exception:
            pass
        sys.exit(2)

    try:
        run(timeframe=time_frame, run_mode=run_mode)
    except Exception as e:
        # Unexpected runtime error: keep stacktrace in logs and notify
        logging.exception("Unhandled exception during bot run")
        try:
            telegram.send_telegram_message(
                telegram_token,
                telegram.EMOJI_WARNING,
                f"[FATAL] Unhandled exception: {e}",
            )
        except Exception:
            pass
        sys.exit(1)
