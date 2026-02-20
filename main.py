import logging
import math
import sys
import time
import timeit
from datetime import date, datetime, timezone

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
    df = binance.get_close_df(
        symbol=symbol,
        interval=timeframe,
        start_date=start_ms,
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
    df["FastEMA"] = df["Close"].ewm(span=fast_ema, adjust=False).mean()
    df["SlowEMA"] = df["Close"].ewm(span=slow_ema, adjust=False).mean()
    df["SMA50"] = df["Close"].rolling(50).mean()
    df["SMA200"] = df["Close"].rolling(200).mean()


# calc current pnl
def get_current_pnl(symbol, current_price, timeframe):

    try:
        # get buy price
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


def trade(timeframe, run_mode, settings=None):
    if settings is None:
        settings = config.load_settings(refresh=True)

    # Make sure we are only trying to buy positions on symbols included on market phases table
    database.delete_positions_not_top_rank()

    # list of symbols in position - SELL
    df_sell = database.get_positions_by_bot_position(bot=timeframe, position=1)
    list_to_sell = df_sell.Symbol.tolist()

    # list of symbols in position - BUY
    df_buy = database.get_positions_by_bot_position(bot=timeframe, position=0)
    list_to_buy = df_buy.Symbol.tolist()

    # if trading by time frame is
    # Enabled: Buy new positions and sell existing ones.
    # Disabled: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.disabled => Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
    if not database.is_trade_main_timeframe_enabled(timeframe):
        list_to_buy = []

    # check open positions and SELL if conditions are fulfilled
    for symbol in list_to_sell:

        # initialize vars
        fast_ema = 0
        slow_ema = 0

        # get best backtesting results for the strategy
        if settings.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            fast_ema, slow_ema = get_backtesting_results(
                strategy_id=settings.strategy_id, symbol=symbol, time_frame=timeframe
            )

            if fast_ema == 0 or slow_ema == 0:
                msg = f"{symbol} - {settings.strategy_name} - Best EMA values missing"
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

        apply_technicals(df, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        # Current price
        current_price = lastrow.Close

        # Current PnL
        current_pnl = get_current_pnl(symbol, current_price, timeframe)

        # if using stop loss
        sell_stop_loss = False
        if settings.stop_loss > 0:
            sell_stop_loss = current_pnl <= -settings.stop_loss

        # if using take profit 1
        sell_tp_1 = False
        if settings.take_profit_1 > 0:
            # check if tp1 occurred already
            # Filter
            df_tp1 = df_sell.loc[df_sell["Symbol"] == symbol, "Take_Profit_1"]
            # Extract the single value from the result (assuming only one row matches)
            tp1_occurred = df_tp1.values[0]
            # if not occurred
            if tp1_occurred == 0:
                sell_tp_1 = current_pnl >= settings.take_profit_1

        # if using take profit 2
        sell_tp_2 = False
        if settings.take_profit_2 > 0:
            # check if tp2 occurred already
            # Filter
            df_tp2 = df_sell.loc[df_sell["Symbol"] == symbol, "Take_Profit_2"]
            # Extract the single value from the result (assuming only one row matches)
            tp2_occurred = df_tp2.values[0]
            # if not occurred
            if tp2_occurred == 0:
                sell_tp_2 = current_pnl >= settings.take_profit_2

        # if using take profit 3
        sell_tp_3 = False
        if settings.take_profit_3 > 0:
            # check if tp3 occurred already
            # Filter
            df_tp3 = df_sell.loc[df_sell["Symbol"] == symbol, "Take_Profit_3"]
            # Extract the single value from the result (assuming only one row matches)
            tp3_occurred = df_tp3.values[0]
            # if not occurred
            if tp3_occurred == 0:
                sell_tp_3 = current_pnl >= settings.take_profit_3

        # if using take profit 3
        sell_tp_4 = False
        if settings.take_profit_4 > 0:
            # check if tp4 occurred already
            # Filter
            df_tp4 = df_sell.loc[df_sell["Symbol"] == symbol, "Take_Profit_4"]
            # Extract the single value from the result (assuming only one row matches)
            tp4_occurred = df_tp4.values[0]
            # if not occurred
            if tp4_occurred == 0:
                sell_tp_4 = current_pnl >= settings.take_profit_4

        # check sell condition for the strategy
        sell_condition = False
        if settings.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            condition_crossover = lastrow.SlowEMA > lastrow.FastEMA
            sell_condition = condition_crossover
        elif settings.strategy_id in ["market_phases"]:
            sell_condition = (lastrow.Close < lastrow.SMA50) or (
                lastrow.Close < lastrow.SMA200
            )

        # set current PnL
        current_price = lastrow.Close
        database.update_position_pnl(
            bot=timeframe, symbol=symbol, curr_price=current_price
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
                    reason=f"Stop loss {settings.stop_loss}%",
                )
                continue

            # sell_codition
            if sell_condition:
                binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    fast_ema=fast_ema,
                    slow_ema=slow_ema,
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
            msg = f"{symbol} - {best_emas} {settings.strategy_name} - Sell condition not fulfilled"
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, "", msg)

    # check symbols not in positions and BUY if conditions are fulfilled
    for symbol in list_to_buy:

        # initialize vars
        fast_ema = 0
        slow_ema = 0

        # get best backtesting results for the strategy
        if settings.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
            fast_ema, slow_ema = get_backtesting_results(
                strategy_id=settings.strategy_id, symbol=symbol, time_frame=timeframe
            )

            if fast_ema == 0 or slow_ema == 0:
                msg = f"{symbol} - {settings.strategy_name} - Best EMA values missing"
                msg = telegram_prefix_sl + msg
                print(msg)
                telegram.send_telegram_message(
                    telegram_token, telegram.EMOJI_WARNING, msg
                )
                continue

        df = get_data(symbol=symbol, timeframe=timeframe)

        # skip if no data
        if df.empty:
            msg = f"{symbol} - {settings.strategy_name} - Empty dataframe on BUY loop"
            msg = telegram_prefix_sl + msg
            print(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)

        # last row
        lastrow = df.iloc[-1]

        # check buy condition for the strategy
        if settings.strategy_id in ["ema_cross_with_market_phases"]:

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
            condition_crossover = crossover(df.FastEMA, df.SlowEMA)
            buy_condition = condition_phase and condition_crossover

        elif settings.strategy_id in ["market_phases"]:

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

            buy_condition = accumulation_phase or bullish_phase

        elif settings.strategy_id in ["ema_cross"]:
            buy_condition = crossover(df.FastEMA, df.SlowEMA)

        if buy_condition:
            binance.create_buy_order(
                symbol=symbol, bot=timeframe, fast_ema=fast_ema, slow_ema=slow_ema
            )
        else:
            best_emas = (
                "" if fast_ema == 0 or slow_ema == 0 else f"{fast_ema}/{slow_ema}"
            )

            msg = f"{symbol} - {best_emas} {settings.strategy_name} - Buy condition not fulfilled"
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
