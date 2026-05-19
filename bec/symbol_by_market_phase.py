import pandas as pd
import datetime
import numpy as np
import sys
import logging
import timeit
import pytz
import importlib

from backtesting.lib import crossover

import bec.utils.config as config
import bec.utils.database as database
import bec.exchanges.binance as binance
import bec.utils.telegram as telegram
import bec.utils.telegram_reporting as telegram_reporting
import bec.add_symbol as add_symbol
from bec.my_backtesting import calc_backtesting, get_backtesting_results

def get_blacklist(settings=None):
    """Return set of blacklisted symbols with trade-against suffix applied."""
    if settings is None:
        settings = config.load_settings()

    # Read symbols from blacklist to not trade
    df_blacklist = database.get_symbol_blacklist()
    blacklist = set()
    if not df_blacklist.empty:
        trade_against_suffix = settings.trade_against
        df_blacklist['Symbol'] = df_blacklist['Symbol'].astype(str) + trade_against_suffix
        # Convert blacklist to set
        blacklist = set(df_blacklist["Symbol"].unique())
    return blacklist


def apply_technicals(df):
    """Add market phase technical indicators to the dataframe."""
    if df.empty:
        return
    
    df['DSMA50'] = df['Price'].rolling(50).mean()
    df['DSMA200'] = df['Price'].rolling(200).mean()

    df['Perc_Above_DSMA50'] = ((df['Price'] - df['DSMA50']) / df['DSMA50']) * 100
    df['Perc_Above_DSMA200'] = ((df['Price'] - df['DSMA200']) / df['DSMA200']) * 100
    
def read_arguments(settings=None):    
    """Read CLI args for timeframe and trade-against settings."""
    if settings is None:
        settings = config.load_settings()

    # Arguments
    n = len(sys.argv)

    # Optional CLI arg: timeframe (1d, 4h, 1h). Defaults to 1d.
    if n < 2:
        time_frame = "1d"
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]
        # trade_against = sys.argv[2]

    trade_against_value = settings.trade_against

    return time_frame, trade_against_value

def get_symbols(trade_against, settings=None):
    """Return tradable symbols for the given quote asset."""
    if settings is None:
        settings = config.load_settings()

    # Get blacklist
    blacklist = get_blacklist(settings=settings)

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
            and s['status'] == 'TRADING'
        ):
            symbols.add(s['symbol'])

    # From the symbols to trade, exclude symbols from blacklist
    symbols -= blacklist

    symbols = sorted(symbols)
    return symbols

def set_market_phases_to_symbols(symbols, timeframe, warning_stats=None):
    """Compute market phase labels for a list of symbols."""
    # Empty dataframe
    df_result = pd.DataFrame()
    if warning_stats is None:
        warning_stats = {"warnings": 0}

    for symbol in symbols:
        print("Calculating " + symbol)
        df = binance.get_close_df(
            symbol=symbol,
            interval=timeframe,
            include_symbol=True,
            price_col="Price",            
        )

        if df.empty:
            warning_stats["warnings"] = int(warning_stats.get("warnings", 0)) + 1
            msg = f"Failed after max tries to get historical data for {symbol} ({timeframe}). "
            msg = msg + sys._getframe().f_code.co_name + " - " + symbol
            print(msg)
            telegram.send_error_event(
                action="market phase OHLCV load",
                symbol=symbol,
                timeframe=timeframe,
                reason="Historical dataframe is empty after retries.",
                impact="Symbol skipped from market phase ranking.",
                next_step="Check Binance data availability and local OHLCV cache.",
                notify_main=False,
            )
            continue
        
        apply_technicals(df)
        # Last one is the one with 200DSMA value
        df = df.tail(1)

        if df_result.empty:
            df_result = df
        else:
            df_result = pd.concat([df_result, df])

    if df_result.empty:
        return df_result

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
    df_result['Market_Phase'] = np.select(conditions, values, default="unknown")

    return df_result

def trade_against_auto_switch(settings=None, warning_stats=None):
    """Auto-switch trading quote asset between stablecoin and BTC based on market regime."""
    if settings is None:
        settings = config.load_settings(refresh=True)
    if warning_stats is None:
        warning_stats = {"warnings": 0}

    if settings.trade_against_switch:
        stablecoin = settings.trade_against_switch_stablecoin
        btc_pair = f"BTC{stablecoin}"
        btc_timeframe = "1d"
        sell_timeframes = ["1d", "4h", "1h"]
        sell_message = "Trade against auto switch"

        df_btc = binance.get_close_df(
            symbol=btc_pair,
            interval=btc_timeframe,
            include_symbol=True,
            price_col="Price",
        )

        if df_btc.empty:
            warning_stats["warnings"] = int(warning_stats.get("warnings", 0)) + 1
            msg = f"Failed after max tries to get historical data for {btc_pair} ({btc_timeframe}). "
            print(msg)
            telegram.send_error_event(
                action="trade against switch OHLCV load",
                symbol=btc_pair,
                timeframe=btc_timeframe,
                reason="Historical dataframe is empty after retries.",
                impact="Trade-against auto-switch was skipped for this run.",
                next_step="Check Binance data availability and rerun market phase job.",
                notify_main=False,
            )
            return settings
        

        if df_btc.empty or len(df_btc) < 2:
            return

        btc_strategy = settings.btc_strategy

        # get buy and sell conditions
        buy_condition = False
        sell_condition = False
        if btc_strategy in ['ema_cross']:

            fast_ema, slow_ema = get_backtesting_results(strategy_id=btc_strategy, symbol=btc_pair, time_frame=btc_timeframe)
            
            # technical indicators
            df_btc['FastEma'] = df_btc['Price'].ewm(span=fast_ema, adjust=False).mean()
            df_btc['SlowEma'] = df_btc['Price'].ewm(span=slow_ema, adjust=False).mean()

            buy_condition = crossover(df_btc.FastEma, df_btc.SlowEma)
            sell_condition = crossover(df_btc.SlowEma, df_btc.FastEma)

        elif btc_strategy in ["market_phases"]:

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

        # convert USDT/USDC to BTC
        if settings.trade_against in ["USDC", "USDT"] and buy_condition:
            
            msg = telegram_reporting.format_trade_against_switch_event(
                direction=f"{settings.trade_against} -> BTC",
                reason=f"{btc_pair} entered a bullish/accumulation regime.",
                actions=[
                    "Sell all open positions to the current stablecoin.",
                    "Release locked balance values.",
                    f"Convert available {settings.trade_against} balance to BTC.",
                    "Update trade_against to BTC.",
                ],
            )
            msg = telegram.telegram_prefix_signals_sl + msg
            telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_ENTER_TRADE, msg)

            # sell all positions to USDT/USDC
            for tf in sell_timeframes:
                df_sell = database.get_positions_by_bot_position( bot=tf, position=1)
                for _, pos_row in df_sell.iterrows():
                    binance.create_sell_order(
                        symbol=str(pos_row["Symbol"]),
                        bot=tf,
                        reason=f"{sell_message}",
                        strategy_id=str(pos_row.get("Strategy_Id") or ""),
                        strategy_name=str(pos_row.get("Strategy_Name") or ""),
                        position_id=int(pos_row["Id"]),
                    )
            
            # release locked values from the balance
            database.release_all_values()
            
            # convert all USDT/USDC to BTC
            btc_pair_to_buy = f"BTC{settings.trade_against}"
            binance.create_buy_order(symbol=btc_pair_to_buy, bot=btc_timeframe, convert_all_balance=True)
                
            # change trade against to BTC
            config.update_settings({"trade_against": "BTC"})
            min_position_size = 0.0001
            config.update_settings({"min_position_size": min_position_size})
            settings = config.load_settings(refresh=True)

        # convert BTC to USDT/USDC
        elif settings.trade_against == "BTC" and sell_condition:
            msg = telegram_reporting.format_trade_against_switch_event(
                direction=f"BTC -> {settings.trade_against_switch_stablecoin}",
                reason=f"{btc_pair} left the bullish/accumulation regime.",
                actions=[
                    "Sell all BTC-quoted open positions.",
                    "Release locked balance values.",
                    f"Convert available BTC balance to {settings.trade_against_switch_stablecoin}.",
                    f"Update trade_against to {settings.trade_against_switch_stablecoin}.",
                ],
            )
            msg = telegram.telegram_prefix_signals_sl + msg
            telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_ENTER_TRADE, msg)

            # sell all positions to BTC
            for tf in sell_timeframes:
                df_sell = database.get_positions_by_bot_position( bot=tf, position=1)
                for _, pos_row in df_sell.iterrows():
                    binance.create_sell_order(
                        symbol=str(pos_row["Symbol"]),
                        bot=tf,
                        reason=f"{sell_message}",
                        strategy_id=str(pos_row.get("Strategy_Id") or ""),
                        strategy_name=str(pos_row.get("Strategy_Name") or ""),
                        position_id=int(pos_row["Id"]),
                    )
                    
            # release locked values from the balance
            database.release_all_values()

            # convert all BTC to Stablecoin
            btc_pair_to_sell = f"BTC{settings.trade_against_switch_stablecoin}"
            binance.create_sell_order(symbol=btc_pair_to_sell, bot=btc_timeframe, convert_all_balance=True, reason=f"{sell_message}")

            # change trade against to stablecoin
            config.update_settings({"trade_against": settings.trade_against_switch_stablecoin})
            min_position_size = 20
            config.update_settings({"min_position_size": min_position_size})
            settings = config.load_settings(refresh=True)

    return settings

def main(timeframe):
    """Run market phase scan, reporting, and updates."""
    settings = config.load_settings(refresh=True)
    warning_stats = {"warnings": 0}
    backtesting_stats = {}

    # Calculate program run time
    start = timeit.default_timer()
    print(f"MKT {timeframe} started")

    # Log file to store error messages
    log_filename = "symbol_by_market_phase.log"
    logging.basicConfig(filename=log_filename, level=logging.INFO,
                        format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

    # create daily balance snapshot
    binance.create_balance_snapshot(telegram_prefix="", notify=False)

    # run backtesting for all available BTC Strategies 
    stablecoin = settings.trade_against_switch_stablecoin
    btc_pair = f"BTC{stablecoin}"
    df_strategies_btc = database.get_strategies_for_btc()
    for index, row in df_strategies_btc.iterrows():    
        # Dynamically import the entire strategies module
        strategy_module = importlib.import_module("bec.my_backtesting")
        # Dynamically get the strategy class
        btc_strategy = row['Id']
        btc_strategy_impl = (
            strategy_module.resolve_strategy(btc_strategy)
            if hasattr(strategy_module, "resolve_strategy")
            else getattr(strategy_module, btc_strategy)
        )
        if btc_strategy_impl is None:
            print(f"Skipping unavailable BTC strategy: {btc_strategy}")
            continue

        # run backtesting
        calc_backtesting(
            symbol=btc_pair, 
            time_frame=timeframe,
            strategy=btc_strategy_impl,
            optimize=bool(row['Backtest_Optimize'])
        )
    
    # Automatically switch trade against
    settings = trade_against_auto_switch(settings=settings, warning_stats=warning_stats)

    settings = config.load_settings(refresh=True)
    trade_against = settings.trade_against

    symbols = get_symbols(trade_against=trade_against, settings=settings)
    msg = str(len(symbols)) + " symbols found. Calculating market phases..."
    print(msg)

    df_result = set_market_phases_to_symbols(symbols, timeframe, warning_stats=warning_stats)

    # Keep those in accumulation or bullish phases
    if df_result.empty:
        df_union = pd.DataFrame()
    else:
        df_union = df_result.query("Market_Phase in ['bullish', 'accumulation']")

    if df_union.empty or "Perc_Above_DSMA200" not in df_union.columns:
        df_top = pd.DataFrame()
    else:
        df_top = df_union.sort_values(by=['Perc_Above_DSMA200'], ascending=False)
    df_top = df_top.head(settings.trade_top_performance)
    for column in ["Symbol", "Price", "Market_Phase", "Perc_Above_DSMA50", "Perc_Above_DSMA200"]:
        if column not in df_top.columns:
            df_top[column] = pd.Series(dtype="object")

    # Set rank for highest strength
    df_top['Rank'] = np.arange(len(df_top)) + 1

    # Delete existing data
    database.delete_all_symbols_by_market_phase()

    # Insert new symbols
    for index, row in df_top.iterrows():
        database.insert_symbols_by_market_phase(
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
    database.insert_symbols_by_market_phase_historical(formatted_date)

    selected_columns = df_top[["Symbol", "Price", "Market_Phase"]]
    df_top_print = selected_columns.copy()
    # Reset the index and set number beginning from 1
    df_top_print = df_top_print.reset_index(drop=True)
    df_top_print.index += 1

    print(f"Top {str(settings.trade_top_performance)} performance symbols:")
    print(df_top_print.to_string(index=True))

    # Create file to import to TradingView with the list of top performers and symbols in position
    df_tv_list = database.get_distinct_symbol_by_market_phase_and_positions()
    df_top = df_tv_list
    df_tv_list['symbol'] = "BINANCE:" + df_tv_list['symbol']
    # Write DataFrame to CSV file
    filename = "Top_performers_" + trade_against + ".txt"
    df_tv_list.to_csv(filename, header=False, index=False)

    if not df_top.empty:
        # Remove symbols from positions table that are not top performers in accumulation or bullish phase
        database.delete_positions_not_top_rank()

        # Add top rank symbols to positions files for every selected trading strategy.
        for strategy_id in settings.main_strategies:
            database.add_top_rank_to_positions(strategy_id=strategy_id)

        # Delete rows with calc completed and keep only symbols with calc not completed
        database.delete_symbols_to_calc_completed()

        # Add the symbols with open positions to calc
        database.add_symbols_with_open_positions_to_calc()

        # Add the symbols in top rank to calc
        database.add_symbols_top_rank_to_calc()

        # Calc best ema for each symbol on 1d, 4h and 1h time frame and save on positions table
        backtesting_stats = add_symbol.run(settings=settings)

    else:
        # if there are no symbols in accumulation or bullish phase remove all not open from positions
        database.delete_all_positions_not_open()

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.calc_duration(total_seconds)
    report = telegram_reporting.format_market_phase_report(
        timeframe=timeframe,
        trade_against=trade_against,
        duration=duration,
        symbols_scanned=len(symbols),
        df_result=df_result,
        df_top=df_top,
        backtesting_stats=backtesting_stats,
        warnings=int(warning_stats.get("warnings", 0)),
        tradingview_attached=True,
    )
    msg = telegram.telegram_prefix_market_phases_sl + report
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
    telegram.send_telegram_file(telegram.telegram_token_main, filename)

if __name__ == "__main__":
    time_frame, trade_against_value = read_arguments()
    main(timeframe=time_frame)
