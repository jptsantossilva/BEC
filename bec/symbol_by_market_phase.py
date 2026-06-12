import pandas as pd
import datetime
import numpy as np
import sys
import logging
import timeit
import pytz
import importlib

import bec.utils.config as config
import bec.utils.database as database
import bec.exchanges.binance as binance
import bec.utils.telegram as telegram
import bec.utils.telegram_reporting as telegram_reporting
import bec.add_symbol as add_symbol
from bec.my_backtesting import calc_backtesting
from bec.strategy_builder import engine as strategy_engine

MARKET_PHASE_MIN_CANDLES = 200
# Last SMA200/ROC60 values only need the latest 200 closed candles plus the current row.
MARKET_PHASE_LOOKBACK_CANDLES = 201

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
    df['ROC_30'] = (df['Price'] / df['Price'].shift(30)) - 1
    df['ROC_60'] = (df['Price'] / df['Price'].shift(60)) - 1
    
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


def _market_phase_start_date(timeframe, now_utc=None):
    """Return the start date for the fixed market phase candle lookback."""
    timeframe_minutes = strategy_engine.TIMEFRAME_MINUTES.get(str(timeframe))
    if not timeframe_minutes:
        return None

    now_utc = now_utc or datetime.datetime.now(datetime.timezone.utc)
    return now_utc - datetime.timedelta(
        minutes=timeframe_minutes * MARKET_PHASE_LOOKBACK_CANDLES
    )


def set_market_phases_to_symbols(symbols, timeframe, warning_stats=None):
    """Compute market phase labels for a list of symbols."""
    # Empty dataframe
    df_result = pd.DataFrame()
    if warning_stats is None:
        warning_stats = {"warnings": 0}
    start_date = _market_phase_start_date(timeframe)

    for symbol in symbols:
        print("Calculating " + symbol)
        df = binance.get_close_df(
            symbol=symbol,
            interval=timeframe,
            start_date=start_date,
            include_symbol=True,
            price_col="Price",            
        )

        if df.empty:
            msg = (
                f"Skipping {symbol} ({timeframe}): no closed historical candles available. "
                "This is expected for newly listed symbols before the first candle closes."
            )
            print(msg)
            continue

        if len(df) < MARKET_PHASE_MIN_CANDLES:
            msg = (
                f"Skipping {symbol} ({timeframe}): only {len(df)} closed candles available; "
                f"{MARKET_PHASE_MIN_CANDLES} required for DSMA200."
            )
            print(msg)
            continue
        
        apply_technicals(df)
        # Last one is the one with 200DSMA value
        df = df.tail(1)

        required_columns = [
            "Price",
            "DSMA50",
            "DSMA200",
            "Perc_Above_DSMA50",
            "Perc_Above_DSMA200",
            "ROC_30",
            "ROC_60",
        ]
        if df[required_columns].isna().any(axis=None):
            msg = (
                f"Skipping {symbol} ({timeframe}): market phase indicators are incomplete. "
                f"{MARKET_PHASE_MIN_CANDLES} valid closed candles are required."
            )
            print(msg)
            continue

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
            return settings

        btc_strategy = settings.btc_strategy
        signal_timeframe = _infer_btc_auto_switch_signal_timeframe(
            btc_strategy,
            btc_timeframe,
        )
        candle_id = _get_btc_auto_switch_candle_id(btc_pair, signal_timeframe)
        if not candle_id:
            warning_stats["warnings"] = int(warning_stats.get("warnings", 0)) + 1
            telegram.send_error_event(
                action="trade against switch signal candle",
                symbol=btc_pair,
                timeframe=signal_timeframe,
                reason="Could not identify the closed signal candle.",
                impact="Trade-against auto-switch was skipped to avoid repeated execution.",
                next_step="Check Binance OHLCV availability for the selected Bitcoin Strategy timeframe.",
                notify_main=False,
            )
            return settings

        # get buy and sell conditions
        buy_condition, sell_condition = _evaluate_btc_auto_switch_strategy(
            btc_strategy,
            btc_pair,
            btc_timeframe,
        )

        # convert USDT/USDC to BTC
        if settings.trade_against in ["USDC", "USDT"] and buy_condition:
            if database.auto_switch_signal_processed(
                btc_strategy,
                btc_pair,
                "buy",
                signal_timeframe,
                candle_id,
            ):
                print(
                    "Auto-switch buy signal already processed: "
                    f"{btc_strategy} {btc_pair} {signal_timeframe} {candle_id}"
                )
                return settings
            
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
            database.record_auto_switch_signal(
                btc_strategy,
                btc_pair,
                "buy",
                signal_timeframe,
                candle_id,
            )
            settings = config.load_settings(refresh=True)

        # convert BTC to USDT/USDC
        elif settings.trade_against == "BTC" and sell_condition:
            if database.auto_switch_signal_processed(
                btc_strategy,
                btc_pair,
                "sell",
                signal_timeframe,
                candle_id,
            ):
                print(
                    "Auto-switch sell signal already processed: "
                    f"{btc_strategy} {btc_pair} {signal_timeframe} {candle_id}"
                )
                return settings

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
            telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_EXIT_TRADE, msg)

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
            database.record_auto_switch_signal(
                btc_strategy,
                btc_pair,
                "sell",
                signal_timeframe,
                candle_id,
            )
            settings = config.load_settings(refresh=True)

    return settings


def _infer_btc_auto_switch_signal_timeframe(strategy_id: str, base_timeframe: str = "1d") -> str:
    """Return the candle timeframe used to deduplicate BTC auto-switch signals."""
    try:
        definition = database.get_strategy_definition(strategy_id)
    except Exception as exc:
        print(f"Could not load BTC strategy definition for {strategy_id}: {exc}")
        return base_timeframe

    fixed_timeframes = [
        timeframe
        for timeframe in strategy_engine.ast_timeframes(definition)
        if timeframe != "current" and timeframe in strategy_engine.TIMEFRAME_MINUTES
    ]
    if not fixed_timeframes:
        return base_timeframe

    return max(
        fixed_timeframes,
        key=lambda timeframe: strategy_engine.TIMEFRAME_MINUTES[timeframe],
    )


def _get_btc_auto_switch_candle_id(symbol: str, timeframe: str) -> str:
    """Return the latest closed candle identifier for a BTC auto-switch signal."""
    df = binance.get_close_df(
        symbol=symbol,
        interval=timeframe,
        include_symbol=True,
        price_col="Price",
    )
    if df.empty:
        return ""

    candle_index = df.index[-1]
    try:
        timestamp = pd.to_datetime(candle_index)
        if getattr(timestamp, "tzinfo", None) is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(candle_index)


def _evaluate_btc_auto_switch_strategy(strategy_id: str, symbol: str, timeframe: str):
    """Evaluate declarative BTC strategy transitions for auto-switch decisions."""
    import bec.main as live_main

    df = live_main.get_data(symbol=symbol, timeframe=timeframe)
    if df.empty or len(df) < 2:
        return False, False

    strategy_context = live_main.get_strategy_runtime_context(
        strategy_id,
        symbol,
        timeframe,
    )
    parameters = strategy_context["parameters"]
    lastrow = df.iloc[-1]
    previous_df = df.iloc[:-1].copy()
    previous_lastrow = previous_df.iloc[-1]

    buy_current = live_main.get_strategy_buy_condition(
        strategy_id,
        symbol,
        timeframe,
        df,
        lastrow,
        parameters=parameters,
    )[0]
    buy_previous = live_main.get_strategy_buy_condition(
        strategy_id,
        symbol,
        timeframe,
        previous_df,
        previous_lastrow,
        parameters=parameters,
    )[0]
    sell_current = live_main.get_strategy_sell_condition(
        strategy_id,
        symbol,
        timeframe,
        df,
        lastrow,
        parameters=parameters,
    )
    sell_previous = live_main.get_strategy_sell_condition(
        strategy_id,
        symbol,
        timeframe,
        previous_df,
        previous_lastrow,
        parameters=parameters,
    )
    return bool(buy_current and not buy_previous), bool(sell_current and not sell_previous)


def _run_btc_auto_switch_backtest_if_needed(settings, timeframe):
    """Refresh the selected BTC strategy backtest only when its cached result is stale."""
    stats = {"evaluated": 0, "runs": 0, "skipped": 0, "failed": 0}
    if not settings.trade_against_switch:
        return stats

    stablecoin = settings.trade_against_switch_stablecoin
    btc_pair = f"BTC{stablecoin}"
    df_strategies_btc = database.get_strategies_for_btc()
    df_strategies_btc = df_strategies_btc[
        df_strategies_btc["Id"].astype(str) == str(settings.btc_strategy)
    ]
    if df_strategies_btc.empty:
        print(f"Skipping BTC auto-switch backtest: strategy {settings.btc_strategy} not found")
        return stats

    strategy_module = importlib.import_module("bec.my_backtesting")
    backtesting_settings = database.get_backtesting_settings()
    refresh_days = int(backtesting_settings.get("Candidate_Backtest_Refresh_Days", 7))

    for _, row in df_strategies_btc.iterrows():
        stats["evaluated"] += 1
        btc_strategy = row["Id"]
        optimize = bool(row["Backtest_Optimize"])
        work_fingerprint = database.build_backtesting_work_fingerprint(
            btc_strategy,
            optimize,
            backtesting_settings,
            strategy_row=row,
        )
        df_strategy_results = database.get_backtesting_results_by_symbol_timeframe_strategy(
            symbol=btc_pair,
            time_frame=timeframe,
            strategy_id=btc_strategy,
        )
        if add_symbol._backtest_result_current(
            df_strategy_results,
            work_fingerprint,
            refresh_days,
        ):
            stats["skipped"] += 1
            print(
                "Skipping BTC auto-switch backtest cache hit: "
                f"{btc_strategy} - {btc_pair} - {timeframe}"
            )
            continue

        btc_strategy_impl = (
            strategy_module.resolve_strategy(btc_strategy)
            if hasattr(strategy_module, "resolve_strategy")
            else getattr(strategy_module, btc_strategy)
        )
        if btc_strategy_impl is None:
            print(f"Skipping unavailable BTC strategy: {btc_strategy}")
            continue

        stats["runs"] += 1
        backtest_ok = calc_backtesting(
            symbol=btc_pair,
            time_frame=timeframe,
            strategy=btc_strategy_impl,
            optimize=optimize,
        )
        if not backtest_ok:
            stats["failed"] += 1
            print(f"BTC auto-switch backtest failed: {btc_strategy} - {btc_pair} - {timeframe}")

    return stats


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

    _run_btc_auto_switch_backtest_if_needed(settings, timeframe)
    
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
    for column in [
        "Symbol",
        "Price",
        "Market_Phase",
        "Perc_Above_DSMA50",
        "Perc_Above_DSMA200",
        "ROC_30",
        "ROC_60",
    ]:
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
            row['ROC_30'],
            row['ROC_60'],
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

    if not df_top.empty:
        # Remove symbols from positions table that are not top performers in accumulation or bullish phase
        database.delete_positions_not_top_rank()
        database.delete_inactive_position_candidates(settings.main_strategies)

        # Delete rows with calc completed and keep only symbols with calc not completed
        database.delete_symbols_to_calc_completed()

        # Add the symbols with open positions to calc
        database.add_symbols_with_open_positions_to_calc()

        # Add the symbols in top rank to calc
        database.add_symbols_top_rank_to_calc()

        # Calc best ema for each symbol on 1d, 4h and 1h time frame and save on positions table
        backtesting_stats = add_symbol.run(settings=settings)

        # Add any remaining top-rank candidates after backtesting refresh/cache validation.
        for strategy_id in settings.main_strategies:
            database.add_top_rank_to_positions(strategy_id=strategy_id)

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
    )
    msg = telegram.telegram_prefix_market_phases_sl + report
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

if __name__ == "__main__":
    time_frame, trade_against_value = read_arguments()
    main(timeframe=time_frame)
