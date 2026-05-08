import pandas as pd
import importlib

import bec.utils.telegram as telegram
import bec.utils.config as config
import bec.utils.database as database
from bec.my_backtesting import calc_backtesting


# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

timeframe = ["1d", "4h", "1h"]

def run(settings=None):
    if settings is None:
        settings = config.load_settings(refresh=True)

    # get the symbols list not yet calculated 
    list_not_completed = database.get_symbols_to_calc_by_calc_completed( completed = 0)

    # reset the index and set number beginning from 1
    list_not_completed = list_not_completed.reset_index(drop=True)
    list_not_completed.index += 1

    if not list_not_completed.empty: # not empty 
        msg = f"{telegram.telegram_prefix_market_phases_sl}Backtesting the following symbols:"
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
        msg = telegram.telegram_prefix_market_phases_sl +"\n"+ list_not_completed.to_string(index=True, header = False)
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg) 

    # Dynamically import the entire strategies module
    strategy_module = importlib.import_module("bec.my_backtesting")
        
    df_strategies = database.get_strategies_for_main()
    selected_strategy_ids = list(settings.main_strategies)
    selected_strategy_set = set(selected_strategy_ids)
    selected_order = {strategy_id: idx for idx, strategy_id in enumerate(selected_strategy_ids)}
    df_strategies["_Selected_Order"] = df_strategies["Id"].map(selected_order).fillna(9999)
    df_strategies = df_strategies.sort_values(["_Selected_Order", "Id"]).drop(columns=["_Selected_Order"])

    for index, row in df_strategies.iterrows():    
        # Dynamically get the strategy class
        strategy_id = row["Id"]
        strategy_name = row["Name"]
        strategy_backtest_optimize = row["Backtest_Optimize"]
        strategy = getattr(strategy_module, strategy_id)

        msg = f"{telegram.telegram_prefix_market_phases_sl}Backtesting the strategy {strategy_name}"
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
    
        # calc BestEMA for each symbol and each time frame and save on positions table
        for symbol in list_not_completed.Symbol:
            for tf in timeframe: 
                # backtesting
                calc_backtesting(symbol, tf, strategy=strategy, optimize=strategy_backtest_optimize)
                
                # get strategy backtesting results
                df_strategy_results = database.get_backtesting_results_by_symbol_timeframe_strategy( symbol=symbol, time_frame=tf, strategy_id=strategy_id)

                # check backtest approval rules
                approved = False
                reasons = []
                if not df_strategy_results.empty:
                    approved, reasons = database.is_backtest_approved(tf, df_strategy_results.iloc[0])
                else:
                    reasons = ["Missing_Backtest"]

                database.set_backtesting_approval(
                    symbol=symbol,
                    time_frame=tf,
                    strategy_id=strategy_id,
                    trading_approved=approved,
                    trading_rejection_reasons="" if approved else ";".join(reasons),
                )

                # Keep EMAs in Positions synchronized with latest backtesting results
                # even when a symbol is rejected by approval rules.
                if (
                    strategy_id in selected_strategy_set
                    and strategy_id in ["ema_cross_with_market_phases", "ema_cross", "hma_rsi_linreg"]
                    and not df_strategy_results.empty
                ):
                    ema_fast = int(df_strategy_results.Ema_Fast.values[0])
                    ema_slow = int(df_strategy_results.Ema_Slow.values[0])
                    symbol_exist = database.get_all_positions_by_bot_symbol_strategy(bot=tf, symbol=symbol, strategy_id=strategy_id)
                    if symbol_exist:
                        database.set_backtesting_results_from_position_strategy(
                            symbol=symbol,
                            timeframe=tf,
                            strategy_id=strategy_id,
                            ema_fast=ema_fast,
                            ema_slow=ema_slow,
                        )

                if not approved:
                    print(f"{symbol} {tf} rejected by approval rules: {reasons}")
                    continue

                # If the backtesting strategy is selected for trading, add/update its candidate row.
                if strategy_id in selected_strategy_set:
                    # initialize vars
                    ema_fast = 0
                    ema_slow = 0

                    if strategy_id in ["ema_cross_with_market_phases", "ema_cross", "hma_rsi_linreg"]:
                        ema_fast = int(df_strategy_results.Ema_Fast.values[0])
                        ema_slow = int(df_strategy_results.Ema_Slow.values[0])  
                    strategy_params_json = database.build_strategy_params_json(strategy_id, ema_fast, ema_slow)
                    
                    # if symbol do not exist in positions table then add it
                    symbol_exist = database.get_all_positions_by_bot_symbol_strategy(bot=tf, symbol=symbol, strategy_id=strategy_id)
                    if not symbol_exist:
                        database.insert_position(
                            bot=tf,
                            symbol=symbol,
                            ema_fast=ema_fast,
                            ema_slow=ema_slow,
                            strategy_id=strategy_id,
                            strategy_name=strategy_name,
                            strategy_params_json=strategy_params_json,
                        )
                    else:
                        # update rank
                        rank = database.get_rank_from_symbols_by_market_phase_by_symbol(symbol)
                        database.set_rank_from_positions(symbol=symbol, rank=rank)
                        # update best ema for those symbols with no positions open
                        if strategy_id in ["ema_cross_with_market_phases", "ema_cross", "hma_rsi_linreg"]:
                            database.set_backtesting_results_from_position_strategy(symbol=symbol, timeframe=tf, strategy_id=strategy_id, ema_fast=ema_fast, ema_slow=ema_slow)
            
    # mark symbols as calc completed
    for symbol in list_not_completed.Symbol:    
        database.set_symbols_to_calc_completed( symbol=symbol)

if __name__ == "__main__":
    run(settings=config.load_settings(refresh=True))
