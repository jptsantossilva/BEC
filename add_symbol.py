import pandas as pd
import importlib

import utils.telegram as telegram
import utils.config as config
import utils.database as database
from my_backtesting import calc_backtesting


# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

timeframe = ["1d", "4h", "1h"]

def run():
    # get the symbols list not yet calculated 
    list_not_completed = database.get_symbols_to_calc_by_calc_completed(database.conn, completed = 0)

    # reset the index and set number beginning from 1
    list_not_completed = list_not_completed.reset_index(drop=True)
    list_not_completed.index += 1

    if not list_not_completed.empty: # not empty 
        msg = f"{telegram.telegram_prefix_market_phases_sl}Backtesting the following symbols:"
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
        msg = telegram.telegram_prefix_market_phases_sl +"\n"+ list_not_completed.to_string(index=True, header = False)
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg) 

    # Dynamically import the entire strategies module
    strategy_module = importlib.import_module('my_backtesting')
        
    df_strategies = database.get_strategies_for_main(database.conn)
    # Move row with current main strategy to the top to be the first to be calculated
    df_strategies = pd.concat([df_strategies[df_strategies['Id'] == config.strategy_id], df_strategies[df_strategies['Id'] != config.strategy_id]])

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
                result = calc_backtesting(symbol, tf, strategy=strategy, optimize=strategy_backtest_optimize)
                
                # get strategy backtesting results
                df_strategy_results = database.get_backtesting_results_by_symbol_timeframe_strategy(database.conn, symbol=symbol, time_frame=tf, strategy_id=strategy_id)

                # if return percentage of best ema is < 0 we dont want to trade that symbol pair
                if not df_strategy_results.empty:
                    backtest_return_percentage = int(df_strategy_results.Return_Perc.values[0])
                    if backtest_return_percentage < 0:
                        continue      

                # if the backtesting strategy is the one we are currently using, we want to add the symbol to positions table and update rank
                if strategy_id == config.strategy_id:
                    # initialize vars
                    ema_fast = 0
                    ema_slow = 0

                    if strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                        ema_fast = int(df_strategy_results.Ema_Fast.values[0])
                        ema_slow = int(df_strategy_results.Ema_Slow.values[0])  
                    
                    # if symbol do not exist in positions table then add it
                    symbol_exist = database.get_all_positions_by_bot_symbol(database.conn, bot=tf, symbol=symbol)
                    if not symbol_exist:
                        database.insert_position(
                            database.conn, 
                            bot=tf, 
                            symbol=symbol, 
                            ema_fast=ema_fast,
                            ema_slow=ema_slow
                        )
                    else:
                        # update rank
                        rank = database.get_rank_from_symbols_by_market_phase_by_symbol(database.conn, symbol)
                        database.set_rank_from_positions(database.conn, symbol=symbol, rank=rank)
                        # update best ema for those symbols with no positions open
                        if strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                            database.set_backtesting_results_from_positions(database.conn, symbol=symbol, timeframe=tf, ema_fast=ema_fast, ema_slow=ema_slow)
            
    # mark symbols as calc completed
    for symbol in list_not_completed.Symbol:    
        database.set_symbols_to_calc_completed(database.conn, symbol=symbol)

if __name__ == "__main__":
    # use the strategy from config file 
    # strategy = config.strategy
    # run(strategy=strategy)
    run()