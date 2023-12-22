import pandas as pd

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

    # get strategy name
    # strategy_name = database.get_strategy_name(database.conn, strategy_id=strategy.)
    

    if not list_not_completed.empty: # not empty 
        msg = f"{telegram.telegram_prefix_market_phases_sl}Calculating the best results for the strategy '{config.strategy_name}' for the following symbols:"
        
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
        msg = telegram.telegram_prefix_market_phases_sl +"\n"+ list_not_completed.to_string(index=True, header = False)
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg) 
    
    # calc BestEMA for each symbol and each time frame and save on positions table
    for symbol in list_not_completed.Symbol:
        for tf in timeframe: 

            # calc BestEMA
            resultBestEma = calc_backtesting(symbol, tf, strategy=config.strategy, optimize=config.strategy_backtest_optimize)
            
            # get strategy backtesting results
            df_strategy_results = database.get_backtesting_results_by_symbol_timeframe_strategy(database.conn, symbol=symbol, time_frame=tf, strategy_id=config.strategy_id)

            # if return percentage of best ema is < 0 we dont want to trade that coin pair
            if not df_strategy_results.empty:
                backtest_return_percentage = int(df_strategy_results.Return_Perc.values[0])
                if backtest_return_percentage < 0:
                    continue      

            
            # initialize vars
            ema_fast = 0
            ema_slow = 0

            if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                ema_fast = int(df_strategy_results.Ema_Fast.values[0])
                ema_slow = int(df_strategy_results.Ema_Slow.values[0])  
            
            # if symbol do not exist in positions table then add it
            symbol_exist = database.get_all_positions_by_bot_symbol(database.conn, bot=tf, symbol=symbol)
            if not symbol_exist:
                database.insert_position(database.conn, 
                                         bot=tf, 
                                         symbol=symbol, 
                                         ema_fast=ema_fast,
                                         ema_slow=ema_slow)
            else:
                # update rank
                rank = database.get_rank_from_symbols_by_market_phase_by_symbol(database.conn, symbol)
                database.set_rank_from_positions(database.conn, symbol=symbol, rank=rank)
                # update best ema
                if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                    database.set_backtesting_results_from_positions(database.conn, symbol=symbol, timeframe=tf, ema_fast=ema_fast, ema_slow=ema_slow)
        
        # mark as calc completed
        database.set_symbols_to_calc_completed(database.conn, symbol=symbol)

if __name__ == "__main__":
    # use the strategy from config file 
    # strategy = config.strategy
    # run(strategy=strategy)
    run()