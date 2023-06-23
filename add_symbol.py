from best_ema import calc_best_ema
import pandas as pd
import utils.telegram as telegram
import utils.database as database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

timeframe = ["1d", "4h", "1h"]

def run():
    
    list_not_completed = database.get_symbols_to_calc_by_calc_completed(database.conn, completed = 0)

    # reset the index and set number beginning from 1
    list_not_completed = list_not_completed.reset_index(drop=True)
    list_not_completed.index += 1

    if not list_not_completed.empty: # not empty 
        msg = f"{telegram.telegram_prefix_market_phases_sl}Calculating best EMA for the following coins:"
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
        msg = telegram.telegram_prefix_market_phases_sl +"\n"+ list_not_completed.to_string(index=True, header = False)
        telegram.send_telegram_message(telegram.telegram_token_main, "", msg) 
    
    # calc BestEMA for each symbol and each time frame and save on positions table
    for symbol in list_not_completed.Symbol:
        for tf in timeframe: 

            # calc BestEMA
            resultBestEma = calc_best_ema(symbol, tf)
            
            # get best ema
            df_best_ema = database.get_best_ema_by_symbol_timeframe(database.conn, symbol=symbol, time_frame=tf)

            # if return percentage of best ema is < 0 we dont want to trade that coin pair
            if not df_best_ema.empty:
                if int(df_best_ema.Return_Perc.values[0]) < 0:
                    continue        
            
            # if symbol do not exist in positions table then add it
            symbol_exist = database.get_all_positions_by_bot_symbol(database.conn, bot=tf, symbol=symbol)
            if not symbol_exist:
                database.insert_position(database.conn, bot=tf, symbol=symbol)
            else:
            # if exist update rank
                rank = database.get_rank_from_symbols_by_market_phase_by_symbol(database.conn, symbol)
                database.set_rank_from_positions(database.conn, symbol=symbol, rank=rank)
        
        # mark as calc completed
        database.set_symbols_to_calc_completed(database.conn, symbol=symbol)

if __name__ == "__main__":
    run()