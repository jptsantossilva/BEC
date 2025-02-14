import sys
import importlib

import pandas as pd

# from utils import telegram
# from utils import database
import utils.telegram as telegram
import utils.database as database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# global vars
stake_amount_type = None
max_number_of_open_positions = None
tradable_balance_ratio = None
min_position_size = None
trade_against = None
stop_loss = None
trade_top_performance = None
bot_1d = None
bot_4h = None
bot_1h = None
main_strategy = None
main_strategy_name = None
btc_strategy = None
btc_strategy_name = None
strategy_name = None
trade_against_switch = None
run_mode = None
bot_prefix = None

n_decimals = None

def get_setting(setting_name):
    """Fetch a setting from the database using the global connection."""
    return database.get_setting(database.conn, setting_name)

def set_trade_against(value):
    """Calls database function to set trade_against."""
    return database.set_trade_against(database.conn, value)

def set_setting(name, value):
    """Calls database function to set a setting."""
    return database.set_setting(database.conn, name, value)

# Function to handle errors (common code for error handling)
def handle_error(error, message):
    msg = f"Error: {message}. {sys._getframe().f_code.co_name} - {repr(error)}"
    print(msg)
    # logging.exception(msg)
    telegram.send_telegram_message(telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg)
    sys.exit(msg)

def get_all_settings():
        
    global stake_amount_type, max_number_of_open_positions, tradable_balance_ratio, min_position_size 
    global trade_against, stop_loss, trade_top_performance
    global bot_1d, bot_4h, bot_1h
    global main_strategy, main_strategy_name 
    global btc_strategy, btc_strategy_name, btc_strategy_backtest_optimize
    global trade_against_switch
    global strategy_id, strategy_name, strategy, strategy_backtest_optimize
    global take_profit_1_pnl_perc, take_profit_1_amount_perc, take_profit_2_pnl_perc, take_profit_2_amount_perc, take_profit_3_pnl_perc, take_profit_3_amount_perc, take_profit_4_pnl_perc, take_profit_4_amount_perc
    global run_mode
    global lock_values
    global bot_prefix

    # get settings from config file
    stake_amount_type            = get_setting("stake_amount_type")
    max_number_of_open_positions = get_setting("max_number_of_open_positions")
    tradable_balance_ratio       = get_setting("tradable_balance_ratio")
    min_position_size            = get_setting("min_position_size")
    trade_against                = get_setting("trade_against")
    stop_loss                    = get_setting("stop_loss")
    trade_top_performance        = get_setting("trade_top_performance")
    bot_1d                       = get_setting("bot_1d")
    bot_4h                       = get_setting("bot_4h")
    bot_1h                       = get_setting("bot_1h")
    main_strategy                = get_setting("main_strategy")
    btc_strategy                 = get_setting("btc_strategy")
    trade_against_switch         = get_setting("trade_against_switch")
    take_profit_1_pnl_perc       = get_setting("take_profit_1")
    take_profit_1_amount_perc    = get_setting("take_profit_1_amount")
    take_profit_2_pnl_perc       = get_setting("take_profit_2")
    take_profit_2_amount_perc    = get_setting("take_profit_2_amount")
    take_profit_3_pnl_perc       = get_setting("take_profit_3")
    take_profit_3_amount_perc    = get_setting("take_profit_3_amount")
    take_profit_4_pnl_perc       = get_setting("take_profit_4")
    take_profit_4_amount_perc    = get_setting("take_profit_4_amount")
    run_mode                     = get_setting("run_mode")
    lock_values                  = get_setting("lock_values")
    bot_prefix                   = get_setting("bot_prefix")
    trade_against_switch_stablecoin = get_setting("trade_against_switch_stablecoin")

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    df_main_strategy = database.get_strategy_by_id(database.conn, main_strategy)
    if not df_main_strategy.empty:
        main_strategy_name = str(df_main_strategy.Name.values[0])
        main_strategy_backtest_optimize = bool(df_main_strategy.Backtest_Optimize.values[0])

    df_btc_strategy = database.get_strategy_by_id(database.conn, btc_strategy)
    if not df_btc_strategy.empty:
        btc_strategy_name = str(df_btc_strategy.Name.values[0])
        btc_strategy_backtest_optimize = bool(df_btc_strategy.Backtest_Optimize.values[0])

    # if trade_against == "USDT":
    #     strategy_id = main_strategy
    #     strategy_name = main_strategy_name
    #     strategy_backtest_optimize = main_strategy_backtest_optimize
    # elif trade_against == "BTC":
    #     strategy_id = btc_strategy
    #     strategy_name = btc_strategy_name
    #     strategy_backtest_optimize = btc_strategy_backtest_optimize

    strategy_id = main_strategy
    strategy_name = main_strategy_name
    strategy_backtest_optimize = main_strategy_backtest_optimize

    # Dynamically import the entire strategies module
    strategy_module = importlib.import_module('my_backtesting')
    # Dynamically get the strategy class
    strategy = getattr(strategy_module, strategy_id)
    btc_strategy = getattr(strategy_module, btc_strategy)

    global n_decimals
    if trade_against == "BTC":
        n_decimals = 8
    elif trade_against in ["USDT", "USDC"]:    
        n_decimals = 2

get_all_settings()