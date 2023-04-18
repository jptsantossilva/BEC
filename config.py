import sys
import yaml
import telegram
import os
import pandas as pd

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

n_decimals = None

# get settings from config file
def get_setting(setting_name):
    try:
        with open("config.yaml", "r") as file:
            config = yaml.safe_load(file)

        setting_value = config[setting_name]

        return setting_value

    except FileNotFoundError as e:
        msg = "Error: The file config.yaml could not be found."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
        # sys.exit(msg) 

    except yaml.YAMLError as e:
        msg = "Error: There was an issue with the YAML file."
        msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
        sys.exit(msg)

# environment variables
def get_env_var(var_name):
    try:
        # Binance
        # api_key = os.environ.get('binance_api')
        # api_secret = os.environ.get('binance_secret')

        var_value = os.environ.get(var_name)
        return var_value
    
    except KeyError as e: 
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        # logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
        sys.exit(msg) 

def get_all_settings():
        
    global stake_amount_type, max_number_of_open_positions, tradable_balance_ratio, min_position_size, trade_against, stop_loss, trade_top_performance

    # get settings from config file
    stake_amount_type            = get_setting("stake_amount_type")
    max_number_of_open_positions = get_setting("max_number_of_open_positions")
    tradable_balance_ratio       = get_setting("tradable_balance_ratio")
    min_position_size            = get_setting("min_position_size")
    trade_against                = get_setting("trade_against")
    stop_loss                    = get_setting("stop_loss")
    trade_top_performance        = get_setting("trade_top_performance")

    global n_decimals
    if trade_against == "BTC":
        n_decimals = 8
    elif trade_against in ["BUSD","USDT"]:    
        n_decimals = 2

get_all_settings()