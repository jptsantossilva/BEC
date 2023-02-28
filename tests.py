# %%


from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
import sys
import os

# Binance
api_key = os.environ.get('binance_api')
api_secret = os.environ.get('binance_secret')

stake_amount_type = "unlimited"
max_number_of_open_positions = 10
tradable_balance_ratio = 1
min_position_size = 0.001
coin = "BTC"

client = Client(api_key, api_secret)
balance = client.get_asset_balance(asset=coin)['free']

def calc_stake_amount(coin):

    if stake_amount_type == "unlimited":
        num_open_positions = 0

        # if error occurred
        if num_open_positions == -1:
            return 0
        if num_open_positions >= max_number_of_open_positions:
            return -2 

        try:
            balance = float(client.get_asset_balance(asset=coin)['free'])
            
        except BinanceAPIException as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            # logging.exception(msg)
            # telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            # logging.exception(msg)
            # telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
    
        tradable_balance = balance*tradable_balance_ratio 
        stake_amount = int(tradable_balance/(max_number_of_open_positions-num_open_positions))
        
        # make sure the size is >= the minimum size
        if stake_amount < min_position_size:
            stake_amount = min_position_size

        # make sure there are enough funds otherwise abort the buy position
        if balance < stake_amount:
            stake_amount = 0

        return stake_amount
    elif int(stake_amount_type) >= 0:
        return stake_amount_type
    else:
        return 0

coin = "BTC"
calc_stake_amount(coin)



# %%



