import sys
from binance.client import Client
import config
import logging
import telegram

client = None

def connect():
    api_key = config.get_env_var('binance_api')
    api_secret = config.get_env_var('binance_secret')

    # Binance Client
    try:
        global client
        client = Client(api_key, api_secret)
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
            sys.exit(msg) 

connect()

def get_exchange_info():
    try:
        exchange_info = client.get_exchange_info()
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
            sys.exit(msg) 

    return exchange_info