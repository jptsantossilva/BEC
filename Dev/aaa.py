# %%
import requests 
import os
import logging
import sys
import pandas as pd
# from telegram.error import BadRequest, NetworkError, TelegramError

# log file to store error messages
log_filename = "app.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')


telegram_chat_id = os.environ.get('telegram_chat_id')
telegramToken = os.environ.get('telegramToken1d') 

def telegram_send_message():    
    lmsg = "telegram_send_message"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML3",
    }
    try:
        resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken), params=params)
        resp.raise_for_status()

    except requests.exceptions.HTTPError as errh:
        msg = sys._getframe(  ).f_code.co_name+" - An Http Error occurred:" + repr(errh)
        print(msg)
        logging.exception(msg)
    except requests.exceptions.ConnectionError as errc:
        msg = sys._getframe(  ).f_code.co_name+" - An Error Connecting to the API occurred:" + repr(errc)
        print(msg)
        logging.exception(msg)
    except requests.exceptions.Timeout as errt:
        msg = sys._getframe(  ).f_code.co_name+" - A Timeout Error occurred:" + repr(errt)
        print(msg)
        logging.exception(msg)
    except requests.exceptions.RequestException as err:
        msg = sys._getframe(  ).f_code.co_name+" - An Unknown Error occurred" + repr(err)
        print(msg)
        logging.exception(msg)

gTimeFrameNum = 1
gtimeframeTypeShort = "m"

def read_csv_files():

    global df_positions
    global df_orders
    global df_best_ema

    
    try:
        # read positions
        filename = 'positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
        df_positions = pd.read_csv(filename)

        # read orders csv
        # we just want the header, there is no need to get all the existing orders.
        # at the end we will append the orders to the csv
        filename = 'orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
        df_orders = pd.read_csv(filename, nrows=0)

        # read best ema cross
        filename = 'coinpairBestEma.csv'
        df_best_ema = pd.read_csv(filename)

    except FileNotFoundError as e:
        print("Error: The file "+filename+" could not be found.")
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception("Error: The file "+filename+" could not be found.")
    except PermissionError:
        print("Error: You do not have permission to write to the file "+filename+".")
        logging.exception("Error: You do not have permission to write to the file "+filename+".")
    except Exception as e:
        # Log the error message for debugging purposes
        print(f'An unexpected error occurred: {str(e)}')
        logging.exception("Error: You do not have permission to write to the file "+filename+".")

# %%
print("BEGIN")
read_csv_files()
print("END")


