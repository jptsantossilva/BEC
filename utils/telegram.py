import requests
import os
import sys
import logging
import yaml

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

# get settings from config file
# get trade_against to know which telegram bots to use (BUSD or BTC)
try:
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    trade_against = config["trade_against"]

except FileNotFoundError as e:
    msg = "Error: The file config.yaml could not be found."
    msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    sys.exit(msg) 

except yaml.YAMLError as e:
    msg = "Error: There was an issue with the YAML file."
    msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
    print(msg)
    logging.exception(msg)
    sys.exit(msg) 

# emoji
EMOJI_START = u'\U000025B6'
EMOJI_STOP = u'\U000023F9'
EMOJI_WARNING = u'\U000026A0'
EMOJI_ENTER_TRADE = u'\U0001F91E' # crossfingers
EMOJI_EXIT_TRADE = u'\U0001F91E' # crossfingers
EMOJI_TRADE_WITH_PROFIT = u'\U0001F44D' # thumbs up
EMOJI_TRADE_WITH_LOSS = u'\U0001F44E' # thumbs down
EMOJI_INFORMATION = u'\U00002139'

telegram_chat_id = ""
telegram_token_closed_position = ""
telegram_token_errors = ""
telegram_token_main = ""
telegram_token_signals = ""

# telegram timeout 5 seg
telegram_timeout = 5

# telegram messages prefix to identify the process sending the message
telegram_prefix_market_phases_sl = "MKT - "
telegram_prefix_market_phases_ml = "MKT\n"

telegram_prefix_signals_sl = "SGN - "
telegram_prefix_signals_ml = "SGN\n"

telegram_prefix_errors_sl = "ERR - "
telegram_prefix_errors_ml = "ERR\n"

telegram_prefix_bot_1d_sl = "B1D - "
telegram_prefix_bot_1d_ml = "B1D\n"

telegram_prefix_bot_4h_sl = "B4H - "
telegram_prefix_bot_4h_ml = "B4H\n"

telegram_prefix_bot_1h_sl = "B1H - "
telegram_prefix_bot_1h_ml = "B1H\n"

def read_env_var():
    # environment variables
    
    global telegram_chat_id
    global telegram_token_closed_position
    global telegram_token_errors
    global telegram_token_main
    global telegram_token_signals

    try:
        add_at_the_end = "_btc" if trade_against == "BTC" else ""

        telegram_chat_id = os.environ.get('telegram_chat_id')
        telegram_token_closed_position = os.environ.get('telegram_token_closed_positions'+add_at_the_end) 
        telegram_token_errors = os.environ.get('telegram_token_errors'+add_at_the_end)
        telegram_token_main = os.environ.get('telegram_token_main'+add_at_the_end)
        telegram_token_signals = os.environ.get('telegram_token_signals'+add_at_the_end)

    except KeyError as e: 
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        logging.exception(msg)

# fulfill telegram vars
read_env_var()

def remove_chars_exceptions(string):
    
    # define the characters to be removed
    chars_to_remove = ['<', '>', '{', '}', "'", '"']

    # use a loop to replace each character with an empty string
    for char in chars_to_remove:
        string = string.replace(char, '')

    return string

def get_telegram_token(bot) -> str:
    telegram_token = telegram_token_main
    return telegram_token

def get_telegram_prefix(bot, multi_line=False):
    if bot == "1h":
        return telegram_prefix_bot_1h_ml if multi_line else telegram_prefix_bot_1h_sl
    elif bot == "4h":
        return telegram_prefix_bot_4h_ml if multi_line else telegram_prefix_bot_4h_sl
    elif bot == "1d":
        return telegram_prefix_bot_1d_ml if multi_line else telegram_prefix_bot_1d_sl
    else:
        raise ValueError(f"Invalid bot type: {bot}")

def send_telegram_message(telegram_token, emoji, msg):

    msg = remove_chars_exceptions(msg)

    max_limit = 4096
    if emoji:
        additional_characters = EMOJI_WARNING+" <pre> </pre>Part [10/99]"
    else:
        additional_characters = "<pre> </pre>Part [10/99]"

    if emoji:
        msg = emoji+" "+msg

    num_additional_characters = len(additional_characters)
    max_limit = 4096 - num_additional_characters 

    if len(msg+additional_characters) > max_limit:
        # Split the message into multiple parts
        message_parts = [msg[i:i+max_limit] for i in range(0, len(msg), max_limit)]
        n_parts = len(message_parts)
        for i, part in enumerate(message_parts):
            print(f'Part [{i+1}/{n_parts}]\n{part}')
        
            lmsg = "<pre>Part ["+str(i+1)+"/"+str(n_parts)+"]\n"+part+"</pre>"
            
            params = {
            "chat_id": telegram_chat_id,
            "text": lmsg,
            "parse_mode": "HTML",
            }

            try:
                # if message is a warning, send message also to the errors telegram chat bot 
                if emoji == EMOJI_WARNING:
                    resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token_errors), params=params, timeout=telegram_timeout)
                    resp.raise_for_status()

                if telegram_token != telegram_token_errors:
                    resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token), params=params, timeout=telegram_timeout)
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
            
    else: # message size < max size 4096

        # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
        # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
        lmsg = "<pre>"+msg+"</pre>"

        params = {
        "chat_id": telegram_chat_id,
        "text": lmsg,
        "parse_mode": "HTML",
        }
        
        try:            
            # if message is a warning, send message also to the errors telegram chat bot 
            if emoji == EMOJI_WARNING:
                resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token_errors), params=params, timeout=telegram_timeout)
                resp.raise_for_status()

            resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token), params=params, timeout=telegram_timeout)
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

def send_telegram_alert(telegram_token, telegram_prefix, emoji, date, coin, timeframe, strategy, ordertype, unitValue, amount, trade_against_value, pnlPerc = '', pnl_trade_against = '', exit_reason = ''):
    lmsg = telegram_prefix + emoji + " " + str(date) + "\n" + coin + "\n" + strategy + "\n" + timeframe + "\n" + ordertype + "\n" + "UnitPrice: " + str(unitValue) + "\n" + "Qty: " + str(amount)+ "\n" + trade_against + ": " + str(trade_against_value)
    if pnlPerc != '':
        lmsg = lmsg + "\n"+"PnL%: "+str(round(float(pnlPerc),2)) + "\n"+"PnL "+trade_against+": "+str(float(pnl_trade_against))
    if exit_reason != '':
        lmsg = lmsg + "\n"+"Exit Reason: "+exit_reason

    print(lmsg)

    # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
    # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
    lmsg = "<pre>"+lmsg+"</pre>"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML",
    }
    
    try:
        resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token), params=params, timeout=telegram_timeout)
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

    # if is a closed position send also to telegram of closed positions
    if emoji in [EMOJI_TRADE_WITH_PROFIT, EMOJI_TRADE_WITH_LOSS]:
        
        params = {
        "chat_id": telegram_chat_id,
        "text": lmsg,
        "parse_mode": "HTML",
        }

        try: 
            resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegram_token_closed_position), params=params, timeout=telegram_timeout)
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

def send_telegram_photo(telegram_token, file_name):
    
    # get current dir
    cwd = os.getcwd()
    limg = cwd+"/"+file_name
    # print(limg)
    oimg = open(limg, 'rb')
    url = f"https://api.telegram.org/bot{telegram_token}/sendPhoto?chat_id={telegram_chat_id}"
    
    try:
        resp = requests.post(url, files={'photo':oimg}, timeout=telegram_timeout) # this sends the message
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

def send_telegram_file(telegram_token, file_name):
    
    # get current dir
    cwd = os.getcwd()
    file = cwd+"/"+file_name
    # print(limg)
    url = f"https://api.telegram.org/bot{telegram_token}/sendDocument"
    
    try:
        with open(file, 'rb') as f:
            resp = requests.post(url, data={'chat_id': telegram_chat_id},files={'document':f}, timeout=telegram_timeout) # this sends the message
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
