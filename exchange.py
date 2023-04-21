import sys
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance.helpers import round_step_size
import config
import logging
import telegram
import database
import pandas as pd

client: Client = None

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
            telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
            sys.exit(msg) 

connect()

def get_exchange_info():
    try:
        exchange_info = client.get_exchange_info()
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
            sys.exit(msg) 

    return exchange_info

def adjust_size(symbol, amount):
    for filt in client.get_symbol_info(symbol)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            stepSize = float(filt['stepSize'])
            minQty = float(filt['minQty'])
            break

    order_quantity = round_step_size(amount, stepSize)
    return order_quantity

def get_symbol_balance(symbol, bot):
    telegram_token = telegram.get_telegram_token(bot)
    try:
        qty = float(client.get_asset_balance(asset=symbol)['free'])  
        return qty
    except BinanceAPIException as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        return -1
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        return -1  

def separate_symbol_and_trade_against(symbol):
    if symbol.endswith("BTC"):
        symbol_only = symbol[:-3]
        symbol_stable = symbol[-3:]
    elif symbol.endswith(("BUSD","USDT")):    
        symbol_only = symbol[:-4]
        symbol_stable = symbol[-4:]

    return symbol_only, symbol_stable

def create_buy_order(symbol, qty, bot, fast_ema, slow_ema):
    telegram_token = telegram.get_telegram_token(bot)

    try:
        order = client.create_order(symbol=symbol,
                                    side=client.SIDE_BUY,
                                    type=client.ORDER_TYPE_MARKET,
                                    quoteOrderQty = qty,
                                    newOrderRespType = 'FULL') 
    except BinanceAPIException as e:
        msg = "BUY create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except BinanceOrderException as e:
        msg = "BUY create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except Exception as e:
        msg = "BUY create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        
    fills = order['fills']
    avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
    avg_price = round(avg_price,8)
        
    # update position with the buy order
    database.set_position_buy(bot=bot, 
                              symbol=symbol,
                              qty=float(order['executedQty']),
                              buy_price=avg_price,
                              date=pd.to_datetime(order['transactTime'], unit='ms'),
                              buy_order_id=order['orderId']
                              )
        
    database.add_order_buy(exchange_order_id=order['orderId'],
                            date=pd.to_datetime(order['transactTime'], unit='ms'),
                            bot=bot,
                            symbol=symbol,
                            price=avg_price,
                            qty=float(order['executedQty']),
                            ema_fast=fast_ema,
                            ema_slow=slow_ema
                            )
                            
    strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

    telegram.send_telegram_alert(telegram_token, telegram.EMOJI_ENTER_TRADE,
                    pd.to_datetime(order['transactTime'], unit='ms'),
                    order['symbol'], 
                    bot, 
                    strategy_name,
                    order['side'],
                    avg_price,
                    order['executedQty'],
                    qty)  

def create_sell_order(symbol, qty, bot, fast_ema=0, slow_ema=0, reason = ''):
    telegram_token = telegram.get_telegram_token(bot)

    try:
        order = client.create_order(symbol=symbol,
                                    side=client.SIDE_SELL,
                                    type=client.ORDER_TYPE_MARKET,
                                    quantity = qty
                                    )
    except BinanceAPIException as e:
        msg = "SELL create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except BinanceOrderException as e:
        msg = "SELL create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except Exception as e:
        msg = "SELL create_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
        
    fills = order['fills']
    avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
    avg_price = round(avg_price,8)

    # update position with the sell order
    database.set_position_sell(bot=bot,
                               symbol=symbol)
    
    # add to orders database table
    pnl_value, pnl_perc = database.add_order_sell(exchange_order_id = str(order['orderId']),
                                                  date = pd.to_datetime(order['transactTime'], unit='ms'),
                                                  bot = bot,
                                                  symbol = symbol,
                                                  price = avg_price,
                                                  qty = order['executedQty'],
                                                  ema_fast = fast_ema,
                                                  ema_slow = slow_ema,
                                                  exit_reason = reason
                                                  )
            
                
    # determine the alert type based on the value of pnl_value
    if pnl_value > 0:
        alert_type = telegram.EMOJI_TRADE_WITH_PROFIT
    else:
        alert_type = telegram.EMOJI_TRADE_WITH_LOSS

    strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

    # call send_telegram_alert with the appropriate alert type
    telegram.send_telegram_alert(telegram_token, 
                                    alert_type,
                                    pd.to_datetime(order['transactTime'], unit='ms'), 
                                    order['symbol'], 
                                    bot,
                                    strategy_name,
                                    order['side'],
                                    avg_price,
                                    order['executedQty'],
                                    avg_price*float(order['executedQty']),
                                    pnl_perc,
                                    pnl_value
                                    )

