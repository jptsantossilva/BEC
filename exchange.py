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

def calc_stake_amount(symbol, bot):
    telegram_token = telegram.get_telegram_token(bot)

    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(database.conn)

        if num_open_positions >= config.max_number_of_open_positions:
            return -2 

        try:
            balance = float(client.get_asset_balance(asset = symbol)['free'])
            
        except BinanceAPIException as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            return 0
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
            return 0
    
        tradable_balance = balance*config.tradable_balance_ratio 
        
        stake_amount = tradable_balance/(config.max_number_of_open_positions-num_open_positions)
        
        if symbol == "BTC":
            stake_amount = round(stake_amount, 8)
        elif symbol in ("BUSD", "USDT"):
            stake_amount = int(stake_amount)
        
        # make sure the size is >= the minimum size
        if stake_amount < config.min_position_size:
            stake_amount = config.min_position_size

        # make sure there are enough funds otherwise abort the buy position
        if balance < stake_amount:
            stake_amount = 0

        return stake_amount
    
    elif int(config.stake_amount_type) >= 0:
        return config.stake_amount_type
    else:
        return 0

def create_buy_order(symbol: str, bot: str, fast_ema: int, slow_ema: int):
    telegram_token = telegram.get_telegram_token(bot)

    try:
        # separate symbol from stable. example symbol=BTCUSDT coinOnly=BTC coinStable=USDT
        symbol_only, symbol_stable = separate_symbol_and_trade_against(symbol)

        position_size = calc_stake_amount(symbol=symbol_stable, bot=bot)
            
        if position_size > 0:
            order = client.create_order(symbol=symbol,
                                        side=client.SIDE_BUY,
                                        type=client.ORDER_TYPE_MARKET,
                                        quoteOrderQty = position_size,
                                        newOrderRespType = 'FULL') 
            
            fills = order['fills']
            avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
            avg_price = round(avg_price,8)
                
            # update position with the buy order
            database.set_position_buy(database.conn,
                                      bot=bot, 
                                      symbol=symbol,
                                      qty=float(order['executedQty']),
                                      buy_price=avg_price,
                                      date=str(pd.to_datetime(order['transactTime'], unit='ms')),
                                      ema_fast = fast_ema,
                                      ema_slow = slow_ema,
                                      buy_order_id=str(order['orderId']))
                
            database.add_order_buy(database.conn,
                                   exchange_order_id=str(order['orderId']),
                                   date=str(pd.to_datetime(order['transactTime'], unit='ms')),
                                   bot=bot,
                                   symbol=symbol,
                                   price=avg_price,
                                   qty=float(order['executedQty']),
                                   ema_fast=fast_ema,
                                   ema_slow=slow_ema)
                                    
            strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

            telegram.send_telegram_alert(telegram_token, telegram.EMOJI_ENTER_TRADE,
                                         pd.to_datetime(order['transactTime'], unit='ms'),
                                         order['symbol'], 
                                         bot, 
                                         strategy_name,
                                         order['side'],
                                         avg_price,
                                         order['executedQty'],
                                         position_size)  
            
        elif position_size == -2:
            num_open_positions = database.get_num_open_positions(database.conn, bot=bot)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, client.SIDE_BUY+" "+symbol+" - Max open positions ("+str(num_open_positions)+"/"+str(config.max_number_of_open_positions)+") already occupied!")
        else:
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, client.SIDE_BUY+" "+symbol+" - Not enough "+symbol_stable+" funds!")

        
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
        
    

def create_sell_order(symbol, bot, fast_ema=0, slow_ema=0, reason = ''):
    telegram_token = telegram.get_telegram_token(bot)

    try:
        symbol_only, symbol_stable = separate_symbol_and_trade_against(symbol)
        # get balance
        balance_qty = get_symbol_balance(symbol=symbol_only, bot=bot)  
        # verify sell quantity
        df_pos = database.get_positions_by_bot_symbol_position(database.conn, bot=bot, symbol=symbol, position=1)
        if not df_pos.empty:
            buy_order_qty = df_pos['Qty'].iloc[0]
        else:
            buy_order_qty = 0
        
        sell_qty = buy_order_qty
        if balance_qty < buy_order_qty:
            sell_qty = balance_qty
        sell_qty = adjust_size(symbol, sell_qty)

        if sell_qty > 0:
            order = client.create_order(symbol=symbol,
                                        side=client.SIDE_SELL,
                                        type=client.ORDER_TYPE_MARKET,
                                        quantity = sell_qty
                                        )
        
            fills = order['fills']
            avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
            avg_price = round(avg_price,8)

            # update position with the sell order
            database.set_position_sell(database.conn,
                                       bot=bot, 
                                       symbol=symbol)
    
            # add to orders database table
            pnl_value, pnl_perc = database.add_order_sell(database.conn,
                                                          exchange_order_id = str(order['orderId']),
                                                          date = str(pd.to_datetime(order['transactTime'], unit='ms')),
                                                          bot = bot,
                                                          symbol = symbol,
                                                          price = avg_price,
                                                          qty = float(order['executedQty']),
                                                          ema_fast = fast_ema,
                                                          ema_slow = slow_ema,
                                                          exit_reason = reason)            
                
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
                                         pnl_value)
        else:
            # if there is no qty on balance to sell we set the qty on positions table to zero
            # this can happen if we sell on the exchange (for example, due to a pump) before the bot sells it. 
            database.set_position_sell(database.conn,
                                       bot=bot, 
                                       symbol=symbol)
        
    except BinanceAPIException as e:
        msg = "create_sell_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except BinanceOrderException as e:
        msg = "create_sell_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except Exception as e:
        msg = "create_sell_order - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)

