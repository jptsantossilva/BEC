import sys
import logging
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance.helpers import round_step_size

import utils.config as config
import utils.telegram as telegram
import utils.database as database

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

            telegram_prefix = telegram.get_telegram_prefix(bot)

            telegram.send_telegram_alert(telegram_token, 
                                         telegram_prefix,
                                         telegram.EMOJI_ENTER_TRADE,
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

            telegram_prefix = telegram.get_telegram_prefix(bot)

            # call send_telegram_alert with the appropriate alert type
            telegram.send_telegram_alert(telegram_token,
                                         telegram_prefix,
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

def get_price_close_by_symbol_and_date(symbol: str, date: date):
    try:
        # Convert date to timestamp
        # start_date_str = date.strftime('%Y-%m-%d')
        timestamp = int(datetime.timestamp(date))
        start_date = str(timestamp)

        end_date = date + timedelta(days=1)
        # end_date_str = date.strftime('%Y-%m-%d')
        timestamp = int(datetime.timestamp(end_date))
        end_date = str(timestamp)

        # Get historical klines for symbol on date
        df = pd.DataFrame(client.get_historical_klines(symbol=symbol,
                                                       interval=Client.KLINE_INTERVAL_1DAY,
                                                       start_str=start_date,
                                                       end_str=end_date
                                                       ))

        if df.empty:
            return float(0)

        df = df[[0,4]]
        df.columns = ['Time','Close']
        df.Close = df.Close.astype(float)
        df.Time = pd.to_datetime(df.Time, unit='ms')

        # Return closing price
        return float(df['Close'][0])
    except BinanceAPIException as e:
        print(f"Binance API exception occurred: {e} - {symbol} - {date}")
        return float(0)
    except Exception as e:
        print(f"An unexpected error occurred: {e} - {symbol} - {date}")
        return float(0)

def create_balance_snapshot(telegram_prefix: str):
    msg = "Creating balance snapshot..."
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    last_date = database.get_last_date_from_balances(database.conn)
    if last_date == '0':
         today = datetime.now()
         start_date = today - timedelta(days=30)
    else:
         start_date = datetime.strptime(last_date, '%Y-%m-%d')        

    snapshots = client.get_account_snapshot(type="SPOT",
                                            startTime=int(start_date.timestamp()*1000), 
                                            # endTime=int(end_date.timestamp()*1000)
                                            limit=30 #max                                        
                                            )

    code = snapshots['code']
    msg = snapshots['msg']

    # get list of available symbols. 
    # This is usefull to avoid getting price from symbol that do not trade against stable
    exchange_info = get_exchange_info()
    symbols = set()
    trade_against = "BUSD"
    for s in exchange_info['symbols']:
        if (s['symbol'].endswith(trade_against)
            and s['status'] == 'TRADING'):
                symbols.add(s['symbol']) 

    # Create a Pandas DataFrame to store the daily balances for each asset
    df_balance = pd.DataFrame()

    # Iterate through the snapshots and get the daily balance for each asset
    for snapshot in snapshots['snapshotVos']:
        if snapshot['type'] == 'spot' and snapshot['data'] is not None:
            snapshot_date = datetime.fromtimestamp(snapshot['updateTime']/1000).date()
            totalAssetOfBtc = snapshot['data']['totalAssetOfBtc']
            for balance in snapshot['data']['balances']:
                asset = balance['asset']
                daily_balance = float(balance['free'])

                print(f"{snapshot_date}-{asset}")
                
                # ignore if balance = 0
                if daily_balance == 0.0:
                    continue

                symbol_with_trade_against = asset+trade_against

                if asset in [trade_against]:
                     balance_usd = daily_balance
                elif symbol_with_trade_against not in symbols:
                     print(f"{asset} not in available symbols")
                     balance_usd = 0
                else:
                    # convert snapshot_date from date to datetime
                    date = datetime.combine(snapshot_date, datetime.min.time())
                    unit_price = get_price_close_by_symbol_and_date(symbol_with_trade_against, date)
                    balance_usd = unit_price * daily_balance

                df_new = pd.DataFrame({
                    'Date': [snapshot_date],
                    'Asset': [asset],
                    'Balance': [daily_balance],
                    'Balance_USD': [balance_usd],
                    'Total_Balance_Of_BTC': [totalAssetOfBtc]
                    })
                # add to total
                df_balance = pd.concat([df_balance, df_new], ignore_index=True)

    # Print the daily balances for each asset
    # print(df_balance)

    # add data to table Balance
    database.add_balances(database.conn, df_balance)

    msg = "Balance snapshot finished"
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)


