import sys
import os
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
        telegram.send_telegram_message(telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg)
        sys.exit(msg) 

def connect():
    api_key = get_env_var('binance_api')
    api_secret = get_env_var('binance_secret')

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

def get_symbol_info(symbol):
    try:
        exchange_info = client.get_symbol_info(symbol)
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

def get_symbol_balance(symbol):
    telegram_token = telegram.get_telegram_token()
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
    elif symbol.endswith(("BUSD","USDT","USDC")):    
        symbol_only = symbol[:-4]
        symbol_stable = symbol[-4:]

    return symbol_only, symbol_stable

def calc_stake_amount(symbol, bot):
    telegram_token = telegram.get_telegram_token()

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
        # remove locked values from the balance
        lock_values = config.get_setting("lock_values")
        if lock_values:
            locked_values = database.get_total_locked_values(database.conn)
            tradable_balance = tradable_balance-locked_values
            
        stake_amount = tradable_balance/(config.max_number_of_open_positions-num_open_positions)
        
        if symbol == "BTC":
            stake_amount = round(stake_amount, 8)
        elif symbol in ("BUSD", "USDT", "USDC"):
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

def create_buy_order(symbol: str, bot: str, fast_ema: int = 0, slow_ema: int = 0, convert_all_balance: bool = False):
    
    run_mode = config.get_setting("run_mode")
    if run_mode == "test":
        print("Exiting the function because run_mode is 'test'.")
        return
    
    telegram_token = telegram.get_telegram_token()

    try:
        # separate symbol from stable. example symbol=BTCUSDT symbol_only=BTC symbol_stable=USDT
        symbol_only, symbol_trade_against = separate_symbol_and_trade_against(symbol)

        if not convert_all_balance:
            position_size = calc_stake_amount(symbol=symbol_trade_against, bot=bot)
        else:
            # convert full symbol_trade_against balance to symbol_trade_against
            try:
                balance = float(client.get_asset_balance(asset = symbol_trade_against)['free'])                
                tradable_balance = balance*config.tradable_balance_ratio      
                position_size = tradable_balance
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                logging.exception(msg)
                telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)   
        
        if position_size > 0:

            # check if Quote Order Qty MARKET orders are enabled
            info = get_symbol_info(symbol)
            # check if quote order feature is enabled
            quote_order = info['quoteOrderQtyMarketAllowed']
            # get symbol precision
            symbol_precision = info['baseAssetPrecision']
            
            if quote_order:
                order = client.create_order(symbol=symbol,
                                            side=client.SIDE_BUY,
                                            type=client.ORDER_TYPE_MARKET,
                                            quoteOrderQty = position_size,
                                            newOrderRespType = 'FULL')
            else:
                # get symbol price
                symbol_price = client.get_symbol_ticker(symbol=symbol)
                # get symbol precision
                symbol_precision = info['baseAssetPrecision']
                # calc buy qty
                buy_quantity = round(position_size/float(symbol_price['price']), symbol_precision)
                # adjust buy qty considering binance LOT_SIZE rules
                buy_quantity = adjust_size(symbol, buy_quantity)
                # place order
                order = client.create_order(symbol=symbol,
                                            side=client.SIDE_BUY,
                                            type=client.ORDER_TYPE_MARKET,
                                            quantity = buy_quantity,
                                            newOrderRespType = 'FULL')
                 
            fills = order['fills']
            avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
            avg_price = round(avg_price,8)
                
            # update position with the buy order
            if not convert_all_balance:
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
        
            if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                strategy_name = str(fast_ema)+"/"+str(slow_ema)+" "+config.strategy_name
            elif config.strategy_id in ["market_phases"]:
                strategy_name = config.strategy_name

            if convert_all_balance:
                convert_message = "Trade against auto switch"
                strategy_name = f"{convert_message} - {strategy_name}"

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
            num_open_positions = database.get_num_open_positions(database.conn)
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, client.SIDE_BUY+" "+symbol+" - Max open positions ("+str(num_open_positions)+"/"+str(config.max_number_of_open_positions)+") already occupied!")
        else:
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, client.SIDE_BUY+" "+symbol+" - Not enough "+symbol_trade_against+" funds!")

        
    except BinanceAPIException as e:
        msg = "BUY create_order - "+repr(e)
        msg = msg + " - " + symbol
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except BinanceOrderException as e:
        msg = "BUY create_order - "+repr(e)
        msg = msg + " - " + symbol
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except Exception as e:
        msg = "BUY create_order - "+repr(e)
        msg = msg + " - " + symbol
        print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)

def delete_position(symbol, bot, symbol_delisted: bool = True):

    telegram_token = telegram.get_telegram_token()

    df_pos = database.get_positions_by_bot_symbol_position(database.conn, bot=bot, symbol=symbol, position=1)
    
    # get Buy_Order_Id from positions table
    if not df_pos.empty:
        buy_order_id = str(df_pos['Buy_Order_Id'].iloc[0])
        qty = df_pos['Qty'].iloc[0]
    else:
        buy_order_id = str(0)
        qty = 0

    # update position as closed position
    database.set_position_sell(database.conn, bot=bot, symbol=symbol)
    
    # release all locked values from position
    if not df_pos.empty:
        database.release_value(database.conn, df_pos['Id'].iloc[0])
    
    # Get the current date and time
    current_datetime = datetime.now()
    # Format the date and time as 'YYYY-MM-DD HH:MM:SS'
    order_sell_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    if symbol_delisted:
        reason = "Symbol delisted from exchange"

    fast_ema = 0
    slow_ema = 0

    # add to orders database table
    pnl_value, pnl_perc = database.add_order_sell(
        database.conn,
        sell_order_id = 0,
        buy_order_id = buy_order_id,
        date = str(order_sell_date),
        bot = bot,
        symbol = symbol,
        price = 0,
        qty = qty,
        ema_fast = fast_ema,
        ema_slow = slow_ema,
        exit_reason = reason
    )            

    # determine the alert type based on the value of pnl_value
    if pnl_value > 0:
        alert_type = telegram.EMOJI_TRADE_WITH_PROFIT
    else:
        alert_type = telegram.EMOJI_TRADE_WITH_LOSS    
        
    telegram_prefix = telegram.get_telegram_prefix(bot)

    order_side = "SELL"
    order_avg_price = 0

    # call send_telegram_alert with the appropriate alert type
    telegram.send_telegram_alert(
        telegram_token=telegram_token,
        telegram_prefix=telegram_prefix,
        emoji=alert_type,
        date=current_datetime, 
        symbol=symbol, 
        timeframe=bot,
        strategy="",
        ordertype=order_side,
        unitValue=order_avg_price,
        amount=qty,
        trade_against_value=order_avg_price*qty,
        pnlPerc=pnl_perc,
        pnl_trade_against=pnl_value,
        exit_reason=reason)
        
def create_sell_order(symbol, bot, fast_ema=0, slow_ema=0, reason = '', percentage = 100, take_profit_num = 0, convert_all_balance: bool = False):

    run_mode = config.get_setting("run_mode")
    if run_mode == "test":
        print("Exiting the function because run_mode is 'test'.")
        return
    
    telegram_token = telegram.get_telegram_token()

    try:
        symbol_only, symbol_trade_against = separate_symbol_and_trade_against(symbol)
        # get balance
        balance_qty = get_symbol_balance(symbol=symbol_only)  
        
        # verify sell quantity
        if convert_all_balance:
            sell_qty = balance_qty
        else:
            df_pos = database.get_positions_by_bot_symbol_position(database.conn, bot=bot, symbol=symbol, position=1)
            if not df_pos.empty:
                pos_qty = df_pos['Qty'].iloc[0]
            else:
                pos_qty = 0
            
            sell_qty = pos_qty

            if balance_qty < pos_qty:
                sell_qty = balance_qty

        # sell by percentage
        if percentage < 100:
            sell_qty = sell_qty*(percentage/100)

        sell_qty = adjust_size(symbol, sell_qty)

        if sell_qty > 0:
            order = client.create_order(symbol=symbol,
                                        side=client.SIDE_SELL,
                                        type=client.ORDER_TYPE_MARKET,
                                        quantity = sell_qty
                                        )
            
            result = True
            msg = "Sold Successfully"
        
            sell_order_id = str(order['orderId'])

            fills = order['fills']
            order_avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
            order_avg_price = round(order_avg_price,8)

            order_sell_date = pd.to_datetime(order['transactTime'], unit='ms')
            order_symbol = order['symbol']
            order_qty = float(order['executedQty'])
            order_side = order['side']

            if convert_all_balance:
                buy_order_id = str(0)
            else:
                # get Buy_Order_Id from positions table
                df_pos = database.get_positions_by_bot_symbol_position(
                    connection=database.conn,
                    bot=bot, 
                    symbol=symbol, 
                    position=1
                )
                
                if not df_pos.empty:
                    buy_order_id = str(df_pos['Buy_Order_Id'].iloc[0])
                else:
                    buy_order_id = str(0)
                
                if percentage == 100:
                    # update position as closed position
                    database.set_position_sell(database.conn, bot=bot, symbol=symbol)
                    
                    # release all locked values from position
                    if not df_pos.empty:
                        database.release_value(database.conn, df_pos['Id'].iloc[0])
                else: # percentage < 100     
                    if not df_pos.empty:
                        # if we are selling a position percentage we must update the qty
                        previous_qty = float(df_pos['Qty'].iloc[0])
                        new_qty = previous_qty - order_qty
                        database.set_position_qty(database.conn, bot=bot, symbol=symbol, qty=new_qty)

                        # update take profit to inform that we already took profit 1, 2, 3 or 4
                        if take_profit_num == 1:
                            database.set_position_take_profit_1(database.conn, bot=bot, symbol=symbol, take_profit_1=1)
                        elif take_profit_num == 2:
                            database.set_position_take_profit_2(database.conn, bot=bot, symbol=symbol, take_profit_2=1)
                        elif take_profit_num == 3:
                            database.set_position_take_profit_3(database.conn, bot=bot, symbol=symbol, take_profit_3=1)
                        elif take_profit_num == 4:
                            database.set_position_take_profit_4(database.conn, bot=bot, symbol=symbol, take_profit_4=1)
                        
                        # lock values from parcial sales amounts
                        lock_values = config.get_setting("lock_values")
                        if lock_values:
                            database.lock_value(
                                database.conn, 
                                position_id=df_pos['Id'].iloc[0],
                                buy_order_id=buy_order_id,
                                amount=order_avg_price*order_qty
                            )
    
            # add to orders database table
            pnl_value, pnl_perc = database.add_order_sell(
                database.conn,
                sell_order_id = sell_order_id,
                buy_order_id = buy_order_id,
                date = str(order_sell_date),
                bot = bot,
                symbol = symbol,
                price = order_avg_price,
                qty = order_qty,
                ema_fast = fast_ema,
                ema_slow = slow_ema,
                exit_reason = reason,
                sell_percentage = percentage
            )            

            # determine the alert type based on the value of pnl_value
            if pnl_value > 0:
                alert_type = telegram.EMOJI_TRADE_WITH_PROFIT
            else:
                alert_type = telegram.EMOJI_TRADE_WITH_LOSS

            if config.strategy_id in ["ema_cross_with_market_phases", "ema_cross"]:
                strategy_name = str(fast_ema)+"/"+str(slow_ema)+" "+config.strategy_name
            elif config.strategy_id in ["market_phases"]:
                strategy_name = config.strategy_name 
            
            if convert_all_balance:
                convert_message = "Trade against auto switch"
                strategy_name = f"{convert_message} - {strategy_name}"

            telegram_prefix = telegram.get_telegram_prefix(bot)

            # if is a sale from crossover
            if (slow_ema != 0 and fast_ema != 0) and reason == "":
                reason = strategy_name

            # call send_telegram_alert with the appropriate alert type
            telegram.send_telegram_alert(telegram_token=telegram_token,
                                         telegram_prefix=telegram_prefix,
                                         emoji=alert_type,
                                         date=order_sell_date, 
                                         symbol=order_symbol, 
                                         timeframe=bot,
                                         strategy=strategy_name,
                                         ordertype=order_side,
                                         unitValue=order_avg_price,
                                         amount=order_qty,
                                         trade_against_value=order_avg_price*order_qty,
                                         pnlPerc=pnl_perc,
                                         pnl_trade_against=pnl_value,
                                         exit_reason=reason)
        else:
            # if there is no qty on balance to sell we set the qty on positions table to zero
            # this can happen if we sell on the exchange before the bot sells it. 
            database.set_position_sell(connection=database.conn,
                                       bot=bot, 
                                       symbol=symbol)
            result = False
            msg = "Unable to sell position. The position size in your balance is currently zero. No sell order was placed, and the position was removed from the unrealized PnL table."
        
    except BinanceAPIException as e:
        result = False
        # customize error message based on the exception
        if e.code == -1013:
            error_description = "Sorry, your sell order cannot be placed because the total value of the trade (notional) is too low. Please adjust the quantity or price to meet the minimum notional value requirement set by the exchange."
            msg = f"create_sell_order - {bot} - {symbol} - Sell_Qty:{sell_qty} - {error_description}"
        else:
            msg = f"create_sell_order - {bot} - {symbol} - {repr(e)}"
        # print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except BinanceOrderException as e:
        result = False
        msg = f"create_sell_order - {bot} - {symbol} - {repr(e)}"
        # print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)
    except Exception as e:
        result = False
        msg = f"create_sell_order - {bot} - {symbol} - {repr(e)}"
        # print(msg)
        telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)

    return result, msg

def get_price_close_by_symbol_and_date(symbol: str, date: date):
    # Convert date to timestamp
    # start_date_str = date.strftime('%Y-%m-%d')
    timestamp = int(datetime.timestamp(date))
    start_date = str(timestamp)

    end_date = date + timedelta(days=1)
    # end_date_str = date.strftime('%Y-%m-%d')
    timestamp = int(datetime.timestamp(end_date))
    end_date = str(timestamp)

    # Get historical klines for symbol on date
    # makes 3 attempts to get historical data
    max_retry = 3
    retry_count = 1
    success = False

    while retry_count < max_retry and not success:
        try:
            df = pd.DataFrame(client.get_historical_klines(symbol=symbol,
                                                        interval=Client.KLINE_INTERVAL_1DAY,
                                                        start_str=start_date,
                                                        end_str=end_date
                                                        ))
            success = True
        except Exception as e:
            retry_count += 1
            msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
            print(msg)

    if not success:
        msg = f"Failed after {max_retry} tries to get historical data. Unable to retrieve data. "
        msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
        msg = telegram.telegram_prefix_market_phases_sl + msg
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING, msg)
        return float(0)
    else:
        if df.empty:
            return float(0)

        df = df[[0,4]]
        df.columns = ['Time','Close']
        # using dictionary to convert specific columns
        convert_dict = {'Close': float}
        df = df.astype(convert_dict)
        df.Time = pd.to_datetime(df.Time, unit='ms')
        # Return closing price
        return float(df['Close'][0])
        
def create_balance_snapshot(telegram_prefix: str):
    msg = "Creating balance snapshot. It can take a few minutes..."
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    # Check if connection is already established
    if database.is_connection_open(database.conn):
        print("Database connection is already established.")
    else:
        # Create a new connection
        database.conn = database.connect()

    # Retrieve the balances of all coins in the user’s Binance account
    account_balances = client.get_account()['balances']

    # Get the current price of all tickers from the Binance API
    ticker_info = client.get_all_tickers()

    # Create a dictionary of tickers and their corresponding prices
    ticker_prices = {ticker['symbol']: float(ticker['price']) for ticker in ticker_info}
    btc_price = ticker_prices.get('BTCUSDC')

    # Calculate yesterday's date
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Calculate the USD value of each coin in the user’s account
    symbol_values = []
    for symbol_balance in account_balances:
        # Get the coin symbol and the free and locked balance of each coin
        symbol = symbol_balance['asset']
        unlocked_balance = float(symbol_balance['free'])
        # locked_balance = float(coin_balance['locked'])
    
        # If the coin is USDT and the total balance is greater than 1, add it to the list of coins with their USDT values
        if symbol in ["USDT", "USDC"] and unlocked_balance > 1:
            
            symbol_balance = unlocked_balance
            symbol_usd_price = 1
            symbol_balance_usd = symbol_balance*symbol_usd_price
            symbol_balance_btc = symbol_balance*(symbol_usd_price/btc_price)

            new_row = [date, symbol, symbol_balance, symbol_usd_price, btc_price, symbol_balance_usd, symbol_balance_btc]
            symbol_values.append(new_row)

        # Otherwise, check if the coin has a USDT trading pair or a BTC trading pair
        elif unlocked_balance > 0.0:
            # Check if the coin has a USDT trading pair
            if (any(symbol + 'USDC' in i for i in ticker_prices)):
                # If it does, calculate its USDC value and add it to the list of coins with their USDC values
                ticker_symbol = symbol + 'USDC'
                # ticker_price = ticker_prices.get(ticker_symbol)
                # coin_usdt_value = (unlocked_balance) * ticker_price

                symbol_balance = unlocked_balance
                symbol_usd_price = ticker_prices.get(ticker_symbol)
                symbol_balance_usd = symbol_balance*symbol_usd_price
                symbol_balance_btc = symbol_balance*(symbol_usd_price/btc_price)

                if symbol_balance_usd > 1:   
                    new_row = [date, symbol, symbol_balance, symbol_usd_price, btc_price, symbol_balance_usd, symbol_balance_btc]
                    symbol_values.append(new_row)
            
            # If the coin does not have a USDT trading pair, check if it has a BTC trading pair
            elif (any(symbol + 'BTC' in i for i in ticker_prices)):
                # If it does, calculate its USDT value and add it to the list of coins with their USDT values
                ticker_symbol = symbol + 'BTC'
                symbol_btc_price = ticker_prices.get(ticker_symbol)
                
                symbol_balance = unlocked_balance
                symbol_usd_value = symbol_btc_price*btc_price
                symbol_balance_usd = symbol_balance*symbol_usd_price
                symbol_balance_btc = symbol_balance*symbol_btc_price
                
                if symbol_balance_usd > 1:
                    new_row = [date, symbol, symbol_balance, symbol_usd_price, btc_price, symbol_balance_usd, symbol_balance_btc]
                    symbol_values.append(new_row)
        
    # Define column names
    columns = ['Date', 'Asset', 'Balance', 'USD_Price', 'BTC_Price', 'Balance_USD', 'Balance_BTC']

    # Convert the list to a DataFrame
    df_balance = pd.DataFrame(symbol_values, columns=columns)

    # Calculate the sum of the 'Balance_BTC' column
    total_balance_btc = df_balance['Balance_BTC'].sum()

    # Insert the sum into a new column 'Total_Balance_BTC'
    df_balance['Total_Balance_BTC'] = total_balance_btc

    # Sort the DataFrame by 'Balance_USD' in descending order
    df_balance.sort_values(by='Balance_USD', ascending=False, inplace=True)

    # df_new = pd.DataFrame({
    #     'Date': [snapshot_date],
    #     'Asset': [asset],
    #     'Balance': [daily_balance],
    #     'USD_Price': [unit_price_usd],
    #     'BTC_Price': [btc_value],
    #     'Balance_USD': [balance_usd],
    #     'Balance_BTC': [balance_btc],
    #     'Total_Balance_BTC': [totalAssetOfBtc]
    #     })


    # last_date = database.get_last_date_from_balances(database.conn)
    # if last_date == '0':
    #      today = datetime.now()
    #      start_date = today - timedelta(days=30)
    # else:
    #      start_date = datetime.strptime(last_date, '%Y-%m-%d')    

    # start_date = datetime.now()    

    # snapshots = client.get_account_snapshot(type="SPOT",
    #                                         startTime=int(start_date.timestamp()*1000), 
    #                                         # endTime=int(end_date.timestamp()*1000)
    #                                         limit=30 #max                                        
    #                                         )

    # code = snapshots['code']
    # msg = snapshots['msg']

    # get list of available symbols. 
    # This is usefull to avoid getting price from symbol that do not trade against stable
    # exchange_info = get_exchange_info()
    # symbols = set()
    # trade_against = "USDT"
    # for s in exchange_info['symbols']:
    #     if (s['symbol'].endswith(trade_against)
    #         and s['status'] == 'TRADING'):
    #             symbols.add(s['symbol']) 

    # Create a Pandas DataFrame to store the daily balances for each asset
    # df_balance = pd.DataFrame()


    # ignore if balance = 0
    # if daily_balance == 0.0:
    #     continue

    # symbol_with_trade_against = asset+trade_against

    # # convert snapshot_date from date to datetime
    # date = datetime.combine(snapshot_date, datetime.min.time())
    # btc_value = get_price_close_by_symbol_and_date("BTCUSDT", date)

    # if asset in [trade_against]:
    #     balance_usd = daily_balance
    #     unit_price_usd = 1
    #     unit_price_btc = unit_price_usd/btc_value
    #     balance_btc = unit_price_btc * daily_balance
    # elif symbol_with_trade_against not in symbols:
    #         print(f"{asset} not in available symbols")
    #         balance_usd = 0
    #         balance_btc = 0
    #         unit_price_usd = 0
    # else:
    #     # convert snapshot_date from date to datetime
    #     date = datetime.combine(snapshot_date, datetime.min.time())
    #     # get unit USDT price
    #     unit_price_usd = get_price_close_by_symbol_and_date(symbol_with_trade_against, date)
    #     balance_usd = unit_price_usd * daily_balance
    #     unit_price_btc = unit_price_usd/btc_value
    #     balance_btc = unit_price_btc * daily_balance

    # df_new = pd.DataFrame({
    #     'Date': [snapshot_date],
    #     'Asset': [asset],
    #     'Balance': [daily_balance],
    #     'USD_Price': [unit_price_usd],
    #     'BTC_Price': [btc_value],
    #     'Balance_USD': [balance_usd],
    #     'Balance_BTC': [balance_btc],
    #     'Total_Balance_BTC': [totalAssetOfBtc]
    #     })
    # # add to total
    # df_balance = pd.concat([df_balance, df_new], ignore_index=True)

    # # Iterate through the snapshots and get the daily balance for each asset
    # for snapshot in snapshots['snapshotVos']:
    #     if snapshot['type'] == 'spot' and snapshot['data'] is not None:
    #         snapshot_date = datetime.fromtimestamp(snapshot['updateTime']/1000).date()
    #         totalAssetOfBtc = snapshot['data']['totalAssetOfBtc']
    #         for balance in snapshot['data']['balances']:
    #             asset = balance['asset']
    #             daily_balance = float(balance['free'])

    #             print(f"{snapshot_date}-{asset}")
                
    #             # ignore if balance = 0
    #             if daily_balance == 0.0:
    #                 continue

    #             symbol_with_trade_against = asset+trade_against

    #             # convert snapshot_date from date to datetime
    #             date = datetime.combine(snapshot_date, datetime.min.time())
    #             btc_value = get_price_close_by_symbol_and_date("BTCUSDT", date)

    #             if asset in [trade_against]:
    #                 balance_usd = daily_balance
    #                 unit_price_usd = 1
    #                 unit_price_btc = unit_price_usd/btc_value
    #                 balance_btc = unit_price_btc * daily_balance
    #             elif symbol_with_trade_against not in symbols:
    #                  print(f"{asset} not in available symbols")
    #                  balance_usd = 0
    #                  balance_btc = 0
    #                  unit_price_usd = 0
    #             else:
    #                 # convert snapshot_date from date to datetime
    #                 date = datetime.combine(snapshot_date, datetime.min.time())
    #                 # get unit USDT price
    #                 unit_price_usd = get_price_close_by_symbol_and_date(symbol_with_trade_against, date)
    #                 balance_usd = unit_price_usd * daily_balance
    #                 unit_price_btc = unit_price_usd/btc_value
    #                 balance_btc = unit_price_btc * daily_balance

    #             df_new = pd.DataFrame({
    #                 'Date': [snapshot_date],
    #                 'Asset': [asset],
    #                 'Balance': [daily_balance],
    #                 'USD_Price': [unit_price_usd],
    #                 'BTC_Price': [btc_value],
    #                 'Balance_USD': [balance_usd],
    #                 'Balance_BTC': [balance_btc],
    #                 'Total_Balance_BTC': [totalAssetOfBtc]
    #                 })
    #             # add to total
    #             df_balance = pd.concat([df_balance, df_new], ignore_index=True)

    # Print the daily balances for each asset
    # print(df_balance)

    # add data to table Balance
    database.add_balances(database.conn, df_balance)

    msg = "Balance snapshot finished"
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)


