import sys
import os
import logging
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from dateutil.relativedelta import relativedelta
import time
from typing import Optional, Union, Callable
import sqlite3

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException
from binance.helpers import round_step_size

import utils.config as config
import utils.telegram as telegram
import utils.database as database

_client: Optional[Client] = None # private to module

def _env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        msg = f"[binance] Missing env var: {name}"
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
        raise RuntimeError(msg)
    return val

def connect(api_key: Optional[str]=None, api_secret: Optional[str]=None, test_ping: bool=True) -> Client:
    """Create (or recreate) the Binance client. No side-effects on import."""
    global _client
    api_key = api_key or _env("binance_api")
    api_secret = api_secret or _env("binance_secret")

    max_retry = 3
    for attempt in range(1, max_retry + 1):
        try:
            _client = Client(api_key, api_secret, requests_params={"timeout": 15})
            if test_ping:
                _client.ping()  # valida conectividade/credenciais
            return _client
        except BinanceAPIException as e:
            msg = f"[binance] Error connecting to Binance: {repr(e)}"
            print(msg)
            logging.exception(msg)
            if e.status_code in (418, 429, 503) and attempt < max_retry:
                time.sleep(2 ** attempt)
                continue
            telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
            _client = None
            raise
        except Exception as e:
            msg = f"[binance] Error connecting to Binance: {repr(e)}"
            print(msg)
            logging.exception(msg)
            if attempt < max_retry:
                time.sleep(2 ** attempt)
                continue
            telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
            _client = None
            raise

def get_client() -> Client:
    """Lazy getter: use existing or create new."""
    global _client
    if _client is None:
        return connect()
    return _client

def get_exchange_info():
    try:
        return get_client().get_exchange_info()
    except Exception as e:
        msg = f"[binance] get_exchange_info failed: {repr(e)}"
        print(msg); logging.exception(msg)
        telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
        raise

def adjust_size(symbol, amount):
    info = get_client().get_symbol_info(symbol)
    lot = next((f for f in info['filters'] if f['filterType']=='LOT_SIZE'), None)
    if not lot:
        # without LOT_SIZE, returns simple rounding as fallback
        return amount
    step = float(lot['stepSize'])
    min_qty = float(lot['minQty'])
    qty = round_step_size(amount, step)
    if qty < min_qty:
        qty = 0.0  # forces the logic to abort the order by minimum value
    return qty

def get_symbol_balance(symbol):
    telegram_token = telegram.get_telegram_token()
    try:
        qty = float(get_client().get_asset_balance(asset=symbol)['free'])  
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
        symbol_only = symbol[:-3]; symbol_stable = symbol[-3:]
    elif symbol.endswith(("USDT","USDC")):
        symbol_only = symbol[:-4]; symbol_stable = symbol[-4:]
    else:
        # avoids UnboundLocalError and gives clear error
        raise ValueError(f"Unsupported symbol suffix for {symbol}. Expected BTC/USDT/USDC.")
    return symbol_only, symbol_stable

def calc_stake_amount(symbol, bot):
    telegram_token = telegram.get_telegram_token()

    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions()

        if num_open_positions >= config.max_number_of_open_positions:
            return -2 

        try:
            balance = float(get_client().get_asset_balance(asset = symbol)['free'])
            
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
            locked_values = database.get_total_locked_values()
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
        return float(config.stake_amount_type)
    else:
        return 0

def create_buy_order(symbol: str, bot: str, fast_ema: int = 0, slow_ema: int = 0, convert_all_balance: bool = False):
    
    run_mode = config.get_setting("run_mode")
    if run_mode == "test":
        print("Exiting the function because run_mode is 'test'.")
        return
    
    telegram_token = telegram.get_telegram_token()

    position_size = 0.0

    try:
        # separate symbol from stable. example symbol=BTCUSDT symbol_only=BTC symbol_stable=USDT
        symbol_only, symbol_trade_against = separate_symbol_and_trade_against(symbol)

        if not convert_all_balance:
            position_size = calc_stake_amount(symbol=symbol_trade_against, bot=bot)
        else:
            # convert full symbol_trade_against balance to symbol_trade_against
            try:
                balance = float(get_client().get_asset_balance(asset = symbol_trade_against)['free'])                
                tradable_balance = balance*config.tradable_balance_ratio      
                position_size = tradable_balance
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                logging.exception(msg)
                telegram.send_telegram_message(telegram_token, telegram.EMOJI_WARNING, msg)   
                position_size = 0.0  # ensures that it is always defined
        
        if position_size > 0:

            # check if Quote Order Qty MARKET orders are enabled
            info = get_client().get_symbol_info(symbol)
            # check if quote order feature is enabled
            quote_order = info['quoteOrderQtyMarketAllowed']
            # get symbol precision
            symbol_precision = info['baseAssetPrecision']
            
            if quote_order:
                order = get_client().create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET,
                    quoteOrderQty = position_size,
                    newOrderRespType = 'FULL'
                )
            else:
                # get symbol price
                symbol_price = get_client().get_symbol_ticker(symbol=symbol)
                # get symbol precision
                symbol_precision = info['baseAssetPrecision']
                # calc buy qty
                buy_quantity = round(position_size/float(symbol_price['price']), symbol_precision)
                # adjust buy qty considering binance LOT_SIZE rules
                buy_quantity = adjust_size(symbol, buy_quantity)
                # place order
                order = get_client().create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity = buy_quantity,
                    newOrderRespType = 'FULL'
                )
                 
            fills = order['fills']
            avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
            avg_price = round(avg_price,8)
                
            # update position with the buy order
            if not convert_all_balance:
                database.set_position_buy(
                                        bot=bot, 
                                        symbol=symbol,
                                        qty=float(order['executedQty']),
                                        buy_price=avg_price,
                                        date=str(pd.to_datetime(order['transactTime'], unit='ms')),
                                        ema_fast = fast_ema,
                                        ema_slow = slow_ema,
                                        buy_order_id=str(order['orderId']))
                
            database.add_order_buy(
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
            num_open_positions = database.get_num_open_positions()
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, Client.SIDE_BUY+" "+symbol+" - Max open positions ("+str(num_open_positions)+"/"+str(config.max_number_of_open_positions)+") already occupied!")
        else:
            telegram.send_telegram_message(telegram_token, telegram.EMOJI_INFORMATION, Client.SIDE_BUY+" "+symbol+" - Not enough "+symbol_trade_against+" funds!")

        
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
            df_pos = database.get_positions_by_bot_symbol_position( bot=bot, symbol=symbol, position=1)
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
            order = get_client().create_order(
                symbol=symbol,
                side=Client.SIDE_SELL,
                type=Client.ORDER_TYPE_MARKET,
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
                    database.set_position_sell( bot=bot, symbol=symbol)
                    
                    # release all locked values from position
                    if not df_pos.empty:
                        database.release_value( df_pos['Id'].iloc[0])
                else: # percentage < 100     
                    if not df_pos.empty:
                        # if we are selling a position percentage we must update the qty
                        previous_qty = float(df_pos['Qty'].iloc[0])
                        new_qty = previous_qty - order_qty
                        database.set_position_qty( bot=bot, symbol=symbol, qty=new_qty)

                        # update take profit to inform that we already took profit 1, 2, 3 or 4
                        if take_profit_num == 1:
                            database.set_position_take_profit_1( bot=bot, symbol=symbol, take_profit_1=1)
                        elif take_profit_num == 2:
                            database.set_position_take_profit_2( bot=bot, symbol=symbol, take_profit_2=1)
                        elif take_profit_num == 3:
                            database.set_position_take_profit_3( bot=bot, symbol=symbol, take_profit_3=1)
                        elif take_profit_num == 4:
                            database.set_position_take_profit_4( bot=bot, symbol=symbol, take_profit_4=1)
                        
                        # lock values from parcial sales amounts
                        lock_values = config.get_setting("lock_values")
                        if lock_values:
                            database.lock_value(
                                position_id=df_pos['Id'].iloc[0],
                                buy_order_id=buy_order_id,
                                amount=order_avg_price*order_qty
                            )
    
            # add to orders database table
            pnl_value, pnl_perc = database.add_order_sell(
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
            database.set_position_sell(bot=bot, symbol=symbol)
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

def _get_ohlcv_raw(
    symbol: str,
    interval: str = Client.KLINE_INTERVAL_1DAY,
    start_date: Union[str, int, datetime] = "1 Jan, 2010",
    end_date: Optional[Union[str, int, datetime]] = None,
    max_retries: int = 3,
    backoff_sec: float = 1.5,
    force_drop_last: bool = False,
    only_closed: bool = True,
    notify: bool = True,
    telegram_token: Optional[str] = None,
    telegram_prefix: str = "",
) -> pd.DataFrame:
    """
    Fetch historical OHLCV from Binance and return a clean, typed DataFrame.

    Columns: ['timestamp','open','high','low','close','volume','symbol']
    Index:   timestamp (tz-aware, UTC)

    only_closed=True:
      - 1d/3d: set end_str to today's 00:00 UTC (exclusive) to avoid today's in-progress candle,
               then keep rows with open_time < today0_ms (safety).
      - fixed-size intraday intervals: if end_date is None, set end_str to 'now' floored by interval,
               so the API won't return the in-progress candle.
      - variable intervals (1w/1M): post-filter last row if its close_time is in the future.
    force_drop_last=True:
      - unconditionally drop the last row (useful for bespoke workflows/tests).
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            interval_norm = _normalize_interval(interval)
            raw = get_client().get_historical_klines(
                symbol, 
                interval_norm, 
                start_str=_to_binance_param(start_date), 
                end_str=_to_binance_param(end_date)
            )
            if not raw:
                msg = f"[get_OHLCV] Empty result for {symbol} {interval} {start_date}→{end_date}"
                msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
                msg = telegram_prefix + msg
                print(msg)
                if notify:
                    token = telegram_token or telegram.telegram_token_main
                    telegram.send_telegram_message(token, telegram.EMOJI_WARNING, msg)
                return pd.DataFrame(columns=["timestamp","open","high","low","close","volume","symbol"]).set_index(
                    pd.DatetimeIndex([], name="timestamp")
                )

            df = pd.DataFrame(raw, columns=[
                "open_time","open","high","low","close","volume",
                "close_time","quote_asset_volume","number_of_trades",
                "taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore"
            ])

            # --- optional: drop the last candle if it's incomplete, or force-drop it ---
            if len(df) > 0:
                if force_drop_last:
                    df = df.iloc[:-1].copy()
                elif only_closed:
                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    last_close_ms = int(df.iloc[-1]["close_time"])
                    # if the last candle hasn't closed yet, remove it
                    if last_close_ms >= now_ms:
                        df = df.iloc[:-1].copy()
            # --- end drop last candle ---


            df = df[["open_time","open","high","low","close","volume"]].copy()
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].apply(pd.to_numeric, errors="coerce")
            df["symbol"] = symbol
            df = df[["timestamp","open","high","low","close","volume","symbol"]].dropna(subset=["timestamp"])
            df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").set_index("timestamp")
            return df

        except BinanceAPIException as e:
            last_exc = e
            # invalid symbol -> no retries
            if getattr(e, "code", None) == -1121:
                return pd.DataFrame(columns=["timestamp","open","high","low","close","volume","symbol"]).set_index(
                    pd.DatetimeIndex([], name="timestamp")
                )
            retry_after = None
            response = getattr(e, "response", None)
            if response is not None:
                retry_after = response.headers.get("Retry-After")
            msg = f"[get_OHLCV] attempt {attempt}/{max_retries} failed for {symbol} {interval}: {repr(e)}"
            msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
            msg = telegram_prefix + msg
            print(msg)
            if notify:
                token = telegram_token or telegram.telegram_token_main
                telegram.send_telegram_message(token, telegram.EMOJI_WARNING, msg)
            if attempt < max_retries:
                if retry_after is not None and str(retry_after).isdigit():
                    time.sleep(int(retry_after))
                else:
                    time.sleep(backoff_sec * (2 ** (attempt - 1)))
            continue
        except BinanceRequestException as e:
            last_exc = e
            msg = f"[get_OHLCV] attempt {attempt}/{max_retries} failed for {symbol} {interval}: {repr(e)}"
            msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
            msg = telegram_prefix + msg
            print(msg)
            if notify:
                token = telegram_token or telegram.telegram_token_main
                telegram.send_telegram_message(token, telegram.EMOJI_WARNING, msg)
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** (attempt - 1)))
            continue
        except Exception as e:
            last_exc = e
            msg = f"[get_OHLCV] attempt {attempt}/{max_retries} failed for {symbol} {interval}: {repr(e)}"
            # print(msg, file=sys.stderr)
            msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
            msg = telegram_prefix + msg
            print(msg)
            if notify:
                token = telegram_token or telegram.telegram_token_main
                telegram.send_telegram_message(token, telegram.EMOJI_WARNING, msg)
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** (attempt - 1)))

    # All retries failed
    msg = f"[get_OHLCV] FAILED after {max_retries} attempts for {symbol} {interval}: {repr(last_exc)}"
    # print(msg, file=sys.stderr)
    msg = msg + sys._getframe(  ).f_code.co_name+" - "+symbol
    msg = telegram_prefix + msg
    print(msg)
    if notify:
        token = telegram_token or telegram.telegram_token_main
        telegram.send_telegram_message(token, telegram.EMOJI_WARNING, msg)
    return pd.DataFrame(columns=["timestamp","open","high","low","close","volume","symbol"]).set_index(
        pd.DatetimeIndex([], name="timestamp")
    )

# --- util ---
def _floor_day_epoch_s(ts_utc_series: pd.Series) -> pd.Series:
    """Return epoch seconds at the start of the day (UTC) as Python int (not numpy)."""
    s = pd.to_datetime(ts_utc_series, utc=True).dt.floor("D").view("int64") // 10**9
    # garantir 'int' puro para não ir como BLOB
    return s.astype("int64").astype(int)

def _utc_yesterday_midnight(now_utc: datetime | None = None) -> datetime:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    return (now_utc.replace(hour=0, minute=0, second=0, microsecond=0))

def _to_binance_param(x: Optional[Union[str, int, float, datetime]]) -> Optional[Union[str, int]]:
    """
    Binance aceita str (ex. '1 Jan, 2010') OU epoch em ms (int).
    Converte datetime → epoch ms; deixa str como está; números passam tal como estão.
    """
    if x is None:
        return None
    if isinstance(x, str):
        if x.isdigit():
            val = int(x)
            if val < 10**12:
                val *= 1000
            return val
        return x
    if isinstance(x, (int, float)):
        return int(x)
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        return int(x.timestamp() * 1000)  # ms
    raise TypeError(f"Unsupported type for Binance time param: {type(x)}")

def _normalize_interval(interval: str) -> str:
    """Normalize common interval aliases to Binance client constants."""
    if not isinstance(interval, str):
        return interval
    raw = interval.strip()
    mapping = {
        "1m": Client.KLINE_INTERVAL_1MINUTE,
        "3m": Client.KLINE_INTERVAL_3MINUTE,
        "5m": Client.KLINE_INTERVAL_5MINUTE,
        "15m": Client.KLINE_INTERVAL_15MINUTE,
        "30m": Client.KLINE_INTERVAL_30MINUTE,
        "1h": Client.KLINE_INTERVAL_1HOUR,
        "2h": Client.KLINE_INTERVAL_2HOUR,
        "4h": Client.KLINE_INTERVAL_4HOUR,
        "6h": Client.KLINE_INTERVAL_6HOUR,
        "8h": Client.KLINE_INTERVAL_8HOUR,
        "12h": Client.KLINE_INTERVAL_12HOUR,
        "1d": Client.KLINE_INTERVAL_1DAY,
        "3d": Client.KLINE_INTERVAL_3DAY,
        "1w": Client.KLINE_INTERVAL_1WEEK,
        "1M": Client.KLINE_INTERVAL_1MONTH,
    }
    return mapping.get(raw, raw)

def get_ohlcv(
    symbol: str,
    interval: str,
    start_date: Optional[Union[str, int, float, datetime]] = None,
    end_date: Optional[Union[str, int, float, datetime]] = None,
    *,
    max_retries: int = 3,
    backoff_sec: float = 1.5,
    drop_last: bool = False,
    drop_incomplete: bool = False,
    include_symbol: bool = False,
    set_index: bool = True,
    keep_time_col: bool = True,
) -> pd.DataFrame:
    if start_date is None:
        start_date = "1 Jan, 2010"
    df = _get_ohlcv_raw(
        symbol=symbol,
        interval=interval,
        start_date=start_date,
        end_date=end_date,
        max_retries=max_retries,
        backoff_sec=backoff_sec,
        force_drop_last=drop_last,
        only_closed=drop_incomplete,
        notify=False,
    )
    if df.empty:
        return df

    df = df.reset_index()
    df = df.rename(columns={
        "timestamp": "Time",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })

    df["Time"] = pd.to_datetime(df["Time"], utc=True).dt.tz_localize(None)
    df[["Open", "High", "Low", "Close", "Volume"]] = df[["Open", "High", "Low", "Close", "Volume"]].apply(
        pd.to_numeric, errors="coerce"
    )
    if include_symbol:
        df["Symbol"] = symbol

    if set_index:
        df.set_index(pd.DatetimeIndex(df["Time"]), inplace=True)

    if not keep_time_col and "Time" in df.columns:
        df = df.drop(columns=["Time"])

    return df

def get_close_df(
    symbol: str,
    interval: str,
    start_date: Optional[Union[str, int, float, datetime]] = None,
    end_date: Optional[Union[str, int, float, datetime]] = None,
    *,
    max_retries: int = 3,
    backoff_sec: float = 1.5,
    drop_last: bool = False,
    drop_incomplete: bool = True,
    include_symbol: bool = False,
    set_index: bool = True,
    keep_time_col: bool = False,
    price_col: str = "Close",
) -> pd.DataFrame:
    df = get_ohlcv(
        symbol=symbol,
        interval=interval,
        start_date=start_date,
        end_date=end_date,
        max_retries=max_retries,
        backoff_sec=backoff_sec,
        drop_last=drop_last,
        drop_incomplete=drop_incomplete,
        include_symbol=include_symbol,
        set_index=set_index,
        keep_time_col=keep_time_col,
    )
    if df.empty:
        return df

    cols = []
    if keep_time_col and "Time" in df.columns:
        cols.append("Time")
    if include_symbol and "Symbol" in df.columns:
        cols.append("Symbol")
    cols.append("Close")
    df = df[cols]
    if price_col != "Close":
        df = df.rename(columns={"Close": price_col})
    return df

# --- main sync ---
def sync_ohlcv_binance_daily(conn: sqlite3.Connection,
                             symbol: str = "BTCUSD",
                             timeframe: str = "1d",
                             source: str = "binance",
                             start_buffer_days: int = 1) -> int:
    """
    Sync missing daily OHLCV rows from Binance into SQLite 'ohlcv' table.
    Only fetches up to yesterday (UTC). Returns number of rows inserted/replaced.
    """
    assert timeframe == "1d", "This sync is for daily timeframe only."

    symbol_db = "BTCUSD"
    # 1) last stored day
    cur = conn.execute("""
        SELECT MAX(ts_utc) FROM ohlcv WHERE symbol=? AND timeframe=?
    """, (symbol_db, timeframe))
    last_ts = cur.fetchone()[0]  # epoch seconds or None

    # 2) compute start & end (UTC)
    # start: day after last_ts; or a safe default if table empty
    if last_ts is not None:
        start_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc) + timedelta(days=1)
    else:
        # se nunca inseriste, começa "há muito tempo"
        start_dt = datetime(2010, 1, 1, tzinfo=timezone.utc)

    # end: véspera (excluindo hoje)
    # Binance aceita datetime; a tua _get_ohlcv_raw trata disso.
    # Para evitar apanhar vela em curso, vamos buscar até 00:00 UTC de hoje (exclusive).
    end_dt_exclusive = _utc_yesterday_midnight()  # 00:00 de hoje
    # se não há nada novo, termina
    if start_dt >= end_dt_exclusive:
        return 0

    # 3) fetch from Binance (a tua função já devolve df com index timestamp UTC de open_time)
    df = _get_ohlcv_raw(symbol=symbol,
                   interval=Client.KLINE_INTERVAL_1DAY,
                   start_date=start_dt,
                   end_date=end_dt_exclusive)

    if df.empty:
        return 0

    # 4) preparar DataFrame para a tabela 'ohlcv'
    df = df.reset_index()  # 'timestamp' volta a coluna
    df["ts_utc"] = _floor_day_epoch_s(df["timestamp"])
    df = df.dropna(subset=["ts_utc", "open", "high", "low", "close"]).copy()
    df["symbol"] = symbol
    df["timeframe"] = timeframe
    df["source"] = source

    cols = ["symbol","timeframe","ts_utc","open","high","low","close","volume","source"]
    # garantir tipos corretos
    df["ts_utc"] = df["ts_utc"].astype(int)
    for c in ["open","high","low","close","volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    rows = list(df[cols].itertuples(index=False, name=None))
    if not rows:
        return 0

    # 5) UPSERT (PRIMARY KEY: symbol,timeframe,ts_utc)
    with conn:
        conn.executemany("""
            INSERT OR REPLACE INTO ohlcv
            (symbol, timeframe, ts_utc, open, high, low, close, volume, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

    return len(rows)
        
def create_balance_snapshot(telegram_prefix: str):
    msg = "Creating balance snapshot. It can take a few minutes..."
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)

    

    # Retrieve the balances of all coins in the user’s Binance account
    account_balances = get_client().get_account()['balances']

    # Get the current price of all tickers from the Binance API
    ticker_info = get_client().get_all_tickers()

    # Create a dictionary of tickers and their corresponding prices
    ticker_prices = {ticker['symbol']: float(ticker['price']) for ticker in ticker_info}
    btc_price = ticker_prices.get('BTCUSDC') or ticker_prices.get('BTCUSDT')
    if btc_price is None:
        telegram.send_telegram_message(telegram.telegram_token_main, telegram.EMOJI_WARNING,
            "[create_balance_snapshot] BTC price not found (BTCUSDC/USDT). Aborting.")
        return

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

        # Otherwise, check if the coin has a USD trading pair or a BTC trading pair
        elif unlocked_balance > 0.0:
            # Check if the coin has a USD trading pair
            # if (any(symbol + 'USDC' in i for i in ticker_prices)):
            if symbol + 'USDC' in ticker_prices:

                # If it does, calculate its USDC value and add it to the list of coins with their USDC values
                ticker_symbol = symbol + 'USDC'
                # ticker_price = ticker_prices.get(ticker_symbol)
                # coin_usdt_value = (unlocked_balance) * ticker_price

                symbol_balance = unlocked_balance
                symbol_usd_price = ticker_prices.get(ticker_symbol)

                if symbol_usd_price is not None:
                    symbol_balance_usd = symbol_balance * symbol_usd_price
                    symbol_balance_btc = symbol_balance * (symbol_usd_price/btc_price)

                    if symbol_balance_usd > 1:   
                        new_row = [date, symbol, symbol_balance, symbol_usd_price, btc_price, symbol_balance_usd, symbol_balance_btc]
                        symbol_values.append(new_row)
                else:
                    print(f"Price not found for ticker {ticker_symbol}")
            
            # If the coin does not have a USD trading pair, check if it has a BTC trading pair
            # elif (any(symbol + 'BTC' in i for i in ticker_prices)):
            elif symbol + 'BTC' in ticker_prices:

                # If it does, calculate its USD value and add it to the list of coins with their USD values
                ticker_symbol = symbol + 'BTC'
                symbol_btc_price = ticker_prices.get(ticker_symbol)

                if symbol_btc_price is not None:                
                    symbol_balance = unlocked_balance
                    symbol_usd_price = symbol_btc_price * btc_price
                    symbol_balance_usd = symbol_balance * symbol_usd_price
                    symbol_balance_btc = symbol_balance * symbol_btc_price
                    
                    if symbol_balance_usd > 1:
                        new_row = [date, symbol, symbol_balance, symbol_usd_price, btc_price, symbol_balance_usd, symbol_balance_btc]
                        symbol_values.append(new_row)
                else:
                    print(f"Price not found for ticker {ticker_symbol}")
        
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

    # add data to table Balance
    database.add_balances( df_balance)

    msg = "Balance snapshot finished"
    msg = telegram_prefix + msg
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)
