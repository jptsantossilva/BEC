
import pandas as pd
import config
from exchange import client, get_exchange_info
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance.helpers import round_step_size
import sys
import math
import numpy as np
from backtesting.lib import crossover
import logging
import telegram
import timeit
import config
import database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# calculate program run time
start = timeit.default_timer()

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')


# Global Vars
timeframe = None
runMode = None
gTimeFrameNum = None
gtimeframeTypeShort = None
gtimeframeTypeLong = None
telegramToken = None

# strategy
strategy_name = None

# create empty dataframes
# df_positions = None
# df_orders    = None

def read_arguments():
    # Check for the timeframe and runMode arguments
    
    # total arguments
    n = len(sys.argv)
    
    global timeframe
    global runMode
    global gTimeFrameNum
    global gtimeframeTypeShort
    global gtimeframeTypeLong
    global telegramToken

    if n < 2:
        print("Argument is missing")
        timeframe = input('Enter timeframe (1d, 4h or 1h):')
        runMode = input('Enter run mode (test, prod):')
    else:
        # argv[0] in Python is always the name of the script.
        timeframe = sys.argv[1]

        # run modes 
        # test - does not execute orders on the exchange
        # prod - execute orders on the exchange
        runMode = sys.argv[2]

    if timeframe == "1h":
        gTimeFrameNum = int("1")
        gtimeframeTypeShort = "h" # h, d
        gtimeframeTypeLong = "hour" # hour, day

        telegramToken = telegram.telegramToken_1h

    elif timeframe == "4h":
        gTimeFrameNum = int("4")
        gtimeframeTypeShort = "h" # h, d
        gtimeframeTypeLong = "hour" # hour, day

        telegramToken = telegram.telegramToken_4h

    elif timeframe == "1d":
        gTimeFrameNum = int("1")
        gtimeframeTypeShort = "d" # h, d
        gtimeframeTypeLong = "day" # hour, day

        telegramToken = telegram.telegramToken_1d

def calc_stake_amount(symbol):
    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions()

        if num_open_positions >= config.max_number_of_open_positions:
            return -2 

        try:
            balance = float(client.get_asset_balance(asset = symbol)['free'])
            
        except BinanceAPIException as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            return 0
        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
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
    

def get_data(symbol, aTimeframeNum, aTimeframeTypeShort):

    try:
        # update EMAs from the best EMA return ratio
        global strategy_name

        lTimeFrame = str(aTimeframeNum)+aTimeframeTypeShort
        if aTimeframeTypeShort == "h":
            lTimeframeTypeLong = "hour"
        elif aTimeframeTypeShort == "d":
            lTimeframeTypeLong = "day"
        
        # get best ema
        df_best_ema = database.get_best_ema_by_symbol_timeframe(symbol=symbol, time_frame=lTimeFrame)

        if not df_best_ema.empty:
            fast_ema = int(df_best_ema.Ema_Fast.values[0])
            slow_ema = int(df_best_ema.Ema_Slow.values[0])
        else:
            fast_ema = int("0")
            slow_ema = int("0")

        strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

        # if bestEMA does not exist return empty dataframe in order to no use that trading pair
        if fast_ema == 0:
            frame = pd.DataFrame()
            return frame, fast_ema, slow_ema
        
        # if best Ema exist get price data 
        # lstartDate = str(1+gSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC"
        sma200 = 200
        # lstartDate = str(sma200*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
        ltimeframe = str(aTimeframeNum)+aTimeframeTypeShort
        frame = pd.DataFrame(client.get_historical_klines(symbol
                                                        ,ltimeframe
    
                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        # ,lstartDate
                                                        
                                                        ))

        frame = frame[[0,4]]
        frame.columns = ['Time','Close']
        frame.Close = frame.Close.astype(float)
        frame.Time = pd.to_datetime(frame.Time, unit='ms')
        return frame, fast_ema, slow_ema
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        frame = pd.DataFrame()
        return frame 

# calculates moving averages 
def apply_technicals(df, fast_ema, slow_ema): 
    df['FastEMA'] = df['Close'].ewm(span=fast_ema, adjust=False).mean()
    df['SlowEMA'] = df['Close'].ewm(span=slow_ema, adjust=False).mean()
    df['SMA50']   = df['Close'].rolling(50).mean()
    df['SMA200']  = df['Close'].rolling(200).mean()

# calc current pnl  
def get_current_pnl(symbol, current_price):

    try:
        # get buy price
        df_buy_price = database.get_positions_by_bot_symbol_position(bot=timeframe, symbol=symbol, position=1)
        buy_price = 0
        pnl_perc = 0
        
        if not df_buy_price.empty:
            # get buy price
            buy_price = df_buy_price['Buy_Price'].iloc[0]
            # check if buy price is fulfilled 
            if not math.isnan(buy_price) and buy_price > 0:
                # calc pnl percentage
                pnl_perc = ((current_price - buy_price) / buy_price) * 100
                pnl_perc = round(pnl_perc, 2)
        
        return pnl_perc
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)


def adjust_size(symbol, amount):
    
    for filt in client.get_symbol_info(symbol)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            stepSize = float(filt['stepSize'])
            minQty = float(filt['minQty'])
            break

    order_quantity = round_step_size(amount, stepSize)
    return order_quantity

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        return -1

def trade():

    # Make sure we are only trying to buy positions on symbols included on market phases table
    database.delete_positions_not_top_rank()

    # read Positions
    # global df_positions   
    # df_positions = database.get_positions_by_bot_position(bot=timeframe, position=1)

    # list of coins in position - SELL
    # list_to_sell = df_positions[df_positions.position == 1].Symbol
    df_sell = database.get_positions_by_bot_position(bot=timeframe, position=1)
    list_to_sell = df_sell.Symbol.tolist()
    
    
    # list of coins in position - BUY
    # list_to_buy = df_positions[df_positions.position == 0].Symbol
    df_buy = database.get_positions_by_bot_position(bot=timeframe, position=0)
    list_to_buy = df_buy.Symbol.tolist()
    
    # check open positions and SELL if conditions are fulfilled 
    for symbol in list_to_sell:
        df, fast_ema, slow_ema = get_data(symbol, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
            print(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)
        lastrow = df.iloc[-1]

        # separate symbol from stable. example symbol=BTCUSDT coinOnly=BTC coinStable=USDT
        if symbol.endswith("BTC"):
            symbol_only = symbol[:-3]
            symbol_stable = symbol[-3:]
        elif symbol.endswith(("BUSD","USDT")):    
            symbol_only = symbol[:-4]
            symbol_stable = symbol[-4:]

        # if using stop loss
        sell_stop_loss = False
        if config.stop_loss > 0:
            # check current price
            current_price = lastrow.Close
            # check current pnl
            current_pnl = get_current_pnl(symbol, current_price)
            sell_stop_loss = current_pnl <= -config.stop_loss

        if (lastrow.SlowEMA > lastrow.FastEMA) or sell_stop_loss:
            try:
                balanceQty = float(client.get_asset_balance(asset=symbol_only)['free'])  
            except BinanceAPIException as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                continue
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
                print(msg)
                telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                continue            

            # verify sell quantity
            # buy_order_qty = float(df_positions[df_positions.Symbol == symbol].Qty.values[0])
            df_buy_price = database.get_positions_by_bot_symbol_position(bot=timeframe, symbol=symbol, position=1)
            if not df_buy_price.empty:
                buy_order_qty = df_buy_price['Buy_Price'].iloc[0]
            
            sellQty = buy_order_qty
            if balanceQty < buy_order_qty:
                sellQty = balanceQty
            sellQty = adjust_size(symbol, sellQty)

            if sellQty > 0:                
                try:        
                    if runMode == "prod":
                        order = client.create_order(symbol=symbol,
                                                side=client.SIDE_SELL,
                                                type=client.ORDER_TYPE_MARKET,
                                                quantity = sellQty
                                                )
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)

                        # update position file with the sell order
                        database.set_position_sell(timeframe, symbol)

                except BinanceAPIException as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                except BinanceOrderException as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                except Exception as e:
                    msg = "SELL create_order - "+repr(e)
                    print(msg)
                    telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)

                # add new row to end of DataFrame
                if runMode == "prod":
                    pnl_value, pnl_perc = database.add_order_sell(exchange_order_id = order['orderId'],
                                                                  date = pd.to_datetime(order['transactTime'], unit='ms'),
                                                                  bot = timeframe,
                                                                  symbol = symbol,
                                                                  price = avg_price,
                                                                  qty = order['executedQty'],
                                                                  ema_fast = fast_ema,
                                                                  ema_slow = slow_ema,
                                                                  exit_reason = "EMA cross"
                                                                  )
                
                    
                    # determine the alert type based on the value of pnl_value
                    if pnl_value > 0:
                        alert_type = telegram.eTradeWithProfit
                    else:
                        alert_type = telegram.eTradeWithLoss
                    # call send_telegram_alert with the appropriate alert type
                    telegram.send_telegram_alert(telegramToken, 
                                                 alert_type,
                                                 pd.to_datetime(order['transactTime'], unit='ms'), 
                                                 order['symbol'], 
                                                 timeframe,
                                                 strategy_name,
                                                 order['side'],
                                                 avg_price,
                                                 order['executedQty'],
                                                 avg_price*float(order['executedQty']),
                                                 pnl_perc,
                                                 pnl_value
                                                 )

                        
            else:
                if runMode == "prod":
                    # if there is no qty on balance to sell we set the qty on positions file to zero
                    # this can happen if we sell on the exchange (for example, due to a pump) before the bot sells it. 
                    database.set_position_sell(timeframe, symbol)
        else:
            msg = f'{symbol} - {strategy_name} - Sell condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)
            
            # set current PnL
            lastrow = df.iloc[-1]
            current_price = lastrow.Close
            database.update_position_pnl(bot=timeframe, symbol=symbol, curr_price=current_price)


    # check coins not in positions and BUY if conditions are fulfilled
    for symbol in list_to_buy:
        df, fast_ema, slow_ema = get_data(symbol, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
            print(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)
        lastrow = df.iloc[-1]

        # separate symbol from stable. example symbol=BTCUSDT symbol_only=BTC symbol_stable=USDT 
        if symbol.endswith("BTC"):
            symbol_only = symbol[:-3]
            symbol_stable = symbol[-3:]
        elif symbol.endswith(("BUSD","USDT")):    
            symbol_only = symbol[:-4]
            symbol_stable = symbol[-4:]

        # if we wanna be more agressive we can use the following approach:
        # since the coin pair by marketphase is already choosing the coins in bullish and accumulation phase on daily time frame 
        # we can pass the verification of those market phases in lower timeframes, 4h and 1h, otherwise we will loose some oportunities
        # to be more conservative = use the same approach as the backtesting and keep those market phase verification in lower timeframes
        accumulationPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
        bullishPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)
        
        if (accumulationPhase or bullishPhase) and crossover(df.FastEMA, df.SlowEMA):
            positionSize = calc_stake_amount(symbol=symbol_stable)
            if positionSize > 0:
                
                if runMode == "prod":
                    try:
                        order = client.create_order(symbol=symbol,
                                                            side=client.SIDE_BUY,
                                                            type=client.ORDER_TYPE_MARKET,
                                                            quoteOrderQty = positionSize,
                                                            newOrderRespType = 'FULL') 
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)
                        
                        # update positions with the buy order
                        database.set_position_buy(bot=timeframe, 
                                                  symbol=symbol,
                                                  qty=float(order['executedQty']),
                                                  buy_price=avg_price,
                                                  date=pd.to_datetime(order['transactTime'], unit='ms'),
                                                  buy_order_id=order['orderId']
                                                  )
                    
                    except BinanceAPIException as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                    except BinanceOrderException as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                    except Exception as e:
                        msg = "BUY create_order - "+repr(e)
                        print(msg)
                        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
                
                if runMode == "prod":
                    database.add_order_buy(exchange_order_id=order['orderId'],
                                           date=pd.to_datetime(order['transactTime'], unit='ms'),
                                           bot=timeframe,
                                           symbol=symbol,
                                           price=avg_price,
                                           qty=float(order['executedQty']),
                                           ema_fast=fast_ema,
                                           ema_slow=slow_ema
                                           )
                            
                    
                    telegram.send_telegram_alert(telegramToken, telegram.eEnterTrade,
                                    pd.to_datetime(order['transactTime'], unit='ms'),
                                    order['symbol'], 
                                    str(gTimeFrameNum)+gtimeframeTypeShort, 
                                    strategy_name,
                                    order['side'],
                                    avg_price,
                                    order['executedQty'],
                                    positionSize)
            
            elif positionSize == -2:
                num_open_positions = database.get_num_open_positions(bot=timeframe)
                telegram.send_telegram_message(telegramToken, telegram.eInformation, client.SIDE_BUY+" "+symbol+" - Max open positions ("+str(num_open_positions)+"/"+str(config.max_number_of_open_positions)+") already occupied!")
            else:
                telegram.send_telegram_message(telegramToken, telegram.eInformation, client.SIDE_BUY+" "+symbol+" - Not enough "+symbol_stable+" funds!")
                
        else:
            msg = f'{symbol} - {strategy_name} - Buy condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)

def positions_summary():
        
    df_summary = database.get_positions_by_bot_position(bot=timeframe, position=1)
    
    # remove unwanted columns
    df_dropped = df_summary.drop(columns=['Bot','Position','Rank','Qty'])
    
    # sort by symbol
    df_sorted = df_dropped.sort_values("Symbol")
    
    # df_cp_to_print.rename(columns={"Currency": "Symbol", "Close": "Price", }, inplace=True)
    df_sorted.reset_index(drop=True, inplace=True) # gives consecutive numbers to each row
    if df_sorted.empty:
        print("Result: no open positions yet")
        telegram.send_telegram_message(telegramToken, "", "Result: no open positions")
    else:
        print(df_sorted)
        telegram.send_telegram_message(telegramToken, "", df_sorted.to_string())

    if config.stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions()
        msg = f"{str(num_open_positions)}/{str(config.max_number_of_open_positions)} positions occupied"
        print(msg)
        telegram.send_telegram_message(telegramToken, "", msg=msg)


def main():

    read_arguments()

    # inform that bot has started
    telegram.send_telegram_message(telegramToken, telegram.eStart, "Start")
    
    trade()

    positions_summary()

    # Close the database connection
    database.connection.close()

    # inform that ended
    telegram.send_telegram_message(telegramToken, telegram.eStop, "Binance Trader Bot - End")

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.duration(total_seconds)

    msg = f'Execution Time: {duration}'
    print(msg)
    telegram.send_telegram_message(telegramToken, "", msg)

if __name__ == "__main__":
    main()



