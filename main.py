"""
Read position table, get symbols with open position and check sell conditions, then get symbols not in position and check consitions to buy.  
"""


import os
# import re
from xml.dom import ValidationErr
import pandas as pd
import config
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
# from binance import BinanceSocketManager
from binance.helpers import round_step_size
import requests
from datetime import datetime
import time
import sys
import math
import numpy as np
#import dataframe_image as dfi
from numbers import Number
from typing import Sequence
from backtesting.lib import crossover
import logging
import telegram
import timeit
import yaml
import config
import database
# from add_symbol import get_performance_rank



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

connection = None

binance_client = None

# strategy
fast_ema = int("8")
slow_ema = int("34")
strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA CROSS"

# create empty dataframes
df_positions = None
df_orders    = None
df_best_ema  = None

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


# try:
#     with open("config.yaml", "r") as file:
#         config = yaml.safe_load(file)

#     stake_amount_type               = config["stake_amount_type"]
#     max_number_of_open_positions    = config["max_number_of_open_positions"]
#     tradable_balance_ratio          = config["tradable_balance_ratio"]
#     min_position_size               = config["min_position_size"]
#     trade_against                   = config["trade_against"]
#     stop_loss                       = config["stop_loss"]

# except FileNotFoundError as e:
#     msg = "Error: The file config.yaml could not be found."
#     msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
#     sys.exit(msg) 

# except yaml.YAMLError as e:
#     msg = "Error: There was an issue with the YAML file."
#     msg = msg + " " + sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
#     sys.exit(msg)

# environment variables
# try:
#     # Binance
#     api_key = os.environ.get('binance_api')
#     api_secret = os.environ.get('binance_secret')

# except KeyError as e: 
#     msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
#     print(msg)
#     logging.exception(msg)
#     telegram.send_telegram_message(telegram.telegramToken_errors, telegram.eWarning, msg)
#     sys.exit(msg) 

def connect_binance():
    api_key = config.get_env_var('binance_api')
    api_secret = config.get_env_var('binance_secret')

    # Binance Client
    try:
        global binance_client
        binance_client = Client(api_key, api_secret)
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            sys.exit(msg) 





# def read_csv_files():

#     global df_positions
#     global df_orders
#     global df_best_ema

#     try:
#         # read positions
#         # make sure performance rank is fulfilled
#         # set_performance_rank()

#         filename = 'positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
#         df_positions = pd.read_csv(filename)

#         # read orders csv
#         # we just want the header, there is no need to get all the existing orders.
#         # at the end we will append the orders to the csv
#         filename = 'orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
#         df_orders = pd.read_csv(filename, nrows=0)

#         # read best ema cross
#         filename = 'coinpairBestEma.csv'
#         df_best_ema = pd.read_csv(filename)

#     except FileNotFoundError as e:
#         msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
#         print(msg)
#         logging.exception(msg)
#         telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)         
#     except PermissionError as e:
#         msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
#         print(msg)
#         logging.exception(msg)
#         telegram.send_telegram_message(telegramToken, telegram.eWarning, msg) 
#     except Exception as e:
#         msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
#         print(msg)
#         logging.exception(msg)
#         telegram.send_telegram_message(telegramToken, telegram.eWarning, msg) 

def read_tables(connection, bot):

    global df_positions
    global df_orders
    global df_best_ema
        
    df_positions = pd.DataFrame(database.get_all_positions_by_bot(connection, bot = bot))

    # read orders csv
    # we just want the header, there is no need to get all the existing orders.
    # # at the end we will append the orders to the csv
    # filename = 'orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
    # df_orders = pd.read_csv(filename, nrows=0)

    # read best ema
    df_best_ema = pd.DataFrame(database.get_all_best_ema(connection))


# def get_num_open_positions():
#     try:
#         df_open_positions_1h = pd.read_csv('positions1h.csv')
#         df_open_positions_1h = df_open_positions_1h[df_open_positions_1h.position == 1].Currency
        
        

#         df_open_positions_4h = pd.read_csv('positions4h.csv')
#         df_open_positions_4h = df_open_positions_4h[df_open_positions_4h.position == 1].Currency

#         df_open_positions_1d = pd.read_csv('positions1d.csv')
#         df_open_positions_1d = df_open_positions_1d[df_open_positions_1d.position == 1].Currency

#         total_open_positions = len(df_open_positions_1h) +len(df_open_positions_4h) +len(df_open_positions_1d)
#         return total_open_positions

#     except Exception as e:
#         msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
#         print(msg)
#         logging.exception(msg)
#         telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
#         return -1
    
def calc_stake_amount(symbol):
    if stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(connection)

        if num_open_positions >= max_number_of_open_positions:
            return -2 

        try:
            balance = float(binance_client.get_asset_balance(asset = symbol)['free'])
            
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
    
        tradable_balance = balance*tradable_balance_ratio 
        
        stake_amount = tradable_balance/(max_number_of_open_positions-num_open_positions)
        
        if symbol == "BTC":
            stake_amount = round(stake_amount, 8)
        elif symbol in ("BUSD", "USDT"):
            stake_amount = int(stake_amount)
        
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
    

def get_data(symbol, aTimeframeNum, aTimeframeTypeShort):

    try:
        # update EMAs from the best EMA return ratio
        global fast_ema
        global slow_ema
        global strategy_name

        lTimeFrame = str(aTimeframeNum)+aTimeframeTypeShort
        if aTimeframeTypeShort == "h":
            lTimeframeTypeLong = "hour"
        elif aTimeframeTypeShort == "d":
            lTimeframeTypeLong = "day"
        
        listEMAvalues = df_best_ema[(df_best_ema.Symbol == symbol) & (df_best_ema.Time_Frame == lTimeFrame)]

        if not listEMAvalues.empty:
            fast_ema = int(listEMAvalues.Ema_Fast.values[0])
            slow_ema = int(listEMAvalues.Ema_Slow.values[0])
        else:
            fast_ema = int("0")
            slow_ema = int("0")

        strategy_name = str(fast_ema)+"/"+str(slow_ema)+" EMA cross"

        # if bestEMA does not exist return empty dataframe in order to no use that trading pair
        if fast_ema == 0:
            frame = pd.DataFrame()
            return frame
        
        # if best Ema exist get price data 
        # lstartDate = str(1+gSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC"
        sma200 = 200
        lstartDate = str(sma200*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
        ltimeframe = str(aTimeframeNum)+aTimeframeTypeShort
        frame = pd.DataFrame(binance_client.get_historical_klines(symbol
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
        return frame
    
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        frame = pd.DataFrame()
        return frame 

#-----------------------------------------------------------------------
# calculates moving averages 
#-----------------------------------------------------------------------
def apply_technicals(df, aFastMA, aSlowMA):
    
    if aFastMA > 0: 
        df['FastEMA'] = df['Close'].ewm(span=aFastMA, adjust=False).mean()
        df['SlowEMA'] = df['Close'].ewm(span=aSlowMA, adjust=False).mean()
        df['SMA50'] = df['Close'].rolling(50).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# calc current pnl  
#-----------------------------------------------------------------------
def get_current_pnl(dfPos, symbol, current_price):

    try:
        # go to symbol line 
        pos = dfPos.loc[dfPos['Symbol'] == symbol]
        pnl_perc = 0
        
        if not pos.empty:
            # get buy price
            buy_price = pos.at[pos.index[0], 'Buy_Price']
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
        
#-----------------------------------------------------------------------

# #-----------------------------------------------------------------------
# # Update positions files 
# #-----------------------------------------------------------------------
# def change_pos(dfPos, curr, order, typePos, buyPrice=0, currentPrice=0):

#     # type = buy, sell or updatePnL

#     try:

#         if typePos == "buy":
#             dfPos.loc[dfPos['Currency'] == curr, ['position','quantity','buyPrice','currentPrice']] = [1,float(order['executedQty']),float(buyPrice),float(buyPrice)]
#         elif typePos == "sell":
#             dfPos.loc[dfPos['Currency'] == curr, ['position','quantity','buyPrice','currentPrice','PnLperc']] = [0,0,0,0,0]
#         elif typePos == "updatePnL":
#             pos = dfPos.loc[dfPos['Currency'] == curr]
#             if len(pos) > 0:
#                 lBuyPrice = pos['buyPrice'].values[0]
#                 if not math.isnan(lBuyPrice) and (lBuyPrice > 0):
#                     PnLperc = ((currentPrice-lBuyPrice)/lBuyPrice)*100
#                     PnLperc = round(PnLperc, 2)
#                     dfPos.loc[dfPos['Currency'] == curr, ['currentPrice','PnLperc']] = [currentPrice,PnLperc]

#         dfPos.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

#     except Exception as e:
#         msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
#         print(msg)
#         telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        
# #-----------------------------------------------------------------------


# Make sure we are only trying to buy positions on symbols included on market phases table.  
# this is needed here specially for 1h/4h time frames when coin is no longer on bullish or accumulation and a close position occurred
# and we dont want to back in position during the same day
def remove_coins_position():
    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv

    # dfAllByMarketPhase = pd.read_csv(f'coinpairByMarketPhase_{trade_against}_1d.csv')
    # # dfBullish = dfAllByMarketPhase.query("MarketPhase == 'bullish'")
    # # dfAccumulation= dfAllByMarketPhase.query("MarketPhase == 'accumulation'")
    # # # union accumulation and bullish results
    # # dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)
    # # accuBullishCoinPairs = dfUnion.Coinpair.to_list()

    # positionsfile = pd.read_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv')

    # filter1 = (positionsfile['position'] == 1) & (positionsfile['quantity'] > 0)
    # filter2 = positionsfile['Currency'].isin(dfAllByMarketPhase['Coinpair'])
    # positionsfile = positionsfile[filter1 | filter2]

    # # order by name
    # positionsfile.sort_values(by=['Currency'], inplace=True)

    # positionsfile.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

    database.delete_positions_not_top_rank(connection)


def adjust_size(symbol, amount):
    
    for filt in binance_client.get_symbol_info(symbol)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            stepSize = float(filt['stepSize'])
            minQty = float(filt['minQty'])
            break

    order_quantity = round_step_size(amount, stepSize)
    return order_quantity


# def calc_pnl(symbol, sellprice: float, sellqty: float):
#     # try:

#     df_last_buy_order = pd.DataFrame(database.get_last_buy_order_by_bot_symbol(connection, bot = timeframe, symbol = symbol))

#     if df_last_buy_order.empty:
#         print("DataFrame is empty")
#     else:
#         buy_order_id = df_last_buy_order.loc[0, 'Buy_Order_Id']
#         buy_price = df_last_buy_order.loc[0, 'Price']
#         buy_qty = df_last_buy_order.loc[0, 'Qty']

#         PnLperc = (((sellprice*sellqty)-(buy_price*buy_qty))/(buy_price*buy_qty))*100
#         PnLperc = round(PnLperc, 2)
#         PnLvalue = (sellprice*sellqty)-(buy_price*buy_qty)
#         PnLvalue = round(PnLvalue, n_decimals)

#         list = [buy_order_id, PnLperc, PnLvalue]
        
#         return list


    #     # open orders file to search last buy order for the coin and time frame provided on the argument.
    #     with open(r"orders"+str(gTimeFrameNum)+gtimeframeTypeShort+".csv", 'r') as fp:
    #         for l_no, line in reversed(list(enumerate(fp))):
    #             # search string
    #             if (symbol in line) and ("BUY" in line):

    #                 # print('string found in a file')
    #                 # print('Line Number:', l_no)
    #                 # print('Line:', line)
                    
    #                 # sellprice = 300
    #                 orderid = line.split(',')[0]
    #                 # print('orderid:', orderid)
    #                 buyprice = float(line.split(',')[4])
    #                 # print('Buy Price:', buyprice)
    #                 buyqty = float(line.split(',')[5])
    #                 # print('Buy qty:', buyqty)
    #                 # print('Sell price:', sellprice)
    #                 # sellqty = buyqty
    #                 # PnLperc = ((sellprice-buyprice)/buyprice)*100
    #                 PnLperc = (((sellprice*sellqty)-(buyprice*buyqty))/(buyprice*buyqty))*100
    #                 PnLperc = round(PnLperc, 2)
    #                 PnLvalue = (sellprice*sellqty)-(buyprice*buyqty)
    #                 PnLvalue = round(PnLvalue, n_decimals)
    #                 # print('Buy USD =', round(buyprice*buyqty,2))
    #                 # print('Sell USD =', round(sellprice*sellqty,2))
    #                 # print('PnL% =', PnLperc)
    #                 # print('PnL USD =', PnLvalue)
                    
    #                 lista = [orderid, PnLperc, PnLvalue]
    #                 return lista
                    
    #                 # terminate the loop
    #                 break

    # except Exception as e:
    #     msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
    #     print(msg)
    #     telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
    #     return []

def get_open_positions(df):
    try:
        df_open_positions = df[df.position == 1]
        return df_open_positions

    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
        return -1

# get performance rank values from coinpairByMarketPhase and set to position file
# def set_performance_rank():

#     filename = 'positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv'
#     df_pos = pd.read_csv(filename)  

#     filename = f'coinpairByMarketPhase_{trade_against}_1d.csv'
#     df_mp = pd.read_csv(filename)
    
#     df_merged = df_pos.merge(df_mp, left_on='Currency', right_on='Coinpair', how='left')
#     df_pos['performance_rank'] = df_merged['performance_rank_y']

#     # those that dont have performance rank number will set rank num to 1000 to make sure that they are at the end of the list
#     df_pos['performance_rank'].fillna(1000, inplace=True)

#     df_pos.to_csv('positions'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', index=False)

def trade():

    # remove coin pairs from position file not in accumulation or bullish phase -> coinpairByMarketPhase_BUSD_1d.csv
    remove_coins_position()

    # read Positions, Orders and Best_Ema
    read_tables()

    # sort positions by performance rank
    # df_positions.sort_values(by=['performance_rank'], inplace=True)

    # list of coins in position - SELL
    list_to_sell = df_positions[df_positions.position == 1].Symbol
    
    # list of coins in position - BUY
    list_to_buy = df_positions[df_positions.position == 0].Symbol
    
    # ------------------------------------------------------------
    # check open positions and SELL if conditions are fulfilled 
    # ------------------------------------------------------------
    for symbol in list_to_sell:
        df = get_data(symbol, gTimeFrameNum, gtimeframeTypeShort)

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
        if stop_loss > 0:
            # check current price
            current_price = lastrow.Close
            # check current pnl
            current_pnl = get_current_pnl(df_positions, symbol, current_price)
            sell_stop_loss = current_pnl <= -stop_loss

        if (lastrow.SlowEMA > lastrow.FastEMA) or sell_stop_loss:
            try:
                balanceQty = float(binance_client.get_asset_balance(asset=symbol_only)['free'])  
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
            buyOrderQty = float(df_positions[df_positions.Symbol == symbol].Qty.values[0])
            sellQty = buyOrderQty
            if balanceQty < buyOrderQty:
                sellQty = balanceQty
            sellQty = adjust_size(symbol, sellQty)

            if sellQty > 0:                
                try:        
                    if runMode == "prod":
                        order = binance_client.create_order(symbol=symbol,
                                                side=binance_client.SIDE_SELL,
                                                type=binance_client.ORDER_TYPE_MARKET,
                                                quantity = sellQty
                                                )
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)

                        # update position file with the sell order
                        # changepos(df_positions, coinPair,'',buy=False)
                        # change_pos(df_positions, symbol, '', typePos="sell")
                        database.set_position_sell(connection, timeframe, symbol)

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

                #add new row to end of DataFrame
                if runMode == "prod":
                    # addPnL = calc_pnl(symbol, float(avg_price), float(order['executedQty']))
                    # if addPnL:
                    #     df_orders.loc[len(df_orders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), symbol, 
                    #                                         order['side'], avg_price, order['executedQty'],
                    #                                         addPnL[0], # buyorderid 
                    #                                         addPnL[1], # PnL%
                    #                                         addPnL[2]  # PnL trade against
                    #                                         ]
                        
                    pnl_value, pnl_perc = database.add_order_sell(connection, 
                                            exchange_order_id = order['orderId'],
                                            date = pd.to_datetime(order['transactTime'], unit='ms'),
                                            bot = timeframe,
                                            symbol = symbol,
                                            price = avg_price,
                                            qty = order['executedQty'],
                                            ema_fast = fast_ema,
                                            ema_slow = slow_ema,
                                            exit_reason = "EMA cross"
                                            )
                
                    # if addPnL[2] > 0: 
                    if pnl_value > 0:
                        # trade with profit
                        telegram.send_telegram_alert(telegramToken, telegram.eTradeWithProfit,
                                    pd.to_datetime(order['transactTime'], unit='ms'), 
                                    order['symbol'], 
                                    # str(gTimeFrameNum)+gtimeframeTypeShort, 
                                    timeframe,
                                    strategy_name,
                                    order['side'],
                                    avg_price,
                                    order['executedQty'],
                                    avg_price*float(order['executedQty']),
                                    # addPnL[1], # PnL%
                                    # addPnL[2]  # PnL trade against
                                    pnl_perc,
                                    pnl_value
                                    )
                    else:
                        # trade with loss
                        telegram.send_telegram_alert(telegramToken, telegram.eTradeWithLoss,
                                    pd.to_datetime(order['transactTime'], unit='ms'), 
                                    order['symbol'], 
                                    # str(gTimeFrameNum)+gtimeframeTypeShort, 
                                    timeframe,
                                    strategy_name,
                                    order['side'],
                                    avg_price,
                                    order['executedQty'],
                                    avg_price*float(order['executedQty']),
                                    # addPnL[1], # PnL%
                                    # addPnL[2]  # PnL trade against
                                    pnl_perc,
                                    pnl_value
                                    )

                        
            else:
                if runMode == "prod":
                    # if there is no qty on balance to sell we set the qty on positions file to zero
                    # this can happen if we sell on the exchange (for example, due to a pump) before the bot sells it. 
                    # changepos(df_positions, coinPair,'',buy=False)
                    # change_pos(df_positions, symbol, '', typePos="sell")
                    database.set_position_sell(connection, timeframe, symbol)
        else:
            msg = f'{symbol} - {strategy_name} - Sell condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)
            
            # set current PnL
            lastrow = df.iloc[-1]
            current_price = lastrow.Close
            # changepos(df_positions, coinPair,'',buy=False)
            # change_pos(df_positions, symbol, '', typePos="updatePnL", currentPrice=current_price)
            database.update_position_pnl(connection, bot=timeframe, symbol=symbol, curr_price=current_price)


    # ------------------------------------------------------------------
    # check coins not in positions and BUY if conditions are fulfilled
    # ------------------------------------------------------------------
    for symbol in list_to_buy:
        # sendTelegramMessage("",coinPair) 
        df = get_data(symbol, gTimeFrameNum, gtimeframeTypeShort)

        if df.empty:
            msg = f'{symbol} - {strategy_name} - Best EMA values missing'
            print(msg)
            telegram.send_telegram_message(telegramToken, telegram.eWarning, msg)
            continue

        apply_technicals(df, fast_ema, slow_ema)
        lastrow = df.iloc[-1]

        # separate coin from stable. example symbol=BTCUSDT symbol_only=BTC symbol_stable=USDT 
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
        # if crossover(df.FastEMA, df.SlowEMA):
            positionSize = calc_stake_amount(coin=symbol_stable)
            # sendTelegramMessage("", "calc position size 5")
            # print("positionSize: ", positionSize)
            # sendTelegramMessage('',client.SIDE_BUY+" "+symbol+" BuyStableQty="+str(positionSize))  
            if positionSize > 0:
                
                if runMode == "prod":
                    try:
                        order = binance_client.create_order(symbol=symbol,
                                                            side=binance_client.SIDE_BUY,
                                                            type=binance_client.ORDER_TYPE_MARKET,
                                                            quoteOrderQty = positionSize,
                                                            newOrderRespType = 'FULL') 
                        
                        fills = order['fills']
                        avg_price = sum([float(f['price']) * (float(f['qty']) / float(order['executedQty'])) for f in fills])
                        avg_price = round(avg_price,8)
                        # print('avg_price=',avg_price)

                        # update positions file with the buy order
                        # changepos(df_positions, symbol,order,buy=True,buyPrice=avg_price)
                        # change_pos(df_positions, symbol, order, typePos="buy", buyPrice=avg_price)
                        database.set_position_buy(connection=connection, 
                                                  bot=timeframe, 
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
                    # df_orders.loc[len(df_orders.index)] = [order['orderId'], pd.to_datetime(order['transactTime'], unit='ms'), symbol, 
                    #                                     order['side'], avg_price, order['executedQty'],
                    #                                     0,0,0]
                    
                    database.add_order_buy(connection=connection,
                                           exchange_order_id=order['orderId'],
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
                num_open_positions = database.get_num_open_positions(connection)
                telegram.send_telegram_message(telegramToken, telegram.eInformation, binance_client.SIDE_BUY+" "+symbol+" - Max open positions ("+str(num_open_positions)+"/"+str(max_number_of_open_positions)+") already occupied!")
            else:
                telegram.send_telegram_message(telegramToken, telegram.eInformation, binance_client.SIDE_BUY+" "+symbol+" - Not enough "+symbol_stable+" funds!")
                
        else:
            msg = f'{symbol} - {strategy_name} - Buy condition not fulfilled'
            print(msg)
            telegram.send_telegram_message(telegramToken, "", msg)

def positions_summary():
        
    df_positions = pd.DataFrame(database.get_all_positions_by_bot_position1(connection, timeframe))
    
    # remove unwanted columns
    df_dropped = df_positions.drop(columns=['Bot','Position','Rank','Qty'])
    
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

    if stake_amount_type == "unlimited":
        num_open_positions = database.get_num_open_positions(connection)
        msg = f"{str(num_open_positions)}/{str(max_number_of_open_positions)} positions occupied"
        print(msg)
        telegram.send_telegram_message(telegramToken, "", msg=msg)


def main():

    read_arguments()

    # inform that bot has started
    telegram.send_telegram_message(telegramToken, telegram.eStart, "Start")



    # global stake_amount_type, max_number_of_open_positions, tradable_balance_ratio, min_position_size, trade_against, stop_loss

    # # get settings from config file
    # stake_amount_type            = config.get_settings_config_file("stake_amount_type")
    # max_number_of_open_positions = config.get_settings_config_file("max_number_of_open_positions")
    # tradable_balance_ratio       = config.get_settings_config_file("tradable_balance_ratio")
    # min_position_size            = config.get_settings_config_file("min_position_size")
    # trade_against                = config.get_settings_config_file("trade_against")
    # stop_loss                    = config.get_settings_config_file("stop_loss")

    # global n_decimals
    # if trade_against == "BTC":
    #     n_decimals = 8
    # elif trade_against in ["BUSD","USDT"]:    
    #     n_decimals = 2

    connect_binance()

    # sqlite database
    global connection
    connection = database.connect()
    database.create_tables(connection)
    
    trade()

    # add orders to csv file
    # df_orders.to_csv('orders'+str(gTimeFrameNum)+gtimeframeTypeShort+'.csv', mode='a', index=False, header=False)
     
    positions_summary()

    # dfi.export(posframe, 'balance.png', fontsize=8, table_conversion='matplotlib')
    # sendTelegramPhoto() 

    # inform that ended
    telegram.send_telegram_message(telegramToken, telegram.eStop, "Binance Trader Bot - End")

    # calculate execution time
    stop = timeit.default_timer()
    total_seconds = stop - start
    duration = database.duration(total_seconds)

    # days, remainder = divmod(total_seconds, 3600*24)
    # hours, remainder = divmod(remainder, 3600)
    # minutes, seconds = divmod(remainder, 60)

    # # Creating a string that displays the time in the hms format
    # time_format = ""
    # if days > 0:
    #     time_format += "{:2d}d ".format(int(days))
    # if hours > 0 or (days > 0 and (minutes > 0 or seconds > 0)):
    #     time_format += "{:2d}h ".format(int(hours))
    # if minutes > 0 or (hours > 0 and seconds > 0) or (days > 0 and seconds > 0):
    #     time_format += "{:2d}m ".format(int(minutes))
    # if seconds > 0 or (days == 0 and hours == 0 and minutes == 0):
    #     time_format += "{:2d}s".format(int(seconds))

    msg = f'Execution Time: {duration}'
    print(msg)
    telegram.send_telegram_message(telegramToken, "", msg)

if __name__ == "__main__":
    main()



