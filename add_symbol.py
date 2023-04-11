"""
Gets coin pairs not yet calculated (completed = 0)
and calculates best ema for 1d, 4h and 1h time frames and then adds 
symbol to positions table
"""

from best_ema import addcoinpair
import pandas as pd
from binance.client import Client
import os
from datetime import date
import sys
import logging
import telegram
import sqlite3
import database

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it 
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "addCoinPair.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

result_1d = False
result_4h = False
result_1h = False
timeframe = ["1d", "4h", "1h"]

def get_performance_rank(symbol):

    if symbol.endswith("BTC"):
        coin_only = symbol[:-3]
        coin_stable = symbol[-3:]
    elif symbol.endswith(("BUSD","USDT")):    
        coin_only = symbol[:-4]
        coin_stable = symbol[-4:]

    filename = f'coinpairByMarketPhase_{coin_stable}_1d.csv'
    df = pd.read_csv(filename)

    # get performance_rank value 
    if 'performance_rank' in df.columns:
        res = df.loc[df['Coinpair'] == symbol, 'performance_rank'].values
        if len(res) > 0:
            return res[0]
        else:
            return 1000
    else:
        return 1000

def main():
    # try:
    connection = database.connect()
    database.create_tables(connection)

        # filename = 'addcoinpair.csv'
        # ListAddcoinpair = pd.read_csv(filename)

    list_not_completed = pd.DataFrame(database.get_symbols_to_calc_by_calc_completed(connection, completed = 0))

    # except Exception as e:
        # msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        # print(msg)
        # logging.exception(msg)
        # telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 
        # sys.exit(msg)
        

    # list = ListNotCompleted.drop(columns = ['Completed','Date'])
    # reset the index and set number beginning from 1
    list_not_completed = list_not_completed.reset_index(drop=True)
    list_not_completed.index += 1

    if not list_not_completed.empty: # not empty 
        telegram.send_telegram_message(telegram.telegramToken_market_phases, "", "Calculating best EMA for the following coins:")
        telegram.send_telegram_message(telegram.telegramToken_market_phases, "", list.to_string(index=True, header = False)) 
    
    # insertupdate
    # calc BestEMA for each coin pair and each time frame and save on positions files
    for symbol in list_not_completed.Symbol:
        for tf in timeframe: 

            # calc BestEMA
            resultBestEma = addcoinpair(symbol, tf)
            # print("Add Coin pair - "+coinPair+" - "+tf+" - run successfully")

            # check if exists in coinpairBestEma to make sure we have stored best ema
            # try:
            #     filename = 'coinpairBestEma.csv'
            #     dfBestEMA = pd.read_csv(filename)

            # except Exception as e:
            #     msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
            #     print(msg)
            #     logging.exception(msg)
            #     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)  

            # listEMAvalues = dfBestEMA[(dfBestEMA.coinPair == coinPair) & (dfBestEMA.timeFrame == tf)]

            df_best_ema = pd.DataFrame(database.get_best_ema_by_symbol_timeframe(connection, symbol = symbol, time_frame = tf))

            # if return percentage of best ema is < 0 we dont want to trade that coin pair
            if not df_best_ema.empty:
                if int(df_best_ema.returnPerc.values[0]) < 0:
                    continue
            
            if not df_best_ema.empty:
                fastEMA = int(df_best_ema.fastEMA.values[0])
                slowEMA = int(df_best_ema.slowEMA.values[0])
            else:
                fastEMA = 0
                slowEMA = 0
                msg = "Warning: there is no line in coinpairBestEma file with coinPair "+str(symbol)+ " and timeframe "+str(tf)+". "
                print(msg)
                # telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
                continue

                
            # add to positions files
            # try:
                # filename = 'positions'+tf+'.csv'
                # positionsfile = pd.read_csv(filename)
            
                # # append if not exist 
                # linha = positionsfile.index[(positionsfile.Currency == coinPair)].to_list()

                # if not linha:
                    # print("There is no line in positions"+tf+" file with coinPair "+str(coinPair)+ " and timeframe "+tf+". New line will be added.")
                    
                    # ------------------------------------
                    # Code below was used when condition FastEMA>SlowEMA, but since we are now using crossover, 
                    # this issues does not exist anymore, and so there is no need to calculate fast and slow ema position 
                    # to check value for position, 1 or 0.  
                    # ------------------------------------
                    ## check position value.
                    ## position = 1 if fastEMA > slowEMA
                    ## position = 0 if slowEMA > fastEMA
                    # timeframeNum = int(tf[0])
                    # timeframeType = str(tf[1])

                    # df = getdata(coinPair, timeframeNum, timeframeType)
                    # applytechnicals(df, fastEMA, slowEMA)
                    # # print(df)
                    # lastrow = df.iloc[-1]

                    # accumulationPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 < lastrow.SMA200)
                    # bullishPhase = (lastrow.Close > lastrow.SMA50) and (lastrow.Close > lastrow.SMA200) and (lastrow.SMA50 > lastrow.SMA200)

                    # if (accumulationPhase or bullishPhase) and (lastrow.FastEMA > lastrow.SlowEMA):
                    #     position = 1
                    # else:
                    #     position = 0
                    # ------------------------------------

                    # position = 0
                    # rank = get_performance_rank(symbol)

                    # # if column does not exist add it as the second column
                    # if 'performance_rank' not in positionsfile.columns:
                    #     positionsfile.insert(loc=1, column="performance_rank", value=[0]*len(positionsfile))

                    #add line
                    # positionsfile.loc[len(positionsfile.index)] = [coinPair
                    #                                                 ,rank # performance rank
                    #                                                 ,position
                    #                                                 ,0 # qty
                    #                                                 ,0 # buyPrice
                    #                                                 ,0 # currentPrice
                    #                                                 ,0 # PnLperc
                    #                                                 ]
                    
                    # positionsfile.to_csv(filename, index=False)

            # except Exception as e:
            #     msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
            #     print(msg)
            #     logging.exception(msg)
            #     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)

        rank = get_performance_rank(symbol)
        database.insert_position(connection, bot = tf, symbol = symbol, rank = rank)
        
        # mark as calc completed
        # try:
            # filename = 'addcoinpair.csv'
            # completedcoinpair = pd.read_csv(filename)
            # completedcoinpair.loc[completedcoinpair.Currency == coinPair, 'Completed'] = 1
            # completedcoinpair.loc[completedcoinpair.Currency == coinPair, 'Date'] = str(date.today())        
            # completedcoinpair.Completed = completedcoinpair.Completed.astype(int, errors='ignore')
            
            # # coinpairBestEma
            # # print("Mark coin "+str(coinPair)+ " as Completed to addcoinpair file")
            # completedcoinpair.to_csv(filename, index=False, header=True)

        # except Exception as e:
        #     msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        #     print(msg)
        #     logging.exception(msg)
        #     telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        
        # mark as calc completed
        database.set_symbols_to_calc_completed(connection, symbol = symbol)
        
        #remove all coin pairs from addcoinpair file
        # dfaddcoinpair = pd.read_csv('addcoinpair.csv', nrows=0)
        # dfaddcoinpair.to_csv('addcoinpair.csv', mode='w', index=False)    

if __name__ == "__main__":
    main()