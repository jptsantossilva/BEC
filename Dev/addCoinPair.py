"""
Gets coin pairs from addcoinpair.csv not yet calculated (completed = 0)
and calculates best ema for 1d, 4h and 1h time frames and then adds 
coinpair to positons files positions1d.csv, positions4h.csv, positions1h.csv
"""

import BestEMA
import pandas as pd
from binance.client import Client
import os
from datetime import date
import sys
import logging
import telegram

# log file to store error messages
log_filename = "addCoinPair.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')

result_1d = False
result_4h = False
result_1h = False
timeframe = ["1d", "4h", "1h"]

# def getdata(coinPair, aTimeframeNum, aTimeframeTypeShort, aSlowSMA=200):

#     lTimeFrame = str(aTimeframeNum)+aTimeframeTypeShort
#     if aTimeframeTypeShort == "h":
#         lTimeframeTypeLong = "hour"
#     elif aTimeframeTypeShort == "d":
#         lTimeframeTypeLong = "day"
    
#     # gStrategyName = str(aFastMA)+"/"+str(aSlowMA)+" EMA cross"

#     # if bestEMA does not exist return empty dataframe in order to no use that trading pair
#     # if aFastMA == 0:
#     #     frame = pd.DataFrame()
#     #     return frame
    
#     # if best Ema exist get price data 
#     lstartDate = str(aSlowSMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
#     ltimeframe = str(aTimeframeNum)+aTimeframeTypeShort
#     frame = pd.DataFrame(BestEMA.client.get_historical_klines(coinPair,
#                                                     ltimeframe,
#                                                     lstartDate))

#     frame = frame[[0,4]]
#     frame.columns = ['Time','Close']
#     frame.Close = frame.Close.astype(float)
#     frame.Time = pd.to_datetime(frame.Time, unit='ms')
#     return frame

# %%
# def applytechnicals(df, aFastMA, aSlowMA):
    
#     if aFastMA > 0: 
#         df['FastEMA'] = df['Close'].ewm(span=aFastMA, adjust=False).mean()
#         df['SlowEMA'] = df['Close'].ewm(span=aSlowMA, adjust=False).mean()
#         df['SMA50']  = df['Close'].rolling(50).mean()
#         df['SMA200'] = df['Close'].rolling(200).mean()

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

    try:
        filename = 'addcoinpair.csv'
        ListAddcoinpair = pd.read_csv(filename)
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
        print(msg)
        logging.exception(msg)
        telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg) 
        sys.exit(msg)
        

    # get coin pairs with no completed calculation
    ListNotCompleted = ListAddcoinpair[(ListAddcoinpair.Completed != 1)]
    # Listcoinpair

    list = ListNotCompleted.drop(columns = ['Completed','Date'])

    if not list.empty: # not empty 
        telegram.send_telegram_message(telegram.telegramToken_market_phases, "", "Calculating best EMA for the following coins:")
        telegram.send_telegram_message(telegram.telegramToken_market_phases, "", list.to_string(index=False, header = False)) 
    
    # insertupdate
    # calc BestEMA for each coin pair and each time frame and save on positions files
    for coinPair in ListNotCompleted.Currency:
        for tf in timeframe: 

            # calc BestEMA
            resultBestEma = BestEMA.addcoinpair(coinPair, tf)
            print("Add Coin pair - "+coinPair+" - "+tf+" - run successfully")

            # check if exists in coinpairBestEma to make sure we have stored best ema
            try:
                filename = 'coinpairBestEma.csv'
                dfBestEMA = pd.read_csv(filename)
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
                print(msg)
                logging.exception(msg)
                telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)  

            listEMAvalues = dfBestEMA[(dfBestEMA.coinPair == coinPair) & (dfBestEMA.timeFrame == tf)]

            # if return percentage of best ema is < 0 we dont want to trade that coin pair
            if not listEMAvalues.empty:
                if int(listEMAvalues.returnPerc.values[0]) < 0:
                    continue
            
            if not listEMAvalues.empty:
                fastEMA = int(listEMAvalues.fastEMA.values[0])
                slowEMA = int(listEMAvalues.slowEMA.values[0])
            else:
                fastEMA = 0
                slowEMA = 0
                msg = "Warning: there is no line in coinpairBestEma file with coinPair "+str(coinPair)+ " and timeframe "+str(tf)+". "
                print(msg)
                # telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
                continue

                
            # add to positions files
            try:
                filename = 'positions'+tf+'.csv'
                positionsfile = pd.read_csv(filename)
            

                # append if not exist 
                linha = positionsfile.index[(positionsfile.Currency == coinPair)].to_list()

                if not linha:
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

                    position = 0
                    rank = get_performance_rank(coinPair)

                    # if column does not exist add it as the second column
                    if 'performance_rank' not in positionsfile.columns:
                        positionsfile.insert(loc=1, column="performance_rank", value=[0]*len(positionsfile))

                    #add line
                    positionsfile.loc[len(positionsfile.index)] = [coinPair
                                                                    ,rank # performance rank
                                                                    ,position
                                                                    ,0 # qty
                                                                    ,0 # buyPrice
                                                                    ,0 # currentPrice
                                                                    ,0 # PnLperc
                                                                    ]
                    
                    positionsfile.to_csv(filename, index=False)
            except Exception as e:
                msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
                print(msg)
                logging.exception(msg)
                telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)

        # mark as calc completed
        try:
            filename = 'addcoinpair.csv'
            completedcoinpair = pd.read_csv(filename)
            completedcoinpair.loc[completedcoinpair.Currency == coinPair, 'Completed'] = 1
            completedcoinpair.loc[completedcoinpair.Currency == coinPair, 'Date'] = str(date.today())        
            completedcoinpair.Completed = completedcoinpair.Completed.astype(int, errors='ignore')
            
            # coinpairBestEma
            print("Mark coin "+str(coinPair)+ " as Completed to addcoinpair file")
            completedcoinpair.to_csv(filename, index=False, header=True)

        except Exception as e:
            msg = sys._getframe(  ).f_code.co_name+f" - {filename} - " + repr(e)
            print(msg)
            logging.exception(msg)
            telegram.send_telegram_message(telegram.telegramToken_market_phases, telegram.eWarning, msg)
        
        #remove all coin pairs from addcoinpair file
        # dfaddcoinpair = pd.read_csv('addcoinpair.csv', nrows=0)
        # dfaddcoinpair.to_csv('addcoinpair.csv', mode='w', index=False)    

if __name__ == "__main__":
    main()