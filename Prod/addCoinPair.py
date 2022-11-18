import BestEMA
import pandas as pd
from binance.client import Client
import os

result_1d = False
result_4h = False
result_1h = False
timeframe = ["1d", "4h", "1h"]

def getdata(coinPair, aTimeframeNum, aTimeframeTypeShort, aFastMA=0, aSlowMA=0):

    lTimeFrame = str(aTimeframeNum)+aTimeframeTypeShort
    if aTimeframeTypeShort == "h":
        lTimeframeTypeLong = "hour"
    elif aTimeframeTypeShort == "d":
        lTimeframeTypeLong = "day"
    
    gStrategyName = str(aFastMA)+"/"+str(aSlowMA)+" EMA cross"

    # if bestEMA does not exist return empty dataframe in order to no use that trading pair
    if aFastMA == 0:
        frame = pd.DataFrame()
        return frame
    
    # if best Ema exist get price data 
    lstartDate = str(1+aSlowMA*aTimeframeNum)+" "+lTimeframeTypeLong+" ago UTC" 
    ltimeframe = str(aTimeframeNum)+aTimeframeTypeShort
    frame = pd.DataFrame(BestEMA.client.get_historical_klines(coinPair,
                                                    ltimeframe,
                                                    lstartDate))

    frame = frame[[0,4]]
    frame.columns = ['Time','Close']
    frame.Close = frame.Close.astype(float)
    frame.Time = pd.to_datetime(frame.Time, unit='ms')
    return frame

# %%
def applytechnicals(df, aFastMA, aSlowMA):
    
    if aFastMA > 0: 
        df['FastMA'] = df['Close'].ewm(span=aFastMA, adjust=False).mean()
        df['SlowMA'] = df['Close'].ewm(span=aSlowMA, adjust=False).mean()

def main():

    ListAddcoinpair = pd.read_csv('addcoinpair')
    # get coin pairs with no completed calculation
    ListNotCompleted = ListAddcoinpair[(ListAddcoinpair.Completed != 1)]
    # Listcoinpair
    
    # insertupdate
    # calc BestEMA for each coin pair and each time frame and save on positions files
    for coinPair in ListNotCompleted.Currency:
        for tf in timeframe: 

            resultBestEma = BestEMA.addcoinpair(coinPair, tf)
            print("Add Coin pair - "+coinPair+" - "+tf+" - run successfully")

            #check if exists in coinpairBestEma to make sure we have stored best ema
            dfBestEMA = pd.read_csv('coinpairBestEma')
            listEMAvalues = dfBestEMA[(dfBestEMA.coinPair == coinPair) & (dfBestEMA.timeFrame == tf)]
            
            if not listEMAvalues.empty:
                fastMA = int(listEMAvalues.fastEMA.values[0])
                slowMA = int(listEMAvalues.slowEMA.values[0])
            else:
                fastMA = 0
                slowMA = 0
                print("Warning: there is no line in coinpairBestEma file with coinPair "+str(coinPair)+ " and timeframe "+str(tf)+". ")
                continue

                
            # add to positions files
            positionsfile = pd.read_csv('positions'+tf)
            # append if not exist 
            linha = positionsfile.index[(positionsfile.Currency == coinPair)].to_list()

            if not linha:
                # print("There is no line in positions"+tf+" file with coinPair "+str(coinPair)+ " and timeframe "+tf+". New line will be added.")
                
                # check position value.
                # position = 1 if fastEMA > slowEMA
                # position = 0 if slowEMA > fastEMA
                timeframeNum = int(tf[0])
                timeframeType = str(tf[1])
                df = getdata(coinPair, timeframeNum, timeframeType, fastMA, slowMA)
                applytechnicals(df, fastMA, slowMA)
                lastrow = df.iloc[-1]
                if lastrow.FastMA > lastrow.SlowMA:
                    position = 1
                else:
                    position = 0

                #add line
                positionsfile.loc[len(positionsfile.index)] = [coinPair 
                                                                ,position
                                                                ,0 #qty
                                                                ]
            
                positionsfile.to_csv('positions'+tf, index=False)

        # mark as calc completed
        completedcoinpair = pd.read_csv('addcoinpair')
        completedcoinpair.loc[completedcoinpair.Currency == coinPair, 'Completed'] = 1
        completedcoinpair.Completed = completedcoinpair.Completed.astype(int, errors='ignore')
        
        # coinpairBestEma
        print("Mark coin "+str(coinPair)+ " as Completed to addcoinpair file")
        completedcoinpair.to_csv('addcoinpair', index=False, header=True)
        
        #remove all coin pairs from addcoinpair file
        # dfaddcoinpair = pd.read_csv('addcoinpair', nrows=0)
        # dfaddcoinpair.to_csv('addcoinpair', mode='w', index=False)    

if __name__ == "__main__":
    main()