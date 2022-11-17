import BestEMA
import Main
import pandas as pd

result_1d = False
result_4h = False
result_1h = False
timeframe = ["1d", "4h", "1h"]

result_1d = BestEMA.addcoinpair("1d")
if result_1d:
    result_4h = BestEMA.addcoinpair("4h")
if result_4h:
    result_1h = BestEMA.addcoinpair("1h")
if result_1h:
    print("Add Coin pair - 1D/4h/1h time frames ran successfully")

    Listcoinpair = pd.read_csv('addcoinpair')
    Listcoinpair = Listcoinpair.Currency
    # for each coin pair and each time frame...
    for coinPair in Listcoinpair:
        for tf in timeframe: 
            #check if exists in coinpairBestEma to make sure we have stored best ema
            coinpairBestEma = pd.read_csv('coinpairBestEma')
            linha = coinpairBestEma.index[(coinpairBestEma.coinPair == coinPair) & (coinpairBestEma.timeFrame == timeframe)].to_list()
            # if no line found we are not adding to position file and coin pair will not be traded on the selected timeframe
            if not linha:
                print("Warning: there is no line in coinpairBestEma file with coinPair "+str(coinPair)+ " and timeframe "+str(timeframe)+". ")
                continue

            # add to positions files
            positionsfile = pd.read_csv('positions'+tf)
            # if exist then update else append
            linha = positionsfile.index[(positionsfile.coinPair == coinPair) & (positionsfile.timeFrame == tf)].to_list()

            if not linha:
                print("There is no line in positions"+tf+" file with coinPair "+str(coinPair)+ " and timeframe "+tf+". New line will be added.")
                
                # check position value.
                # position = 1 if fastEMA > slowEMA
                # position = 0 if slowEMA > fastEMA
                df = Main.getdata(coinPair)
                if df.empty:
                    print(f'{coinPair} - Best EMA values missing')
                    # sendTelegramMessage(eWarning,f'{coinPair} - {gStrategyName} - Best EMA values missing')
                    continue

                Main.applytechnicals(df, coinPair)
                lastrow = df.iloc[-1]
                position = lastrow.FastMA > lastrow.slowMA

                
                #add line
                positionsfile.loc[len(positionsfile.index)] = [coinPair, 
                                                            1, #position
                                                            0, #qty
                                                            ]
            else:
                print("linha=",linha[0])
                # update linha
                #check position value.
                # !!!!!!!
                positionsfile.loc[linha[0],['position','quantity']] = [0, 0]

    #remove all coin pairs from addcoinpair file
    dfaddcoinpair = pd.read_csv('addcoinpair', nrows=0)
    dfaddcoinpair.to_csv('addcoinpair', mode='w', index=False)

if not result_1d: 
    print("Add Coin pair - 1D time frame ERROR OCCURRED")
if not result_4h: 
    print("Add Coin pair - 4h time frame ERROR OCCURRED")
if not result_1h: 
    print("Add Coin pair - 1h time frame ERROR OCCURRED")