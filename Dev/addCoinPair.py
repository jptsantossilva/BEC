import BestEMA
import pandas as pd

result_1d = False
result_4h = False
result_1h = False

result_1d = BestEMA.addcoinpair("1d")
if result_1d:
    result_4h = BestEMA.addcoinpair("4h")
if result_4h:
    result_1h = BestEMA.addcoinpair("1h")
if result_1h:
    print("Add Coin pair - 1D/4h/1h time frames ran successfully")
    #remove all coin pairs from addcoinpair file
    dfaddcoinpair = pd.read_csv('addcoinpair', nrows=0)
    dfaddcoinpair.to_csv('addcoinpair', mode='w', index=False)

if not result_1d: 
    print("Add Coin pair - 1D time frame ERROR OCCURRED")
if not result_4h: 
    print("Add Coin pair - 4h time frame ERROR OCCURRED")
if not result_1h: 
    print("Add Coin pair - 1h time frame ERROR OCCURRED")