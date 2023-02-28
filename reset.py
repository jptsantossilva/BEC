
import pandas as pd
import os
import sys

res = input('This procedure will ERASE ALL DATA from positions and orders. Are you sure? [yes/no]\n')     # \n ---> newline  ---> It causes a line break

if res != "yes":
  msg = "Bye"
  sys.exit(msg)


# reset csv files

#orders
dfOrders = pd.read_csv('orders1d.csv', nrows=0)
dfOrders.to_csv('orders1d.csv', index=False)
dfOrders = pd.read_csv('orders4h.csv', nrows=0)
dfOrders.to_csv('orders4h.csv', index=False)
dfOrders = pd.read_csv('orders1h.csv', nrows=0)
dfOrders.to_csv('orders1h.csv', index=False)

# positions
dfPositions = pd.read_csv('positions1d.csv', nrows=0)
dfPositions.to_csv('positions1d.csv', index=False)
dfPositions = pd.read_csv('positions4h.csv', nrows=0)
dfPositions.to_csv('positions4h.csv', index=False)
dfPositions = pd.read_csv('positions1h.csv', nrows=0)
dfPositions.to_csv('positions1h.csv', index=False)

# add coin pair
dfAddCoinPair = pd.read_csv('addcoinpair.csv', nrows=0)
dfAddCoinPair.to_csv('addcoinpair.csv', index=False)

# coinpairBestEma - best ema values
dfAddCoinPair = pd.read_csv('coinpairBestEma.csv', nrows=0)
dfAddCoinPair.to_csv('coinpairBestEma.csv', index=False)

# blacklist - coins not to trade
dfBlacklist = pd.read_csv('blacklist.csv', nrows=0)
dfBlacklist.to_csv('blacklist.csv', index=False)

# clean log files
filename = "main.log"
if os.path.exists(filename):
  os.remove(filename)
else:
  print(f"The file {filename} does not exist")
with open(filename, 'w'):
    pass

filename = "coinpairByMarketPhase.log"
if os.path.exists(filename):
  os.remove(filename)
else:
  print(f"The file {filename} does not exist")
with open(filename, 'w'):
    pass

# run in terminal
# python3 coinpairByMarketPhase.py 1d BUSD
