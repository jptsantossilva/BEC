
import os
import sys
import pandas as pd

import utils.database as database

res = input('This procedure will ERASE ALL DATA from positions and orders. Are you sure? [yes/no]\n')     # \n ---> newline  ---> It causes a line break

if res != "yes":
  msg = "Bye"
  sys.exit(msg)

# delete orders
database.delete_all_orders()

# positions
database.delete_all_positions()

# market phase
database.delete_all_symbols_by_market_phase()

# symbols_To_Calc
database.delete_all_symbols_to_calc()

# Backtesting Results
database.delete_all_backtesting_results()

# blacklist - symbols not to trade
database.delete_all_blacklist()

# clean log files
filename = "main.log"
if os.path.exists(filename):
  os.remove(filename)
else:
  print(f"The file {filename} does not exist")
with open(filename, 'w'):
    pass

filename = "symbol_by_market_phase.log"
if os.path.exists(filename):
  os.remove(filename)
else:
  print(f"The file {filename} does not exist")
with open(filename, 'w'):
    pass

# run in terminal
# python3 symbol_by_market_phase.py 1d BUSD
