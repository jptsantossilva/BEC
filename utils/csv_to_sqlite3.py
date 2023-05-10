import sqlite3
import csv

# import utils.database as database # keep to make sure database tables are created if not exist
from utils import database
import utils.config as config

# Connect to the database
# conn = sqlite3.connect('data.db')

# Create a cursor object
# cursor = conn.cursor()

# Create a cursor object
cursor = database.conn.cursor()

timeframes = ["1d","4h","1h"]

# ORDERS
try:
    for tf in timeframes:
        # Open the CSV file
        filename = "orders"+tf+".csv" 
        with open(filename, 'r') as csv_file:
            # Create a CSV reader object
            csv_reader = csv.DictReader(csv_file)

            # Iterate over each row in the CSV file
            for row in csv_reader:
                # Insert the row into the table
                cursor.execute('INSERT INTO Orders (Exchange_Order_Id, Date, Bot, Symbol, Side, Price, Qty, Buy_Order_Id, Pnl_Perc, Pnl_Value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (row['orderId'], row['time'], tf ,row['symbol'], row['side'], row['price'], row['executedQty'], row['buyorderid'], row['pnlperc'], row['pnlusd']))
except FileNotFoundError:
    print(f"The file {filename} does not exist.")               

# POSITIONS
try:
    for tf in timeframes:
        # Open the CSV file
        filename = "positions"+tf+".csv" 
        with open(filename, 'r') as csv_file:
            # Create a CSV reader object
            csv_reader = csv.DictReader(csv_file)

            # Iterate over each row in the CSV file
            for row in csv_reader:
                # Insert the row into the table
                calc_pnl_value = (float(row['currentPrice'])*float(row['quantity']))-(float(row['buyPrice'])*float(row['quantity']))
                cursor.execute('INSERT INTO Positions (Bot, Symbol, Position, Rank, Buy_Price, Curr_Price, Qty, Pnl_Perc, Pnl_Value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (tf, row['Currency'], row['position'], row['performance_rank'], row['buyPrice'], row['currentPrice'], row['quantity'], row['PnLperc'], calc_pnl_value))
except FileNotFoundError:
    print(f"The file {filename} does not exist.")      

# BEST_EMA
# Open the CSV file
try:
    filename = "coinpairBestEma.csv"
    with open(filename, 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            cursor.execute('INSERT OR REPLACE INTO Best_Ema (Symbol, Ema_Fast, Ema_Slow, Time_Frame, Return_Perc, BuyHold_Return_Perc, Backtest_Start_Date) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (row['coinPair'], row['fastEMA'], row['slowEMA'], row['timeFrame'], row['returnPerc'], row['BuyHoldReturnPerc'], row['BacktestStartDate']))
except FileNotFoundError:
    print(f"The file {filename} does not exist.") 

# BLACKLIST
# Open the CSV file
try:
    filename = "blacklist.csv"
    with open(filename, 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            cursor.execute('INSERT INTO Blacklist (Symbol) VALUES (?)',
                (row['Currency'],))
except FileNotFoundError:
    print(f"The file {filename} does not exist.") 

# SYMBOLS_TO_CALC
try:
    filename = "addcoinpair.csv"
    with open(filename, 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            cursor.execute('INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added) VALUES (?, ?, ?)',
                (row['Currency'],row['Completed'],row['Date']))
except FileNotFoundError:
    print(f"The file {filename} does not exist.") 

# SYMBOLS_BY_MARKET_PHASE
try:
    filename = "coinpairByMarketPhase_"+config.trade_against+"_1d.csv"
    with open(filename, 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            cursor.execute('INSERT INTO Symbols_By_Market_Phase (Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200, Rank) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (row['Coinpair'], row['Close'], row['50DSMA'] ,row['200DSMA'], row['MarketPhase'], row['perc_above_50DSMA'], row['perc_above_200DSMA'], row['performance_rank']))
except FileNotFoundError:
    print(f"The file {filename} does not exist.") 


# Commit the changes to the database
database.conn.commit()

# Close the database connection
database.conn.close()