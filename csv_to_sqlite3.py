import sqlite3
import csv
import database

# Connect to the database
conn = sqlite3.connect('data.db')

# Create a cursor object
cursor = conn.cursor()

# ORDERS
timeframes = ["1d","4h","1h"]
for tf in timeframes:
    # Open the CSV file
    with open("orders"+tf+".csv", 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            cursor.execute('INSERT INTO Orders (Exchange_Order_Id, Date, Bot, Symbol, Side, Price, Qty, Buy_Order_Id, Pnl_Perc, Pnl_Value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['orderId'], row['time'], tf ,row['symbol'], row['side'], row['price'], row['executedQty'], row['buyorderid'], row['pnlperc'], row['pnlusd']))

# POSITIONS
timeframes = ["1d","4h","1h"]
for tf in timeframes:
    # Open the CSV file
    with open("positions"+tf+".csv", 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Insert the row into the table
            calc_pnl_value = (float(row['currentPrice'])*float(row['quantity']))-(float(row['buyPrice'])*float(row['quantity']))
            cursor.execute('INSERT INTO Positions (Bot, Symbol, Position, Rank, Buy_Price, Curr_Price, Qty, Pnl_Perc, Pnl_Value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (tf, row['Currency'], row['position'], row['performance_rank'], row['buyPrice'], row['currentPrice'], row['quantity'], row['PnLperc'], calc_pnl_value))

# BEST_EMA
# Open the CSV file
with open("coinpairBestEma.csv", 'r') as csv_file:
    # Create a CSV reader object
    csv_reader = csv.DictReader(csv_file)

    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Insert the row into the table
        cursor.execute('INSERT OR REPLACE INTO Best_Ema (Symbol, Ema_Fast, Ema_Slow, Time_Frame, Return_Perc, BuyHold_Return_Perc, Backtest_Start_Date) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (row['coinPair'], row['fastEMA'], row['slowEMA'], row['timeFrame'], row['returnPerc'], row['BuyHoldReturnPerc'], row['BacktestStartDate']))

# BLACKLIST
# Open the CSV file
with open("blacklist.csv", 'r') as csv_file:
    # Create a CSV reader object
    csv_reader = csv.DictReader(csv_file)

    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Insert the row into the table
        cursor.execute('INSERT INTO Blacklist (Symbol) VALUES (?)',
            (row['Currency'],))

# SYMBOLS_TO_CALC
# Open the CSV file
with open("addcoinpair.csv", 'r') as csv_file:
    # Create a CSV reader object
    csv_reader = csv.DictReader(csv_file)

    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Insert the row into the table
        cursor.execute('INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added) VALUES (?, ?, ?)',
            (row['Currency'],row['Completed'],row['Date']))
        
        # Currency,Completed,Date
        
# Commit the changes to the database
conn.commit()

# Close the database connection
conn.close()