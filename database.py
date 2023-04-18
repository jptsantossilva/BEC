import sqlite3
import os
import math
from datetime import datetime
import pandas as pd
import config

def connect(path=""):
    file_path = os.path.join(path, "data.db")
    return sqlite3.connect(file_path)

connection = connect()

# change connection from PnL program
def set_connection(path):
    global connection
    connection = connect(path)

# ORDERS
create_orders_table = """
    CREATE TABLE IF NOT EXISTS Orders (
        Id INTEGER PRIMARY KEY,
        Exchange_Order_Id INTEGER,
        Date TEXT,
        Bot TEXT,
        Symbol TEXT,
        Side TEXT,
        Price REAL,
        Qty REAL,
        Ema_Fast INTEGER,
        Ema_Slow INTEGER,
        PnL_Perc REAL,
        PnL_Value REAL,
        Buy_Order_Id INTEGER,
        Exit_Reason text
    );
"""

sql_get_all_orders = "SELECT * FROM Orders;"  
def get_all_orders():
    return pd.read_sql(sql_get_all_orders, connection)

sql_get_orders_by_bot = "SELECT * FROM Orders WHERE Bot = ?;"
def get_orders_by_bot(bot):
    return pd.read_sql(sql_get_orders_by_bot, connection, params=(bot,))

sql_delete_all_orders = "DELETE FROM Orders;"
def delete_all_orders():
    with connection:
        connection.execute(sql_delete_all_orders)

sql_get_years_from_orders = """
    SELECT DISTINCT(strftime('%Y', Date)) AS Year 
    FROM Orders 
    ORDER BY Year DESC;"""
def get_years_from_orders():
    with connection:
        df = pd.read_sql(sql_get_years_from_orders, connection)
        result = []
        if not df.empty:
            result = df.Year.tolist()
        return result
    
sql_get_months_from_orders_by_year ="""
    SELECT DISTINCT(strftime('%m', Date)) AS Month 
    FROM Orders
    WHERE 
        Date LIKE ?
    ORDER BY Month DESC;"""
def get_months_from_orders_by_year(year):
    result = []

    if year == None:
        return result

    year = str(year)+"-%"
    with connection:
        df = pd.read_sql(sql_get_months_from_orders_by_year, connection, params=(year,))
        if not df.empty:
            # convert month from string to integer
            df['Month'] = df['Month'].apply(lambda x: int(x))
            result = df.Month.tolist()
        return result

sql_add_order_buy = """
    INSERT INTO Orders (
        Exchange_Order_Id,
        Date,
        Bot,
        Symbol,
        Side,
        Price,
        Qty,
        Ema_Fast,
        Ema_Slow)
    VALUES (
        ?,?,?,?,?,?,?,?,?        
        );        
"""
def add_order_buy(exchange_order_id, date, bot, symbol, price, qty, ema_fast, ema_slow):
    side = "BUY"
    with connection:
        connection.execute(sql_add_order_buy, (exchange_order_id, date, bot, symbol, side, price, qty, ema_fast, ema_slow))

sql_add_order_sell = """
    INSERT INTO Orders (
        Exchange_Order_Id,
        Date,
        Bot,
        Symbol,
        Side,
        Price,
        Qty,
        Ema_Fast,
        Ema_Slow,
        PnL_Perc,
        PnL_Value,
        Buy_Order_Id,
        Exit_Reason)
    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?        
        );        
"""
def add_order_sell(exchange_order_id, date, bot, symbol, price, qty, ema_fast, ema_slow, exit_reason):
    # calc

    df_last_buy_order = get_last_buy_order_by_bot_symbol(connection, bot, symbol)

    if df_last_buy_order.empty:
        print("DataFrame is empty")
    else:
        buy_order_id = df_last_buy_order.loc[0, 'Buy_Order_Id']
        buy_price = float(df_last_buy_order.loc[0, 'Price'])
        buy_qty = float(df_last_buy_order.loc[0, 'Qty'])

        sell_price = price
        sell_qty = qty

        pnl_perc = (((sell_price*sell_qty)-(buy_price*buy_qty))/(buy_price*buy_qty))*100
        pnl_perc = float(round(pnl_perc, 2))
        pnl_value = (sell_price*sell_qty)-(buy_price*buy_qty)
        pnl_value = float(round(pnl_value, config.n_decimals))
  
    side = "SELL"

    with connection:
        connection.execute(sql_add_order_sell, (exchange_order_id, date, bot, symbol, side, price, qty, ema_fast, ema_slow, pnl_perc, pnl_value, buy_order_id, exit_reason))
        return pnl_value, pnl_perc

sql_get_last_buy_order_by_bot_symbol = """
    SELECT * FROM Orders
    WHERE 
        Side = 'BUY' 
        AND bot = ?
        AND Symbol = ?
    ORDER BY id DESC LIMIT 1;
"""
def get_last_buy_order_by_bot_symbol(bot, symbol):
    return pd.read_sql(sql_get_last_buy_order_by_bot_symbol, connection, params=(bot, symbol,))

sql_get_orders_by_bot_side_year_month = """
    SELECT Bot,
        Date,
        Qty,
        PnL_Perc,
        PnL_Value
    FROM Orders
    WHERE
        Bot = ?
        AND Side = ?
        AND Date LIKE ?;
"""
def get_orders_by_bot_side_year_month(bot, side, year, month):
    if month == 13:
        year_month = str(year)+"-%"
    else:
        year_month = str(year)+"-"+str(month)+"-%"
    return pd.read_sql(sql_get_orders_by_bot_side_year_month, connection, params=(bot, side, year_month))

# POSITIONS
sql_create_positions_table = """
    CREATE TABLE IF NOT EXISTS Positions (
        Id INTEGER PRIMARY KEY,
        Date TEXT,
        Bot TEXT,
        Symbol TEXT,
        Position INTEGER,
        Rank INTEGER,
        Buy_Price REAL,
        Curr_Price REAL,
        Qty REAL,
        Ema_Fast INTEGER,
        Ema_Slow INTEGER,
        PnL_Perc REAL,
        PnL_Value REAL,
        Duration TEXT,
        Buy_Order_Id INTEGER
    );
"""

sql_insert_position = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank)
        VALUES (?,?,0,?);        
"""
def insert_position(bot, symbol):
    rank = get_rank_from_symbols_by_market_phase_by_symbol(symbol)
    with connection:
        connection.execute(sql_insert_position, (bot, symbol, rank))
  
sql_get_positions_by_bot_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Position = ?
"""
def get_positions_by_bot_position(bot, position):
    return pd.read_sql(sql_get_positions_by_bot_position, connection, params=(bot, position))

sql_get_unrealized_pnl_by_bot = """
    SELECT Bot, Symbol, Qty, Buy_Price, PnL_Value, PnL_Perc, Duration
    FROM Positions 
    WHERE 
        Bot = ?
        AND Position = ?
"""
def get_unrealized_pnl_by_bot(bot):
    position = 1
    return pd.read_sql(sql_get_unrealized_pnl_by_bot, connection, params=(bot, position))

sql_get_positions_by_bot_symbol_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Symbol = ?
        AND Position = ?
"""
def get_positions_by_bot_symbol_position(bot, symbol, position):
    return pd.read_sql(sql_get_positions_by_bot_symbol_position, connection, params=(bot, symbol, position))

sql_get_all_positions_by_bot_symbol = """
    SELECT COUNT(*)
    FROM Positions 
    WHERE 
        Bot = ?
        AND symbol = ?
"""
def get_all_positions_by_bot_symbol(bot, symbol):
    df = pd.read_sql(sql_get_all_positions_by_bot_symbol, connection, params=(bot, symbol,))
    result = int(df.iloc[0, 0]) == 1
    return result
    
    
sql_get_distinct_symbol_from_positions_where_position1 = """
    SELECT DISTINCT(symbol)
    FROM Positions 
    WHERE 
        Position = 1
"""
def get_distinct_symbol_from_positions_where_position1():
    return pd.read_sql(sql_get_distinct_symbol_from_positions_where_position1, connection)
    
sql_get_all_positions_by_bot = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
    ORDER BY
        Rank
"""
def get_all_positions_by_bot(bot):
    return pd.read_sql(sql_get_all_positions_by_bot, connection, params=(bot,))
    
sql_get_num_open_positions = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1;
"""
def get_num_open_positions(): 
    df = pd.read_sql(sql_get_num_open_positions, connection)
    result = int(df.iloc[0, 0])
    return result
    
sql_get_num_open_positions_by_bot = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1 and Bot = ?;
"""
def get_num_open_positions_by_bot(bot): 
    df = pd.read_sql(sql_get_num_open_positions_by_bot, connection, params=(bot,))
    result = int(df.iloc[0, 0])
    return result

#   
sql_add_top_rank_to_position = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank, Ema_Fast, Ema_Slow)
    SELECT be.Time_Frame, mp.Symbol, 0, mp.Rank, be.Ema_Fast, be.Ema_Slow
    FROM 
        Symbols_By_Market_Phase mp
        INNER JOIN Best_Ema be ON mp.Symbol = be.Symbol
    WHERE   
        be.Return_Perc > 0
        AND NOT EXISTS (
            SELECT 1 
            FROM Positions 
            WHERE Bot = be.Time_Frame AND Symbol = mp.Symbol
        );
"""
def add_top_rank_to_position():
    with connection:
        connection.execute(sql_add_top_rank_to_position)

sql_set_rank_from_positions = """
    UPDATE Positions
    SET
        Rank = ?
    WHERE 
        Symbol = ?
"""
def set_rank_from_positions(symbol, rank):
    with connection:
        connection.execute(sql_set_rank_from_positions, (rank, symbol,))


sql_update_position_pnl = """
    UPDATE Positions
    SET 
        Curr_Price = ?,
        PnL_Perc = ?,
        PnL_Value = ?,
        Duration = ?,
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def update_position_pnl (bot, symbol, curr_price):
    items = get_positions_by_bot_symbol_position(bot, symbol, position=1)
    buy_price = float(items[5])
    buy_price = float(items['Buy_Price'])
    qty = float(items[7])
    date = str(items[2])

    if not math.isnan(buy_price) and (buy_price > 0):
        pnl_perc = ((curr_price - buy_price)/buy_price)*100
        pnl_perc = float(round(pnl_perc,2))

        pnl_value = (curr_price*qty)-(buy_price*qty)
        pnl_value = float(round(pnl_value, config.n_decimals))
        
        # duration
        datetime_now = datetime.now()
        datetime_open_position = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        diff_seconds = (datetime_now - datetime_open_position).total_seconds()
        duration = str(duration(diff_seconds))

    with connection:
        connection.execute(sql_update_position_pnl, (curr_price, pnl_perc, pnl_value, duration, bot, symbol))

sql_set_position_buy = """
    UPDATE Positions
    SET 
        Position = 1,
        Qty = ?,
        Buy_Price = ?,
        Curr_Price = ?,
        Date = ?
        Buy_Order_Id = ?
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_buy(bot, symbol, qty, buy_price, date, buy_order_id):
    curr_price = float(buy_price)    
    with connection:
        connection.execute(sql_set_position_buy, (qty, buy_price, curr_price, date, buy_order_id, bot, symbol))

sql_set_position_sell = """
    UPDATE Positions
    SET 
        Position = 0,
        Qty = 0,
        Buy_Price = 0,
        Curr_Price = 0,
        PnL_Perc = 0,
        PnL_Value = 0,
        Duration = 0
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_sell(bot, symbol):
    with connection:
        connection.execute(sql_set_position_sell, (bot, symbol))

sql_delete_all_positions = "DELETE FROM Positions;"
def delete_all_positions():
    with connection:
        connection.execute(sql_delete_all_positions)

sql_delete_positions_not_top_rank = "DELETE FROM Positions where Position = 0 and Symbol not in (select Symbol from Symbols_By_Market_Phase);"
def delete_positions_not_top_rank():
    with connection:
        connection.execute(sql_delete_positions_not_top_rank)
    
sql_delete_all_positions_not_open = "DELETE FROM Positions where Position = 0"
def delete_all_positions_not_open():
    with connection:
        connection.execute(sql_delete_all_positions_not_open)

# BLACKLIST
sql_create_blacklist_table = """
    CREATE TABLE IF NOT EXISTS Blacklist (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT
    );
"""

sql_get_all_blacklist = "SELECT * FROM Blacklist;"
def get_all_blacklist():
    return pd.read_sql(sql_get_all_blacklist, connection)

sql_delete_all_blacklist = "DELETE FROM Blacklist;"
def delete_all_blacklist():
    with connection:
        connection.execute(sql_delete_all_blacklist)
    
# BEST_EMA
sql_create_best_ema_table = """
    CREATE TABLE IF NOT EXISTS Best_Ema (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Ema_Fast INTEGER,
        Ema_Slow INTEGER,
        Time_Frame TEXT,
        Return_Perc REAL,
        BuyHold_Return_Perc REAL,
        Backtest_Start_Date TEXT,
        CONSTRAINT symbol_time_frame_unique UNIQUE (Symbol, Time_Frame)
    );
"""

sql_get_all_best_ema = "SELECT * FROM Best_Ema;"
def get_all_best_ema():
    return pd.read_sql(sql_get_all_best_ema, connection)
    
sql_get_best_ema_by_symbol_timeframe = """
    SELECT * 
    FROM Best_Ema
    WHERE
        Symbol = ?
        AND Time_Frame = ?;
"""
def get_best_ema_by_symbol_timeframe(symbol, time_frame):
    return pd.read_sql(sql_get_best_ema_by_symbol_timeframe, connection, params=(symbol, time_frame))

sql_add_best_ema = """
    INSERT OR REPLACE INTO Best_Ema (
        Symbol, Ema_Fast, Ema_Slow, Time_Frame, Return_Perc, BuyHold_Return_Perc, Backtest_Start_Date
        ) 
        VALUES (?, ?, ? ,? ,? ,? ,?);
"""
def add_best_ema(timeframe, symbol, ema_fast, ema_slow, return_perc, buy_hold_return_perc, backtest_start_date):
    with connection:
        connection.execute(sql_add_best_ema, (symbol, int(ema_fast), int(ema_slow), timeframe, return_perc, buy_hold_return_perc, backtest_start_date))
    
sql_delete_all_best_ema = "DELETE FROM Best_Ema;"
def delete_all_best_ema():
    with connection:
        connection.execute(sql_delete_all_best_ema)

# SYMBOLS_TO_CALC
sql_create_symbols_to_calc_table = """
    CREATE TABLE IF NOT EXISTS Symbols_To_Calc (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Calc_Completed INTEGER,
        Date_Added TEXT,
        Date_Completed TEXT
    );
"""

#
sql_get_all_symbols_to_calc = "SELECT * FROM Symbols_To_Calc;"
def get_all_symbols_to_calc():
    return pd.read_sql(sql_get_all_symbols_to_calc, connection)

#    
sql_get_symbols_to_calc_by_calc_completed = """
    SELECT Symbol 
    FROM Symbols_To_Calc 
    WHERE
        Calc_Completed = ?;
"""
def get_symbols_to_calc_by_calc_completed(completed):
    return pd.read_sql(sql_get_symbols_to_calc_by_calc_completed, connection, params=(completed,))
    
#    
sql_set_symbols_to_calc_completed = """
    UPDATE Symbols_To_Calc 
    SET Calc_Completed = 1,
        Date_Completed = datetime('now')
    WHERE
        Symbol = ?;
"""
def set_symbols_to_calc_completed(symbol):
    with connection:
        connection.execute(sql_set_symbols_to_calc_completed, (symbol,))
    
sql_delete_symbols_to_calc_completed = """
    DELETE FROM Symbols_To_Calc 
    WHERE Calc_Completed = 1;
"""
def delete_symbols_to_calc_completed():
    with connection:
        connection.execute(sql_delete_symbols_to_calc_completed)

sql_delete_all_symbols_to_calc = "DELETE FROM Symbols_To_Calc;"
def delete_all_symbols_to_calc():
    with connection:
        connection.execute(sql_delete_all_symbols_to_calc)
    
# add to calc the symbols with open positions 
sql_add_symbols_with_open_positions_to_calc = """
INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added)
SELECT DISTINCT Symbol, 0, datetime('now')
FROM Positions 
WHERE Position = 1
    AND Symbol NOT IN (SELECT Symbol FROM Symbols_To_Calc WHERE Calc_Completed = 0)
"""
def add_symbols_with_open_positions_to_calc():
    with connection:
        connection.execute(sql_add_symbols_with_open_positions_to_calc)
    
# add to calc the symbols in top rank
sql_add_symbols_top_rank_to_calc = """
INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added)
SELECT DISTINCT Symbol, 0, datetime('now')
FROM Symbols_By_Market_Phase 
WHERE Symbol NOT IN (SELECT Symbol FROM Symbols_To_Calc WHERE Calc_Completed = 0)
"""
def add_symbols_top_rank_to_calc():
    with connection:
        connection.execute(sql_add_symbols_top_rank_to_calc)
    
# Symbols_By_Market_Phase
sql_create_symbols_by_market_phase_table = """
    CREATE TABLE IF NOT EXISTS Symbols_By_Market_Phase (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Price REAL,
        DSMA50 REAL,
        DSMA200 REAL,
        Market_Phase TEXT,
        Perc_Above_DSMA50 REAL,
        Perc_Above_DSMA200 REAL,
        Rank INTEGER
    );
"""

sql_get_all_symbols_by_market_phase = "SELECT * FROM Symbols_By_Market_Phase;"
def get_all_symbols_by_market_phase():
    return pd.read_sql(sql_get_all_symbols_by_market_phase, connection)
    
sql_get_symbols_from_symbols_by_market_phase = "SELECT symbol FROM Symbols_By_Market_Phase;"
def get_symbols_from_symbols_by_market_phase():
    return pd.read_sql(sql_get_symbols_from_symbols_by_market_phase, connection)

sql_get_rank_from_symbols_by_market_phase_by_symbol = """
    SELECT Rank 
    FROM Symbols_By_Market_Phase
    WHERE Symbol = ?
    ;
"""
def get_rank_from_symbols_by_market_phase_by_symbol(symbol):
    df = pd.read_sql(sql_get_rank_from_symbols_by_market_phase_by_symbol, connection, params=(symbol,))
    if df.empty:
        result = 1000
    else:
        result = int(df.iloc[0, 0])
    return result
    
sql_insert_symbols_by_market_phase = """
    INSERT INTO Symbols_By_Market_Phase (
        Symbol,
        Price,
        DSMA50,
        DSMA200,
        Market_Phase,
        Perc_Above_DSMA50,
        Perc_Above_DSMA200,
        Rank)
    VALUES(?,?,?,?,?,?,?,?);
"""
def insert_symbols_by_market_phase(symbol, price, dsma50, dsma200, market_phase, perc_above_dsma50, perc_above_dsma200, rank):
    with connection:
        connection.execute(sql_insert_symbols_by_market_phase,(symbol, price, dsma50, dsma200, market_phase, perc_above_dsma50, perc_above_dsma200, rank))
    
sql_delete_all_symbols_by_market_phase = "DELETE FROM Symbols_By_Market_Phase;"
def delete_all_symbols_by_market_phase():
    with connection:
        connection.execute(sql_delete_all_symbols_by_market_phase)

sql_get_distinct_symbol_by_market_phase_and_positions = """  
SELECT DISTINCT symbol FROM (
    SELECT symbol FROM Symbols_By_Market_Phase
    UNION
    SELECT symbol FROM Positions WHERE Position=1
) AS symbols;
"""
def get_distinct_symbol_by_market_phase_and_positions():
    return pd.read_sql(sql_get_distinct_symbol_by_market_phase_and_positions, connection)
    
# create tables
def create_tables(connection):
    with connection:
        connection.execute(create_orders_table)
        connection.execute(sql_create_positions_table)
        connection.execute(sql_create_blacklist_table)
        connection.execute(sql_create_best_ema_table)
        connection.execute(sql_create_symbols_to_calc_table)
        connection.execute(sql_create_symbols_by_market_phase_table)
    
# convert 123456 seconds to 1d 2h 3m 4s format    
def duration(seconds):
    days, remainder = divmod(seconds, 3600*24)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Creating a string that displays the time in the hms format
    time_format = ""
    if days > 0:
        time_format += "{:2d}d ".format(int(days))
    if hours > 0 or (days > 0 and (minutes > 0 or seconds > 0)):
        time_format += "{:2d}h ".format(int(hours))
    if minutes > 0 or (hours > 0 and seconds > 0) or (days > 0 and seconds > 0):
        time_format += "{:2d}m ".format(int(minutes))
    if seconds > 0 or (days == 0 and hours == 0 and minutes == 0):
        time_format += "{:2d}s".format(int(seconds))

    # msg = f'Execution Time: {time_format}'
    # print(msg)

    return time_format

##############################

create_tables(connection)


    




        