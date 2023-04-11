import sqlite3
import millify
import math
# from main import n_decimals
from datetime import datetime
import pandas as pd
import config

def connect():
    return sqlite3.connect("data.db")

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
        Pnl_Perc REAL,
        Pnl_Value REAL,
        Buy_Order_Id INTEGER,
        Exit_Reason text
    );
"""

get_all_orders = "SELECT * FROM Orders;"  
def get_all_orders(connection):
    with connection:
        return connection.execute(get_all_orders).fetchall()

get_orders_by_bot = "SELECT * FROM Orders WHERE Bot = ?;"
def get_orders_by_bot(connection, bot):
    with connection:
        return connection.execute(get_orders_by_bot, (bot)).fetchall()

add_order_buy = """
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
def add_order_buy(connection, exchange_order_id, date, bot, symbol, price, qty, ema_fast, ema_slow):
    side = "BUY"
    with connection:
        connection.execute(add_order_buy, (exchange_order_id, date, bot, symbol, side, price, qty, ema_fast, ema_slow))

add_order_sell = """
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
        Pnl_Perc,
        Pnl_Value,
        Buy_Order_Id,
        Exit_Reason)
    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?        
        );        
"""
def add_order_sell(connection, exchange_order_id, date, bot, symbol, price, qty, ema_fast, ema_slow, exit_reason):
    # calc

    df_last_buy_order = pd.DataFrame(get_last_buy_order_by_bot_symbol(connection, bot = bot, symbol = symbol))

    if df_last_buy_order.empty:
        print("DataFrame is empty")
    else:
        buy_order_id = df_last_buy_order.loc[0, 'Buy_Order_Id']
        buy_price = df_last_buy_order.loc[0, 'Price']
        buy_qty = df_last_buy_order.loc[0, 'Qty']

        sell_price = price
        sell_qty = qty

        pnl_perc = (((sell_price*sell_qty)-(buy_price*buy_qty))/(buy_price*buy_qty))*100
        pnl_perc = round(pnl_perc, 2)
        pnl_value = (sell_price*sell_qty)-(buy_price*buy_qty)
        pnl_value = round(pnl_value, n_decimals)
  
    side = "SELL"

    with connection:
        connection.execute(add_order_sell, (exchange_order_id, date, bot, symbol, side, price, qty, ema_fast, ema_slow, pnl_perc, pnl_value, buy_order_id, exit_reason))
        return pnl_value, pnl_perc

get_last_buy_order_by_bot_symbol = """
    SELECT * FROM Orders
    WHERE 
        Side = 'BUY' 
        AND bot = ?
        AND Symbol = ?
    ORDER BY id DESC LIMIT 1;
"""
def get_last_buy_order_by_bot_symbol(connection, bot, symbol):
    with connection:
        return connection.execute(get_last_buy_order_by_bot_symbol, (bot, symbol)).fetchone()


# POSITIONS
create_positions_table = """
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
        Pnl_Perc REAL,
        Pnl_Value REAL,
        Duration TEXT,
        Buy_Order_Id INTEGER
    );
"""

insert_position = """
    IF NOT EXISTS (SELECT * FROM Positions WHERE Bot = ? and Symbol = ?)
    BEGIN
        INSERT INTO Positions (Bot, Symbol, Position, Rank)
        VALUES (?,?,?,?)
    END;        
"""
def insert_position(connection, bot, symbol, rank):
    position = get_last_buy_order_by_bot_symbol(connection, bot, symbol)
    with connection:
        connection.execute(insert_position, (bot, symbol, position, rank))


get_position_by_bot_symbol_position1 = """
    SELECT * 
    FROM Positions 
    WHERE 
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1
    LIMIT 1
"""
def get_position_by_bot_symbol_position1(connection, bot, symbol):
    with connection:
        return connection.execute(get_position_by_bot_symbol_position1,(bot, symbol)).fetchone()
    
get_all_positions_by_bot_position1 = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Position = 1
"""
def get_all_positions_by_bot_position1(connection, bot):
    with connection:
        return connection.execute(get_all_positions_by_bot_position1,(bot)).fetchall()
    
get_all_positions_by_bot = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
    ORDER BY
        Rank
"""
def get_all_positions_by_bot(connection, bot):
    with connection:
        return connection.execute(get_all_positions_by_bot,(bot)).fetchall()
    
get_num_open_positions = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1;
"""
def get_num_open_positions(connection): 
    with connection:
        return connection.execute(get_num_open_positions(connection)).fetchone()
    
get_num_open_positions_by_bot = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1 and Bot = ?;
"""
def get_num_open_positions(connection, bot): 
    with connection:
        return connection.execute(get_num_open_positions(connection, bot)).fetchone()

#   
add_top_rank_to_position = """
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
def add_top_rank_to_position(connection):
    with connection:
        connection.execute(add_top_rank_to_position)


update_position_pnl = """
    UPDATE Positions
    SET 
        Curr_Price = ?,
        Pnl_Perc = ?,
        Pnl_Value = ?,
        Duration = ?,
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def update_position_pnl (connection, bot, symbol, curr_price):
    items = get_position_by_bot_symbol_position1(connection, bot, symbol)
    buy_price = items[5]
    qty = items[7]
    date = items[2]

    if not math.isnan(buy_price) and (buy_price > 0):
        pnl_perc = ((curr_price - buy_price)/buy_price)*100
        pnl_perc = round(pnl_perc,2)

        pnl_value = (curr_price*qty)-(buy_price*qty)
        pnl_value = round(pnl_value, n_decimals)
        
        # duration
        datetime_now = datetime.now()
        datetime_open_position = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        diff_seconds = (datetime_now - datetime_open_position).total_seconds()
        duration = duration(diff_seconds)

    with connection:
        connection.execute(update_position_pnl, (curr_price, pnl_perc, pnl_value, duration, bot, symbol))

set_position_buy = """
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
def set_position_buy(connection, bot, symbol, qty, buy_price, date, buy_order_id):
    curr_price = buy_price    
    with connection:
        connection.execute(set_position_buy, (qty, buy_price, curr_price, date, buy_order_id, bot, symbol))

set_position_sell = """
    UPDATE Positions
    SET 
        Position = 0,
        Qty = 0,
        Buy_Price = 0,
        Curr_Price = 0,
        Pnl_Perc = 0,
        Pnl_Value = 0,
        Duration = 0
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_sell(connection, bot, symbol):
    with connection:
        connection.execute(set_position_sell, (bot, symbol))

delete_positions_not_top_rank = "DELETE FROM Positions where Position = 0 and Symbols not in (select Symbol from Symbols_By_Market_Phase);"
def delete_positions_not_top_rank(connection):
    with connection:
        return connection.execute(delete_positions_not_top_rank)
    
delete_all_positions_not_open = "DELETE FROM Positions where Position = 0"
def delete_all_positions_not_open(connection):
    with connection:
        return connection.execute(delete_all_positions_not_open)

# BLACKLIST
create_blacklist_table = """
    CREATE TABLE IF NOT EXISTS Blacklist (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT
    );
"""

get_all_blacklist_sql = "SELECT * FROM Blacklist;"
def get_all_blacklist(connection):
    with connection:
        return connection.execute(get_all_blacklist_sql).fetchall()
    
# BEST_EMA
create_best_ema_table = """
    CREATE TABLE IF NOT EXISTS Best_Ema (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Ema_Fast INTEGER,
        Ema_Slow INTEGER,
        Time_Frame TEXT,
        Return_Perc REAL,
        BuyHold_Return_Perc REAL,
        Backtest_Start_Date TEXT
    );
"""

get_all_best_ema = "SELECT * FROM Best_Ema;"
def get_all_best_ema(connection):
    with connection:
        return connection.execute(get_all_best_ema).fetchall()
    
get_best_ema_by_symbol_timeframe = """
    SELECT * 
    FROM Best_Ema
    WHERE
        Symbol = ?
        AND Time_Frame = ?;
"""
def get_best_ema_by_symbol_timeframe(connection, symbol, time_frame):
    with connection:
        return connection.execute(get_best_ema_by_symbol_timeframe,(symbol, time_frame)).fetchone()
    
# SYMBOLS_TO_CALC
create_symbols_to_calc_table = """
    CREATE TABLE IF NOT EXISTS Symbols_To_Calc (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Calc_Completed INTEGER,
        Date_Added TEXT,
        Date_Completed TEXT
    );
"""

#
get_all_symbols_to_calc = "SELECT * FROM Symbols_To_Calc;"
def get_all_symbols_to_calc(connection):
    with connection:
        return connection.execute(get_all_symbols_to_calc).fetchall()

#    
get_symbols_to_calc_by_calc_completed = """
    SELECT * 
    FROM Symbols_To_Calc 
    WHERE
        Calc_Completed= ?;
"""
def get_symbols_to_calc_by_calc_completed(connection, completed):
    with connection:
        return connection.execute(get_symbols_to_calc_by_calc_completed,(completed)).fetchall()
    
#    
set_symbols_to_calc_completed = """
    UPDATE Symbols_To_Calc 
    SET Calc_Completed = 1,
        Date_Completed = GETDATE()
    WHERE
        Symbol = ?;
"""
def set_symbols_to_calc_completed(connection, symbol):
    with connection:
        return connection.execute(set_symbols_to_calc_completed,(symbol))
    
delete_symbols_to_calc_completed = """
    DELETE FROM Symbols_To_Calc 
    WHERE Calc_Completed = 1;
"""
def delete_symbols_to_calc_completed(connection):
    with connection:
        return connection.execute(delete_symbols_to_calc_completed)
    
# add to calc the symbols with open positions 
add_symbols_with_open_positions_to_calc = """
INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added)
SELECT DISTINCT Symbol, 0, GETDATE() 
FROM Positions 
WHERE Position = 1
    AND Symbol NOT IN (SELECT Symbol FROM Symbols_To_Calc WHERE Calc_Completed = 0)
"""
def add_symbols_with_open_positions_to_calc(connection):
    with connection:
        return connection.execute(add_symbols_with_open_positions_to_calc)
    
# add to calc the symbols in top rank
add_symbols_top_rank_to_calc = """
INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added)
SELECT DISTINCT Symbol, 0, GETDATE() 
FROM Symbols_By_Market_Phase 
WHERE Symbol NOT IN (SELECT Symbol FROM Symbols_To_Calc WHERE Calc_Completed = 0)
"""
def add_symbols_top_rank_to_calc(connection):
    with connection:
        return connection.execute(add_symbols_top_rank_to_calc)
    
# Symbols_By_Market_Phase
create_symbols_by_market_phase_table = """
    CREATE TABLE IF NOT EXISTS Symbols_By_Market_Phase (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Price_Close REAL,
        Volume INTEGER,
        DSMA50 REAL,
        DSMA200 REAL,
        Market_Phase TEXT,
        Perc_Above_DSMA50 REAL,
        Perc_Above_DSMA200 REAL
    );
"""

get_all_symbols_by_market_phase = "SELECT * FROM Symbols_By_Market_Phase;"
def get_all_symbols_by_market_phase(connection):
    with connection:
        return connection.execute(get_all_symbols_by_market_phase).fetchall()
    
get_symbols_from_symbols_by_market_phase = "SELECT symbols FROM Symbols_By_Market_Phase;"
def get_symbols_from_symbols_by_market_phase(connection):
    with connection:
        return connection.execute(get_symbols_from_symbols_by_market_phase).fetchall()
    
insert_symbols_by_market_phase = """
    INSERT INTO Symbols_By_Market_Phase (
        Symbol,
        Price,
        Volume,
        DSMA50,
        DSMA200,
        Market_Phase,
        Perc_Above_DSMA50,
        Perc_Above_DSMA200,
        Rank INTEGER)
    VALUES(?,?,?,?,?,?,?,?,?);
"""
def insert_symbols_by_market_phase(connection, symbol, price, volume, dsma50, dsma200, market_phase, perc_above_dsma50, perc_above_dsma200):
    with connection:
        return connection.execute(insert_symbols_by_market_phase,(symbol, price, volume, dsma50, dsma200, market_phase, perc_above_dsma50, perc_above_dsma200))
    
delete_all_symbols_by_market_phase = "DELETE FROM Symbols_By_Market_Phase;"
def delete_all_symbols_by_market_phase(connection):
    with connection:
        return connection.execute(insert_symbols_by_market_phase)
    
# create tables
def create_tables(connection):
    with connection:
        connection.execute(create_orders_table)
        connection.execute(create_positions_table)
        connection.execute(create_blacklist_table)
        connection.execute(create_best_ema_table)
        connection.execute(create_symbols_to_calc_table)
        connection.execute(create_symbols_by_market_phase_table)
    
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


    




        