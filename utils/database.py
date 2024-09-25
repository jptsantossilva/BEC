import sqlite3
import os
import math
from datetime import datetime
import pandas as pd

import streamlit_authenticator as stauth

from utils import config
from utils import general

def connect(path: str = ""):
    conn = None
    
    try:
        file_path = os.path.join(path, "data.db")
        conn = sqlite3.connect(file_path, check_same_thread=False)

        # create tables if not exist
        create_tables(conn)
    except sqlite3.Error as e:
        print(e)

    return conn

def is_connection_open(conn):
    if conn is None:
        return False
    try:
        # Execute a simple query to test the connection
        conn.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False

# change connection on Dashboard
def connect_to_bot(folder_name: str):
    #Connects to an SQLite database file located in a child folder of the grandparent folder.
    grandparent_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    child_folder = os.path.join(grandparent_folder, folder_name)
    return connect(child_folder)

def get_users_credentials(connection):
    df_users = get_all_users(connection)
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict('index')
    formatted_credentials = {'usernames': {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials['usernames'][username] = user_info

    return formatted_credentials

# ORDERS
create_orders_table = """
    CREATE TABLE IF NOT EXISTS Orders (
        Id INTEGER PRIMARY KEY,
        Exchange_Order_Id TEXT,
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
        Buy_Order_Id TEXT,
        Exit_Reason TEXT,
        Sell_Perc INTEGER
    );
"""

sql_get_all_orders = "SELECT * FROM Orders;"  
def get_all_orders(connection):
    return pd.read_sql(sql_get_all_orders, connection)

sql_get_orders_by_bot = "SELECT * FROM Orders WHERE Bot = ?;"
def get_orders_by_bot(connection, bot):
    return pd.read_sql(sql_get_orders_by_bot, connection, params=(bot,))

sql_get_orders_by_exchange_order_id = """
    SELECT * 
    FROM Orders 
    WHERE 
        Exchange_Order_Id = ?
    LIMIT 1;
    """
def get_orders_by_exchange_order_id(connection, order_id):
    return pd.read_sql(sql_get_orders_by_exchange_order_id, connection, params=(order_id,))
    
sql_delete_all_orders = "DELETE FROM Orders;"
def delete_all_orders(connection):
    with connection:
        connection.execute(sql_delete_all_orders)

sql_get_years_from_orders = """
    SELECT DISTINCT(strftime('%Y', Date)) AS Year 
    FROM Orders 
    ORDER BY Year DESC;"""
def get_years_from_orders(connection):
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
def get_months_from_orders_by_year(connection, year: str):
    result = []

    if year == None:
        return result

    year = year+"-%"
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
def add_order_buy(connection, exchange_order_id: str, date: str, bot: str, symbol: str, price: float, qty: float, ema_fast: int, ema_slow: int):
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
        Exit_Reason,
        Sell_Perc)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);        
"""
def add_order_sell(connection, sell_order_id: str, buy_order_id: str, date: str, bot: str, symbol: str, price: float, qty: float, ema_fast: int, ema_slow: int, exit_reason: str, sell_percentage: int = 100):
    # sell_order_id and buy_order_id are the exchange ids from the exchange order

    if buy_order_id == "0":
        msg = "No Buy_Order_ID!"
        print(msg)        

        order_id = str(0)
        buy_price = 0
        buy_qty = 0
        pnl_perc = 0
        pnl_value = 0
        
    else:
        df_buy_order = get_orders_by_exchange_order_id(connection=connection, order_id=buy_order_id)
        if not df_buy_order.empty:
            # buy_order_id = buy_order_id #str(df_last_buy_order.loc[0, 'Id'])
            buy_price = float(df_buy_order.loc[0, 'Price'])
            buy_qty = float(df_buy_order.loc[0, 'Qty'])
            
            # order_id is the primary key of Orders table
            order_id = str(df_buy_order.loc[0, 'Id'])

            sell_price = price
            sell_qty = qty

            pnl_perc = (((sell_price)-(buy_price))/(buy_price))*100
            pnl_perc = float(round(pnl_perc, 2))

            # 50% = 0.5
            # percentage = sell_percentage/100

            # calc the PnL value
            # since we can make multiple sells, I will use the buy_qty = sell_qty to get the pnl_value for the partial sold position 
            # pnl_value = (sell_price*sell_qty)-(buy_price*buy_qty)
            pnl_value = (sell_price*sell_qty)-(buy_price*sell_qty)
            pnl_value = float(round(pnl_value, config.n_decimals))
        else:
            msg = "No Buy_Order_ID!"
            print(msg)        

            order_id = str(0)
            buy_price = 0
            buy_qty = 0
            pnl_perc = 0
            pnl_value = 0
  
    side = "SELL"

    with connection:
        connection.execute(sql_add_order_sell, (sell_order_id, 
                                                date, 
                                                bot, 
                                                symbol, 
                                                side, 
                                                price, 
                                                qty, 
                                                ema_fast, 
                                                ema_slow, 
                                                pnl_perc, 
                                                pnl_value, 
                                                order_id, 
                                                exit_reason,
                                                sell_percentage))
        return float(pnl_value), float(pnl_perc)

sql_get_last_buy_order_by_bot_symbol = """
    SELECT * FROM Orders
    WHERE 
        Side = 'BUY' 
        AND Bot = ?
        AND Symbol LIKE ?
    ORDER BY Id DESC LIMIT 1;
"""
def get_last_buy_order_by_bot_symbol(connection, bot: str, symbol: str):
    symbol_only, symbol_stable = general.separate_symbol_and_trade_against(symbol)

    # For those cases where the trade against changed, for example from BUSD to USDT, the BUY order can be BTCBUSD and the sell BTCUSDT.
    # So, I want to search for the buy order in any stablecoin trading pair. BTCBUSD, BTCUSDT, BTCUSDC
    four_chars = "____"
    symbol = f'{symbol_only+four_chars}'  # Used underscores to represent any single character. 
    return pd.read_sql(sql_get_last_buy_order_by_bot_symbol, connection, params=(bot, symbol,))

# sql_get_orders_by_bot_side_year_month = """
#     SELECT Bot,
#         Symbol,
#         Date,
#         Qty,
#         PnL_Perc,
#         PnL_Value,
#         Ema_Fast,
#         Ema_Slow,
#         Exit_Reason
#     FROM Orders
#     WHERE
#         Bot = ?
#         AND Side = ?
#         AND Date LIKE ?;
# """
sql_get_orders_by_bot_side_year_month = """
    SELECT   
        os.Id,
        os.Bot,
        os.Symbol,
        os.PnL_Perc,
        os.PnL_Value,
        ob.Date as Buy_Date,
        ob.Price as Buy_Price,
        ob.Qty as Buy_Qty,
        (ob.Qty*ob.Price) Buy_Position_Value,
        os.Date as Sell_Date,
        os.Price as Sell_Price,
        os.Qty as Sell_Qty,
        (os.Qty*os.Price) Sell_Position_Value,
        os.Ema_Fast,
        os.Ema_Slow,
        os.Exit_Reason    
    FROM Orders as os
    LEFT JOIN orders ob ON os.Buy_Order_Id = ob.Id    
    WHERE
        os.Bot = ?
        AND os.Side = ?
        AND os.Date LIKE ?;
"""
def get_orders_by_bot_side_year_month(connection, bot: str, side: str, year: str, month: str):
    # add a leading zero if necessary
    month = month.zfill(2)

    if year == None:
        df = pd.DataFrame(columns=['Bot', 'Symbol', 'PnL_Perc', 'PnL_Value','Buy_Date', 'Buy_Price', 'Buy_Qty', 'Position_Value', 'Sell_Date','Sell_Price','Sell_Qty','Sell_Position_Value','Ema_Fast','Ema_Slow','Exit_Reason'])
        return df
    
    if month == '13':
        year_month = str(year)+"-%"
    else:
        year_month = str(year)+"-"+str(month)+"-%"

    df = pd.read_sql(sql_get_orders_by_bot_side_year_month, connection, params=(bot, side, year_month))
    return df

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
        Buy_Order_Id TEXT,
        Take_Profit_1 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_2 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_3 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_4 INTEGER NOT NULL DEFAULT 0
    );
"""

sql_insert_position = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank, Ema_Fast, Ema_Slow)
        VALUES (?,?,0,?,?,?);        
"""
def insert_position(connection, bot: str, symbol: str, ema_fast: int, ema_slow: int):
    rank = get_rank_from_symbols_by_market_phase_by_symbol(connection, symbol)
    with connection:
        connection.execute(sql_insert_position, (bot, symbol, rank, ema_fast, ema_slow))

sql_get_positions_by_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Position = ?
"""
def get_positions_by_position(connection, position):
    return pd.read_sql(sql_get_positions_by_position, connection, params=(position,))
  
sql_get_positions_by_bot_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Position = ?
    ORDER BY Rank
"""
def get_positions_by_bot_position(connection, bot: str, position: int):
    return pd.read_sql(sql_get_positions_by_bot_position, connection, params=(bot, position))

sql_get_unrealized_pnl_by_bot = """
    SELECT pos.Id, pos.Bot, pos.Symbol, pos.PnL_Perc, pos.PnL_Value, pos.Take_Profit_1 as TP1, pos.Take_Profit_2 as TP2, pos.Take_Profit_3 as TP3, pos.Take_Profit_4 as TP4, ROUND((pos.Qty/ord.Qty)*100,2) as "RPQ%", pos.Qty, pos.Buy_Price, (pos.Qty*pos.Buy_Price) Position_Value, pos.Date, pos.Duration, pos.Ema_Fast, pos.Ema_Slow
    FROM Positions pos
    JOIN Orders ord ON pos.Buy_Order_Id = ord.Exchange_Order_Id 
    WHERE 
        pos.Bot = ?
        AND pos.Position = ?
"""
def get_unrealized_pnl_by_bot(connection, bot: str):
    position = 1
    df = pd.read_sql(sql_get_unrealized_pnl_by_bot, connection, params=(bot, position))
    
    # convert column
    df['PnL_Perc'] = df['PnL_Perc'].astype(float)
    df['PnL_Value'] = df['PnL_Value'].astype(float)
    df['Qty'] = df['Qty'].astype(float)
    df['Position_Value'] = df['Position_Value'].astype(float)
    df['RPQ%'] = df['RPQ%'].astype(str)
    df['Buy_Price'] = df['Buy_Price'].astype(float)
    df['Ema_Fast'] = df['Ema_Fast'].astype(int)
    df['Ema_Slow'] = df['Ema_Slow'].astype(int)
    return df

sql_get_positions_by_bot_symbol_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Symbol = ?
        AND Position = ?
"""
def get_positions_by_bot_symbol_position(connection, bot: str, symbol: str, position: int):
    return pd.read_sql(sql_get_positions_by_bot_symbol_position, connection, params=(bot, symbol, position))

sql_get_all_positions_by_bot_symbol = """
    SELECT COUNT(*)
    FROM Positions 
    WHERE 
        Bot = ?
        AND symbol = ?
"""
def get_all_positions_by_bot_symbol(connection, bot: str, symbol: str):
    df = pd.read_sql(sql_get_all_positions_by_bot_symbol, connection, params=(bot, symbol,))
    result = int(df.iloc[0, 0]) == 1
    return result
    
    
sql_get_distinct_symbol_from_positions_where_position1 = """
    SELECT DISTINCT(symbol)
    FROM Positions 
    WHERE 
        Position = 1
"""
def get_distinct_symbol_from_positions_where_position1(connection):
    return pd.read_sql(sql_get_distinct_symbol_from_positions_where_position1, connection)
    
sql_get_all_positions_by_bot = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
    ORDER BY
        Rank
"""
def get_all_positions_by_bot(connection, bot: str):
    return pd.read_sql(sql_get_all_positions_by_bot, connection, params=(bot,))
    
sql_get_num_open_positions = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1;
"""
def get_num_open_positions(connection): 
    df = pd.read_sql(sql_get_num_open_positions, connection)
    result = int(df.iloc[0, 0])
    return result
    
sql_get_num_open_positions_by_bot = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1 and Bot = ?;
"""
def get_num_open_positions_by_bot(connection, bot: str): 
    df = pd.read_sql(sql_get_num_open_positions_by_bot, connection, params=(bot,))
    result = int(df.iloc[0, 0])
    return result

#   
sql_add_top_rank_to_positions = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank, Ema_Fast, Ema_Slow)
    SELECT br.Time_Frame, mp.Symbol, 0, mp.Rank, br.Ema_Fast, br.Ema_Slow
    FROM 
        Symbols_By_Market_Phase mp
        INNER JOIN Backtesting_Results br ON mp.Symbol = br.Symbol
    WHERE   
        br.Return_Perc > 0
        AND br.Strategy_Id = ?
        AND NOT EXISTS (
            SELECT 1 
            FROM Positions 
            WHERE Bot = br.Time_Frame AND Symbol = mp.Symbol
        );
"""
def add_top_rank_to_positions(connection, strategy_id: str):
    with connection:
        connection.execute(sql_add_top_rank_to_positions, (strategy_id,))

sql_set_rank_from_positions = """
    UPDATE Positions
    SET
        Rank = ?
    WHERE 
        Symbol = ?
"""
def set_rank_from_positions(connection, symbol: str, rank: int):
    with connection:
        connection.execute(sql_set_rank_from_positions, (rank, symbol,))

sql_set_backtesting_results_from_positions = """
    UPDATE Positions
    SET
        Ema_Fast = ?,
        Ema_Slow = ?
    WHERE 
        Symbol = ?
        and Bot = ?
        and Position = 0
"""
def set_backtesting_results_from_positions(connection, symbol: str, timeframe: str, ema_fast: int, ema_slow: int):
    with connection:
        connection.execute(sql_set_backtesting_results_from_positions, (ema_fast, ema_slow, symbol, timeframe))

sql_update_position_pnl = """
    UPDATE Positions
    SET 
        Curr_Price = ?,
        PnL_Perc = ?,
        PnL_Value = ?,
        Duration = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def update_position_pnl(connection, bot: str, symbol: str, curr_price: float):
    df = get_positions_by_bot_symbol_position(connection, bot, symbol, position=1)
    buy_price = float(df.loc[0,'Buy_Price'])
    qty = float(df.loc[0,'Qty'])
    date = str(df.loc[0,'Date'])

    if not math.isnan(buy_price) and (buy_price > 0):
        pnl_perc = ((curr_price - buy_price)/buy_price)*100
        pnl_perc = float(round(pnl_perc,2))

        pnl_value = (curr_price*qty)-(buy_price*qty)
        pnl_value = float(round(pnl_value, config.n_decimals))
        
        # duration
        datetime_now = datetime.now()
        
        duration = None
        if date != 'None':
            try:
                # Try parsing with milliseconds format
                datetime_open_position = datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                # If parsing with milliseconds format fails, try parsing without milliseconds format
                datetime_open_position = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

            diff_seconds = int((datetime_now - datetime_open_position).total_seconds())
            duration = str(calc_duration(diff_seconds))

    with connection:
        connection.execute(sql_update_position_pnl, (curr_price, pnl_perc, pnl_value, duration, bot, symbol))

sql_set_position_buy = """
    UPDATE Positions
    SET 
        Position = 1,
        Qty = ?,
        Buy_Price = ?,
        Curr_Price = ?,
        Date = ?,
        Ema_Fast = ?,
        Ema_Slow = ?,
        Buy_Order_Id = ?,
        PnL_Perc = 0,
        PnL_Value = 0,
        Duration = 0
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_buy(connection, bot: str, symbol: str, qty: float, buy_price: float, date: str, ema_fast: int, ema_slow: int, buy_order_id: str):
    curr_price = buy_price    
    with connection:
        connection.execute(sql_set_position_buy, (qty, 
                                                  buy_price, 
                                                  curr_price, 
                                                  date,
                                                  ema_fast,
                                                  ema_slow, 
                                                  buy_order_id, 
                                                  bot, 
                                                  symbol))

sql_set_position_sell = """
    UPDATE Positions
    SET 
        Date = NULL,
        Position = 0,
        Buy_Price = 0,
        Curr_Price = 0,
        Qty = 0,
        Ema_Fast = NULL,
        Ema_Slow = NULL,
        PnL_Perc = 0,
        PnL_Value = 0,
        Duration = 0,        
        Buy_Order_Id = NULL,
        Take_Profit_1 = 0,
        Take_Profit_2 = 0,
        Take_Profit_3 = 0,
        Take_Profit_4 = 0
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_sell(connection, bot: str, symbol: str):
    with connection:
        connection.execute(sql_set_position_sell, (bot, symbol))

sql_set_position_qty = """
    UPDATE Positions
    SET 
        Qty = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_qty(connection, bot: str, symbol: str, qty: float):
    with connection:
        connection.execute(sql_set_position_qty, (qty, bot, symbol))

sql_set_position_take_profit_1 = """
    UPDATE Positions
    SET 
        Take_Profit_1 = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_take_profit_1(connection, bot: str, symbol: str, take_profit_1: int):
    with connection:
        connection.execute(sql_set_position_take_profit_1, (take_profit_1, bot, symbol))

sql_set_position_take_profit_2 = """
    UPDATE Positions
    SET 
        Take_Profit_2 = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_take_profit_2(connection, bot: str, symbol: str, take_profit_2: int):
    with connection:
        connection.execute(sql_set_position_take_profit_2, (take_profit_2, bot, symbol))

sql_set_position_take_profit_3 = """
    UPDATE Positions
    SET 
        Take_Profit_3 = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_take_profit_3(connection, bot: str, symbol: str, take_profit_3: int):
    with connection:
        connection.execute(sql_set_position_take_profit_3, (take_profit_3, bot, symbol))

sql_set_position_take_profit_4 = """
    UPDATE Positions
    SET 
        Take_Profit_4 = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_take_profit_4(connection, bot: str, symbol: str, take_profit_4: int):
    with connection:
        connection.execute(sql_set_position_take_profit_4, (take_profit_4, bot, symbol))

sql_delete_all_positions = "DELETE FROM Positions;"
def delete_all_positions(connection):
    with connection:
        connection.execute(sql_delete_all_positions)

sql_delete_positions_not_top_rank = "DELETE FROM Positions where Position = 0 and Symbol not in (select Symbol from Symbols_By_Market_Phase);"
def delete_positions_not_top_rank(connection):
    with connection:
        connection.execute(sql_delete_positions_not_top_rank)
    
sql_delete_all_positions_not_open = "DELETE FROM Positions where Position = 0"
def delete_all_positions_not_open(connection):
    with connection:
        connection.execute(sql_delete_all_positions_not_open)

sql_total_value = """
    SELECT SUM(Curr_Price*Qty) as Total_Value({})
"""

# BLACKLIST
sql_create_blacklist_table = """
    CREATE TABLE IF NOT EXISTS Blacklist (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT
    );
"""

sql_get_symbol_blacklist = "SELECT * FROM Blacklist;"
def get_symbol_blacklist(connection):
    return pd.read_sql(sql_get_symbol_blacklist, connection, index_col="Id") 

sql_delete_all_blacklist = "DELETE FROM Blacklist;"
def delete_all_blacklist(connection):
    with connection:
        connection.execute(sql_delete_all_blacklist)

sql_delete_id_blacklist = "DELETE FROM Blacklist WHERE Id = ?;"
def delete_id_blacklist(connection, ids: list):
    with connection:
        connection.executemany(sql_delete_id_blacklist, [(id,) for id in ids])

sql_add_blacklist = "INSERT OR REPLACE INTO Blacklist (Symbol) VALUES (?);"
def add_blacklist(connection, symbols: list):
    with connection:
        connection.executemany(sql_add_blacklist, [(symbol,) for symbol in symbols])
    
# STRATEGIES
sql_create_strategies_table = """
    CREATE TABLE IF NOT EXISTS Strategies (
    Id TEXT NOT NULL PRIMARY KEY,
    Name TEXT,
    Backtest_Optimize INTEGER NOT NULL DEFAULT 1,
    Main_Strategy INTEGER NOT NULL DEFAULT 1,
    BTC_Strategy INTEGER NOT NULL DEFAULT 0
    ); 
"""

sql_strategies_add_default_strategies = """
INSERT OR IGNORE INTO Strategies (Id, Name) VALUES ('ema_cross_with_market_phases', 'EMA Cross with Market Phases');
INSERT OR IGNORE INTO Strategies (Id, Name, BTC_Strategy) VALUES ('ema_cross', 'EMA Cross', 1);
INSERT OR IGNORE INTO Strategies (Id, Name, Backtest_Optimize, BTC_Strategy) VALUES ('market_phases', 'Market Phases', 0, 1);
"""

sql_get_all_strategies = "SELECT * FROM Strategies;"
def get_all_strategies(connection):
    return pd.read_sql(sql_get_all_strategies, connection)

sql_get_strategies_for_main = "SELECT * FROM Strategies where Main_Strategy = 1;"
def get_strategies_for_main(connection):
    return pd.read_sql(sql_get_strategies_for_main, connection)

sql_get_strategies_for_btc = "SELECT * FROM Strategies where BTC_Strategy = 1;"
def get_strategies_for_btc(connection):
    return pd.read_sql(sql_get_strategies_for_btc, connection)

sql_get_strategy_name = "SELECT Name FROM Strategies where Id = ?;"
def get_strategy_name(connection, strategy_id: str):
    df = pd.read_sql(sql_get_strategy_name, connection, params=(strategy_id,))
    if df.empty:
        result = ""
    else:
        result = df.iloc[0, 0]
    return result

sql_get_strategy_by_id = "SELECT * FROM Strategies where Id = ?;"
def get_strategy_by_id(connection, strategy_id: str):
    return pd.read_sql(sql_get_strategy_by_id, connection, params=(strategy_id,))
    
# BACKTESTING_RESULTS
sql_create_backtesting_results_table = """
    CREATE TABLE IF NOT EXISTS Backtesting_Results (
        Id INTEGER PRIMARY KEY,
        Symbol TEXT,
        Ema_Fast INTEGER,
        Ema_Slow INTEGER,
        Time_Frame TEXT,
        Return_Perc REAL,
        BuyHold_Return_Perc REAL,
        Backtest_Start_Date TEXT,
        Backtest_End_Date TEXT,
        Strategy_Id TEXT,
        CONSTRAINT symbol_time_frame_strategy_unique UNIQUE (Symbol, Time_Frame, Strategy_Id)
    );
"""

sql_get_all_backtesting_results = """
    SELECT br.Symbol, br.Time_Frame, br.Return_Perc, br.BuyHold_Return_Perc, br.Backtest_Start_Date, br.Backtest_End_Date, br.Strategy_Id, st.Name as Strategy_Name, br.Ema_Fast, br.Ema_Slow
    FROM Backtesting_Results AS br
    JOIN Strategies AS st ON br.Strategy_Id = st.Id
    ORDER BY br.Symbol, st.Name;
"""
def get_all_backtesting_results(connection):
    return pd.read_sql(sql_get_all_backtesting_results, connection)
    # return pd.read_sql(sql_get_all_backtesting_results, connection)
    
sql_get_backtesting_results_by_symbol_timeframe_strategy = """
    SELECT be.*, st.Name
    FROM Backtesting_Results as be
    JOIN Strategies as st on be.Strategy_Id = st.Id
    WHERE
        be.Symbol = ?
        AND be.Time_Frame = ?
        AND be.Strategy_Id = ?;
"""
def get_backtesting_results_by_symbol_timeframe_strategy(connection, symbol: str, time_frame: str, strategy_id: str):
    return pd.read_sql(sql_get_backtesting_results_by_symbol_timeframe_strategy, connection, params=(symbol, time_frame, strategy_id))

sql_add_backtesting_results = """
    INSERT OR REPLACE INTO Backtesting_Results (
        Symbol, Ema_Fast, Ema_Slow, Time_Frame, Return_Perc, BuyHold_Return_Perc, Backtest_Start_Date, Backtest_End_Date, Strategy_Id
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
def add_backtesting_results(connection, timeframe: str, symbol: str, ema_fast: int, ema_slow: int, return_perc: float, buy_hold_return_perc: float, backtest_start_date: str, backtest_end_date: str, strategy_Id: str):
    with connection:
        connection.execute(sql_add_backtesting_results, (str(symbol), 
                                              int(ema_fast), 
                                              int(ema_slow), 
                                              str(timeframe), 
                                              float(return_perc), 
                                              float(buy_hold_return_perc), 
                                              str(backtest_start_date),
                                              str(backtest_end_date),
                                              str(strategy_Id)
                                              )
                            )
    
sql_delete_all_backtesting_results = "DELETE FROM Backtesting_Results;"
def delete_all_backtesting_results(connection):
    with connection:
        connection.execute(sql_delete_all_backtesting_results)

# BACKTESTING_TRADES
sql_create_backtesting_trades_table = """
    CREATE TABLE IF NOT EXISTS "Backtesting_Trades" (
        Id INTEGER PRIMARY KEY,
        "Symbol"	TEXT,
        "Time_Frame"	TEXT,
        "Strategy_Id"	TEXT,
        "EntryBar"	INTEGER,
        "ExitBar"	INTEGER,
        "EntryPrice"	REAL,
        "ExitPrice"	REAL,
        "PnL"	REAL,
        "ReturnPct"	REAL,
        "EntryTime"	TIMESTAMP,
        "ExitTime"	TIMESTAMP,
        "Duration"	TEXT,
        CONSTRAINT "bt_symbol__timeframe_strategy_entrytime_exittime" UNIQUE("Symbol","Time_Frame","Strategy_Id","EntryTime","ExitTime")
);
"""

sql_get_all_backtesting_trades = """
    SELECT bt.Symbol, bt.Time_Frame, bt.ReturnPct, 
    bt.Strategy_Id, st.Name as Strategy_Name, 
    bt.EntryTime, bt.ExitTime, bt.EntryPrice, bt.ExitPrice, bt.PnL, bt.Duration  
    FROM Backtesting_Trades AS bt
    JOIN Strategies AS st ON bt.Strategy_Id = st.Id
    ORDER BY bt.Symbol, st.Name;
"""
def get_all_backtesting_trades(connection):
    return pd.read_sql(sql_get_all_backtesting_trades, connection)

sql_add_backtesting_trade = """
    INSERT OR REPLACE INTO Backtesting_Trades (
        Symbol, Time_Frame, Strategy_Id, EntryBar, ExitBar, EntryPrice, ExitPrice, PnL, ReturnPct, EntryTime, ExitTime, Duration
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
def add_backtesting_trade(connection, symbol: str, timeframe: str, strategy_id: str, entry_bar: int, exit_bar: int, entry_price: float, exit_price: float, pnl: float, return_pct: float, entry_time: str, exit_time: str, duration: str):
    with connection:
        connection.execute(sql_add_backtesting_trade, (
            str(symbol),
            str(timeframe),
            str(strategy_id),
            int(entry_bar),
            int(exit_bar),
            float(entry_price),
            float(exit_price),
            float(pnl),
            float(return_pct),
            str(entry_time),
            str(exit_time),
            str(duration)
        ))

def delete_backtesting_trades_symbol_timeframe_strategy(connection, symbol, timeframe, strategy_id):
    sql = """
        DELETE FROM Backtesting_Trades 
        WHERE 
            Symbol = ?
            AND Time_Frame = ?
            AND Strategy_Id = ?;
    """
    with connection:
        connection.execute(sql, (symbol, timeframe, strategy_id, ))

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
def get_all_symbols_to_calc(connection):
    return pd.read_sql(sql_get_all_symbols_to_calc, connection)

#    
sql_get_symbols_to_calc_by_calc_completed = """
    SELECT Symbol 
    FROM Symbols_To_Calc 
    WHERE
        Calc_Completed = ?;
"""
def get_symbols_to_calc_by_calc_completed(connection, completed: int):
    return pd.read_sql(sql_get_symbols_to_calc_by_calc_completed, connection, params=(completed,))
    
#    
sql_set_symbols_to_calc_completed = """
    UPDATE Symbols_To_Calc 
    SET Calc_Completed = 1,
        Date_Completed = datetime('now')
    WHERE
        Symbol = ?;
"""
def set_symbols_to_calc_completed(connection, symbol: str):
    with connection:
        connection.execute(sql_set_symbols_to_calc_completed, (symbol,))
    
sql_delete_symbols_to_calc_completed = """
    DELETE FROM Symbols_To_Calc 
    WHERE Calc_Completed = 1;
"""
def delete_symbols_to_calc_completed(connection):
    with connection:
        connection.execute(sql_delete_symbols_to_calc_completed)

sql_delete_all_symbols_to_calc = "DELETE FROM Symbols_To_Calc;"
def delete_all_symbols_to_calc(connection):
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
def add_symbols_with_open_positions_to_calc(connection):
    with connection:
        connection.execute(sql_add_symbols_with_open_positions_to_calc)
    
# add to calc the symbols in top rank
sql_add_symbols_top_rank_to_calc = """
INSERT INTO Symbols_To_Calc (Symbol, Calc_Completed, Date_Added)
SELECT DISTINCT Symbol, 0, datetime('now')
FROM Symbols_By_Market_Phase 
WHERE Symbol NOT IN (SELECT Symbol FROM Symbols_To_Calc WHERE Calc_Completed = 0)
"""
def add_symbols_top_rank_to_calc(connection):
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

sql_create_symbols_by_market_phase_historical_table = """
    CREATE TABLE IF NOT EXISTS Symbols_By_Market_Phase_Historical (
            Id INTEGER PRIMARY KEY,
            Symbol TEXT,
            Price REAL,
            DSMA50 REAL,
            DSMA200 REAL,
            Market_Phase TEXT,
            Perc_Above_DSMA50 REAL,
            Perc_Above_DSMA200 REAL,
            Rank INTEGER,
            Date_Inserted TEXT
        );
"""

sql_symbols_by_market_phase_Historical_get_symbols_days_at_top = """
    SELECT symbol, 
        COUNT(DISTINCT Date_Inserted) AS Days_at_TOP,
        MIN(Date_Inserted) AS First_Date, 
        MAX(Date_Inserted) AS Last_Date  
    FROM Symbols_By_Market_Phase_Historical
    GROUP BY symbol
    ORDER BY Days_at_TOP DESC
"""
def symbols_by_market_phase_Historical_get_symbols_days_at_top(connection):
    return pd.read_sql(sql_symbols_by_market_phase_Historical_get_symbols_days_at_top, connection)


sql_get_all_symbols_by_market_phase = "SELECT Id,Rank, Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200 FROM Symbols_By_Market_Phase;"
def get_all_symbols_by_market_phase(connection):
    return pd.read_sql(sql_get_all_symbols_by_market_phase, connection, index_col="Id")
    
sql_get_symbols_from_symbols_by_market_phase = "SELECT Symbol FROM Symbols_By_Market_Phase;"
def get_symbols_from_symbols_by_market_phase(connection):
    return pd.read_sql(sql_get_symbols_from_symbols_by_market_phase, connection)

sql_get_rank_from_symbols_by_market_phase_by_symbol = """
    SELECT Rank 
    FROM Symbols_By_Market_Phase
    WHERE Symbol = ?;
"""
def get_rank_from_symbols_by_market_phase_by_symbol(connection, symbol: str):
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
def insert_symbols_by_market_phase(connection, symbol: str, price: float, dsma50: float, dsma200: float, market_phase: str, perc_above_dsma50: float, perc_above_dsma200: float, rank: int):
    with connection:
        connection.execute(sql_insert_symbols_by_market_phase,(symbol, price, dsma50, dsma200, market_phase, perc_above_dsma50, perc_above_dsma200, rank))

sql_insert_symbols_by_market_phase_historical = """
    INSERT INTO Symbols_By_Market_Phase_Historical 
        (Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200, Rank, Date_Inserted)
    SELECT Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200, Rank, ?
    FROM Symbols_By_Market_Phase;
"""
def insert_symbols_by_market_phase_historical(connection, date_inserted: str):
    with connection:
        connection.execute(sql_insert_symbols_by_market_phase_historical,(date_inserted,))


sql_delete_all_symbols_by_market_phase = "DELETE FROM Symbols_By_Market_Phase;"
def delete_all_symbols_by_market_phase(connection):
    with connection:
        connection.execute(sql_delete_all_symbols_by_market_phase)

sql_get_distinct_symbol_by_market_phase_and_positions = """  
    SELECT DISTINCT symbol 
    FROM (
        SELECT symbol, Rank FROM Symbols_By_Market_Phase
        UNION
        SELECT symbol, 100 as Rank FROM Positions WHERE Position=1
    ) AS symbols
    ORDER BY Rank ASC;
"""
def get_distinct_symbol_by_market_phase_and_positions(connection):
    return pd.read_sql(sql_get_distinct_symbol_by_market_phase_and_positions, connection)
    
# Users
sql_create_users_table = """
    CREATE TABLE IF NOT EXISTS Users (
        username TEXT PRIMARY KEY,
        email TEXT,
        name TEXT,
        password TEXT
    );
"""

sql_users_add_admin = """
    INSERT OR IGNORE INTO Users (
        username, email, name, password) 
    VALUES (
        ?, ?, ?, ?
        );
"""
sql_get_all_users = "SELECT * FROM Users;"
def get_all_users(connection):
    return pd.read_sql(sql_get_all_users, connection, index_col="username")

sql_get_user_by_username = "SELECT * FROM Users WHERE username = ?;"
def get_user_by_username(connection, username: str):
    return pd.read_sql(sql_get_user_by_username, connection, params=(username,))

sql_add_user = """
    INSERT OR REPLACE INTO Users (
        username, email, name, password
        ) 
        VALUES (?, ?, ? ,?);
"""
def add_user(connection, username: str, email: str, name: str, password: str):
    with connection:
        connection.execute(sql_add_user, (username, email, name, password))

sql_update_user_password = """
    UPDATE Users
    SET
        password = ?
    WHERE 
        username = ?
"""
def update_user_password(connection, username: str, password: str):
    with connection:
        connection.execute(sql_update_user_password, (password, username,))

# Balances
sql_create_balances_table = """
    CREATE TABLE IF NOT EXISTS Balances (
    Id INTEGER PRIMARY KEY,
    Date TEXT,
    Asset TEXT,
    Balance REAL,
	USD_Price REAL,
	BTC_Price REAL,
    Balance_USD REAL,
    Balance_BTC REAL,
    Total_Balance_Of_BTC REAL
);
"""

sql_add_balances = """
    INSERT OR IGNORE INTO Balances (Date, Asset, Balance, USD_Price, BTC_Price, Balance_USD, Balance_BTC, Total_Balance_Of_BTC) VALUES (?, ?, ?, ?, ?,?, ?, ?);
"""
def add_balances(connection, balances: pd.DataFrame):
    if balances.empty:
        return
    # convert dataframe to a list of tuples
    data = list(balances.to_records(index=False))
    for row in data:
        with connection:
            connection.execute(sql_add_balances, row)
    
def get_asset_balances_last_n_days(connection, n_days):
    sql_get_balances_last_n_days = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE Date >= date('now', ? || ' days')
        AND Balance_USD > 1;
    """
    params = (str(-n_days),)  # Convert n_days to a negative string for date subtraction
    return pd.read_sql(sql_get_balances_last_n_days, connection, params=params)

def get_asset_balances_ytd(connection):
    sql_get_balances_ytd = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE strftime('%Y', Date) = strftime('%Y', 'now')
        AND Balance_USD > 1;
    """
    return pd.read_sql(sql_get_balances_ytd, connection)

def get_asset_balances_all_time(connection):
    sql_get_balances_all_time = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE Balance_USD > 1;
    """
    return pd.read_sql(sql_get_balances_all_time, connection)

def get_total_balance_last_n_days(connection, n_days, asset):
    if asset not in ["USD", "BTC"]:
        # Return an empty pandas DataFrame
        return pd.DataFrame()
    
    if asset == "USD":
        num_decimals = 2
    
        sql_get_total_balance_last_n_days = f"""
            SELECT Date, ROUND(SUM(Balance_{asset}), {num_decimals}) as Total_Balance_{asset}
            FROM Balances
            WHERE Date >= date('now', ? || ' days')
            GROUP BY Date
        """
    elif asset == "BTC":
        sql_get_total_balance_last_n_days = f"""
            SELECT Date, Total_Balance_Of_BTC as Total_Balance_{asset}
            FROM Balances
            WHERE Date >= date('now', ? || ' days')
            GROUP BY Date
        """

    params = (str(-n_days),)  # Convert n_days to a negative string for date subtraction
    return pd.read_sql(sql_get_total_balance_last_n_days, connection, params=params)

def get_total_balance_ytd(connection, asset):
    if asset not in ["USD", "BTC"]:
        # Return an empty pandas DataFrame
        return pd.DataFrame()
    
    if asset == "USD":
        num_decimals = 2
    elif asset == "BTC":
        num_decimals = 5
    
    sql_get_total_balance_last_n_days = f"""
        SELECT Date, ROUND(SUM(Balance_{asset}), {num_decimals}) as Total_Balance_{asset}
        FROM Balances
        WHERE strftime('%Y', Date) = strftime('%Y', 'now')
        GROUP BY Date
    """
    return pd.read_sql(sql_get_total_balance_last_n_days, connection)

def get_total_balance_all_time(connection, asset):
    if asset not in ["USD", "BTC"]:
        # Return an empty pandas DataFrame
        return pd.DataFrame()
    
    if asset == "USD":
        num_decimals = 2
    elif asset == "BTC":
        num_decimals = 5
    
    sql_get_total_balance_all_time = f"""
        SELECT Date, ROUND(SUM(Balance_{asset}), {num_decimals}) as Total_Balance_{asset}
        FROM Balances
        GROUP BY Date
    """
    return pd.read_sql(sql_get_total_balance_all_time, connection)

sql_get_last_date_from_balances="""
    SELECT Date FROM Balances ORDER BY Date DESC LIMIT 1;
"""
def get_last_date_from_balances(connection):
    df = pd.read_sql(sql_get_last_date_from_balances, connection)
    if df.empty:
        result = '0'
    else:
        result = str(df.iloc[0, 0])
    return result

# SIGNALS LOG
sql_create_signals_log_table ="""
    CREATE TABLE IF NOT EXISTS Signals_Log (
    Date TEXT NOT NULL,
    Signal TEXT NOT NULL,
    Signal_Message TEXT,
    Symbol TEXT NOT NULL,
    Notes TEXT
);
"""
sql_get_all_signals_log = """
    SELECT *
    FROM Signals_Log
    ORDER BY Date DESC LIMIT ?;
"""
def get_all_signals_log(connection, num_rows):
    return pd.read_sql(sql_get_all_signals_log, connection, params=(num_rows,))

sql_add_signal_log = """
    INSERT INTO Signals_Log (Date, Signal, Signal_Message, Symbol, Notes) VALUES (?, ?, ?, ?, ?);
"""
def add_signal_log(connection, date: datetime, signal: str, signal_message: str, symbol: str, notes: str):
    # format the current date and time
    date_formatted = date.strftime("%Y-%m-%d %H:%M:%S")
    with connection:
        connection.execute(sql_add_signal_log, (date_formatted, signal, signal_message, symbol, notes))


# Locked_Values
sql_create_locked_values_table = """
    CREATE TABLE IF NOT EXISTS Locked_Values (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        Position_Id INTEGER NOT NULL,
        Buy_Order_Id TEXT NOT NULL,
        Locked_Amount REAL NOT NULL,
        Locked_At DATETIME DEFAULT CURRENT_TIMESTAMP,
        Released BOOLEAN DEFAULT 0,
        Released_At DATETIME DEFAULT NULL,
        FOREIGN KEY (Position_Id) REFERENCES Positions(Id)
);
"""
# Function to lock a value for a specific position
def lock_value(connection, position_id, buy_order_id, amount):
    with connection:
        connection.execute("INSERT INTO Locked_Values (Position_Id, Buy_Order_Id, Locked_Amount) VALUES (?, ?, ?)", (str(position_id), buy_order_id, amount))
    

# Function to release a value when the position is fully closed
def release_value(connection, position_id):
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Position_Id = ?"
    with connection:
        connection.execute(sql, (str(position_id),))

# Function to release all locked values
def release_all_values(connection):
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Released = 0"
    with connection:
        connection.execute(sql)

# Function to release a value when the position is fully closed
def release_locked_value_by_id(connection, id):
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Id = ?"
    with connection:
        connection.execute(sql, (str(id),))

def get_total_locked_values(connection):
    sql = """
        SELECT COALESCE(SUM(Locked_Amount), 0) AS Total_Locked
        FROM Locked_Values
        WHERE Released = 0;
    """
 
    df = pd.read_sql(sql, connection)
    if df.empty:
        result = float(0)
    else:
        result = float(df.iloc[0, 0])
    return result

def get_all_locked_values(connection):
    sql = """
        WITH cte AS (
            SELECT lv.Id, po.Bot, po.Symbol, lv.Locked_Amount, lv.Locked_At
            FROM Locked_Values lv
            JOIN Positions po ON po.Id = lv.Position_Id
            WHERE Released = 0
            ORDER BY Bot, Symbol
            )
        SELECT *
        FROM cte
        UNION ALL
        SELECT 0, 'Total', '', COALESCE(SUM(Locked_Amount), 0), ''
        FROM cte;
    """

    return pd.read_sql(sql, connection)
        
# PRAGMA
sql_get_pragma_user_version = """
    PRAGMA user_version;
"""
def get_pragma_user_version(connection):
    df = pd.read_sql(sql_get_pragma_user_version, connection)
    result = df.iloc[0, 0]
    return result

sql_set_pragma_user_version = """
    PRAGMA user_version = {};
"""
def set_pragma_user_version(connection, version):
    with connection:
        query = sql_set_pragma_user_version.format(version)
        connection.execute(query)

# create tables
def create_tables(connection):
    with connection:

        # --------
        # apply database scripts updates
        # check changelog version
        version_changelog = general.extract_date_from_local_changelog()
        # Remove "-" characters
        version_changelog = int(version_changelog.replace("-", ""))  
        # check changelog version
        version_db = get_pragma_user_version(connection=connection)
        # if database is new then ignore the updates
        if (version_db > 0) and (version_db != version_changelog):
            apply_database_scripts_updates(connection=connection)
        # --------

        connection.execute(create_orders_table)
        connection.execute(sql_create_positions_table)
        connection.execute(sql_create_blacklist_table)
        connection.execute(sql_create_backtesting_results_table)
        connection.execute(sql_create_backtesting_trades_table)
        connection.execute(sql_create_strategies_table)
        # Split the SQL statements and execute them one by one
        for statement in sql_strategies_add_default_strategies.split(';'):
            if statement.strip():
                connection.execute(statement)
        
        connection.execute(sql_create_symbols_to_calc_table)
        connection.execute(sql_create_symbols_by_market_phase_table)
        connection.execute(sql_create_symbols_by_market_phase_historical_table)
        # users
        connection.execute(sql_create_users_table)
        default_admin_password = "admin"
        hashed_password = stauth.Hasher([default_admin_password]).generate()
        connection.execute(sql_users_add_admin, ("admin", "admin@admin.com", "admin", hashed_password[0]))
        # balances
        connection.execute(sql_create_balances_table)
        # signals log
        connection.execute(sql_create_signals_log_table)
        # locked values
        connection.execute(sql_create_locked_values_table)

        # update version on db
        # commented because the db user version must be in the end of database script to make sure everything was ok with the script
        # set_pragma_user_version(connection=connection, version=version_changelog)

def apply_database_scripts_updates(connection):
    
    # Define the path to the folder containing the file
    folder_path = 'utils/db_scripts'
    version = general.extract_date_from_local_changelog()
    filename = f'db_scripts_{version}'
    filename_full = filename+".sql"

    # Check if the file exists within the specified folder
    file_path = os.path.join(folder_path, filename_full)
    
    # Check if the file exists
    if os.path.exists(file_path):
        # Connect to database
        conn = connection
        cursor = conn.cursor()

        # Read and execute SQL scripts from the file
        with open(file_path, 'r') as script_file:
            sql_script = script_file.read()
            cursor.executescript(sql_script)

        # Commit the changes to the database
        conn.commit()

        # Close the database connection
        # conn.close()

        # Rename the file with a datetime timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_filename = f'{filename}_{timestamp}.sql'
        new_file_path = os.path.join(folder_path, new_filename)
        os.rename(file_path, new_file_path)
    else:
        show_message = False
        if show_message:
            print(f"File '{file_path}' does not exist.")
    
# convert 123456 seconds to 1d 2h 3m 4s format    
def calc_duration(seconds):
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

# create db connection
conn = connect()




    




        