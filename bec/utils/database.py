import math
import os
import json
import secrets
import shutil
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import yaml

from bec.utils import general


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MONTE_CARLO_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "static", "backtest_results", "monte_carlo")


def build_strategy_params_json(strategy_id: str, fast_value=0, slow_value=0) -> str:
    strategy_id = str(strategy_id or "").strip()

    def _int_or_zero(value):
        try:
            if pd.isna(value):
                return 0
        except TypeError:
            pass
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0

    fast = _int_or_zero(fast_value)
    slow = _int_or_zero(slow_value)

    if strategy_id in {"ema_cross", "ema_cross_with_market_phases"}:
        params = {"ema_fast": fast, "ema_slow": slow}
        if strategy_id == "ema_cross_with_market_phases":
            params["phase_filter"] = True
    elif strategy_id == "hma_rsi_linreg":
        params = {
            "hma_fast": fast,
            "hma_slow": slow,
            "rsi_period": 14,
            "rsi_min": 52,
            "daily_linreg_period": 50,
        }
    elif strategy_id == "market_phases":
        params = {"sma_fast": 50, "sma_slow": 200}
    else:
        params = {}
        if fast:
            params["fast"] = fast
        if slow:
            params["slow"] = slow

    return json.dumps(params, separators=(",", ":"))


def parse_strategy_params(value) -> dict:
    if value in (None, ""):
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}

# Global connection handle (initialized later)
conn = None
_thread_local = threading.local()

def connect(path: str = ""):
    try:
        file_path = os.path.join(path, "data.db")
        return sqlite3.connect(file_path, check_same_thread=False)
    except sqlite3.Error as e:
        print(e)
        return None

def is_connection_open(conn):
    if conn is None:
        return False
    try:
        # Execute a simple query to test the connection
        conn.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False

# --- Connection resolver (use one SQLite connection per thread) ---
def _get_conn():
    connection = getattr(_thread_local, "conn", None)
    if connection is None or not is_connection_open(connection):
        connection = connect()
        _thread_local.conn = connection
    return connection


# change connection on Dashboard
def connect_to_bot(folder_name: str):
    #Connects to an SQLite database file located in a child folder of the grandparent folder.
    grandparent_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
    child_folder = os.path.join(grandparent_folder, folder_name)
    return connect(child_folder)

def get_users_credentials():
    connection = _get_conn()
    df_users = get_all_users(connection)
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict('index')
    formatted_credentials = {'usernames': {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials['usernames'][username] = user_info

    return formatted_credentials

# SETTINGS
def get_setting(setting_name):
    connection = _get_conn()

    """Fetches setting from database. If missing, initializes it with a default."""

    # Default values for settings
    default_values = {
        "main_strategies": '["ema_cross_with_market_phases"]',
        "btc_strategy": "market_phases",
        "trade_against_switch": False,
        "take_profit_1": 0,
        "take_profit_1_amount": 5,
        "take_profit_2": 0,
        "take_profit_2_amount": 5,
        "take_profit_3": 0,
        "take_profit_3_amount": 5,
        "take_profit_4": 0,
        "take_profit_4_amount": 5,
        "take_profit_enabled": True,
        "run_mode": "prod",
        "lock_values": True,
        "bot_prefix": "BEC",
        "max_number_of_open_positions": 20,
        "tradable_balance_ratio": 1.0,
        "min_position_size": 20.0,
        "trade_against": "USDC",
        "stop_loss": 10.0,
        "atr_trailing_enabled": True,
        "atr_period": 14,
        "atr_multiplier": 1.8,
        "atr_activation_pnl": 2.0,
        "trade_top_performance": 500,
        "stake_amount_type": "unlimited",
        "trade_against_switch_stablecoin": "USDC",
        "delisting_start_date": datetime.now().isoformat()
    }

    # Corresponding comments for each setting
    setting_comments = {
        "main_strategies": "Primary strategies used for trading, stored as a JSON list.",
        "btc_strategy": "Strategy for trading BTC.",
        "trade_against_switch": "Toggle trading against BTC or USDT/USDC (True/False).",
        "take_profit_1": "First take profit percentage.",
        "take_profit_1_amount": "Amount percentage for first take profit.",
        "take_profit_2": "Second take profit percentage.",
        "take_profit_2_amount": "Amount percentage for second take profit.",
        "take_profit_3": "Third take profit percentage.",
        "take_profit_3_amount": "Amount percentage for third take profit.",
        "take_profit_4": "Fourth take profit percentage.",
        "take_profit_4_amount": "Amount percentage for fourth take profit.",
        "take_profit_enabled": "Enable/disable take-profit levels globally.",
        "run_mode": "Trading mode ('prod' for production, 'test' for testing).",
        "lock_values": "Any amount obtained from partially selling a position will be temporarily locked and cannot be used to purchase another position until the entire position is sold. (True/False).",
        "bot_prefix": "Prefix used for bot-related identifiers.",
        "max_number_of_open_positions": "Maximum number of open trades at a time.",
        "tradable_balance_ratio": "Fraction of balance allowed for trading (0.0-1.0).",
        "min_position_size": "Minimum trade size.",
        "trade_against": "The asset to trade against ('BTC', 'USDC', 'USDT').",
        "stop_loss": "Stop-loss percentage.",
        "atr_trailing_enabled": "Enable/disable ATR-based trailing stop-loss.",
        "atr_period": "ATR lookback period used by trailing stop logic.",
        "atr_multiplier": "ATR multiplier (k) used to calculate the trailing stop distance.",
        "atr_activation_pnl": "PnL percentage threshold required to activate ATR trailing stop.",
        "trade_top_performance": "Top assets considered for trading.",
        "stake_amount_type": "Determines staking limits ('unlimited' or other values).",
        "trade_against_switch_stablecoin": "Choose the stablecoin for auto-switching.",
        "delisting_start_date": "Defines the starting point for monitoring Binance delisting announcements."
    }
    
    try:
        cursor = connection.cursor()

        # Try fetching setting from the database
        cursor.execute("SELECT value FROM Settings WHERE name = ?", (setting_name,))
        row = cursor.fetchone()

        if row:
            value = row[0]  # Return the value from the database
        
            # Convert back to the correct data type
            if setting_name in default_values:
                default_type = type(default_values[setting_name])

                try:
                    if default_type == bool:
                        return value.lower() in ("true", "1")  # Convert 'true'/'1' to boolean
                    elif default_type == int:
                        return int(value)
                    elif default_type == float:
                        return float(value)
                    return value  # Return as string if it's not numeric or boolean
                except ValueError:
                    print(f"Warning: Could not convert {setting_name} value '{value}' to {default_type}. Returning as string.")
                    pass  # If conversion fails, return as string
        
            return value  # Return raw value if no default type is found

        # Setting not found, use default
        if setting_name in default_values:
            setting_value = default_values[setting_name]
            setting_comment = setting_comments.get(setting_name, "No description available.")
        
            # Insert default value into the database
            cursor.execute(
                "INSERT OR IGNORE INTO Settings (name, value, comment) VALUES (?, ?, ?)", 
                (setting_name, str(setting_value), setting_comment)
            )
            connection.commit()

            return setting_value  # Return the default value

        raise ValueError(f"Setting '{setting_name}' not found and no default available.")    
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

def set_trade_against(value):
    connection = _get_conn()

    """Sets the trade_against variable in the database."""
    try:
        cursor = connection.cursor()

        # Use UPSERT (INSERT or UPDATE)
        cursor.execute(
            "INSERT INTO Settings (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = ?",
            ("trade_against", str(value), str(value))
        )
        connection.commit()

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise

def set_setting(name, value):
    connection = _get_conn()

    """Sets a setting in the database."""
    try:
        cursor = connection.cursor()

        # Use UPSERT (INSERT or UPDATE)
        cursor.execute(
            "INSERT INTO Settings (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = ?",
            (name, str(value), str(value))
        )
        connection.commit()

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise


def remove_obsolete_settings():
    connection = _get_conn()
    connection.execute(
        "DELETE FROM Settings WHERE name = ?",
        ("main_strategy",),
    )
    connection.commit()


def get_or_create_secret_setting(name, length=48, comment=""):
    connection = _get_conn()

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT value FROM Settings WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row and row[0]:
            return str(row[0])

        value = secrets.token_urlsafe(length)
        cursor.execute(
            "INSERT INTO Settings (name, value, comment) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = excluded.value, comment = excluded.comment",
            (name, value, comment),
        )
        connection.commit()
        return value
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise

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
        Strategy_Id TEXT,
        Strategy_Params_JSON TEXT,
        PnL_Perc REAL,
        PnL_Value REAL,
        Buy_Order_Id TEXT,
        Exit_Reason TEXT,
        Sell_Perc INTEGER,
        Stop_Type TEXT,
        Stop_Trigger_Price REAL,
        Trail_Stop_ATR_At_Exit REAL,
        Highest_Price_Since_Entry_At_Exit REAL,
        Atr_Params_At_Exit TEXT
    );
"""

sql_get_all_orders = "SELECT * FROM Orders;"  
def get_all_orders():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_orders, connection)

sql_get_orders_by_bot = "SELECT * FROM Orders WHERE Bot = ?;"
def get_orders_by_bot(bot):
    connection = _get_conn()

    return pd.read_sql(sql_get_orders_by_bot, connection, params=(bot,))

sql_get_orders_by_exchange_order_id = """
    SELECT * 
    FROM Orders 
    WHERE 
        Exchange_Order_Id = ?
    LIMIT 1;
    """
def get_orders_by_exchange_order_id(order_id):
    connection = _get_conn()
    return pd.read_sql(sql_get_orders_by_exchange_order_id, connection, params=(order_id,))
    
sql_delete_all_orders = "DELETE FROM Orders;"
def delete_all_orders():
    connection = _get_conn()
    with connection:
        connection.execute(sql_delete_all_orders)

sql_get_years_from_orders = """
    SELECT DISTINCT(strftime('%Y', Date)) AS Year 
    FROM Orders 
    ORDER BY Year DESC;"""
def get_years_from_orders():
    connection = _get_conn()
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
def get_months_from_orders_by_year(year: str):
    connection = _get_conn()

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
        Ema_Slow,
        Strategy_Id,
        Strategy_Params_JSON)
    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?
        );
"""
def add_order_buy(exchange_order_id: str, date: str, bot: str, symbol: str, price: float, qty: float, ema_fast: int, ema_slow: int, strategy_id: str = "", strategy_params_json: str = ""):
    connection = _get_conn()

    side = "BUY"
    if not strategy_params_json:
        strategy_params_json = build_strategy_params_json(strategy_id, ema_fast, ema_slow)
    with connection:
        connection.execute(sql_add_order_buy, (exchange_order_id, date, bot, symbol, side, price, qty, ema_fast, ema_slow, strategy_id, strategy_params_json))

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
        Strategy_Id,
        Strategy_Params_JSON,
        PnL_Perc,
        PnL_Value,
        Buy_Order_Id,
        Exit_Reason,
        Sell_Perc,
        Stop_Type,
        Stop_Trigger_Price,
        Trail_Stop_ATR_At_Exit,
        Highest_Price_Since_Entry_At_Exit,
        Atr_Params_At_Exit)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"""
def add_order_sell(
    sell_order_id: str,
    buy_order_id: str,
    date: str,
    bot: str,
    symbol: str,
    price: float,
    qty: float,
    ema_fast: int,
    ema_slow: int,
    exit_reason: str,
    strategy_id: str = "",
    strategy_params_json: str = "",
    sell_percentage: int = 100,
    stop_type: str = "",
    stop_trigger_price: float = 0.0,
    trail_stop_atr_at_exit: float = 0.0,
    highest_price_since_entry_at_exit: float = 0.0,
    atr_params_at_exit: str = "",
):
    # sell_order_id and buy_order_id are the exchange ids from the exchange order
    connection = _get_conn()

    if buy_order_id == "0":
        msg = "No Buy_Order_ID!"
        print(msg)        

        order_id = str(0)
        buy_price = 0
        buy_qty = 0
        pnl_perc = 0
        pnl_value = 0
        
    else:
        df_buy_order = get_orders_by_exchange_order_id(order_id=buy_order_id)
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
            from bec.utils import config as _config
            settings = _config.load_settings()
            pnl_value = (sell_price*sell_qty)-(buy_price*sell_qty)
            pnl_value = float(round(pnl_value, settings.n_decimals))
        else:
            msg = "No Buy_Order_ID!"
            print(msg)        

            order_id = str(0)
            buy_price = 0
            buy_qty = 0
            pnl_perc = 0
            pnl_value = 0
  
    side = "SELL"
    if not strategy_params_json:
        strategy_params_json = build_strategy_params_json(strategy_id, ema_fast, ema_slow)

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
                                                strategy_id,
                                                strategy_params_json,
                                                pnl_perc, 
                                                pnl_value, 
                                                order_id, 
                                                exit_reason,
                                                sell_percentage,
                                                stop_type,
                                                stop_trigger_price,
                                                trail_stop_atr_at_exit,
                                                highest_price_since_entry_at_exit,
                                                atr_params_at_exit))
        return float(pnl_value), float(pnl_perc)

sql_get_last_buy_order_by_bot_symbol = """
    SELECT * FROM Orders
    WHERE 
        Side = 'BUY' 
        AND Bot = ?
        AND Symbol LIKE ?
    ORDER BY Id DESC LIMIT 1;
"""
def get_last_buy_order_by_bot_symbol(bot: str, symbol: str):
    connection = _get_conn()
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
        os.Strategy_Id,
        os.Strategy_Params_JSON,
        os.Exit_Reason,
        os.Stop_Type,
        os.Stop_Trigger_Price,
        os.Trail_Stop_ATR_At_Exit,
        os.Highest_Price_Since_Entry_At_Exit,
        os.Atr_Params_At_Exit
    FROM Orders as os
    LEFT JOIN orders ob ON os.Buy_Order_Id = ob.Id    
    WHERE
        os.Bot = ?
        AND os.Side = ?
        AND os.Date LIKE ?;
"""
def get_orders_by_bot_side_year_month(bot: str, side: str, year: str, month: str):
    connection = _get_conn()

    # add a leading zero if necessary
    month = month.zfill(2)

    if year == None:
        df = pd.DataFrame(columns=[
            'Bot', 'Symbol', 'PnL_Perc', 'PnL_Value', 'Buy_Date', 'Buy_Price', 'Buy_Qty',
            'Position_Value', 'Sell_Date', 'Sell_Price', 'Sell_Qty', 'Sell_Position_Value',
            'Ema_Fast', 'Ema_Slow', 'Exit_Reason', 'Stop_Type', 'Stop_Trigger_Price',
            'Trail_Stop_ATR_At_Exit', 'Highest_Price_Since_Entry_At_Exit', 'Atr_Params_At_Exit'
        ])
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
        Strategy_Id TEXT,
        Strategy_Name TEXT,
        Strategy_Params_JSON TEXT,
        PnL_Perc REAL,
        PnL_Value REAL,
        Duration TEXT,
        Buy_Order_Id TEXT,
        Take_Profit_1 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_2 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_3 INTEGER NOT NULL DEFAULT 0,
        Take_Profit_4 INTEGER NOT NULL DEFAULT 0,
        Highest_Price_Since_Entry REAL NOT NULL DEFAULT 0,
        Trail_Stop_ATR REAL NOT NULL DEFAULT 0
    );
"""

sql_insert_position = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank, Ema_Fast, Ema_Slow, Strategy_Id, Strategy_Name, Strategy_Params_JSON)
        VALUES (?,?,0,?,?,?,?,?,?);
"""
def insert_position(bot: str, symbol: str, ema_fast: int, ema_slow: int, strategy_id: str = "", strategy_name: str = "", strategy_params_json: str = ""):
    connection = _get_conn()
    rank = get_rank_from_symbols_by_market_phase_by_symbol(symbol)
    if not strategy_params_json:
        strategy_params_json = build_strategy_params_json(strategy_id, ema_fast, ema_slow)
    with connection:
        connection.execute(sql_insert_position, (bot, symbol, rank, ema_fast, ema_slow, strategy_id, strategy_name, strategy_params_json))

sql_get_positions_by_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Position = ?
"""
def get_positions_by_position(position):
    connection = _get_conn()
    return pd.read_sql(sql_get_positions_by_position, connection, params=(position,))
  
sql_get_positions_by_bot_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Position = ?
    ORDER BY Rank
"""
def get_positions_by_bot_position(bot: str, position: int):
    connection = _get_conn()
    return pd.read_sql(sql_get_positions_by_bot_position, connection, params=(bot, position))

sql_get_unrealized_pnl_by_bot = """
    SELECT pos.Id, pos.Bot, pos.Symbol, pos.Strategy_Id, pos.Strategy_Name, pos.Strategy_Params_JSON, pos.PnL_Perc, pos.PnL_Value, pos.Take_Profit_1 as TP1, pos.Take_Profit_2 as TP2, pos.Take_Profit_3 as TP3, pos.Take_Profit_4 as TP4, ROUND((pos.Qty/ord.Qty)*100,2) as "RPQ%", pos.Qty, pos.Buy_Price, (pos.Qty*pos.Buy_Price) Position_Value, pos.Date, pos.Duration, pos.Ema_Fast, pos.Ema_Slow, pos.Trail_Stop_ATR, pos.Highest_Price_Since_Entry
    FROM Positions pos
    JOIN Orders ord ON pos.Buy_Order_Id = ord.Exchange_Order_Id 
    WHERE 
        pos.Bot = ?
        AND pos.Position = ?
"""
def get_unrealized_pnl_by_bot(bot: str):
    connection = _get_conn()
    position = 1
    df = pd.read_sql(sql_get_unrealized_pnl_by_bot, connection, params=(bot, position))
    
    # convert column
    df['PnL_Perc'] = df['PnL_Perc'].astype(float)
    df['PnL_Value'] = df['PnL_Value'].astype(float)
    df['Qty'] = df['Qty'].astype(float)
    df['Position_Value'] = df['Position_Value'].astype(float)
    df['RPQ%'] = df['RPQ%'].astype(str)
    df['Buy_Price'] = df['Buy_Price'].astype(float)
    df['Ema_Fast'] = df['Ema_Fast'].fillna(0).astype(int)
    df['Ema_Slow'] = df['Ema_Slow'].fillna(0).astype(int)
    df['Trail_Stop_ATR'] = df['Trail_Stop_ATR'].astype(float)
    df['Highest_Price_Since_Entry'] = df['Highest_Price_Since_Entry'].astype(float)
    return df

sql_get_positions_by_bot_symbol_position = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
        AND Symbol = ?
        AND Position = ?
"""
def get_positions_by_bot_symbol_position(bot: str, symbol: str, position: int):
    connection = _get_conn()
    return pd.read_sql(sql_get_positions_by_bot_symbol_position, connection, params=(bot, symbol, position))

sql_get_positions_by_bot_symbol_strategy_position = """
    SELECT *
    FROM Positions
    WHERE
        Bot = ?
        AND Symbol = ?
        AND Strategy_Id = ?
        AND Position = ?
"""
def get_positions_by_bot_symbol_strategy_position(bot: str, symbol: str, strategy_id: str, position: int):
    connection = _get_conn()
    return pd.read_sql(sql_get_positions_by_bot_symbol_strategy_position, connection, params=(bot, symbol, strategy_id, position))

sql_get_position_by_id = "SELECT * FROM Positions WHERE Id = ?;"
def get_position_by_id(position_id: int):
    connection = _get_conn()
    return pd.read_sql(sql_get_position_by_id, connection, params=(position_id,))

sql_get_all_positions_by_bot_symbol = """
    SELECT COUNT(*)
    FROM Positions 
    WHERE 
        Bot = ?
        AND symbol = ?
"""
def get_all_positions_by_bot_symbol(bot: str, symbol: str):
    connection = _get_conn()
    df = pd.read_sql(sql_get_all_positions_by_bot_symbol, connection, params=(bot, symbol,))
    result = int(df.iloc[0, 0]) == 1
    return result

sql_get_all_positions_by_bot_symbol_strategy = """
    SELECT COUNT(*)
    FROM Positions
    WHERE
        Bot = ?
        AND Symbol = ?
        AND Strategy_Id = ?
"""
def get_all_positions_by_bot_symbol_strategy(bot: str, symbol: str, strategy_id: str):
    connection = _get_conn()
    df = pd.read_sql(sql_get_all_positions_by_bot_symbol_strategy, connection, params=(bot, symbol, strategy_id))
    return int(df.iloc[0, 0]) >= 1
    
    
sql_get_distinct_symbol_from_positions_where_position1 = """
    SELECT DISTINCT(symbol)
    FROM Positions 
    WHERE 
        Position = 1
"""
def get_distinct_symbol_from_positions_where_position1():
    connection = _get_conn()
    return pd.read_sql(sql_get_distinct_symbol_from_positions_where_position1, connection)
    
sql_get_all_positions_by_bot = """
    SELECT *
    FROM Positions 
    WHERE 
        Bot = ?
    ORDER BY
        Rank
"""
def get_all_positions_by_bot(bot: str):
    connection = _get_conn()
    return pd.read_sql(sql_get_all_positions_by_bot, connection, params=(bot,))
    
sql_get_num_open_positions = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1;
"""
def get_num_open_positions(): 
    connection = _get_conn()
    df = pd.read_sql(sql_get_num_open_positions, connection)
    result = int(df.iloc[0, 0])
    return result
    
sql_get_num_open_positions_by_bot = """
    SELECT COUNT(*) FROM Positions WHERE Position = 1 and Bot = ?;
"""
def get_num_open_positions_by_bot(bot: str):
    connection = _get_conn() 
    df = pd.read_sql(sql_get_num_open_positions_by_bot, connection, params=(bot,))
    result = int(df.iloc[0, 0])
    return result

# Candidates for Positions from current top-ranked symbols and backtesting results.
sql_get_top_rank_position_candidates = """
    SELECT
        br.Time_Frame AS Bot,
        mp.Symbol AS Symbol,
        mp.Rank AS Rank,
        br.Strategy_Id AS Strategy_Id,
        st.Name AS Strategy_Name,
        br.Ema_Fast AS Ema_Fast,
        br.Ema_Slow AS Ema_Slow,
        br.Return_Perc,
        br.BuyHold_Return_Perc,
        br.Max_Drawdown_Perc,
        br.Trades,
        br.Profit_Factor,
        br.SQN,
        br.Quality_Score,
        br.Quality_Grade
    FROM Symbols_By_Market_Phase mp
    INNER JOIN Backtesting_Results br ON mp.Symbol = br.Symbol
    LEFT JOIN Strategies st ON st.Id = br.Strategy_Id
    WHERE
        br.Return_Perc > 0
        AND br.Strategy_Id = ?
        AND NOT EXISTS (
            SELECT 1
            FROM Positions
            WHERE Bot = br.Time_Frame AND Symbol = mp.Symbol AND Strategy_Id = br.Strategy_Id
        );
"""

sql_insert_top_rank_position = """
    INSERT INTO Positions (Bot, Symbol, Position, Rank, Ema_Fast, Ema_Slow, Strategy_Id, Strategy_Name, Strategy_Params_JSON)
    VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?);
"""

def add_top_rank_to_positions(strategy_id: str):
    connection = _get_conn()
    candidates = pd.read_sql(sql_get_top_rank_position_candidates, connection, params=(strategy_id,))
    if candidates.empty:
        return

    to_insert = []
    for _, row in candidates.iterrows():
        timeframe = str(row["Bot"])
        approved, _ = is_backtest_approved(timeframe, row)
        if not approved:
            continue

        ema_fast = None if pd.isna(row["Ema_Fast"]) else int(row["Ema_Fast"])
        ema_slow = None if pd.isna(row["Ema_Slow"]) else int(row["Ema_Slow"])
        strategy_id = str(row["Strategy_Id"])
        to_insert.append(
            (
                timeframe,
                str(row["Symbol"]),
                int(row["Rank"]),
                ema_fast,
                ema_slow,
                strategy_id,
                str(row.get("Strategy_Name") or strategy_id),
                build_strategy_params_json(strategy_id, ema_fast, ema_slow),
            )
        )

    if not to_insert:
        return

    with connection:
        connection.executemany(sql_insert_top_rank_position, to_insert)

sql_set_rank_from_positions = """
    UPDATE Positions
    SET
        Rank = ?
    WHERE 
        Symbol = ?
"""
def set_rank_from_positions(symbol: str, rank: int):
    connection = _get_conn()
    with connection:
        connection.execute(sql_set_rank_from_positions, (rank, symbol,))

sql_set_rank_from_position = "UPDATE Positions SET Rank = ? WHERE Id = ?"
def set_rank_from_position(position_id: int, rank: int):
    connection = _get_conn()
    with connection:
        connection.execute(sql_set_rank_from_position, (rank, position_id))

sql_set_backtesting_results_from_positions = """
    UPDATE Positions
    SET
        Ema_Fast = ?,
        Ema_Slow = ?
    WHERE 
        Symbol = ?
        and Bot = ?
"""
def set_backtesting_results_from_positions(symbol: str, timeframe: str, ema_fast: int, ema_slow: int):
    connection = _get_conn()
    with connection:
        connection.execute(sql_set_backtesting_results_from_positions, (ema_fast, ema_slow, symbol, timeframe))

sql_set_backtesting_results_from_position_strategy = """
    UPDATE Positions
    SET Ema_Fast = ?, Ema_Slow = ?, Strategy_Params_JSON = ?
    WHERE Symbol = ? AND Bot = ? AND Strategy_Id = ? AND Position = 0
"""
def set_backtesting_results_from_position_strategy(symbol: str, timeframe: str, strategy_id: str, ema_fast: int, ema_slow: int):
    connection = _get_conn()
    strategy_params_json = build_strategy_params_json(strategy_id, ema_fast, ema_slow)
    with connection:
        connection.execute(sql_set_backtesting_results_from_position_strategy, (ema_fast, ema_slow, strategy_params_json, symbol, timeframe, strategy_id))

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
def update_position_pnl(bot: str, symbol: str, curr_price: float, position_id: int | None = None):
    df = get_position_by_id(position_id) if position_id is not None else get_positions_by_bot_symbol_position(bot, symbol, position=1)
    if df.empty:
        return
    buy_price = float(df.loc[0,'Buy_Price'])
    qty = float(df.loc[0,'Qty'])
    date = str(df.loc[0,'Date'])

    if not math.isnan(buy_price) and (buy_price > 0):
        pnl_perc = ((curr_price - buy_price)/buy_price)*100
        pnl_perc = float(round(pnl_perc,2))

        from bec.utils import config as _config
        settings = _config.load_settings()
        pnl_value = (curr_price*qty)-(buy_price*qty)
        pnl_value = float(round(pnl_value, settings.n_decimals))
        
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

    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute(
                """
                UPDATE Positions
                SET Curr_Price = ?, PnL_Perc = ?, PnL_Value = ?, Duration = ?
                WHERE Id = ? AND Position = 1
                """,
                (curr_price, pnl_perc, pnl_value, duration, position_id),
            )
        else:
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
        Duration = 0,
        Highest_Price_Since_Entry = ?,
        Trail_Stop_ATR = 0,
        Strategy_Params_JSON = COALESCE(NULLIF(?, ''), Strategy_Params_JSON)
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_buy(bot: str, symbol: str, qty: float, buy_price: float, date: str, ema_fast: int, ema_slow: int, buy_order_id: str, position_id: int | None = None, strategy_id: str = "", strategy_name: str = "", strategy_params_json: str = ""):
    connection = _get_conn()
    curr_price = buy_price
    if not strategy_params_json:
        strategy_params_json = build_strategy_params_json(strategy_id, ema_fast, ema_slow)
    with connection:
        if position_id is not None:
            connection.execute(
                """
                UPDATE Positions
                SET Position = 1, Qty = ?, Buy_Price = ?, Curr_Price = ?, Date = ?,
                    Ema_Fast = ?, Ema_Slow = ?, Buy_Order_Id = ?, PnL_Perc = 0,
                    PnL_Value = 0, Duration = 0, Highest_Price_Since_Entry = ?,
                    Trail_Stop_ATR = 0, Strategy_Id = COALESCE(NULLIF(?, ''), Strategy_Id),
                    Strategy_Name = COALESCE(NULLIF(?, ''), Strategy_Name),
                    Strategy_Params_JSON = COALESCE(NULLIF(?, ''), Strategy_Params_JSON)
                WHERE Id = ?
                """,
                (qty, buy_price, curr_price, date, ema_fast, ema_slow, buy_order_id, buy_price, strategy_id, strategy_name, strategy_params_json, position_id),
            )
        else:
            connection.execute(sql_set_position_buy, (qty,
                                                      buy_price,
                                                      curr_price,
                                                      date,
                                                      ema_fast,
                                                      ema_slow,
                                                      buy_order_id,
                                                      buy_price,
                                                      strategy_params_json,
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
        Take_Profit_4 = 0,
        Highest_Price_Since_Entry = 0,
        Trail_Stop_ATR = 0
    WHERE
        Bot = ? 
        AND Symbol = ? ;        
"""
def set_position_sell(bot: str, symbol: str, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute(
                """
                UPDATE Positions
                SET Date = NULL, Position = 0, Buy_Price = 0, Curr_Price = 0,
                    Qty = 0, PnL_Perc = 0, PnL_Value = 0, Duration = 0,
                    Buy_Order_Id = NULL, Take_Profit_1 = 0, Take_Profit_2 = 0,
                    Take_Profit_3 = 0, Take_Profit_4 = 0,
                    Highest_Price_Since_Entry = 0, Trail_Stop_ATR = 0
                WHERE Id = ?
                """,
                (position_id,),
            )
        else:
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
def set_position_qty(bot: str, symbol: str, qty: float, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute("UPDATE Positions SET Qty = ? WHERE Id = ? AND Position = 1", (qty, position_id))
        else:
            connection.execute(sql_set_position_qty, (qty, bot, symbol))

sql_update_position_risk = """
    UPDATE Positions
    SET
        Highest_Price_Since_Entry = ?,
        Trail_Stop_ATR = ?
    WHERE
        Bot = ?
        AND Symbol = ?
        AND Position = 1;
"""
def update_position_risk(
    bot: str,
    symbol: str,
    highest_price_since_entry: float,
    trail_stop_atr: float,
    position_id: int | None = None,
):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute(
                """
                UPDATE Positions
                SET Highest_Price_Since_Entry = ?, Trail_Stop_ATR = ?
                WHERE Id = ? AND Position = 1
                """,
                (float(highest_price_since_entry), float(trail_stop_atr), position_id),
            )
        else:
            connection.execute(
                sql_update_position_risk,
                (
                    float(highest_price_since_entry),
                    float(trail_stop_atr),
                    bot,
                    symbol,
                ),
            )

sql_set_position_take_profit_1 = """
    UPDATE Positions
    SET 
        Take_Profit_1 = ?
    WHERE
        Bot = ? 
        AND Symbol = ? 
        AND Position = 1;        
"""
def set_position_take_profit_1(bot: str, symbol: str, take_profit_1: int, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute("UPDATE Positions SET Take_Profit_1 = ? WHERE Id = ? AND Position = 1", (take_profit_1, position_id))
        else:
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
def set_position_take_profit_2(bot: str, symbol: str, take_profit_2: int, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute("UPDATE Positions SET Take_Profit_2 = ? WHERE Id = ? AND Position = 1", (take_profit_2, position_id))
        else:
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
def set_position_take_profit_3(bot: str, symbol: str, take_profit_3: int, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute("UPDATE Positions SET Take_Profit_3 = ? WHERE Id = ? AND Position = 1", (take_profit_3, position_id))
        else:
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
def set_position_take_profit_4(bot: str, symbol: str, take_profit_4: int, position_id: int | None = None):
    connection = _get_conn()
    with connection:
        if position_id is not None:
            connection.execute("UPDATE Positions SET Take_Profit_4 = ? WHERE Id = ? AND Position = 1", (take_profit_4, position_id))
        else:
            connection.execute(sql_set_position_take_profit_4, (take_profit_4, bot, symbol))

sql_delete_all_positions = "DELETE FROM Positions;"
def delete_all_positions():
    connection = _get_conn() 
    with connection:
        connection.execute(sql_delete_all_positions)

sql_delete_positions_not_top_rank = "DELETE FROM Positions where Position = 0 and Symbol not in (select Symbol from Symbols_By_Market_Phase);"
def delete_positions_not_top_rank():
    connection = _get_conn()
    with connection:
        connection.execute(sql_delete_positions_not_top_rank)
    
sql_delete_all_positions_not_open = "DELETE FROM Positions where Position = 0"
def delete_all_positions_not_open():
    connection = _get_conn()
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

def get_symbol_blacklist():
    connection = _get_conn()
    sql_get_symbol_blacklist = "SELECT * FROM Blacklist;"
    return pd.read_sql(sql_get_symbol_blacklist, connection) 

sql_delete_all_blacklist = "DELETE FROM Blacklist;"
def delete_all_blacklist():
    connection = _get_conn()
    with connection:
        connection.execute(sql_delete_all_blacklist)

sql_delete_id_blacklist = "DELETE FROM Blacklist WHERE Id = ?;"
def delete_id_blacklist(ids: list):
    connection = _get_conn()
    with connection:
        connection.executemany(sql_delete_id_blacklist, [(id,) for id in ids])

sql_add_blacklist = "INSERT OR REPLACE INTO Blacklist (Symbol) VALUES (?);"
def add_blacklist(symbols: list):
    connection = _get_conn()
    with connection:
        connection.executemany(sql_add_blacklist, [(symbol,) for symbol in symbols])

def update_blacklist(df_blacklist):
    connection = _get_conn()
    """Efficiently update the Blacklist table without replacing it entirely."""
    cursor = connection.cursor()

    # Ensure 'Id' exists in the DataFrame, or use None to allow auto-increment
    df_blacklist = df_blacklist.reset_index()  # Ensure 'Id' is a column, not index

    # Convert Symbol to string, strip spaces, and ensure None/empty values are removed
    df_blacklist["Symbol"] = df_blacklist["Symbol"].astype(str).str.strip()
    
    # Explicitly replace invalid values with None
    df_blacklist["Symbol"] = df_blacklist["Symbol"].apply(
        lambda x: None if x in ["", "None", "nan", "NaN"] else x
    )
    
    # Drop rows where Symbol is still None or NaN
    df_blacklist = df_blacklist.dropna(subset=["Symbol"])

    # Convert NaN Ids to None (SQLite will auto-generate them)
    df_blacklist["Id"] = df_blacklist["Id"].apply(lambda x: None if pd.isna(x) else int(x))

    # Prepare data for batch execution
    data = list(df_blacklist[["Id", "Symbol"]].itertuples(index=False, name=None))

    if not data:  # Ensure there's valid data to insert
        import streamlit as st
        st.warning("No valid symbols to save.")
        return

    try: 
        # Use `executemany()` for efficiency
        cursor.executemany(
            """
            INSERT INTO Blacklist (Id, Symbol)
            VALUES (?, ?)
            ON CONFLICT(Id) DO UPDATE SET Symbol = excluded.Symbol;
            """,
            data
        )

        connection.commit()

        import streamlit as st
        st.success("Blacklist changes saved")
        time.sleep(2)

    except sqlite3.IntegrityError:
        import streamlit as st
        st.error("Symbol already exists!")

def delete_from_blacklist(df_blacklist):
    connection = _get_conn()
    """Delete symbols from the Blacklist table."""
    cursor = connection.cursor()

    # Delete rows based on the Symbol column
    cursor.executemany(
        "DELETE FROM Blacklist WHERE Symbol = ?",
        [(symbol,) for symbol in df_blacklist["Symbol"]]
    )

    connection.commit()
    
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
INSERT OR IGNORE INTO Strategies (Id, Name, Backtest_Optimize, BTC_Strategy) VALUES ('hma_rsi_linreg', 'HMA RSI LINREG', 1, 1);
"""

sql_get_all_strategies = "SELECT * FROM Strategies;"
def get_all_strategies():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_strategies, connection)

sql_get_strategies_for_main = "SELECT * FROM Strategies where Main_Strategy = 1;"
def get_strategies_for_main():
    connection = _get_conn()
    return pd.read_sql(sql_get_strategies_for_main, connection)

sql_get_strategies_for_btc = "SELECT * FROM Strategies where BTC_Strategy = 1;"
def get_strategies_for_btc():
    connection = _get_conn()
    return pd.read_sql(sql_get_strategies_for_btc, connection)

sql_get_strategy_name = "SELECT Name FROM Strategies where Id = ?;"
def get_strategy_name(strategy_id: str):
    connection = _get_conn()
    df = pd.read_sql(sql_get_strategy_name, connection, params=(strategy_id,))
    if df.empty:
        result = ""
    else:
        result = df.iloc[0, 0]
    return result

sql_get_strategy_by_id = "SELECT * FROM Strategies where Id = ?;"
def get_strategy_by_id(strategy_id: str):
    connection = _get_conn()
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
        Max_Drawdown_Perc REAL,
        Trades INTEGER,
        Win_Rate_Perc REAL,
        Best_Trade_Perc REAL,
        Worst_Trade_Perc REAL,
        Avg_Trade_Perc REAL,
        Max_Trade_Duration TEXT,
        Avg_Trade_Duration TEXT,
        Profit_Factor REAL,
        Expectancy_Perc REAL,
        SQN REAL,
        Kelly_Criterion REAL,
        Trading_Approved INTEGER NOT NULL DEFAULT 0,
        Trading_Rejection_Reasons TEXT,
        Quality_Score REAL,
        Quality_Grade TEXT,
        Backtest_Config_JSON TEXT,
        Strategy_Id TEXT,
        CONSTRAINT symbol_time_frame_strategy_unique UNIQUE (Symbol, Time_Frame, Strategy_Id)
    );
"""

sql_get_all_backtesting_results = """
    SELECT br.Symbol,
           br.Time_Frame,
           br.Return_Perc,
           br.BuyHold_Return_Perc,
           br.Backtest_Start_Date,
           br.Backtest_End_Date,
           br.Max_Drawdown_Perc,
           br.Trades,
           br.Win_Rate_Perc,
           br.Best_Trade_Perc,
           br.Worst_Trade_Perc,
           br.Avg_Trade_Perc,
           br.Max_Trade_Duration,
           br.Avg_Trade_Duration,
           br.Profit_Factor,
           br.Expectancy_Perc,
           br.SQN,
           br.Kelly_Criterion,
           br.Quality_Score,
           br.Quality_Grade,
           br.Trading_Approved,
           br.Trading_Rejection_Reasons,
           br.Backtest_Config_JSON,
           br.Strategy_Id,
           st.Name as Strategy_Name,
           br.Ema_Fast,
           br.Ema_Slow
    FROM Backtesting_Results AS br
    JOIN Strategies AS st ON br.Strategy_Id = st.Id
    ORDER BY br.Symbol, st.Name;
"""
def get_all_backtesting_results():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_backtesting_results, connection)
    # return pd.read_sql(sql_get_all_backtesting_results, connection)

def get_backtesting_results_for_ai():
    connection = _get_conn()
    sql = """
        SELECT
            br.Symbol,
            br.Time_Frame,
            br.Return_Perc,
            br.BuyHold_Return_Perc,
            br.Trades,
            br.Profit_Factor,
            br.SQN,
            br.Max_Drawdown_Perc,
            br.Win_Rate_Perc,
            br.Expectancy_Perc,
            br.Kelly_Criterion,
            br.Strategy_Id,
            st.Name AS Strategy_Name
        FROM Backtesting_Results br
        LEFT JOIN Strategies st ON br.Strategy_Id = st.Id
    """
    return pd.read_sql(sql, connection)
    
sql_get_backtesting_results_by_symbol_timeframe_strategy = """
    SELECT be.*, st.Name
    FROM Backtesting_Results as be
    JOIN Strategies as st on be.Strategy_Id = st.Id
    WHERE
        be.Symbol = ?
        AND be.Time_Frame = ?
        AND be.Strategy_Id = ?;
"""
def get_backtesting_results_by_symbol_timeframe_strategy(symbol: str, time_frame: str, strategy_id: str):
    connection = _get_conn()
    return pd.read_sql(sql_get_backtesting_results_by_symbol_timeframe_strategy, connection, params=(symbol, time_frame, strategy_id))

sql_add_backtesting_results = """
    INSERT OR REPLACE INTO Backtesting_Results (
        Symbol, Ema_Fast, Ema_Slow, Time_Frame, Return_Perc, BuyHold_Return_Perc, Backtest_Start_Date, Backtest_End_Date,
        Max_Drawdown_Perc, Trades, Win_Rate_Perc, Best_Trade_Perc, Worst_Trade_Perc, Avg_Trade_Perc, Max_Trade_Duration, Avg_Trade_Duration,
        Profit_Factor, Expectancy_Perc, SQN, Kelly_Criterion, Trading_Approved, Trading_Rejection_Reasons, Quality_Score, Quality_Grade, Backtest_Config_JSON, Strategy_Id
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
def add_backtesting_results(timeframe: str,
                            symbol: str,
                            ema_fast: int,
                            ema_slow: int,
                            return_perc: float,
                            buy_hold_return_perc: float,
                            backtest_start_date: str,
                            backtest_end_date: str,
                            max_drawdown_perc: float,
                            trades: int,
                            win_rate_perc: float,
                            best_trade_perc: float,
                            worst_trade_perc: float,
                            avg_trade_perc: float,
                            max_trade_duration: str,
                            avg_trade_duration: str,
                            profit_factor: float,
                            expectancy_perc: float,
                            sqn: float,
                            kelly_criterion: float,
                            strategy_Id: str,
                            trading_approved: bool = False,
                            trading_rejection_reasons: str = "",
                            quality_score: float = None,
                            quality_grade: str = "",
                            backtest_config_json: str = ""):
    connection = _get_conn()
    with connection:
        connection.execute(sql_add_backtesting_results, (str(symbol), 
                                              int(ema_fast), 
                                              int(ema_slow), 
                                              str(timeframe), 
                                              float(return_perc), 
                                              float(buy_hold_return_perc), 
                                              str(backtest_start_date),
                                              str(backtest_end_date),
                                              float(max_drawdown_perc) if max_drawdown_perc is not None else None,
                                              int(trades) if trades is not None else None,
                                              float(win_rate_perc) if win_rate_perc is not None else None,
                                              float(best_trade_perc) if best_trade_perc is not None else None,
                                              float(worst_trade_perc) if worst_trade_perc is not None else None,
                                              float(avg_trade_perc) if avg_trade_perc is not None else None,
                                              str(max_trade_duration) if max_trade_duration is not None else None,
                                              str(avg_trade_duration) if avg_trade_duration is not None else None,
                                              float(profit_factor) if profit_factor is not None else None,
                                              float(expectancy_perc) if expectancy_perc is not None else None,
                                              float(sqn) if sqn is not None else None,
                                              float(kelly_criterion) if kelly_criterion is not None else None,
                                              1 if trading_approved else 0,
                                              str(trading_rejection_reasons) if trading_rejection_reasons is not None else "",
                                              float(quality_score) if quality_score is not None else None,
                                              str(quality_grade) if quality_grade is not None else "",
                                              str(backtest_config_json) if backtest_config_json is not None else "",
                                              str(strategy_Id)
                                              )
                            )

sql_update_backtesting_approval = """
    UPDATE Backtesting_Results
    SET
        Trading_Approved = ?,
        Trading_Rejection_Reasons = ?
    WHERE
        Symbol = ?
        AND Time_Frame = ?
        AND Strategy_Id = ?;
"""
def set_backtesting_approval(symbol: str, time_frame: str, strategy_id: str, trading_approved: bool, trading_rejection_reasons: str = ""):
    connection = _get_conn()
    with connection:
        connection.execute(
            sql_update_backtesting_approval,
            (
                1 if trading_approved else 0,
                trading_rejection_reasons or "",
                symbol,
                time_frame,
                strategy_id,
            ),
        )
    
sql_delete_all_backtesting_results = "DELETE FROM Backtesting_Results;"
def delete_all_backtesting_results():
    connection = _get_conn()
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
        "Exit_Reason"	TEXT,
        "Hard_Stop_Loss"	REAL,
        "ATR_Stop_Loss"	REAL,
        "Active_Stop_Loss"	REAL,
        CONSTRAINT "bt_symbol__timeframe_strategy_entrytime_exittime" UNIQUE("Symbol","Time_Frame","Strategy_Id","EntryTime","ExitTime")
);
"""

sql_get_all_backtesting_trades = """
    SELECT bt.Symbol, bt.Time_Frame, bt.ReturnPct, 
    bt.Strategy_Id, st.Name as Strategy_Name, 
    bt.EntryTime, bt.ExitTime, bt.EntryPrice, bt.ExitPrice, bt.PnL, bt.Duration, bt.Exit_Reason,
    bt.Hard_Stop_Loss, bt.ATR_Stop_Loss, bt.Active_Stop_Loss
    FROM Backtesting_Trades AS bt
    JOIN Strategies AS st ON bt.Strategy_Id = st.Id
    ORDER BY bt.Symbol, st.Name;
"""
def get_all_backtesting_trades():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_backtesting_trades, connection)

sql_add_backtesting_trade = """
    INSERT OR REPLACE INTO Backtesting_Trades (
        Symbol, Time_Frame, Strategy_Id, EntryBar, ExitBar, EntryPrice, ExitPrice, PnL, ReturnPct, EntryTime, ExitTime, Duration, Exit_Reason,
        Hard_Stop_Loss, ATR_Stop_Loss, Active_Stop_Loss
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
def add_backtesting_trade(symbol: str, timeframe: str, strategy_id: str, entry_bar: int, exit_bar: int, entry_price: float, exit_price: float, pnl: float, return_pct: float, entry_time: str, exit_time: str, duration: str, exit_reason: str = "", hard_stop_loss=None, atr_stop_loss=None, active_stop_loss=None):
    connection = _get_conn()
    def _nullable_float(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if value == "":
            return None
        try:
            if math.isnan(float(value)):
                return None
        except Exception:
            return None
        return float(value)

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
            str(duration),
            str(exit_reason),
            _nullable_float(hard_stop_loss),
            _nullable_float(atr_stop_loss),
            _nullable_float(active_stop_loss),
        ))

def delete_backtesting_trades_symbol_timeframe_strategy(symbol, timeframe, strategy_id):
    connection = _get_conn()
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
def get_all_symbols_to_calc():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_symbols_to_calc, connection)

#    
sql_get_symbols_to_calc_by_calc_completed = """
    SELECT Symbol 
    FROM Symbols_To_Calc 
    WHERE
        Calc_Completed = ?;
"""
def get_symbols_to_calc_by_calc_completed(completed: int):
    connection = _get_conn()
    return pd.read_sql(sql_get_symbols_to_calc_by_calc_completed, connection, params=(completed,))
    
#    
sql_set_symbols_to_calc_completed = """
    UPDATE Symbols_To_Calc 
    SET Calc_Completed = 1,
        Date_Completed = datetime('now')
    WHERE
        Symbol = ?;
"""
def set_symbols_to_calc_completed(symbol: str):
    connection = _get_conn()
    with connection:
        connection.execute(sql_set_symbols_to_calc_completed, (symbol,))
    
sql_delete_symbols_to_calc_completed = """
    DELETE FROM Symbols_To_Calc 
    WHERE Calc_Completed = 1;
"""
def delete_symbols_to_calc_completed():
    connection = _get_conn()
    with connection:
        connection.execute(sql_delete_symbols_to_calc_completed)

sql_delete_all_symbols_to_calc = "DELETE FROM Symbols_To_Calc;"
def delete_all_symbols_to_calc():
    connection = _get_conn()
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
    connection = _get_conn()
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
    connection = _get_conn()
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
def symbols_by_market_phase_Historical_get_symbols_days_at_top():
    connection = _get_conn()
    return pd.read_sql(sql_symbols_by_market_phase_Historical_get_symbols_days_at_top, connection)


sql_get_all_symbols_by_market_phase = """
    SELECT
        Id,
        Rank,
        Symbol,
        Price,
        DSMA50,
        DSMA200,
        Market_Phase,
        Perc_Above_DSMA50,
        Perc_Above_DSMA200
    FROM Symbols_By_Market_Phase;
"""
def get_all_symbols_by_market_phase():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_symbols_by_market_phase, connection, index_col="Id")

sql_get_top_performers_trading_status = """
    SELECT
        mp.Rank,
        mp.Symbol,
        br.Strategy_Id,
        st.Name AS Strategy_Name,
        br.Time_Frame,
        br.Trading_Approved,
        br.Trading_Rejection_Reasons
    FROM Symbols_By_Market_Phase mp
    JOIN Backtesting_Results br ON br.Symbol = mp.Symbol
    LEFT JOIN Strategies st ON st.Id = br.Strategy_Id
    WHERE br.Strategy_Id = ?
    ORDER BY
        mp.Rank ASC,
        CASE br.Time_Frame
            WHEN '1d' THEN 1
            WHEN '4h' THEN 2
            WHEN '1h' THEN 3
            ELSE 4
        END,
        br.Time_Frame ASC;
"""
def get_top_performers_trading_status(strategy_id: str):
    connection = _get_conn()
    if isinstance(strategy_id, (list, tuple, set)):
        strategy_ids = [str(value) for value in strategy_id if str(value).strip()]
        if not strategy_ids:
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in strategy_ids)
        sql = sql_get_top_performers_trading_status.replace(
            "WHERE br.Strategy_Id = ?",
            f"WHERE br.Strategy_Id IN ({placeholders})",
        )
        return pd.read_sql(sql, connection, params=tuple(strategy_ids))
    return pd.read_sql(sql_get_top_performers_trading_status, connection, params=(strategy_id,))
    
sql_get_symbols_from_symbols_by_market_phase = "SELECT Symbol FROM Symbols_By_Market_Phase;"
def get_symbols_from_symbols_by_market_phase():
    connection = _get_conn()
    return pd.read_sql(sql_get_symbols_from_symbols_by_market_phase, connection)

sql_get_rank_from_symbols_by_market_phase_by_symbol = """
    SELECT Rank 
    FROM Symbols_By_Market_Phase
    WHERE Symbol = ?;
"""
def get_rank_from_symbols_by_market_phase_by_symbol(symbol: str):
    connection = _get_conn()
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
def insert_symbols_by_market_phase(
    symbol: str,
    price: float,
    dsma50: float,
    dsma200: float,
    market_phase: str,
    perc_above_dsma50: float,
    perc_above_dsma200: float,
    rank: int,
):
    connection = _get_conn()
    with connection:
        connection.execute(
            sql_insert_symbols_by_market_phase,
            (
                symbol,
                price,
                dsma50,
                dsma200,
                market_phase,
                perc_above_dsma50,
                perc_above_dsma200,
                rank,
            ),
        )

sql_insert_symbols_by_market_phase_historical = """
    INSERT INTO Symbols_By_Market_Phase_Historical 
        (Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200, Rank, Date_Inserted)
    SELECT Symbol, Price, DSMA50, DSMA200, Market_Phase, Perc_Above_DSMA50, Perc_Above_DSMA200, Rank, ?
    FROM Symbols_By_Market_Phase;
"""
def insert_symbols_by_market_phase_historical(date_inserted: str):
    connection = _get_conn()
    with connection:
        connection.execute(sql_insert_symbols_by_market_phase_historical,(date_inserted,))


sql_delete_all_symbols_by_market_phase = "DELETE FROM Symbols_By_Market_Phase;"
def delete_all_symbols_by_market_phase():
    connection = _get_conn()
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
def get_distinct_symbol_by_market_phase_and_positions():
    connection = _get_conn()
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
def get_all_users():
    connection = _get_conn()
    return pd.read_sql(sql_get_all_users, connection, index_col="username")

sql_get_user_by_username = "SELECT * FROM Users WHERE username = ?;"
def get_user_by_username(username: str):
    connection = _get_conn()
    return pd.read_sql(sql_get_user_by_username, connection, params=(username,))

sql_add_user = """
    INSERT OR REPLACE INTO Users (
        username, email, name, password
        ) 
        VALUES (?, ?, ? ,?);
"""
def add_user(username: str, email: str, name: str, password: str):
    connection = _get_conn()
    with connection:
        connection.execute(sql_add_user, (username, email, name, password))

sql_update_user_password = """
    UPDATE Users
    SET
        password = ?
    WHERE 
        username = ?
"""
def update_user_password(username: str, password: str):
    connection = _get_conn()
    with connection:
        connection.execute(sql_update_user_password, (password, username,))

sql_update_username = """
    UPDATE Users
    SET username = ?
    WHERE username = ?
"""

def update_username(old_username: str, new_username: str):
    connection = _get_conn()
    with connection:
        connection.execute(sql_update_username, (new_username, old_username))

def update_user_profile(old_username: str, new_username: str, new_email: str) -> int:
    """
    Atomically update username and email. Assumes Users table has columns:
    username (PRIMARY KEY or UNIQUE), email (TEXT).
    """
    sql = """
        UPDATE Users
        SET username = ?, email = ?
        WHERE username = ?
    """
    conn = _get_conn()
    try:
        with conn:
            cur = conn.execute(sql, (new_username, new_email, old_username))
            return cur.rowcount
    except sqlite3.IntegrityError as e:
        # Likely a UNIQUE constraint violation on username
        return 0

def update_email(username: str, email: str) -> int:
    sql = """
        UPDATE Users
        SET email = ?
        WHERE username = ?
    """
    conn = _get_conn()
    with conn:
        cur = conn.execute(sql, (email, username))
        return cur.rowcount

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
def add_balances(balances: pd.DataFrame):
    connection = _get_conn()
    if balances.empty:
        return
    # convert dataframe to a list of tuples
    data = list(balances.to_records(index=False))
    for row in data:
        with connection:
            connection.execute(sql_add_balances, row)
    
def get_asset_balances_last_n_days(n_days):
    connection = _get_conn()
    sql_get_balances_last_n_days = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE Date >= date('now', ? || ' days')
        AND Balance_USD > 1;
    """
    params = (str(-n_days),)  # Convert n_days to a negative string for date subtraction
    return pd.read_sql(sql_get_balances_last_n_days, connection, params=params)

def get_asset_balances_ytd():
    connection = _get_conn()
    sql_get_balances_ytd = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE strftime('%Y', Date) = strftime('%Y', 'now')
        AND Balance_USD > 1;
    """
    return pd.read_sql(sql_get_balances_ytd, connection)

def get_asset_balances_all_time():
    connection = _get_conn()
    sql_get_balances_all_time = """  
        SELECT Date, Asset, ROUND(Balance_USD, 2) as Balance_USD
        FROM Balances
        WHERE Balance_USD > 1;
    """
    return pd.read_sql(sql_get_balances_all_time, connection)

def get_total_balance_last_n_days(n_days, asset):
    connection = _get_conn()
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

def get_total_balance_ytd(asset):
    connection = _get_conn()
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

def get_total_balance_all_time(asset):
    connection = _get_conn()
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
def get_last_date_from_balances():
    connection = _get_conn()
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
def get_all_signals_log(num_rows):
    connection = _get_conn()
    return pd.read_sql(sql_get_all_signals_log, connection, params=(num_rows,))

sql_add_signal_log = """
    INSERT INTO Signals_Log (Date, Signal, Signal_Message, Symbol, Notes) VALUES (?, ?, ?, ?, ?);
"""
def add_signal_log(date: datetime, signal: str, signal_message: str, symbol: str, notes: str):
    connection = _get_conn()
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
def lock_value(position_id, buy_order_id, amount):
    connection = _get_conn()
    with connection:
        connection.execute("INSERT INTO Locked_Values (Position_Id, Buy_Order_Id, Locked_Amount) VALUES (?, ?, ?)", (str(position_id), buy_order_id, amount))
    

# Function to release a value when the position is fully closed
def release_value(position_id):
    connection = _get_conn()
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Position_Id = ?"
    with connection:
        connection.execute(sql, (str(position_id),))

# Function to release all locked values
def release_all_values():
    connection = _get_conn()
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Released = 0"
    with connection:
        connection.execute(sql)

# Function to release a value when the position is fully closed
def release_locked_value_by_id(id):
    connection = _get_conn()
    sql = "UPDATE Locked_Values SET Released_At = CURRENT_TIMESTAMP, Released = 1 WHERE Id = ?"
    with connection:
        connection.execute(sql, (str(id),))

def get_total_locked_values():
    connection = _get_conn()
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

def get_all_locked_values():
    connection = _get_conn()
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

# Locked_Values
sql_create_settings_table = """
    CREATE TABLE IF NOT EXISTS Settings (
        name TEXT PRIMARY KEY,
        value TEXT,
        comment TEXT
    );
"""

sql_create_backtesting_settings_table = """
    CREATE TABLE IF NOT EXISTS Backtesting_Settings (
        Id INTEGER PRIMARY KEY,
        Commission_Value REAL NOT NULL,
        Cash_Value REAL NOT NULL,
        Maximize TEXT NOT NULL,
        Use_Intraday_Current_Timeframe_Market_Phase_Filter INTEGER NOT NULL DEFAULT 1,
        Market_Phase_1h_SMA_Fast INTEGER NOT NULL DEFAULT 50,
        Market_Phase_1h_SMA_Slow INTEGER NOT NULL DEFAULT 200,
        Market_Phase_4h_SMA_Fast INTEGER NOT NULL DEFAULT 50,
        Market_Phase_4h_SMA_Slow INTEGER NOT NULL DEFAULT 200,
        Market_Phase_1d_SMA_Fast INTEGER NOT NULL DEFAULT 50,
        Market_Phase_1d_SMA_Slow INTEGER NOT NULL DEFAULT 200,
        Buy_Hold_Start_Mode TEXT NOT NULL DEFAULT 'indicator_warmup',
        Strategy_Quality_Return_Weight REAL NOT NULL DEFAULT 20,
        Strategy_Quality_Risk_Weight REAL NOT NULL DEFAULT 25,
        Strategy_Quality_Risk_Adjusted_Weight REAL NOT NULL DEFAULT 20,
        Strategy_Quality_Trade_Quality_Weight REAL NOT NULL DEFAULT 20,
        Strategy_Quality_Robustness_Weight REAL NOT NULL DEFAULT 15
    );
"""

sql_create_Approval_Rule_Definitions_table = """
    CREATE TABLE IF NOT EXISTS Approval_Rule_Definitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

sql_create_Backtest_Approval_Rules_table = """
    CREATE TABLE IF NOT EXISTS Backtest_Approval_Rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_id INTEGER NOT NULL,
        rule_value REAL NOT NULL,
        timeframe TEXT NULL,
        enabled INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(rule_id) REFERENCES Approval_Rule_Definitions(id),
        UNIQUE(rule_id, timeframe)
    );
"""

sql_seed_default_approval_rules = """
    INSERT OR IGNORE INTO Approval_Rule_Definitions (rule_name, description) VALUES
      ('Min_Trades', 'Minimum number of completed trades. Helps avoid overfitting to a tiny sample size (timeframe-specific thresholds).'),
      ('SQN_min', 'Minimum System Quality Number (SQN). Screens for robustness beyond raw return (higher is better).'),
      ('Return_Min_Pct', 'Minimum total return percentage over the backtest period (floor for profitability).'),
      ('Profit_Factor_min', 'Hard floor for Profit Factor (gross profit / gross loss). Below this, the strategy is rejected.'),
      ('Quality_Grade_Min', 'Minimum strategy quality grade required for trading approval. Screening profiles: C = baseline, B = quality-focused, A = top-tier only.'),
      ('Quality_Score_Min', 'Minimum strategy quality score from 0 to 100. Optional numeric alternative to Quality_Grade_Min.'),
      ('Max_Drawdown_Pct', 'Maximum allowed absolute drawdown percentage. Limits worst peak-to-trough equity loss.'),
      ('Require_Drawdown_Limit_When_Underperform_BuyHold',
       'If the strategy underperforms Buy & Hold, enforce the drawdown limit (1=enable, 0=disable).');

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 1.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'SQN_min';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 1.0, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'SQN_min')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 0.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Return_Min_Pct';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 0.0, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Return_Min_Pct')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 1.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Profit_Factor_min';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 1.0, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Profit_Factor_min')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 'C', NULL, 1
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Quality_Grade_Min';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 'C', enabled = 1
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Quality_Grade_Min')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 70.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Quality_Score_Min';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 70.0, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Quality_Score_Min')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 60, '1h', 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Min_Trades';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 60, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Min_Trades')
      AND timeframe = '1h';

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 30, '4h', 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Min_Trades';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 30, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Min_Trades')
      AND timeframe = '4h';

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 15, '1d', 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Min_Trades';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 15, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Min_Trades')
      AND timeframe = '1d';

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 45.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Max_Drawdown_Pct';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 45.0, enabled = 0
    WHERE rule_id = (SELECT id FROM Approval_Rule_Definitions WHERE rule_name = 'Max_Drawdown_Pct')
      AND timeframe IS NULL;

    INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
    SELECT id, 1.0, NULL, 0
    FROM Approval_Rule_Definitions
    WHERE rule_name = 'Require_Drawdown_Limit_When_Underperform_BuyHold';

    UPDATE Backtest_Approval_Rules
    SET rule_value = 1.0, enabled = 0
    WHERE rule_id = (
        SELECT id FROM Approval_Rule_Definitions
        WHERE rule_name = 'Require_Drawdown_Limit_When_Underperform_BuyHold'
    )
    AND timeframe IS NULL;
"""

DEFAULT_BACKTESTING_SETTINGS = {
    "Commission_Value": 0.005,
    "Cash_Value": 10000.0,
    "Maximize": "SQN",
    "Use_Intraday_Current_Timeframe_Market_Phase_Filter": 1,
    "Market_Phase_1h_SMA_Fast": 50,
    "Market_Phase_1h_SMA_Slow": 200,
    "Market_Phase_4h_SMA_Fast": 50,
    "Market_Phase_4h_SMA_Slow": 200,
    "Market_Phase_1d_SMA_Fast": 50,
    "Market_Phase_1d_SMA_Slow": 200,
    "Buy_Hold_Start_Mode": "indicator_warmup",
    "Strategy_Quality_Return_Weight": 20.0,
    "Strategy_Quality_Risk_Weight": 25.0,
    "Strategy_Quality_Risk_Adjusted_Weight": 20.0,
    "Strategy_Quality_Trade_Quality_Weight": 20.0,
    "Strategy_Quality_Robustness_Weight": 15.0,
}

sql_create_job_schedules_table = """
    CREATE TABLE IF NOT EXISTS Job_Schedules (
        name TEXT PRIMARY KEY,
        script TEXT NOT NULL,
        script_args TEXT,
        cadence TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        description TEXT,
        last_run TEXT
    );
"""

sql_create_backtesting_jobs_table = """
    CREATE TABLE IF NOT EXISTS Backtesting_Jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        optimize INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'queued',
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        return_code INTEGER,
        log_path TEXT,
        error_message TEXT
    );
"""

sql_create_monte_carlo_jobs_table = """
    CREATE TABLE IF NOT EXISTS Monte_Carlo_Jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        method TEXT NOT NULL,
        scenarios INTEGER NOT NULL,
        seed INTEGER NOT NULL DEFAULT 42,
        status TEXT NOT NULL DEFAULT 'queued',
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        return_code INTEGER,
        log_path TEXT,
        error_message TEXT
    );
"""

sql_create_monte_carlo_results_table = """
    CREATE TABLE IF NOT EXISTS Monte_Carlo_Results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Symbol TEXT NOT NULL,
        Time_Frame TEXT NOT NULL,
        Strategy_Id TEXT NOT NULL,
        Method TEXT NOT NULL,
        Scenarios INTEGER NOT NULL,
        Valid_Scenarios INTEGER NOT NULL,
        Seed INTEGER NOT NULL,
        Robustness_Score REAL,
        Interpretation TEXT,
        Net_Profit_Original REAL,
        Net_Profit_Worst_5 REAL,
        Net_Profit_Median REAL,
        Net_Profit_Best_5 REAL,
        Max_Drawdown_Original REAL,
        Max_Drawdown_Worst_5 REAL,
        Max_Drawdown_Median REAL,
        Max_Drawdown_Best_5 REAL,
        Html_Path TEXT,
        Csv_Path TEXT,
        Json_Path TEXT,
        Result_JSON TEXT,
        Created_At TEXT NOT NULL,
        CONSTRAINT monte_carlo_target_unique UNIQUE (Symbol, Time_Frame, Strategy_Id, Method)
    );
"""

DEFAULT_JOB_SCHEDULES = [
    ("main_1h", "main.py", "1h", "1h", 1, "Trading bot (1h) using the configured strategy."),
    ("main_4h", "main.py", "4h", "4h", 1, "Trading bot (4h) using the configured strategy."),
    ("main_1d", "main.py", "1d", "1d", 1, "Trading bot (1d) using the configured strategy."),
    ("symbol_by_market_phase_1d", "symbol_by_market_phase.py", "1d", "1d", 1, "Rebuild market phase rankings (1d). Calculates market-phase scores and runs backtesting strategies."),
    ("super_rsi_15m", "signals/super_rsi.py", "", "15m", 1, "Super RSI alerts on 15m data."),
    # ("delisting_checker_1h", "delisting_checker.py", "", "1h", 1, "Checks Binance delisting announcements."),
]
        
# PRAGMA
sql_get_pragma_user_version = """
    PRAGMA user_version;
"""
def get_pragma_user_version():
    connection = _get_conn()
    df = pd.read_sql(sql_get_pragma_user_version, connection)
    result = df.iloc[0, 0]
    return result

sql_set_pragma_user_version = """
    PRAGMA user_version = {};
"""
def set_pragma_user_version(version):
    connection = _get_conn()
    with connection:
        query = sql_set_pragma_user_version.format(version)
        connection.execute(query)

# migrate config file to database
def migrate_config_to_db():
    """Migrates settings from config.yaml to SQLite"""
    
    connection = _get_conn()
    cursor = connection.cursor()

    # Check if the settings table already has data
    cursor.execute("SELECT COUNT(*) FROM Settings")
    count = cursor.fetchone()[0]

    if count > 0:
        # print("Settings already exist in database. Skipping migration.")
        return
    
    try:
        config_file = "config.yaml"

        if os.path.exists(config_file):
            with open(config_file, "r") as file:
                try:
                    config = yaml.safe_load(file) or {}
                except yaml.YAMLError as e:
                    print(f"YAML parsing error: {e}")
                    return  # Exit if YAML file is corrupt

            if config:
                for key, value in config.items():
                    # Ensure safe storage of non-string values
                    cursor.execute(
                        "INSERT OR IGNORE INTO Settings (name, value) VALUES (?, ?)", 
                        (key, str(value))
                    )

                connection.commit()
                print(f"Settings migrated from {config_file} to SQLite.")

                # Create a timestamped backup file
                backup_filename = f"config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
                shutil.copy(config_file, backup_filename)
                print(f"Backup created: {backup_filename}")

                # Delete the original config.yaml
                os.remove(config_file)
                print(f"Original {config_file} deleted after successful migration.")

            else:
                print(f"{config_file} not found, skipping migration.")

    except sqlite3.Error as db_error:
        print(f"Database error: {db_error}")

    except Exception as e:
        print(f"Error during migration: {e}")

# create tables
def create_tables():
    connection = _get_conn()
    with connection:

        connection.execute(create_orders_table)
        _ensure_orders_columns(connection)
        connection.execute(sql_create_settings_table)
        remove_obsolete_settings()
        connection.execute(sql_create_positions_table)
        _ensure_positions_columns(connection)
        connection.execute(sql_create_blacklist_table)
        connection.execute(sql_create_backtesting_results_table)
        _ensure_backtesting_results_columns(connection)
        connection.execute(sql_create_backtesting_trades_table)
        _ensure_backtesting_trades_columns(connection)
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
        cursor = connection.execute("SELECT COUNT(*) FROM Users")
        user_count = cursor.fetchone()[0]
        if user_count == 0:
            default_admin_password = "not-financial-advice"
            import streamlit_authenticator as stauth
            hashed_password = stauth.Hasher.hash(default_admin_password)
            connection.execute(sql_users_add_admin, ("admin", "admin@admin.com", "admin", hashed_password))
        # balances
        connection.execute(sql_create_balances_table)
        # signals log
        connection.execute(sql_create_signals_log_table)
        # locked values
        connection.execute(sql_create_locked_values_table)
        # settings
        connection.execute(sql_create_settings_table)
        # backtesting settings
        connection.execute(sql_create_backtesting_settings_table)
        _ensure_backtesting_settings_columns(connection)
        # approval rules
        connection.execute(sql_create_Approval_Rule_Definitions_table)
        connection.execute(sql_create_Backtest_Approval_Rules_table)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtest_rules_rule_id ON Backtest_Approval_Rules(rule_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtest_rules_timeframe ON Backtest_Approval_Rules(timeframe)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtest_rules_enabled ON Backtest_Approval_Rules(enabled)")
        cursor = connection.execute("SELECT COUNT(*) FROM Approval_Rule_Definitions")
        if cursor.fetchone()[0] == 0:
            connection.executescript(sql_seed_default_approval_rules)
        ensure_quality_approval_rules()
        # job schedules
        connection.execute(sql_create_job_schedules_table)
        connection.execute(sql_create_backtesting_jobs_table)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_status_created ON Backtesting_Jobs(status, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_batch ON Backtesting_Jobs(batch_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_target ON Backtesting_Jobs(strategy_id, symbol, timeframe, status)")
        connection.execute(sql_create_monte_carlo_jobs_table)
        connection.execute(sql_create_monte_carlo_results_table)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_status_created ON Monte_Carlo_Jobs(status, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_batch ON Monte_Carlo_Jobs(batch_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_target ON Monte_Carlo_Jobs(strategy_id, symbol, timeframe, method, status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_results_target ON Monte_Carlo_Results(Strategy_Id, Symbol, Time_Frame, Method)")

        # --------
        # apply database scripts updates
        # check changelog version
        version_changelog = general.extract_date_from_local_changelog()
        # Remove "-" characters
        version_changelog = int(version_changelog.replace("-", ""))
        # check changelog version
        version_db = get_pragma_user_version()
        # if database is new then ignore the updates
        if (version_db > 0) and (version_db != version_changelog):
            apply_database_scripts_updates()
        # --------

        # update version on db
        # commented because the db user version must be in the end of database script to make sure everything was ok with the script
        # set_pragma_user_version(version=version_changelog)

def _ensure_backtesting_results_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Backtesting_Results)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Trades": "INTEGER",
        "Win_Rate_Perc": "REAL",
        "Best_Trade_Perc": "REAL",
        "Worst_Trade_Perc": "REAL",
        "Avg_Trade_Perc": "REAL",
        "Max_Trade_Duration": "TEXT",
        "Avg_Trade_Duration": "TEXT",
        "Profit_Factor": "REAL",
        "Expectancy_Perc": "REAL",
        "SQN": "REAL",
        "Kelly_Criterion": "REAL",
        "Max_Drawdown_Perc": "REAL",
        "Trading_Approved": "INTEGER NOT NULL DEFAULT 0",
        "Trading_Rejection_Reasons": "TEXT",
        "Quality_Score": "REAL",
        "Quality_Grade": "TEXT",
        "Backtest_Config_JSON": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Backtesting_Results ADD COLUMN {column_name} {column_type}"
            )

def _ensure_orders_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Orders)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Stop_Type": "TEXT",
        "Stop_Trigger_Price": "REAL",
        "Trail_Stop_ATR_At_Exit": "REAL",
        "Highest_Price_Since_Entry_At_Exit": "REAL",
        "Atr_Params_At_Exit": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Orders ADD COLUMN {column_name} {column_type}"
            )

def _ensure_backtesting_trades_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Backtesting_Trades)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Exit_Reason": "TEXT",
        "Hard_Stop_Loss": "REAL",
        "ATR_Stop_Loss": "REAL",
        "Active_Stop_Loss": "REAL",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Backtesting_Trades ADD COLUMN {column_name} {column_type}"
            )

def _ensure_backtesting_settings_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Backtesting_Settings)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Use_Intraday_Current_Timeframe_Market_Phase_Filter": "INTEGER NOT NULL DEFAULT 1",
        "Market_Phase_1h_SMA_Fast": "INTEGER NOT NULL DEFAULT 50",
        "Market_Phase_1h_SMA_Slow": "INTEGER NOT NULL DEFAULT 200",
        "Market_Phase_4h_SMA_Fast": "INTEGER NOT NULL DEFAULT 50",
        "Market_Phase_4h_SMA_Slow": "INTEGER NOT NULL DEFAULT 200",
        "Market_Phase_1d_SMA_Fast": "INTEGER NOT NULL DEFAULT 50",
        "Market_Phase_1d_SMA_Slow": "INTEGER NOT NULL DEFAULT 200",
        "Buy_Hold_Start_Mode": "TEXT NOT NULL DEFAULT 'indicator_warmup'",
        "Strategy_Quality_Return_Weight": "REAL NOT NULL DEFAULT 20",
        "Strategy_Quality_Risk_Weight": "REAL NOT NULL DEFAULT 25",
        "Strategy_Quality_Risk_Adjusted_Weight": "REAL NOT NULL DEFAULT 20",
        "Strategy_Quality_Trade_Quality_Weight": "REAL NOT NULL DEFAULT 20",
        "Strategy_Quality_Robustness_Weight": "REAL NOT NULL DEFAULT 15",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Backtesting_Settings ADD COLUMN {column_name} {column_type}"
            )

def _ensure_positions_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Positions)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Highest_Price_Since_Entry": "REAL NOT NULL DEFAULT 0",
        "Trail_Stop_ATR": "REAL NOT NULL DEFAULT 0",
        "Strategy_Id": "TEXT",
        "Strategy_Name": "TEXT",
        "Strategy_Params_JSON": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Positions ADD COLUMN {column_name} {column_type}"
            )

    try:
        main_strategies = get_setting("main_strategies")
        if isinstance(main_strategies, str):
            parsed_main_strategies = json.loads(main_strategies)
            main_strategies = parsed_main_strategies if isinstance(parsed_main_strategies, list) else [parsed_main_strategies]
        default_strategy_id = str(main_strategies[0]).strip() if main_strategies else "ema_cross_with_market_phases"
    except Exception:
        default_strategy_id = "ema_cross_with_market_phases"
    try:
        default_strategy_name = get_strategy_name(default_strategy_id)
    except Exception:
        default_strategy_name = default_strategy_id
    connection.execute(
        "UPDATE Positions SET Strategy_Id = ? WHERE Strategy_Id IS NULL OR Strategy_Id = ''",
        (default_strategy_id,),
    )
    connection.execute(
        "UPDATE Positions SET Strategy_Name = ? WHERE Strategy_Name IS NULL OR Strategy_Name = ''",
        (default_strategy_name,),
    )
    rows = connection.execute(
        """
        SELECT Id, Strategy_Id, Ema_Fast, Ema_Slow
        FROM Positions
        WHERE Strategy_Params_JSON IS NULL OR Strategy_Params_JSON = ''
        """
    ).fetchall()
    for position_id, strategy_id, ema_fast, ema_slow in rows:
        connection.execute(
            "UPDATE Positions SET Strategy_Params_JSON = ? WHERE Id = ?",
            (build_strategy_params_json(strategy_id, ema_fast, ema_slow), position_id),
        )

def _ensure_orders_columns(connection):
    cursor = connection.execute("PRAGMA table_info(Orders)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "Stop_Type": "TEXT",
        "Stop_Trigger_Price": "REAL",
        "Trail_Stop_ATR_At_Exit": "REAL",
        "Highest_Price_Since_Entry_At_Exit": "REAL",
        "Atr_Params_At_Exit": "TEXT",
        "Strategy_Id": "TEXT",
        "Strategy_Params_JSON": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_cols:
            connection.execute(
                f"ALTER TABLE Orders ADD COLUMN {column_name} {column_type}"
            )
    rows = connection.execute(
        """
        SELECT Id, Strategy_Id, Ema_Fast, Ema_Slow
        FROM Orders
        WHERE Strategy_Params_JSON IS NULL OR Strategy_Params_JSON = ''
        """
    ).fetchall()
    for order_id, strategy_id, ema_fast, ema_slow in rows:
        connection.execute(
            "UPDATE Orders SET Strategy_Params_JSON = ? WHERE Id = ?",
            (build_strategy_params_json(strategy_id, ema_fast, ema_slow), order_id),
        )

def apply_database_scripts_updates():
    connection = _get_conn()
    
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

# Signal schedules
def ensure_job_schedules():
    connection = _get_conn()
    with connection:
        connection.execute(sql_create_job_schedules_table)
        for name, script, script_args, cadence, enabled, description in DEFAULT_JOB_SCHEDULES:
            connection.execute(
                "INSERT OR IGNORE INTO Job_Schedules (name, script, script_args, cadence, enabled, description) VALUES (?, ?, ?, ?, ?, ?)",
                (name, script, script_args, cadence, enabled, description),
            )

def ensure_backtesting_jobs():
    connection = _get_conn()
    with connection:
        connection.execute(sql_create_backtesting_jobs_table)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_status_created ON Backtesting_Jobs(status, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_batch ON Backtesting_Jobs(batch_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_backtesting_jobs_target ON Backtesting_Jobs(strategy_id, symbol, timeframe, status)")

def ensure_monte_carlo_tables():
    connection = _get_conn()
    with connection:
        connection.execute(sql_create_monte_carlo_jobs_table)
        connection.execute(sql_create_monte_carlo_results_table)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_status_created ON Monte_Carlo_Jobs(status, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_batch ON Monte_Carlo_Jobs(batch_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_jobs_target ON Monte_Carlo_Jobs(strategy_id, symbol, timeframe, method, status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_monte_carlo_results_target ON Monte_Carlo_Results(Strategy_Id, Symbol, Time_Frame, Method)")

def enqueue_backtesting_jobs(jobs, batch_id: str = ""):
    import uuid

    connection = _get_conn()
    batch_id = batch_id or uuid.uuid4().hex
    created_at = datetime.utcnow().isoformat(timespec="seconds")
    queued = []
    skipped = []

    with connection:
        connection.execute(sql_create_backtesting_jobs_table)
        for job in jobs:
            strategy_id = str(job["strategy_id"]).strip()
            symbol = str(job["symbol"]).strip().upper()
            timeframe = str(job["timeframe"]).strip()
            optimize = 1 if bool(job.get("optimize")) else 0
            cursor = connection.execute(
                """
                SELECT id
                FROM Backtesting_Jobs
                WHERE strategy_id = ?
                  AND symbol = ?
                  AND timeframe = ?
                  AND status IN ('queued', 'running')
                LIMIT 1
                """,
                (strategy_id, symbol, timeframe),
            )
            existing = cursor.fetchone()
            if existing:
                skipped.append(
                    {
                        "id": existing[0],
                        "strategy_id": strategy_id,
                        "symbol": symbol,
                        "timeframe": timeframe,
                    }
                )
                continue

            cursor = connection.execute(
                """
                INSERT INTO Backtesting_Jobs (
                    batch_id, strategy_id, symbol, timeframe, optimize, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?)
                """,
                (batch_id, strategy_id, symbol, timeframe, optimize, created_at),
            )
            queued.append(
                {
                    "id": cursor.lastrowid,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                }
            )

    return {"batch_id": batch_id, "queued": queued, "skipped": skipped}

def get_backtesting_jobs(limit: int = 50):
    connection = _get_conn()
    connection.execute(sql_create_backtesting_jobs_table)
    return pd.read_sql(
        """
        SELECT
            id,
            batch_id,
            strategy_id,
            symbol,
            timeframe,
            optimize,
            status,
            created_at,
            started_at,
            finished_at,
            return_code,
            log_path,
            error_message
        FROM Backtesting_Jobs
        ORDER BY
            CASE status
                WHEN 'running' THEN 0
                WHEN 'queued' THEN 1
                WHEN 'failed' THEN 2
                ELSE 3
            END,
            COALESCE(started_at, created_at) DESC,
            id DESC
        LIMIT ?
        """,
        connection,
        params=(int(limit),),
    )

def get_backtesting_job_counts():
    connection = _get_conn()
    connection.execute(sql_create_backtesting_jobs_table)
    return pd.read_sql(
        """
        SELECT status, COUNT(*) AS count
        FROM Backtesting_Jobs
        GROUP BY status
        """,
        connection,
    )

def get_backtesting_job_counts_by_batch(batch_id: str):
    connection = _get_conn()
    connection.execute(sql_create_backtesting_jobs_table)
    return pd.read_sql(
        """
        SELECT status, COUNT(*) AS count
        FROM Backtesting_Jobs
        WHERE batch_id = ?
        GROUP BY status
        """,
        connection,
        params=(str(batch_id),),
    )

def claim_next_backtesting_job():
    connection = _get_conn()
    started_at = datetime.utcnow().isoformat(timespec="seconds")
    with connection:
        connection.execute(sql_create_backtesting_jobs_table)
        cursor = connection.execute(
            """
            SELECT id, batch_id, strategy_id, symbol, timeframe, optimize
            FROM Backtesting_Jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None

        job_id = row[0]
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET status = 'running',
                started_at = ?,
                error_message = NULL,
                return_code = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (started_at, job_id),
        )

    return {
        "id": row[0],
        "batch_id": row[1],
        "strategy_id": row[2],
        "symbol": row[3],
        "timeframe": row[4],
        "optimize": bool(row[5]),
    }

def set_backtesting_job_log_path(job_id: int, log_path: str):
    connection = _get_conn()
    with connection:
        connection.execute(
            "UPDATE Backtesting_Jobs SET log_path = ? WHERE id = ?",
            (str(log_path), int(job_id)),
        )

def complete_backtesting_job(job_id: int, return_code: int, error_message: str = ""):
    connection = _get_conn()
    finished_at = datetime.utcnow().isoformat(timespec="seconds")
    status = "completed" if int(return_code) == 0 else "failed"
    with connection:
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET status = ?,
                finished_at = ?,
                return_code = ?,
                error_message = ?
            WHERE id = ?
            """,
            (status, finished_at, int(return_code), str(error_message or ""), int(job_id)),
        )

def reset_running_backtesting_jobs(error_message: str = "Interrupted before completion."):
    connection = _get_conn()
    finished_at = datetime.utcnow().isoformat(timespec="seconds")
    with connection:
        connection.execute(sql_create_backtesting_jobs_table)
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET status = 'failed',
                finished_at = ?,
                error_message = ?
            WHERE status = 'running'
            """,
            (finished_at, error_message),
        )

def enqueue_monte_carlo_jobs(jobs, batch_id: str = ""):
    import uuid

    connection = _get_conn()
    batch_id = batch_id or uuid.uuid4().hex
    created_at = datetime.utcnow().isoformat(timespec="seconds")
    queued = []
    skipped = []

    with connection:
        connection.execute(sql_create_monte_carlo_jobs_table)
        for job in jobs:
            strategy_id = str(job["strategy_id"]).strip()
            symbol = str(job["symbol"]).strip().upper()
            timeframe = str(job["timeframe"]).strip()
            method = str(job["method"]).strip()
            scenarios = int(job["scenarios"])
            seed = int(job.get("seed", 42))
            cursor = connection.execute(
                """
                SELECT id
                FROM Monte_Carlo_Jobs
                WHERE strategy_id = ?
                  AND symbol = ?
                  AND timeframe = ?
                  AND method = ?
                  AND status IN ('queued', 'running')
                LIMIT 1
                """,
                (strategy_id, symbol, timeframe, method),
            )
            existing = cursor.fetchone()
            if existing:
                skipped.append(
                    {
                        "id": existing[0],
                        "strategy_id": strategy_id,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "method": method,
                    }
                )
                continue

            cursor = connection.execute(
                """
                INSERT INTO Monte_Carlo_Jobs (
                    batch_id, strategy_id, symbol, timeframe, method, scenarios, seed, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?)
                """,
                (batch_id, strategy_id, symbol, timeframe, method, scenarios, seed, created_at),
            )
            queued.append(
                {
                    "id": cursor.lastrowid,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "method": method,
                }
            )

    return {"batch_id": batch_id, "queued": queued, "skipped": skipped}

def get_monte_carlo_jobs(limit: int = 50):
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_jobs_table)
    return pd.read_sql(
        """
        SELECT id, batch_id, strategy_id, symbol, timeframe, method, scenarios, seed,
               status, created_at, started_at, finished_at, return_code, log_path, error_message
        FROM Monte_Carlo_Jobs
        ORDER BY
            CASE status
                WHEN 'running' THEN 0
                WHEN 'queued' THEN 1
                WHEN 'failed' THEN 2
                ELSE 3
            END,
            COALESCE(started_at, created_at) DESC,
            id DESC
        LIMIT ?
        """,
        connection,
        params=(int(limit),),
    )

def get_monte_carlo_job_counts():
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_jobs_table)
    return pd.read_sql(
        "SELECT status, COUNT(*) AS count FROM Monte_Carlo_Jobs GROUP BY status",
        connection,
    )

def get_monte_carlo_job_counts_by_batch(batch_id: str):
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_jobs_table)
    return pd.read_sql(
        """
        SELECT status, COUNT(*) AS count
        FROM Monte_Carlo_Jobs
        WHERE batch_id = ?
        GROUP BY status
        """,
        connection,
        params=(str(batch_id),),
    )

def claim_next_monte_carlo_job():
    connection = _get_conn()
    started_at = datetime.utcnow().isoformat(timespec="seconds")
    with connection:
        connection.execute(sql_create_monte_carlo_jobs_table)
        cursor = connection.execute(
            """
            SELECT id, batch_id, strategy_id, symbol, timeframe, method, scenarios, seed
            FROM Monte_Carlo_Jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None

        connection.execute(
            """
            UPDATE Monte_Carlo_Jobs
            SET status = 'running',
                started_at = ?,
                error_message = NULL,
                return_code = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (started_at, row[0]),
        )

    return {
        "id": row[0],
        "batch_id": row[1],
        "strategy_id": row[2],
        "symbol": row[3],
        "timeframe": row[4],
        "method": row[5],
        "scenarios": int(row[6]),
        "seed": int(row[7]),
    }

def set_monte_carlo_job_log_path(job_id: int, log_path: str):
    connection = _get_conn()
    with connection:
        connection.execute(
            "UPDATE Monte_Carlo_Jobs SET log_path = ? WHERE id = ?",
            (str(log_path), int(job_id)),
        )

def complete_monte_carlo_job(job_id: int, return_code: int, error_message: str = ""):
    connection = _get_conn()
    finished_at = datetime.utcnow().isoformat(timespec="seconds")
    status = "completed" if int(return_code) == 0 else "failed"
    with connection:
        connection.execute(
            """
            UPDATE Monte_Carlo_Jobs
            SET status = ?,
                finished_at = ?,
                return_code = ?,
                error_message = ?
            WHERE id = ?
            """,
            (status, finished_at, int(return_code), str(error_message or ""), int(job_id)),
        )

def reset_running_monte_carlo_jobs(error_message: str = "Interrupted before completion."):
    connection = _get_conn()
    finished_at = datetime.utcnow().isoformat(timespec="seconds")
    with connection:
        connection.execute(sql_create_monte_carlo_jobs_table)
        connection.execute(
            """
            UPDATE Monte_Carlo_Jobs
            SET status = 'failed',
                finished_at = ?,
                error_message = ?
            WHERE status = 'running'
            """,
            (finished_at, error_message),
        )

def get_backtesting_trades_by_symbol_timeframe_strategy(symbol: str, timeframe: str, strategy_id: str):
    connection = _get_conn()
    connection.execute(sql_create_backtesting_trades_table)
    return pd.read_sql(
        """
        SELECT *
        FROM Backtesting_Trades
        WHERE Symbol = ?
          AND Time_Frame = ?
          AND Strategy_Id = ?
        ORDER BY EntryTime, ExitTime, Id
        """,
        connection,
        params=(str(symbol), str(timeframe), str(strategy_id)),
    )

def upsert_monte_carlo_result(result: dict):
    connection = _get_conn()
    created_at = datetime.utcnow().isoformat(timespec="seconds")
    summary = result.get("summary", {}) if isinstance(result, dict) else {}
    metrics = result.get("metrics", {}) if isinstance(result, dict) else {}

    def _metric(metric_name, column_name):
        try:
            value = metrics.get(metric_name, {}).get(column_name)
            return float(value) if value is not None else None
        except Exception:
            return None

    with connection:
        connection.execute(sql_create_monte_carlo_results_table)
        connection.execute(
            """
            INSERT OR REPLACE INTO Monte_Carlo_Results (
                Symbol, Time_Frame, Strategy_Id, Method, Scenarios, Valid_Scenarios, Seed,
                Robustness_Score, Interpretation,
                Net_Profit_Original, Net_Profit_Worst_5, Net_Profit_Median, Net_Profit_Best_5,
                Max_Drawdown_Original, Max_Drawdown_Worst_5, Max_Drawdown_Median, Max_Drawdown_Best_5,
                Html_Path, Csv_Path, Json_Path, Result_JSON, Created_At
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(result.get("symbol")),
                str(result.get("timeframe")),
                str(result.get("strategy_id")),
                str(result.get("method")),
                int(summary.get("total_scenarios", result.get("scenarios", 0)) or 0),
                int(summary.get("valid_scenarios", 0) or 0),
                int(result.get("seed", 42) or 42),
                float(summary.get("robustness_score")) if summary.get("robustness_score") is not None else None,
                str(summary.get("interpretation", "")),
                _metric("Net Profit", "original"),
                _metric("Net Profit", "worst_5"),
                _metric("Net Profit", "median"),
                _metric("Net Profit", "best_5"),
                _metric("Max Drawdown", "original"),
                _metric("Max Drawdown", "worst_5"),
                _metric("Max Drawdown", "median"),
                _metric("Max Drawdown", "best_5"),
                str(result.get("html_path", "")),
                str(result.get("csv_path", "")),
                str(result.get("json_path", "")),
                json.dumps(result, ensure_ascii=True),
                created_at,
            ),
        )

def get_all_monte_carlo_results():
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_results_table)
    return pd.read_sql(
        """
        SELECT id, Symbol, Time_Frame, Strategy_Id, Method, Scenarios, Valid_Scenarios,
               Seed, Robustness_Score, Interpretation,
               Net_Profit_Original, Net_Profit_Worst_5, Net_Profit_Median, Net_Profit_Best_5,
               Max_Drawdown_Original, Max_Drawdown_Worst_5, Max_Drawdown_Median, Max_Drawdown_Best_5,
               Html_Path, Csv_Path, Json_Path, Created_At
        FROM Monte_Carlo_Results
        ORDER BY Created_At DESC
        """,
        connection,
    )

def get_monte_carlo_result(symbol: str, timeframe: str, strategy_id: str, method: str):
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_results_table)
    return pd.read_sql(
        """
        SELECT *
        FROM Monte_Carlo_Results
        WHERE Symbol = ?
          AND Time_Frame = ?
          AND Strategy_Id = ?
          AND Method = ?
        LIMIT 1
        """,
        connection,
        params=(str(symbol), str(timeframe), str(strategy_id), str(method)),
    )

def _monte_carlo_result_paths(row):
    paths = []
    for column in ["Html_Path", "Csv_Path", "Json_Path"]:
        try:
            value = row[column]
        except Exception:
            value = None
        if value:
            paths.append(str(value))
    return paths

def _safe_delete_monte_carlo_file(path: str):
    if not path:
        return {"path": str(path or ""), "deleted": False, "skipped": True, "error": ""}

    normalized = str(path).replace("\\", os.sep)
    abs_path = normalized if os.path.isabs(normalized) else os.path.join(PROJECT_ROOT, normalized)
    real_base = os.path.realpath(MONTE_CARLO_OUTPUT_DIR)
    real_path = os.path.realpath(abs_path)

    try:
        common = os.path.commonpath([real_base, real_path])
    except ValueError:
        common = ""
    if common != real_base:
        return {"path": str(path), "deleted": False, "skipped": True, "error": "outside_monte_carlo_dir"}
    if os.path.isdir(real_path):
        return {"path": str(path), "deleted": False, "skipped": True, "error": "is_directory"}
    if not os.path.exists(real_path):
        return {"path": str(path), "deleted": False, "skipped": True, "error": ""}

    try:
        os.remove(real_path)
        return {"path": str(path), "deleted": True, "skipped": False, "error": ""}
    except OSError as exc:
        return {"path": str(path), "deleted": False, "skipped": False, "error": repr(exc)}

def _delete_monte_carlo_files(paths):
    file_results = [_safe_delete_monte_carlo_file(path) for path in paths]
    return {
        "files": file_results,
        "deleted_files": sum(1 for item in file_results if item["deleted"]),
        "skipped_files": sum(1 for item in file_results if item["skipped"]),
        "file_errors": [item for item in file_results if item["error"] and not item["skipped"]],
        "unsafe_paths": [item for item in file_results if item["error"] == "outside_monte_carlo_dir"],
    }

def get_monte_carlo_cleanup_candidates(method: str = "", older_than_days: int = None, result_ids=None):
    connection = _get_conn()
    connection.execute(sql_create_monte_carlo_results_table)
    clauses = []
    params = []
    if method:
        clauses.append("Method = ?")
        params.append(str(method))
    if older_than_days is not None:
        cutoff = datetime.utcnow() - timedelta(days=int(older_than_days))
        clauses.append("Created_At < ?")
        params.append(cutoff.isoformat(timespec="seconds"))
    if result_ids:
        ids = [int(value) for value in result_ids]
        placeholders = ",".join(["?"] * len(ids))
        clauses.append(f"id IN ({placeholders})")
        params.extend(ids)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return pd.read_sql(
        f"""
        SELECT id, Symbol, Time_Frame, Strategy_Id, Method, Created_At,
               Html_Path, Csv_Path, Json_Path
        FROM Monte_Carlo_Results
        {where}
        ORDER BY Created_At DESC
        """,
        connection,
        params=tuple(params),
    )

def delete_monte_carlo_results(result_ids):
    ids = [int(value) for value in (result_ids or [])]
    if not ids:
        return {"deleted_results": 0, "deleted_files": 0, "skipped_files": 0, "file_errors": [], "unsafe_paths": []}

    candidates = get_monte_carlo_cleanup_candidates(result_ids=ids)
    paths = []
    for _, row in candidates.iterrows():
        paths.extend(_monte_carlo_result_paths(row))

    connection = _get_conn()
    placeholders = ",".join(["?"] * len(ids))
    with connection:
        cursor = connection.execute(
            f"DELETE FROM Monte_Carlo_Results WHERE id IN ({placeholders})",
            tuple(ids),
        )
    file_summary = _delete_monte_carlo_files(paths)
    return {
        "deleted_results": int(cursor.rowcount if cursor.rowcount is not None else len(candidates)),
        **file_summary,
    }

def delete_monte_carlo_results_by_method(method: str):
    candidates = get_monte_carlo_cleanup_candidates(method=str(method))
    return delete_monte_carlo_results(candidates["id"].tolist() if not candidates.empty else [])

def delete_old_monte_carlo_results(days: int = 30, method: str = ""):
    candidates = get_monte_carlo_cleanup_candidates(method=str(method or ""), older_than_days=int(days))
    return delete_monte_carlo_results(candidates["id"].tolist() if not candidates.empty else [])

def get_job_schedules():
    connection = _get_conn()
    return pd.read_sql("SELECT name, script, script_args, cadence, enabled, description, last_run FROM Job_Schedules ORDER BY name", connection)

def set_job_schedule_enabled(name: str, enabled: bool):
    connection = _get_conn()
    with connection:
        connection.execute(
            "UPDATE Job_Schedules SET enabled = ? WHERE name = ?",
            (1 if enabled else 0, name),
        )

def get_job_schedule_enabled(name: str) -> bool:
    connection = _get_conn()
    cursor = connection.execute(
        "SELECT enabled FROM Job_Schedules WHERE name = ?",
        (name,),
    )
    row = cursor.fetchone()
    if row is None:
        return True
    return bool(row[0])

def is_trade_main_timeframe_enabled(time_frame: str) -> bool:
    schedule_map = {
        "1h": "main_1h",
        "4h": "main_4h",
        "1d": "main_1d",
    }
    schedule_name = schedule_map.get(time_frame)
    if not schedule_name:
        return True
    return get_job_schedule_enabled(schedule_name)

def update_job_last_run(name: str, last_run: str):
    connection = _get_conn()
    with connection:
        connection.execute(
            "UPDATE Job_Schedules SET last_run = ? WHERE name = ?",
            (last_run, name),
        )

# Backtesting settings
def ensure_backtesting_settings():
    connection = _get_conn()
    with connection:
        connection.execute(sql_create_backtesting_settings_table)
        _ensure_backtesting_settings_columns(connection)
        cursor = connection.execute("SELECT COUNT(*) FROM Backtesting_Settings")
        count = cursor.fetchone()[0]
        if count == 0:
            connection.execute(
                """
                INSERT INTO Backtesting_Settings (
                    Commission_Value,
                    Cash_Value,
                    Maximize,
                    Use_Intraday_Current_Timeframe_Market_Phase_Filter,
                    Market_Phase_1h_SMA_Fast,
                    Market_Phase_1h_SMA_Slow,
                    Market_Phase_4h_SMA_Fast,
                    Market_Phase_4h_SMA_Slow,
                    Market_Phase_1d_SMA_Fast,
                    Market_Phase_1d_SMA_Slow,
                    Buy_Hold_Start_Mode,
                    Strategy_Quality_Return_Weight,
                    Strategy_Quality_Risk_Weight,
                    Strategy_Quality_Risk_Adjusted_Weight,
                    Strategy_Quality_Trade_Quality_Weight,
                    Strategy_Quality_Robustness_Weight
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    float(DEFAULT_BACKTESTING_SETTINGS["Commission_Value"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Cash_Value"]),
                    str(DEFAULT_BACKTESTING_SETTINGS["Maximize"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Use_Intraday_Current_Timeframe_Market_Phase_Filter"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_1h_SMA_Fast"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_1h_SMA_Slow"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_4h_SMA_Fast"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_4h_SMA_Slow"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_1d_SMA_Fast"]),
                    int(DEFAULT_BACKTESTING_SETTINGS["Market_Phase_1d_SMA_Slow"]),
                    str(DEFAULT_BACKTESTING_SETTINGS["Buy_Hold_Start_Mode"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Strategy_Quality_Return_Weight"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Strategy_Quality_Risk_Weight"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Strategy_Quality_Risk_Adjusted_Weight"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Strategy_Quality_Trade_Quality_Weight"]),
                    float(DEFAULT_BACKTESTING_SETTINGS["Strategy_Quality_Robustness_Weight"]),
                ),
            )

def get_backtesting_settings():
    connection = _get_conn()
    _ensure_backtesting_settings_columns(connection)
    df = pd.read_sql(
        """
        SELECT
            Commission_Value,
            Cash_Value,
            Maximize,
            Use_Intraday_Current_Timeframe_Market_Phase_Filter,
            Market_Phase_1h_SMA_Fast,
            Market_Phase_1h_SMA_Slow,
            Market_Phase_4h_SMA_Fast,
            Market_Phase_4h_SMA_Slow,
            Market_Phase_1d_SMA_Fast,
            Market_Phase_1d_SMA_Slow,
            Buy_Hold_Start_Mode,
            Strategy_Quality_Return_Weight,
            Strategy_Quality_Risk_Weight,
            Strategy_Quality_Risk_Adjusted_Weight,
            Strategy_Quality_Trade_Quality_Weight,
            Strategy_Quality_Robustness_Weight
        FROM Backtesting_Settings
        LIMIT 1
        """,
        connection,
    )
    if df.empty:
        return DEFAULT_BACKTESTING_SETTINGS.copy()
    return {
        "Commission_Value": float(df.iloc[0]["Commission_Value"]),
        "Cash_Value": float(df.iloc[0]["Cash_Value"]),
        "Maximize": str(df.iloc[0]["Maximize"]),
        "Use_Intraday_Current_Timeframe_Market_Phase_Filter": int(df.iloc[0]["Use_Intraday_Current_Timeframe_Market_Phase_Filter"]),
        "Market_Phase_1h_SMA_Fast": int(df.iloc[0]["Market_Phase_1h_SMA_Fast"]),
        "Market_Phase_1h_SMA_Slow": int(df.iloc[0]["Market_Phase_1h_SMA_Slow"]),
        "Market_Phase_4h_SMA_Fast": int(df.iloc[0]["Market_Phase_4h_SMA_Fast"]),
        "Market_Phase_4h_SMA_Slow": int(df.iloc[0]["Market_Phase_4h_SMA_Slow"]),
        "Market_Phase_1d_SMA_Fast": int(df.iloc[0]["Market_Phase_1d_SMA_Fast"]),
        "Market_Phase_1d_SMA_Slow": int(df.iloc[0]["Market_Phase_1d_SMA_Slow"]),
        "Buy_Hold_Start_Mode": str(df.iloc[0]["Buy_Hold_Start_Mode"] or "indicator_warmup"),
        "Strategy_Quality_Return_Weight": float(df.iloc[0]["Strategy_Quality_Return_Weight"]),
        "Strategy_Quality_Risk_Weight": float(df.iloc[0]["Strategy_Quality_Risk_Weight"]),
        "Strategy_Quality_Risk_Adjusted_Weight": float(df.iloc[0]["Strategy_Quality_Risk_Adjusted_Weight"]),
        "Strategy_Quality_Trade_Quality_Weight": float(df.iloc[0]["Strategy_Quality_Trade_Quality_Weight"]),
        "Strategy_Quality_Robustness_Weight": float(df.iloc[0]["Strategy_Quality_Robustness_Weight"]),
    }

def update_backtesting_settings(
    commission_value: float,
    cash_value: float,
    maximize: str,
    use_intraday_current_timeframe_market_phase_filter: bool = True,
    market_phase_1h_sma_fast: int = 50,
    market_phase_1h_sma_slow: int = 200,
    market_phase_4h_sma_fast: int = 50,
    market_phase_4h_sma_slow: int = 200,
    market_phase_1d_sma_fast: int = 50,
    market_phase_1d_sma_slow: int = 200,
    buy_hold_start_mode: str = "indicator_warmup",
    strategy_quality_return_weight: float = 20.0,
    strategy_quality_risk_weight: float = 25.0,
    strategy_quality_risk_adjusted_weight: float = 20.0,
    strategy_quality_trade_quality_weight: float = 20.0,
    strategy_quality_robustness_weight: float = 15.0,
):
    connection = _get_conn()
    with connection:
        _ensure_backtesting_settings_columns(connection)
        connection.execute(
            """
            UPDATE Backtesting_Settings
            SET Commission_Value = ?,
                Cash_Value = ?,
                Maximize = ?,
                Use_Intraday_Current_Timeframe_Market_Phase_Filter = ?,
                Market_Phase_1h_SMA_Fast = ?,
                Market_Phase_1h_SMA_Slow = ?,
                Market_Phase_4h_SMA_Fast = ?,
                Market_Phase_4h_SMA_Slow = ?,
                Market_Phase_1d_SMA_Fast = ?,
                Market_Phase_1d_SMA_Slow = ?,
                Buy_Hold_Start_Mode = ?,
                Strategy_Quality_Return_Weight = ?,
                Strategy_Quality_Risk_Weight = ?,
                Strategy_Quality_Risk_Adjusted_Weight = ?,
                Strategy_Quality_Trade_Quality_Weight = ?,
                Strategy_Quality_Robustness_Weight = ?
            WHERE Id = (SELECT Id FROM Backtesting_Settings LIMIT 1)
            """,
            (
                float(commission_value),
                float(cash_value),
                str(maximize),
                1 if use_intraday_current_timeframe_market_phase_filter else 0,
                int(market_phase_1h_sma_fast),
                int(market_phase_1h_sma_slow),
                int(market_phase_4h_sma_fast),
                int(market_phase_4h_sma_slow),
                int(market_phase_1d_sma_fast),
                int(market_phase_1d_sma_slow),
                str(buy_hold_start_mode),
                float(strategy_quality_return_weight),
                float(strategy_quality_risk_weight),
                float(strategy_quality_risk_adjusted_weight),
                float(strategy_quality_trade_quality_weight),
                float(strategy_quality_robustness_weight),
            ),
        )

# Approval rules
def get_Approval_Rule_Definitions():
    connection = _get_conn()
    return pd.read_sql("SELECT id, rule_name, description FROM Approval_Rule_Definitions ORDER BY rule_name", connection)

def get_Backtest_Approval_Rules():
    connection = _get_conn()
    sql = """
        SELECT r.id,
               d.rule_name,
               d.description,
               r.rule_value,
               r.timeframe,
               r.enabled
        FROM Backtest_Approval_Rules r
        JOIN Approval_Rule_Definitions d ON d.id = r.rule_id
        ORDER BY d.rule_name, r.timeframe;
    """
    return pd.read_sql(sql, connection)

QUALITY_GRADE_RANKS = {"F": 1, "D": 2, "C": 3, "B": 4, "A": 5}


def _normalize_approval_rule_value(rule_name: str, rule_value):
    if rule_name == "Quality_Grade_Min":
        grade = str(rule_value or "").strip().upper()
        if grade not in QUALITY_GRADE_RANKS:
            raise ValueError("Quality_Grade_Min must be one of A, B, C, D, or F.")
        return grade
    return float(rule_value)


def _ensure_global_approval_rule(rule_name: str, description: str, rule_value, enabled: bool):
    connection = _get_conn()
    with connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO Approval_Rule_Definitions (rule_name, description)
            VALUES (?, ?)
            """,
            (rule_name, description),
        )
        rule_id_row = connection.execute(
            "SELECT id FROM Approval_Rule_Definitions WHERE rule_name = ?",
            (rule_name,),
        ).fetchone()
        if not rule_id_row:
            return
        rule_id = rule_id_row[0]
        connection.execute(
            "UPDATE Backtest_Approval_Rules SET timeframe = NULL WHERE rule_id = ? AND timeframe = 'global'",
            (rule_id,),
        )
        connection.execute(
            """
            DELETE FROM Backtest_Approval_Rules
            WHERE rule_id = ?
              AND timeframe IS NULL
              AND id NOT IN (
                  SELECT MAX(id)
                  FROM Backtest_Approval_Rules
                  WHERE rule_id = ? AND timeframe IS NULL
              )
            """,
            (rule_id, rule_id),
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
            SELECT ?, ?, NULL, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM Backtest_Approval_Rules
                WHERE rule_id = ? AND timeframe IS NULL
            )
            """,
            (rule_id, _normalize_approval_rule_value(rule_name, rule_value), 1 if enabled else 0, rule_id),
        )


def ensure_quality_approval_rules():
    _ensure_global_approval_rule(
        rule_name="Quality_Grade_Min",
        description="Minimum strategy quality grade required for trading approval. Screening profiles: C = baseline, B = quality-focused, A = top-tier only.",
        rule_value="C",
        enabled=True,
    )
    _ensure_global_approval_rule(
        rule_name="Quality_Score_Min",
        description="Minimum strategy quality score from 0 to 100. Optional numeric alternative to Quality_Grade_Min.",
        rule_value=70.0,
        enabled=False,
    )


def reset_backtest_approval_rules_to_defaults():
    connection = _get_conn()
    with connection:
        connection.execute("DELETE FROM Backtest_Approval_Rules")
        connection.executescript(sql_seed_default_approval_rules)
        ensure_quality_approval_rules()


def upsert_backtest_approval_rule(rule_name: str, rule_value, timeframe: str | None, enabled: bool):
    connection = _get_conn()
    rule_id_row = connection.execute(
        "SELECT id FROM Approval_Rule_Definitions WHERE rule_name = ?",
        (rule_name,),
    ).fetchone()
    if not rule_id_row:
        return False
    rule_id = rule_id_row[0]
    normalized_rule_value = _normalize_approval_rule_value(rule_name, rule_value)
    with connection:
        # Clean up duplicate global rules (timeframe NULL) keeping the newest row
        connection.execute(
            "UPDATE Backtest_Approval_Rules SET timeframe = NULL WHERE rule_id = ? AND timeframe = 'global'",
            (rule_id,),
        )
        connection.execute(
            """
            DELETE FROM Backtest_Approval_Rules
            WHERE rule_id = ?
              AND timeframe IS NULL
              AND id NOT IN (
                  SELECT MAX(id)
                  FROM Backtest_Approval_Rules
                  WHERE rule_id = ? AND timeframe IS NULL
              )
            """,
            (rule_id, rule_id),
        )
        if timeframe is None:
            cursor = connection.execute(
                """
                UPDATE Backtest_Approval_Rules
                SET rule_value = ?, enabled = ?
                WHERE rule_id = ? AND (timeframe IS NULL OR timeframe = 'global')
                """,
                (normalized_rule_value, 1 if enabled else 0, rule_id),
            )
            if cursor.rowcount == 0:
                connection.execute(
                    """
                    INSERT INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
                    VALUES (?, ?, NULL, ?)
                    """,
                    (rule_id, normalized_rule_value, 1 if enabled else 0),
                )
        else:
            cursor = connection.execute(
                """
                UPDATE Backtest_Approval_Rules
                SET rule_value = ?, enabled = ?
                WHERE rule_id = ? AND timeframe = ?
                """,
                (normalized_rule_value, 1 if enabled else 0, rule_id, timeframe),
            )
            if cursor.rowcount == 0:
                connection.execute(
                    """
                    INSERT INTO Backtest_Approval_Rules (rule_id, rule_value, timeframe, enabled)
                    VALUES (?, ?, ?, ?)
                    """,
                    (rule_id, normalized_rule_value, timeframe, 1 if enabled else 0),
                )
    return True

def delete_backtest_approval_rule(rule_name: str, timeframe: str | None):
    connection = _get_conn()
    rule_id_row = connection.execute(
        "SELECT id FROM Approval_Rule_Definitions WHERE rule_name = ?",
        (rule_name,),
    ).fetchone()
    if not rule_id_row:
        return False
    rule_id = rule_id_row[0]
    with connection:
        if timeframe is None:
            connection.execute(
                "DELETE FROM Backtest_Approval_Rules WHERE rule_id = ? AND (timeframe IS NULL OR timeframe = 'global')",
                (rule_id,),
            )
        else:
            connection.execute(
                "DELETE FROM Backtest_Approval_Rules WHERE rule_id = ? AND timeframe = ?",
                (rule_id, timeframe),
            )
    return True

def _load_enabled_rules_by_timeframe(timeframe: str):
    connection = _get_conn()
    sql = """
        SELECT d.rule_name, r.rule_value
        FROM Backtest_Approval_Rules r
        JOIN Approval_Rule_Definitions d ON d.id = r.rule_id
        WHERE r.enabled = 1
          AND r.timeframe = ?
    """
    rows = connection.execute(sql, (timeframe,)).fetchall()
    return {
        name: _normalize_approval_rule_value(name, value)
        for name, value in rows
    }

def _load_enabled_rules_global():
    connection = _get_conn()
    sql = """
        SELECT d.rule_name, r.rule_value
        FROM Backtest_Approval_Rules r
        JOIN Approval_Rule_Definitions d ON d.id = r.rule_id
        WHERE r.enabled = 1
          AND r.timeframe IS NULL
    """
    rows = connection.execute(sql).fetchall()
    return {
        name: _normalize_approval_rule_value(name, value)
        for name, value in rows
    }

def resolve_Backtest_Approval_Rules(timeframe: str):
    rules = _load_enabled_rules_global()
    rules.update(_load_enabled_rules_by_timeframe(timeframe))
    return rules

def is_backtest_approved(timeframe: str, stats_row):
    rules = resolve_Backtest_Approval_Rules(timeframe)
    if not rules:
        return True, []

    def _get(key, default=None):
        try:
            return float(stats_row.get(key, default)) if stats_row.get(key, default) is not None else default
        except Exception:
            return default

    reasons = []
    return_perc = _get("Return_Perc")
    buy_hold_perc = _get("BuyHold_Return_Perc")
    trades = _get("Trades")
    profit_factor = _get("Profit_Factor")
    sqn = _get("SQN")
    max_drawdown = _get("Max_Drawdown_Perc")
    quality_score = _get("Quality_Score")
    quality_grade = str(stats_row.get("Quality_Grade", "") or "").strip().upper()

    if "Return_Min_Pct" in rules and return_perc is not None:
        if return_perc <= rules["Return_Min_Pct"]:
            reasons.append("Return_Min_Pct")
    if "SQN_min" in rules and sqn is not None:
        if sqn < rules["SQN_min"]:
            reasons.append("SQN_min")
    if "Min_Trades" in rules and trades is not None:
        if trades < rules["Min_Trades"]:
            reasons.append("Min_Trades")
    if "Profit_Factor_min" in rules and profit_factor is not None:
        if profit_factor <= rules["Profit_Factor_min"]:
            reasons.append("Profit_Factor_min")
    if "Quality_Grade_Min" in rules:
        minimum_grade = str(rules["Quality_Grade_Min"]).strip().upper()
        if (
            quality_grade not in QUALITY_GRADE_RANKS
            or minimum_grade not in QUALITY_GRADE_RANKS
            or QUALITY_GRADE_RANKS[quality_grade] < QUALITY_GRADE_RANKS[minimum_grade]
        ):
            reasons.append("Quality_Grade_Min")
    if "Quality_Score_Min" in rules:
        if quality_score is None or quality_score < rules["Quality_Score_Min"]:
            reasons.append("Quality_Score_Min")

    require_dd = rules.get("Require_Drawdown_Limit_When_Underperform_BuyHold")
    if require_dd is not None and int(require_dd) == 1 and "Max_Drawdown_Pct" in rules:
        if return_perc is not None and buy_hold_perc is not None and max_drawdown is not None:
            if return_perc < buy_hold_perc:
                if abs(max_drawdown) > rules["Max_Drawdown_Pct"]:
                    reasons.append("Max_Drawdown_Pct")

    return len(reasons) == 0, reasons

##############################

# --- Module initialization ---
conn = connect()                  # open global connection
create_tables()                   # create/update schema
migrate_config_to_db()            # migrate config.yaml to the DB (uses _get_conn internally)
ensure_job_schedules()            # seed default schedules
ensure_backtesting_jobs()         # create backtesting job queue
ensure_monte_carlo_tables()       # create Monte Carlo queue/results
ensure_backtesting_settings()     # seed backtesting settings
# --- End of module initialization ---





    




        
