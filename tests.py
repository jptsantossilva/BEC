# %%
import pandas as pd
import sqlite3
import os
import datetime

def connect(path: str = ""):
    file_path = os.path.join(path, "data.db")
    return sqlite3.connect(file_path)

conn = connect()

n_decimals = 8


sql_get_last_buy_order_by_bot_symbol = """
    SELECT * FROM Orders
    WHERE 
        Side = 'BUY' 
        AND Bot = ?
        AND Symbol = ?
    ORDER BY Id DESC LIMIT 1;
"""
def get_last_buy_order_by_bot_symbol(connection, bot: str, symbol: str):
    return pd.read_sql(sql_get_last_buy_order_by_bot_symbol, connection, params=(bot, symbol,))

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
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);        
"""
def add_order_sell(connection, exchange_order_id: str, date: str, bot: str, symbol: str, price: float, qty: float, ema_fast: int, ema_slow: int, exit_reason: str):
    df_last_buy_order = get_last_buy_order_by_bot_symbol(connection, bot, symbol)

    if df_last_buy_order.empty:
        print("DataFrame is empty")
        buy_order_id = ''
        buy_price = 0
        buy_qty = 0
        pnl_perc = 0
        pnl_value = 0
    else:
        buy_order_id = str(df_last_buy_order.loc[0, 'Id'])
        buy_price = float(df_last_buy_order.loc[0, 'Price'])
        buy_qty = float(df_last_buy_order.loc[0, 'Qty'])

        sell_price = price
        sell_qty = qty

        pnl_perc = (((sell_price*sell_qty)-(buy_price*buy_qty))/(buy_price*buy_qty))*100
        pnl_perc = float(round(pnl_perc, 2))
        pnl_value = (sell_price*sell_qty)-(buy_price*buy_qty)
        pnl_value = float(round(pnl_value, n_decimals))
  
    side = "SELL"

    with connection:
        connection.execute(sql_add_order_sell, (exchange_order_id, 
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
                                                buy_order_id, 
                                                exit_reason))
        return float(pnl_value), float(pnl_perc)

# %%
# add to orders database table
orderId = "123"
current_time = datetime.datetime.now()
transactTime = str(pd.to_datetime(current_time.timestamp() * 1000, unit='ms'))
bot="1d"
symbol="TOMOBTC"
avg_price=2.87e-05
executedQty=71.6
fast_ema=45
slow_ema=120
reason="testes"


pnl_value, pnl_perc = add_order_sell(conn,
                                                exchange_order_id = str(orderId),
                                                date = str(transactTime),
                                                bot = bot,
                                                symbol = symbol,
                                                price = avg_price,
                                                qty = float(executedQty),
                                                ema_fast = fast_ema,
                                                ema_slow = slow_ema,
                                                exit_reason = reason) 


