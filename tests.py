import os
from binance.client import Client
from datetime import datetime, timedelta
import utils.config as config
import pandas as pd
import docs.database.database as database
import exchange.exchange as exchange


client: Client = None

def connect():
    api_key = config.get_env_var('binance_api')
    api_secret = config.get_env_var('binance_secret')

    # Binance Client
    try:
        global client
        client = Client(api_key, api_secret)
    except Exception as e:
            msg = "Error connecting to Binance. "+ repr(e)
            print(msg)
            # logging.exception(msg)
            # telegram.send_telegram_message(telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg)
            # sys.exit(msg) 

connect()


# Set start and end date for daily account snapshots
# start_date = datetime.now() - timedelta(days=10) # 30 days ago
# end_date = datetime.now() # today



def create_account_snapshot():

    last_date = database.get_last_date_from_balances(database.conn)
    if last_date == '0':
         today = datetime.now()
         start_date = today - timedelta(days=30)
    else:
         start_date = datetime.strptime(last_date, '%Y-%m-%d')        

    snapshots = client.get_account_snapshot(type="SPOT",
                                            startTime=int(start_date.timestamp()*1000), 
                                            # endTime=int(end_date.timestamp()*1000)
                                            limit=30 #max                                        
                                            )

    code = snapshots['code']
    msg = snapshots['msg']

    # get list of available symbols. 
    # This is usefull to avoid getting price from symbol that do not trade against stable
    exchange_info = exchange.get_exchange_info()
    symbols = set()
    trade_against = "BUSD"
    for s in exchange_info['symbols']:
        if (s['symbol'].endswith(trade_against)
            and s['status'] == 'TRADING'):
                symbols.add(s['symbol']) 

    # Create a Pandas DataFrame to store the daily balances for each asset
    df_balance = pd.DataFrame()

    # Iterate through the snapshots and get the daily balance for each asset
    for snapshot in snapshots['snapshotVos']:
        if snapshot['type'] == 'spot' and snapshot['data'] is not None:
            snapshot_date = datetime.fromtimestamp(snapshot['updateTime']/1000).date()
            totalAssetOfBtc = snapshot['data']['totalAssetOfBtc']
            for balance in snapshot['data']['balances']:
                asset = balance['asset']
                daily_balance = float(balance['free'])

                print(f"{snapshot_date}-{asset}")
                
                # ignore if balance = 0
                if daily_balance == 0.0:
                    continue

                symbol_with_trade_against = asset+trade_against

                if asset in [trade_against]:
                     balance_usd = daily_balance
                elif symbol_with_trade_against not in symbols:
                     print(f"{asset} not in available symbols")
                     balance_usd = 0
                else:
                    # convert snapshot_date from date to datetime
                    date = datetime.combine(snapshot_date, datetime.min.time())
                    unit_price = exchange.get_price_close_by_symbol_and_date(symbol_with_trade_against, date)
                    balance_usd = unit_price * daily_balance

                df_new = pd.DataFrame({
                    'Date': [snapshot_date],
                    'Asset': [asset],
                    'Balance': [daily_balance],
                    'Balance_USD': [balance_usd],
                    'Total_Balance_Of_BTC': [totalAssetOfBtc]
                    })
                # add to total
                df_balance = pd.concat([df_balance, df_new], ignore_index=True)

    # Print the daily balances for each asset
    # print(df_balance)

    # add data to table Balance
    database.add_balances(database.conn, df_balance)


create_account_snapshot()

# symbol = "BTCUSDT"
# date = datetime(2023, 5, 2)

# price = exchange.get_price_close_by_symbol_and_date(symbol, date)
# print(price)
    