import config
import pandas as pd
from datetime import datetime
import numpy as np
import sys
import timeit
import add_symbol
import telegram
import logging
import database
from exchange import client, get_exchange_info

# calculate program run time
start = timeit.default_timer() 

# inform start
telegram.send_telegram_message(telegram.telegram_token_market_phases, telegram.EMOJI_START, "Start")

# log file to store error messages
log_filename = "symbol_by_market_phase.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')


# Arguments
n = len(sys.argv)
# print("Total arguments passed:", n)
if n < 2:
    print("Argument is missing")
    timeframe = input('Enter timeframe (1d, 8h, 4h):')
    trade_against = input('Trade against USDT, BUSD or BTC:')
else:
    # argv[0] in Python is always the name of the script.
    timeframe = sys.argv[1]
    trade_against = sys.argv[2]

if timeframe == "1d": startdate = "200 day ago UTC"
elif timeframe == "8h": startdate = str(8*200)+" hour ago UTC"
elif timeframe == "4h": startdate = str(4*200)+" hour ago UTC"


def get_blacklist():
    # read symbols from blacklist to not trade
    df_blacklist = database.get_symbol_blacklist()
    blacklist = set()
    if not df_blacklist.empty:
        df_blacklist['Symbol'] = df_blacklist['Symbol'].astype(str)+trade_against
        # blacklist to set
        blacklist = set(df_blacklist["Symbol"].unique())

    return blacklist

# get blacklist
blacklist = get_blacklist()

exchange_info = get_exchange_info()
        
symbols = set()

# get symbols
for s in exchange_info['symbols']:
    if (s['symbol'].endswith(trade_against)
        and not(s['symbol'].endswith('DOWN'+trade_against))
        and not(s['symbol'].endswith('UP'+trade_against))
        and not(s['symbol'] == "AUD"+trade_against) # Australian Dollar
        and not(s['symbol'] == "EUR"+trade_against) # Euro
        and not(s['symbol'] == "GBP"+trade_against) # British pound
        and s['status'] == 'TRADING'):
            symbols.add(s['symbol'])

# from the symbols to trade, exclude coins from Blacklist
symbols -= blacklist

symbols = sorted(symbols)
msg = str(len(symbols))+" symbols found. Calculating..."
print(msg)
telegram.send_telegram_message(telegram.telegram_token_market_phases, "", msg)

def apply_technicals(df):
        df['DSMA50'] = df['Price'].rolling(50).mean()
        df['DSMA200'] = df['Price'].rolling(200).mean()

        df['Perc_Above_DSMA50'] = ((df['Price']-df['DSMA50'])/df['DSMA50'])*100
        df['Perc_Above_DSMA200'] = ((df['Price']-df['DSMA200'])/df['DSMA200'])*100        

def get_data(symbol):
    try:
        frame = pd.DataFrame(client.get_historical_klines(symbol
                                                        ,timeframe                                        
                                                        
                                                        # better get all historical data. 
                                                        # Using a defined start date will affect ema values. 
                                                        # To get same ema and sma values of tradingview all historical data must be used. 
                                                        # ,startDate

                                                        ))
        
        frame = frame.iloc[:,[0,4]] # columns selection
        frame.columns = ['Time','Price'] #rename columns
        # new dataframe with price only
        frame[['Price']] = frame[['Price']].astype(float) #cast to float
        # frame.Time = pd.to_datetime(frame.Time, unit='ms') #make human readable timestamp
        frame['Symbol'] = symbol
        frame.index = [datetime.fromtimestamp(x/1000.0) for x in frame.Time]
        
        frame = frame[['Symbol','Price']]
        return frame
    except Exception as e:
        msg = sys._getframe(  ).f_code.co_name+" - "+symbol+" - "+repr(e)
        print(msg)
        telegram.send_telegram_message(telegram.telegram_token_market_phases, telegram.EMOJI_WARNING, msg)

        # return empty dataframe
        frame = pd.DataFrame()
        return frame 

# empty dataframe
dfResult = pd.DataFrame()

for symbol in symbols:

    print("calculating "+symbol)
    df = get_data(symbol)
    apply_technicals(df)
    # last one is the one with 200dsma value
    df = df.tail(1)

    if dfResult.empty:
        dfResult = df
    else:
        dfResult = pd.concat([dfResult, df])

# Coins in accumulation and Bullish phases
conditions = [
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200']), # recovery phase
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200']), # accumulation phase
    (dfResult['Price'] > dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # bullish phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] > dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # warning phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] > dfResult['DSMA200']), # distribution phase
    (dfResult['Price'] < dfResult['DSMA50']) & (dfResult['Price'] < dfResult['DSMA200']) & (dfResult['DSMA50'] < dfResult['DSMA200'])  # bearish phase
]
# set marketphase to each coin
values = ['recovery', 'accumulation', 'bullish', 'warning','distribution','bearish']
dfResult['Market_Phase'] = np.select(conditions, values)
# print(dfResult)

# currentDate = date.today().strftime('%Y%m%d')
# dfResult.to_csv("coinPairByMarketPhase/coinpairByMarketPhase_"+trade_against+"_"+timeframe+"_"+currentDate+".csv")

dfBullish = dfResult.query("Market_Phase == 'bullish'")
dfAccumulation= dfResult.query("Market_Phase == 'accumulation'")

# union accumulation and bullish results
dfUnion = pd.concat([dfBullish, dfAccumulation], ignore_index=True)

df_top = dfUnion.sort_values(by=['Perc_Above_DSMA200'], ascending=False)
df_top = df_top.head(config.trade_top_performance)

# set rank for highest strength
df_top['Rank'] = np.arange(len(df_top))+1

# delete existing data
database.delete_all_symbols_by_market_phase()

# insert new symbols
for index, row in df_top.iterrows():
    database.insert_symbols_by_market_phase( 
                                            row['Symbol'],
                                            row['Price'],
                                            row['DSMA50'],
                                            row['DSMA200'],
                                            row['Market_Phase'],
                                            row['Perc_Above_DSMA50'],
                                            row['Perc_Above_DSMA200'],
                                            row['Rank']
                                            )
    
selected_columns = df_top[["Symbol","Price","Market_Phase"]]
df_top_print = selected_columns.copy()
# reset the index and set number beginning from 1
df_top_print = df_top_print.reset_index(drop=True)
df_top_print.index += 1

msg = f"Top {str(config.trade_top_performance)} performance coins:"
print(msg)
print(df_top_print.to_string(index=True))

telegram.send_telegram_message(telegram.telegram_token_market_phases, "", msg)
telegram.send_telegram_message(telegram.telegram_token_market_phases, "", df_top_print.to_string(index=True))

# create file to import to TradingView with the list of top performers and symbols in position 
df_tv_list = database.get_distinct_symbol_by_market_phase_and_positions()

df_top = df_tv_list

df_tv_list['symbol'] = "BINANCE:"+df_tv_list['symbol']
# Write DataFrame to CSV file
filename = "Top_performers_"+trade_against+".txt" 
df_tv_list.to_csv(filename, header=False, index=False)
msg = "TradingView List:"
telegram.send_telegram_message(telegram.telegram_token_market_phases, "", msg)
telegram.send_telegram_file(telegram.telegram_token_market_phases, filename)
#---------------------------------------------

if not df_top.empty:

    # remove coins from position files that are not top performers in accumulation or bullish phase
    database.delete_positions_not_top_rank()
    
    # add top rank coins with positive returns to positions files
    database.add_top_rank_to_position()

    # delete rows with calc completed and keep only symbols with calc not completed
    database.delete_symbols_to_calc_completed()

    # add the symbols with open positions to calc 
    database.add_symbols_with_open_positions_to_calc()
    
    # add the symbols in top rank to calc
    database.add_symbols_top_rank_to_calc()

    # calc best ema for each symbol on 1d, 4h and 1h time frame and save on positions table
    add_symbol.main()    

else:

    # if there are no symbols in accumulation or bullish phase remove all not open from positions
    database.delete_all_positions_not_open()

# Close the database connection
database.connection.close()

# inform that ended
telegram.send_telegram_message(telegram.telegram_token_market_phases, telegram.EMOJI_STOP, "End")

# calculate execution time
stop = timeit.default_timer()
total_seconds = stop - start

duration = database.calc_duration(total_seconds)

msg = f'Execution Time: {duration}'
print(msg)
telegram.send_telegram_message(telegram.telegram_token_market_phases, "", msg)


