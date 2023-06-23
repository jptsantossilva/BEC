import datetime
import pytz
import schedule
import time

import utils.config as config
from main import scheduled_run as run_bot
from utils.exchange import create_balance_snapshot as run_bs
from symbol_by_market_phase import scheduled_run as run_mp
from signals.super_rsi import run as run_srsi

def run_this(msg):
    # Get the current UTC+0 time
    utc_time = datetime.datetime.now(pytz.timezone('UTC'))
    # Get the current time on the computer
    local_time = datetime.datetime.now()    
    print(f"{msg} - Current UTC+0 time: {utc_time} | Current local time: {local_time}")


def run_bot_1h():
    time_frame = "1h"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_bot_4h():
    time_frame = "4h"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_bot_1d():
    time_frame = "1d"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_mp():
    time_frame= "1d"
    trade_against = config.trade_against
    run_mp(time_frame=time_frame, trade_against=trade_against)

def run_balance_snapshot():
    run_bs(telegram_prefix="")

def run_super_rsi():
    run_srsi()

# tests
# run_bot_1h()
# run_bot_4h()
# run_bot_1d()
# run_mp()
# run_balance_snapshot()

local_time = datetime.datetime.now()    
utc_time = datetime.datetime.now(pytz.timezone('UTC'))
print(f"START - Current UTC+0 time: {utc_time} | Current local time: {local_time}")
    
# Define the time intervals in UTC+0 timezone
utc = pytz.timezone('UTC')

# Schedule bot 1h to run every 1 hour
schedule.every().hour.at(":00").do(run_bot_1h)

# Schedule tbot 4h to run every 4 hours
# Define the specific times to run the function every 4 hours
times = ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00']
# Schedule the function to run every 4 hours at the specified times in UTC+0 timezone
for t in times:
    schedule.every().day.at(t).do(run_bot_4h)

# Schedule bot 1d to run every day at 0:00 in UTC+0 timezone
schedule.every().day.at('00:00').do(run_bot_1d)

# Schedule balance snaphsot to run every 1 hour 
schedule.every().hour.at(":00").do(run_balance_snapshot)

# Schedule market phases to run every day
schedule.every().day.at('00:00').do(run_mp)

# Schedule super-rsi to run every 15 minutes
times = [':00', ':15', ':30', ':45']
for t in times:
    schedule.every().hour.at(t).do(run_super_rsi)

while True:
    # Run any pending jobs
    schedule.run_pending()

    # Wait for 1 second before checking again
    time.sleep(1)
