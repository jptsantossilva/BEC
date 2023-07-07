import datetime as dt
import pytz
from scheduler import Scheduler
# import scheduler.trigger as trigger
import time

import utils.config as config
from main import scheduled_run as run_bot
from utils.exchange import create_balance_snapshot as run_bs
from symbol_by_market_phase import scheduled_run as run_mp
from signals.super_rsi import run as run_srsi

def run_this(msg):
    # Get the current UTC+0 time
    utc_time = dt.datetime.now(pytz.timezone('UTC'))
    # Get the current time on the computer
    local_time = dt.datetime.now()   
    # msg = "TEST" 
    print(f"{msg} - Current UTC+0 time: {utc_time} | Current local time: {local_time}")


def run_bot_1h():
    run_this("B1H")
    time_frame = "1h"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_bot_4h():
    run_this("B4H")
    time_frame = "4h"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_bot_1d():
    run_this("B1D")
    time_frame = "1d"
    run_mode = "test"
    run_bot(time_frame, run_mode)

def run_mp():
    run_this("MP")
    time_frame= "1d"
    trade_against = config.trade_against
    run_mp(time_frame=time_frame, trade_against=trade_against)

def run_balance_snapshot():
    run_this("BAL")
    run_bs(telegram_prefix="")

def run_super_rsi():
    run_this("SRSI")
    run_srsi()
    
# tests
# run_bot_1h()
# run_bot_4h()
# run_bot_1d()
# run_mp()
# run_balance_snapshot()

local_time = dt.datetime.now()    
utc_time = dt.datetime.now(pytz.timezone('UTC'))
print(f"START - Current UTC+0 time: {utc_time} | Current local time: {local_time}")
    

# Define the time intervals in UTC+0 timezone
# utc = pytz.timezone('UTC')
# schedule = Scheduler(tzinfo=dt.timezone.utc)
tz_utc = pytz.timezone('UTC')
schedule = Scheduler(tzinfo=dt.timezone.utc, n_threads=0)

# Schedule bot 1h to run every 1 hour
# schedule.every().hour.at(":00").do(run_bot_1h)
job_b1h = schedule.hourly(dt.time(minute=00, tzinfo=tz_utc), run_bot_1h)

# Schedule bot 4h to run every 4 hours
# Define the specific times to run the function every 4 hours
times = [0, 4, 8, 12, 16, 20]
# Schedule the function to run every 4 hours at the specified times in UTC+0 timezone
for t in times:
    # schedule.every().day.at(t).do(run_bot_4h)
    job_b4h = schedule.daily(dt.time(hour=t, tzinfo=tz_utc), run_bot_4h)

# Schedule bot 1d to run every day at 0:00 in UTC+0 timezone
# schedule.every().day.at('00:00').do(run_bot_1d)
job_b1d = schedule.daily(dt.time(hour=0, tzinfo=tz_utc), run_bot_1d)

# Schedule balance snapshot to run every 1 hour 
# schedule.every().hour.at(":00").do(run_balance_snapshot)
job_balance = schedule.hourly(dt.time(minute=00, tzinfo=tz_utc), run_balance_snapshot)

# Schedule market phases to run every day
# schedule.every().day.at('00:00').do(run_mp)
job_mp = schedule.daily(dt.time(hour=0, tzinfo=tz_utc), run_mp)

# Schedule super-rsi to run every 15 minutes
times = [00, 15, 30, 45]
for t in times:
    # schedule.every().hour.at(t).do(run_super_rsi)
    job_srsi = schedule.hourly(dt.time(minute=t, tzinfo=tz_utc), run_super_rsi)

print(schedule) 

while True:
    # Run any pending jobs
    # schedule.run_pending()
    schedule.exec_jobs()

    # Wait for 1 second before checking again
    time.sleep(1)

