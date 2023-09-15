
# Change Log
All notable changes to this project will be documented in this file.

## [2023-09-15]
 
### Added
- Dashboard - Unrealized PnL - added Date to show when position was opened
- Dashboard - Daily Account Balance now also in BTC. Was only in USD. Useful if you are more interested in accumulating satoshi rather than US dollars.
### Changed
- Dashboard - Settings - Trade against - removed BUSD option since it is going to be delisted. Check official announcement https://www.binance.com/en/support/announcement/binance-encourages-users-to-convert-busd-to-other-stablecoins-prior-to-february-2024-d392843e81fd4bc3a5f7e219aa01f34d 
- Dashboard - Unrealized PnL - Buy_Price with increased decimal precision to make sure complete number is shown.    
- Market Phases - removed trade against input argument. It is getting from config file. This is useful to apply the correct trade against when changing trade against on settings. Previously there was a need to change the call for the symbols_by_market_phase.py in the cron jobs.
- Backtest - EMAs with steps of 10 instead of 5. The idea is to avoid overfitting and decrease duration of the daily backtests.
General - create balance snapshot - went from occurring every time the bot runs to when it calculates market phases. So now it only happens once a day.
### Fixed
- Dashboard - Realized PnL - Error fix when Buy_Price is None    

## [2023-09-08]
 
### Added
- Dashboard - Realized PnL - Added Buy_Price, Buy_Date, Buy_Qty to easily check buy and sell order details.
- Settings - When changing Trading Against (only stables for now), for example from BUSD to USDT, it is now able to deal with a buy position in BUSD and sell it in USDT.   
### Changed
 - 
### Fixed
- Dashboard - Top Performers - Download top perf to TradingView is now using correct trade against symbol. The issue occurred when changing trade against. 
- Database - When position sold was not cleaning the Buy_Order_Id   

## [2023-07-27]
 
### Added
- Dashboard - Realized PnL - Show EMA fast and EMA slow values. 
### Changed
- 
### Fixed
- General - fix bug when calculating position duration due to position open date using milliseconds or just seconds 

## [2023-07-21]
 
### Added
- Dashboard - Added update feature. Easy way to update BEC to most recent version. 
### Changed
- Dashboard - About information updated
### Fixed
-

## [2023-07-20]
 
### Added
-
### Changed
- Dashboard - Top performers description update
- General - Avoid quote order qty order if not supported
### Fixed
- Signals - Fix symbol uppercase
- General - Fix error message on getting binance historical chart data

## [2023-07-17]
 
### Added
-
### Changed
- 
### Fixed
- General - Makes 3 attempts to get binance historical chart data.


## [2023-07-07]
 
### Added
-
### Changed
- General - The Scheduler is still under development, so balances are still calculated at the end of each bot run.
### Fixed
- Dashboard - Realized and Unrealized profits - PnL_Values with 8 decimals places if trading against BTC 

## [2023-06-27]
 
### Added
- Scheduler - Scheduler will be responsable to run bot on each time frame, market phases and signals at the correct time using UTC timezone.
- Dashboard - Top performers are now saved for historical purpose. Useful for finding out which symbols spend the most number of days in the bullish or accumulating phase. 
### Changed
- General - avoid error message in telegram if error is related to non-existing trading pair 
- Dashboard - Auto adjust height on chart asset balances
- Dashboard - Current Total Balance - display value
- General - Code adaptation to use scheduler
### Fixed
- Dashboard - Top Performers - display the prices with 8 decimal places 
- Dashboard - Balances - avoid error if balances table is empty; refresh button to create balance snapshot
- Dashboard - User reset password fix

## [2023-05-19]
 
### Added
- Dashboard - Total Balance and Asset Balances - get data from last 7d, 30d, 90d, YTD and All-Time
- Dashboard - Top Performers - Download as a TradingView List
- Dashboard - Unrealized PnL - Show ema_slow and ema_fast columns
- Database - csv_to_sqlite.py file update with step-by-step to migrate BEC from csv to sqlite database
### Changed
- Dashboard - Realized PnL - format decimal places on PnL_Perc and PnL_Value columns
- General - Updated files sctructure and number of telegram bots was reduced 
### Fixed
- Database - Ignore when there is no new data to add to balances table

## [2023-05-05]
 
### Added
- Dashboard - user login
- Dashboard - blacklist grid editable
- Dashboard - Settings tab to adjust bot settings
- Dashboard - Charts  - Assets balances and Total USD balance 
### Changed
- Dashboard - web dashboard no longer supports multiple bots with different trading pairs. Instead, each bot/trading pair now has its own dashboard.  
### Fixed
- 

## [2023-04-18]
 
### Added
- Sqlite database. Migration from csv files to database.  
- Dashboad - force exit position; show top performers, blacklist and Best Ema
### Changed
- 
### Fixed
- 

## [2023-04-04]
 
### Added
- 
### Changed
- Web dashboard - support for multiple bots. Side panel to choose which bot you want to get data from. Bots must all be located in the same parent folder.
### Fixed
- 

## [2023-03-31]
 
### Added
- PnL analysis web dashboard
### Changed
- 
### Fixed
- Fulfill currentPrice when buy position to make sure PnL is showing correct results

## [2023-03-21]
 
### Added
- PnL analysis for closed positions within a year/month and current open positions
### Changed
- 
### Fixed
- PnL value - decimal values corrected
- TradingView import list with symbols in position

## [2023-03-13]
 
### Added
-
### Changed
- set pandas display.precision to 8. Useful when trading against BTC
- execution time format d h m s
### Fixed
- round values to n decimals depending on trade against.


## [2023-03-05]
 
### Added
- TradingView watchlist file with top performers to import to TradingView  
### Changed
- Calculate the best ema for top performers and also for those where we have positions and are no longer top performers
### Fixed
- 

## [2023-03-03]
 
### Added
- Trading - Trade against BTC 
- Risk Management - set stop loss percentage 
### Changed
- Backtesting - runs backtesting on a daily basis for all top performers. This will constantly adjust best ema pairs.
- Trading - print opened and closed positions     
- Market Phases - removed best performers numbers column. This list is automatically ordered from the best to the worst performer.  
### Fixed
- Some message exceptions from Binance API were not possible to send by telegram  
 
