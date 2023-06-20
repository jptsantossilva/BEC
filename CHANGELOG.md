
# Change Log
All notable changes to this project will be documented in this file.


## [2023-06-15]
 
### Added

### Changed
- General - avoid error message in telegram if error is related to non-existing trading pair 
- Dashboard - Auto adjust height on chart asset balances

### Fixed



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
 
