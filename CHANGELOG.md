
# Change Log
All notable changes to this project will be documented in this file.

## [2023-03-13]
 
### Added
  
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
 
