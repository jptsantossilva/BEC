
# Change Log
All notable changes to this project will be documented in this file.

## [2025-03-27]
### Fixed 
- Trading - Fixed an issue where some crypto assets were not showing up correctly in the balance snapshot. This also affected the calculation of top-performing assets, which could prevent the list of symbols to trade from updating as expected. Backtesting results were also impacted by this issue.

## [2025-03-25]

### Added
- General - Added an automated script to detect new Binance delisting announcements and sends alerts via Telegram if new delistings are detected.
- General - Moved settings from the config.yaml file to the Settings table in the database. This change prevents rare cases where the YAML file could lose its configurations.
- Dashboard - Auto Switch - Previously, when using auto-switch and trading against BTC, the system would automatically convert BTC to USDC upon triggering the auto-switch sell event. A new option - Stablecoin for Auto-Switching - now allows users to select their preferred stablecoin (USDT or USDC) for conversion.
### Changed
- Dashboard - Run Backtesting - Removed the Run Backtesting button as it no longer serves a functional purpose. It was only used for testing.
- Dashboard - The maximum number of open positions is no longer limited to 50.
- Dashboard - Next Trade Position Size – Improved the display format to enhance readability.
- Update - The update process no longer saves backtesting files in the backup folder, as they can take up several gigabytes of storage. Increased the compression ratio of backup files to reduce their size further.
### Fixed 
- Dashboard - Blacklist - Resolved issues where blacklist items were not being correctly stored or deleted.
- Dashboard - Backtesting Results - Fixed an issue where the start and end date filters were not applied correctly. Previously, if a backtest did not fully fall within the selected date range, no results were shown. Now, results are displayed when the backtest period overlaps with the selected date range.
- Dashboard - Backtesting Results - Corrected the trade counter for positions with a return < -20% to ensure accurate tracking. 

## [2024-12-26]

### Added
- Trading - added the option to use trade against USDC
### Changed
- 
### Fixed 
- 

## [2024-09-25]

### Added
- 
### Changed
- 
### Fixed 
- Trading - Auto Switch - The auto switch is getting the latest candle data of BTC to make decisions. Previously, was mistakenly including the current real-time price along with daily close data. This has been corrected to only use the previous daily close prices.

## [2024-09-22]

### IMPORTANT NOTE:
- Its is mandatory to restart the server after update due to an upgrade on the web framework. 
 
### Added
- 
### Changed
- 
### Fixed 
- Trading - An error occurred when a symbol, with an open position, was delisted from the exchange, preventing access to its price history. To resolve this issue and allow for future trading opportunities, an option to delete the position has been introduced.

## [2024-04-09]
 
### Added
- 
### Changed
- 
### Fixed 
- Trading - In certain instances, disparities were observed in the EMA values. To address this issue, it was necessary to augment the quantity of historical price data required for accurate EMA calculations.

## [2024-04-08]
 
### Added
- Trading - Telegram - When 'Trade Against Auto Switch' is enabled and a trigger occurs, users will receive a Telegram alert message notifying them that the auto switch has occurred.
### Changed
- Backtesting - Previously, in the backtesting process, the Best EMAs results were being updated without distinction for both open positions and symbols waiting for position to be opened. However, this update apply the results exclusively on symbols with no open positions. Through analysis, it was discovered that updating the best EMA values for an open position led to premature position closures, resulting in missed profit opportunities.
- Dashboard - Enable/disable trading in a time-frame - Previously, disabling the bot on a particular timeframe would result in open positions on that same timeframe becoming stuck, necessitating manual forced closing to close them; otherwise, they would remain open indefinitely. With this update, while the bot on the disabled timeframe will refrain from initiating new positions, it will still actively seek to sell existing positions based on the established sell strategy conditions.
- Trading - Telegram - The messages indicating 'Sell/Buy condition not fulfilled' now include the pair of EMAs that were used to determine the condition.
### Fixed
- Dashboard - Backtesting Trades - Due to the daily occurrence of backtesting, the last trade's end date differed, resulting in duplicated trades and inaccurate data for analysis. To ensure the most up-to-date backtesting trade results, previous data is now replaced with the most recent data. 
- Trading - Auto Switch - Min position size is now being updated

## [2024-03-29]
 
### Added
- Dashboard - Backtesting Results - Filters for start date, end date, use top performance symbols and return percentage > 0.  
### Changed
- 
### Fixed
- 

## [2024-03-17]
 
### Added
- Dashboard - Settings - Added 2 more take profit levels. TP3 and TP4.
- Dashboard - Backtesting Results - Backtesting Trades - Trades added to show the return percentage by levels for every backtesting trade. This can be usefull to decide the values to insert on the Take Profit levels.  
- Dashboard - Settings - Bot Prefix - When there are multiple instances of BEC running in the same server, the prefix is useful to distinguish which BEC the telegram message belongs to.
- Dashboard - Settings - Max Number Open Positions - Displays a caption that gives usefull information regarding the calculation on the position value for the next position.
- Dashboard - Locked Values - Added the option to release a single or multiple locked values.  
### Changed
- Dashboard - Unrealized PnL - Shows ocupied and Max Number Open Positions on The Unrealized PnL Total grid.  
- Trading - When selling a position, partial or full, if the value is below minimal required by the exchange it will throw a custom message instead of a error code message.
### Fixed
- Dashboard - Realized PnL - Exit reason column size adjusted to show full text
- BEC Update - Avoid removing backtesting html and csv files when updating BEC to newer versions 

## [2024-03-03]
 
### Added
- 
### Changed
-  
### Fixed
- General - Fix for the error: BUY create_order - TypeError(float() argument must be a string or a real number, not NoneType)

## [2024-03-02]
 
### Added
- 
### Changed
-  
### Fixed
- Dashboard - Settings - Locked Values - Was not locking values when enabled.

## [2024-02-23]
 
### Added
- Dashboard - Settings - Locked Values - When enabled, means that any amount obtained from partially selling a position will be temporarily locked and cannot be used to purchase another position until the entire position is sold. When disabled, partial sales can be freely reinvested into new positions. It's important to note that when disabled it may increase the risk of larger position amounts, as funds from partial sales may be immediately reinvested without reservation.
- Dashboard - Unrealized PnL - Position_Value column added. We had Qty and Buy_Price, why not multiply both :)
- Dashboard - Realized PnL - Buy_Position_Value and Sell_Position_Value columns added. We had Qty and Price, why not multiply both :)
### Changed
-  
### Fixed
- Dashboard - Realized PnL - PnL_Perc and PnL_Value were not correctly ordered when user clicked on column header   

## [2024-02-17]
 
### Added
-  
### Changed
-  
### Fixed
- Trading - When profit levels were triggered, the PnL value was not being updated immediately after. 
- Backtesting - After backtesting calculation, the symbols in table Symbols_To_Calc were not being set as completed.  

## [2024-02-06]
 
### Added
-  
### Changed
-  
### Fixed
- Backtesting - variable name pSymbol was not defined.

## [2024-01-25]
 
### Added
-  
### Changed
-  
### Fixed
- General - Technicals - SMA and EMA values now are more accurate and match those used on TradingView. The main difference is that we are getting more historical price data like we do on backtesting.

## [2024-01-24]
 
### Added
-  
### Changed
-  
### Fixed
- General - Trade - Some BEC instances were opening positions while others don't. Data collection doesn't always coincide precisely with the closing time of a candle. As a result, the last row in our candle dataset represents the most current price information. This becomes significant when applying technical analysis, as it directly influences the accuracy of metrics and indicators. The implications extend to the decision-making process for buying or selling, making it essential to account for the real-time nature of the last row in our data. So, now we are now making sure the last row of the dataset is the one from the last candle close. 
- General - Positions table had duplicated rows for the same symbol and time frame. This happened due to getting backtesting results without filtering the strategy.
- General - Technicals - SMA and EMA values now are more accurate and match those used on TradingView. The main difference is that we are getting more historical price data like we do on backtesting.

## [2024-01-22]
 
### Added
-  
### Changed
-  
### Fixed
- General - Best EMA values missing - Backtesting_Results table constraint fixed. It was missing the Strategy_Id as Unique. 

## [2024-01-19]
 
### Added
-  Backtesting - On our daily backtesting, we are now backtesting all Strategies and not only the one being used. With this we are able to compare the results from the used strategy with those strategies not in use.
### Changed
- Telegram - Take Profit level achieved - the sell order message now includes not only TP level % Pnl but also position amount %.     
- Dashboard - Backtesting Results - Backtesting HTML and csv results now have string Open instead of path to file. 
### Fixed
- 

## [2024-01-12]
 
### Added
- Dashboard - Backtesting Results - HTML and CSV link to get statistics and trades for the best strategy results  
### Changed
- Dashboard - Settings - Visual components adjustments
- Telegram - When closing position, partial or full, more info was added to related telegram messages.
- Dashboard - RPQ% - Remaining Position Qty column - moved in the grid for easier reading in mobiles
### Fixed
- Dashboard - Settings - Sometimes settings changes were not being saved 

## [2023-12-22]
 
### Added
- Dashboard - Settings - Added Multi strategy option. There are 3 strategies for USDT and 2 strategies for BTC. There is also a Auto Switch to trade against USDT or BTC. If activated, when BTC is in a bullish or accumulation phase (price above DSMA50 and DSMA200) will convert all assests to BTC and trade against BTC, or else when BTC is in a bear phase will convert all assets to USDT and trade against USDT.  
- Dashboard - Unrealized PnL - Force Selling - you can now sell a position percentage. Before you could only sell full position.
- Dashboard - Settings - Implemented a two-tiered take-profit system as a risk control feature. If the profit percentage reaches the specified level, a percentage of the position will be sold.
- Dashboard - Backtesting Results - renamed tab from Best Ema to Backtesting Results. Filter by strategies and symbols to compare results.
### Changed
- Dashboard - renamed Dashboard python file from pnl.py to dashboard.py. Make sure to change your cronjob. See installation instructions [here](https://docs.google.com/document/d/1ERtxjcdrznMWXragmBh5ZimIn6_PGn2sde0j_x4CktA/edit?usp=sharing) for details. 
### Fixed
- Dashboard - Daily balance Snapshot calculation.

## [2023-09-27]
 
### Added
- Signals - Super-RSI - Added BTC and ETH to calculate in each iteration. Even if they are not in an open position.
### Changed
-
### Fixed
-    

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
 
