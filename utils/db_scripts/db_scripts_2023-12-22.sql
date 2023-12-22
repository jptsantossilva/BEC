
-- change table name
ALTER TABLE Best_Ema RENAME TO Backtesting_Results;

-- Add columns to the Backtesting_Results table
ALTER TABLE Backtesting_Results ADD Backtest_End_Date TEXT;
ALTER TABLE Backtesting_Results ADD Strategy_Id TEXT;

-- add default strategy to existing values
UPDATE Backtesting_Results SET Strategy_Id = "ema_cross_with_market_phases";

-- add sell percentage
UPDATE Orders ADD Sell_Perc	INTEGER

ALTER TABLE Positions ADD Take_Profit_1 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE Positions ADD Take_Profit_2 INTEGER NOT NULL DEFAULT 0;

-- update db version
PRAGMA user_version = 20231220
