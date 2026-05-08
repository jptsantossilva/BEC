ALTER TABLE Backtesting_Results ADD COLUMN Trades INTEGER;
ALTER TABLE Backtesting_Results ADD COLUMN Win_Rate_Perc REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Best_Trade_Perc REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Worst_Trade_Perc REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Avg_Trade_Perc REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Max_Trade_Duration TEXT;
ALTER TABLE Backtesting_Results ADD COLUMN Avg_Trade_Duration TEXT;
ALTER TABLE Backtesting_Results ADD COLUMN Profit_Factor REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Expectancy_Perc REAL;
ALTER TABLE Backtesting_Results ADD COLUMN SQN REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Kelly_Criterion REAL;
ALTER TABLE Backtesting_Results ADD COLUMN Max_Drawdown_Perc REAL;

-- update db version
PRAGMA user_version = 20260212;
