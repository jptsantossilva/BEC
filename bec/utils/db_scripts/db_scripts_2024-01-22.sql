ALTER TABLE Backtesting_Results RENAME TO Backtesting_Results_old;

CREATE TABLE Backtesting_Results (
  Id INTEGER,
  Symbol TEXT,
  Ema_Fast INTEGER,
  Ema_Slow INTEGER,
  Time_Frame TEXT,
  Return_Perc REAL,
  BuyHold_Return_Perc REAL,
  Backtest_Start_Date TEXT,
  Backtest_End_Date TEXT,
  Strategy_Id TEXT,
  PRIMARY KEY(Id),
  CONSTRAINT symbol_time_frame_strategy_unique UNIQUE(Symbol, Time_Frame, Strategy_Id)
);

INSERT INTO Backtesting_Results
SELECT
  Id,
  Symbol,
  Ema_Fast,
  Ema_Slow,
  Time_Frame,
  Return_Perc,
  BuyHold_Return_Perc,
  Backtest_Start_Date,
  Backtest_End_Date,
  Strategy_Id
FROM Backtesting_Results_old;

DROP TABLE Backtesting_Results_old;


-- update db version
PRAGMA user_version = 20240122
