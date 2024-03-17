ALTER TABLE Positions ADD Take_Profit_3 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE Positions ADD Take_Profit_4 INTEGER NOT NULL DEFAULT 0;

-- update db version
PRAGMA user_version = 20240303
