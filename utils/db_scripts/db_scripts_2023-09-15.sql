-- Create the NewBalances table if it doesn't exist
CREATE TABLE IF NOT EXISTS NewBalances (
    Id INTEGER PRIMARY KEY,
    Date TEXT,
    Asset TEXT,
    Balance REAL,
    USD_Price REAL,
    BTC_Price REAL,
    Balance_USD REAL,
    Balance_BTC REAL,
    Total_Balance_Of_BTC REAL
);

-- Check if the Balances table exists and insert data if it does
INSERT INTO NewBalances (Date, Asset, Balance, USD_Price, BTC_Price, Balance_USD, Balance_BTC, Total_Balance_Of_BTC)
SELECT Date, Asset, Balance, 0.0 AS USD_Price, 0.0 AS BTC_Price, Balance_USD, 0.0 AS Balance_BTC, Total_Balance_Of_BTC
FROM Balances;

-- Drop the Balances table if it exists
DROP TABLE Balances;

-- Rename the NewBalances table to Balances
ALTER TABLE NewBalances RENAME TO Balances;

-- Remove the data from the last 30 days in order to get the most recent data related to BTC daily asset balance when getting the snapshot
DELETE FROM Balances
WHERE DATE >= date('now', '-30 days') AND DATE <= date('now');