-- Migrate executed legacy TP flags to Take_Profits_JSON, then drop legacy columns.
-- Run this before upgrading if a production database still has Take_Profit_1..4.
-- The app startup migration performs the same operation without requiring JSON1.

BEGIN TRANSACTION;

UPDATE Positions
SET Take_Profits_JSON = CASE
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[1,2,3,4]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
        THEN '[1,2,3]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[1,2,4]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[1,3,4]'
    WHEN COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[2,3,4]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_2, 0) != 0
        THEN '[1,2]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
        THEN '[1,3]'
    WHEN COALESCE(Take_Profit_1, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[1,4]'
    WHEN COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_3, 0) != 0
        THEN '[2,3]'
    WHEN COALESCE(Take_Profit_2, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[2,4]'
    WHEN COALESCE(Take_Profit_3, 0) != 0
     AND COALESCE(Take_Profit_4, 0) != 0
        THEN '[3,4]'
    WHEN COALESCE(Take_Profit_1, 0) != 0 THEN '[1]'
    WHEN COALESCE(Take_Profit_2, 0) != 0 THEN '[2]'
    WHEN COALESCE(Take_Profit_3, 0) != 0 THEN '[3]'
    WHEN COALESCE(Take_Profit_4, 0) != 0 THEN '[4]'
    ELSE '[]'
END
WHERE Take_Profits_JSON IS NULL
   OR Take_Profits_JSON = ''
   OR Take_Profits_JSON = '[]';

-- Requires SQLite 3.35.0 or newer. For older SQLite versions, let the app
-- startup migration rebuild the Positions table through _drop_table_columns.
ALTER TABLE Positions DROP COLUMN Take_Profit_1;
ALTER TABLE Positions DROP COLUMN Take_Profit_2;
ALTER TABLE Positions DROP COLUMN Take_Profit_3;
ALTER TABLE Positions DROP COLUMN Take_Profit_4;

COMMIT;
