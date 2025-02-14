CREATE TABLE IF NOT EXISTS Settings (
    name TEXT PRIMARY KEY,
    value TEXT,
    comments TEXT
)


-- Blacklist
PRAGMA foreign_keys = OFF;

-- Step 1: Create a new table with the correct schema
CREATE TABLE "Blacklist_New" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Symbol" TEXT UNIQUE
);

-- Step 2: Copy data from the old table to the new table
INSERT INTO Blacklist_New (Id, Symbol)
SELECT Id, Symbol FROM Blacklist;

-- Step 3: Drop the old table
DROP TABLE Blacklist;

-- Step 4: Rename the new table to the original name
ALTER TABLE Blacklist_New RENAME TO Blacklist;

PRAGMA foreign_keys = ON;


-- update db version
PRAGMA user_version = 20250105
