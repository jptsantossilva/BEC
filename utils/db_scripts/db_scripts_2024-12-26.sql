-- Enable foreign key support
PRAGMA foreign_keys = OFF;

-- Create Settings table
CREATE TABLE IF NOT EXISTS Settings (
    name TEXT PRIMARY KEY,
    value TEXT,
    comments TEXT
);

-- Blacklist Table Migration
-- Step 1: Create a new table with the correct schema
CREATE TABLE IF NOT EXISTS "Blacklist_New" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Symbol" TEXT UNIQUE
);

-- Step 2: Copy data from the old table to the new table (if it exists)
INSERT INTO Blacklist_New (Id, Symbol)
SELECT Id, Symbol FROM Blacklist;

-- Step 3: Drop the old table if it exists
DROP TABLE IF EXISTS Blacklist;

-- Step 4: Rename the new table to the original name
ALTER TABLE Blacklist_New RENAME TO Blacklist;

-- Re-enable foreign key support
PRAGMA foreign_keys = ON;

-- Update database version
PRAGMA user_version = 20241226;
