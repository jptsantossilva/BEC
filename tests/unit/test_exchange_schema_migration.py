import sqlite3

import pytest

from bec.db.exchange_schema import apply_exchange_aware_schema
from bec.db.migrations import (
    MIGRATIONS,
    PendingManualMigrationError,
    apply_database_migrations,
    apply_pending_migrations,
    prepare_startup_migrations,
    run_dry_run,
)


def _create_legacy_database(path):
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE Orders (
                Id INTEGER PRIMARY KEY, Exchange_Order_Id TEXT, Symbol TEXT,
                Price REAL, Qty REAL
            );
            CREATE TABLE Positions (Id INTEGER PRIMARY KEY, Symbol TEXT);
            CREATE TABLE Backtesting_Results (
                Id INTEGER PRIMARY KEY, Symbol TEXT, Time_Frame TEXT, Strategy_Id TEXT,
                UNIQUE(Symbol, Time_Frame, Strategy_Id)
            );
            CREATE TABLE Backtesting_Trades (
                Id INTEGER PRIMARY KEY, Symbol TEXT, Time_Frame TEXT, Strategy_Id TEXT,
                EntryTime TEXT, ExitTime TEXT,
                UNIQUE(Symbol, Time_Frame, Strategy_Id, EntryTime, ExitTime)
            );
            CREATE TABLE Balances (
                Id INTEGER PRIMARY KEY, Date TEXT, Asset TEXT, Balance REAL,
                UNIQUE(Date, Asset)
            );
            CREATE TABLE Symbols_To_Calc (Id INTEGER PRIMARY KEY, Symbol TEXT);
            CREATE TABLE Symbols_By_Market_Phase (Id INTEGER PRIMARY KEY, Symbol TEXT);
            CREATE TABLE Symbols_By_Market_Phase_Historical (Id INTEGER PRIMARY KEY, Symbol TEXT);
            CREATE TABLE Locked_Values (
                Id INTEGER PRIMARY KEY, Position_Id INTEGER, Released INTEGER,
                FOREIGN KEY(Position_Id) REFERENCES Positions(Id)
            );
            CREATE TABLE Backtesting_Jobs (
                id INTEGER PRIMARY KEY, symbol TEXT, strategy_id TEXT, timeframe TEXT
            );
            CREATE TABLE Monte_Carlo_Jobs (id INTEGER PRIMARY KEY, symbol TEXT);
            CREATE TABLE Monte_Carlo_Results (
                id INTEGER PRIMARY KEY, Symbol TEXT NOT NULL, Time_Frame TEXT NOT NULL,
                Strategy_Id TEXT NOT NULL, Method TEXT NOT NULL, Scenarios INTEGER NOT NULL,
                Valid_Scenarios INTEGER NOT NULL, Seed INTEGER NOT NULL, Created_At TEXT NOT NULL,
                UNIQUE(Symbol, Time_Frame, Strategy_Id, Method)
            );
            CREATE TABLE Auto_Switch_Signals (
                id INTEGER PRIMARY KEY, strategy_id TEXT NOT NULL, symbol TEXT NOT NULL,
                signal TEXT NOT NULL, signal_timeframe TEXT NOT NULL,
                candle_id TEXT NOT NULL, processed_at TEXT NOT NULL,
                UNIQUE(strategy_id, symbol, signal, signal_timeframe, candle_id)
            );
            CREATE TABLE Signals_Log (Symbol TEXT);
            CREATE TABLE Settings (name TEXT PRIMARY KEY, value TEXT);

            INSERT INTO Orders VALUES (1, '123', 'BTCUSDC', 60000, 0.01);
            INSERT INTO Positions VALUES (1, 'ETHUSDT');
            INSERT INTO Backtesting_Results VALUES (1, 'BTCUSDC', '1h', 'ema');
            INSERT INTO Backtesting_Trades VALUES (1, 'BTCUSDC', '1h', 'ema', 'a', 'b');
            INSERT INTO Balances VALUES (1, '2026-07-01', 'USDC', 100);
            INSERT INTO Symbols_To_Calc VALUES (1, 'UNKNOWNPAIR');
            INSERT INTO Symbols_By_Market_Phase VALUES (1, 'ETHUSDT');
            INSERT INTO Locked_Values VALUES (1, 1, 1);
            INSERT INTO Settings VALUES ('trade_against', 'USDT');
            """
        )
        apply_pending_migrations(connection, MIGRATIONS[:1], allow_rebuild=True)


def test_exchange_schema_rebuild_is_manual_and_backfills_binance(tmp_path):
    database = tmp_path / "legacy.db"
    _create_legacy_database(database)

    with sqlite3.connect(database) as connection:
        with pytest.raises(PendingManualMigrationError):
            prepare_startup_migrations(connection, MIGRATIONS, new_database=False)

    report = run_dry_run(database, MIGRATIONS)
    assert report.applied == ["2:exchange_aware_schema"]
    assert report.unresolved_legacy_symbols == ["UNKNOWNPAIR"]

    applied = apply_database_migrations(database, MIGRATIONS, backup=True)
    assert applied.applied == ["2:exchange_aware_schema"]
    assert applied.unresolved_legacy_symbols == ["UNKNOWNPAIR"]

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT Code, Enabled, Is_Default, Quote_Asset FROM Exchanges"
        ).fetchall() == [("binance", 1, 1, "USDT")]
        assert connection.execute(
            "SELECT Exchange_Id, Symbol, Symbol_Normalized, Exchange_Symbol, "
            "Base_Asset, Quote_Asset, Order_Status, Executed_Qty, Average_Price, Net_Qty "
            "FROM Orders"
        ).fetchone() == (
            1, "BTCUSDC", "BTC/USDC", "BTCUSDC", "BTC", "USDC",
            "filled", 0.01, 60000.0, 0.01,
        )
        assert connection.execute(
            "SELECT Exchange_Id, Symbol_Normalized FROM Positions"
        ).fetchone() == (1, "ETH/USDT")
        assert connection.execute(
            "SELECT Exchange_Id FROM Balances"
        ).fetchone() == (1,)
        connection.execute(
            """
            INSERT OR REPLACE INTO Backtesting_Results
                (Symbol, Time_Frame, Strategy_Id)
            VALUES ('BTCUSDC', '1h', 'ema')
            """
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM Backtesting_Results"
        ).fetchone()[0] == 1
        connection.execute(
            """
            INSERT INTO Exchanges
                (Id, Code, Name, Enabled, Is_Default, Trading_Mode)
            VALUES (2, 'test-exchange', 'Test Exchange', 0, 0, 'spot')
            """
        )
        connection.execute(
            """
            INSERT INTO Backtesting_Results
                (Symbol, Time_Frame, Strategy_Id, Exchange_Id)
            VALUES ('BTCUSDC', '1h', 'ema', 2)
            """
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM Backtesting_Results"
        ).fetchone()[0] == 2
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    repeated = apply_database_migrations(database, MIGRATIONS, backup=False)
    assert repeated.pending == []
    assert repeated.applied == []


def test_new_install_exchange_metadata_has_no_active_default(tmp_path):
    database = tmp_path / "new.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Symbol TEXT, Price REAL, Qty REAL);
            CREATE TABLE Positions (Id INTEGER PRIMARY KEY, Symbol TEXT);
            CREATE TABLE Backtesting_Results (
                Id INTEGER PRIMARY KEY, Symbol TEXT, Time_Frame TEXT, Strategy_Id TEXT,
                UNIQUE(Symbol, Time_Frame, Strategy_Id)
            );
            CREATE TABLE Balances (
                Id INTEGER PRIMARY KEY, Date TEXT, Asset TEXT,
                UNIQUE(Date, Asset)
            );
            """
        )
        connection.execute("BEGIN IMMEDIATE")
        apply_exchange_aware_schema(connection, upgraded_install=False)
        connection.commit()

        assert connection.execute(
            "SELECT Code, Enabled, Is_Default FROM Exchanges"
        ).fetchone() == ("binance", 0, 0)

        connection.execute("BEGIN IMMEDIATE")
        apply_exchange_aware_schema(connection, upgraded_install=True)
        connection.commit()
        assert connection.execute(
            "SELECT Code, Enabled, Is_Default FROM Exchanges"
        ).fetchone() == ("binance", 0, 0)
