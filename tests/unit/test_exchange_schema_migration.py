import multiprocessing
import sqlite3

import pytest

from bec.db.exchange_schema import (
    apply_exchange_aware_schema,
    prepare_exchange_schema_for_startup,
    validate_exchange_aware_schema,
)
from bec.db.migrations import (
    MIGRATIONS,
    PendingManualMigrationError,
    apply_database_migrations,
    apply_pending_migrations,
    mark_new_database_current,
    pending_migrations,
    prepare_startup_migrations,
    run_dry_run,
)


def _validate_schema_in_process(database, ready, release):
    with sqlite3.connect(database, timeout=0) as connection:
        validate_exchange_aware_schema(connection)
        ready.set()
        release.wait(5)


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
    assert report.applied == [
        "2:exchange_aware_schema",
        "3:kraken_public_exchange",
        "4:exchange_specific_backtesting",
        "5:kraken_backtesting_defaults",
        "6:gated_kraken_live_execution",
        "7:durable_order_fills",
        "8:okx_configuration",
        "9:backtesting_exchange_context",
        "10:okx_demo_execution",
    ]
    assert report.unresolved_legacy_symbols == ["UNKNOWNPAIR"]

    applied = apply_database_migrations(database, MIGRATIONS, backup=True)
    assert applied.applied == [
        "2:exchange_aware_schema",
        "3:kraken_public_exchange",
        "4:exchange_specific_backtesting",
        "5:kraken_backtesting_defaults",
        "6:gated_kraken_live_execution",
        "7:durable_order_fills",
        "8:okx_configuration",
        "9:backtesting_exchange_context",
        "10:okx_demo_execution",
    ]
    assert applied.unresolved_legacy_symbols == ["UNKNOWNPAIR"]

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT Code, Enabled, Is_Default, Quote_Asset FROM Exchanges"
        ).fetchall() == [
            ("binance", 1, 1, "USDT"),
            ("kraken", 1, 0, "USDC"),
            ("okx", 0, 0, "USDC"),
            ("okx_demo", 0, 0, "USDC"),
        ]
        assert connection.execute(
            "SELECT Code, Buy_Enabled, Sell_Enabled, Partial_Sell_Policy "
            "FROM Exchanges ORDER BY Id"
        ).fetchall() == [
            ("binance", 1, 1, "accumulate"),
            ("kraken", 0, 0, "accumulate"),
            ("okx", 0, 0, "accumulate"),
            ("okx_demo", 0, 0, "accumulate"),
        ]
        assert connection.execute(
            "SELECT Code, Adapter_Id, Execution_Environment "
            "FROM Exchanges ORDER BY Id"
        ).fetchall() == [
            ("binance", "binance", "production"),
            ("kraken", "kraken", "production"),
            ("okx", "myokx", "production"),
            ("okx_demo", "myokx", "demo"),
        ]
        assert connection.execute(
            """
            SELECT COUNT(*) FROM Exchange_Backtesting_Settings ebs
            JOIN Exchanges e ON e.Id=ebs.Exchange_Id
            WHERE e.Code IN ('okx', 'okx_demo')
            """
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT Sizing_Buffer_Pct FROM Exchanges WHERE Code='okx_demo'"
        ).fetchone() == (5.0,)
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='OKX_Demo_Validation_Records'"
        ).fetchone() == (1,)
        assert {row[1] for row in connection.execute("PRAGMA table_info(Exchange_Symbols)")} >= {
            "Amount_Step", "Price_Step", "Min_Amount", "Max_Amount",
            "Min_Cost", "Max_Cost", "Market_Type", "Is_Spot", "Is_Contract",
            "Contract_Size", "Is_Linear", "Is_Inverse", "Settle_Asset",
            "Raw_Metadata_JSON", "Last_Synced_At", "Availability_Status",
        }
        assert connection.execute(
            "SELECT ebs.Commission_Value FROM Exchange_Backtesting_Settings ebs "
            "JOIN Exchanges e ON e.Id=ebs.Exchange_Id WHERE e.Code='kraken'"
        ).fetchone()[0] == pytest.approx(0.004)
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
            "SELECT name FROM sqlite_master WHERE type='table' AND name='Order_Fills'"
        ).fetchone() == ("Order_Fills",)
        assert {row[1] for row in connection.execute("PRAGMA table_info(Orders)")} >= {
            "Executed_Cost",
            "Fees_JSON",
        }
        assert {row[1] for row in connection.execute("PRAGMA table_info(Backtesting_Jobs)")} >= {
            "Exchange_Code",
            "Exchange_Adapter_Id",
            "Exchange_Quote_Asset",
            "Exchange_Execution_Environment",
        }
        assert {row[1] for row in connection.execute("PRAGMA table_info(Backtesting_Results)")} >= {
            "Backtest_Adapter_Id",
            "Backtest_Quote_Asset",
            "Backtest_Execution_Environment",
        }
        assert {row[1] for row in connection.execute("PRAGMA table_info(Monte_Carlo_Jobs)")} >= {
            "Exchange_Code",
            "Exchange_Adapter_Id",
            "Exchange_Quote_Asset",
            "Exchange_Execution_Environment",
        }
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
                    VALUES (5, 'test-exchange', 'Test Exchange', 0, 0, 'spot')
            """
        )
        connection.execute(
            """
                INSERT INTO Backtesting_Results
                    (Symbol, Time_Frame, Strategy_Id, Exchange_Id)
                VALUES ('BTCUSDC', '1h', 'ema', 5)
            """
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM Backtesting_Results"
        ).fetchone()[0] == 2
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    repeated = apply_database_migrations(database, MIGRATIONS, backup=False)
    assert repeated.pending == []
    assert repeated.applied == []


def test_existing_database_startup_only_reads_exchange_schema(tmp_path):
    database = tmp_path / "existing.db"
    _create_legacy_database(database)
    apply_database_migrations(database, MIGRATIONS, backup=True)

    with sqlite3.connect(database) as connection:
        before = connection.total_changes
        statements = []
        connection.set_trace_callback(statements.append)

        prepare_exchange_schema_for_startup(connection, new_database=False)
        prepare_exchange_schema_for_startup(connection, new_database=False)

        assert connection.total_changes == before
        mutating = (
            "ALTER TABLE",
            "CREATE TABLE",
            "DELETE FROM",
            "DROP TABLE",
            "INSERT INTO",
            "REPLACE INTO",
            "UPDATE ",
        )
        assert not [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(mutating)
        ]


def test_exchange_schema_migration_is_data_idempotent(tmp_path):
    database = tmp_path / "idempotent.db"
    _create_legacy_database(database)
    apply_database_migrations(database, MIGRATIONS, backup=True)

    with sqlite3.connect(database) as connection:
        before = connection.total_changes
        apply_exchange_aware_schema(connection, upgraded_install=True)
        connection.commit()
        assert connection.total_changes == before


def test_existing_database_startup_rejects_incomplete_exchange_schema(tmp_path):
    database = tmp_path / "incomplete.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Symbol TEXT)")
        with pytest.raises(RuntimeError, match="Exchange-aware schema validation failed"):
            validate_exchange_aware_schema(connection)


def test_read_only_startup_validation_does_not_block_job_claim(tmp_path):
    database = tmp_path / "concurrent.db"
    _create_legacy_database(database)
    apply_database_migrations(database, MIGRATIONS, backup=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE Test_Jobs (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL)"
        )
        connection.execute("INSERT INTO Test_Jobs VALUES (1, 'queued')")

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    process = context.Process(
        target=_validate_schema_in_process,
        args=(str(database), ready, release),
    )
    process.start()
    try:
        assert ready.wait(5)
        with sqlite3.connect(database, timeout=0) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT Id FROM Test_Jobs WHERE Status='queued'"
            ).fetchone()
            connection.execute(
                "UPDATE Test_Jobs SET Status='running' WHERE Id=? AND Status='queued'",
                (job[0],),
            )
            connection.commit()
            assert connection.execute(
                "SELECT Status FROM Test_Jobs WHERE Id=1"
            ).fetchone() == ("running",)
    finally:
        release.set()
        process.join(5)
        if process.is_alive():
            process.terminate()
            process.join(5)
    assert process.exitcode == 0


def test_new_install_exchange_metadata_defaults_to_kraken(tmp_path):
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
        prepare_exchange_schema_for_startup(connection, new_database=True)
        connection.commit()

        assert connection.execute(
            "SELECT Code, Enabled, Is_Default FROM Exchanges ORDER BY Id"
        ).fetchall() == [
            ("binance", 0, 0), ("kraken", 1, 1),
            ("okx", 0, 0), ("okx_demo", 0, 0),
        ]
        assert connection.execute(
            "SELECT Buy_Enabled, Sell_Enabled FROM Exchanges WHERE Code='kraken'"
        ).fetchone() == (0, 0)

        connection.execute("BEGIN IMMEDIATE")
        apply_exchange_aware_schema(connection, upgraded_install=True)
        connection.commit()
        assert connection.execute(
            "SELECT Code, Enabled, Is_Default FROM Exchanges"
        ).fetchone() == ("binance", 0, 0)


def test_kraken_metadata_migration_applies_automatically_after_version_two(tmp_path):
    database = tmp_path / "version-two.db"
    _create_legacy_database(database)
    with sqlite3.connect(database) as connection:
        apply_pending_migrations(connection, MIGRATIONS[:2], allow_rebuild=True)

        applied = prepare_startup_migrations(
            connection,
            MIGRATIONS,
            new_database=False,
        )

        assert [migration.name for migration in applied] == [
            "kraken_public_exchange",
            "exchange_specific_backtesting",
            "kraken_backtesting_defaults",
            "gated_kraken_live_execution",
            "durable_order_fills",
            "okx_configuration",
            "backtesting_exchange_context",
            "okx_demo_execution",
        ]
        assert connection.execute(
            "SELECT Enabled, Is_Default FROM Exchanges WHERE Code='kraken'"
        ).fetchone() == (1, 0)
        assert connection.execute(
            "SELECT ebs.Commission_Value FROM Exchange_Backtesting_Settings ebs "
            "JOIN Exchanges e ON e.Id=ebs.Exchange_Id WHERE e.Code='kraken'"
        ).fetchone()[0] == pytest.approx(0.004)


def test_new_database_is_marked_current_with_kraken_public_metadata(tmp_path):
    database = tmp_path / "new-current.db"
    with sqlite3.connect(database) as connection:
        prepare_startup_migrations(connection, MIGRATIONS, new_database=True)
        prepare_exchange_schema_for_startup(connection, new_database=True)
        mark_new_database_current(connection, MIGRATIONS)

        assert pending_migrations(connection, MIGRATIONS) == ()
        assert connection.execute(
            "SELECT Code, Enabled, Is_Default FROM Exchanges ORDER BY Id"
        ).fetchall() == [
            ("binance", 0, 0), ("kraken", 1, 1),
            ("okx", 0, 0), ("okx_demo", 0, 0),
        ]


def test_kraken_defaults_migration_preserves_existing_fee(tmp_path):
    database = tmp_path / "kraken-fee.db"
    _create_legacy_database(database)
    with sqlite3.connect(database) as connection:
        apply_pending_migrations(connection, MIGRATIONS[:4], allow_rebuild=True)
        kraken_id = connection.execute(
            "SELECT Id FROM Exchanges WHERE Code='kraken'"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO Exchange_Backtesting_Settings "
            "(Exchange_Id, Commission_Value) VALUES (?, ?)",
            (kraken_id, 0.0026),
        )
        connection.commit()

        applied = apply_pending_migrations(connection, MIGRATIONS, allow_rebuild=False)

        assert [migration.name for migration in applied] == [
            "kraken_backtesting_defaults",
            "gated_kraken_live_execution",
            "durable_order_fills",
            "okx_configuration",
            "backtesting_exchange_context",
            "okx_demo_execution",
        ]
        assert connection.execute(
            "SELECT Commission_Value FROM Exchange_Backtesting_Settings "
            "WHERE Exchange_Id=?",
            (kraken_id,),
        ).fetchone()[0] == pytest.approx(0.0026)
