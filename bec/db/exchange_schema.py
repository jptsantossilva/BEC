"""Exchange-aware SQLite schema shared by fresh installs and migrations."""

from __future__ import annotations

import sqlite3


BINANCE_ID = 1
KRAKEN_ID = 2
KNOWN_QUOTES = ("FDUSD", "BUSD", "USDT", "USDC", "TUSD", "EUR", "GBP", "BTC", "ETH", "BNB")

SYMBOL_TABLES = {
    "Orders": "Symbol",
    "Positions": "Symbol",
    "Backtesting_Results": "Symbol",
    "Backtesting_Trades": "Symbol",
    "Symbols_To_Calc": "Symbol",
    "Symbols_By_Market_Phase": "Symbol",
    "Symbols_By_Market_Phase_Historical": "Symbol",
    "Backtesting_Jobs": "symbol",
    "Monte_Carlo_Jobs": "symbol",
    "Monte_Carlo_Results": "Symbol",
    "Auto_Switch_Signals": "symbol",
    "Signals_Log": "Symbol",
}

COMMON_SYMBOL_COLUMNS = {
    "Exchange_Id": "INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id)",
    "Symbol_Normalized": "TEXT",
    "Exchange_Symbol": "TEXT",
    "Base_Asset": "TEXT",
    "Quote_Asset": "TEXT",
}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _add_columns(connection: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    if not _table_exists(connection, table):
        return
    existing = _columns(connection, table)
    for name, definition in columns.items():
        if name not in existing:
            connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def normalize_legacy_binance_symbol(symbol: object) -> tuple[str, str, str, bool]:
    value = str(symbol or "").strip().upper()
    if not value:
        return "", "", "", False
    if "/" in value:
        base, quote = value.split("/", 1)
        valid = bool(base and quote)
        return (f"{base}/{quote}" if valid else value), base, quote, valid
    for quote in KNOWN_QUOTES:
        if value.endswith(quote) and len(value) > len(quote):
            base = value[: -len(quote)]
            return f"{base}/{quote}", base, quote, True
    return value, "", "", False


def create_exchange_metadata(connection: sqlite3.Connection, *, upgraded_install: bool) -> None:
    quote_asset = "USDC"
    if _table_exists(connection, "Settings"):
        configured_quote = connection.execute(
            "SELECT value FROM Settings WHERE name='trade_against' LIMIT 1"
        ).fetchone()
        if configured_quote and str(configured_quote[0] or "").strip():
            quote_asset = str(configured_quote[0]).strip().upper()
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS Exchanges (
            Id INTEGER PRIMARY KEY,
            Code TEXT NOT NULL UNIQUE,
            Name TEXT NOT NULL,
            Enabled INTEGER NOT NULL DEFAULT 0 CHECK (Enabled IN (0, 1)),
            Is_Default INTEGER NOT NULL DEFAULT 0 CHECK (Is_Default IN (0, 1)),
            Quote_Asset TEXT,
            Trading_Mode TEXT NOT NULL DEFAULT 'spot' CHECK (Trading_Mode = 'spot'),
            Adapter_Id TEXT NOT NULL DEFAULT 'binance',
            Execution_Environment TEXT NOT NULL DEFAULT 'production',
            Created_At TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            Updated_At TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_exchanges_one_default "
        "ON Exchanges(Is_Default) WHERE Is_Default = 1"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS Exchange_Symbols (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Exchange_Id INTEGER NOT NULL REFERENCES Exchanges(Id),
            Symbol_Normalized TEXT NOT NULL,
            Exchange_Symbol TEXT NOT NULL,
            Base_Asset TEXT,
            Quote_Asset TEXT,
            Is_Tradable INTEGER NOT NULL DEFAULT 1 CHECK (Is_Tradable IN (0, 1)),
            Resolution_Status TEXT NOT NULL DEFAULT 'resolved',
            UNIQUE (Exchange_Id, Exchange_Symbol),
            UNIQUE (Exchange_Id, Symbol_Normalized)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO Exchanges
            (Id, Code, Name, Enabled, Is_Default, Quote_Asset, Trading_Mode,
             Adapter_Id, Execution_Environment)
        VALUES (?, 'binance', 'Binance', ?, ?, ?, 'spot', 'binance', 'production')
        ON CONFLICT(Code) DO UPDATE SET
            Name = excluded.Name,
            Trading_Mode = excluded.Trading_Mode
        WHERE Exchanges.Name IS NOT excluded.Name
           OR Exchanges.Trading_Mode IS NOT excluded.Trading_Mode
        """,
        (BINANCE_ID, int(upgraded_install), int(upgraded_install), quote_asset),
    )


def register_kraken_public_exchange(connection: sqlite3.Connection) -> None:
    """Register Kraken as selectable public-data-only infrastructure."""
    connection.execute(
        """
        INSERT INTO Exchanges
            (Id, Code, Name, Enabled, Is_Default, Quote_Asset, Trading_Mode,
             Adapter_Id, Execution_Environment)
        VALUES (?, 'kraken', 'Kraken', 0, 0, 'EUR', 'spot', 'kraken', 'production')
        ON CONFLICT(Code) DO UPDATE SET
            Name=excluded.Name,
            Trading_Mode=excluded.Trading_Mode
        WHERE Exchanges.Name IS NOT excluded.Name
           OR Exchanges.Trading_Mode IS NOT excluded.Trading_Mode
        """,
        (KRAKEN_ID,),
    )


def _rebuild_unique_tables(connection: sqlite3.Connection) -> None:
    from bec.db.migrations.rebuild import rebuild_table

    definitions = {
        "Backtesting_Results": """
            CREATE TABLE {new_table} (
                Id INTEGER PRIMARY KEY, Symbol TEXT, Time_Frame TEXT, Return_Perc REAL,
                BuyHold_Return_Perc REAL, Backtest_Start_Date TEXT, Backtest_End_Date TEXT,
                Max_Drawdown_Perc REAL, Trades INTEGER, Win_Rate_Perc REAL,
                Best_Trade_Perc REAL, Worst_Trade_Perc REAL, Avg_Trade_Perc REAL,
                Max_Trade_Duration TEXT, Avg_Trade_Duration TEXT, Profit_Factor REAL,
                Expectancy_Perc REAL, SQN REAL, Kelly_Criterion REAL,
                Trading_Approved INTEGER NOT NULL DEFAULT 0, Trading_Rejection_Reasons TEXT,
                Quality_Score REAL, Quality_Grade TEXT, Backtest_Config_JSON TEXT,
                Backtest_Work_Fingerprint TEXT, Backtest_Work_Candle TEXT,
                Backtest_Work_Executed_At TEXT, Backtest_Commission_Value REAL,
                Strategy_Id TEXT,
                Exchange_Id INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id),
                Symbol_Normalized TEXT, Exchange_Symbol TEXT, Base_Asset TEXT, Quote_Asset TEXT,
                UNIQUE (Exchange_Id, Symbol, Time_Frame, Strategy_Id)
            )""",
        "Backtesting_Trades": """
            CREATE TABLE {new_table} (
                Id INTEGER PRIMARY KEY, Symbol TEXT, Time_Frame TEXT, Strategy_Id TEXT,
                EntryBar INTEGER, ExitBar INTEGER, EntryPrice REAL, ExitPrice REAL, PnL REAL,
                ReturnPct REAL, EntryTime TIMESTAMP, ExitTime TIMESTAMP, Duration TEXT,
                Exit_Reason TEXT, Hard_Stop_Loss REAL, ATR_Stop_Loss REAL, Active_Stop_Loss REAL,
                Exchange_Id INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id), Symbol_Normalized TEXT,
                Exchange_Symbol TEXT, Base_Asset TEXT, Quote_Asset TEXT,
                UNIQUE (Exchange_Id, Symbol, Time_Frame, Strategy_Id, EntryTime, ExitTime)
            )""",
        "Balances": """
            CREATE TABLE {new_table} (
                Id INTEGER PRIMARY KEY, Date TEXT, Asset TEXT, Balance REAL, USD_Price REAL,
                BTC_Price REAL, Balance_USD REAL, Balance_BTC REAL, Total_Balance_Of_BTC REAL,
                Exchange_Id INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id),
                UNIQUE (Exchange_Id, Date, Asset)
            )""",
        "Monte_Carlo_Results": """
            CREATE TABLE {new_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT, Symbol TEXT NOT NULL, Time_Frame TEXT NOT NULL,
                Strategy_Id TEXT NOT NULL, Method TEXT NOT NULL, Scenarios INTEGER NOT NULL,
                Valid_Scenarios INTEGER NOT NULL, Seed INTEGER NOT NULL, Robustness_Score REAL,
                Interpretation TEXT, Net_Profit_Original REAL, Net_Profit_Worst_5 REAL,
                Net_Profit_Median REAL, Net_Profit_Best_5 REAL, Max_Drawdown_Original REAL,
                Max_Drawdown_Worst_5 REAL, Max_Drawdown_Median REAL, Max_Drawdown_Best_5 REAL,
                Html_Path TEXT, Csv_Path TEXT, Json_Path TEXT, Result_JSON TEXT, Created_At TEXT NOT NULL,
                Exchange_Id INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id), Symbol_Normalized TEXT,
                Exchange_Symbol TEXT, Base_Asset TEXT, Quote_Asset TEXT,
                UNIQUE (Exchange_Id, Symbol, Time_Frame, Strategy_Id, Method)
            )""",
        "Auto_Switch_Signals": """
            CREATE TABLE {new_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT, strategy_id TEXT NOT NULL,
                symbol TEXT NOT NULL, signal TEXT NOT NULL, signal_timeframe TEXT NOT NULL,
                candle_id TEXT NOT NULL, processed_at TEXT NOT NULL,
                Exchange_Id INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id), Symbol_Normalized TEXT,
                Exchange_Symbol TEXT, Base_Asset TEXT, Quote_Asset TEXT,
                UNIQUE (Exchange_Id, strategy_id, symbol, signal, signal_timeframe, candle_id)
            )""",
    }
    expected_unique = {
        "Backtesting_Results": "unique(exchange_id,symbol,time_frame,strategy_id)",
        "Backtesting_Trades": "unique(exchange_id,symbol,time_frame,strategy_id,entrytime,exittime)",
        "Balances": "unique(exchange_id,date,asset)",
        "Monte_Carlo_Results": "unique(exchange_id,symbol,time_frame,strategy_id,method)",
        "Auto_Switch_Signals": "unique(exchange_id,strategy_id,symbol,signal,signal_timeframe,candle_id)",
    }
    indexes = {
        "Balances": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_balances_exchange_date_asset "
            "ON Balances(Exchange_Id, Date, Asset)",
        ),
        "Monte_Carlo_Results": (
            "CREATE INDEX IF NOT EXISTS idx_monte_carlo_results_target "
            "ON Monte_Carlo_Results(Exchange_Id, Strategy_Id, Symbol_Normalized, Time_Frame, Method)",
        ),
        "Auto_Switch_Signals": (
            "CREATE INDEX IF NOT EXISTS idx_auto_switch_signals_target "
            "ON Auto_Switch_Signals(Exchange_Id, strategy_id, Symbol_Normalized, signal_timeframe, candle_id)",
        ),
    }
    for table, create_sql in definitions.items():
        if not _table_exists(connection, table):
            continue
        table_sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        compact_sql = "".join(str(table_sql_row[0] or "").lower().split())
        if expected_unique[table] in compact_sql:
            for statement in indexes.get(table, ()):
                connection.execute(statement)
            continue
        columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
        quoted = ", ".join(f'"{column}"' for column in columns)
        rebuild_table(
            connection,
            table=table,
            create_sql=create_sql,
            copy_sql=f"INSERT INTO {{new_table}} ({quoted}) SELECT {quoted} FROM {{source_table}}",
            index_sql=indexes.get(table, ()),
        )


def _backfill_symbols(connection: sqlite3.Connection) -> None:
    catalog: dict[str, tuple[str, str, str, bool]] = {}
    for table, symbol_column in SYMBOL_TABLES.items():
        if not _table_exists(connection, table):
            continue
        for row_id, symbol in connection.execute(
            f'SELECT rowid, "{symbol_column}" FROM "{table}" '
            "WHERE Exchange_Id IS NULL OR Symbol_Normalized IS NULL "
            "OR Exchange_Symbol IS NULL OR Base_Asset IS NULL OR Quote_Asset IS NULL"
        ).fetchall():
            normalized, base, quote, resolved = normalize_legacy_binance_symbol(symbol)
            exchange_symbol = str(symbol or "").strip().upper()
            connection.execute(
                f"""
                UPDATE "{table}"
                SET Exchange_Id=?, Symbol_Normalized=?, Exchange_Symbol=?, Base_Asset=?, Quote_Asset=?
                WHERE rowid=?
                """,
                (BINANCE_ID, normalized, exchange_symbol, base, quote, row_id),
            )
            if exchange_symbol:
                catalog[exchange_symbol] = (normalized, base, quote, resolved)
    for exchange_symbol, (normalized, base, quote, resolved) in catalog.items():
        connection.execute(
            """
            INSERT INTO Exchange_Symbols
                (Exchange_Id, Symbol_Normalized, Exchange_Symbol, Base_Asset, Quote_Asset,
                 Is_Tradable, Resolution_Status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(Exchange_Id, Exchange_Symbol) DO UPDATE SET
                Symbol_Normalized=excluded.Symbol_Normalized,
                Base_Asset=excluded.Base_Asset, Quote_Asset=excluded.Quote_Asset,
                Is_Tradable=excluded.Is_Tradable, Resolution_Status=excluded.Resolution_Status
            WHERE Exchange_Symbols.Symbol_Normalized IS NOT excluded.Symbol_Normalized
               OR Exchange_Symbols.Base_Asset IS NOT excluded.Base_Asset
               OR Exchange_Symbols.Quote_Asset IS NOT excluded.Quote_Asset
               OR Exchange_Symbols.Is_Tradable IS NOT excluded.Is_Tradable
               OR Exchange_Symbols.Resolution_Status IS NOT excluded.Resolution_Status
            """,
            (
                BINANCE_ID, normalized, exchange_symbol, base, quote,
                int(resolved), "resolved" if resolved else "unresolved_legacy_symbol",
            ),
        )


def apply_exchange_aware_schema(
    connection: sqlite3.Connection, *, upgraded_install: bool
) -> None:
    """Apply the exchange-aware schema during migration or new DB initialization.

    This function may alter and rebuild tables. Normal application startup for
    an existing database must call :func:`validate_exchange_aware_schema`
    instead.
    """
    create_exchange_metadata(connection, upgraded_install=upgraded_install)
    for table in SYMBOL_TABLES:
        _add_columns(connection, table, COMMON_SYMBOL_COLUMNS)
    _add_columns(
        connection,
        "Balances",
        {"Exchange_Id": "INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id)"},
    )
    _add_columns(
        connection,
        "Locked_Values",
        {"Exchange_Id": "INTEGER NOT NULL DEFAULT 1 REFERENCES Exchanges(Id)"},
    )
    _add_columns(
        connection,
        "Orders",
        {
            "Order_Status": "TEXT NOT NULL DEFAULT 'filled'",
            "Executed_Qty": "REAL",
            "Average_Price": "REAL",
            "Fee_Asset": "TEXT",
            "Fee_Amount": "REAL NOT NULL DEFAULT 0",
            "Net_Qty": "REAL",
        },
    )
    if _table_exists(connection, "Orders"):
        connection.execute(
            "UPDATE Orders SET Executed_Qty=COALESCE(Executed_Qty, Qty), "
            "Average_Price=COALESCE(Average_Price, Price), Net_Qty=COALESCE(Net_Qty, Qty) "
            "WHERE Executed_Qty IS NULL OR Average_Price IS NULL OR Net_Qty IS NULL"
        )
    if _table_exists(connection, "Balances"):
        connection.execute("UPDATE Balances SET Exchange_Id=? WHERE Exchange_Id IS NULL", (BINANCE_ID,))
    if _table_exists(connection, "Locked_Values"):
        connection.execute("UPDATE Locked_Values SET Exchange_Id=? WHERE Exchange_Id IS NULL", (BINANCE_ID,))
    _backfill_symbols(connection)
    _rebuild_unique_tables(connection)
    for table, index_name in (
        ("Orders", "idx_orders_exchange_symbol"),
        ("Positions", "idx_positions_exchange_symbol"),
        ("Symbols_By_Market_Phase", "idx_market_phase_exchange_symbol"),
    ):
        if _table_exists(connection, table):
            connection.execute(
                f'CREATE INDEX IF NOT EXISTS "{index_name}" '
                f'ON "{table}"(Exchange_Id, Symbol_Normalized)'
            )


def validate_exchange_aware_schema(connection: sqlite3.Connection) -> None:
    """Validate the migrated schema without changing database state."""
    required_tables = ("Exchanges", "Exchange_Symbols")
    missing_tables = [
        table for table in required_tables if not _table_exists(connection, table)
    ]

    required_columns: dict[str, set[str]] = {
        table: set(COMMON_SYMBOL_COLUMNS)
        for table in SYMBOL_TABLES
        if _table_exists(connection, table)
    }
    required_columns.update(
        {
            "Balances": {"Exchange_Id"},
            "Locked_Values": {"Exchange_Id"},
            "Orders": {
                *COMMON_SYMBOL_COLUMNS,
                "Order_Status",
                "Executed_Qty",
                "Average_Price",
                "Fee_Asset",
                "Fee_Amount",
                "Net_Qty",
            },
        }
    )
    missing_columns: list[str] = []
    for table, expected in required_columns.items():
        if not _table_exists(connection, table):
            continue
        for column in sorted(expected - _columns(connection, table)):
            missing_columns.append(f"{table}.{column}")

    binance = None
    if not missing_tables:
        binance = connection.execute(
            "SELECT Trading_Mode FROM Exchanges WHERE Code='binance'"
        ).fetchone()

    problems = []
    if missing_tables:
        problems.append("missing tables: " + ", ".join(missing_tables))
    if missing_columns:
        problems.append("missing columns: " + ", ".join(missing_columns))
    if not missing_tables and binance != ("spot",):
        problems.append("Binance spot metadata is missing")
    if problems:
        database_path = next(
            (
                str(row[2])
                for row in connection.execute("PRAGMA database_list")
                if row[1] == "main"
            ),
            "data.db",
        )
        raise RuntimeError(
            "Exchange-aware schema validation failed ("
            + "; ".join(problems)
            + "). Do not start BEC with an incomplete schema. Validate a backup with: "
            + f"python -m bec.db.migrations --database {database_path} --dry-run"
        )


def prepare_exchange_schema_for_startup(
    connection: sqlite3.Connection, *, new_database: bool
) -> None:
    """Initialize a new database or read-only validate an existing one."""
    if new_database:
        apply_exchange_aware_schema(connection, upgraded_install=False)
        register_kraken_public_exchange(connection)
        from bec.db.backtesting_schema import apply_exchange_backtesting_schema
        from bec.db.backtesting_schema import configure_new_install_kraken_default
        from bec.db.live_execution_schema import apply_live_execution_schema
        from bec.db.order_fills_schema import apply_durable_order_fills_schema
        from bec.db.okx_configuration_schema import apply_okx_configuration_schema
        from bec.db.backtesting_context_schema import apply_backtesting_context_schema
        from bec.db.okx_demo_execution_schema import apply_okx_demo_execution_schema

        apply_exchange_backtesting_schema(connection)
        configure_new_install_kraken_default(connection)
        apply_live_execution_schema(connection)
        apply_durable_order_fills_schema(connection)
        apply_okx_configuration_schema(connection)
        apply_backtesting_context_schema(connection)
        apply_okx_demo_execution_schema(connection)
        return
    validate_exchange_aware_schema(connection)


def unresolved_legacy_symbols(connection: sqlite3.Connection) -> list[str]:
    if not _table_exists(connection, "Exchange_Symbols"):
        return []
    return [
        str(row[0])
        for row in connection.execute(
            "SELECT Exchange_Symbol FROM Exchange_Symbols WHERE Is_Tradable=0 ORDER BY Exchange_Symbol"
        )
    ]
