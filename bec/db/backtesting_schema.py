"""Exchange-specific backtesting schema introduced by PR 6."""

from __future__ import annotations

import json
import math
import sqlite3


DEFAULT_KRAKEN_COMMISSION = 0.004


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _add_column(
    connection: sqlite3.Connection, table: str, name: str, definition: str
) -> None:
    if _table_exists(connection, table) and name not in _columns(connection, table):
        connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def _result_commission(config_json: object) -> float | None:
    try:
        config = json.loads(str(config_json or ""))
        percent = config["backtesting"]["commission_pct"]
        return float(percent) / 100.0
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def apply_exchange_backtesting_schema(connection: sqlite3.Connection) -> None:
    """Add immutable exchange/fee context without rebuilding existing tables."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS Exchange_Backtesting_Settings (
            Exchange_Id INTEGER PRIMARY KEY REFERENCES Exchanges(Id),
            Commission_Value REAL NOT NULL CHECK (Commission_Value >= 0),
            Updated_At TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    if _table_exists(connection, "Backtesting_Settings"):
        connection.execute(
            """
            INSERT OR IGNORE INTO Exchange_Backtesting_Settings
                (Exchange_Id, Commission_Value)
            SELECT e.Id, bs.Commission_Value
            FROM Exchanges e
            CROSS JOIN Backtesting_Settings bs
            WHERE e.Code='binance'
            ORDER BY bs.Id
            LIMIT 1
            """
        )

    _add_column(
        connection, "Backtesting_Results", "Backtest_Commission_Value", "REAL"
    )
    _add_column(
        connection, "Backtesting_Jobs", "Backtest_Commission_Value", "REAL"
    )
    _add_column(
        connection, "Backtesting_Jobs", "Backtest_Work_Fingerprint", "TEXT"
    )
    job_columns = (
        _columns(connection, "Backtesting_Jobs")
        if _table_exists(connection, "Backtesting_Jobs")
        else set()
    )
    if {"Exchange_Id", "Backtest_Commission_Value"} <= job_columns:
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET Backtest_Commission_Value=(
                SELECT Commission_Value
                FROM Exchange_Backtesting_Settings
                WHERE Exchange_Id=Backtesting_Jobs.Exchange_Id
            )
            WHERE Backtest_Commission_Value IS NULL
            """
        )
    if {
        "status",
        "finished_at",
        "error_message",
        "Backtest_Work_Fingerprint",
    } <= job_columns:
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET status='failed', finished_at=CURRENT_TIMESTAMP,
                error_message='Requeue after PR6 to capture exchange backtesting context.'
            WHERE status IN ('queued', 'running')
              AND COALESCE(Backtest_Work_Fingerprint, '')=''
            """
        )

    if _table_exists(connection, "Backtesting_Results"):
        rows = connection.execute(
            """
            SELECT Id, Backtest_Config_JSON
            FROM Backtesting_Results
            WHERE Backtest_Commission_Value IS NULL
            """
        ).fetchall()
        fallback = connection.execute(
            """
            SELECT Commission_Value
            FROM Exchange_Backtesting_Settings ebs
            JOIN Exchanges e ON e.Id=ebs.Exchange_Id
            WHERE e.Code='binance'
            """
        ).fetchone()
        fallback_fee = float(fallback[0]) if fallback else None
        for result_id, config_json in rows:
            fee = _result_commission(config_json)
            if fee is None:
                fee = fallback_fee
            if fee is not None:
                connection.execute(
                    "UPDATE Backtesting_Results SET Backtest_Commission_Value=? WHERE Id=?",
                    (fee, result_id),
                )


def validate_exchange_backtesting_schema(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "Exchange_Backtesting_Settings"):
        raise ValueError("Exchange_Backtesting_Settings is missing")
    expected = {
        "Backtesting_Results": {"Backtest_Commission_Value"},
        "Backtesting_Jobs": {
            "Backtest_Commission_Value",
            "Backtest_Work_Fingerprint",
        },
    }
    missing = [
        f"{table}.{column}"
        for table, columns in expected.items()
        if _table_exists(connection, table)
        for column in sorted(columns - _columns(connection, table))
    ]
    if missing:
        raise ValueError("Missing exchange backtesting columns: " + ", ".join(missing))

    binance = connection.execute(
        """
        SELECT ebs.Commission_Value
        FROM Exchange_Backtesting_Settings ebs
        JOIN Exchanges e ON e.Id=ebs.Exchange_Id
        WHERE e.Code='binance'
        """
    ).fetchone()
    has_legacy_settings = _table_exists(connection, "Backtesting_Settings") and (
        connection.execute("SELECT 1 FROM Backtesting_Settings LIMIT 1").fetchone()
        is not None
    )
    if has_legacy_settings and binance is None:
        raise ValueError("The Binance backtesting fee was not preserved")


def apply_kraken_backtesting_defaults(connection: sqlite3.Connection) -> None:
    """Enable Kraken public workflows without changing an existing default."""
    connection.execute(
        """
        UPDATE Exchanges
        SET Enabled=1, Quote_Asset='USDC', Updated_At=CURRENT_TIMESTAMP
        WHERE Code='kraken'
        """
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO Exchange_Backtesting_Settings
            (Exchange_Id, Commission_Value)
        SELECT Id, ? FROM Exchanges WHERE Code='kraken'
        """,
        (DEFAULT_KRAKEN_COMMISSION,),
    )


def configure_new_install_kraken_default(connection: sqlite3.Connection) -> None:
    apply_kraken_backtesting_defaults(connection)
    connection.execute("UPDATE Exchanges SET Is_Default=0")
    connection.execute(
        """
        UPDATE Exchanges
        SET Enabled=1, Is_Default=1, Quote_Asset='USDC',
            Updated_At=CURRENT_TIMESTAMP
        WHERE Code='kraken'
        """
    )


def validate_kraken_backtesting_defaults(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        """
        SELECT e.Enabled, e.Quote_Asset, ebs.Commission_Value
        FROM Exchanges e
        LEFT JOIN Exchange_Backtesting_Settings ebs ON ebs.Exchange_Id=e.Id
        WHERE e.Code='kraken'
        """
    ).fetchone()
    if (
        row is None
        or int(row[0]) != 1
        or str(row[1]) != "USDC"
        or row[2] is None
        or not math.isfinite(float(row[2]))
        or float(row[2]) < 0
    ):
        raise ValueError("Kraken backtesting defaults are missing")
