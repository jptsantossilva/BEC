"""Immutable exchange context captured for queued and completed backtests."""

from __future__ import annotations

import sqlite3


JOB_COLUMNS = {
    "Exchange_Code": "TEXT",
    "Exchange_Adapter_Id": "TEXT",
    "Exchange_Quote_Asset": "TEXT",
    "Exchange_Execution_Environment": "TEXT",
}

MONTE_CARLO_JOB_COLUMNS = {
    "Exchange_Code": "TEXT",
    "Exchange_Adapter_Id": "TEXT",
    "Exchange_Quote_Asset": "TEXT",
    "Exchange_Execution_Environment": "TEXT",
}

RESULT_COLUMNS = {
    "Backtest_Adapter_Id": "TEXT",
    "Backtest_Quote_Asset": "TEXT",
    "Backtest_Execution_Environment": "TEXT",
}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f'PRAGMA table_info("{table}")')
    }


def _add_columns(
    connection: sqlite3.Connection, table: str, definitions: dict[str, str]
) -> None:
    if not _table_exists(connection, table):
        return
    existing = _columns(connection, table)
    for name, definition in definitions.items():
        if name not in existing:
            connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def apply_backtesting_context_schema(connection: sqlite3.Connection) -> None:
    """Backfill immutable context without changing any result or job identity."""
    _add_columns(connection, "Backtesting_Jobs", JOB_COLUMNS)
    _add_columns(connection, "Monte_Carlo_Jobs", MONTE_CARLO_JOB_COLUMNS)
    _add_columns(connection, "Backtesting_Results", RESULT_COLUMNS)

    if _table_exists(connection, "Backtesting_Jobs"):
        connection.execute(
            """
            UPDATE Backtesting_Jobs
            SET Exchange_Code=COALESCE(NULLIF(Exchange_Code, ''), (
                    SELECT Code FROM Exchanges WHERE Id=Backtesting_Jobs.Exchange_Id
                )),
                Exchange_Adapter_Id=COALESCE(NULLIF(Exchange_Adapter_Id, ''), (
                    SELECT Adapter_Id FROM Exchanges WHERE Id=Backtesting_Jobs.Exchange_Id
                )),
                Exchange_Quote_Asset=COALESCE(NULLIF(Exchange_Quote_Asset, ''), (
                    SELECT Quote_Asset FROM Exchanges WHERE Id=Backtesting_Jobs.Exchange_Id
                )),
                Exchange_Execution_Environment=COALESCE(
                    NULLIF(Exchange_Execution_Environment, ''), (
                    SELECT Execution_Environment FROM Exchanges WHERE Id=Backtesting_Jobs.Exchange_Id
                ))
            """
        )
    if _table_exists(connection, "Backtesting_Results"):
        connection.execute(
            """
            UPDATE Backtesting_Results
            SET Backtest_Adapter_Id=COALESCE(NULLIF(Backtest_Adapter_Id, ''), (
                    SELECT Adapter_Id FROM Exchanges WHERE Id=Backtesting_Results.Exchange_Id
                )),
                Backtest_Quote_Asset=COALESCE(NULLIF(Backtest_Quote_Asset, ''), (
                    SELECT Quote_Asset FROM Exchanges WHERE Id=Backtesting_Results.Exchange_Id
                )),
                Backtest_Execution_Environment=COALESCE(
                    NULLIF(Backtest_Execution_Environment, ''), (
                    SELECT Execution_Environment FROM Exchanges WHERE Id=Backtesting_Results.Exchange_Id
                ))
            """
        )
    if _table_exists(connection, "Monte_Carlo_Jobs"):
        connection.execute(
            """
            UPDATE Monte_Carlo_Jobs
            SET Exchange_Code=COALESCE(NULLIF(Exchange_Code, ''), (
                    SELECT Code FROM Exchanges WHERE Id=Monte_Carlo_Jobs.Exchange_Id
                )),
                Exchange_Adapter_Id=COALESCE(NULLIF(Exchange_Adapter_Id, ''), (
                    SELECT Adapter_Id FROM Exchanges WHERE Id=Monte_Carlo_Jobs.Exchange_Id
                )),
                Exchange_Quote_Asset=COALESCE(NULLIF(Exchange_Quote_Asset, ''), (
                    SELECT Quote_Asset FROM Exchanges WHERE Id=Monte_Carlo_Jobs.Exchange_Id
                )),
                Exchange_Execution_Environment=COALESCE(
                    NULLIF(Exchange_Execution_Environment, ''), (
                    SELECT Execution_Environment FROM Exchanges WHERE Id=Monte_Carlo_Jobs.Exchange_Id
                ))
            """
        )


def validate_backtesting_context_schema(connection: sqlite3.Connection) -> None:
    missing = []
    for table, expected in (
        ("Backtesting_Jobs", set(JOB_COLUMNS)),
        ("Monte_Carlo_Jobs", set(MONTE_CARLO_JOB_COLUMNS)),
        ("Backtesting_Results", set(RESULT_COLUMNS)),
    ):
        if _table_exists(connection, table):
            missing.extend(
                f"{table}.{column}" for column in sorted(expected - _columns(connection, table))
            )
    if missing:
        raise ValueError("Missing immutable backtesting context columns: " + ", ".join(missing))
