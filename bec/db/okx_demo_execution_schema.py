"""Additive schema and safe defaults for mandatory OKX demo execution."""

from __future__ import annotations

import sqlite3


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def apply_okx_demo_execution_schema(connection: sqlite3.Connection) -> None:
    """Create durable demo-validation evidence without enabling any operation."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS OKX_Demo_Validation_Records (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Exchange_Id INTEGER NOT NULL REFERENCES Exchanges(Id),
            Adapter_Id TEXT NOT NULL,
            Tested_Symbols_JSON TEXT NOT NULL,
            Buy_Order_Ids_JSON TEXT NOT NULL,
            Sell_Order_Ids_JSON TEXT NOT NULL,
            Reconciliation_Result_JSON TEXT NOT NULL,
            Completed_At TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_okx_demo_validation_exchange_completed
        ON OKX_Demo_Validation_Records(Exchange_Id, Completed_At DESC)
        """
    )
    if not _table_exists(connection, "Exchanges"):
        return
    # The migration is intentionally inert: it neither enables the identity nor
    # arms either side. Five percent is the conservative initial demo reserve.
    connection.execute(
        """
        UPDATE Exchanges
        SET Enabled=0, Is_Default=0, Buy_Enabled=0, Sell_Enabled=0,
            Sizing_Buffer_Pct=5.0, Updated_At=CURRENT_TIMESTAMP
        WHERE Code='okx_demo'
        """
    )
    connection.execute(
        """
        UPDATE Exchanges
        SET Buy_Enabled=0, Sell_Enabled=0, Updated_At=CURRENT_TIMESTAMP
        WHERE Code='okx'
        """
    )


def validate_okx_demo_execution_schema(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "OKX_Demo_Validation_Records"):
        raise ValueError("OKX_Demo_Validation_Records is missing")
    indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='OKX_Demo_Validation_Records'"
        )
    }
    if "idx_okx_demo_validation_exchange_completed" not in indexes:
        raise ValueError("OKX demo validation index is missing")
    rows = {
        str(row[0]): tuple(row[1:])
        for row in connection.execute(
            """
            SELECT Code, Enabled, Is_Default, Buy_Enabled, Sell_Enabled,
                   Sizing_Buffer_Pct, Execution_Environment
            FROM Exchanges WHERE Code IN ('okx', 'okx_demo')
            """
        )
    }
    if rows.get("okx", (None,))[2:4] != (0, 0):
        raise ValueError("OKX production order flags must remain disabled")
    demo = rows.get("okx_demo")
    if (
        demo is None
        or demo[:4] != (0, 0, 0, 0)
        or float(demo[4]) != 5.0
        or demo[5] != "demo"
    ):
        raise ValueError("OKX demo execution defaults are invalid")
