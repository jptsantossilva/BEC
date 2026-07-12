"""Additive durable fill persistence for exchange order reconciliation."""

from __future__ import annotations

import sqlite3


ORDER_COLUMNS = {
    "Executed_Cost": "REAL NOT NULL DEFAULT 0",
    "Fees_JSON": "TEXT NOT NULL DEFAULT '{}'",
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


def apply_durable_order_fills_schema(connection: sqlite3.Connection) -> None:
    """Add durable fill records without changing historical order semantics."""
    if _table_exists(connection, "Orders"):
        existing = _columns(connection, "Orders")
        for name, definition in ORDER_COLUMNS.items():
            if name not in existing:
                connection.execute(f'ALTER TABLE Orders ADD COLUMN "{name}" {definition}')
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS Order_Fills (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Order_Id INTEGER NOT NULL REFERENCES Orders(Id),
            Exchange_Id INTEGER NOT NULL REFERENCES Exchanges(Id),
            Exchange_Order_Id TEXT,
            Fill_Key TEXT NOT NULL,
            Trade_Id TEXT,
            Symbol_Normalized TEXT,
            Exchange_Symbol TEXT,
            Price REAL NOT NULL,
            Qty REAL NOT NULL,
            Fee_Asset TEXT,
            Fee_Amount REAL NOT NULL DEFAULT 0,
            Filled_At TEXT,
            Raw_JSON TEXT,
            Updated_At TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (Order_Id, Fill_Key)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_fills_exchange_order
        ON Order_Fills(Exchange_Id, Exchange_Order_Id, Order_Id)
        """
    )


def validate_durable_order_fills_schema(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "Orders"):
        return
    missing = sorted(set(ORDER_COLUMNS) - _columns(connection, "Orders"))
    if missing:
        raise ValueError("Missing durable order columns: " + ", ".join(missing))
    if not _table_exists(connection, "Order_Fills"):
        raise ValueError("Order_Fills is missing")
    indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='Order_Fills'"
        )
    }
    if "idx_order_fills_exchange_order" not in indexes:
        raise ValueError("Order_Fills reconciliation index is missing")
