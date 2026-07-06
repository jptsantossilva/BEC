"""Additive schema for gated Kraken live execution (PR 7)."""

from __future__ import annotations

import sqlite3


EXCHANGE_COLUMNS = {
    "Buy_Enabled": "INTEGER NOT NULL DEFAULT 0",
    "Sell_Enabled": "INTEGER NOT NULL DEFAULT 0",
    "Partial_Sell_Policy": "TEXT NOT NULL DEFAULT 'accumulate'",
    "Sizing_Buffer_Pct": "REAL NOT NULL DEFAULT 1.0",
}

ORDER_COLUMNS = {
    "Client_Order_Id": "TEXT",
    # Retain the historical position id even after candidate cleanup.
    "Position_Id": "INTEGER",
    "Take_Profit_Num": "INTEGER NOT NULL DEFAULT 0",
    "Intent_State": "TEXT NOT NULL DEFAULT 'legacy_filled'",
    "Requested_Qty": "REAL",
    "Requested_Quote_Qty": "REAL",
    "Applied_Executed_Qty": "REAL NOT NULL DEFAULT 0",
    "Applied_Net_Qty": "REAL NOT NULL DEFAULT 0",
    "Submission_Attempts": "INTEGER NOT NULL DEFAULT 0",
    "Last_Reconciled_At": "TEXT",
    "Raw_Response_JSON": "TEXT",
    "Error_Message": "TEXT",
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
            connection.execute(
                f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}'
            )


def apply_live_execution_schema(connection: sqlite3.Connection) -> None:
    """Add guarded live settings and durable order-intent metadata."""
    _add_columns(connection, "Exchanges", EXCHANGE_COLUMNS)
    _add_columns(connection, "Orders", ORDER_COLUMNS)
    if _table_exists(connection, "Orders"):
        order_columns = _columns(connection, "Orders")
        if {"Exchange_Id", "Client_Order_Id"} <= order_columns:
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_exchange_client_order
                ON Orders(Exchange_Id, Client_Order_Id)
                WHERE Client_Order_Id IS NOT NULL AND Client_Order_Id <> ''
                """
            )
        if {"Exchange_Id", "Order_Status", "Intent_State", "Id"} <= order_columns:
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_orders_reconciliation
                ON Orders(Exchange_Id, Order_Status, Intent_State, Id)
                """
            )
        if {
            "Exchange_Id",
            "Position_Id",
            "Symbol",
            "Side",
            "Order_Status",
        } <= order_columns:
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_unsettled_action
                ON Orders(Exchange_Id, COALESCE(Position_Id, -1), Symbol, Side)
                WHERE Order_Status IN ('pending','open','partially_filled','unknown')
                """
            )

    # A migration or fresh install must never activate live Kraken operations.
    if _table_exists(connection, "Exchanges"):
        connection.execute(
            """
            UPDATE Exchanges
            SET Buy_Enabled=1, Sell_Enabled=1
            WHERE Code='binance' AND Enabled=1 AND Is_Default=1
            """
        )
        connection.execute(
            """
            UPDATE Exchanges
            SET Buy_Enabled=0, Sell_Enabled=0,
                Partial_Sell_Policy=COALESCE(NULLIF(Partial_Sell_Policy, ''), 'accumulate'),
                Sizing_Buffer_Pct=CASE
                    WHEN Sizing_Buffer_Pct IS NULL OR Sizing_Buffer_Pct < 0
                    THEN 1.0 ELSE Sizing_Buffer_Pct END
            WHERE Code='kraken'
            """
        )


def validate_live_execution_schema(connection: sqlite3.Connection) -> None:
    missing = []
    for table, expected in (
        ("Exchanges", set(EXCHANGE_COLUMNS)),
        ("Orders", set(ORDER_COLUMNS)),
    ):
        if not _table_exists(connection, table):
            continue
        missing.extend(
            f"{table}.{column}" for column in sorted(expected - _columns(connection, table))
        )
    if missing:
        raise ValueError("Missing PR7 live execution columns: " + ", ".join(missing))

    if _table_exists(connection, "Orders"):
        order_columns = _columns(connection, "Orders")
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='Orders'"
            )
        }
        expected_indexes = set()
        if {"Exchange_Id", "Client_Order_Id"} <= order_columns:
            expected_indexes.add("idx_orders_exchange_client_order")
        if {"Exchange_Id", "Order_Status", "Intent_State", "Id"} <= order_columns:
            expected_indexes.add("idx_orders_reconciliation")
        if {"Exchange_Id", "Position_Id", "Symbol", "Side", "Order_Status"} <= order_columns:
            expected_indexes.add("idx_orders_unsettled_action")
        if not expected_indexes <= indexes:
            raise ValueError(
                "Missing PR7 order indexes: "
                + ", ".join(sorted(expected_indexes - indexes))
            )

    kraken = connection.execute(
        """
        SELECT Buy_Enabled, Sell_Enabled, Partial_Sell_Policy, Sizing_Buffer_Pct
        FROM Exchanges WHERE Code='kraken'
        """
    ).fetchone()
    if kraken is None:
        raise ValueError("Kraken exchange metadata is missing")
    if str(kraken[2]) not in {"accumulate", "sell_all", "skip"}:
        raise ValueError("Invalid Kraken partial-sell policy")
    if float(kraken[3]) < 0:
        raise ValueError("Invalid Kraken sizing buffer")
