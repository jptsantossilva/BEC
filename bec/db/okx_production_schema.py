"""Production-only OKX schema and retirement of the legacy Demo identity."""

from __future__ import annotations

import sqlite3

from bec.db.okx_configuration_schema import EXCHANGE_COLUMNS, SYMBOL_COLUMNS


OKX_PRODUCTION_ID = 3
LIVE_TRADING_JOBS = ("main_1h", "main_4h", "main_1d")
UNSETTLED_ORDER_STATUSES = ("pending", "open", "partially_filled", "unknown")


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


def apply_okx_production_schema(connection: sqlite3.Connection) -> None:
    """Create the current production-only OKX identity for a new database."""
    _add_columns(connection, "Exchanges", EXCHANGE_COLUMNS)
    _add_columns(connection, "Exchange_Symbols", SYMBOL_COLUMNS)
    if not _table_exists(connection, "Exchanges"):
        return
    connection.execute(
        "UPDATE Exchanges SET Adapter_Id='binance', "
        "Execution_Environment='production' WHERE Code='binance'"
    )
    connection.execute(
        "UPDATE Exchanges SET Adapter_Id='kraken', "
        "Execution_Environment='production' WHERE Code='kraken'"
    )
    connection.execute(
        """
        INSERT INTO Exchanges (
            Id, Code, Name, Enabled, Is_Default, Quote_Asset, Trading_Mode,
            Adapter_Id, Execution_Environment, Buy_Enabled, Sell_Enabled
        ) VALUES (?, 'okx', 'OKX', 0, 0, 'USDC', 'spot', 'myokx',
                  'production', 0, 0)
        ON CONFLICT(Code) DO UPDATE SET
            Name='OKX', Trading_Mode='spot', Adapter_Id='myokx',
            Execution_Environment='production'
        """,
        (OKX_PRODUCTION_ID,),
    )


def _demo_exchange_id(connection: sqlite3.Connection) -> int | None:
    if not _table_exists(connection, "Exchanges"):
        return None
    row = connection.execute(
        "SELECT Id FROM Exchanges WHERE Code='okx_demo'"
    ).fetchone()
    return None if row is None else int(row[0])


def _demo_runtime_blockers(
    connection: sqlite3.Connection, exchange_id: int
) -> tuple[int, int]:
    open_positions = 0
    if _table_exists(connection, "Positions") and {
        "Exchange_Id",
        "Position",
    } <= _columns(connection, "Positions"):
        open_positions = int(
            connection.execute(
                "SELECT COUNT(*) FROM Positions WHERE Exchange_Id=? AND Position=1",
                (exchange_id,),
            ).fetchone()[0]
        )
    unsettled_orders = 0
    if _table_exists(connection, "Orders") and {
        "Exchange_Id",
        "Order_Status",
    } <= _columns(connection, "Orders"):
        placeholders = ",".join("?" for _ in UNSETTLED_ORDER_STATUSES)
        unsettled_orders = int(
            connection.execute(
                f"SELECT COUNT(*) FROM Orders WHERE Exchange_Id=? "
                f"AND LOWER(COALESCE(Order_Status, '')) IN ({placeholders})",
                (exchange_id, *UNSETTLED_ORDER_STATUSES),
            ).fetchone()[0]
        )
    return open_positions, unsettled_orders


def apply_retire_okx_demo(connection: sqlite3.Connection) -> None:
    """Archive Demo safely and disarm OKX before removing its validation gate."""
    if not _table_exists(connection, "Exchanges"):
        return
    active = connection.execute(
        "SELECT Code FROM Exchanges WHERE Enabled=1 AND Is_Default=1 LIMIT 1"
    ).fetchone()
    active_code = str(active[0]) if active else ""
    demo_id = _demo_exchange_id(connection)
    if active_code == "okx_demo":
        raise ValueError(
            "OKX Demo is active; finish the legacy validation workflow and select "
            "a production exchange before retiring OKX Demo"
        )
    if demo_id is not None:
        open_positions, unsettled_orders = _demo_runtime_blockers(connection, demo_id)
        if open_positions or unsettled_orders:
            raise ValueError(
                "OKX Demo retirement requires no open Demo positions or unsettled "
                f"Demo orders (open_positions={open_positions}, "
                f"unsettled_orders={unsettled_orders})"
            )
        connection.execute(
            """
            UPDATE Exchanges
            SET Enabled=0, Is_Default=0, Buy_Enabled=0, Sell_Enabled=0,
                Updated_At=CURRENT_TIMESTAMP
            WHERE Id=?
            """,
            (demo_id,),
        )
    connection.execute(
        """
        UPDATE Exchanges
        SET Buy_Enabled=0, Sell_Enabled=0, Updated_At=CURRENT_TIMESTAMP
        WHERE Code='okx'
        """
    )
    if _table_exists(connection, "Settings"):
        connection.execute(
            "UPDATE Settings SET value='test' WHERE name='run_mode' AND value='demo'"
        )
        if active_code == "okx":
            connection.execute(
                """
                INSERT INTO Settings (name, value)
                VALUES ('run_mode', 'test')
                ON CONFLICT(name) DO UPDATE SET value='test'
                """
            )
    if active_code == "okx" and _table_exists(connection, "Job_Schedules"):
        placeholders = ",".join("?" for _ in LIVE_TRADING_JOBS)
        connection.execute(
            f"UPDATE Job_Schedules SET enabled=0 WHERE name IN ({placeholders})",
            LIVE_TRADING_JOBS,
        )


def validate_retire_okx_demo(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "Exchanges"):
        raise ValueError("Exchanges is missing")
    production = connection.execute(
        """
        SELECT Name, Execution_Environment, Buy_Enabled, Sell_Enabled
        FROM Exchanges WHERE Code='okx'
        """
    ).fetchone()
    if production != ("OKX", "production", 0, 0):
        raise ValueError("OKX production retirement defaults are invalid")
    demo_id = _demo_exchange_id(connection)
    if demo_id is not None:
        demo = connection.execute(
            """
            SELECT Enabled, Is_Default, Buy_Enabled, Sell_Enabled
            FROM Exchanges WHERE Id=?
            """,
            (demo_id,),
        ).fetchone()
        if demo != (0, 0, 0, 0):
            raise ValueError("The archived OKX Demo identity is not fully disabled")
        open_positions, unsettled_orders = _demo_runtime_blockers(connection, demo_id)
        if open_positions or unsettled_orders:
            raise ValueError("The archived OKX Demo identity still has runtime activity")
    if _table_exists(connection, "Settings"):
        demo_mode = connection.execute(
            "SELECT 1 FROM Settings WHERE name='run_mode' AND value='demo'"
        ).fetchone()
        if demo_mode is not None:
            raise ValueError("The retired OKX Demo run mode is still configured")
