"""Additive registration schema for the disabled OKX identities."""

from __future__ import annotations

import sqlite3


EXCHANGE_COLUMNS = {
    "Adapter_Id": "TEXT NOT NULL DEFAULT 'binance'",
    "Execution_Environment": "TEXT NOT NULL DEFAULT 'production'",
}

SYMBOL_COLUMNS = {
    "Amount_Step": "REAL",
    "Price_Step": "REAL",
    "Min_Amount": "REAL",
    "Max_Amount": "REAL",
    "Min_Cost": "REAL",
    "Max_Cost": "REAL",
    "Market_Type": "TEXT NOT NULL DEFAULT 'spot'",
    "Is_Spot": "INTEGER NOT NULL DEFAULT 1",
    "Is_Contract": "INTEGER NOT NULL DEFAULT 0",
    "Contract_Size": "REAL",
    "Is_Linear": "INTEGER NOT NULL DEFAULT 0",
    "Is_Inverse": "INTEGER NOT NULL DEFAULT 0",
    "Settle_Asset": "TEXT",
    "Raw_Metadata_JSON": "TEXT",
    "Last_Synced_At": "TEXT",
    "Availability_Status": "TEXT NOT NULL DEFAULT 'unknown'",
}

OKX_IDENTITIES = (
    (3, "okx", "OKX (Production)", "myokx", "production"),
    (4, "okx_demo", "OKX (Demo)", "myokx", "demo"),
)


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


def apply_okx_configuration_schema(connection: sqlite3.Connection) -> None:
    """Register disabled OKX production/demo identities without API access."""
    _add_columns(connection, "Exchanges", EXCHANGE_COLUMNS)
    _add_columns(connection, "Exchange_Symbols", SYMBOL_COLUMNS)
    if not _table_exists(connection, "Exchanges"):
        return

    # Existing identities gain static adapter/environment metadata only. Their
    # enabled/default state, quote asset, flags, schedules, and fees are untouched.
    connection.execute(
        "UPDATE Exchanges SET Adapter_Id='binance', Execution_Environment='production' "
        "WHERE Code='binance'"
    )
    connection.execute(
        "UPDATE Exchanges SET Adapter_Id='kraken', Execution_Environment='production' "
        "WHERE Code='kraken'"
    )
    for exchange_id, code, name, adapter_id, environment in OKX_IDENTITIES:
        connection.execute(
            """
            INSERT INTO Exchanges (
                Id, Code, Name, Enabled, Is_Default, Quote_Asset, Trading_Mode,
                Adapter_Id, Execution_Environment, Buy_Enabled, Sell_Enabled
            ) VALUES (?, ?, ?, 0, 0, 'USDC', 'spot', ?, ?, 0, 0)
            ON CONFLICT(Code) DO UPDATE SET
                Name=excluded.Name,
                Trading_Mode=excluded.Trading_Mode,
                Adapter_Id=excluded.Adapter_Id,
                Execution_Environment=excluded.Execution_Environment
            """,
            (exchange_id, code, name, adapter_id, environment),
        )


def validate_okx_configuration_schema(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "Exchanges"):
        raise ValueError("Exchanges is missing")
    missing_exchange = sorted(set(EXCHANGE_COLUMNS) - _columns(connection, "Exchanges"))
    if missing_exchange:
        raise ValueError("Missing exchange configuration columns: " + ", ".join(missing_exchange))
    if _table_exists(connection, "Exchange_Symbols"):
        missing_symbols = sorted(set(SYMBOL_COLUMNS) - _columns(connection, "Exchange_Symbols"))
        if missing_symbols:
            raise ValueError("Missing exchange symbol metadata columns: " + ", ".join(missing_symbols))

    expected = {
        "binance": ("binance", "production"),
        "kraken": ("kraken", "production"),
        "okx": ("myokx", "production"),
        "okx_demo": ("myokx", "demo"),
    }
    rows = {
        str(code): (
            str(adapter_id), str(environment), int(enabled), int(default), str(quote),
            int(buy_enabled), int(sell_enabled),
        )
        for code, adapter_id, environment, enabled, default, quote, buy_enabled, sell_enabled in connection.execute(
            """
            SELECT Code, Adapter_Id, Execution_Environment, Enabled, Is_Default, Quote_Asset,
                   Buy_Enabled, Sell_Enabled
            FROM Exchanges WHERE Code IN ('binance', 'kraken', 'okx', 'okx_demo')
            """
        )
    }
    for code, (adapter_id, environment) in expected.items():
        if code not in rows or rows[code][:2] != (adapter_id, environment):
            raise ValueError(f"{code} adapter/environment metadata is invalid")
    for code in ("okx", "okx_demo"):
        _, _, enabled, default, quote, buy_enabled, sell_enabled = rows[code]
        if enabled or default or buy_enabled or sell_enabled or quote != "USDC":
            raise ValueError(
                f"{code} must remain disabled, non-default, and quoted in USDC with live flags off"
            )
    configured_fee = connection.execute(
        """
        SELECT 1 FROM Exchange_Backtesting_Settings ebs
        JOIN Exchanges e ON e.Id=ebs.Exchange_Id
        WHERE e.Code IN ('okx', 'okx_demo') LIMIT 1
        """
    ).fetchone()
    if configured_fee is not None:
        raise ValueError("OKX must not receive an implicit backtesting fee")
