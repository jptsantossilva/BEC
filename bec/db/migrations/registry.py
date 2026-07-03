"""Ordered migration registry.

PR 2 introduces only the framework baseline. Exchange-aware schema migrations
are intentionally added by PR 3.
"""

from __future__ import annotations

import sqlite3

from bec.db.exchange_schema import apply_exchange_aware_schema
from bec.db.migrations.core import Migration, MigrationKind


def _framework_baseline(connection: sqlite3.Connection) -> None:
    # The tracking table and its index are bootstrapped by the framework before
    # migrations are evaluated. This marker establishes the first code version.
    del connection


def _exchange_aware_schema(connection: sqlite3.Connection) -> None:
    apply_exchange_aware_schema(connection, upgraded_install=True)


def _validate_exchange_aware_schema(connection: sqlite3.Connection) -> None:
    binance = connection.execute(
        "SELECT Trading_Mode FROM Exchanges WHERE Code='binance'"
    ).fetchone()
    if binance != ("spot",):
        raise ValueError("Binance spot metadata is missing")
    for table in ("Orders", "Positions", "Backtesting_Results", "Balances"):
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is None:
            continue
        columns = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if "Exchange_Id" not in columns:
            raise ValueError(f"Missing Exchange_Id on {table}")


MIGRATIONS = (
    Migration(
        version=1,
        name="migration_framework_baseline",
        kind=MigrationKind.ADDITIVE,
        apply=_framework_baseline,
        signature="bec-migrations-v1",
    ),
    Migration(
        version=2,
        name="exchange_aware_schema",
        kind=MigrationKind.REBUILD,
        apply=_exchange_aware_schema,
        validate=_validate_exchange_aware_schema,
        signature="bec-exchange-aware-schema-v1",
    ),
)
