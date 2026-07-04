"""Ordered migration registry.

PR 2 introduces only the framework baseline. Exchange-aware schema migrations
are intentionally added by PR 3.
"""

from __future__ import annotations

import sqlite3

from bec.db.exchange_schema import (
    apply_exchange_aware_schema,
    register_kraken_public_exchange,
    validate_exchange_aware_schema,
)
from bec.db.migrations.core import Migration, MigrationKind


def _framework_baseline(connection: sqlite3.Connection) -> None:
    # The tracking table and its index are bootstrapped by the framework before
    # migrations are evaluated. This marker establishes the first code version.
    del connection


def _exchange_aware_schema(connection: sqlite3.Connection) -> None:
    apply_exchange_aware_schema(connection, upgraded_install=True)


def _kraken_public_exchange(connection: sqlite3.Connection) -> None:
    register_kraken_public_exchange(connection)


def _validate_kraken_public_exchange(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT Name, Is_Default, Trading_Mode FROM Exchanges WHERE Code='kraken'"
    ).fetchone()
    if row != ("Kraken", 0, "spot"):
        raise ValueError("Kraken public exchange metadata is missing or unsafe")


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
        validate=validate_exchange_aware_schema,
        signature="bec-exchange-aware-schema-v1",
    ),
    Migration(
        version=3,
        name="kraken_public_exchange",
        kind=MigrationKind.ADDITIVE,
        apply=_kraken_public_exchange,
        validate=_validate_kraken_public_exchange,
        signature="bec-kraken-public-exchange-v1",
    ),
)
