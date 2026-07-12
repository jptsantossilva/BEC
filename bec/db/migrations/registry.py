"""Ordered migration registry.

PR 2 introduces only the framework baseline. Exchange-aware schema migrations
are intentionally added by PR 3.
"""

from __future__ import annotations

import sqlite3

from bec.db.backtesting_schema import (
    apply_exchange_backtesting_schema,
    apply_kraken_backtesting_defaults,
    validate_exchange_backtesting_schema,
    validate_kraken_backtesting_defaults,
)
from bec.db.backtesting_context_schema import (
    apply_backtesting_context_schema,
    validate_backtesting_context_schema,
)
from bec.db.exchange_schema import (
    apply_exchange_aware_schema,
    register_kraken_public_exchange,
    validate_exchange_aware_schema,
)
from bec.db.migrations.core import Migration, MigrationKind
from bec.db.live_execution_schema import (
    apply_live_execution_schema,
    validate_live_execution_schema,
)
from bec.db.order_fills_schema import (
    apply_durable_order_fills_schema,
    validate_durable_order_fills_schema,
)
from bec.db.okx_configuration_schema import (
    apply_okx_configuration_schema,
    validate_okx_configuration_schema,
)


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
        "SELECT Name, Trading_Mode FROM Exchanges WHERE Code='kraken'"
    ).fetchone()
    if row != ("Kraken", "spot"):
        raise ValueError("Kraken public exchange metadata is missing or unsafe")


def _exchange_specific_backtesting(connection: sqlite3.Connection) -> None:
    apply_exchange_backtesting_schema(connection)


def _kraken_backtesting_defaults(connection: sqlite3.Connection) -> None:
    apply_kraken_backtesting_defaults(connection)


def _gated_kraken_live_execution(connection: sqlite3.Connection) -> None:
    apply_live_execution_schema(connection)


def _durable_order_fills(connection: sqlite3.Connection) -> None:
    apply_durable_order_fills_schema(connection)


def _okx_configuration(connection: sqlite3.Connection) -> None:
    apply_okx_configuration_schema(connection)


def _backtesting_context(connection: sqlite3.Connection) -> None:
    apply_backtesting_context_schema(connection)


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
    Migration(
        version=4,
        name="exchange_specific_backtesting",
        kind=MigrationKind.ADDITIVE,
        apply=_exchange_specific_backtesting,
        validate=validate_exchange_backtesting_schema,
        signature="bec-exchange-specific-backtesting-v1",
    ),
    Migration(
        version=5,
        name="kraken_backtesting_defaults",
        kind=MigrationKind.ADDITIVE,
        apply=_kraken_backtesting_defaults,
        validate=validate_kraken_backtesting_defaults,
        signature="bec-kraken-backtesting-defaults-v1",
    ),
    Migration(
        version=6,
        name="gated_kraken_live_execution",
        kind=MigrationKind.ADDITIVE,
        apply=_gated_kraken_live_execution,
        validate=validate_live_execution_schema,
        signature="bec-gated-kraken-live-execution-v1",
    ),
    Migration(
        version=7,
        name="durable_order_fills",
        kind=MigrationKind.ADDITIVE,
        apply=_durable_order_fills,
        validate=validate_durable_order_fills_schema,
        signature="bec-durable-order-fills-v1",
    ),
    Migration(
        version=8,
        name="okx_configuration",
        kind=MigrationKind.ADDITIVE,
        apply=_okx_configuration,
        validate=validate_okx_configuration_schema,
        signature="bec-okx-configuration-v1",
    ),
    Migration(
        version=9,
        name="backtesting_exchange_context",
        kind=MigrationKind.ADDITIVE,
        apply=_backtesting_context,
        validate=validate_backtesting_context_schema,
        signature="bec-backtesting-exchange-context-v1",
    ),
)
