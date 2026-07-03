"""Ordered migration registry.

PR 2 introduces only the framework baseline. Exchange-aware schema migrations
are intentionally added by PR 3.
"""

from __future__ import annotations

import sqlite3

from bec.db.migrations.core import Migration, MigrationKind


def _framework_baseline(connection: sqlite3.Connection) -> None:
    # The tracking table and its index are bootstrapped by the framework before
    # migrations are evaluated. This marker establishes the first code version.
    del connection


MIGRATIONS = (
    Migration(
        version=1,
        name="migration_framework_baseline",
        kind=MigrationKind.ADDITIVE,
        apply=_framework_baseline,
        signature="bec-migrations-v1",
    ),
)

