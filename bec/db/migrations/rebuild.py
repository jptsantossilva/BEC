"""Rollback-safe helpers for future SQLite table rebuild migrations."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Callable, Sequence

from bec.db.migrations.core import MigrationError


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class RebuildResult:
    table: str
    rows_before: int
    rows_after: int


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise MigrationError(f"Unsafe SQLite identifier: {value!r}")
    return f'"{value}"'


def rebuild_table(
    connection: sqlite3.Connection,
    *,
    table: str,
    create_sql: str,
    copy_sql: str,
    index_sql: Sequence[str] = (),
    required_indexes: Sequence[str] = (),
    validate_new_table: Callable[[sqlite3.Connection, str], None] | None = None,
) -> RebuildResult:
    """Rebuild one table inside the caller's transaction.

    ``create_sql`` and ``copy_sql`` may use ``{new_table}`` and
    ``{source_table}`` placeholders. The source table is dropped only after the
    new table has passed row-count and custom validation.
    """
    if not connection.in_transaction:
        raise MigrationError("rebuild_table must run inside a migration transaction")

    source_name = _quote_identifier(table)
    new_name_raw = f"__migration_new_{table}"
    new_name = _quote_identifier(new_name_raw)
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone() is None:
        raise MigrationError(f"Cannot rebuild missing table: {table}")
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (new_name_raw,),
    ).fetchone() is not None:
        raise MigrationError(f"Temporary migration table already exists: {new_name_raw}")

    rows_before = int(
        connection.execute(f"SELECT COUNT(*) FROM {source_name}").fetchone()[0]
    )
    substitutions = {"new_table": new_name, "source_table": source_name}
    connection.execute(create_sql.format(**substitutions))
    connection.execute(copy_sql.format(**substitutions))
    rows_after = int(
        connection.execute(f"SELECT COUNT(*) FROM {new_name}").fetchone()[0]
    )
    if rows_after != rows_before:
        raise MigrationError(
            f"Row-count mismatch rebuilding {table}: {rows_before} != {rows_after}"
        )
    if validate_new_table is not None:
        validate_new_table(connection, new_name_raw)

    connection.execute(f"DROP TABLE {source_name}")
    connection.execute(f"ALTER TABLE {new_name} RENAME TO {source_name}")
    for statement in index_sql:
        connection.execute(statement)

    existing_indexes = {
        str(row[1]) for row in connection.execute(f"PRAGMA index_list({source_name})")
    }
    missing_indexes = sorted(set(required_indexes) - existing_indexes)
    if missing_indexes:
        raise MigrationError(
            f"Missing indexes after rebuilding {table}: {missing_indexes}"
        )

    final_count = int(
        connection.execute(f"SELECT COUNT(*) FROM {source_name}").fetchone()[0]
    )
    if final_count != rows_before:
        raise MigrationError(
            f"Post-swap row-count mismatch rebuilding {table}: "
            f"{rows_before} != {final_count}"
        )
    return RebuildResult(table, rows_before, final_count)
