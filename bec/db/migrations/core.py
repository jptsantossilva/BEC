"""Versioned, auditable SQLite migration primitives."""

from __future__ import annotations

import hashlib
import json
import shlex
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Sequence


MIGRATIONS_TABLE = "Schema_Migrations"
MIGRATION_STATE_TABLE = "Schema_Migration_State"
NEW_DATABASE_STATE_KEY = "new_database_initialization"


class MigrationError(RuntimeError):
    pass


class MigrationIntegrityError(MigrationError):
    pass


class PendingManualMigrationError(MigrationError):
    pass


class BackupRequiredError(MigrationError):
    pass


class MigrationKind(str, Enum):
    ADDITIVE = "additive"
    REBUILD = "rebuild"


MigrationCallable = Callable[[sqlite3.Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    kind: MigrationKind
    apply: MigrationCallable
    signature: str
    validate: MigrationCallable | None = None

    @property
    def checksum(self) -> str:
        payload = f"{self.version}:{self.name}:{self.kind.value}:{self.signature}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AppliedMigration:
    version: int
    name: str
    kind: str
    checksum: str
    applied_at: str


@dataclass
class MigrationReport:
    mode: str
    database: str
    started_at: str
    finished_at: str = ""
    pending: list[str] = field(default_factory=list)
    applied: list[str] = field(default_factory=list)
    row_counts_before: dict[str, int] = field(default_factory=dict)
    row_counts_after: dict[str, int] = field(default_factory=dict)
    schema_sha256_before: str = ""
    schema_sha256_after: str = ""
    source_sha256_before: str = ""
    source_sha256_after: str = ""
    backup_path: str = ""
    backup_sha256: str = ""
    integrity_check: str = ""
    foreign_key_violations: int = 0
    unresolved_legacy_symbols: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_registry(migrations: Sequence[Migration]) -> tuple[Migration, ...]:
    ordered = tuple(sorted(migrations, key=lambda item: item.version))
    versions = [item.version for item in ordered]
    names = [item.name for item in ordered]
    if any(version <= 0 for version in versions):
        raise MigrationIntegrityError("Migration versions must be positive integers")
    if len(versions) != len(set(versions)):
        raise MigrationIntegrityError("Migration versions must be unique")
    if len(names) != len(set(names)):
        raise MigrationIntegrityError("Migration names must be unique")
    return ordered


def ensure_migrations_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            Version INTEGER PRIMARY KEY,
            Name TEXT NOT NULL UNIQUE,
            Kind TEXT NOT NULL,
            Checksum TEXT NOT NULL,
            Applied_At TEXT NOT NULL
        )
        """
    )
    connection.execute(
        f"CREATE INDEX IF NOT EXISTS idx_schema_migrations_kind "
        f"ON {MIGRATIONS_TABLE}(Kind)"
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATION_STATE_TABLE} (
            Key TEXT PRIMARY KEY,
            Value TEXT NOT NULL,
            Updated_At TEXT NOT NULL
        )
        """
    )
    connection.commit()


def migrations_table_exists(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (MIGRATIONS_TABLE,),
    ).fetchone()
    return row is not None


def read_applied_migrations(connection: sqlite3.Connection) -> dict[int, AppliedMigration]:
    if not migrations_table_exists(connection):
        return {}
    rows = connection.execute(
        f"SELECT Version, Name, Kind, Checksum, Applied_At "
        f"FROM {MIGRATIONS_TABLE} ORDER BY Version"
    ).fetchall()
    return {
        int(row[0]): AppliedMigration(
            version=int(row[0]),
            name=str(row[1]),
            kind=str(row[2]),
            checksum=str(row[3]),
            applied_at=str(row[4]),
        )
        for row in rows
    }


def pending_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
) -> tuple[Migration, ...]:
    registry = validate_registry(migrations)
    applied = read_applied_migrations(connection)
    registry_by_version = {item.version: item for item in registry}

    unknown = sorted(set(applied) - set(registry_by_version))
    if unknown:
        raise MigrationIntegrityError(
            f"Database contains migration versions unknown to this build: {unknown}"
        )
    for version, record in applied.items():
        migration = registry_by_version[version]
        if (
            record.name != migration.name
            or record.kind != migration.kind.value
            or record.checksum != migration.checksum
        ):
            raise MigrationIntegrityError(
                f"Applied migration {version} does not match this build"
            )
    applied_versions = sorted(applied)
    expected_prefix = [item.version for item in registry[: len(applied_versions)]]
    if applied_versions != expected_prefix:
        raise MigrationIntegrityError(
            "Applied migrations are not a contiguous registry prefix: "
            f"{applied_versions} != {expected_prefix}"
        )
    return tuple(item for item in registry if item.version not in applied)


def database_path(connection: sqlite3.Connection) -> Path:
    for _, name, path in connection.execute("PRAGMA database_list").fetchall():
        if name == "main" and path:
            return Path(path).resolve()
    raise MigrationError("The main SQLite database has no filesystem path")


@contextmanager
def database_migration_lock(database: str | Path):
    """Serialize application startup and explicit migration apply operations."""
    import fcntl

    source = Path(database).resolve()
    lock_path = source.with_name(f"{source.name}.migration.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def is_new_database(connection: sqlite3.Connection) -> bool:
    state_table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (MIGRATION_STATE_TABLE,),
    ).fetchone()
    if state_table_exists is not None:
        pending_state = connection.execute(
            f"SELECT Value FROM {MIGRATION_STATE_TABLE} WHERE Key = ?",
            (NEW_DATABASE_STATE_KEY,),
        ).fetchone()
        if pending_state is not None and str(pending_state[0]) == "pending":
            return True
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
          AND name != ?
          AND name != ?
        """,
        (MIGRATIONS_TABLE, MIGRATION_STATE_TABLE),
    ).fetchone()
    return int(row[0]) == 0


def validate_database(connection: sqlite3.Connection) -> tuple[str, int]:
    integrity_rows = connection.execute("PRAGMA integrity_check").fetchall()
    integrity = "; ".join(str(row[0]) for row in integrity_rows)
    if integrity.lower() != "ok":
        raise MigrationIntegrityError(f"SQLite integrity_check failed: {integrity}")

    foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_rows:
        raise MigrationIntegrityError(
            f"SQLite foreign_key_check found {len(foreign_key_rows)} violation(s)"
        )
    return integrity, len(foreign_key_rows)


def collect_unresolved_legacy_symbols(connection: sqlite3.Connection) -> list[str]:
    try:
        from bec.db.exchange_schema import unresolved_legacy_symbols

        return unresolved_legacy_symbols(connection)
    except sqlite3.OperationalError:
        return []


def table_row_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = [
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
    ]
    counts: dict[str, int] = {}
    for table in tables:
        quoted = table.replace('"', '""')
        counts[table] = int(
            connection.execute(f'SELECT COUNT(*) FROM "{quoted}"').fetchone()[0]
        )
    return counts


def schema_snapshot(connection: sqlite3.Connection) -> dict[str, dict]:
    snapshot: dict[str, dict] = {}
    tables = connection.execute(
        """
        SELECT name, sql FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    for table, create_sql in tables:
        quoted = str(table).replace('"', '""')
        columns = [
            {
                "name": row[1],
                "type": row[2],
                "not_null": bool(row[3]),
                "default": row[4],
                "primary_key": bool(row[5]),
            }
            for row in connection.execute(f'PRAGMA table_info("{quoted}")').fetchall()
        ]
        indexes = []
        for index_row in connection.execute(f'PRAGMA index_list("{quoted}")').fetchall():
            index_name = str(index_row[1])
            index_quoted = index_name.replace('"', '""')
            index_columns = [
                row[2]
                for row in connection.execute(
                    f'PRAGMA index_info("{index_quoted}")'
                ).fetchall()
            ]
            indexes.append(
                {
                    "name": index_name,
                    "unique": bool(index_row[2]),
                    "columns": index_columns,
                }
            )
        snapshot[str(table)] = {
            "sql": create_sql,
            "columns": columns,
            "indexes": indexes,
        }
    return snapshot


def schema_fingerprint(connection: sqlite3.Connection) -> str:
    serialized = json.dumps(
        schema_snapshot(connection),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def apply_pending_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
    *,
    allow_rebuild: bool,
) -> list[Migration]:
    ensure_migrations_table(connection)
    applied: list[Migration] = []

    while True:
        migration: Migration | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            pending = pending_migrations(connection, migrations)
            if not pending:
                connection.commit()
                break
            migration = pending[0]
            if migration.kind is MigrationKind.REBUILD and not allow_rebuild:
                path = database_path(connection)
                quoted_path = shlex.quote(str(path))
                connection.rollback()
                raise PendingManualMigrationError(
                    f"Migration {migration.version} ({migration.name}) rebuilds tables. "
                    f"Run: python -m bec.db.migrations --database {quoted_path} --dry-run "
                    f"and then: python -m bec.db.migrations --database {quoted_path} "
                    f"--backup --apply"
                )
            migration.apply(connection)
            if migration.validate is not None:
                migration.validate(connection)
            validate_database(connection)
            connection.execute(
                f"""
                INSERT INTO {MIGRATIONS_TABLE}
                    (Version, Name, Kind, Checksum, Applied_At)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    migration.version,
                    migration.name,
                    migration.kind.value,
                    migration.checksum,
                    utc_now(),
                ),
            )
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, MigrationError):
                raise
            migration_label = (
                f"{migration.version} ({migration.name})"
                if migration is not None
                else "before migration selection"
            )
            raise MigrationError(
                f"Migration {migration_label} failed: {exc}"
            ) from exc
        applied.append(migration)
    return applied


def prepare_startup_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
    *,
    new_database: bool,
) -> list[Migration]:
    ensure_migrations_table(connection)
    if new_database:
        connection.execute(
            f"""
            INSERT INTO {MIGRATION_STATE_TABLE} (Key, Value, Updated_At)
            VALUES (?, 'pending', ?)
            ON CONFLICT(Key) DO UPDATE SET
                Value = excluded.Value,
                Updated_At = excluded.Updated_At
            """,
            (NEW_DATABASE_STATE_KEY, utc_now()),
        )
        connection.commit()
        return []
    return apply_pending_migrations(connection, migrations, allow_rebuild=False)


def mark_new_database_current(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
) -> None:
    ensure_migrations_table(connection)
    registry = validate_registry(migrations)
    pending = pending_migrations(connection, registry)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for migration in pending:
            if migration.validate is not None:
                migration.validate(connection)
            connection.execute(
                f"""
                INSERT INTO {MIGRATIONS_TABLE}
                    (Version, Name, Kind, Checksum, Applied_At)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    migration.version,
                    migration.name,
                    migration.kind.value,
                    migration.checksum,
                    utc_now(),
                ),
            )
        connection.execute(
            f"DELETE FROM {MIGRATION_STATE_TABLE} WHERE Key = ?",
            (NEW_DATABASE_STATE_KEY,),
        )
        validate_database(connection)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file_handle:
        for block in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def copy_database(source: str | Path, destination: str | Path) -> None:
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source_path) as source_conn, sqlite3.connect(
        destination_path
    ) as destination_conn:
        source_conn.backup(destination_conn)


def create_backup(database: str | Path) -> tuple[Path, str]:
    source = Path(database).resolve()
    if not source.is_file():
        raise MigrationError(f"Database does not exist: {source}")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = source.with_name(f"{source.name}.backup-{timestamp}.sqlite3")
    suffix = 1
    while destination.exists():
        destination = source.with_name(
            f"{source.name}.backup-{timestamp}-{suffix}.sqlite3"
        )
        suffix += 1
    copy_database(source, destination)
    with sqlite3.connect(destination) as backup_connection:
        validate_database(backup_connection)
    return destination, sha256_file(destination)


def restore_backup(backup: str | Path, database: str | Path) -> None:
    backup_path = Path(backup).resolve()
    if not backup_path.is_file():
        raise MigrationError(f"Backup does not exist: {backup_path}")
    with sqlite3.connect(backup_path) as backup_connection:
        validate_database(backup_connection)
    destination = Path(database).resolve()
    copy_database(backup_path, destination)
    with sqlite3.connect(destination) as restored_connection:
        validate_database(restored_connection)


def run_dry_run(
    database: str | Path,
    migrations: Sequence[Migration],
) -> MigrationReport:
    source = Path(database).resolve()
    if not source.is_file():
        raise MigrationError(f"Database does not exist: {source}")
    source_hash_before = sha256_file(source)
    report = MigrationReport(
        mode="dry-run",
        database=str(source),
        started_at=utc_now(),
        source_sha256_before=source_hash_before,
    )

    with tempfile.TemporaryDirectory(prefix="bec-migration-") as temp_dir:
        temporary = Path(temp_dir) / source.name
        copy_database(source, temporary)
        with sqlite3.connect(temporary) as connection:
            report.row_counts_before = table_row_counts(connection)
            report.schema_sha256_before = schema_fingerprint(connection)
            report.pending = [
                f"{item.version}:{item.name}"
                for item in pending_migrations(connection, migrations)
            ]
            applied = apply_pending_migrations(
                connection, migrations, allow_rebuild=True
            )
            report.applied = [f"{item.version}:{item.name}" for item in applied]
            report.row_counts_after = table_row_counts(connection)
            report.schema_sha256_after = schema_fingerprint(connection)
            report.integrity_check, report.foreign_key_violations = validate_database(
                connection
            )
            report.unresolved_legacy_symbols = collect_unresolved_legacy_symbols(
                connection
            )

    report.source_sha256_after = sha256_file(source)
    if report.source_sha256_after != source_hash_before:
        raise MigrationIntegrityError("Dry-run modified the source database")
    report.finished_at = utc_now()
    return report


def _apply_database_migrations_unlocked(
    database: str | Path,
    migrations: Sequence[Migration],
    *,
    backup: bool,
) -> MigrationReport:
    source = Path(database).resolve()
    if not source.is_file():
        raise MigrationError(f"Database does not exist: {source}")
    source_hash_before = sha256_file(source)
    report = MigrationReport(
        mode="apply",
        database=str(source),
        started_at=utc_now(),
        source_sha256_before=source_hash_before,
    )

    with sqlite3.connect(source) as inspection_connection:
        pending = pending_migrations(inspection_connection, migrations)
        report.pending = [f"{item.version}:{item.name}" for item in pending]
        report.row_counts_before = table_row_counts(inspection_connection)
        report.schema_sha256_before = schema_fingerprint(inspection_connection)

    if pending and not backup:
        raise BackupRequiredError("--apply requires --backup when migrations are pending")
    if pending:
        backup_path, backup_hash = create_backup(source)
        report.backup_path = str(backup_path)
        report.backup_sha256 = backup_hash

    with sqlite3.connect(source) as connection:
        applied = apply_pending_migrations(connection, migrations, allow_rebuild=True)
        report.applied = [f"{item.version}:{item.name}" for item in applied]
        report.row_counts_after = table_row_counts(connection)
        report.schema_sha256_after = schema_fingerprint(connection)
        report.integrity_check, report.foreign_key_violations = validate_database(
            connection
        )
        report.unresolved_legacy_symbols = collect_unresolved_legacy_symbols(connection)

    report.source_sha256_after = sha256_file(source)
    report.finished_at = utc_now()
    return report


def apply_database_migrations(
    database: str | Path,
    migrations: Sequence[Migration],
    *,
    backup: bool,
) -> MigrationReport:
    source = Path(database).resolve()
    with database_migration_lock(source):
        return _apply_database_migrations_unlocked(
            source, migrations, backup=backup
        )
