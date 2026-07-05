import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from json import loads
from pathlib import Path

import pytest

from bec.db.migrations import (
    BackupRequiredError,
    Migration,
    MigrationError,
    MigrationIntegrityError,
    MigrationKind,
    PendingManualMigrationError,
    apply_database_migrations,
    apply_pending_migrations,
    create_backup,
    database_migration_lock,
    is_new_database,
    mark_new_database_current,
    pending_migrations,
    prepare_startup_migrations,
    restore_backup,
    run_dry_run,
)
from bec.db.migrations.core import (
    MIGRATIONS_TABLE,
    ensure_migrations_table,
    sha256_file,
)
from bec.db.migrations.rebuild import rebuild_table
from bec.db.migrations.__main__ import main as migration_cli


def _create_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE Items (Id INTEGER PRIMARY KEY, Value TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO Items (Id, Value) VALUES (?, ?)",
            [(1, "one"), (2, "two")],
        )


def _additive_migration(version=1, name="add_note") -> Migration:
    def apply(connection):
        connection.execute("ALTER TABLE Items ADD COLUMN Note TEXT")

    return Migration(
        version=version,
        name=name,
        kind=MigrationKind.ADDITIVE,
        apply=apply,
        signature=f"{name}-v1",
    )


def _rebuild_migration(version=1, name="rebuild_items") -> Migration:
    def apply(connection):
        rebuild_table(
            connection,
            table="Items",
            create_sql=(
                "CREATE TABLE {new_table} ("
                "Id INTEGER PRIMARY KEY, Value TEXT NOT NULL, Note TEXT)"
            ),
            copy_sql=(
                "INSERT INTO {new_table} (Id, Value, Note) "
                "SELECT Id, Value, 'migrated' FROM {source_table}"
            ),
            index_sql=("CREATE INDEX idx_items_value ON Items(Value)",),
            required_indexes=("idx_items_value",),
        )

    return Migration(
        version=version,
        name=name,
        kind=MigrationKind.REBUILD,
        apply=apply,
        signature=f"{name}-v1",
    )


def test_startup_applies_additive_migrations_once(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    migrations = (_additive_migration(),)

    with sqlite3.connect(database) as connection:
        assert is_new_database(connection) is False
        applied = prepare_startup_migrations(
            connection, migrations, new_database=False
        )
        repeated = prepare_startup_migrations(
            connection, migrations, new_database=False
        )

        assert [item.name for item in applied] == ["add_note"]
        assert repeated == []
        assert [row[1] for row in connection.execute("PRAGMA table_info(Items)")] == [
            "Id",
            "Value",
            "Note",
        ]
        assert connection.execute(
            f"SELECT COUNT(*) FROM {MIGRATIONS_TABLE}"
        ).fetchone()[0] == 1


def test_concurrent_startup_applies_migration_once(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)

    def apply(connection):
        connection.execute("CREATE TABLE Concurrent_Result (Id INTEGER)")

    migration = Migration(
        1,
        "concurrent_startup",
        MigrationKind.ADDITIVE,
        apply,
        "concurrent-v1",
    )

    def start_process():
        with sqlite3.connect(database, timeout=10) as connection:
            return len(
                prepare_startup_migrations(
                    connection, (migration,), new_database=False
                )
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: start_process(), range(2)))

    assert sorted(results) == [0, 1]
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            f"SELECT COUNT(*) FROM {MIGRATIONS_TABLE}"
        ).fetchone()[0] == 1


def test_database_migration_lock_serializes_initialization(tmp_path):
    database = tmp_path / "app.db"
    database.touch()
    order = []

    def locked_step(label):
        with database_migration_lock(database):
            order.append(f"{label}:start")
            if label == "first":
                time.sleep(0.05)
            order.append(f"{label}:end")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(locked_step, "first")
        time.sleep(0.01)
        second = executor.submit(locked_step, "second")
        first.result()
        second.result()

    assert order == ["first:start", "first:end", "second:start", "second:end"]


def test_startup_blocks_rebuild_and_later_migrations(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)

    def create_later_table(connection):
        connection.execute("CREATE TABLE Later (Id INTEGER PRIMARY KEY)")

    migrations = (
        _additive_migration(),
        _rebuild_migration(2),
        Migration(
            3,
            "later_additive",
            MigrationKind.ADDITIVE,
            create_later_table,
            "later-v1",
        ),
    )

    with sqlite3.connect(database) as connection:
        with pytest.raises(PendingManualMigrationError) as exc_info:
            prepare_startup_migrations(connection, migrations, new_database=False)

        assert "--dry-run" in str(exc_info.value)
        assert "--backup --apply" in str(exc_info.value)
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'Later'"
        ).fetchone() is None
        assert "Note" in {
            row[1] for row in connection.execute("PRAGMA table_info(Items)")
        }
        assert connection.execute(
            f"SELECT COUNT(*) FROM {MIGRATIONS_TABLE}"
        ).fetchone()[0] == 1


def test_dry_run_migrates_temporary_copy_without_touching_source(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    checksum_before = sha256_file(database)

    report = run_dry_run(database, (_rebuild_migration(),))

    assert report.mode == "dry-run"
    assert report.applied == ["1:rebuild_items"]
    assert report.row_counts_before["Items"] == 2
    assert report.row_counts_after["Items"] == 2
    assert report.integrity_check == "ok"
    assert report.schema_sha256_before != report.schema_sha256_after
    assert report.source_sha256_before == checksum_before
    assert report.source_sha256_after == checksum_before
    with sqlite3.connect(database) as connection:
        assert [row[1] for row in connection.execute("PRAGMA table_info(Items)")] == [
            "Id",
            "Value",
        ]
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = ?", (MIGRATIONS_TABLE,)
        ).fetchone() is None


def test_apply_requires_backup_and_creates_auditable_backup(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    migrations = (_rebuild_migration(),)

    with pytest.raises(BackupRequiredError):
        apply_database_migrations(database, migrations, backup=False)

    report = apply_database_migrations(database, migrations, backup=True)

    backup_path = Path(report.backup_path)
    assert backup_path.is_file()
    assert report.backup_sha256 == sha256_file(backup_path)
    assert report.applied == ["1:rebuild_items"]
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT Note FROM Items ORDER BY Id"
        ).fetchall() == [("migrated",), ("migrated",)]
        indexes = connection.execute("PRAGMA index_list(Items)").fetchall()
        assert "idx_items_value" in {row[1] for row in indexes}

    repeated = apply_database_migrations(database, migrations, backup=False)
    assert repeated.pending == []
    assert repeated.applied == []


def test_failed_migration_rolls_back_schema_and_tracking_row(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)

    def fail_after_write(connection):
        connection.execute("CREATE TABLE Must_Roll_Back (Id INTEGER)")
        raise ValueError("planned failure")

    migration = Migration(
        1,
        "failing_migration",
        MigrationKind.ADDITIVE,
        fail_after_write,
        "failure-v1",
    )
    with sqlite3.connect(database) as connection:
        with pytest.raises(MigrationError, match="planned failure"):
            apply_pending_migrations(connection, (migration,), allow_rebuild=True)

        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'Must_Roll_Back'"
        ).fetchone() is None
        assert connection.execute(
            f"SELECT COUNT(*) FROM {MIGRATIONS_TABLE}"
        ).fetchone()[0] == 0


def test_foreign_key_validation_failure_rolls_back_migration(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)

    def create_invalid_foreign_key(connection):
        connection.execute(
            "CREATE TABLE Invalid_Child ("
            "Id INTEGER PRIMARY KEY, Parent_Id INTEGER, "
            "FOREIGN KEY(Parent_Id) REFERENCES Missing_Parent(Id))"
        )
        connection.execute("INSERT INTO Invalid_Child VALUES (1, 999)")

    migration = Migration(
        1,
        "invalid_foreign_key",
        MigrationKind.ADDITIVE,
        create_invalid_foreign_key,
        "invalid-foreign-key-v1",
    )
    with sqlite3.connect(database) as connection:
        with pytest.raises(MigrationIntegrityError, match="foreign_key_check"):
            apply_pending_migrations(connection, (migration,), allow_rebuild=True)

        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'Invalid_Child'"
        ).fetchone() is None


def test_rebuild_row_count_mismatch_rolls_back_original_table(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)

    def bad_rebuild(connection):
        rebuild_table(
            connection,
            table="Items",
            create_sql=(
                "CREATE TABLE {new_table} (Id INTEGER PRIMARY KEY, Value TEXT)"
            ),
            copy_sql=(
                "INSERT INTO {new_table} (Id, Value) "
                "SELECT Id, Value FROM {source_table} WHERE Id = 1"
            ),
        )

    migration = Migration(
        1,
        "bad_rebuild",
        MigrationKind.REBUILD,
        bad_rebuild,
        "bad-rebuild-v1",
    )
    with sqlite3.connect(database) as connection:
        with pytest.raises(MigrationError, match="Row-count mismatch"):
            apply_pending_migrations(connection, (migration,), allow_rebuild=True)

        assert connection.execute("SELECT * FROM Items ORDER BY Id").fetchall() == [
            (1, "one"),
            (2, "two"),
        ]
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = '__migration_new_Items'"
        ).fetchone() is None


def test_backup_can_be_restored(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    backup, _ = create_backup(database)

    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM Items")
    restore_backup(backup, database)

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM Items").fetchone()[0] == 2


def test_checksum_drift_is_rejected(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    original = _additive_migration()

    with sqlite3.connect(database) as connection:
        apply_pending_migrations(connection, (original,), allow_rebuild=True)
        changed = Migration(
            original.version,
            original.name,
            original.kind,
            original.apply,
            "changed-signature",
        )
        with pytest.raises(MigrationIntegrityError, match="does not match"):
            pending_migrations(connection, (changed,))


def test_non_contiguous_applied_history_is_rejected(tmp_path):
    database = tmp_path / "app.db"
    _create_database(database)
    first = _additive_migration()

    def second_apply(connection):
        connection.execute("CREATE TABLE Second (Id INTEGER PRIMARY KEY)")

    second = Migration(
        2,
        "second",
        MigrationKind.ADDITIVE,
        second_apply,
        "second-v1",
    )
    with sqlite3.connect(database) as connection:
        ensure_migrations_table(connection)
        connection.execute(
            f"""
            INSERT INTO {MIGRATIONS_TABLE}
                (Version, Name, Kind, Checksum, Applied_At)
            VALUES (?, ?, ?, ?, ?)
            """,
            (second.version, second.name, second.kind.value, second.checksum, "now"),
        )
        connection.commit()

        with pytest.raises(MigrationIntegrityError, match="contiguous"):
            pending_migrations(connection, (first, second))


def test_new_database_is_marked_current_after_latest_schema_is_created(tmp_path):
    database = tmp_path / "new.db"
    migrations = (_additive_migration(), _rebuild_migration(2))

    with sqlite3.connect(database) as connection:
        new_database = is_new_database(connection)
        assert new_database is True
        assert prepare_startup_migrations(
            connection, migrations, new_database=new_database
        ) == []

        connection.execute(
            "CREATE TABLE Items ("
            "Id INTEGER PRIMARY KEY, Value TEXT NOT NULL, Note TEXT)"
        )
        mark_new_database_current(connection, migrations)

        assert pending_migrations(connection, migrations) == ()
        assert connection.execute(
            f"SELECT COUNT(*) FROM {MIGRATIONS_TABLE}"
        ).fetchone()[0] == 2


def test_interrupted_new_database_initialization_remains_recoverable(tmp_path):
    database = tmp_path / "new.db"
    migrations = (_additive_migration(), _rebuild_migration(2))

    with sqlite3.connect(database) as connection:
        assert is_new_database(connection) is True
        prepare_startup_migrations(connection, migrations, new_database=True)
        connection.execute(
            "CREATE TABLE Items ("
            "Id INTEGER PRIMARY KEY, Value TEXT NOT NULL, Note TEXT)"
        )
        connection.commit()

    with sqlite3.connect(database) as connection:
        assert is_new_database(connection) is True
        prepare_startup_migrations(connection, migrations, new_database=True)
        mark_new_database_current(connection, migrations)
        assert is_new_database(connection) is False
        assert pending_migrations(connection, migrations) == ()


def test_cli_dry_run_and_backup_apply(tmp_path, capsys):
    database = tmp_path / "app.db"
    _create_database(database)

    assert migration_cli(["--database", str(database), "--dry-run"]) == 0
    dry_run_report = loads(capsys.readouterr().out)
    assert dry_run_report["mode"] == "dry-run"
    assert dry_run_report["applied"] == [
        "1:migration_framework_baseline",
        "2:exchange_aware_schema",
        "3:kraken_public_exchange",
        "4:exchange_specific_backtesting",
        "5:kraken_backtesting_defaults",
    ]

    assert migration_cli(["--database", str(database), "--apply"]) == 2
    assert "requires --backup" in capsys.readouterr().err

    assert migration_cli(
        ["--database", str(database), "--backup", "--apply"]
    ) == 0
    apply_report = loads(capsys.readouterr().out)
    assert apply_report["mode"] == "apply"
    assert Path(apply_report["backup_path"]).is_file()
