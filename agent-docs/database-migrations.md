# Database Migration Operations

Use the versioned migration framework for every new SQLite schema change. Do
not add new changelog-dated SQL execution or rename migration files after use.

## Migration Classes

- `additive`: safe changes such as creating a table/index or adding a nullable
  column. Pending additive migrations may run during application startup.
- `rebuild`: changes that copy, replace, or drop tables/columns. Application
  startup blocks until the operator validates and applies them manually.

New databases are created directly from the current schema definitions and are
then marked at the latest registered migration. An initialization marker makes
this process recoverable if startup is interrupted. A database-adjacent
`*.migration.lock` file serializes dashboard, jobs-runner, and manual apply
operations during schema initialization or migration.

## Production Procedure

Stop every process that can write to the database before validation or apply.
Test the migration against a current production database copy before touching
the VPS database.

Run a dry-run first. It uses SQLite's backup API to create a temporary,
transactionally consistent copy and never modifies the source database:

```bash
python -m bec.db.migrations --database prod.db --dry-run
```

Review the JSON report, including pending/applied migrations, source checksums,
table row counts, schema fingerprints, integrity result, and foreign-key result.

Apply only after the dry-run succeeds:

```bash
python -m bec.db.migrations --database prod.db --backup --apply
```

The apply command refuses pending work without `--backup`. It creates a
timestamped SQLite backup beside the source database and reports its SHA-256
checksum. Keep that backup until the deployment and application checks pass.

## Exchange-Aware Schema Migration (Version 2)

Migration `2:exchange_aware_schema` is a manual `rebuild` migration. It adds
the `Exchanges` and `Exchange_Symbols` metadata tables, backfills legacy rows
as Binance, adds normalized symbol and execution fields, and replaces legacy
symbol-only uniqueness with exchange-aware constraints.

For an upgraded database, Binance remains enabled and default so existing
behavior is preserved. A fresh database contains Binance metadata only; it has
no enabled/default exchange and exchange-dependent schedules are disabled.

The migration report lists symbols that could not be normalized in
`unresolved_legacy_symbols`. Their `Exchange_Symbols` records have
`Is_Tradable=0` and require manual review.

Do not deploy this application build over an unmigrated production database.
Startup intentionally blocks on this rebuild. Validate a current production
copy and review its unresolved symbols and row counts before applying during a
write-free maintenance window.

## Rollback

Migration steps run in explicit SQLite transactions. A failed step rolls back
automatically and does not receive a `Schema_Migrations` record. For an
operator-requested restore, stop all database users and restore the reported
backup:

```bash
python -c "from bec.db.migrations import restore_backup; restore_backup('prod.db.backup-TIMESTAMP.sqlite3', 'prod.db')"
```

Restart the application only after `PRAGMA integrity_check` and
`PRAGMA foreign_key_check` succeed on the restored database.

## Implementation Rules

- Register immutable, ordered migrations in `bec.db.migrations.registry`.
- Never change a migration's name, kind, or signature after release; checksum
  drift blocks startup/apply.
- Rebuild migrations must use the rollback-safe `rebuild_table` helper or
  provide equivalent row-count, schema, index, and foreign-key validation.
- Keep schema definitions for new databases aligned with every migration.
- Add tests for success, failure rollback, idempotency, dry-run immutability,
  backup restoration, and new-database initialization.
