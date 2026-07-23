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
behavior is preserved. At the version 2 boundary, a fresh database has no
enabled/default exchange; the latest-schema initialization described under
version 5 subsequently selects Kraken for new installations.

The migration report lists symbols that could not be normalized in
`unresolved_legacy_symbols`. Their `Exchange_Symbols` records have
`Is_Tradable=0` and require manual review.

Do not deploy this application build over an unmigrated production database.
Startup intentionally blocks on this rebuild. Validate a current production
copy and review its unresolved symbols and row counts before applying during a
write-free maintenance window.

## Kraken Public Metadata (Version 3)

Migration `3:kraken_public_exchange` is additive and may run automatically at
startup after version 2 is present. It registers Kraken as disabled and
non-default with `EUR` metadata. It does not add credentials, enable jobs,
change the active Binance selection, or permit private Kraken operations.

## Exchange-Specific Backtesting (Version 4)

Migration `4:exchange_specific_backtesting` is additive and may run at startup.
It creates `Exchange_Backtesting_Settings`, preserves the existing Binance
commission, and adds immutable commission/fingerprint context to backtesting
results and queued jobs. Existing queued or running jobs without a PR 6
fingerprint are failed with an instruction to requeue them; they are not
executed with ambiguous exchange assumptions.

Version 4 itself assigns no implicit Kraken commission. Version 5 supplies the
approved Kraken default while leaving the captured version 4 job context
unchanged.

## Kraken Backtesting Defaults (Version 5)

Migration `5:kraken_backtesting_defaults` is additive and may run at startup.
It enables Kraken public workflows, changes its configured quote asset to
`USDC`, and inserts a 0.4% spot taker fee only when no Kraken fee exists.

On upgrades, the existing active/default exchange is preserved. On new
installations initialized directly at the latest schema, Kraken is enabled and
selected by default. Private Kraken balances and order methods remain disabled
through PR 6, and only the public market-phase schedule may run for Kraken.

## Gated Kraken Live Execution (Version 6)

Migration `6:gated_kraken_live_execution` is additive. It adds disabled-by-
default buy/sell flags and partial-sell/sizing settings to `Exchanges`, plus
durable client-order intent, submission, reconciliation and idempotent fill
metadata to `Orders`. It also adds unique client-order and reconciliation
indexes.

The migration never enables Kraken buy or sell operations. For an upgraded
installation with active Binance, its buy/sell flags are initialized enabled
to preserve existing behavior. New installations keep Binance live flags off.
No credentials are stored in SQLite.

## Backtesting Exchange Context Repair (Version 11)

Migration `11:repair_backtesting_exchange_context` is additive and idempotently
repairs immutable exchange-context columns on backtesting jobs, Monte Carlo
jobs, and backtesting results. It covers databases where migration 9 was
recorded while one of those optional queue tables did not yet exist or had an
older development schema. Existing rows are backfilled from their
`Exchange_Id`; no jobs are requeued and no trading settings are changed.

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
