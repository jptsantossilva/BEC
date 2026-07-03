"""Command-line entry point for BEC SQLite migrations."""

from __future__ import annotations

import argparse
import sys

from bec.db.migrations import (
    MIGRATIONS,
    MigrationError,
    apply_database_migrations,
    run_dry_run,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m bec.db.migrations",
        description="Validate and apply BEC migrations.",
    )
    parser.add_argument("--database", required=True, help="Path to the SQLite database")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Migrate a temporary copy")
    mode.add_argument("--apply", action="store_true", help="Apply pending migrations")
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create a timestamped SQLite backup before applying migrations",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.dry_run:
            if args.backup:
                raise MigrationError("--backup is valid only with --apply")
            report = run_dry_run(args.database, MIGRATIONS)
        else:
            report = apply_database_migrations(
                args.database, MIGRATIONS, backup=bool(args.backup)
            )
    except MigrationError as exc:
        print(f"Migration error: {exc}", file=sys.stderr)
        return 2
    print(report.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
