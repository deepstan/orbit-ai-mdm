"""
Inspect and apply schema migrations.

By default the app migrates itself on startup, so you rarely need this. Use it when
you want migration as an explicit deployment step:

    python3 migrate.py status                 # what's applied, what's pending
    python3 migrate.py upgrade --dry-run      # show the SQL that WOULD run
    python3 migrate.py upgrade                # apply pending migrations

Targets DATABASE_URL (or --db): a SQLite file path, or a postgresql:// URL.

`status` exits 0 when the database is up to date and 1 otherwise, so it can gate a deploy.

Recommended production pattern (least privilege):
    1. Run `migrate.py upgrade` as a role that may CREATE/ALTER (the schema owner).
    2. Start the app with MDM_AUTO_MIGRATE=false under a restricted role. It then only
       VERIFIES the schema is current and refuses to start if it isn't -- it never
       changes the schema itself.

Before upgrading a Postgres database that predates JSONB support, take a backup
(pg_dump): migration 004 rewrites six tables. It is atomic -- if it fails it rolls
back and changes nothing -- but a backup is still the right habit.
"""
import argparse
import os
import sys

from app.migrations import MigrationError, backend_for_url, get_status, run_migrations


def _target(db):
    return db or os.environ.get("DATABASE_URL", "mdm.db")


def cmd_status(args) -> int:
    url = _target(args.db)
    backend = backend_for_url(url)
    status = get_status(backend)
    applied = {a.version: a for a in status.applied}
    from app.migrations.versions import MIGRATIONS

    shown = url if backend.name == "sqlite" else url.split("@")[-1]  # don't print credentials
    print(f"Database: {shown} ({backend.name})\n")
    for m in MIGRATIONS:
        if m.version in applied:
            flag = "  <-- MODIFIED after being applied" if m.version in status.checksum_mismatches else ""
            print(f"  {m.version:03d}  {m.name:30s} applied {applied[m.version].applied_at}{flag}")
        else:
            print(f"  {m.version:03d}  {m.name:30s} PENDING")
    for v in status.unknown_applied:
        print(f"  {v:03d}  (unknown to this release)      applied by a newer version?")
    print()
    if status.ok:
        print("Up to date.")
        return 0
    if status.pending:
        print(f"{len(status.pending)} pending. Run: python3 migrate.py upgrade")
    if status.unknown_applied or status.checksum_mismatches:
        print("INCONSISTENT: run `upgrade` for the full explanation.")
    return 1


def cmd_upgrade(args) -> int:
    url = _target(args.db)
    backend = backend_for_url(url)
    if args.dry_run:
        status = get_status(backend)
        if not status.pending:
            print("Nothing to do: already up to date.")
            return 0
        for m in status.pending:
            print(f"-- {m.version:03d} {m.name}")
            for stmt in m.statements(backend.name):
                print(stmt.strip() + ";\n")
            if m.data_step:
                print(f"-- (then runs data step: {m.data_step.__name__})\n")
        print(f"{len(status.pending)} migration(s) would be applied. Nothing was changed.")
        return 0
    applied = run_migrations(backend)
    print(f"Applied {len(applied)} migration(s): {applied}" if applied else "Already up to date. Nothing applied.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=None, help="SQLite path or postgresql:// URL (default: DATABASE_URL env var, else mdm.db)")
    # Accept --db after the subcommand too (`migrate.py status --db X`). SUPPRESS keeps the
    # subparser's absent --db from overwriting a value given before the subcommand.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Show applied and pending migrations", parents=[common])
    up = sub.add_parser("upgrade", help="Apply pending migrations", parents=[common])
    up.add_argument("--dry-run", action="store_true", help="Print the SQL without running it")
    args = parser.parse_args()
    try:
        return {"status": cmd_status, "upgrade": cmd_upgrade}[args.command](args)
    except MigrationError as e:
        print(f"\nMigration error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
