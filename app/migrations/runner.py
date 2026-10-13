"""
Platform schema migrations.

Replaces the old "CREATE TABLE IF NOT EXISTS on every startup" approach,
which could never change an existing table: a database created by an
older release silently kept its old structure, and the only fix was to
delete it. Migrations are a numbered, append-only history of schema
changes. A `schema_migrations` table records which have been applied,
and the runner applies only the missing ones, in order, preserving data.

Guarantees, and how each is enforced:

  * Forward-only. There are no "down" migrations. Rolling back means
    restoring a backup -- a down-migration that drops a column destroys
    data just as surely as the mistake you're undoing, and is rarely
    tested as carefully as the way up.
  * Atomic. Each migration (its SQL, its optional data step, and the
    row recording it) commits as ONE transaction on both backends. A
    migration that fails halfway leaves the database exactly as it was.
  * One at a time. Postgres: a session-level advisory lock around the
    whole run. SQLite: BEGIN IMMEDIATE per migration, re-reading what's
    applied after the lock is taken. Two app instances starting together
    can't both apply the same migration.
  * Tamper-evident history. Each applied migration's checksum is stored.
    Editing a migration after it shipped -- the classic way schemas
    silently diverge between customers -- is detected at startup.
  * Fail-fast on "database newer than code". If the database holds
    migrations this release doesn't know about (a rollback of the app
    without the database), startup stops rather than guessing.
  * No DDL when already current. If nothing is pending the runner makes
    no schema changes at all, so an app role that has no CREATE/ALTER
    privilege can still start (the least-privilege deployment pattern).

`namespace` is part of the tracking key so a second, independently
versioned list of migrations (e.g. customer extensions) can share this
runner later without changing the tracking table. Only the "platform"
namespace is used today.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Optional

from ..models import now_iso
from ..sql_dialect import get_dialect

PLATFORM_NAMESPACE = "platform"

SCHEMA_MIGRATIONS_DDL = """CREATE TABLE IF NOT EXISTS schema_migrations (
    namespace TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    PRIMARY KEY (namespace, version)
)"""


class MigrationError(Exception):
    """Raised for any migration problem. Always carries an actionable message."""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sqlite: tuple = ()
    postgres: tuple = ()
    # Optional Python step for changes plain SQL can't express (e.g. a
    # backfill that needs application logic). Runs AFTER the SQL
    # statements, inside the same transaction, through the same
    # connection -- see MigrationContext.
    data_step: Optional[Callable] = None

    def statements(self, backend_name: str) -> tuple:
        return {"sqlite": self.sqlite, "postgres": self.postgres}[backend_name]

    def checksum(self, backend_name: str) -> str:
        """Covers the version, name, and the exact SQL for this backend,
        plus the NAME of the data step. It does not cover the data step's
        body (that would make checksums depend on source formatting), so
        a data step's logic is protected by review and tests, not here."""
        payload = json.dumps({
            "version": self.version,
            "name": self.name,
            "statements": list(self.statements(backend_name)),
            "data_step": self.data_step.__name__ if self.data_step else None,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class AppliedMigration:
    version: int
    name: str
    checksum: str
    applied_at: str


@dataclass
class MigrationStatus:
    applied: list = field(default_factory=list)            # list[AppliedMigration]
    pending: list = field(default_factory=list)            # list[Migration]
    unknown_applied: list = field(default_factory=list)    # versions in the DB this code doesn't know
    checksum_mismatches: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.pending or self.unknown_applied or self.checksum_mismatches)


# ---------------------------------------------------------------------------
# Context handed to migrations: one connection, one transaction
# ---------------------------------------------------------------------------
class MigrationContext:
    """What a migration's SQL and data step run through. Deliberately NOT
    the application's Store: the runner holds a transaction (and, on
    SQLite, the write lock) on this connection, so a data step that
    opened a second connection through Store would block against it.
    SQL written for this context uses `?` placeholders on both backends."""

    def __init__(self, backend_name: str, conn):
        self.backend = backend_name
        self.dialect = get_dialect(backend_name)      # the SQL TEXT that differs between databases lives there (app/sql_dialect.py)
        self._conn = conn

    def _sql(self, sql: str) -> str:
        return self.dialect.sql(sql)

    def execute(self, sql: str, params: tuple = ()) -> None:
        if self.backend == "postgres":
            with self._conn.cursor() as cur:
                if params:
                    cur.execute(self._sql(sql), params)
                else:
                    cur.execute(sql)  # no params -> psycopg2 does no % processing
        else:
            self._conn.execute(sql, params)

    def executemany(self, sql: str, rows) -> None:
        rows = list(rows)
        if not rows:
            return
        if self.backend == "postgres":
            with self._conn.cursor() as cur:
                cur.executemany(self._sql(sql), rows)
        else:
            self._conn.executemany(sql, rows)

    def query(self, sql: str, params: tuple = ()) -> list:
        if self.backend == "postgres":
            with self._conn.cursor() as cur:
                if params:
                    cur.execute(self._sql(sql), params)
                else:
                    cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def table_exists(self, name: str) -> bool:
        sql, params = self.dialect.table_exists_query(name)
        return bool(self.query(sql, params))

    def column_types(self, table: str) -> dict:
        """{column: declared type} (SQLite: the declared type, upper-cased; Postgres: information_schema's data_type)."""
        sql, params = self.dialect.column_types_query(table)
        return self.dialect.parse_column_types(self.query(sql, params))

    def columns(self, table: str) -> set:
        sql, params = self.dialect.columns_query(table)
        return self.dialect.parse_columns(self.query(sql, params))


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------
class SQLiteMigrationBackend:
    name = "sqlite"

    def __init__(self, path: str):
        self.path = path

    @contextmanager
    def connection(self):
        # isolation_level=None = autocommit: we issue BEGIN/COMMIT ourselves,
        # because Python's implicit transaction handling does not wrap DDL,
        # and we need DDL to be transactional.
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def lock(self, conn):
        yield  # SQLite locks per transaction, via BEGIN IMMEDIATE in begin()

    def begin(self, conn) -> None:
        conn.execute("BEGIN IMMEDIATE")  # takes the write lock up front

    def commit(self, conn) -> None:
        conn.execute("COMMIT")

    def rollback(self, conn) -> None:
        if conn.in_transaction:
            conn.execute("ROLLBACK")

    def finish_read(self, conn) -> None:
        pass


class PostgresMigrationBackend:
    name = "postgres"
    # Arbitrary constant: every process migrating this database must use the same key.
    ADVISORY_LOCK_KEY = 7283940112

    def __init__(self, database_url: str):
        self.database_url = database_url

    @contextmanager
    def connection(self):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError as e:
            raise ImportError(
                "psycopg2 is not installed. Run:\n  pip install psycopg2-binary\n"
                "or unset DATABASE_URL to use the SQLite backend instead."
            ) from e
        # A dedicated connection, separate from PostgresStore's persistent one,
        # so migrations never share its lock or its per-tenant session settings.
        conn = psycopg2.connect(self.database_url, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def lock(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (self.ADVISORY_LOCK_KEY,))
        conn.commit()  # a session-level advisory lock survives COMMIT
        try:
            yield
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (self.ADVISORY_LOCK_KEY,))
                conn.commit()
            except Exception:
                conn.rollback()  # closing the connection releases the lock anyway

    def begin(self, conn) -> None:
        pass  # psycopg2 opens a transaction implicitly on the first statement

    def commit(self, conn) -> None:
        conn.commit()

    def rollback(self, conn) -> None:
        conn.rollback()

    def finish_read(self, conn) -> None:
        conn.rollback()  # end the read-only transaction psycopg2 opened


def backend_for_url(url: str):
    if url.startswith(("postgresql://", "postgres://")):
        return PostgresMigrationBackend(url)
    return SQLiteMigrationBackend(url)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def _platform_defaults():
    from .versions import MIGRATIONS, legacy_preflight
    return MIGRATIONS, legacy_preflight


def validate_migration_list(migrations) -> list:
    ordered = sorted(migrations, key=lambda m: m.version)
    versions = [m.version for m in ordered]
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(
            f"Migration versions must be unique and contiguous starting at 1; got {versions}. "
            f"Never renumber or delete a migration that has shipped -- add a new one."
        )
    return ordered


def _read_applied(ctx: MigrationContext, namespace: str) -> dict:
    if not ctx.table_exists("schema_migrations"):
        return {}
    rows = ctx.query(
        "SELECT version, name, checksum, applied_at FROM schema_migrations "
        "WHERE namespace = ? ORDER BY version", (namespace,))
    return {r["version"]: AppliedMigration(r["version"], r["name"], r["checksum"], r["applied_at"]) for r in rows}


def _build_status(applied: dict, migrations: list, backend_name: str) -> MigrationStatus:
    known = {m.version: m for m in migrations}
    return MigrationStatus(
        applied=[applied[v] for v in sorted(applied)],
        pending=[m for m in migrations if m.version not in applied],
        unknown_applied=sorted(v for v in applied if v not in known),
        checksum_mismatches=sorted(
            v for v, a in applied.items() if v in known and a.checksum != known[v].checksum(backend_name)),
    )


def _inconsistency_message(status: MigrationStatus) -> str:
    parts = []
    if status.unknown_applied:
        parts.append(
            f"This database has migration(s) {status.unknown_applied} applied that this version of the "
            f"software does not know about -- it was probably upgraded by a NEWER release. Upgrade the "
            f"application to match, or restore the database from before that release.")
    if status.checksum_mismatches:
        parts.append(
            f"Migration(s) {status.checksum_mismatches} were modified after being applied (checksum "
            f"mismatch). An applied migration must never be edited; make the change in a NEW migration.")
    return " ".join(parts)


def get_status(backend, migrations=None, namespace: str = PLATFORM_NAMESPACE) -> MigrationStatus:
    """Read-only. Never creates or changes anything."""
    if migrations is None:
        migrations, _ = _platform_defaults()
    migrations = validate_migration_list(migrations)
    with backend.connection() as conn:
        try:
            ctx = MigrationContext(backend.name, conn)
            return _build_status(_read_applied(ctx, namespace), migrations, backend.name)
        finally:
            backend.finish_read(conn)


def run_migrations(backend, migrations=None, namespace: str = PLATFORM_NAMESPACE, preflight=None) -> list:
    """Applies every pending migration, in order. Returns the versions
    applied by THIS call (empty if the database was already current)."""
    if migrations is None:
        migrations, default_preflight = _platform_defaults()
        preflight = preflight or default_preflight
    migrations = validate_migration_list(migrations)
    applied_now: list = []

    with backend.connection() as conn:
        # Fast path, no locks and no DDL: nothing to do, or something is wrong.
        ctx = MigrationContext(backend.name, conn)
        try:
            status = _build_status(_read_applied(ctx, namespace), migrations, backend.name)
        finally:
            backend.finish_read(conn)
        if status.unknown_applied or status.checksum_mismatches:
            raise MigrationError(_inconsistency_message(status))
        if not status.pending:
            return applied_now

        with backend.lock(conn):
            while True:
                backend.begin(conn)
                try:
                    ctx = MigrationContext(backend.name, conn)
                    ctx.execute(SCHEMA_MIGRATIONS_DDL)
                    applied = _read_applied(ctx, namespace)  # re-read: another process may have won the lock first
                    status = _build_status(applied, migrations, backend.name)
                    if status.unknown_applied or status.checksum_mismatches:
                        raise MigrationError(_inconsistency_message(status))
                    if not applied and preflight is not None:
                        preflight(ctx)
                    if not status.pending:
                        backend.commit(conn)
                        break

                    migration = status.pending[0]
                    for statement in migration.statements(backend.name):
                        ctx.execute(statement)
                    if migration.data_step is not None:
                        migration.data_step(ctx)
                    ctx.execute(
                        "INSERT INTO schema_migrations (namespace, version, name, checksum, applied_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (namespace, migration.version, migration.name,
                         migration.checksum(backend.name), now_iso()))
                    backend.commit(conn)
                    applied_now.append(migration.version)
                except Exception:
                    backend.rollback(conn)
                    raise
    return applied_now


def auto_migrate_enabled(explicit: Optional[bool] = None) -> bool:
    """On by default. MDM_AUTO_MIGRATE=false switches to check-only mode,
    for deployments that run `migrate.py upgrade` as a separate step with
    a privileged role and start the app under a restricted one."""
    if explicit is not None:
        return explicit
    return os.environ.get("MDM_AUTO_MIGRATE", "true").strip().lower() not in ("0", "false", "no", "off")


def apply_or_verify(backend, auto_migrate: Optional[bool] = None, migrations=None, namespace: str = PLATFORM_NAMESPACE) -> list:
    if auto_migrate_enabled(auto_migrate):
        return run_migrations(backend, migrations, namespace)
    status = get_status(backend, migrations, namespace)
    if not status.ok:
        detail = _inconsistency_message(status)
        if status.pending:
            versions = [m.version for m in status.pending]
            detail = (f"{len(versions)} migration(s) pending {versions}. " + detail).strip()
        raise MigrationError(
            f"Database schema is not up to date and auto-migration is disabled (MDM_AUTO_MIGRATE). "
            f"{detail} Run:  python3 migrate.py upgrade")
    return []
