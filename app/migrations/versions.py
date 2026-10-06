"""
The platform's migration history. APPEND ONLY.

Rules for this file:
  1. Never edit, reorder, renumber, or delete a migration once it has
     shipped to anyone. Make the change in a NEW migration at the end.
     (Applied migrations are checksummed; editing one makes every
     already-upgraded database refuse to start. tests/test_migrations.py
     pins the checksums so an accidental edit fails CI first.)
  2. Write migrations to be idempotent (IF NOT EXISTS, or an explicit
     existence check). Databases created before the migration system
     existed already contain these tables; the runner adopts them by
     running the history over them, and idempotent statements make that
     a no-op instead of an error.
  3. Provide both a SQLite and a Postgres version of each change. If a
     change doesn't apply to one backend, leave its tuple empty.

To add a migration, append to MIGRATIONS:

    Migration(
        version=6,
        name="add_phone_verified_to_entities",
        sqlite=("ALTER TABLE entities ADD COLUMN phone_verified INTEGER NOT NULL DEFAULT 0",),
        postgres=("ALTER TABLE entities ADD COLUMN IF NOT EXISTS phone_verified BOOLEAN NOT NULL DEFAULT FALSE",),
    )
"""
from __future__ import annotations

import json

from ..models import now_iso
from .runner import Migration, MigrationContext, MigrationError


def _pg(statements: tuple) -> tuple:
    """SQLite's REAL is Postgres's DOUBLE PRECISION; everything else in
    these statements is portable as written."""
    return tuple(s.replace(" REAL ", " DOUBLE PRECISION ") for s in statements)


# ---------------------------------------------------------------------------
# 001 -- the multi-tenant, multi-domain core tables
# ---------------------------------------------------------------------------
_CORE_TABLES = (
    """CREATE TABLE IF NOT EXISTS source_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    attributes TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    entity_id TEXT
)""",
    """CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    attributes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS match_candidates (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_a_id TEXT NOT NULL,
    record_b_id TEXT NOT NULL,
    score REAL NOT NULL,
    match_type TEXT NOT NULL,
    reasons TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    decided_by TEXT
)""",
    """CREATE TABLE IF NOT EXISTS lineage (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    source_record_id TEXT,
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_source_records_entity ON source_records(tenant_id, domain, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_source_records_scope ON source_records(tenant_id, domain)",
    "CREATE INDEX IF NOT EXISTS idx_entities_scope ON entities(tenant_id, domain)",
    "CREATE INDEX IF NOT EXISTS idx_match_candidates_scope_status ON match_candidates(tenant_id, domain, status)",
    "CREATE INDEX IF NOT EXISTS idx_lineage_scope_entity ON lineage(tenant_id, domain, entity_id)",
)

# ---------------------------------------------------------------------------
# 002 -- auth, audit, and usage tables (Phase 3)
# ---------------------------------------------------------------------------
_PLATFORM_TABLES = (
    """CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    role TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    allowed_domains TEXT,
    created_at TEXT NOT NULL,
    revoked_at TEXT
)""",
    """CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS usage_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    event_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)",
    "CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_usage_events_tenant ON usage_events(tenant_id, created_at)",
)

# ---------------------------------------------------------------------------
# 003 -- indexed candidate lookup for matching
# ---------------------------------------------------------------------------
_BLOCKING_KEYS = (
    """CREATE TABLE IF NOT EXISTS blocking_keys (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_id TEXT NOT NULL,
    blocking_key TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_blocking_keys_lookup ON blocking_keys(tenant_id, domain, blocking_key)",
    "CREATE INDEX IF NOT EXISTS idx_blocking_keys_record ON blocking_keys(tenant_id, domain, record_id)",
)

# ---------------------------------------------------------------------------
# 004 -- Postgres only: JSON-holding TEXT columns become native JSONB
# ---------------------------------------------------------------------------
_JSON_COLUMNS = (
    ("source_records", "attributes"),
    ("entities", "attributes"),
    ("match_candidates", "reasons"),
    ("audit_log", "details"),
    ("usage_events", "metadata"),
    ("api_keys", "allowed_domains"),
)


def _jsonb_conversion(table: str, column: str) -> str:
    # Converts only if the column is still TEXT, so it is a no-op on
    # databases that were created as JSONB already. If any existing value
    # is not valid JSON the cast fails and the whole migration rolls back,
    # leaving the database untouched.
    return f"""DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = '{table}' AND column_name = '{column}') = 'text' THEN
        ALTER TABLE {table} ALTER COLUMN {column} TYPE JSONB USING {column}::jsonb;
    END IF;
END $$"""


# ---------------------------------------------------------------------------
# 005 -- backfill blocking keys for records that predate the index
# ---------------------------------------------------------------------------
_BACKFILL_BATCH = 500


def backfill_blocking_keys(ctx: MigrationContext) -> None:
    """Records ingested before blocking_keys existed have no index rows, so
    new records could never match them. This computes the missing keys.
    Idempotent: it only touches records that have none, so on a database
    that was always indexed it does nothing.

    Uses CURRENT application logic (standardize + blocking_keys) on purpose:
    a backfill should produce keys under today's rules. Caveats:
      * Runs in the migration's transaction, so on a very large table it is
        one long transaction.
      * If Postgres row-level security is enabled with FORCE, run migrations
        as a role that bypasses it (or before enabling it): under RLS with
        no tenant set, this SELECT sees zero rows and silently backfills nothing.
    """
    from ..domains import REGISTRY
    from ..matching import blocking_keys
    from ..standardize import standardize

    last_id = ""
    while True:
        rows = ctx.query(
            "SELECT sr.id, sr.tenant_id, sr.domain, sr.attributes FROM source_records sr "
            "WHERE sr.id > ? AND NOT EXISTS ("
            "  SELECT 1 FROM blocking_keys bk "
            "  WHERE bk.tenant_id = sr.tenant_id AND bk.domain = sr.domain AND bk.record_id = sr.id"
            ") ORDER BY sr.id LIMIT " + str(_BACKFILL_BATCH),
            (last_id,))
        if not rows:
            return
        batch = []
        for row in rows:
            config = REGISTRY.get(row["domain"])
            if config is None:
                continue  # a domain this release doesn't define; leave it alone
            attrs = row["attributes"]
            if isinstance(attrs, str):  # SQLite (and pre-JSONB Postgres) store JSON as text
                attrs = json.loads(attrs)
            keys = blocking_keys(standardize(row["domain"], attrs), config)
            batch.extend((row["tenant_id"], row["domain"], row["id"], k) for k in keys)
        ctx.executemany(
            "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)",
            batch)
        last_id = rows[-1]["id"]


# ---------------------------------------------------------------------------
# 006 -- remember which configuration each domain's blocking index was built under
# ---------------------------------------------------------------------------
_INDEX_STATE = (
    """CREATE TABLE IF NOT EXISTS blocking_index_state (
    domain TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL
)""",
)


def stamp_builtin_index_state(ctx: MigrationContext) -> None:
    """Domains that already hold indexed records were indexed under the
    BUILT-IN configuration (customer-defined domains didn't exist yet), so
    record that fingerprint for them. Without this, upgrading would force a
    pointless reindex of every existing deployment.

    It deliberately stamps the built-in fingerprint, not whatever is loaded
    now: if a customer override that changes blocking is already in place at
    upgrade time, the index really IS stale relative to it, and the mismatch
    is exactly how that gets caught. Idempotent: never overwrites a row."""
    from ..domains import BUILTIN_DOMAINS
    from ..indexing import blocking_fingerprint

    for row in ctx.query("SELECT DISTINCT domain FROM blocking_keys"):
        config = BUILTIN_DOMAINS.get(row["domain"])
        if config is None:
            continue
        if ctx.query("SELECT 1 AS present FROM blocking_index_state WHERE domain = ?", (row["domain"],)):
            continue
        ctx.execute(
            "INSERT INTO blocking_index_state (domain, fingerprint, status, updated_at) VALUES (?, ?, ?, ?)",
            (row["domain"], blocking_fingerprint(config), "ready", now_iso()))


MIGRATIONS = [
    Migration(1, "core_tables", sqlite=_CORE_TABLES, postgres=_pg(_CORE_TABLES)),
    Migration(2, "auth_audit_usage_tables", sqlite=_PLATFORM_TABLES, postgres=_pg(_PLATFORM_TABLES)),
    Migration(3, "blocking_keys_index", sqlite=_BLOCKING_KEYS, postgres=_BLOCKING_KEYS),
    Migration(4, "jsonb_columns_postgres", sqlite=(),
              postgres=tuple(_jsonb_conversion(t, c) for t, c in _JSON_COLUMNS)),
    Migration(5, "backfill_blocking_keys", data_step=backfill_blocking_keys),
    Migration(6, "blocking_index_state", sqlite=_INDEX_STATE, postgres=_INDEX_STATE,
              data_step=stamp_builtin_index_state),
]


# ---------------------------------------------------------------------------
# Adopting databases created before the migration system existed
# ---------------------------------------------------------------------------
_CORE_TABLE_NAMES = ("source_records", "entities", "match_candidates", "lineage")


def legacy_preflight(ctx: MigrationContext) -> None:
    """Runs only when a database has NO migration history. A database with
    core tables from the earlier multi-tenant/multi-domain releases is
    adopted as-is (the idempotent migrations above fill in whatever it
    lacks). One from before that -- no tenant_id/domain columns -- cannot
    be upgraded safely by guessing values, so refuse clearly instead of
    failing later with a bare "no such column"."""
    for table in _CORE_TABLE_NAMES:
        if ctx.table_exists(table):
            missing = {"tenant_id", "domain"} - ctx.columns(table)
            if missing:
                raise MigrationError(
                    f"Table '{table}' exists but has no {sorted(missing)} column(s): this database was "
                    f"created by a release that predates multi-tenant/multi-domain support (and the "
                    f"migration system), so it cannot be upgraded automatically. Export anything you "
                    f"need, then recreate it (delete the SQLite file, or DROP and CREATE the Postgres "
                    f"database). A fresh database is built by the migrations and stays upgradable from "
                    f"here on. Nothing was changed.")
