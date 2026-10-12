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


def add_contributions_column(ctx: MigrationContext) -> None:
    """SQLite has no ADD COLUMN IF NOT EXISTS, so check first: that keeps this migration idempotent like
    the rest (a database whose migration history was lost must be adoptable, not fail on a duplicate column).
    Postgres does it in SQL, above."""
    if ctx.backend == "sqlite" and "contributions" not in ctx.columns("match_candidates"):
        ctx.execute("ALTER TABLE match_candidates ADD COLUMN contributions TEXT")


_STAGING_SQLITE = (
    """CREATE TABLE IF NOT EXISTS standardized_records (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_id TEXT NOT NULL,
    matching TEXT NOT NULL,
    cleansed TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    standardized_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, domain, record_id)
)""",
    "CREATE INDEX IF NOT EXISTS idx_standardized_domain ON standardized_records(domain)",
)
_STAGING_POSTGRES = (_STAGING_SQLITE[0].replace("matching TEXT", "matching JSONB").replace("cleansed TEXT", "cleansed JSONB"),
                     _STAGING_SQLITE[1])


def mark_unstaged_domains_stale(ctx: MigrationContext) -> None:
    """A domain that already has records but nothing staged was indexed before the staging layer existed, so its
    derived data (staging, blocking keys, golden records) must be rebuilt. Migration 006 may just have stamped it
    READY, so overwrite the stamp with one that can never match: the stale-index guard then tells the operator to
    run `domains_cli.py reindex <domain>`, instead of ingestion quietly running on an empty staging table."""
    ctx.execute(
        "UPDATE blocking_index_state SET fingerprint = ?, updated_at = ? "
        "WHERE domain IN (SELECT DISTINCT domain FROM source_records) "
        "AND domain NOT IN (SELECT DISTINCT domain FROM standardized_records)",
        ("needs-staging-layer", now_iso()))


def split_entities_into_domain_tables(ctx: MigrationContext) -> None:
    """Golden records move from ONE table with a JSON `attributes` blob to one table per domain with a real column
    per field (app/golden.py). Entity ids, tenant, status (including 'merged' tombstones) and timestamps are preserved,
    so source_records.entity_id and lineage keep pointing at the same entities, and the values are copied across.

    Columns come from the domain's config when it is loaded; otherwise (migrate.py loads no config) from the keys
    actually present in that domain's rows, so a custom domain's data is not lost. Rows are counted afterwards and the
    migration fails, rolling everything back, if any are missing. The old table is RENAMED, not dropped, as a safety net
    (`entities_json_legacy`; drop it yourself once you have checked). Golden records are derived data anyway: a
    `domains_cli.py reindex <domain>` re-derives them from raw. Idempotent."""
    import json
    from .. import golden
    from ..domains import BUILTIN_DOMAINS, REGISTRY

    legacy = ctx.table_exists("entities")
    domains = set(BUILTIN_DOMAINS)
    if legacy:
        domains |= {r["domain"] for r in ctx.query("SELECT DISTINCT domain FROM entities")}
    moved = 0
    for domain in sorted(domains):
        if not golden.VALID_DOMAIN.match(domain):
            raise MigrationError(f"Domain '{domain}' cannot be a table name; rename it before upgrading")
        cfg = REGISTRY.get(domain) or BUILTIN_DOMAINS.get(domain)
        fields = list(cfg.schema_fields) if cfg else []
        rows = ctx.query("SELECT id, tenant_id, status, attributes, created_at, updated_at FROM entities WHERE domain = ?",
                         (domain,)) if legacy else []
        parsed = []
        for r in rows:
            attrs = r["attributes"]
            attrs = json.loads(attrs) if isinstance(attrs, str) else (attrs or {})
            parsed.append((r, attrs))
            for key in attrs:  # keys the config doesn't (or can't) list, e.g. a domain whose config isn't loaded
                if key not in fields and golden.VALID_FIELD.match(key) and key not in golden.RESERVED_FIELD_NAMES:
                    fields.append(key)
        golden.ensure_table(ctx, domain, fields)
        if not parsed:
            continue
        t = golden.q(golden.table_name(domain))
        cols = ["entity_id", "tenant_id", "status", "created_at", "updated_at", *fields]
        names, marks = ", ".join(golden.q(c) for c in cols), ", ".join("?" for _ in cols)
        verb, tail = ("INSERT OR IGNORE", "") if ctx.backend == "sqlite" else ("INSERT", " ON CONFLICT (entity_id) DO NOTHING")
        ctx.executemany(f"{verb} INTO {t} ({names}) VALUES ({marks}){tail}",
                        [(r["id"], r["tenant_id"], r["status"], r["created_at"], r["updated_at"],
                          *[golden.cell(a.get(f)) for f in fields]) for r, a in parsed])
        if ctx.query(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"] < len(parsed):
            raise MigrationError(f"Moving golden records of domain '{domain}' lost rows; nothing was changed")
        moved += len(parsed)
    if legacy:
        if moved:
            target, n = "entities_json_legacy", 1
            while ctx.table_exists(target):
                n += 1
                target = f"entities_json_legacy_{n}"
            ctx.execute(f"ALTER TABLE entities RENAME TO {target}")
        else:
            ctx.execute("DROP TABLE entities")


_RAW_IDEMPOTENCY_COMMON = (
    "CREATE INDEX IF NOT EXISTS idx_source_records_natural ON source_records(tenant_id, domain, source_system, source_record_id)",
)
_HISTORY_SQLITE = """CREATE TABLE IF NOT EXISTS source_record_history (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    attributes TEXT NOT NULL,
    superseded_at TEXT NOT NULL,
    PRIMARY KEY (record_id, version)
)"""
_RAW_IDEMPOTENCY_SQLITE = _RAW_IDEMPOTENCY_COMMON + (_HISTORY_SQLITE,)
_RAW_IDEMPOTENCY_POSTGRES = _RAW_IDEMPOTENCY_COMMON + (
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS content_hash TEXT",
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS updated_at TEXT",
    _HISTORY_SQLITE.replace("attributes TEXT", "attributes JSONB"),
)


def add_raw_idempotency_columns(ctx: MigrationContext) -> None:
    """SQLite has no ADD COLUMN IF NOT EXISTS, so check first (Postgres does it in SQL, above). Existing rows keep
    content_hash NULL: the pipeline computes the hash from their payload on demand, so no backfill pass is needed.

    NOTE the natural-key index is deliberately NOT unique. Databases from before this migration may already hold the
    same source record more than once (re-sending a file used to create duplicates); a unique index would refuse to
    build, and picking which copy to delete is not a decision a migration should make. Uniqueness is enforced in the
    pipeline; `domains_cli.py status` reports any old duplicates."""
    if ctx.backend != "sqlite":
        return
    have = ctx.columns("source_records")
    for column, ddl in (("content_hash", "TEXT"), ("version", "INTEGER NOT NULL DEFAULT 1"), ("updated_at", "TEXT")):
        if column not in have:
            ctx.execute(f"ALTER TABLE source_records ADD COLUMN {column} {ddl}")


def type_golden_columns(ctx: MigrationContext) -> None:
    """Golden-record columns get the types their fields declare, and every entity table gets `quality_issues`. The only
    built-in field that is not text is product.price. A column whose type changes is rebuilt by copy-and-swap, a value
    that cannot be parsed becomes NULL, and `reindex` re-derives it from staging, where it can be recovered. Domains whose
    config is not loaded are left as they are. Idempotent."""
    from .. import golden
    from ..domains import BUILTIN_DOMAINS, REGISTRY

    for domain in sorted(set(BUILTIN_DOMAINS) | set(REGISTRY)):
        if ctx.table_exists(golden.table_name(domain)):
            golden.ensure_table(ctx, domain, golden.field_columns(REGISTRY.get(domain) or BUILTIN_DOMAINS[domain]))


_PIPELINE_COMMON = (
    "CREATE INDEX IF NOT EXISTS idx_match_candidates_a ON match_candidates(tenant_id, domain, record_a_id)",
    "CREATE INDEX IF NOT EXISTS idx_match_candidates_b ON match_candidates(tenant_id, domain, record_b_id)",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_domain ON pipeline_runs(domain, started_at)",
)
_RUNS_SQLITE = """CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    mode TEXT NOT NULL,
    triggered_by TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    counts TEXT NOT NULL,
    errors TEXT NOT NULL
)"""
_DIRTY = """CREATE TABLE IF NOT EXISTS golden_dirty (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, domain, entity_id)
)"""
_PIPELINE_SQLITE = (_RUNS_SQLITE, _DIRTY) + _PIPELINE_COMMON
_PIPELINE_POSTGRES = (
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS processing_status TEXT NOT NULL DEFAULT 'matched'",
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS processing_attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE source_records ADD COLUMN IF NOT EXISTS processing_error TEXT",
    _RUNS_SQLITE.replace("counts TEXT", "counts JSONB").replace("errors TEXT", "errors JSONB"), _DIRTY,
) + _PIPELINE_COMMON


# Which auto-merged pairs a person was asked to check, and what they said. The training loop must learn only from HUMAN decisions (the system's own
# auto-merges are not labels), and an audit of a random sample is the only way to measure how precise automatic merging really is.
_MERGE_AUDITS = (
    "CREATE TABLE IF NOT EXISTS merge_audits (candidate_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, domain TEXT NOT NULL, sampled_at TEXT NOT NULL, "
    "verdict TEXT, audited_by TEXT, audited_at TEXT)",
    "CREATE INDEX IF NOT EXISTS idx_merge_audits_scope ON merge_audits(tenant_id, domain, verdict)",
)


# Who MADE the merge ('rule': the rule score; 'promoted': the learned scorer), fixed when the merge is flagged. The candidate's own decided_by cannot be used for this
# later: unmerging a pair rewrites it, and a promotion a person unmerged would then be counted against the RULE score instead of the model.
def add_audit_made_by(ctx: MigrationContext) -> None:
    """Idempotent (an adopted database may already have the column): add merge_audits.made_by, fill it for the audits that exist from what their candidates say now
    (correct for every merge nobody has unmerged yet), and replace the old index with one that serves the audit page's queue and counts."""
    if "made_by" not in ctx.columns("merge_audits"):
        ctx.execute("ALTER TABLE merge_audits ADD COLUMN made_by TEXT")
    ctx.execute("UPDATE merge_audits SET made_by = CASE WHEN (SELECT mc.decided_by FROM match_candidates mc WHERE mc.id = merge_audits.candidate_id) = 'system:learned_scorer' "
                "THEN 'promoted' ELSE 'rule' END WHERE made_by IS NULL")
    ctx.execute("DROP INDEX IF EXISTS idx_merge_audits_scope")
    ctx.execute("CREATE INDEX IF NOT EXISTS idx_merge_audits_queue ON merge_audits(tenant_id, domain, verdict, made_by, sampled_at)")


def add_paging_indexes(ctx: MigrationContext) -> None:
    """The two lists a person pages through, each served by an index that makes a page cost the same however far down it is. Idempotent.
    * the review queue: ONLY the pending pairs, in the order shown (best score first; ties by id, both descending so that 'the rows after this one' is ONE row-value
      comparison, (score, id) < (?, ?), which the index answers with a single seek: an OR of two conditions cannot, and costs more the deeper the page). A PARTIAL index: a pair that is decided leaves it, so it costs the bulk load's
      millions of automatic merges nothing, and stays as small as the queue itself.
    * every golden-record table (entity_<domain>): (tenant, entity id), so 'the next 100 entities of this tenant after X' is a range read, not a sort of the tenant."""
    ctx.execute("CREATE INDEX IF NOT EXISTS idx_match_candidates_pending ON match_candidates(tenant_id, domain, score DESC, id DESC) WHERE status = 'pending'")
    if ctx.backend == "sqlite":
        tables = ctx.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ? ESCAPE '\\'", ("entity\\_%",))
    else:
        tables = ctx.query("SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE ?", ("entity\\_%",))
    for row in tables:
        name = row["name"]
        if {"tenant_id", "entity_id"} <= set(ctx.columns(name)):
            ctx.execute(f'CREATE INDEX IF NOT EXISTS "idx_{name}_tenant_entity" ON "{name}"(tenant_id, entity_id)')


def add_processing_state(ctx: MigrationContext) -> None:
    """Each source record now carries how far it got (landed -> staged -> matched). Existing rows count as finished
    ('matched'), EXCEPT the ones the previous release could strand: a record with no golden record and no match candidate
    can only be one whose processing stopped partway (a record waiting for review has a candidate; a standalone record has an
    entity). Those are marked unfinished so `process-pending` completes them. Idempotent."""
    if ctx.backend == "sqlite":
        have = ctx.columns("source_records")
        for column, ddl in (("processing_status", "TEXT NOT NULL DEFAULT 'matched'"), ("processing_attempts", "INTEGER NOT NULL DEFAULT 0"),
                            ("processing_error", "TEXT")):
            if column not in have:
                ctx.execute(f"ALTER TABLE source_records ADD COLUMN {column} {ddl}")
    # a small index over ONLY the unfinished rows: finding work stays cheap however large the table is
    ctx.execute("CREATE INDEX IF NOT EXISTS idx_source_records_unfinished ON source_records(domain, ingested_at) WHERE processing_status <> 'matched'")
    ctx.execute("UPDATE source_records SET processing_status = 'staged' WHERE entity_id IS NULL AND processing_status = 'matched' "
                "AND NOT EXISTS (SELECT 1 FROM match_candidates mc WHERE mc.tenant_id = source_records.tenant_id "
                "AND mc.domain = source_records.domain AND (mc.record_a_id = source_records.id OR mc.record_b_id = source_records.id))")


def every_record_has_an_entity(ctx: MigrationContext) -> None:
    """Until now a record whose only matches were waiting for review had NO entity (so no golden record), and rejecting the
    review never gave it one. Now every record has exactly one. Existing entity-less records are marked unfinished, so
    `process-pending` gives each its own entity through the normal code path (its pending review items stay pending). Idempotent."""
    ctx.execute("UPDATE source_records SET processing_status = 'staged' WHERE entity_id IS NULL AND processing_status = 'matched'")


MIGRATIONS = [
    Migration(1, "core_tables", sqlite=_CORE_TABLES, postgres=_pg(_CORE_TABLES)),
    Migration(2, "auth_audit_usage_tables", sqlite=_PLATFORM_TABLES, postgres=_pg(_PLATFORM_TABLES)),
    Migration(3, "blocking_keys_index", sqlite=_BLOCKING_KEYS, postgres=_BLOCKING_KEYS),
    Migration(4, "jsonb_columns_postgres", sqlite=(),
              postgres=tuple(_jsonb_conversion(t, c) for t, c in _JSON_COLUMNS)),
    Migration(5, "backfill_blocking_keys", data_step=backfill_blocking_keys),
    Migration(6, "blocking_index_state", sqlite=_INDEX_STATE, postgres=_INDEX_STATE,
              data_step=stamp_builtin_index_state),
    # The per-field breakdown of a match score, kept so a steward sees WHY, not just a number.
    Migration(7, "match_candidate_contributions", sqlite=(),
              postgres=("ALTER TABLE match_candidates ADD COLUMN IF NOT EXISTS contributions JSONB",),
              data_step=add_contributions_column),
    # The staging layer: standardized (matching + cleansed) forms, derived from raw and rebuildable.
    Migration(8, "standardized_records_staging", sqlite=_STAGING_SQLITE, postgres=_STAGING_POSTGRES,
              data_step=mark_unstaged_domains_stale),
    # Golden records: from one table with a JSON blob to one table per domain with a real column per field.
    Migration(9, "entity_tables_with_real_columns", data_step=split_entities_into_domain_tables),
    # Idempotent raw ingest: the same source record again is a no-op or a new version, never a duplicate row.
    Migration(10, "idempotent_raw_ingest", sqlite=_RAW_IDEMPOTENCY_SQLITE, postgres=_RAW_IDEMPOTENCY_POSTGRES,
              data_step=add_raw_idempotency_columns),
    # Typed golden columns + quality_issues.
    Migration(11, "typed_golden_columns", data_step=type_golden_columns),
    # Processing state, the run log, and the durable "golden needs rebuilding" marker.
    Migration(12, "processing_state_and_run_log", sqlite=_PIPELINE_SQLITE, postgres=_PIPELINE_POSTGRES, data_step=add_processing_state),
    # Every record belongs to exactly one entity (review-pending records used to have none).
    Migration(13, "every_record_has_an_entity", data_step=every_record_has_an_entity),
    # Merge audits: a random sample of automatic merges, checked by a person. Human labels for training and for certifying a threshold.
    Migration(14, "merge_audits", sqlite=_MERGE_AUDITS, postgres=_MERGE_AUDITS),
    Migration(15, "merge_audit_made_by", data_step=add_audit_made_by),
    Migration(16, "paging_indexes", data_step=add_paging_indexes),
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
