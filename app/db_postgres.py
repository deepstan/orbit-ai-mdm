"""
PostgreSQL backend -- same tenant_id + domain scoping pattern as db.py's
SQLite `Store`, implementing the identical method signatures, so
`pipeline.py` and `api.py` don't care which one they're talking to.

⚠️ UNTESTED IN THIS SANDBOX. There is no network access here to install
`psycopg2` or reach a Postgres server, so unlike every other module in
this codebase, this one has NOT been run. It's written carefully against
the documented psycopg2 API and kept structurally identical to the
proven, fully-tested SQLite `Store` in db.py -- but treat it as a
first draft to validate on your own machine, not as verified code.

Setup (see README for the full walkthrough):
    pip install psycopg2-binary
    export DATABASE_URL=postgresql://user:pass@localhost:5432/mdm_dev
    python3 -c "from app.store_factory import get_store; get_store()"  # creates schema

Differences from the SQLite version, all handled below:
  - Placeholders: %s instead of ?
  - Upsert: INSERT ... ON CONFLICT ... DO UPDATE instead of INSERT OR REPLACE
  - Row access: psycopg2's RealDictCursor instead of sqlite3.Row
  - Connection lifecycle: explicit commit/rollback, psycopg2 doesn't
    autocommit by default (good -- matches the SQLite context manager's
    "commit on success, nothing persisted on exception" behavior)

Deliberately NOT using JSONB for the `attributes` column, even though
that's the more idiomatic Postgres choice (native JSON querying, indexing
into fields) -- kept as TEXT + json.dumps/loads to stay byte-for-byte
consistent with the SQLite schema and minimize what's different between
the two backends while this is unvalidated. Switching to JSONB later is
a reasonable follow-up once this backend is confirmed working, and would
unlock querying entities by attribute value directly in SQL.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager

from .models import ApiKeyRecord, AuditLogEntry, Entity, LineageEvent, MatchCandidate, SourceRecord, UsageEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    attributes TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    entity_id TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    attributes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_candidates (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_a_id TEXT NOT NULL,
    record_b_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    match_type TEXT NOT NULL,
    reasons TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    decided_by TEXT
);

CREATE TABLE IF NOT EXISTS lineage (
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
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    role TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    allowed_domains TEXT,
    created_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    event_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_records_entity ON source_records(tenant_id, domain, entity_id);
CREATE INDEX IF NOT EXISTS idx_source_records_scope ON source_records(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_entities_scope ON entities(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_match_candidates_scope_status ON match_candidates(tenant_id, domain, status);
CREATE INDEX IF NOT EXISTS idx_lineage_scope_entity ON lineage(tenant_id, domain, entity_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_events_tenant ON usage_events(tenant_id, created_at);
"""


class PostgresStore:
    """
    Holds ONE persistent connection, reused across every call, rather
    than opening a fresh connection per query.

    This matters a lot more for Postgres than it would for SQLite:
    SQLite's "connection" is just opening a local file, which is nearly
    free, so db.py's per-call pattern is fine there. A Postgres
    connection is a real TCP handshake + authentication round-trip over
    the network -- and a single `ingest_record()` call triggers many
    individual store method calls (list records, save record, save
    entity, save candidate, add lineage, ...). Opening a new connection
    for every one of those compounds fast: this was measured causing a
    ~150x slowdown (200 records: ~2s expected vs. ~380s actual) on a
    real Windows + Postgres setup before this fix.

    Thread safety: FastAPI's sync route handlers run in a thread pool,
    so multiple threads can call into this Store concurrently -- and a
    single psycopg2 connection isn't safe for concurrent use by
    multiple threads at once. `self._lock` serializes access. It's an
    RLock (reentrant), not a plain Lock, because some methods call
    other methods that also acquire it from the SAME thread while
    already holding it (e.g. get_entity() calls list_source_records()
    from inside its own `with self._conn()` block) -- a plain Lock
    would deadlock on that nested acquisition.

    This does mean all DB access from one process is effectively
    serialized (no real concurrency within a process) -- fine for an
    MVP's request volume, and still correct under concurrency (just not
    maximally fast). A connection pool (e.g. psycopg2.pool or
    SQLAlchemy's engine) would let concurrent requests use separate
    connections instead of queuing on one -- a reasonable next step if
    this process needs to handle many simultaneous requests, not
    needed to fix the bug reported here.
    """

    def __init__(self, database_url: str):
        try:
            import psycopg2  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "psycopg2 is not installed. Run:\n"
                "  pip install psycopg2-binary\n"
                "or unset DATABASE_URL to use the SQLite backend instead."
            ) from e
        self.database_url = database_url
        self._lock = threading.RLock()
        self._connection = None
        self._init_schema()

    def _get_connection(self):
        import psycopg2
        import psycopg2.extras

        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                self.database_url, cursor_factory=psycopg2.extras.RealDictCursor
            )
        return self._connection

    @contextmanager
    def _conn(self):
        with self._lock:
            conn = self._get_connection()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _init_schema(self):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA)

    # --- source records -----------------------------------------------
    def save_source_record(self, rec: SourceRecord) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, "
                "attributes, ingested_at, entity_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (rec.id, rec.tenant_id, rec.domain, rec.source_system, rec.source_record_id,
                 json.dumps(rec.attributes), rec.ingested_at, rec.entity_id),
            )

    def get_source_record(self, rec_id: str, tenant_id: str, domain: str) -> SourceRecord | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM source_records WHERE id = %s AND tenant_id = %s AND domain = %s",
                (rec_id, tenant_id, domain),
            )
            row = cur.fetchone()
            return self._row_to_source_record(row) if row else None

    def list_source_records(self, tenant_id: str, domain: str, entity_id: str | None = None) -> list[SourceRecord]:
        with self._conn() as conn, conn.cursor() as cur:
            if entity_id:
                cur.execute(
                    "SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s AND entity_id = %s",
                    (tenant_id, domain, entity_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s", (tenant_id, domain)
                )
            return [self._row_to_source_record(r) for r in cur.fetchall()]

    def set_record_entity(self, record_id: str, tenant_id: str, domain: str, entity_id: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE source_records SET entity_id = %s WHERE id = %s AND tenant_id = %s AND domain = %s",
                (entity_id, record_id, tenant_id, domain),
            )

    def delete_source_record(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM source_records WHERE id = %s AND tenant_id = %s AND domain = %s",
                (record_id, tenant_id, domain),
            )

    @staticmethod
    def _row_to_source_record(row) -> SourceRecord:
        return SourceRecord(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"],
            source_system=row["source_system"], source_record_id=row["source_record_id"],
            attributes=json.loads(row["attributes"]),
            ingested_at=row["ingested_at"], entity_id=row["entity_id"],
        )

    # --- entities --------------------------------------------------------
    def save_entity(self, ent: Entity) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO entities (id, tenant_id, domain, status, attributes, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                     status = EXCLUDED.status, attributes = EXCLUDED.attributes,
                     updated_at = EXCLUDED.updated_at""",
                (ent.id, ent.tenant_id, ent.domain, ent.status, json.dumps(ent.attributes),
                 ent.created_at, ent.updated_at),
            )

    def get_entity(self, entity_id: str, tenant_id: str, domain: str) -> Entity | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM entities WHERE id = %s AND tenant_id = %s AND domain = %s",
                (entity_id, tenant_id, domain),
            )
            row = cur.fetchone()
            if not row:
                return None
            source_ids = [r.id for r in self.list_source_records(tenant_id, domain, entity_id=entity_id)]
            return Entity(
                id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"], status=row["status"],
                attributes=json.loads(row["attributes"]), source_record_ids=source_ids,
                created_at=row["created_at"], updated_at=row["updated_at"],
            )

    def list_entities(self, tenant_id: str, domain: str, status: str | None = None) -> list[Entity]:
        with self._conn() as conn, conn.cursor() as cur:
            if status:
                cur.execute(
                    "SELECT id FROM entities WHERE tenant_id = %s AND domain = %s AND status = %s",
                    (tenant_id, domain, status),
                )
            else:
                cur.execute("SELECT id FROM entities WHERE tenant_id = %s AND domain = %s", (tenant_id, domain))
            ids = [r["id"] for r in cur.fetchall()]
        return [self.get_entity(i, tenant_id, domain) for i in ids]

    def delete_entity(self, entity_id: str, tenant_id: str, domain: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM entities WHERE id = %s AND tenant_id = %s AND domain = %s",
                (entity_id, tenant_id, domain),
            )

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, "
                "match_type, reasons, status, created_at, decided_at, decided_by) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 json.dumps(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by),
            )

    def update_candidate_status(self, cand_id: str, tenant_id: str, domain: str,
                                 status: str, decided_by: str, decided_at: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE match_candidates SET status = %s, decided_by = %s, decided_at = %s "
                "WHERE id = %s AND tenant_id = %s AND domain = %s",
                (status, decided_by, decided_at, cand_id, tenant_id, domain),
            )

    def list_candidates(self, tenant_id: str, domain: str, status: str | None = "pending") -> list[MatchCandidate]:
        with self._conn() as conn, conn.cursor() as cur:
            if status:
                cur.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = %s "
                    "ORDER BY score DESC",
                    (tenant_id, domain, status),
                )
            else:
                cur.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = %s AND domain = %s ORDER BY score DESC",
                    (tenant_id, domain),
                )
            rows = cur.fetchall()
        return [
            MatchCandidate(
                id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"],
                record_a_id=r["record_a_id"], record_b_id=r["record_b_id"],
                score=r["score"], match_type=r["match_type"], reasons=json.loads(r["reasons"]),
                status=r["status"], created_at=r["created_at"],
                decided_at=r["decided_at"], decided_by=r["decided_by"],
            )
            for r in rows
        ]

    def get_candidate(self, cand_id: str, tenant_id: str, domain: str) -> MatchCandidate | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM match_candidates WHERE id = %s AND tenant_id = %s AND domain = %s",
                (cand_id, tenant_id, domain),
            )
            row = cur.fetchone()
        if not row:
            return None
        return MatchCandidate(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"],
            record_a_id=row["record_a_id"], record_b_id=row["record_b_id"],
            score=row["score"], match_type=row["match_type"], reasons=json.loads(row["reasons"]),
            status=row["status"], created_at=row["created_at"],
            decided_at=row["decided_at"], decided_by=row["decided_by"],
        )

    # --- lineage -----------------------------------------------------------
    def add_lineage(self, ev: LineageEvent) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                "source_record_id, reason, changed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                 ev.source_record_id, ev.reason, ev.changed_at),
            )

    def list_lineage(self, entity_id: str, tenant_id: str, domain: str) -> list[LineageEvent]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM lineage WHERE entity_id = %s AND tenant_id = %s AND domain = %s ORDER BY changed_at",
                (entity_id, tenant_id, domain),
            )
            rows = cur.fetchall()
        return [
            LineageEvent(
                id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], entity_id=r["entity_id"],
                field=r["field"], old_value=r["old_value"], new_value=r["new_value"],
                source_record_id=r["source_record_id"], reason=r["reason"], changed_at=r["changed_at"],
            )
            for r in rows
        ]

    # --- API keys (RBAC / auth) --------------------------------------------
    def save_api_key(self, key: ApiKeyRecord) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO api_keys (id, tenant_id, role, key_hash, label, allowed_domains, "
                "created_at, revoked_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (key.id, key.tenant_id, key.role, key.key_hash, key.label,
                 json.dumps(key.allowed_domains) if key.allowed_domains is not None else None,
                 key.created_at, key.revoked_at),
            )

    def get_api_key_by_hash(self, key_hash: str) -> ApiKeyRecord | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM api_keys WHERE key_hash = %s", (key_hash,))
            row = cur.fetchone()
            return self._row_to_api_key(row) if row else None

    def list_api_keys(self, tenant_id: str) -> list[ApiKeyRecord]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM api_keys WHERE tenant_id = %s", (tenant_id,))
            rows = cur.fetchall()
        return [self._row_to_api_key(r) for r in rows]

    def revoke_api_key(self, key_id: str, tenant_id: str, revoked_at: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE api_keys SET revoked_at = %s WHERE id = %s AND tenant_id = %s",
                (revoked_at, key_id, tenant_id),
            )

    @staticmethod
    def _row_to_api_key(row) -> ApiKeyRecord:
        return ApiKeyRecord(
            id=row["id"], tenant_id=row["tenant_id"], role=row["role"], key_hash=row["key_hash"],
            label=row["label"],
            allowed_domains=json.loads(row["allowed_domains"]) if row["allowed_domains"] else None,
            created_at=row["created_at"], revoked_at=row["revoked_at"],
        )

    # --- Audit log -----------------------------------------------------------
    def append_audit_entry(self, entry: AuditLogEntry) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit_log (id, tenant_id, domain, action, actor, details, prev_hash, "
                "entry_hash, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (entry.id, entry.tenant_id, entry.domain, entry.action, entry.actor,
                 json.dumps(entry.details), entry.prev_hash, entry.entry_hash, entry.created_at),
            )

    def get_last_audit_entry(self, tenant_id: str) -> AuditLogEntry | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM audit_log WHERE tenant_id = %s ORDER BY created_at DESC LIMIT 1",
                (tenant_id,),
            )
            row = cur.fetchone()
            return self._row_to_audit_entry(row) if row else None

    def list_audit_log(self, tenant_id: str, domain: str | None = None, limit: int = 200) -> list[AuditLogEntry]:
        with self._conn() as conn, conn.cursor() as cur:
            if domain:
                cur.execute(
                    "SELECT * FROM audit_log WHERE tenant_id = %s AND domain = %s "
                    "ORDER BY created_at ASC LIMIT %s",
                    (tenant_id, domain, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM audit_log WHERE tenant_id = %s ORDER BY created_at ASC LIMIT %s",
                    (tenant_id, limit),
                )
            rows = cur.fetchall()
        return [self._row_to_audit_entry(r) for r in rows]

    @staticmethod
    def _row_to_audit_entry(row) -> AuditLogEntry:
        return AuditLogEntry(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"], action=row["action"],
            actor=row["actor"], details=json.loads(row["details"]),
            prev_hash=row["prev_hash"], entry_hash=row["entry_hash"], created_at=row["created_at"],
        )

    # --- Usage events (billing metering) -----------------------------------
    def record_usage_event(self, ev: UsageEvent) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO usage_events (id, tenant_id, domain, event_type, quantity, metadata, "
                "created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (ev.id, ev.tenant_id, ev.domain, ev.event_type, ev.quantity,
                 json.dumps(ev.metadata), ev.created_at),
            )

    def list_usage_events(self, tenant_id: str, since: str | None = None) -> list[UsageEvent]:
        with self._conn() as conn, conn.cursor() as cur:
            if since:
                cur.execute(
                    "SELECT * FROM usage_events WHERE tenant_id = %s AND created_at >= %s ORDER BY created_at",
                    (tenant_id, since),
                )
            else:
                cur.execute(
                    "SELECT * FROM usage_events WHERE tenant_id = %s ORDER BY created_at", (tenant_id,)
                )
            rows = cur.fetchall()
        return [
            UsageEvent(
                id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], event_type=r["event_type"],
                quantity=r["quantity"], metadata=json.loads(r["metadata"]), created_at=r["created_at"],
            )
            for r in rows
        ]
