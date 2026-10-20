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

JSON-holding columns (`attributes`, `reasons`, `details`, `metadata`,
`allowed_domains`) are native `JSONB`, not `TEXT`. This unlocks querying
INTO individual attribute fields directly in SQL (e.g.
`WHERE attributes->>'email' = 'x@y.com'`) instead of only ever loading
the whole blob into Python -- something SQLite's schema can't offer the
same way. Two consequences of this, both handled below:

  - WRITES pass a `psycopg2.extras.Json(...)`-wrapped value (see the
    `_j()` helper), not `json.dumps(...)` -- the idiomatic, unambiguous
    way to hand psycopg2 a Python dict/list for a jsonb column.
  - READS get the value back as an already-parsed Python dict/list --
    psycopg2 registers default adapters for the json/jsonb PostgreSQL
    types, so `row["attributes"]` is a dict already, not a JSON string.
    Calling `json.loads()` on it would raise a TypeError. Every
    `_row_to_X()` method below reflects this: no `json.loads()` calls
    remain for these columns.

⚠️ Like the rest of this file, the JSONB read/write round trip has NOT
been run against a real Postgres server (no network in the build
sandbox) -- it's written against psycopg2's documented default
adaptation behavior, not verified live. Validate with
`tests/test_postgres_backend.py` on your own machine.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

from . import golden, protect
from .migrations import PostgresMigrationBackend, apply_or_verify, auto_migrate_enabled
from .sql_dialect import get_dialect
from .models import ApiKeyRecord, AuditLogEntry, Entity, LineageEvent, MatchCandidate, Recommendation, SourceRecord, UsageEvent, now_iso


def _j(value):
    """Wraps a Python dict/list for a JSONB column parameter. None stays
    None (for nullable JSONB columns like api_keys.allowed_domains) --
    Json(None) would otherwise write the JSON literal `null` instead of
    a real SQL NULL, which would break the `IS NULL` checks callers do
    to mean 'no domain restriction'."""
    if value is None:
        return None
    from psycopg2.extras import Json
    return Json(value)



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

    def __init__(self, database_url: str, auto_migrate: bool | None = None):
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
        self._batch_depth = 0      # > 0 while a batch is open; only the thread that holds self._lock can see or change it
        self._savepoints = 0
        self._auto_migrate = auto_migrate
        self._entity_synced: dict = {}
        self._init_schema(auto_migrate)

    def _get_connection(self):
        import psycopg2
        import psycopg2.extras

        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                self.database_url, cursor_factory=psycopg2.extras.RealDictCursor
            )
        return self._connection

    @contextmanager
    def _conn(self, tenant_id: str | None = None):
        """When `tenant_id` is given, sets the Postgres session variable
        `app.current_tenant_id` via SET LOCAL before yielding -- this is
        what RLS policies (see app/rls_policies.py) check to decide
        which rows a query can see. SET LOCAL's effect lasts only for
        the current transaction, matching this method's own
        commit-per-call boundary, so it never leaks into a later call.

        This runs unconditionally whenever tenant_id is passed, whether
        or not RLS policies are actually enabled on the tables --
        setting an unused Postgres custom session variable is harmless
        overhead (no pre-declaration required), so there's no separate
        "is RLS on" flag to keep in sync here. The policies themselves
        are opt-in, applied separately -- see setup_rls.py. Without
        them, this SET LOCAL does nothing observable; WITH them, it's
        the enforcement mechanism.
        """
        with self._lock:
            conn = self._get_connection()
            batched = self._batch_depth > 0        # inside a batch the BATCH commits or rolls back (and a savepoint undoes one block), not every call
            try:
                if tenant_id is not None:
                    with conn.cursor() as cur:
                        cur.execute("SET LOCAL app.current_tenant_id = %s", (tenant_id,))      # set again on every call: it lasts for the whole transaction
                yield conn
                if not batched:
                    conn.commit()
            except Exception:
                if not batched:
                    conn.rollback()
                raise

    # --- units of work ------------------------------------------------
    @staticmethod
    def _tx(conn, *statements: str) -> None:
        """Transaction control (SAVEPOINT ...): standard SQL, and the ONLY place the units of work execute any (the portability ratchet counts it once)."""
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)

    def in_batch(self) -> bool:
        return self._batch_depth > 0

    @contextmanager
    def batch(self, domain: str | None = None):
        """ONE transaction for everything the block writes, committed at the end (or rolled back whole if the block raises): one commit, not one per store call. Reentrant. The store
        keeps ONE connection, so other threads of this process wait on the lock until the batch ends (as they already waited per call). `domain`: see the SQLite store."""
        if domain is not None:
            self._ensure_entity_table(domain)
        with self._lock:
            if self._batch_depth > 0:
                self._batch_depth += 1
                try:
                    yield
                finally:
                    self._batch_depth -= 1
                return
            conn = self._get_connection()
            self._batch_depth, self._savepoints = 1, 0
            try:
                yield
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                self._batch_depth = 0

    @contextmanager
    def savepoint(self):
        """Inside a batch: if the block raises, only ITS writes are undone (this also clears Postgres's 'transaction aborted' state) and the exception goes on. Outside a batch: nothing."""
        if self._batch_depth == 0:
            yield
            return
        with self._lock:
            conn = self._get_connection()
            self._savepoints += 1
            name = f"mdm_sp_{self._savepoints}"
            self._tx(conn, f"SAVEPOINT {name}")
            try:
                yield
            except BaseException:
                self._tx(conn, f"ROLLBACK TO SAVEPOINT {name}", f"RELEASE SAVEPOINT {name}")
                raise
            else:
                self._tx(conn, f"RELEASE SAVEPOINT {name}")

    def _init_schema(self, auto_migrate: bool | None = None) -> None:
        """The schema is owned by app/migrations. Runs on its own dedicated
        connection (not this store's persistent one), holding a Postgres
        advisory lock so concurrent app instances don't race."""
        apply_or_verify(PostgresMigrationBackend(self.database_url), auto_migrate)

    # --- source records -----------------------------------------------
    def save_source_record(self, rec: SourceRecord) -> None:
        with self._conn(tenant_id=rec.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, "
                "ingested_at, entity_id, content_hash, version, updated_at, processing_status, processing_attempts, processing_error) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (rec.id, rec.tenant_id, rec.domain, rec.source_system, rec.source_record_id, _j(rec.attributes),
                 rec.ingested_at, rec.entity_id, rec.content_hash, rec.version, rec.updated_at,
                 rec.processing_status, rec.processing_attempts, rec.processing_error))

    def save_source_records_batch(self, records: list[SourceRecord]) -> None:
        if not records:
            return
        with self._conn(tenant_id=records[0].tenant_id) as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, "
                "ingested_at, entity_id, content_hash, version, updated_at, processing_status, processing_attempts, processing_error) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(r.id, r.tenant_id, r.domain, r.source_system, r.source_record_id, _j(r.attributes), r.ingested_at,
                  r.entity_id, r.content_hash, r.version, r.updated_at,
                  r.processing_status, r.processing_attempts, r.processing_error) for r in records])

    def get_source_record(self, rec_id: str, tenant_id: str, domain: str) -> SourceRecord | None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM source_records WHERE id = %s AND tenant_id = %s AND domain = %s",
                (rec_id, tenant_id, domain),
            )
            row = cur.fetchone()
            return self._row_to_source_record(row) if row else None

    def list_source_records(self, tenant_id: str, domain: str, entity_id: str | None = None) -> list[SourceRecord]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE source_records SET entity_id = %s WHERE id = %s AND tenant_id = %s AND domain = %s",
                (entity_id, record_id, tenant_id, domain),
            )

    def delete_source_record(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM source_records WHERE id = %s AND tenant_id = %s AND domain = %s",
                (record_id, tenant_id, domain),
            )

    # --- Blocking keys (indexed candidate lookup for matching) ------------
    def save_blocking_keys(self, tenant_id: str, domain: str, record_id: str, keys: set[str]) -> None:
        if not keys:
            return
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (%s, %s, %s, %s)",
                [(tenant_id, domain, record_id, k) for k in keys],
            )

    def save_blocking_keys_batch(self, entries: list[tuple[str, str, str, set[str]]]) -> None:
        rows = [
            (tenant_id, domain, record_id, key)
            for tenant_id, domain, record_id, keys in entries
            for key in keys
        ]
        if not rows:
            return
        with self._conn(tenant_id=entries[0][0] if entries else None) as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (%s, %s, %s, %s)",
                rows,
            )

    def find_candidate_record_ids(self, tenant_id: str, domain: str, keys: set[str],
                                   exclude_record_id: str) -> set[str]:
        if not keys:
            return set()
        keys_list = list(keys)
        placeholders = ",".join("%s" for _ in keys_list)
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT record_id FROM blocking_keys "
                f"WHERE tenant_id = %s AND domain = %s AND blocking_key IN ({placeholders}) "
                f"AND record_id != %s",
                (tenant_id, domain, *keys_list, exclude_record_id),
            )
            rows = cur.fetchall()
        return {r["record_id"] for r in rows}

    def delete_blocking_keys(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM blocking_keys WHERE record_id = %s AND tenant_id = %s AND domain = %s",
                (record_id, tenant_id, domain),
            )

    @staticmethod
    def _row_to_source_record(row) -> SourceRecord:
        return SourceRecord(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"],
            source_system=row["source_system"], source_record_id=row["source_record_id"],
            attributes=row["attributes"],
            ingested_at=row["ingested_at"], entity_id=row["entity_id"],
            content_hash=row["content_hash"], version=row["version"], updated_at=row["updated_at"],
            processing_status=row["processing_status"], processing_attempts=row["processing_attempts"],
            processing_error=row["processing_error"],
        )

    # --- entities = golden records: one table per domain, every field a real TYPED column (app/golden.py) ----
    @staticmethod
    def _entity_columns(domain: str):
        from .domains import REGISTRY
        cfg = REGISTRY.get(domain)
        return golden.field_columns(cfg) if cfg else None

    def _entity_fields(self, domain: str):
        cols = self._entity_columns(domain)
        return None if cols is None else [f for f, _ in cols]

    def _ensure_entity_table(self, domain: str) -> None:
        cols = self._entity_columns(domain)
        if cols is None or self._entity_synced.get(domain) == tuple(cols):
            return
        if self.in_batch():
            raise RuntimeError(f"the golden table for domain {domain!r} has not been checked yet, and the check cannot run inside a batch (it opens its own connection and would wait "
                               f"for this transaction for ever): open the batch with store.batch(domain={domain!r})")
        golden.ensure_or_verify(PostgresMigrationBackend(self.database_url), domain, cols, auto_migrate_enabled(self._auto_migrate))
        self._entity_synced[domain] = tuple(cols)

    def sync_entity_table(self, domain: str) -> list:
        cols = self._entity_columns(domain)
        added = golden.sync_table(PostgresMigrationBackend(self.database_url), domain, cols)
        self._entity_synced[domain] = tuple(cols)
        return added

    def save_entity(self, ent: Entity) -> None:
        self._ensure_entity_table(ent.domain)
        cols = self._entity_columns(ent.domain)
        if cols is None:
            raise KeyError(f"No configuration is loaded for domain '{ent.domain}', so its golden records cannot be written")
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        values = [ent.id, ent.tenant_id, ent.status, ent.created_at, ent.updated_at,
                  _j(ent.quality_issues) if ent.quality_issues else None, *[ent.attributes.get(f) for f, _ in cols]]
        updates = ", ".join(f"{golden.q(c)} = EXCLUDED.{golden.q(c)}" for c in names[1:])
        with self._conn(tenant_id=ent.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(f"INSERT INTO {golden.q(golden.table_name(ent.domain))} ({', '.join(golden.q(c) for c in names)}) "
                        f"VALUES ({', '.join(['%s'] * len(names))}) ON CONFLICT (entity_id) DO UPDATE SET {updates}", values)

    def _row_to_entity(self, row, domain: str, source_ids: list) -> Entity:
        fields = self._entity_fields(domain)
        if fields is None:
            fields = [c for c in row.keys() if c not in golden.SYSTEM_COLUMNS]
        return Entity(id=row["entity_id"], tenant_id=row["tenant_id"], domain=domain, status=row["status"],
                      attributes={f: row.get(f) for f in fields}, quality_issues=row.get("quality_issues") or [],
                      source_record_ids=source_ids, created_at=row["created_at"], updated_at=row["updated_at"])

    def get_entity(self, entity_id: str, tenant_id: str, domain: str) -> Entity | None:
        self._ensure_entity_table(domain)
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {golden.q(golden.table_name(domain))} WHERE entity_id = %s AND tenant_id = %s", (entity_id, tenant_id))
            row = cur.fetchone()
        if not row:
            return None
        return self._row_to_entity(row, domain, [r.id for r in self.list_source_records(tenant_id, domain, entity_id=entity_id)])

    def list_entities(self, tenant_id: str, domain: str, status: str | None = None) -> list[Entity]:
        self._ensure_entity_table(domain)
        sql, params = f"SELECT entity_id FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = %s", [tenant_id]
        if status:
            sql, params = sql + " AND status = %s", params + [status]
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            ids = [r["entity_id"] for r in cur.fetchall()]
        return [self.get_entity(i, tenant_id, domain) for i in ids]

    def find_entities_by_attribute(self, tenant_id: str, domain: str, field: str, value: str) -> list[Entity]:
        """Finds golden records by a field's value directly in SQL. The field is a real column now, so this is an
        ordinary indexable equality query (add an index on the column if you use it a lot)."""
        fields = self._entity_fields(domain)
        if fields is None or field not in fields:
            raise ValueError(f"'{field}' is not a field of domain '{domain}'")
        self._ensure_entity_table(domain)
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT entity_id FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = %s AND {golden.q(field)} = %s",
                        (tenant_id, value))
            ids = [r["entity_id"] for r in cur.fetchall()]
        return [self.get_entity(i, tenant_id, domain) for i in ids]

    def delete_entity(self, entity_id: str, tenant_id: str, domain: str) -> None:
        """Hard delete -- right-to-be-forgotten purge only (app/compliance.py)."""
        self._ensure_entity_table(domain)
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {golden.q(golden.table_name(domain))} WHERE entity_id = %s AND tenant_id = %s", (entity_id, tenant_id))

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        with self._conn(tenant_id=c.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, "
                "match_type, reasons, status, created_at, decided_at, decided_by, contributions) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 _j(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by, _j(c.contributions)),
            )

    def update_candidate_status(self, cand_id: str, tenant_id: str, domain: str,
                                 status: str, decided_by: str, decided_at: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE match_candidates SET status = %s, decided_by = %s, decided_at = %s "
                "WHERE id = %s AND tenant_id = %s AND domain = %s",
                (status, decided_by, decided_at, cand_id, tenant_id, domain),
            )

    def list_candidates(self, tenant_id: str, domain: str, status: str | None = "pending") -> list[MatchCandidate]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
                score=r["score"], match_type=r["match_type"], reasons=r["reasons"], contributions=r["contributions"] or [],
                status=r["status"], created_at=r["created_at"],
                decided_at=r["decided_at"], decided_by=r["decided_by"],
            )
            for r in rows
        ]

    def get_candidate(self, cand_id: str, tenant_id: str, domain: str) -> MatchCandidate | None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
            score=row["score"], match_type=row["match_type"], reasons=row["reasons"], contributions=row["contributions"] or [],
            status=row["status"], created_at=row["created_at"],
            decided_at=row["decided_at"], decided_by=row["decided_by"],
        )

    # --- lineage -----------------------------------------------------------
    def add_lineage(self, ev: LineageEvent) -> None:
        with self._conn(tenant_id=ev.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                "source_record_id, reason, changed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                 ev.source_record_id, ev.reason, ev.changed_at),
            )

    def add_lineage_batch(self, events: list) -> None:
        if events:
            with self._conn(tenant_id=events[0].tenant_id) as conn, conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                    "source_record_id, reason, changed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [(ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                      ev.source_record_id, ev.reason, ev.changed_at) for ev in events])

    def list_lineage(self, entity_id: str, tenant_id: str, domain: str) -> list[LineageEvent]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
        with self._conn(tenant_id=key.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO api_keys (id, tenant_id, role, key_hash, label, allowed_domains, "
                "created_at, revoked_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (key.id, key.tenant_id, key.role, key.key_hash, key.label,
                 _j(key.allowed_domains),
                 key.created_at, key.revoked_at),
            )

    def get_api_key_by_hash(self, key_hash: str) -> ApiKeyRecord | None:
        """Deliberately NOT tenant-scoped via _conn(tenant_id=...), unlike
        every other method in this class -- this is the one place that
        genuinely can't be: it's the authentication entry point, called
        BEFORE the caller's tenant is known. The row this query finds is
        what TELLS you the tenant, so there's no tenant_id to set as
        session context ahead of time. If RLS policies are applied to
        the api_keys table with the same tenant-isolation pattern as
        every other table, this specific query would need to run as a
        role that bypasses RLS for this one lookup (or the api_keys
        table gets excluded from RLS entirely and relies on this being
        the only unscoped read path into it, with every other api_keys
        method already tenant-scoped normally). See app/rls_policies.py
        for how this is actually handled."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM api_keys WHERE key_hash = %s", (key_hash,))
            row = cur.fetchone()
            return self._row_to_api_key(row) if row else None

    def list_api_keys(self, tenant_id: str) -> list[ApiKeyRecord]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM api_keys WHERE tenant_id = %s", (tenant_id,))
            rows = cur.fetchall()
        return [self._row_to_api_key(r) for r in rows]

    def revoke_api_key(self, key_id: str, tenant_id: str, revoked_at: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE api_keys SET revoked_at = %s WHERE id = %s AND tenant_id = %s",
                (revoked_at, key_id, tenant_id),
            )

    @staticmethod
    def _row_to_api_key(row) -> ApiKeyRecord:
        return ApiKeyRecord(
            id=row["id"], tenant_id=row["tenant_id"], role=row["role"], key_hash=row["key_hash"],
            label=row["label"],
            allowed_domains=row["allowed_domains"],
            created_at=row["created_at"], revoked_at=row["revoked_at"],
        )

    # --- Audit log -----------------------------------------------------------
    def append_audit_entry(self, entry: AuditLogEntry) -> None:
        with self._conn(tenant_id=entry.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit_log (id, tenant_id, domain, action, actor, details, prev_hash, "
                "entry_hash, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (entry.id, entry.tenant_id, entry.domain, entry.action, entry.actor,
                 _j(entry.details), entry.prev_hash, entry.entry_hash, entry.created_at),
            )

    def get_last_audit_entry(self, tenant_id: str) -> AuditLogEntry | None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM audit_log WHERE tenant_id = %s ORDER BY created_at DESC LIMIT 1",
                (tenant_id,),
            )
            row = cur.fetchone()
            return self._row_to_audit_entry(row) if row else None

    def list_audit_log(self, tenant_id: str, domain: str | None = None, limit: int = 200) -> list[AuditLogEntry]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
            actor=row["actor"], details=row["details"],
            prev_hash=row["prev_hash"], entry_hash=row["entry_hash"], created_at=row["created_at"],
        )

    # --- Usage events (billing metering) -----------------------------------
    def record_usage_event(self, ev: UsageEvent) -> None:
        with self._conn(tenant_id=ev.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO usage_events (id, tenant_id, domain, event_type, quantity, metadata, "
                "created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (ev.id, ev.tenant_id, ev.domain, ev.event_type, ev.quantity,
                 _j(ev.metadata), ev.created_at),
            )

    def list_usage_events(self, tenant_id: str, since: str | None = None) -> list[UsageEvent]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
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
                quantity=r["quantity"], metadata=r["metadata"], created_at=r["created_at"],
            )
            for r in rows
        ]

    # --- Blocking-index state and reindexing (see app/indexing.py) --------
    def get_index_state(self, domain: str):
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT fingerprint, status FROM blocking_index_state WHERE domain = %s", (domain,))
            row = cur.fetchone()
        return (row["fingerprint"], row["status"]) if row else None

    def set_index_state(self, domain: str, fingerprint: str, status: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO blocking_index_state (domain, fingerprint, status, updated_at) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (domain) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, "
                "status = EXCLUDED.status, updated_at = EXCLUDED.updated_at",
                (domain, fingerprint, status, now_iso()),
            )

    def domain_has_index_entries(self, domain: str) -> bool:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS present FROM blocking_keys WHERE domain = %s LIMIT 1", (domain,))
            return cur.fetchone() is not None

    def count_source_records(self, domain: str) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM source_records WHERE domain = %s", (domain,))
            return cur.fetchone()["n"]

    def list_source_records_page(self, domain: str, after_id: str, limit: int) -> list[SourceRecord]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM source_records WHERE domain = %s AND id > %s ORDER BY id LIMIT %s",
                (domain, after_id, limit),
            )
            rows = cur.fetchall()
        return [self._row_to_source_record(r) for r in rows]

    def replace_blocking_keys_batch(self, entries: list[tuple[str, str, str, set[str]]], fresh: bool = False) -> None:
        """fresh=True: these records are NEW and cannot hold keys yet, so nothing is deleted (one statement fewer)."""
        deletes = [] if fresh else [(t, d, rid) for t, d, rid, _ in entries]
        inserts = [(t, d, rid, k) for t, d, rid, keys in entries for k in keys]
        with self._conn() as conn, conn.cursor() as cur:
            if deletes:
                cur.executemany(
                    "DELETE FROM blocking_keys WHERE tenant_id = %s AND domain = %s AND record_id = %s", deletes)
            cur.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (%s, %s, %s, %s)", inserts)

    # --- Bucket sizes and statistics (see matching.py / domains_cli.py blocking-stats) ---
    def iter_blocking_keys(self, tenant_id: str, domain: str, page: int = 50_000):
        last = None
        while True:
            sql, params = "SELECT DISTINCT blocking_key FROM blocking_keys WHERE tenant_id = %s AND domain = %s", [tenant_id, domain]
            if last is not None:
                sql, params = sql + " AND blocking_key > %s", params + [last]
            with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
                cur.execute(sql + " ORDER BY blocking_key LIMIT %s", (*params, page))
                rows = cur.fetchall()
            if not rows:
                return
            yield [r["blocking_key"] for r in rows]
            last = rows[-1]["blocking_key"]

    def count_blocking_keys(self, tenant_id: str, domain: str) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM blocking_keys WHERE tenant_id = %s AND domain = %s", (tenant_id, domain))
            return cur.fetchone()["n"]

    def blocking_bucket_sizes(self, tenant_id: str, domain: str, keys: set[str]) -> dict[str, int]:
        if not keys:
            return {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT blocking_key, COUNT(*) AS n FROM blocking_keys WHERE tenant_id = %s AND domain = %s "
                "AND blocking_key = ANY(%s) GROUP BY blocking_key", (tenant_id, domain, list(keys)))
            return {r["blocking_key"]: r["n"] for r in cur.fetchall()}

    def blocking_key_stats(self, domain: str, top_n: int = 10) -> dict:
        sized = "(SELECT COUNT(*) AS n FROM blocking_keys WHERE domain = %s GROUP BY tenant_id, blocking_key) AS t"
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS buckets, COALESCE(SUM(n), 0) AS key_rows, COALESCE(SUM(n * n), 0) AS sq, "
                        f"COALESCE(MAX(n), 0) AS biggest FROM {sized}", (domain,))
            t = cur.fetchone()
            cur.execute(f"SELECT CASE WHEN n = 1 THEN '1' WHEN n <= 10 THEN '2-10' WHEN n <= 100 THEN '11-100' "
                        f"WHEN n <= 1000 THEN '101-1000' ELSE '1000+' END AS band, COUNT(*) AS buckets FROM {sized} GROUP BY band", (domain,))
            bands = cur.fetchall()
            cur.execute("SELECT tenant_id, blocking_key, COUNT(*) AS n FROM blocking_keys WHERE domain = %s "
                        "GROUP BY tenant_id, blocking_key ORDER BY n DESC, blocking_key LIMIT %s", (domain, top_n))
            top = cur.fetchall()
            cur.execute("SELECT COUNT(DISTINCT record_id) AS n FROM blocking_keys WHERE domain = %s "
                        "AND blocking_key = '__no_block__'", (domain,))
            unkeyed = cur.fetchone()["n"]
        return {"buckets": t["buckets"], "key_rows": int(t["key_rows"]), "sum_squares": int(t["sq"]), "biggest": t["biggest"],
                "bands": {r["band"]: r["buckets"] for r in bands},
                "top": [(r["tenant_id"], r["blocking_key"], r["n"]) for r in top], "records_without_keys": unkeyed}

    # --- Staging layer: standardized_records (see app/staging.py) ---------
    def save_staged_batch(self, rows: list[tuple]) -> None:
        if not rows:
            return
        now = now_iso()
        with self._conn(tenant_id=rows[0][0]) as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO standardized_records (tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (tenant_id, domain, record_id) DO UPDATE SET "
                "matching = EXCLUDED.matching, cleansed = EXCLUDED.cleansed, fingerprint = EXCLUDED.fingerprint, "
                "standardized_at = EXCLUDED.standardized_at",
                [(t, d, rid, _j(m), _j(c), fp, now) for t, d, rid, m, c, fp in rows])

    def get_staged_many(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids = list(record_ids)
        if not ids:
            return {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT record_id, matching, cleansed FROM standardized_records "
                        "WHERE tenant_id = %s AND domain = %s AND record_id = ANY(%s)", (tenant_id, domain, ids))
            return {r["record_id"]: (r["matching"], r["cleansed"]) for r in cur.fetchall()}

    def delete_staged(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM standardized_records WHERE record_id = %s AND tenant_id = %s AND domain = %s",
                        (record_id, tenant_id, domain))

    def count_staged(self, domain: str) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM standardized_records WHERE domain = %s", (domain,))
            return cur.fetchone()["n"]

    def list_entities_page(self, domain: str, after_id: str, limit: int) -> list[tuple[str, str]]:
        self._ensure_entity_table(domain)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT tenant_id, entity_id FROM {golden.q(golden.table_name(domain))} "
                        f"WHERE status = 'active' AND entity_id > %s ORDER BY entity_id LIMIT %s", (after_id, limit))
            return [(r["tenant_id"], r["entity_id"]) for r in cur.fetchall()]

    def replace_derived_batch(self, entries: list[tuple]) -> None:
        now = now_iso()
        deletes = [(t, d, rid) for t, d, rid, *_ in entries]
        keys = [(t, d, rid, k) for t, d, rid, _m, _c, _f, ks in entries for k in ks]
        staged = [(t, d, rid, _j(m), _j(c), fp, now) for t, d, rid, m, c, fp, _ks in entries]
        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany("DELETE FROM blocking_keys WHERE tenant_id = %s AND domain = %s AND record_id = %s", deletes)
            cur.executemany("INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (%s, %s, %s, %s)", keys)
            cur.executemany(
                "INSERT INTO standardized_records (tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (tenant_id, domain, record_id) DO UPDATE SET "
                "matching = EXCLUDED.matching, cleansed = EXCLUDED.cleansed, fingerprint = EXCLUDED.fingerprint, "
                "standardized_at = EXCLUDED.standardized_at", staged)

    # --- Idempotent raw ingest (see pipeline.ingest_record) ----------------
    _NATURAL = ("SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s AND source_system = %s AND source_record_id = %s "
                "ORDER BY ingested_at DESC, id DESC LIMIT 1")

    def find_source_record(self, tenant_id: str, domain: str, source_system: str, source_record_id: str) -> SourceRecord | None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(self._NATURAL, (tenant_id, domain, source_system, source_record_id))
            row = cur.fetchone()
        return self._row_to_source_record(row) if row else None

    def find_source_records_by_keys(self, tenant_id: str, domain: str, keys) -> dict:
        """{(source_system, source_record_id): the latest record held} for the keys that are held. ONE query per source system per chunk of ids (it used to be one query per key)."""
        by_system: dict = {}
        for source_system, source_record_id in keys:
            by_system.setdefault(source_system, []).append(source_record_id)
        d, out = get_dialect("postgres"), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for source_system, ids in by_system.items():
                for chunk in d.chunks(ids):
                    cond, params = d.in_list("source_record_id", chunk)
                    cur.execute(f"SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s AND source_system = %s AND {cond} "
                                f"ORDER BY ingested_at DESC, id DESC", (tenant_id, domain, source_system, *params))
                    for row in cur.fetchall():
                        out.setdefault((source_system, row["source_record_id"]), self._row_to_source_record(row))   # newest first: the first one seen is the latest
        return out

    def update_source_record(self, rec_id: str, tenant_id: str, domain: str, attributes: dict, content_hash: str, version: int) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("UPDATE source_records SET attributes = %s, content_hash = %s, version = %s, updated_at = %s, "
                        "processing_status = 'landed', processing_attempts = 0, processing_error = NULL "
                        "WHERE id = %s AND tenant_id = %s AND domain = %s",
                        (_j(attributes), content_hash, version, now_iso(), rec_id, tenant_id, domain))

    def add_record_history(self, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._conn(tenant_id=rows[0][0]) as conn, conn.cursor() as cur:
            cur.executemany("INSERT INTO source_record_history (tenant_id, domain, record_id, version, content_hash, attributes, superseded_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (record_id, version) DO UPDATE SET "
                            "content_hash = EXCLUDED.content_hash, attributes = EXCLUDED.attributes, superseded_at = EXCLUDED.superseded_at",
                            [(t, d, rid, v, h, _j(a), at) for t, d, rid, v, h, a, at in rows])

    def list_record_history(self, record_id: str, tenant_id: str, domain: str) -> list[dict]:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT version, content_hash, attributes, superseded_at FROM source_record_history "
                        "WHERE record_id = %s AND tenant_id = %s AND domain = %s ORDER BY version", (record_id, tenant_id, domain))
            return [dict(r) for r in cur.fetchall()]

    def delete_record_history(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM source_record_history WHERE record_id = %s AND tenant_id = %s AND domain = %s", (record_id, tenant_id, domain))

    def supersede_pending_candidates(self, tenant_id: str, domain: str, record_id: str) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("UPDATE match_candidates SET status = 'superseded', decided_at = %s, decided_by = 'system:record_changed' "
                        "WHERE tenant_id = %s AND domain = %s AND status = 'pending' AND (record_a_id = %s OR record_b_id = %s)",
                        (now_iso(), tenant_id, domain, record_id, record_id))
            return cur.rowcount

    def count_duplicate_raw_keys(self, domain: str) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM (SELECT 1 FROM source_records WHERE domain = %s "
                        "GROUP BY tenant_id, source_system, source_record_id HAVING COUNT(*) > 1) AS t", (domain,))
            return cur.fetchone()["n"]

    # --- Processing state, run log, deferred golden rebuilds (see pipeline.py) -------------------------
    def set_processing_status(self, record_ids, tenant_id: str, domain: str, status: str, error: str | None = None) -> None:
        ids = list(record_ids)
        if ids:
            with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
                cur.execute("UPDATE source_records SET processing_status = %s, processing_error = %s WHERE tenant_id = %s AND domain = %s "
                            "AND id = ANY(%s)", (status, error, tenant_id, domain, ids))

    def record_processing_failure(self, record_id: str, tenant_id: str, domain: str, message: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("UPDATE source_records SET processing_error = %s, processing_attempts = processing_attempts + 1 "
                        "WHERE id = %s AND tenant_id = %s AND domain = %s", (message, record_id, tenant_id, domain))

    def list_unfinished_records(self, domain: str, limit: int, max_attempts: int) -> list[SourceRecord]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM source_records WHERE domain = %s AND processing_status <> 'matched' AND processing_attempts < %s "
                        "ORDER BY ingested_at, id LIMIT %s", (domain, max_attempts, limit))
            return [self._row_to_source_record(r) for r in cur.fetchall()]

    def count_unfinished(self, domain: str) -> tuple:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN processing_error IS NOT NULL THEN 1 ELSE 0 END), 0) AS failing "
                        "FROM source_records WHERE domain = %s AND processing_status <> 'matched'", (domain,))
            r = cur.fetchone()
        return r["n"], int(r["failing"])

    def list_candidate_pairs(self, tenant_id: str, domain: str, record_id: str) -> dict:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, record_b_id AS other, status, score FROM match_candidates WHERE tenant_id = %s AND domain = %s AND record_a_id = %s AND status <> 'superseded' "
                "UNION ALL SELECT id, record_a_id AS other, status, score FROM match_candidates WHERE tenant_id = %s AND domain = %s AND record_b_id = %s AND status <> 'superseded'",
                (tenant_id, domain, record_id, tenant_id, domain, record_id))
            return {r["other"]: {"id": r["id"], "status": r["status"], "score": r["score"]} for r in cur.fetchall()}

    def create_run(self, run_id: str, tenant_id: str, domain: str, mode: str, triggered_by: str, started_at: str) -> None:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO pipeline_runs (run_id, tenant_id, domain, mode, triggered_by, status, started_at, finished_at, counts, errors) "
                        "VALUES (%s, %s, %s, %s, %s, 'running', %s, NULL, '{}'::jsonb, '[]'::jsonb)", (run_id, tenant_id, domain, mode, triggered_by, started_at))

    def finish_run(self, run_id: str, status: str, counts: dict, errors: list) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE pipeline_runs SET status = %s, finished_at = %s, counts = %s, errors = %s WHERE run_id = %s",
                        (status, now_iso(), _j(counts), _j(errors), run_id))

    def list_runs(self, domain: str | None = None, tenant_id: str | None = None, limit: int = 20) -> list[dict]:
        sql, params = "SELECT * FROM pipeline_runs WHERE TRUE", []
        for col, val in (("domain", domain), ("tenant_id", tenant_id)):
            if val:
                sql, params = sql + f" AND {col} = %s", params + [val]
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql + " ORDER BY started_at DESC, run_id DESC LIMIT %s", (*params, limit))
            return [dict(r) for r in cur.fetchall()]

    def mark_golden_dirty(self, rows: list[tuple]) -> None:
        if rows:
            with self._conn(tenant_id=rows[0][0]) as conn, conn.cursor() as cur:
                cur.executemany("INSERT INTO golden_dirty (tenant_id, domain, entity_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", rows)

    def list_golden_dirty(self, domain: str, limit: int = 10000) -> list[tuple]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT tenant_id, entity_id FROM golden_dirty WHERE domain = %s ORDER BY entity_id LIMIT %s", (domain, limit))
            return [(r["tenant_id"], r["entity_id"]) for r in cur.fetchall()]

    def clear_golden_dirty(self, rows: list[tuple]) -> None:
        if rows:
            with self._conn(tenant_id=rows[0][0]) as conn, conn.cursor() as cur:
                cur.executemany("DELETE FROM golden_dirty WHERE tenant_id = %s AND domain = %s AND entity_id = %s", rows)

    def iter_raw_attributes(self, domain: str, tenant_id: str | None = None, source_system: str | None = None, batch: int = 5000):
        last = ""
        while True:
            sql, params = "SELECT id, attributes FROM source_records WHERE domain = %s AND id > %s", [domain, last]
            for col, val in (("tenant_id", tenant_id), ("source_system", source_system)):
                if val:
                    sql, params = sql + f" AND {col} = %s", params + [val]
            with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
                cur.execute(sql + " ORDER BY id LIMIT %s", (*params, batch))
                rows = cur.fetchall()
            if not rows:
                return
            for r in rows:
                yield r["attributes"] if isinstance(r["attributes"], dict) else json.loads(r["attributes"])
            last = rows[-1]["id"]

    def scrub_protected(self, domain: str, field: str, prefix: str) -> dict:
        """See Store.scrub_protected. (Postgres SQL: written to the same shape, never run against a live server.)"""
        done = {"lineage": 0, "golden": 0, "reasons": 0}
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE lineage SET old_value = CASE WHEN old_value IS NULL OR old_value LIKE '***%%' THEN old_value ELSE %s || right(old_value, 4) END, "
                        "new_value = CASE WHEN new_value IS NULL OR new_value LIKE '***%%' THEN new_value ELSE %s || right(new_value, 4) END "
                        "WHERE domain = %s AND field = %s AND ((old_value IS NOT NULL AND old_value NOT LIKE '***%%') OR (new_value IS NOT NULL AND new_value NOT LIKE '***%%'))",
                        (prefix, prefix, domain, field))
            done["lineage"] = cur.rowcount
            col, table = golden.q(field), golden.q(golden.table_name(domain))
            cur.execute(f"UPDATE {table} SET {col} = %s || right({col}, 4) WHERE {col} IS NOT NULL AND {col} NOT LIKE '***%%'", (prefix,))
            done["golden"] = cur.rowcount
            cur.execute("SELECT id, reasons FROM match_candidates WHERE domain = %s AND (reasons::text LIKE %s OR reasons::text LIKE %s)",
                        (domain, f"%exact {field} match (%", f"%{field} disagrees (%"))
            for r in cur.fetchall():
                reasons = [protect.scrub_reason(field, x) for x in (r["reasons"] if isinstance(r["reasons"], list) else json.loads(r["reasons"]))]
                cur.execute("UPDATE match_candidates SET reasons = %s WHERE id = %s", (_j(reasons), r["id"]))
                done["reasons"] += 1
        return done

    # --- bulk load (app/bulk.py). Written to the same shape as the SQLite versions; never run against a live server. ----------
    def has_source_records(self, tenant_id: str, domain: str) -> bool:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM source_records WHERE tenant_id = %s AND domain = %s LIMIT 1", (tenant_id, domain))
            return cur.fetchone() is not None

    def save_candidates_batch(self, candidates: list) -> None:
        if candidates:
            with self._conn(tenant_id=candidates[0].tenant_id) as conn, conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at, "
                    "decided_at, decided_by, contributions) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [(c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type, _j(c.reasons), c.status, c.created_at,
                      c.decided_at, c.decided_by, _j(c.contributions)) for c in candidates])

    def save_entities_batch(self, entities: list) -> None:
        if not entities:
            return
        domain = entities[0].domain
        self._ensure_entity_table(domain)
        cols = self._entity_columns(domain)
        if cols is None:
            raise KeyError(f"No configuration is loaded for domain '{domain}', so its golden records cannot be written")
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        updates = ", ".join(f"{golden.q(c)} = EXCLUDED.{golden.q(c)}" for c in names[1:])
        rows = [[e.id, e.tenant_id, e.status, e.created_at, e.updated_at, _j(e.quality_issues) if e.quality_issues else None,
                 *[e.attributes.get(f) for f, _ in cols]] for e in entities]
        with self._conn(tenant_id=entities[0].tenant_id) as conn, conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {golden.q(golden.table_name(domain))} ({', '.join(golden.q(c) for c in names)}) "
                            f"VALUES ({', '.join(['%s'] * len(names))}) ON CONFLICT (entity_id) DO UPDATE SET {updates}", rows)

    def rollback_bulk(self, tenant_id: str, domain: str, token: str) -> None:
        rec, ent = f"rec\\_{token}\\_%", f"ent\\_{token}\\_%"
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM merge_audits WHERE tenant_id = %s AND domain = %s AND candidate_id IN (SELECT id FROM match_candidates WHERE tenant_id = %s AND domain = %s "
                        "AND record_a_id LIKE %s)", (tenant_id, domain, tenant_id, domain, rec))   # the pairs this load made were audited too
            for table, col, pattern in (("blocking_keys", "record_id", rec), ("standardized_records", "record_id", rec), ("match_candidates", "record_a_id", rec),
                                        ("lineage", "entity_id", ent), ("source_records", "id", rec)):
                cur.execute(f"DELETE FROM {table} WHERE tenant_id = %s AND domain = %s AND {col} LIKE %s", (tenant_id, domain, pattern))
            cur.execute(f"DELETE FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = %s AND entity_id LIKE %s", (tenant_id, ent))

    # --- onboarding a new source onto existing data (app/bulk.py). Same shape as the SQLite versions; never run against a live server. ----
    def count_tenant_records(self, tenant_id: str, domain: str) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM source_records WHERE tenant_id = %s AND domain = %s", (tenant_id, domain))
            return cur.fetchone()["n"]

    def unfinished_in(self, tenant_id: str, domain: str) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM source_records WHERE tenant_id = %s AND domain = %s AND processing_status <> 'matched'", (tenant_id, domain))
            return cur.fetchone()["n"]

    def blocking_members(self, tenant_id: str, domain: str, keys) -> dict:
        keys, out = list(keys), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(keys), 500):
                cur.execute("SELECT blocking_key, record_id FROM blocking_keys WHERE tenant_id = %s AND domain = %s AND blocking_key = ANY(%s)",
                            (tenant_id, domain, keys[i:i + 500]))
                for r in cur.fetchall():
                    out.setdefault(r["blocking_key"], []).append(r["record_id"])
        return out

    def get_record_entities(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids, out = list(record_ids), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute("SELECT id, entity_id FROM source_records WHERE tenant_id = %s AND domain = %s AND id = ANY(%s)", (tenant_id, domain, ids[i:i + 500]))
                out.update({r["id"]: r["entity_id"] for r in cur.fetchall()})
        return out

    def get_entities_batch(self, tenant_id: str, domain: str, entity_ids) -> dict:
        self._ensure_entity_table(domain)
        ids, out = list(entity_ids), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute(f"SELECT * FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = %s AND entity_id = ANY(%s)", (tenant_id, ids[i:i + 500]))
                for row in cur.fetchall():
                    out[row["entity_id"]] = self._row_to_entity(row, domain, [])
        return out

    def list_records_of_entities(self, tenant_id: str, domain: str, entity_ids) -> list:
        ids, out = list(entity_ids), []
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute("SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s AND entity_id = ANY(%s) ORDER BY ingested_at, id",
                            (tenant_id, domain, ids[i:i + 500]))
                out += [self._row_to_source_record(r) for r in cur.fetchall()]
        return out

    def apply_onboarding(self, tenant_id: str, domain: str, reassign: list, entities: list, lineage: list, *, history=(), record_updates=(), supersede=(),
                         derived=(), candidates=()) -> None:
        table = golden.q(golden.table_name(domain))
        self._ensure_entity_table(domain)
        cols = self._entity_columns(domain)
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        updates = ", ".join(f"{golden.q(c)} = EXCLUDED.{golden.q(c)}" for c in names[1:])
        rows = [[e.id, e.tenant_id, e.status, e.created_at, e.updated_at, _j(e.quality_issues) if e.quality_issues else None,
                 *[e.attributes.get(f) for f, _ in cols]] for e in entities]
        now = now_iso()
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:  # one transaction: the connection rolls back on any exception
            if history:
                cur.executemany("INSERT INTO source_record_history (tenant_id, domain, record_id, version, content_hash, attributes, superseded_at) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (record_id, version) DO UPDATE SET content_hash = EXCLUDED.content_hash, "
                                "attributes = EXCLUDED.attributes, superseded_at = EXCLUDED.superseded_at", [(t, d, rid, v, h, _j(a), at) for t, d, rid, v, h, a, at in history])
            if record_updates:
                cur.executemany("UPDATE source_records SET attributes = %s, content_hash = %s, version = %s, updated_at = %s, processing_status = 'matched', "
                                "processing_attempts = 0, processing_error = NULL WHERE id = %s AND tenant_id = %s AND domain = %s",
                                [(_j(a), h, v, at, rid, tenant_id, domain) for rid, a, h, v, at in record_updates])
            if supersede:
                ids = list(supersede)
                for i in range(0, len(ids), 1000):
                    part = ids[i:i + 1000]
                    cur.execute("UPDATE match_candidates SET status = 'superseded', decided_at = %s, decided_by = 'system:record_changed' WHERE tenant_id = %s AND domain = %s "
                                "AND status = 'pending' AND (record_a_id = ANY(%s) OR record_b_id = ANY(%s))", (now, tenant_id, domain, part, part))
            if derived:
                cur.executemany("DELETE FROM blocking_keys WHERE tenant_id = %s AND domain = %s AND record_id = %s", [(t, d, rid) for t, d, rid, *_ in derived])
                cur.executemany("INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (%s, %s, %s, %s)",
                                [(t, d, rid, k) for t, d, rid, _m, _c, _f, ks in derived for k in ks])
                cur.executemany("INSERT INTO standardized_records (tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                                "ON CONFLICT (tenant_id, domain, record_id) DO UPDATE SET matching = EXCLUDED.matching, cleansed = EXCLUDED.cleansed, "
                                "fingerprint = EXCLUDED.fingerprint, standardized_at = EXCLUDED.standardized_at",
                                [(t, d, rid, _j(m), _j(c), fp, now) for t, d, rid, m, c, fp, _ks in derived])
            if candidates:
                cur.executemany("INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at, "
                                "decided_at, decided_by, contributions) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                [(c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type, _j(c.reasons), c.status, c.created_at,
                                  c.decided_at, c.decided_by, _j(c.contributions)) for c in candidates])
            for loser, survivor in reassign:
                cur.execute("UPDATE source_records SET entity_id = %s WHERE tenant_id = %s AND domain = %s AND entity_id = %s", (survivor, tenant_id, domain, loser))
                cur.execute(f"UPDATE {table} SET status = 'merged' WHERE tenant_id = %s AND entity_id = %s", (tenant_id, loser))
            if rows:
                cur.executemany(f"INSERT INTO {table} ({', '.join(golden.q(c) for c in names)}) VALUES ({', '.join(['%s'] * len(names))}) "
                                f"ON CONFLICT (entity_id) DO UPDATE SET {updates}", rows)
            if lineage:
                cur.executemany("INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, source_record_id, reason, changed_at) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", [(ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                                                                              ev.source_record_id, ev.reason, ev.changed_at) for ev in lineage])

    def blocking_keys_of(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids, out = list(record_ids), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute("SELECT record_id, blocking_key FROM blocking_keys WHERE tenant_id = %s AND domain = %s AND record_id = ANY(%s)", (tenant_id, domain, ids[i:i + 500]))
                for r in cur.fetchall():
                    out.setdefault(r["record_id"], []).append(r["blocking_key"])
        return out

    def get_matching_many(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids, out = list(record_ids), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute("SELECT record_id, matching FROM standardized_records WHERE tenant_id = %s AND domain = %s AND record_id = ANY(%s)", (tenant_id, domain, ids[i:i + 500]))
                out.update({r["record_id"]: r["matching"] for r in cur.fetchall()})
        return out

    # --- label hygiene and merge audits (same shape as the SQLite versions; never run against a live server) -------------------------
    AUDIT_VERDICTS = ("correct", "wrong")

    @staticmethod
    def _candidate_from_row(r) -> MatchCandidate:
        return MatchCandidate(id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], record_a_id=r["record_a_id"], record_b_id=r["record_b_id"], score=r["score"],
                              match_type=r["match_type"], reasons=r["reasons"], contributions=r["contributions"] or [], status=r["status"],
                              created_at=r["created_at"], decided_at=r["decided_at"], decided_by=r["decided_by"])

    def flag_merge_audits(self, tenant_id: str, domain: str, candidate_ids) -> int:
        ids = list(candidate_ids)
        if not ids:
            return 0
        flagged, now = 0, now_iso()
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 1000):
                cur.execute("SELECT id, decided_by FROM match_candidates WHERE tenant_id = %s AND domain = %s AND id = ANY(%s)", (tenant_id, domain, ids[i:i + 1000]))
                mine = [(r["id"], r["decided_by"]) for r in cur.fetchall()]
                for c, by in mine:
                    cur.execute("INSERT INTO merge_audits (candidate_id, tenant_id, domain, sampled_at, made_by) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (candidate_id) DO NOTHING",
                                (c, tenant_id, domain, now, "promoted" if by == "system:learned_scorer" else "rule"))
                    flagged += cur.rowcount
        return flagged

    def sample_merge_audit(self, tenant_id: str, domain: str, rate: float, seed=None) -> int:
        import random
        if not 0 < rate <= 1:
            raise ValueError("rate must be above 0 and at most 1")
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM match_candidates mc WHERE tenant_id = %s AND domain = %s AND status = 'confirmed' AND decided_by LIKE 'system:%%' "
                        "AND NOT EXISTS (SELECT 1 FROM merge_audits a WHERE a.candidate_id = mc.id) ORDER BY id", (tenant_id, domain))
            ids = [r["id"] for r in cur.fetchall()]
        rng = random.Random(seed)
        return self.flag_merge_audits(tenant_id, domain, [i for i in ids if rng.random() < rate])

    def record_merge_audit(self, candidate_id: str, tenant_id: str, domain: str, verdict: str, audited_by: str) -> bool:
        if verdict not in self.AUDIT_VERDICTS:
            raise ValueError(f"verdict must be one of {self.AUDIT_VERDICTS}")
        if not audited_by or audited_by.startswith("system:"):
            raise ValueError("a verdict must come from a person, not from the system")
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("UPDATE merge_audits SET verdict = %s, audited_by = %s, audited_at = %s WHERE candidate_id = %s AND tenant_id = %s AND domain = %s",
                        (verdict, audited_by, now_iso(), candidate_id, tenant_id, domain))
            return cur.rowcount == 1

    def list_merge_audits(self, tenant_id: str, domain: str, pending_only: bool = False) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT a.candidate_id, a.sampled_at, a.verdict, a.audited_by, a.audited_at, COALESCE(a.made_by, 'rule') AS made_by, mc.score, mc.record_a_id, mc.record_b_id, mc.decided_by FROM merge_audits a "
                        "JOIN match_candidates mc ON mc.id = a.candidate_id WHERE a.tenant_id = %s AND a.domain = %s" + (" AND a.verdict IS NULL" if pending_only else "")
                        + " ORDER BY a.sampled_at, a.candidate_id", (tenant_id, domain))
            return [dict(r) for r in cur.fetchall()]

    def human_labeled_candidates(self, tenant_id: str, domain: str) -> list:
        out, seen = [], set()
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT mc.*, CASE a.verdict WHEN 'correct' THEN 1 ELSE 0 END AS label FROM merge_audits a JOIN match_candidates mc ON mc.id = a.candidate_id "
                        "WHERE a.tenant_id = %s AND a.domain = %s AND a.verdict IS NOT NULL AND (mc.decided_by IS NULL OR mc.decided_by NOT LIKE 'unmerge:%%') ORDER BY mc.id", (tenant_id, domain))
            for r in cur.fetchall():
                out.append((self._candidate_from_row(r), r["label"], "audit"))
                seen.add(r["id"])
            cur.execute("SELECT mc.*, CASE mc.status WHEN 'confirmed' THEN 1 ELSE 0 END AS label FROM match_candidates mc WHERE mc.tenant_id = %s AND mc.domain = %s "
                        "AND mc.status IN ('confirmed', 'rejected') AND mc.decided_by IS NOT NULL AND mc.decided_by NOT LIKE 'system:%%' ORDER BY mc.id", (tenant_id, domain))
            for r in cur.fetchall():
                if r["id"] not in seen:
                    out.append((self._candidate_from_row(r), r["label"], "steward"))
        return out

    def count_auto_merges(self, tenant_id: str, domain: str) -> dict:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT CASE WHEN decided_by = 'system:learned_scorer' THEN 'promoted' ELSE 'rule' END AS stratum, COUNT(*) AS n FROM match_candidates "
                        "WHERE tenant_id = %s AND domain = %s AND status = 'confirmed' AND decided_by LIKE 'system:%%' GROUP BY stratum", (tenant_id, domain))
            counts = {"rule": 0, "promoted": 0}
            counts.update({r["stratum"]: r["n"] for r in cur.fetchall()})
        return counts

    # --- unmerge (same shape as the SQLite versions; never run against a live server) ----------------------------------------
    def confirmed_pairs_among(self, tenant_id: str, domain: str, record_ids) -> list:
        ids, out, listed = set(record_ids), [], list(set(record_ids))
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(listed), 500):
                chunk = listed[i:i + 500]
                cur.execute("SELECT id, record_a_id, record_b_id FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'confirmed' AND record_a_id = ANY(%s)", (tenant_id, domain, chunk))
                out.extend((r["id"], r["record_a_id"], r["record_b_id"]) for r in cur.fetchall() if r["record_b_id"] in ids)
        return out

    def has_separations(self, tenant_id: str, domain: str) -> bool:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'rejected' AND decided_by LIKE 'unmerge:%%' LIMIT 1", (tenant_id, domain))
            return cur.fetchone() is not None

    def separated_partners(self, tenant_id: str, domain: str, record_id: str) -> set:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT record_a_id, record_b_id FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'rejected' AND decided_by LIKE 'unmerge:%%' "
                        "AND (record_a_id = %s OR record_b_id = %s)", (tenant_id, domain, record_id, record_id))
            rows = cur.fetchall()
        return {r["record_b_id"] if r["record_a_id"] == record_id else r["record_a_id"] for r in rows}

    def entities_separated(self, tenant_id: str, domain: str, entity_a: str, entity_b: str):
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT mc.id FROM match_candidates mc JOIN source_records ra ON ra.id = mc.record_a_id AND ra.tenant_id = mc.tenant_id AND ra.domain = mc.domain "
                        "JOIN source_records rb ON rb.id = mc.record_b_id AND rb.tenant_id = mc.tenant_id AND rb.domain = mc.domain "
                        "WHERE mc.tenant_id = %s AND mc.domain = %s AND mc.status = 'rejected' AND mc.decided_by LIKE 'unmerge:%%' "
                        "AND ((ra.entity_id = %s AND rb.entity_id = %s) OR (ra.entity_id = %s AND rb.entity_id = %s)) LIMIT 1", (tenant_id, domain, entity_a, entity_b, entity_b, entity_a))
            row = cur.fetchone()
        return row["id"] if row else None

    def separation_violations(self, tenant_id: str, domain: str) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT mc.id, mc.record_a_id, mc.record_b_id, ra.entity_id FROM match_candidates mc JOIN source_records ra ON ra.id = mc.record_a_id AND ra.tenant_id = mc.tenant_id "
                        "AND ra.domain = mc.domain JOIN source_records rb ON rb.id = mc.record_b_id AND rb.tenant_id = mc.tenant_id AND rb.domain = mc.domain "
                        "WHERE mc.tenant_id = %s AND mc.domain = %s AND mc.status = 'rejected' AND mc.decided_by LIKE 'unmerge:%%' "
                        "AND ra.entity_id IS NOT NULL AND ra.entity_id = rb.entity_id", (tenant_id, domain))
            rows = cur.fetchall()
        return [(r["id"], r["record_a_id"], r["record_b_id"], r["entity_id"]) for r in rows]

    def separation_pairs(self, tenant_id: str, domain: str) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT record_a_id, record_b_id FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'rejected' AND decided_by LIKE 'unmerge:%%'", (tenant_id, domain))
            rows = cur.fetchall()
        return [(r["record_a_id"], r["record_b_id"]) for r in rows]

    # --- the audit page and the monitor, computed in SQL (same shape as the SQLite versions; never run against a live server) ---
    def audit_counts(self, tenant_id: str, domain: str) -> dict:
        out = {"rule": {"audited": 0, "wrong": 0}, "promoted": {"audited": 0, "wrong": 0}, "waiting": 0}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT COALESCE(made_by, 'rule') AS kind, COUNT(*) FILTER (WHERE verdict IS NOT NULL) AS audited, COUNT(*) FILTER (WHERE verdict = 'wrong') AS wrong, "
                        "COUNT(*) FILTER (WHERE verdict IS NULL) AS waiting FROM merge_audits WHERE tenant_id = %s AND domain = %s GROUP BY kind", (tenant_id, domain))
            for r in cur.fetchall():
                out[r["kind"]]["audited"], out[r["kind"]]["wrong"] = r["audited"], r["wrong"]
                out["waiting"] += r["waiting"]
        return out

    def audited_wrong(self, tenant_id: str, domain: str) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT candidate_id, COALESCE(made_by, 'rule') AS kind FROM merge_audits WHERE tenant_id = %s AND domain = %s AND verdict = 'wrong' ORDER BY audited_at, candidate_id", (tenant_id, domain))
            return [(r["candidate_id"], r["kind"]) for r in cur.fetchall()]

    def pending_audits(self, tenant_id: str, domain: str, limit: int) -> list:
        out = []
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for kind_clause in ("a.made_by = 'promoted'", "(a.made_by = 'rule' OR a.made_by IS NULL)"):
                if len(out) >= limit:
                    break
                cur.execute("SELECT a.candidate_id, a.sampled_at, COALESCE(a.made_by, 'rule') AS made_by FROM merge_audits a JOIN match_candidates mc ON mc.id = a.candidate_id "
                            f"WHERE a.tenant_id = %s AND a.domain = %s AND a.verdict IS NULL AND mc.status = 'confirmed' AND {kind_clause} "
                            "ORDER BY a.sampled_at, a.candidate_id LIMIT %s", (tenant_id, domain, limit - len(out)))
                out.extend(dict(r) for r in cur.fetchall())
        return out

    # --- pages of the two lists a person scrolls (same shape as the SQLite versions; never run against a live server) ---------------
    def pending_page(self, tenant_id: str, domain: str, limit: int, after=None) -> list:
        sql, params = "SELECT * FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'pending'", [tenant_id, domain]
        if after is not None:
            sql, params = sql + " AND (score, id) < (%s, %s)", params + [after[0], after[1]]
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql + " ORDER BY score DESC, id DESC LIMIT %s", (*params, limit))
            return [self._candidate_from_row(r) for r in cur.fetchall()]

    def count_pending(self, tenant_id: str, domain: str) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'pending'", (tenant_id, domain))
            return cur.fetchone()["n"]

    # --- agent recommendations (see the SQLite store) ---
    @staticmethod
    def _recommendation_from_row(r) -> Recommendation:
        return Recommendation(candidate_id=r["candidate_id"], recommendation=r["recommendation"], confidence=r["confidence"], agent=r["agent"], reasons=json.loads(r["reasons"]),
                              evidence=json.loads(r["evidence"]), model=r["model"], prompt_version=r["prompt_version"], id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"],
                              created_at=r["created_at"], outcome=r["outcome"], outcome_decision=r["outcome_decision"], outcome_by=r["outcome_by"], outcome_at=r["outcome_at"],
                              outcome_seen=None if r["outcome_seen"] is None else bool(r["outcome_seen"]))

    def save_recommendation(self, rec: Recommendation) -> Recommendation:
        sql = get_dialect("postgres").upsert("agent_recommendations", ["id", "tenant_id", "domain", "candidate_id", "recommendation", "confidence", "reasons", "evidence", "agent", "model", "prompt_version", "created_at"], key=["tenant_id", "domain", "candidate_id", "agent", "model", "prompt_version"],
                                             update=["recommendation", "confidence", "reasons", "evidence", "created_at"])
        with self._conn(tenant_id=rec.tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql, (rec.id, rec.tenant_id, rec.domain, rec.candidate_id, rec.recommendation, rec.confidence, json.dumps(rec.reasons), json.dumps(rec.evidence), rec.agent,
                              rec.model, rec.prompt_version, rec.created_at))
            cur.execute("SELECT * FROM agent_recommendations WHERE tenant_id = %s AND domain = %s AND candidate_id = %s AND agent = %s AND model = %s AND prompt_version = %s",
                        (rec.tenant_id, rec.domain, rec.candidate_id, rec.agent, rec.model, rec.prompt_version))
            return self._recommendation_from_row(cur.fetchone())

    def list_recommendations_for(self, tenant_id: str, domain: str, candidate_id: str) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM agent_recommendations WHERE tenant_id = %s AND domain = %s AND candidate_id = %s ORDER BY created_at DESC, id DESC", (tenant_id, domain, candidate_id))
            return [self._recommendation_from_row(r) for r in cur.fetchall()]

    def recommendations_page(self, tenant_id: str, domain: str, limit: int, after=None, outcome: str | None = None) -> list:
        sql, params = "SELECT * FROM agent_recommendations WHERE tenant_id = %s AND domain = %s", [tenant_id, domain]
        if outcome == "open":
            sql += " AND outcome IS NULL"
        elif outcome:
            sql, params = sql + " AND outcome = %s", params + [outcome]
        if after is not None:
            sql, params = sql + " AND " + get_dialect("postgres").keyset_after(["created_at", "id"], descending=True), params + [after[0], after[1]]
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql + " ORDER BY created_at DESC, id DESC LIMIT %s", (*params, limit))
            return [self._recommendation_from_row(r) for r in cur.fetchall()]

    def record_recommendation_outcomes(self, tenant_id: str, domain: str, candidate_id: str, decision: str, decided_by: str, decided_at: str, suggestion_viewed: bool | None = None) -> int:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("UPDATE agent_recommendations SET outcome = CASE WHEN recommendation = 'needs_human' THEN 'human_decided' WHEN recommendation = %s THEN 'agreed' ELSE 'disagreed' END, "
                        "outcome_decision = %s, outcome_by = %s, outcome_at = %s, outcome_seen = %s WHERE tenant_id = %s AND domain = %s AND candidate_id = %s AND outcome IS NULL",
                        (decision, decision, decided_by, decided_at, None if suggestion_viewed is None else int(bool(suggestion_viewed)), tenant_id, domain, candidate_id))
            return cur.rowcount

    def recommendation_counts(self, tenant_id: str, domain: str, limit: int = 20000) -> list:
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute("SELECT recommendation, outcome, confidence, outcome_seen, COUNT(*) AS n FROM agent_recommendations WHERE tenant_id = %s AND domain = %s "
                        "GROUP BY recommendation, outcome, confidence, outcome_seen LIMIT %s", (tenant_id, domain, limit))
            return [(r["recommendation"], r["outcome"], r["confidence"], None if r["outcome_seen"] is None else bool(r["outcome_seen"]), r["n"]) for r in cur.fetchall()]

    def delete_recommendations(self, tenant_id: str, domain: str, candidate_ids) -> int:
        d, n = get_dialect("postgres"), 0
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for chunk in d.chunks(list(candidate_ids)):
                cond, params = d.in_list("candidate_id", chunk)
                cur.execute(f"DELETE FROM agent_recommendations WHERE tenant_id = %s AND domain = %s AND {cond}", (tenant_id, domain, *params))
                n += cur.rowcount
        return n

    def get_source_records_batch(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids, out = list(dict.fromkeys(record_ids)), {}
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            for i in range(0, len(ids), 500):
                cur.execute("SELECT * FROM source_records WHERE tenant_id = %s AND domain = %s AND id = ANY(%s)", (tenant_id, domain, ids[i:i + 500]))
                for r in cur.fetchall():
                    out[r["id"]] = self._row_to_source_record(r)
        return out

    def entities_page(self, tenant_id: str, domain: str, limit: int, after_id: str | None = None, status: str | None = None) -> list:
        self._ensure_entity_table(domain)
        sql, params = f"SELECT entity_id FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = %s", [tenant_id]
        if after_id is not None:
            sql, params = sql + " AND entity_id > %s", params + [after_id]
        if status:
            sql, params = sql + " AND status = %s", params + [status]
        with self._conn(tenant_id=tenant_id) as conn, conn.cursor() as cur:
            cur.execute(sql + " ORDER BY entity_id LIMIT %s", (*params, limit))
            ids = [r["entity_id"] for r in cur.fetchall()]
        entities = self.get_entities_batch(tenant_id, domain, ids)
        sources: dict = {}
        for rec in self.list_records_of_entities(tenant_id, domain, ids):
            sources.setdefault(rec.entity_id, []).append(rec.id)
        page = []
        for entity_id in ids:
            if entity_id in entities:
                entities[entity_id].source_record_ids = sources.get(entity_id, [])
                page.append(entities[entity_id])
        return page
