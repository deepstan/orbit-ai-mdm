"""
Persistence layer, scoped by BOTH tenant_id and domain.

tenant_id: which customer of the MDM SaaS this data belongs to.
domain:    which kind of entity it is (customer, product, ...) --
           records of different domains must never be compared or merged,
           exactly like records of different tenants must never be.

Still SQLite via stdlib `sqlite3` -- see the Phase 2 README section on
what's tested here vs. what needs a real Postgres instance to validate.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

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
    score REAL NOT NULL,
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


class Store:
    def __init__(self, path: str = "mdm.db"):
        """Open (or create) the SQLite database and make sure the schema exists.

        Creates the parent folder of ``path`` if needed, then runs the
        ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS`` script,
        so it is safe to call on an existing database.

        Args:
            path: File path of the SQLite database. Defaults to ``"mdm.db"`` in
                the current working directory.
        """
        self.path = path
        parent = Path(path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        """Context manager that yields a short-lived SQLite connection.

        A new connection is opened for every call, rows come back as
        ``sqlite3.Row`` (so columns can be read by name), the transaction is
        committed if the ``with`` block finishes without an error, and the
        connection is always closed afterwards. If the block raises, nothing is
        committed.

        Yields:
            sqlite3.Connection: The open connection.
        """
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self):
        """Create all tables and indexes if they don't already exist.

        Runs the module-level ``SCHEMA`` script. Called once from ``__init__``.
        """
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    # --- source records -----------------------------------------------
    def save_source_record(self, rec: SourceRecord) -> None:
        """Insert one raw source record.

        The ``attributes`` dict is stored as a JSON string. This is a plain
        INSERT, so saving a record whose ``id`` already exists raises
        ``sqlite3.IntegrityError``.

        Args:
            rec: The record to store. Its own ``tenant_id`` and ``domain`` decide
                where it lives.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, "
                "attributes, ingested_at, entity_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (rec.id, rec.tenant_id, rec.domain, rec.source_system, rec.source_record_id,
                 json.dumps(rec.attributes), rec.ingested_at, rec.entity_id),
            )

    def get_source_record(self, rec_id: str, tenant_id: str, domain: str) -> SourceRecord | None:
        """Fetch one source record by ID.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            rec_id: The record's ``id`` (e.g. ``"rec_..."``).
            tenant_id: Tenant that must own the record.
            domain: Domain the record must belong to.

        Returns:
            SourceRecord | None: The record, or ``None`` if it doesn't exist in
            this tenant and domain.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM source_records WHERE id = ? AND tenant_id = ? AND domain = ?",
                (rec_id, tenant_id, domain),
            ).fetchone()
            return self._row_to_source_record(row) if row else None

    def list_source_records(self, tenant_id: str, domain: str, entity_id: str | None = None) -> list[SourceRecord]:
        """List source records for a tenant and domain.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            tenant_id: Tenant to list records for.
            domain: Domain to list records for.
            entity_id: If given, return only the records clustered into this
                golden record. If ``None``, return every record in scope.

        Returns:
            list[SourceRecord]: Matching records, in no guaranteed order.
        """
        with self._conn() as conn:
            if entity_id:
                rows = conn.execute(
                    "SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND entity_id = ?",
                    (tenant_id, domain, entity_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM source_records WHERE tenant_id = ? AND domain = ?", (tenant_id, domain)
                ).fetchall()
            return [self._row_to_source_record(r) for r in rows]

    def set_record_entity(self, record_id: str, tenant_id: str, domain: str, entity_id: str) -> None:
        """Link a source record to the golden record (entity) it was clustered into.

        Args:
            record_id: The source record to update.
            tenant_id: Tenant that owns the record.
            domain: Domain of the record.
            entity_id: The golden record's ``id``.

        Does nothing if no record matches the ID, tenant and domain.
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE source_records SET entity_id = ? WHERE id = ? AND tenant_id = ? AND domain = ?",
                (entity_id, record_id, tenant_id, domain),
            )

    def delete_source_record(self, record_id: str, tenant_id: str, domain: str) -> None:
        """Hard delete -- used only by the right-to-be-forgotten purge
        workflow (app/compliance.py). Normal operations never delete a
        source record; this bypasses lineage/history by design, since
        erasure requests are explicitly about removing data, not
        recording its removal in the normal audit trail (the purge
        itself is separately logged to audit_log).

        Args:
            record_id: The source record to delete.
            tenant_id: Tenant that owns the record.
            domain: Domain of the record.
        """
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM source_records WHERE id = ? AND tenant_id = ? AND domain = ?",
                (record_id, tenant_id, domain),
            )

    @staticmethod
    def _row_to_source_record(row) -> SourceRecord:
        """Convert a ``source_records`` row into a ``SourceRecord`` object.

        Decodes the JSON ``attributes`` column back into a dict.

        Args:
            row: A ``sqlite3.Row`` from the ``source_records`` table.

        Returns:
            SourceRecord: The rebuilt record.
        """
        return SourceRecord(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"],
            source_system=row["source_system"], source_record_id=row["source_record_id"],
            attributes=json.loads(row["attributes"]),
            ingested_at=row["ingested_at"], entity_id=row["entity_id"],
        )

    # --- entities --------------------------------------------------------
    def save_entity(self, ent: Entity) -> None:
        """Insert or overwrite a golden record (entity).

        Uses ``INSERT OR REPLACE``, so this is both "create" and "update".
        ``attributes`` is stored as JSON. ``source_record_ids`` is NOT stored
        here: which records belong to an entity is kept on the source records
        themselves (see ``set_record_entity``).

        Args:
            ent: The entity to save.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO entities (id, tenant_id, domain, status, attributes, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ent.id, ent.tenant_id, ent.domain, ent.status, json.dumps(ent.attributes),
                 ent.created_at, ent.updated_at),
            )

    def get_entity(self, entity_id: str, tenant_id: str, domain: str) -> Entity | None:
        """Fetch one golden record by ID, including its source record IDs.

        ``source_record_ids`` is rebuilt by looking up every source record whose
        ``entity_id`` points at this entity.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            entity_id: The entity's ``id`` (e.g. ``"ent_..."``).
            tenant_id: Tenant that must own the entity.
            domain: Domain the entity must belong to.

        Returns:
            Entity | None: The entity, or ``None`` if it isn't found in scope.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM entities WHERE id = ? AND tenant_id = ? AND domain = ?",
                (entity_id, tenant_id, domain),
            ).fetchone()
            if not row:
                return None
            source_ids = [r.id for r in self.list_source_records(tenant_id, domain, entity_id=entity_id)]
            return Entity(
                id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"], status=row["status"],
                attributes=json.loads(row["attributes"]), source_record_ids=source_ids,
                created_at=row["created_at"], updated_at=row["updated_at"],
            )

    def list_entities(self, tenant_id: str, domain: str, status: str | None = None) -> list[Entity]:
        """List golden records for a tenant and domain.

        Each entity is loaded with ``get_entity``, which means one extra query
        per entity (plus one for its source records). Fine for small datasets,
        slow for large ones.

        Args:
            tenant_id: Tenant to list entities for.
            domain: Domain to list entities for.
            status: If given, only entities with this status (e.g. ``"active"``,
                ``"merged"``). If ``None``, all statuses.

        Returns:
            list[Entity]: The matching entities.
        """
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT id FROM entities WHERE tenant_id = ? AND domain = ? AND status = ?",
                    (tenant_id, domain, status),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id FROM entities WHERE tenant_id = ? AND domain = ?", (tenant_id, domain)
                ).fetchall()
        return [self.get_entity(r["id"], tenant_id, domain) for r in rows]

    def delete_entity(self, entity_id: str, tenant_id: str, domain: str) -> None:
        """Hard delete -- right-to-be-forgotten purge only (app/compliance.py).

        Args:
            entity_id: The entity to delete.
            tenant_id: Tenant that owns the entity.
            domain: Domain of the entity.

        Source records that pointed at this entity are not changed here; the
        purge workflow deletes those separately.
        """
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM entities WHERE id = ? AND tenant_id = ? AND domain = ?",
                (entity_id, tenant_id, domain),
            )

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        """Insert a new match candidate (a proposed pair awaiting review).

        ``reasons`` is stored as a JSON list. Plain INSERT, so a duplicate
        ``id`` raises ``sqlite3.IntegrityError``.

        Args:
            c: The candidate to store.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, "
                "match_type, reasons, status, created_at, decided_at, decided_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 json.dumps(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by),
            )

    def update_candidate_status(self, cand_id: str, tenant_id: str, domain: str,
                                 status: str, decided_by: str, decided_at: str) -> None:
        """Record a decision on a match candidate.

        Args:
            cand_id: The candidate's ``id``.
            tenant_id: Tenant that owns the candidate.
            domain: Domain of the candidate.
            status: New status, normally ``"confirmed"`` or ``"rejected"``.
            decided_by: Who decided (e.g. an API key label, or ``"system"``).
            decided_at: ISO timestamp of the decision.

        Does nothing if no candidate matches the ID, tenant and domain.
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE match_candidates SET status = ?, decided_by = ?, decided_at = ? "
                "WHERE id = ? AND tenant_id = ? AND domain = ?",
                (status, decided_by, decided_at, cand_id, tenant_id, domain),
            )

    def list_candidates(self, tenant_id: str, domain: str, status: str | None = "pending") -> list[MatchCandidate]:
        """List match candidates, highest score first.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            tenant_id: Tenant to list candidates for.
            domain: Domain to list candidates for.
            status: Only candidates with this status. Defaults to ``"pending"``
                (the review queue). Pass ``None`` to get every status.

        Returns:
            list[MatchCandidate]: Candidates sorted by ``score`` descending.
        """
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = ? "
                    "ORDER BY score DESC",
                    (tenant_id, domain, status),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = ? AND domain = ? ORDER BY score DESC",
                    (tenant_id, domain),
                ).fetchall()
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
        """Fetch one match candidate by ID.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            cand_id: The candidate's ``id`` (e.g. ``"cand_..."``).
            tenant_id: Tenant that must own it.
            domain: Domain it must belong to.

        Returns:
            MatchCandidate | None: The candidate, or ``None`` if not found in scope.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM match_candidates WHERE id = ? AND tenant_id = ? AND domain = ?",
                (cand_id, tenant_id, domain),
            ).fetchone()
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
        """Append one lineage event (a record of a field changing on an entity).

        Lineage is append-only: events are never updated or deleted in normal
        operation, so an entity's full history can be replayed.

        Args:
            ev: The lineage event to store.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                "source_record_id, reason, changed_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                 ev.source_record_id, ev.reason, ev.changed_at),
            )

    def list_lineage(self, entity_id: str, tenant_id: str, domain: str) -> list[LineageEvent]:
        """Return the change history of one entity, oldest first.

        Every query is filtered by both ``tenant_id`` and ``domain``, so data
        from another tenant or another domain is never returned or changed.

        Args:
            entity_id: The entity whose history to fetch.
            tenant_id: Tenant that owns the entity.
            domain: Domain of the entity.

        Returns:
            list[LineageEvent]: Events ordered by ``changed_at`` ascending.
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM lineage WHERE entity_id = ? AND tenant_id = ? AND domain = ? ORDER BY changed_at",
                (entity_id, tenant_id, domain),
            ).fetchall()
        return [
            LineageEvent(
                id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], entity_id=r["entity_id"],
                field=r["field"], old_value=r["old_value"], new_value=r["new_value"],
                source_record_id=r["source_record_id"], reason=r["reason"], changed_at=r["changed_at"],
            )
            for r in rows
        ]

    # --- API keys (RBAC / auth, see app/auth.py) --------------------------
    def save_api_key(self, key: ApiKeyRecord) -> None:
        """Store a new API key record (hash only, never the plaintext key).

        ``allowed_domains`` is stored as a JSON list, or NULL meaning "all
        domains". ``key_hash`` is UNIQUE, so saving the same hash twice raises
        ``sqlite3.IntegrityError``.

        Args:
            key: The key record to store.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO api_keys (id, tenant_id, role, key_hash, label, allowed_domains, "
                "created_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key.id, key.tenant_id, key.role, key.key_hash, key.label,
                 json.dumps(key.allowed_domains) if key.allowed_domains is not None else None,
                 key.created_at, key.revoked_at),
            )

    def get_api_key_by_hash(self, key_hash: str) -> ApiKeyRecord | None:
        """Look up an API key by its hash.

        Used on every authenticated request (see ``app/auth.py``). This lookup is
        deliberately NOT tenant-scoped: the key itself is what tells us which
        tenant the caller belongs to. Revoked keys are returned too; the caller
        must check ``revoked_at``.

        Args:
            key_hash: SHA-256 hex digest of the presented key.

        Returns:
            ApiKeyRecord | None: The key record, or ``None`` if unknown.
        """
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM api_keys WHERE key_hash = ?", (key_hash,)).fetchone()
            return self._row_to_api_key(row) if row else None

    def list_api_keys(self, tenant_id: str) -> list[ApiKeyRecord]:
        """List all API keys for a tenant, including revoked ones.

        Args:
            tenant_id: Tenant whose keys to list.

        Returns:
            list[ApiKeyRecord]: The tenant's key records (hashes only).
        """
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM api_keys WHERE tenant_id = ?", (tenant_id,)).fetchall()
        return [self._row_to_api_key(r) for r in rows]

    def revoke_api_key(self, key_id: str, tenant_id: str, revoked_at: str) -> None:
        """Mark an API key as revoked.

        The row is kept (for audit purposes); setting ``revoked_at`` is what
        makes authentication reject it.

        Args:
            key_id: The key's ``id`` (e.g. ``"key_..."``).
            tenant_id: Tenant that must own the key, so one tenant can't revoke
                another tenant's key.
            revoked_at: ISO timestamp of the revocation.
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND tenant_id = ?",
                (revoked_at, key_id, tenant_id),
            )

    @staticmethod
    def _row_to_api_key(row) -> ApiKeyRecord:
        """Convert an ``api_keys`` row into an ``ApiKeyRecord`` object.

        Decodes ``allowed_domains`` from JSON; NULL (or empty) becomes ``None``,
        meaning "all domains".

        Args:
            row: A ``sqlite3.Row`` from the ``api_keys`` table.

        Returns:
            ApiKeyRecord: The rebuilt key record.
        """
        return ApiKeyRecord(
            id=row["id"], tenant_id=row["tenant_id"], role=row["role"], key_hash=row["key_hash"],
            label=row["label"],
            allowed_domains=json.loads(row["allowed_domains"]) if row["allowed_domains"] else None,
            created_at=row["created_at"], revoked_at=row["revoked_at"],
        )

    # --- Audit log (immutable, hash-chained -- see app/audit.py) ----------
    def append_audit_entry(self, entry: AuditLogEntry) -> None:
        """Append one entry to the tenant's hash-chained audit log.

        The entry's hashes must already be computed (see ``app/audit.py``). This
        method only inserts; audit entries are never updated or deleted.

        Args:
            entry: The audit entry to store.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO audit_log (id, tenant_id, domain, action, actor, details, prev_hash, "
                "entry_hash, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (entry.id, entry.tenant_id, entry.domain, entry.action, entry.actor,
                 json.dumps(entry.details), entry.prev_hash, entry.entry_hash, entry.created_at),
            )

    def get_last_audit_entry(self, tenant_id: str) -> AuditLogEntry | None:
        """Most recent entry for a tenant -- needed to chain the next
        entry's prev_hash. Returns None if this tenant has no audit
        history yet (the chain's genesis case).

        Args:
            tenant_id: Tenant whose latest entry to fetch.

        Returns:
            AuditLogEntry | None: The newest entry, or ``None`` for a new tenant.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (tenant_id,),
            ).fetchone()
            return self._row_to_audit_entry(row) if row else None

    def list_audit_log(self, tenant_id: str, domain: str | None = None, limit: int = 200) -> list[AuditLogEntry]:
        """List a tenant's audit entries, oldest first.

        Args:
            tenant_id: Tenant whose log to read.
            domain: If given, only entries for this domain. If ``None``, all
                entries for the tenant.
            limit: Maximum number of entries to return. Defaults to 200. Because
                the order is oldest first, the limit keeps the OLDEST entries.

        Returns:
            list[AuditLogEntry]: Entries ordered by ``created_at`` ascending.
        """
        with self._conn() as conn:
            if domain:
                rows = conn.execute(
                    "SELECT * FROM audit_log WHERE tenant_id = ? AND domain = ? "
                    "ORDER BY created_at ASC LIMIT ?",
                    (tenant_id, domain, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY created_at ASC LIMIT ?",
                    (tenant_id, limit),
                ).fetchall()
        return [self._row_to_audit_entry(r) for r in rows]

    @staticmethod
    def _row_to_audit_entry(row) -> AuditLogEntry:
        """Convert an ``audit_log`` row into an ``AuditLogEntry`` object.

        Args:
            row: A ``sqlite3.Row`` from the ``audit_log`` table.

        Returns:
            AuditLogEntry: The rebuilt entry, with ``details`` decoded from JSON.
        """
        return AuditLogEntry(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"], action=row["action"],
            actor=row["actor"], details=json.loads(row["details"]),
            prev_hash=row["prev_hash"], entry_hash=row["entry_hash"], created_at=row["created_at"],
        )

    # --- Usage events (billing metering -- see app/billing.py) ------------
    def record_usage_event(self, ev: UsageEvent) -> None:
        """Insert one billing usage event.

        Args:
            ev: The usage event to store. ``metadata`` is stored as JSON.
        """
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO usage_events (id, tenant_id, domain, event_type, quantity, metadata, "
                "created_at) VALUES (?,?,?,?,?,?,?)",
                (ev.id, ev.tenant_id, ev.domain, ev.event_type, ev.quantity,
                 json.dumps(ev.metadata), ev.created_at),
            )

    def list_usage_events(self, tenant_id: str, since: str | None = None) -> list[UsageEvent]:
        """List a tenant's usage events, oldest first.

        Args:
            tenant_id: Tenant whose usage to read.
            since: Optional ISO timestamp; only events created at or after it are
                returned. Compared as strings, which works because all timestamps
                come from ``now_iso()`` in the same format.

        Returns:
            list[UsageEvent]: Events ordered by ``created_at`` ascending.
        """
        with self._conn() as conn:
            if since:
                rows = conn.execute(
                    "SELECT * FROM usage_events WHERE tenant_id = ? AND created_at >= ? ORDER BY created_at",
                    (tenant_id, since),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM usage_events WHERE tenant_id = ? ORDER BY created_at", (tenant_id,)
                ).fetchall()
        return [
            UsageEvent(
                id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], event_type=r["event_type"],
                quantity=r["quantity"], metadata=json.loads(r["metadata"]), created_at=r["created_at"],
            )
            for r in rows
        ]
