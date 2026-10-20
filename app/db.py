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
import threading
from contextlib import contextmanager
from pathlib import Path

from . import fieldtypes, golden, protect
from .migrations import SQLiteMigrationBackend, apply_or_verify, auto_migrate_enabled
from .sql_dialect import get_dialect
from .models import ApiKeyRecord, AuditLogEntry, Entity, LineageEvent, MatchCandidate, Recommendation, SourceRecord, UsageEvent, now_iso



class Store:
    def __init__(self, path: str = "mdm.db", auto_migrate: bool | None = None):
        self.path = path
        self._auto_migrate = auto_migrate
        self._entity_synced: dict = {}
        self._tls = threading.local()      # the batch (unit of work) of THIS thread: one Store object is shared by every request thread
        parent = Path(path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._init_schema(auto_migrate)

    @contextmanager
    def _conn(self):
        shared = getattr(self._tls, "conn", None)
        if shared is not None:          # inside a batch: the batch commits (or rolls back), not every call
            yield shared
            return
        conn = sqlite3.connect(self.path, timeout=30)    # (a batch elsewhere may hold the write lock for a moment)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # --- units of work ------------------------------------------------
    @staticmethod
    def _tx(conn, *statements: str) -> None:
        """Transaction control (BEGIN, COMMIT, ROLLBACK, SAVEPOINT ...): standard SQL, and the ONLY place the units of work execute any (the portability ratchet counts it once)."""
        for statement in statements:
            conn.execute(statement)

    def in_batch(self) -> bool:
        return getattr(self._tls, "conn", None) is not None

    @contextmanager
    def batch(self, domain: str | None = None):
        """ONE transaction for everything the block writes, committed at the end (or rolled back whole if the block raises). Every store call inside it shares one connection, so it
        also costs one commit, not one per call. Reentrant: a batch inside a batch joins it. Per thread: other threads and other processes wait for the write lock until this one ends.
        `domain` makes sure that domain's golden table has been checked BEFORE the transaction opens: that check uses a connection of its own and would wait for this one for ever."""
        if domain is not None:
            self._ensure_entity_table(domain)
        tls = self._tls
        if getattr(tls, "conn", None) is not None:
            tls.depth += 1
            try:
                yield
            finally:
                tls.depth -= 1
            return
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)   # autocommit mode: BEGIN and COMMIT are explicit, so savepoints behave
        conn.row_factory = sqlite3.Row
        self._tx(conn, "BEGIN IMMEDIATE")
        tls.conn, tls.depth, tls.savepoints = conn, 1, 0
        try:
            yield
            self._tx(conn, "COMMIT")
        except BaseException:
            try:
                self._tx(conn, "ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            tls.conn, tls.depth = None, 0
            conn.close()

    @contextmanager
    def savepoint(self):
        """Inside a batch: if the block raises, only ITS writes are undone and the exception goes on; the batch carries on. Outside a batch: does nothing (as before)."""
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            yield
            return
        self._tls.savepoints += 1
        name = f"mdm_sp_{self._tls.savepoints}"
        self._tx(conn, f"SAVEPOINT {name}")
        try:
            yield
        except BaseException:
            self._tx(conn, f"ROLLBACK TO SAVEPOINT {name}", f"RELEASE SAVEPOINT {name}")
            raise
        else:
            self._tx(conn, f"RELEASE SAVEPOINT {name}")

    def _init_schema(self, auto_migrate: bool | None = None) -> None:
        """The schema is owned by app/migrations (versioned, upgradable).
        By default this brings the database up to date; with
        MDM_AUTO_MIGRATE=false (or auto_migrate=False) it only verifies
        that it already is, and refuses to start otherwise."""
        apply_or_verify(SQLiteMigrationBackend(self.path), auto_migrate)

    # --- source records -----------------------------------------------
    def save_source_record(self, rec: SourceRecord) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, "
                "attributes, ingested_at, entity_id, content_hash, version, updated_at, processing_status, processing_attempts, processing_error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (rec.id, rec.tenant_id, rec.domain, rec.source_system, rec.source_record_id,
                 json.dumps(rec.attributes), rec.ingested_at, rec.entity_id, rec.content_hash, rec.version, rec.updated_at,
                 rec.processing_status, rec.processing_attempts, rec.processing_error),
            )

    def save_source_records_batch(self, records: list[SourceRecord]) -> None:
        """Bulk write: inserts many records in ONE transaction/commit
        instead of one commit per record. This is the actual win for
        bulk loads -- each individual save_source_record() call opens
        and commits its own transaction, which is fine for one-off
        ingestion but adds up fast across thousands of rows. Used by
        pipeline.ingest_batch() -- see that function for why matching
        itself still has to happen per-record even though writing
        doesn't."""
        if not records:
            return
        with self._conn() as conn:
            conn.executemany(
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, "
                "attributes, ingested_at, entity_id, content_hash, version, updated_at, processing_status, processing_attempts, processing_error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(r.id, r.tenant_id, r.domain, r.source_system, r.source_record_id,
                  json.dumps(r.attributes), r.ingested_at, r.entity_id, r.content_hash, r.version, r.updated_at,
                  r.processing_status, r.processing_attempts, r.processing_error) for r in records],
            )

    def get_source_record(self, rec_id: str, tenant_id: str, domain: str) -> SourceRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM source_records WHERE id = ? AND tenant_id = ? AND domain = ?",
                (rec_id, tenant_id, domain),
            ).fetchone()
            return self._row_to_source_record(row) if row else None

    def list_source_records(self, tenant_id: str, domain: str, entity_id: str | None = None) -> list[SourceRecord]:
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
        with self._conn() as conn:
            conn.execute(
                "UPDATE source_records SET entity_id = ? WHERE id = ? AND tenant_id = ? AND domain = ?",
                (entity_id, record_id, tenant_id, domain),
            )

    # --- Blocking keys (indexed candidate lookup for matching) ------------
    # This is what turns ingest_record()'s candidate search from "fetch and
    # re-standardize every existing record for this (tenant, domain)" --
    # O(n) work per ingest, O(n^2) for a whole bulk load -- into an indexed
    # lookup that returns only records sharing at least one blocking key
    # with the new record. See app/pipeline.py for how it's used, and
    # app/matching.py's blocking_keys() for how the keys themselves are
    # computed (unchanged -- this table just indexes that function's output
    # instead of recomputing it from scratch on every call).
    def save_blocking_keys(self, tenant_id: str, domain: str, record_id: str, keys: set[str]) -> None:
        with self._conn() as conn:
            conn.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)",
                [(tenant_id, domain, record_id, k) for k in keys],
            )

    def save_blocking_keys_batch(self, entries: list[tuple[str, str, str, set[str]]]) -> None:
        """Bulk write: entries is [(tenant_id, domain, record_id, keys), ...]
        for many records at once, flattened into ONE executemany INSERT
        and ONE commit -- instead of one commit per record's key set."""
        rows = [
            (tenant_id, domain, record_id, key)
            for tenant_id, domain, record_id, keys in entries
            for key in keys
        ]
        if not rows:
            return
        with self._conn() as conn:
            conn.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)",
                rows,
            )

    def find_candidate_record_ids(self, tenant_id: str, domain: str, keys: set[str],
                                   exclude_record_id: str) -> set[str]:
        """Returns the IDs of every OTHER record (for this tenant+domain)
        that shares at least one blocking key with `keys`. This is the
        indexed replacement for pulling every record and re-blocking in
        Python -- the query below hits idx_blocking_keys_lookup directly."""
        if not keys:
            return set()
        placeholders = ",".join("?" for _ in keys)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT DISTINCT record_id FROM blocking_keys "
                f"WHERE tenant_id = ? AND domain = ? AND blocking_key IN ({placeholders}) "
                f"AND record_id != ?",
                (tenant_id, domain, *keys, exclude_record_id),
            ).fetchall()
        return {r["record_id"] for r in rows}

    def delete_blocking_keys(self, record_id: str, tenant_id: str, domain: str) -> None:
        """Called by the right-to-be-forgotten purge (app/compliance.py) --
        without this, a purged record's blocking keys would silently
        outlive the record itself, leaking indexed fragments of erased
        data (and wasting index space) even though the record and entity
        are gone."""
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM blocking_keys WHERE record_id = ? AND tenant_id = ? AND domain = ?",
                (record_id, tenant_id, domain),
            )

    def delete_source_record(self, record_id: str, tenant_id: str, domain: str) -> None:
        """Hard delete -- used only by the right-to-be-forgotten purge
        workflow (app/compliance.py). Normal operations never delete a
        source record; this bypasses lineage/history by design, since
        erasure requests are explicitly about removing data, not
        recording its removal in the normal audit trail (the purge
        itself is separately logged to audit_log)."""
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM source_records WHERE id = ? AND tenant_id = ? AND domain = ?",
                (record_id, tenant_id, domain),
            )

    @staticmethod
    def _row_to_source_record(row) -> SourceRecord:
        return SourceRecord(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"],
            source_system=row["source_system"], source_record_id=row["source_record_id"],
            attributes=json.loads(row["attributes"]),
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
            return  # no config loaded for this domain (use the table as it is), or already checked
        if self.in_batch():
            raise RuntimeError(f"the golden table for domain {domain!r} has not been checked yet, and the check cannot run inside a batch (it opens its own connection and would wait "
                               f"for this transaction for ever): open the batch with store.batch(domain={domain!r})")
        golden.ensure_or_verify(SQLiteMigrationBackend(self.path), domain, cols, auto_migrate_enabled(self._auto_migrate))
        self._entity_synced[domain] = tuple(cols)

    def sync_entity_table(self, domain: str) -> list:
        """Creates/extends/re-types a domain's golden-record table (used by reindex). Returns the columns added."""
        cols = self._entity_columns(domain)
        added = golden.sync_table(SQLiteMigrationBackend(self.path), domain, cols)
        self._entity_synced[domain] = tuple(cols)
        return added

    def save_entity(self, ent: Entity) -> None:
        self._ensure_entity_table(ent.domain)
        cols = self._entity_columns(ent.domain)
        if cols is None:
            raise KeyError(f"No configuration is loaded for domain '{ent.domain}', so its golden records cannot be written")
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        values = [ent.id, ent.tenant_id, ent.status, ent.created_at, ent.updated_at,
                  json.dumps(ent.quality_issues) if ent.quality_issues else None,
                  *[fieldtypes.to_db("sqlite", t, ent.attributes.get(f)) for f, t in cols]]
        # An UPSERT, not INSERT OR REPLACE: REPLACE deletes the row and re-inserts it with only the columns given, which would
        # wipe the values of any field since dropped from the config. (The Postgres store always upserted.)
        updates = ", ".join(f"{golden.q(c)} = excluded.{golden.q(c)}" for c in names[1:])
        with self._conn() as conn:
            conn.execute(f"INSERT INTO {golden.q(golden.table_name(ent.domain))} ({', '.join(golden.q(c) for c in names)}) "
                         f"VALUES ({', '.join('?' for _ in names)}) ON CONFLICT (entity_id) DO UPDATE SET {updates}", values)

    def _row_to_entity(self, row, domain: str, source_ids: list) -> Entity:
        cols = self._entity_columns(domain)
        keys = set(row.keys())
        if cols is None:  # domain config not loaded: whatever columns the table has, as stored
            cols = [(c, "text") for c in row.keys() if c not in golden.SYSTEM_COLUMNS]
        return Entity(id=row["entity_id"], tenant_id=row["tenant_id"], domain=domain, status=row["status"],
                      attributes={f: (fieldtypes.from_db("sqlite", t, row[f]) if f in keys else None) for f, t in cols},
                      quality_issues=json.loads(row["quality_issues"]) if "quality_issues" in keys and row["quality_issues"] else [],
                      source_record_ids=source_ids, created_at=row["created_at"], updated_at=row["updated_at"])

    def get_entity(self, entity_id: str, tenant_id: str, domain: str) -> Entity | None:
        self._ensure_entity_table(domain)
        try:
            with self._conn() as conn:
                row = conn.execute(f"SELECT * FROM {golden.q(golden.table_name(domain))} WHERE entity_id = ? AND tenant_id = ?",
                                   (entity_id, tenant_id)).fetchone()
                if not row:
                    return None
                source_ids = [r.id for r in self.list_source_records(tenant_id, domain, entity_id=entity_id)]
                return self._row_to_entity(row, domain, source_ids)
        except sqlite3.OperationalError:  # a domain with no golden-record table (and no config to create one)
            return None

    def list_entities(self, tenant_id: str, domain: str, status: str | None = None) -> list[Entity]:
        self._ensure_entity_table(domain)
        sql, params = f"SELECT entity_id FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = ?", [tenant_id]
        if status:
            sql, params = sql + " AND status = ?", params + [status]
        try:
            with self._conn() as conn:
                rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []
        return [self.get_entity(r["entity_id"], tenant_id, domain) for r in rows]

    def delete_entity(self, entity_id: str, tenant_id: str, domain: str) -> None:
        """Hard delete -- right-to-be-forgotten purge only (app/compliance.py)."""
        self._ensure_entity_table(domain)
        with self._conn() as conn:
            conn.execute(f"DELETE FROM {golden.q(golden.table_name(domain))} WHERE entity_id = ? AND tenant_id = ?", (entity_id, tenant_id))

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, "
                "match_type, reasons, status, created_at, decided_at, decided_by, contributions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 json.dumps(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by,
                 json.dumps(c.contributions)),
            )

    def update_candidate_status(self, cand_id: str, tenant_id: str, domain: str,
                                 status: str, decided_by: str, decided_at: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE match_candidates SET status = ?, decided_by = ?, decided_at = ? "
                "WHERE id = ? AND tenant_id = ? AND domain = ?",
                (status, decided_by, decided_at, cand_id, tenant_id, domain),
            )

    def list_candidates(self, tenant_id: str, domain: str, status: str | None = "pending") -> list[MatchCandidate]:
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
                contributions=json.loads(r["contributions"] or "[]"),
                status=r["status"], created_at=r["created_at"],
                decided_at=r["decided_at"], decided_by=r["decided_by"],
            )
            for r in rows
        ]

    def get_candidate(self, cand_id: str, tenant_id: str, domain: str) -> MatchCandidate | None:
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
            contributions=json.loads(row["contributions"] or "[]"),
            status=row["status"], created_at=row["created_at"],
            decided_at=row["decided_at"], decided_by=row["decided_by"],
        )

    # --- lineage -----------------------------------------------------------
    def add_lineage(self, ev: LineageEvent) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                "source_record_id, reason, changed_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                 ev.source_record_id, ev.reason, ev.changed_at),
            )

    def add_lineage_batch(self, events: list) -> None:
        """All of one golden-record rebuild's lineage in ONE transaction (it used to be one commit per field: the most expensive
        store call in the engine)."""
        if events:
            with self._conn() as conn:
                conn.executemany(
                    "INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, "
                    "source_record_id, reason, changed_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [(ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                      ev.source_record_id, ev.reason, ev.changed_at) for ev in events])

    def list_lineage(self, entity_id: str, tenant_id: str, domain: str) -> list[LineageEvent]:
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
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO api_keys (id, tenant_id, role, key_hash, label, allowed_domains, "
                "created_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key.id, key.tenant_id, key.role, key.key_hash, key.label,
                 json.dumps(key.allowed_domains) if key.allowed_domains is not None else None,
                 key.created_at, key.revoked_at),
            )

    def get_api_key_by_hash(self, key_hash: str) -> ApiKeyRecord | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM api_keys WHERE key_hash = ?", (key_hash,)).fetchone()
            return self._row_to_api_key(row) if row else None

    def list_api_keys(self, tenant_id: str) -> list[ApiKeyRecord]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM api_keys WHERE tenant_id = ?", (tenant_id,)).fetchall()
        return [self._row_to_api_key(r) for r in rows]

    def revoke_api_key(self, key_id: str, tenant_id: str, revoked_at: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND tenant_id = ?",
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

    # --- Audit log (immutable, hash-chained -- see app/audit.py) ----------
    def append_audit_entry(self, entry: AuditLogEntry) -> None:
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
        history yet (the chain's genesis case)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (tenant_id,),
            ).fetchone()
            return self._row_to_audit_entry(row) if row else None

    def list_audit_log(self, tenant_id: str, domain: str | None = None, limit: int = 200) -> list[AuditLogEntry]:
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
        return AuditLogEntry(
            id=row["id"], tenant_id=row["tenant_id"], domain=row["domain"], action=row["action"],
            actor=row["actor"], details=json.loads(row["details"]),
            prev_hash=row["prev_hash"], entry_hash=row["entry_hash"], created_at=row["created_at"],
        )

    # --- Usage events (billing metering -- see app/billing.py) ------------
    def record_usage_event(self, ev: UsageEvent) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO usage_events (id, tenant_id, domain, event_type, quantity, metadata, "
                "created_at) VALUES (?,?,?,?,?,?,?)",
                (ev.id, ev.tenant_id, ev.domain, ev.event_type, ev.quantity,
                 json.dumps(ev.metadata), ev.created_at),
            )

    def list_usage_events(self, tenant_id: str, since: str | None = None) -> list[UsageEvent]:
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

    # --- Blocking-index state and reindexing (see app/indexing.py) --------
    def get_index_state(self, domain: str):
        """(fingerprint, status) of a domain's blocking index, or None if never recorded."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT fingerprint, status FROM blocking_index_state WHERE domain = ?", (domain,)
            ).fetchone()
        return (row["fingerprint"], row["status"]) if row else None

    def set_index_state(self, domain: str, fingerprint: str, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO blocking_index_state (domain, fingerprint, status, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET fingerprint = excluded.fingerprint, "
                "status = excluded.status, updated_at = excluded.updated_at",
                (domain, fingerprint, status, now_iso()),
            )

    def domain_has_index_entries(self, domain: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM blocking_keys WHERE domain = ? LIMIT 1", (domain,)
            ).fetchone() is not None

    def count_source_records(self, domain: str) -> int:
        """All tenants: domain configuration is deployment-wide."""
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM source_records WHERE domain = ?", (domain,)
            ).fetchone()["n"]

    def list_source_records_page(self, domain: str, after_id: str, limit: int) -> list[SourceRecord]:
        """Keyset-paginated, across ALL tenants, for reindexing."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM source_records WHERE domain = ? AND id > ? ORDER BY id LIMIT ?",
                (domain, after_id, limit),
            ).fetchall()
        return [self._row_to_source_record(r) for r in rows]

    def replace_blocking_keys_batch(self, entries: list[tuple[str, str, str, set[str]]], fresh: bool = False) -> None:
        """entries: [(tenant_id, domain, record_id, keys), ...]. Swaps each
        record's keys for the new set; the whole batch is one transaction,
        so a record is never left half-indexed. fresh=True: these records are NEW
        and cannot hold keys yet, so nothing is deleted (one statement fewer)."""
        deletes = [] if fresh else [(t, d, rid) for t, d, rid, _ in entries]
        inserts = [(t, d, rid, k) for t, d, rid, keys in entries for k in keys]
        with self._conn() as conn:
            if deletes:
                conn.executemany(
                    "DELETE FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND record_id = ?", deletes)
            conn.executemany(
                "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)", inserts)

    # --- Bucket sizes and statistics (see matching.py / domains_cli.py blocking-stats) ---
    def iter_blocking_keys(self, tenant_id: str, domain: str, page: int = 50_000):
        """Every DISTINCT blocking key of a tenant and domain, as lists of at most `page`, in key order (keyset paging on the index: no offset, no whole-table read at once).
        The key filter reads the index once, at the start of a load, with this."""
        last = None
        while True:
            sql, params = "SELECT DISTINCT blocking_key FROM blocking_keys WHERE tenant_id = ? AND domain = ?", [tenant_id, domain]
            if last is not None:
                sql, params = sql + " AND blocking_key > ?", params + [last]
            with self._conn() as conn:
                rows = conn.execute(sql + " ORDER BY blocking_key LIMIT ?", (*params, page)).fetchall()
            if not rows:
                return
            yield [r[0] for r in rows]
            last = rows[-1][0]

    def count_blocking_keys(self, tenant_id: str, domain: str) -> int:
        """How many entries the index holds for a tenant and domain (to size the key filter). On a very large table this is a full scan: ask once."""
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM blocking_keys WHERE tenant_id = ? AND domain = ?", (tenant_id, domain)).fetchone()[0]

    def blocking_bucket_sizes(self, tenant_id: str, domain: str, keys: set[str]) -> dict[str, int]:
        """How many records hold each of these blocking keys (this tenant+domain). One indexed query."""
        if not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT blocking_key, COUNT(*) AS n FROM blocking_keys WHERE tenant_id = ? AND domain = ? "
                f"AND blocking_key IN ({placeholders}) GROUP BY blocking_key", (tenant_id, domain, *keys)).fetchall()
        return {r["blocking_key"]: r["n"] for r in rows}

    def blocking_key_stats(self, domain: str, top_n: int = 10) -> dict:
        """Bucket-size distribution for a domain, across all tenants (each tenant's buckets are separate)."""
        sized = "(SELECT COUNT(*) AS n FROM blocking_keys WHERE domain = ? GROUP BY tenant_id, blocking_key) AS t"
        with self._conn() as conn:
            t = conn.execute(f"SELECT COUNT(*) AS buckets, COALESCE(SUM(n), 0) AS key_rows, COALESCE(SUM(n * n), 0) AS sq, "
                             f"COALESCE(MAX(n), 0) AS biggest FROM {sized}", (domain,)).fetchone()
            bands = conn.execute(
                f"SELECT CASE WHEN n = 1 THEN '1' WHEN n <= 10 THEN '2-10' WHEN n <= 100 THEN '11-100' "
                f"WHEN n <= 1000 THEN '101-1000' ELSE '1000+' END AS band, COUNT(*) AS buckets FROM {sized} GROUP BY band", (domain,)).fetchall()
            top = conn.execute("SELECT tenant_id, blocking_key, COUNT(*) AS n FROM blocking_keys WHERE domain = ? "
                               "GROUP BY tenant_id, blocking_key ORDER BY n DESC, blocking_key LIMIT ?", (domain, top_n)).fetchall()
            unkeyed = conn.execute("SELECT COUNT(DISTINCT record_id) AS n FROM blocking_keys WHERE domain = ? "
                                   "AND blocking_key = '__no_block__'", (domain,)).fetchone()["n"]
        return {"buckets": t["buckets"], "key_rows": t["key_rows"], "sum_squares": t["sq"], "biggest": t["biggest"],
                "bands": {r["band"]: r["buckets"] for r in bands},
                "top": [(r["tenant_id"], r["blocking_key"], r["n"]) for r in top], "records_without_keys": unkeyed}

    # --- Staging layer: standardized_records (see app/staging.py) ---------
    def save_staged_batch(self, rows: list[tuple]) -> None:
        """rows: [(tenant_id, domain, record_id, matching: dict, cleansed: dict, fingerprint), ...]. Upserts."""
        if not rows:
            return
        now = now_iso()
        with self._conn() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO standardized_records "
                "(tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) VALUES (?,?,?,?,?,?,?)",
                [(t, d, rid, json.dumps(m), json.dumps(c), fp, now) for t, d, rid, m, c, fp in rows])

    def get_staged_many(self, tenant_id: str, domain: str, record_ids) -> dict:
        """{record_id: (matching, cleansed)} for those that are staged."""
        ids, out = list(record_ids), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                marks = ",".join("?" for _ in chunk)
                for r in conn.execute(
                        f"SELECT record_id, matching, cleansed FROM standardized_records "
                        f"WHERE tenant_id = ? AND domain = ? AND record_id IN ({marks})", (tenant_id, domain, *chunk)):
                    out[r["record_id"]] = (json.loads(r["matching"]), json.loads(r["cleansed"]))
        return out

    def delete_staged(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM standardized_records WHERE record_id = ? AND tenant_id = ? AND domain = ?",
                         (record_id, tenant_id, domain))

    def count_staged(self, domain: str) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) AS n FROM standardized_records WHERE domain = ?", (domain,)).fetchone()["n"]

    def list_entities_page(self, domain: str, after_id: str, limit: int) -> list[tuple[str, str]]:
        """[(tenant_id, entity_id)] of ACTIVE entities across all tenants, keyset-paginated (for rebuilding golden records)."""
        self._ensure_entity_table(domain)
        with self._conn() as conn:
            rows = conn.execute(f"SELECT tenant_id, entity_id FROM {golden.q(golden.table_name(domain))} "
                                f"WHERE status = 'active' AND entity_id > ? ORDER BY entity_id LIMIT ?", (after_id, limit)).fetchall()
        return [(r["tenant_id"], r["entity_id"]) for r in rows]

    def replace_derived_batch(self, entries: list[tuple]) -> None:
        """entries: [(tenant_id, domain, record_id, matching, cleansed, fingerprint, keys), ...]. Swaps each record's
        staged forms AND blocking keys in ONE transaction, so a record is never half-rebuilt."""
        now = now_iso()
        deletes = [(t, d, rid) for t, d, rid, *_ in entries]
        keys = [(t, d, rid, k) for t, d, rid, _m, _c, _f, ks in entries for k in ks]
        staged = [(t, d, rid, json.dumps(m), json.dumps(c), fp, now) for t, d, rid, m, c, fp, _ks in entries]
        with self._conn() as conn:
            conn.executemany("DELETE FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND record_id = ?", deletes)
            conn.executemany("INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)", keys)
            conn.executemany("INSERT OR REPLACE INTO standardized_records "
                             "(tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) VALUES (?,?,?,?,?,?,?)", staged)

    # --- Idempotent raw ingest (see pipeline.ingest_record) ----------------
    _NATURAL = ("SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND source_system = ? AND source_record_id = ? "
                "ORDER BY ingested_at DESC, id DESC LIMIT 1")

    def find_source_record(self, tenant_id: str, domain: str, source_system: str, source_record_id: str) -> SourceRecord | None:
        """The CURRENT row for a source system's record id. (If an old database holds the same key more than once, the newest copy.)"""
        with self._conn() as conn:
            row = conn.execute(self._NATURAL, (tenant_id, domain, source_system, source_record_id)).fetchone()
        return self._row_to_source_record(row) if row else None

    def find_source_records_by_keys(self, tenant_id: str, domain: str, keys) -> dict:
        """{(source_system, source_record_id): the latest record held} for the keys that are held. ONE query per source system per chunk of ids (it used to be one query per key)."""
        by_system: dict = {}
        for source_system, source_record_id in keys:
            by_system.setdefault(source_system, []).append(source_record_id)
        d, out = get_dialect("sqlite"), {}
        with self._conn() as conn:
            for source_system, ids in by_system.items():
                for chunk in d.chunks(ids):
                    cond, params = d.in_list("source_record_id", chunk)
                    for row in conn.execute(f"SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND source_system = ? AND {cond} "
                                            f"ORDER BY ingested_at DESC, id DESC", (tenant_id, domain, source_system, *params)):
                        out.setdefault((source_system, row["source_record_id"]), self._row_to_source_record(row))   # newest first: the first one seen is the latest
        return out

    def update_source_record(self, rec_id: str, tenant_id: str, domain: str, attributes: dict, content_hash: str, version: int) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE source_records SET attributes = ?, content_hash = ?, version = ?, updated_at = ?, "
                         "processing_status = 'landed', processing_attempts = 0, processing_error = NULL "
                         "WHERE id = ? AND tenant_id = ? AND domain = ?",
                         (json.dumps(attributes), content_hash, version, now_iso(), rec_id, tenant_id, domain))

    def add_record_history(self, rows: list[tuple]) -> None:
        """rows: [(tenant_id, domain, record_id, version, content_hash, attributes, superseded_at)]. Append-only log of payloads a change replaced."""
        if not rows:
            return
        with self._conn() as conn:
            conn.executemany("INSERT OR REPLACE INTO source_record_history "
                             "(tenant_id, domain, record_id, version, content_hash, attributes, superseded_at) VALUES (?,?,?,?,?,?,?)",
                             [(t, d, rid, v, h, json.dumps(a), at) for t, d, rid, v, h, a, at in rows])

    def list_record_history(self, record_id: str, tenant_id: str, domain: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT version, content_hash, attributes, superseded_at FROM source_record_history "
                                "WHERE record_id = ? AND tenant_id = ? AND domain = ? ORDER BY version", (record_id, tenant_id, domain)).fetchall()
        return [{"version": r["version"], "content_hash": r["content_hash"], "attributes": json.loads(r["attributes"]),
                 "superseded_at": r["superseded_at"]} for r in rows]

    def delete_record_history(self, record_id: str, tenant_id: str, domain: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM source_record_history WHERE record_id = ? AND tenant_id = ? AND domain = ?", (record_id, tenant_id, domain))

    def supersede_pending_candidates(self, tenant_id: str, domain: str, record_id: str) -> int:
        """A changed record invalidates the review items that were scored on its OLD data."""
        with self._conn() as conn:
            cur = conn.execute("UPDATE match_candidates SET status = 'superseded', decided_at = ?, decided_by = 'system:record_changed' "
                               "WHERE tenant_id = ? AND domain = ? AND status = 'pending' AND (record_a_id = ? OR record_b_id = ?)",
                               (now_iso(), tenant_id, domain, record_id, record_id))
            return cur.rowcount

    def count_duplicate_raw_keys(self, domain: str) -> int:
        """Source records stored more than once, from before ingest was idempotent."""
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) AS n FROM (SELECT 1 FROM source_records WHERE domain = ? "
                                "GROUP BY tenant_id, source_system, source_record_id HAVING COUNT(*) > 1) AS t", (domain,)).fetchone()["n"]

    # --- Processing state, run log, deferred golden rebuilds (see pipeline.py) -------------------------
    def set_processing_status(self, record_ids, tenant_id: str, domain: str, status: str, error: str | None = None) -> None:
        ids = list(record_ids)
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                conn.execute(f"UPDATE source_records SET processing_status = ?, processing_error = ? WHERE tenant_id = ? AND domain = ? "
                             f"AND id IN ({','.join('?' for _ in chunk)})", (status, error, tenant_id, domain, *chunk))

    def record_processing_failure(self, record_id: str, tenant_id: str, domain: str, message: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE source_records SET processing_error = ?, processing_attempts = processing_attempts + 1 "
                         "WHERE id = ? AND tenant_id = ? AND domain = ?", (message, record_id, tenant_id, domain))

    def list_unfinished_records(self, domain: str, limit: int, max_attempts: int) -> list[SourceRecord]:
        """Oldest first (matching is order-sensitive), across all tenants; skips records that already failed max_attempts times."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM source_records WHERE domain = ? AND processing_status <> 'matched' AND processing_attempts < ? "
                                "ORDER BY ingested_at, id LIMIT ?", (domain, max_attempts, limit)).fetchall()
        return [self._row_to_source_record(r) for r in rows]

    def count_unfinished(self, domain: str) -> tuple:
        """(records not yet matched, of which have a recorded error)."""
        with self._conn() as conn:
            r = conn.execute("SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN processing_error IS NOT NULL THEN 1 ELSE 0 END), 0) AS failing "
                             "FROM source_records WHERE domain = ? AND processing_status <> 'matched'", (domain,)).fetchone()
        return r["n"], r["failing"]

    def list_candidate_pairs(self, tenant_id: str, domain: str, record_id: str) -> dict:
        """{other_record_id: {id, status, score}} for the live (not superseded) candidates of a record."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, record_b_id AS other, status, score FROM match_candidates WHERE tenant_id = ? AND domain = ? AND record_a_id = ? AND status <> 'superseded' "
                "UNION ALL SELECT id, record_a_id AS other, status, score FROM match_candidates WHERE tenant_id = ? AND domain = ? AND record_b_id = ? AND status <> 'superseded'",
                (tenant_id, domain, record_id, tenant_id, domain, record_id)).fetchall()
        return {r["other"]: {"id": r["id"], "status": r["status"], "score": r["score"]} for r in rows}

    def create_run(self, run_id: str, tenant_id: str, domain: str, mode: str, triggered_by: str, started_at: str) -> None:
        with self._conn() as conn:
            conn.execute("INSERT INTO pipeline_runs (run_id, tenant_id, domain, mode, triggered_by, status, started_at, finished_at, counts, errors) "
                         "VALUES (?, ?, ?, ?, ?, 'running', ?, NULL, '{}', '[]')", (run_id, tenant_id, domain, mode, triggered_by, started_at))

    def finish_run(self, run_id: str, status: str, counts: dict, errors: list) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE pipeline_runs SET status = ?, finished_at = ?, counts = ?, errors = ? WHERE run_id = ?",
                         (status, now_iso(), json.dumps(counts), json.dumps(errors), run_id))

    def list_runs(self, domain: str | None = None, tenant_id: str | None = None, limit: int = 20) -> list[dict]:
        sql, params = "SELECT * FROM pipeline_runs WHERE 1 = 1", []
        for col, val in (("domain", domain), ("tenant_id", tenant_id)):
            if val:
                sql, params = sql + f" AND {col} = ?", params + [val]
        with self._conn() as conn:
            rows = conn.execute(sql + " ORDER BY started_at DESC, run_id DESC LIMIT ?", (*params, limit)).fetchall()
        return [{**dict(r), "counts": json.loads(r["counts"]), "errors": json.loads(r["errors"])} for r in rows]

    def mark_golden_dirty(self, rows: list[tuple]) -> None:
        """rows: [(tenant_id, domain, entity_id)]. Entities whose golden record is stale because a batch run DEFERRED its rebuild.
        Persisted so a run that dies before its final flush does not leave them stale for ever."""
        if rows:
            with self._conn() as conn:
                conn.executemany("INSERT OR IGNORE INTO golden_dirty (tenant_id, domain, entity_id) VALUES (?, ?, ?)", rows)

    def list_golden_dirty(self, domain: str, limit: int = 10000) -> list[tuple]:
        with self._conn() as conn:
            return [(r["tenant_id"], r["entity_id"]) for r in conn.execute(
                "SELECT tenant_id, entity_id FROM golden_dirty WHERE domain = ? ORDER BY entity_id LIMIT ?", (domain, limit)).fetchall()]

    def clear_golden_dirty(self, rows: list[tuple]) -> None:
        if rows:
            with self._conn() as conn:
                conn.executemany("DELETE FROM golden_dirty WHERE tenant_id = ? AND domain = ? AND entity_id = ?", rows)

    def iter_raw_attributes(self, domain: str, tenant_id: str | None = None, source_system: str | None = None, batch: int = 5000):
        """Streams the RAW attribute dicts of a domain (optionally one tenant / one source system), a page at a time, for profiling."""
        last = ""
        while True:
            sql, params = "SELECT id, attributes FROM source_records WHERE domain = ? AND id > ?", [domain, last]
            for col, val in (("tenant_id", tenant_id), ("source_system", source_system)):
                if val:
                    sql, params = sql + f" AND {col} = ?", params + [val]
            with self._conn() as conn:
                rows = conn.execute(sql + " ORDER BY id LIMIT ?", (*params, batch)).fetchall()
            if not rows:
                return
            for r in rows:
                yield json.loads(r["attributes"])
            last = rows[-1]["id"]

    def scrub_protected(self, domain: str, field: str, prefix: str) -> dict:
        """After a field becomes protected, mask what OLDER rows still hold in plain text: lineage values, the golden column of
        entities that were merged away (reindex does not rebuild tombstones), and the text of old match reasons. Idempotent."""
        done = {"lineage": 0, "golden": 0, "reasons": 0}
        with self._conn() as conn:
            done["lineage"] = conn.execute(
                "UPDATE lineage SET old_value = CASE WHEN old_value IS NULL OR old_value LIKE '***%' THEN old_value ELSE ? || substr(old_value, -4) END, "
                "new_value = CASE WHEN new_value IS NULL OR new_value LIKE '***%' THEN new_value ELSE ? || substr(new_value, -4) END "
                "WHERE domain = ? AND field = ? AND ((old_value IS NOT NULL AND old_value NOT LIKE '***%') OR (new_value IS NOT NULL AND new_value NOT LIKE '***%'))",
                (prefix, prefix, domain, field)).rowcount
            col, table = golden.q(field), golden.q(golden.table_name(domain))
            try:
                done["golden"] = conn.execute(f"UPDATE {table} SET {col} = ? || substr({col}, -4) WHERE {col} IS NOT NULL AND {col} NOT LIKE '***%'", (prefix,)).rowcount
            except sqlite3.OperationalError:
                pass  # this domain's golden table has no such column
            rows = conn.execute("SELECT id, reasons FROM match_candidates WHERE domain = ? AND (reasons LIKE ? OR reasons LIKE ?)",
                                (domain, f"%exact {field} match (%", f"%{field} disagrees (%")).fetchall()
            for r in rows:
                reasons = [protect.scrub_reason(field, x) for x in json.loads(r["reasons"])]
                conn.execute("UPDATE match_candidates SET reasons = ? WHERE id = ?", (json.dumps(reasons), r["id"]))
                done["reasons"] += 1
        return done

    # --- bulk load (app/bulk.py) -----------------------------------------------------------------------
    def has_source_records(self, tenant_id: str, domain: str) -> bool:
        with self._conn() as conn:
            return conn.execute("SELECT 1 FROM source_records WHERE tenant_id = ? AND domain = ? LIMIT 1", (tenant_id, domain)).fetchone() is not None

    def save_candidates_batch(self, candidates: list) -> None:
        if candidates:
            with self._conn() as conn:
                conn.executemany(
                    "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at, "
                    "decided_at, decided_by, contributions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type, json.dumps(c.reasons), c.status, c.created_at,
                      c.decided_at, c.decided_by, json.dumps(c.contributions)) for c in candidates])

    def save_entities_batch(self, entities: list) -> None:
        """save_entity() for many golden records of ONE domain in one transaction."""
        if not entities:
            return
        domain = entities[0].domain
        self._ensure_entity_table(domain)
        cols = self._entity_columns(domain)
        if cols is None:
            raise KeyError(f"No configuration is loaded for domain '{domain}', so its golden records cannot be written")
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        updates = ", ".join(f"{golden.q(c)} = excluded.{golden.q(c)}" for c in names[1:])
        rows = [[e.id, e.tenant_id, e.status, e.created_at, e.updated_at, json.dumps(e.quality_issues) if e.quality_issues else None,
                 *[fieldtypes.to_db("sqlite", t, e.attributes.get(f)) for f, t in cols]] for e in entities]
        with self._conn() as conn:
            conn.executemany(f"INSERT INTO {golden.q(golden.table_name(domain))} ({', '.join(golden.q(c) for c in names)}) "
                             f"VALUES ({', '.join('?' for _ in names)}) ON CONFLICT (entity_id) DO UPDATE SET {updates}", rows)

    def rollback_bulk(self, tenant_id: str, domain: str, token: str) -> None:
        """Removes everything a failed bulk load wrote. Its ids all carry the load's token (rec_<token>_n, ent_<token>_n)."""
        rec, ent = f"rec\\_{token}\\_%", f"ent\\_{token}\\_%"
        with self._conn() as conn:
            conn.execute("DELETE FROM merge_audits WHERE tenant_id = ? AND domain = ? AND candidate_id IN (SELECT id FROM match_candidates WHERE tenant_id = ? AND domain = ? "
                         "AND record_a_id LIKE ? ESCAPE '\\')", (tenant_id, domain, tenant_id, domain, rec))   # the pairs this load made were audited too
            for table, col, pattern in (("blocking_keys", "record_id", rec), ("standardized_records", "record_id", rec), ("match_candidates", "record_a_id", rec),
                                        ("lineage", "entity_id", ent), ("source_records", "id", rec)):
                conn.execute(f"DELETE FROM {table} WHERE tenant_id = ? AND domain = ? AND {col} LIKE ? ESCAPE '\\'", (tenant_id, domain, pattern))
            try:
                conn.execute(f"DELETE FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = ? AND entity_id LIKE ? ESCAPE '\\'", (tenant_id, ent))
            except sqlite3.OperationalError:
                pass  # the golden table was never created: nothing to remove

    # --- onboarding a new source onto existing data (app/bulk.py) ------------------------------------------
    def count_tenant_records(self, tenant_id: str, domain: str) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM source_records WHERE tenant_id = ? AND domain = ?", (tenant_id, domain)).fetchone()[0]

    def unfinished_in(self, tenant_id: str, domain: str) -> int:
        """How many of a tenant's records are unfinished. Asked through the PARTIAL index that holds only unfinished records, so it reads those and nothing
        else: the plan SQLite picks for the plain query reads every record's row to test one column (45 s on 900,000 records with a cold cache)."""
        with self._conn() as conn:
            try:
                return conn.execute("SELECT COUNT(*) FROM source_records INDEXED BY idx_source_records_unfinished WHERE domain = ? AND tenant_id = ? "
                                    "AND processing_status <> 'matched'", (domain, tenant_id)).fetchone()[0]
            except sqlite3.OperationalError:  # a database without that index
                return conn.execute("SELECT COUNT(*) FROM source_records WHERE tenant_id = ? AND domain = ? AND processing_status <> 'matched'", (tenant_id, domain)).fetchone()[0]

    def blocking_members(self, tenant_id: str, domain: str, keys) -> dict:
        """{blocking_key: [record_id, ...]} for these keys: who already holds them."""
        keys, out = list(keys), {}
        with self._conn() as conn:
            for i in range(0, len(keys), 500):
                chunk = keys[i:i + 500]
                for r in conn.execute(f"SELECT blocking_key, record_id FROM blocking_keys WHERE tenant_id = ? AND domain = ? "
                                      f"AND blocking_key IN ({','.join('?' for _ in chunk)})", (tenant_id, domain, *chunk)):
                    out.setdefault(r["blocking_key"], []).append(r["record_id"])
        return out

    # The unary + on tenant_id and domain stops SQLite choosing the (tenant, domain) indexes, which made it read EVERY record (or entity) of the tenant for each batch
    # of ids: 0.45 s per 500 ids on 900,000 records, growing with the data (it was 43 s of a 25,000-record delta). The primary key finds the ids directly, and the
    # tenant and domain are still enforced. tests/test_onboarding.py pins both plans.
    _RECORD_ENTITIES_SQL = "SELECT id, entity_id FROM source_records WHERE id IN ({marks}) AND +tenant_id = ? AND +domain = ?"
    _ENTITIES_BATCH_SQL = "SELECT * FROM {table} WHERE entity_id IN ({marks}) AND +tenant_id = ?"

    def get_record_entities(self, tenant_id: str, domain: str, record_ids) -> dict:
        ids, out = list(record_ids), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                for r in conn.execute(self._RECORD_ENTITIES_SQL.format(marks=",".join("?" for _ in chunk)), (*chunk, tenant_id, domain)):
                    out[r["id"]] = r["entity_id"]
        return out

    def get_entities_batch(self, tenant_id: str, domain: str, entity_ids) -> dict:
        """{entity_id: Entity} (without its source record ids: that is a separate, larger query)."""
        self._ensure_entity_table(domain)
        ids, out = list(entity_ids), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                for row in conn.execute(self._ENTITIES_BATCH_SQL.format(table=golden.q(golden.table_name(domain)), marks=",".join("?" for _ in chunk)), (*chunk, tenant_id)):
                    out[row["entity_id"]] = self._row_to_entity(row, domain, [])
        return out

    def list_records_of_entities(self, tenant_id: str, domain: str, entity_ids) -> list:
        ids, out = list(entity_ids), []
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                out += [self._row_to_source_record(r) for r in conn.execute(
                    f"SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND entity_id IN ({','.join('?' for _ in chunk)}) ORDER BY ingested_at, id",
                    (tenant_id, domain, *chunk))]
        return out

    def apply_onboarding(self, tenant_id: str, domain: str, reassign: list, entities: list, lineage: list, *, history=(), record_updates=(), supersede=(),
                         derived=(), candidates=()) -> None:
        """Every change onboarding makes to EXISTING data, in ONE transaction: all of it applies or none of it does.
        reassign: [(loser entity, survivor entity)]: the loser's records move to the survivor and the loser becomes a 'merged' tombstone.
        entities: the survivors' rebuilt golden records. lineage: the events that rebuilding produced.
        A DELTA (changed records of a source already loaded) adds, in the same transaction: history [(tenant, domain, record_id, old version, old hash,
        old attributes, superseded_at)], record_updates [(record_id, attributes, content_hash, version, updated_at)] (in place, same id and entity, ingested_at untouched,
        finished), supersede [record ids whose pending review items were scored on old data], derived [(tenant, domain, record_id, matching, cleansed,
        fingerprint, keys)] (staging and blocking keys swapped), candidates [MatchCandidate] (the new pairs that involve a changed record)."""
        table = golden.q(golden.table_name(domain))
        self._ensure_entity_table(domain)
        cols = self._entity_columns(domain)
        names = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues", *[f for f, _ in cols]]
        updates = ", ".join(f"{golden.q(c)} = excluded.{golden.q(c)}" for c in names[1:])
        rows = [[e.id, e.tenant_id, e.status, e.created_at, e.updated_at, json.dumps(e.quality_issues) if e.quality_issues else None,
                 *[fieldtypes.to_db("sqlite", t, e.attributes.get(f)) for f, t in cols]] for e in entities]
        now = now_iso()
        with self._conn() as conn:
            if history:
                conn.executemany("INSERT OR REPLACE INTO source_record_history (tenant_id, domain, record_id, version, content_hash, attributes, superseded_at) "
                                 "VALUES (?,?,?,?,?,?,?)", [(t, d, rid, v, h, json.dumps(a), at) for t, d, rid, v, h, a, at in history])
            if record_updates:
                conn.executemany("UPDATE source_records SET attributes = ?, content_hash = ?, version = ?, updated_at = ?, processing_status = 'matched', "
                                 "processing_attempts = 0, processing_error = NULL WHERE id = ? AND tenant_id = ? AND domain = ?",
                                 [(json.dumps(a), h, v, at, rid, tenant_id, domain) for rid, a, h, v, at in record_updates])
            if supersede:  # the pending items are read ONCE; asking per 400 changed records scanned all of them each time
                changing = set(supersede)
                pending = [r[0] for r in conn.execute("SELECT id, record_a_id, record_b_id FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'pending'", (tenant_id, domain))
                           if r[1] in changing or r[2] in changing]
                for i in range(0, len(pending), 500):
                    part = pending[i:i + 500]
                    conn.execute(f"UPDATE match_candidates SET status = 'superseded', decided_at = ?, decided_by = 'system:record_changed' WHERE id IN ({','.join('?' for _ in part)})", (now, *part))
            if derived:
                conn.executemany("DELETE FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND record_id = ?", [(t, d, rid) for t, d, rid, *_ in derived])
                conn.executemany("INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)",
                                 [(t, d, rid, k) for t, d, rid, _m, _c, _f, ks in derived for k in ks])
                conn.executemany("INSERT OR REPLACE INTO standardized_records (tenant_id, domain, record_id, matching, cleansed, fingerprint, standardized_at) "
                                 "VALUES (?,?,?,?,?,?,?)", [(t, d, rid, json.dumps(m), json.dumps(c), fp, now) for t, d, rid, m, c, fp, _ks in derived])
            if candidates:
                conn.executemany("INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at, "
                                 "decided_at, decided_by, contributions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                 [(c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type, json.dumps(c.reasons), c.status, c.created_at,
                                   c.decided_at, c.decided_by, json.dumps(c.contributions)) for c in candidates])
            for loser, survivor in reassign:
                conn.execute("UPDATE source_records SET entity_id = ? WHERE tenant_id = ? AND domain = ? AND entity_id = ?", (survivor, tenant_id, domain, loser))
                conn.execute(f"UPDATE {table} SET status = 'merged' WHERE tenant_id = ? AND entity_id = ?", (tenant_id, loser))
            if rows:
                conn.executemany(f"INSERT INTO {table} ({', '.join(golden.q(c) for c in names)}) VALUES ({', '.join('?' for _ in names)}) "
                                 f"ON CONFLICT (entity_id) DO UPDATE SET {updates}", rows)
            if lineage:
                conn.executemany("INSERT INTO lineage (id, tenant_id, domain, entity_id, field, old_value, new_value, source_record_id, reason, changed_at) "
                                 "VALUES (?,?,?,?,?,?,?,?,?,?)", [(ev.id, ev.tenant_id, ev.domain, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                                                                   ev.source_record_id, ev.reason, ev.changed_at) for ev in lineage])

    def blocking_keys_of(self, tenant_id: str, domain: str, record_ids) -> dict:
        """{record_id: [blocking_key, ...]}: the keys these records hold NOW (a delta needs the keys a changed record is about to lose)."""
        ids, out = list(record_ids), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                for r in conn.execute(f"SELECT record_id, blocking_key FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND record_id IN ({','.join('?' for _ in chunk)})",
                                      (tenant_id, domain, *chunk)):
                    out.setdefault(r["record_id"], []).append(r["blocking_key"])
        return out

    def get_matching_many(self, tenant_id: str, domain: str, record_ids) -> dict:
        """{record_id: matching form}. Scoring reads nothing else of an existing record: the cleansed form is for golden records, and is half the bytes."""
        ids, out = list(record_ids), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                for r in conn.execute(f"SELECT record_id, matching FROM standardized_records WHERE tenant_id = ? AND domain = ? AND record_id IN ({','.join('?' for _ in chunk)})",
                                      (tenant_id, domain, *chunk)):
                    out[r["record_id"]] = json.loads(r["matching"])
        return out

    # --- label hygiene and merge audits (app/learned_scorer.py, train_scorer.py) ---------------------------------------------------
    AUDIT_VERDICTS = ("correct", "wrong")

    @staticmethod
    def _candidate_from_row(r) -> MatchCandidate:
        return MatchCandidate(id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"], record_a_id=r["record_a_id"], record_b_id=r["record_b_id"], score=r["score"],
                              match_type=r["match_type"], reasons=json.loads(r["reasons"]), contributions=json.loads(r["contributions"] or "[]"), status=r["status"],
                              created_at=r["created_at"], decided_at=r["decided_at"], decided_by=r["decided_by"])

    def flag_merge_audits(self, tenant_id: str, domain: str, candidate_ids) -> int:
        """Ask a person to check these pairs (only pairs of this tenant and domain; a pair already flagged is left alone). Returns how many were newly flagged."""
        ids = list(candidate_ids)
        if not ids:
            return 0
        flagged, now = 0, now_iso()
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                mine = [(r[0], r[1]) for r in conn.execute(f"SELECT id, decided_by FROM match_candidates WHERE tenant_id = ? AND domain = ? AND id IN ({','.join('?' for _ in chunk)})", (tenant_id, domain, *chunk))]
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO merge_audits (candidate_id, tenant_id, domain, sampled_at, made_by) VALUES (?,?,?,?,?)",
                                 [(c, tenant_id, domain, now, "promoted" if by == "system:learned_scorer" else "rule") for c, by in mine])   # who made it, fixed now: unmerging rewrites decided_by later
                flagged += conn.total_changes - before
        return flagged

    def sample_merge_audit(self, tenant_id: str, domain: str, rate: float, seed=None) -> int:
        """Flag a random `rate` of the pairs the SYSTEM merged automatically and nobody has been asked about yet. A pair a steward confirmed is already a human
        label, so it is not sampled. Deterministic for a given seed. Returns how many were flagged."""
        import random
        if not 0 < rate <= 1:
            raise ValueError("rate must be above 0 and at most 1")
        with self._conn() as conn:
            ids = [r[0] for r in conn.execute("SELECT id FROM match_candidates mc WHERE tenant_id = ? AND domain = ? AND status = 'confirmed' AND decided_by LIKE 'system:%' "
                                              "AND NOT EXISTS (SELECT 1 FROM merge_audits a WHERE a.candidate_id = mc.id) ORDER BY id", (tenant_id, domain))]
        rng = random.Random(seed)
        return self.flag_merge_audits(tenant_id, domain, [i for i in ids if rng.random() < rate])

    def record_merge_audit(self, candidate_id: str, tenant_id: str, domain: str, verdict: str, audited_by: str) -> bool:
        """A person's verdict on a flagged pair: 'correct' (these are the same) or 'wrong'. False if the pair was never flagged."""
        if verdict not in self.AUDIT_VERDICTS:
            raise ValueError(f"verdict must be one of {self.AUDIT_VERDICTS}")
        if not audited_by or audited_by.startswith("system:"):
            raise ValueError("a verdict must come from a person, not from the system")
        with self._conn() as conn:
            cur = conn.execute("UPDATE merge_audits SET verdict = ?, audited_by = ?, audited_at = ? WHERE candidate_id = ? AND tenant_id = ? AND domain = ?",
                               (verdict, audited_by, now_iso(), candidate_id, tenant_id, domain))
            return cur.rowcount == 1

    def list_merge_audits(self, tenant_id: str, domain: str, pending_only: bool = False) -> list:
        with self._conn() as conn:
            rows = conn.execute("SELECT a.candidate_id, a.sampled_at, a.verdict, a.audited_by, a.audited_at, COALESCE(a.made_by, 'rule') AS made_by, mc.score, mc.record_a_id, mc.record_b_id, mc.decided_by FROM merge_audits a "
                                "JOIN match_candidates mc ON mc.id = a.candidate_id WHERE a.tenant_id = ? AND a.domain = ?" + (" AND a.verdict IS NULL" if pending_only else "")
                                + " ORDER BY a.sampled_at, a.candidate_id", (tenant_id, domain)).fetchall()
        return [dict(r) for r in rows]

    def human_labeled_candidates(self, tenant_id: str, domain: str) -> list:
        """[(MatchCandidate, label, source)]: the ONLY pairs a model may learn from. label 1 = same, 0 = different. source 'steward' = a person confirmed or rejected it;
        'audit' = a person checked an automatic merge. A decision the system made itself (an auto-merge, an LLM verdict) is NOT a label: training on it teaches
        the model to repeat the system. A pair with both an audit verdict and a steward decision counts once, as the audit."""
        out, seen = [], set()
        with self._conn() as conn:
            for r in conn.execute("SELECT mc.*, CASE a.verdict WHEN 'correct' THEN 1 ELSE 0 END AS label FROM merge_audits a JOIN match_candidates mc ON mc.id = a.candidate_id "
                                  "WHERE a.tenant_id = ? AND a.domain = ? AND a.verdict IS NOT NULL AND (mc.decided_by IS NULL OR mc.decided_by NOT LIKE 'unmerge:%') ORDER BY mc.id", (tenant_id, domain)):
                out.append((self._candidate_from_row(r), r["label"], "audit"))
                seen.add(r["id"])
            for r in conn.execute("SELECT mc.*, CASE mc.status WHEN 'confirmed' THEN 1 ELSE 0 END AS label FROM match_candidates mc WHERE mc.tenant_id = ? AND mc.domain = ? "
                                  "AND mc.status IN ('confirmed', 'rejected') AND mc.decided_by IS NOT NULL AND mc.decided_by NOT LIKE 'system:%' ORDER BY mc.id", (tenant_id, domain)):
                if r["id"] not in seen:
                    out.append((self._candidate_from_row(r), r["label"], "steward"))
        return out

    def count_auto_merges(self, tenant_id: str, domain: str) -> dict:
        """{'rule': n, 'promoted': n}: the system's automatic merges, by who made them (the rule score, or the learned scorer promoting a pair out of review)."""
        with self._conn() as conn:
            rows = conn.execute("SELECT CASE WHEN decided_by = 'system:learned_scorer' THEN 'promoted' ELSE 'rule' END AS stratum, COUNT(*) AS n FROM match_candidates "
                                "WHERE tenant_id = ? AND domain = ? AND status = 'confirmed' AND decided_by LIKE 'system:%' GROUP BY stratum", (tenant_id, domain)).fetchall()
        counts = {"rule": 0, "promoted": 0}
        counts.update({r["stratum"]: r["n"] for r in rows})
        return counts

    # --- unmerge: a person's "these two are different", and keeping it true (app/unmerge.py) ------------------------------
    def confirmed_pairs_among(self, tenant_id: str, domain: str, record_ids) -> list:
        """(candidate_id, record_a_id, record_b_id) of every CONFIRMED pair whose two records are both in record_ids."""
        ids, out, listed = set(record_ids), [], list(set(record_ids))
        with self._conn() as conn:
            for i in range(0, len(listed), 500):
                chunk = listed[i:i + 500]
                for r in conn.execute(f"SELECT id, record_a_id, record_b_id FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'confirmed' "
                                      f"AND record_a_id IN ({','.join('?' * len(chunk))})", (tenant_id, domain, *chunk)):
                    if r["record_b_id"] in ids:
                        out.append((r["id"], r["record_a_id"], r["record_b_id"]))
        return out

    def has_separations(self, tenant_id: str, domain: str) -> bool:
        """Has a person unmerged anything in this domain? One tiny query: the engines ask it for every record, and for most tenants the answer is no."""
        with self._conn() as conn:
            return conn.execute("SELECT 1 FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'rejected' AND decided_by LIKE 'unmerge:%' LIMIT 1",
                                (tenant_id, domain)).fetchone() is not None

    def separated_partners(self, tenant_id: str, domain: str, record_id: str) -> set:
        """The records a person has said are NOT the same as this one."""
        with self._conn() as conn:
            rows = conn.execute("SELECT record_a_id, record_b_id FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'rejected' AND decided_by LIKE 'unmerge:%' "
                                "AND (record_a_id = ? OR record_b_id = ?)", (tenant_id, domain, record_id, record_id)).fetchall()
        return {r["record_b_id"] if r["record_a_id"] == record_id else r["record_a_id"] for r in rows}

    def entities_separated(self, tenant_id: str, domain: str, entity_a: str, entity_b: str):
        """The candidate id of a separation between a record of entity_a and a record of entity_b, or None: merging these two entities would undo it."""
        with self._conn() as conn:
            row = conn.execute("SELECT mc.id FROM match_candidates mc JOIN source_records ra ON ra.id = mc.record_a_id AND +ra.tenant_id = mc.tenant_id AND +ra.domain = mc.domain "
                               "JOIN source_records rb ON rb.id = mc.record_b_id AND +rb.tenant_id = mc.tenant_id AND +rb.domain = mc.domain "
                               "WHERE mc.tenant_id = ? AND mc.domain = ? AND mc.status = 'rejected' AND mc.decided_by LIKE 'unmerge:%' "
                               "AND ((ra.entity_id = ? AND rb.entity_id = ?) OR (ra.entity_id = ? AND rb.entity_id = ?)) LIMIT 1",
                               (tenant_id, domain, entity_a, entity_b, entity_b, entity_a)).fetchone()
        return row["id"] if row else None

    def separation_violations(self, tenant_id: str, domain: str) -> list:
        """(candidate_id, record_a_id, record_b_id, entity_id) of every separation a person made whose two records are in the SAME entity again."""
        with self._conn() as conn:
            rows = conn.execute("SELECT mc.id, mc.record_a_id, mc.record_b_id, ra.entity_id FROM match_candidates mc JOIN source_records ra ON ra.id = mc.record_a_id AND +ra.tenant_id = mc.tenant_id "
                                "AND +ra.domain = mc.domain JOIN source_records rb ON rb.id = mc.record_b_id AND +rb.tenant_id = mc.tenant_id AND +rb.domain = mc.domain "
                                "WHERE mc.tenant_id = ? AND mc.domain = ? AND mc.status = 'rejected' AND mc.decided_by LIKE 'unmerge:%' "
                                "AND ra.entity_id IS NOT NULL AND ra.entity_id = rb.entity_id", (tenant_id, domain)).fetchall()
        return [(r["id"], r["record_a_id"], r["record_b_id"], r["entity_id"]) for r in rows]

    def separation_pairs(self, tenant_id: str, domain: str) -> list:
        """(record_a_id, record_b_id) of every pair a person separated by unmerging it."""
        with self._conn() as conn:
            rows = conn.execute("SELECT record_a_id, record_b_id FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'rejected' AND decided_by LIKE 'unmerge:%'",
                                (tenant_id, domain)).fetchall()
        return [(r["record_a_id"], r["record_b_id"]) for r in rows]

    # --- the audit page and the monitor, computed in SQL: their cost must not grow with the number of audits ---------------
    def audit_counts(self, tenant_id: str, domain: str) -> dict:
        """{'rule': {'audited', 'wrong'}, 'promoted': {...}, 'waiting': n}: how the audits stand, by who MADE each merge."""
        out = {"rule": {"audited": 0, "wrong": 0}, "promoted": {"audited": 0, "wrong": 0}, "waiting": 0}
        with self._conn() as conn:
            for r in conn.execute("SELECT COALESCE(made_by, 'rule') AS kind, SUM(verdict IS NOT NULL) AS audited, SUM(verdict = 'wrong') AS wrong, SUM(verdict IS NULL) AS waiting "
                                  "FROM merge_audits WHERE tenant_id = ? AND domain = ? GROUP BY kind", (tenant_id, domain)):
                out[r["kind"]]["audited"], out[r["kind"]]["wrong"] = r["audited"] or 0, r["wrong"] or 0
                out["waiting"] += r["waiting"] or 0
        return out

    def audited_wrong(self, tenant_id: str, domain: str) -> list:
        """(candidate_id, made_by) of every merge a person judged wrong."""
        with self._conn() as conn:
            return [(r["candidate_id"], r["kind"]) for r in conn.execute(
                "SELECT candidate_id, COALESCE(made_by, 'rule') AS kind FROM merge_audits WHERE tenant_id = ? AND domain = ? AND verdict = 'wrong' ORDER BY audited_at, candidate_id", (tenant_id, domain))]

    def pending_audits(self, tenant_id: str, domain: str, limit: int) -> list:
        """The merges still waiting for a person, the model's promotions first, then the oldest flagged: at most `limit`, never the whole table."""
        out = []
        with self._conn() as conn:
            for kind_clause in ("a.made_by = 'promoted'", "(a.made_by = 'rule' OR a.made_by IS NULL)"):
                if len(out) >= limit:
                    break
                for r in conn.execute("SELECT a.candidate_id, a.sampled_at, COALESCE(a.made_by, 'rule') AS made_by FROM merge_audits a JOIN match_candidates mc ON mc.id = a.candidate_id "
                                      f"WHERE a.tenant_id = ? AND a.domain = ? AND a.verdict IS NULL AND mc.status = 'confirmed' AND {kind_clause} "
                                      "ORDER BY a.sampled_at, a.candidate_id LIMIT ?", (tenant_id, domain, limit - len(out))):
                    out.append(dict(r))
        return out

    # --- pages of the two lists a person scrolls (the review queue and the entities): keyset, batched, never the whole table -----------
    def pending_page(self, tenant_id: str, domain: str, limit: int, after=None) -> list:
        """The next `limit` pending pairs, best score first (ties: highest id first), after `after` = (score, candidate id) of the last one seen. Served by the partial index over pending pairs.
        ('pending' is written as a literal so that any planner can match it to the index's WHERE clause: SQLite 3.45 also manages it with a parameter,
        but that is not something to depend on across versions and databases.)"""
        sql, params = "SELECT * FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'pending'", [tenant_id, domain]
        if after is not None:
            sql, params = sql + " AND (score, id) < (?, ?)", params + [after[0], after[1]]   # one row-value comparison: a single index seek, however deep
        with self._conn() as conn:
            return [self._candidate_from_row(r) for r in conn.execute(sql + " ORDER BY score DESC, id DESC LIMIT ?", (*params, limit))]

    def count_pending(self, tenant_id: str, domain: str) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM match_candidates WHERE tenant_id = ? AND domain = ? AND status = 'pending'", (tenant_id, domain)).fetchone()[0]

    # --- agent recommendations: what a stewardship agent thinks about a review item. They change nothing; a person's decision later records an OUTCOME on them ---
    @staticmethod
    def _recommendation_from_row(r) -> Recommendation:
        return Recommendation(candidate_id=r["candidate_id"], recommendation=r["recommendation"], confidence=r["confidence"], agent=r["agent"], reasons=json.loads(r["reasons"]),
                              evidence=json.loads(r["evidence"]), model=r["model"], prompt_version=r["prompt_version"], id=r["id"], tenant_id=r["tenant_id"], domain=r["domain"],
                              created_at=r["created_at"], outcome=r["outcome"], outcome_decision=r["outcome_decision"], outcome_by=r["outcome_by"], outcome_at=r["outcome_at"],
                              outcome_seen=None if r["outcome_seen"] is None else bool(r["outcome_seen"]))

    def save_recommendation(self, rec: Recommendation) -> Recommendation:
        """One recommendation per (candidate, agent, model, prompt version): proposing again REPLACES it (a newer opinion of the same agent). Returns what is stored."""
        sql = get_dialect("sqlite").upsert("agent_recommendations", ["id", "tenant_id", "domain", "candidate_id", "recommendation", "confidence", "reasons", "evidence", "agent", "model", "prompt_version", "created_at"], key=["tenant_id", "domain", "candidate_id", "agent", "model", "prompt_version"],
                                           update=["recommendation", "confidence", "reasons", "evidence", "created_at"])
        with self._conn() as conn:
            conn.execute(sql, (rec.id, rec.tenant_id, rec.domain, rec.candidate_id, rec.recommendation, rec.confidence, json.dumps(rec.reasons), json.dumps(rec.evidence), rec.agent,
                               rec.model, rec.prompt_version, rec.created_at))
            row = conn.execute("SELECT * FROM agent_recommendations WHERE tenant_id = ? AND domain = ? AND candidate_id = ? AND agent = ? AND model = ? AND prompt_version = ?",
                               (rec.tenant_id, rec.domain, rec.candidate_id, rec.agent, rec.model, rec.prompt_version)).fetchone()
        return self._recommendation_from_row(row)

    def list_recommendations_for(self, tenant_id: str, domain: str, candidate_id: str) -> list:
        with self._conn() as conn:
            return [self._recommendation_from_row(r) for r in conn.execute(
                "SELECT * FROM agent_recommendations WHERE tenant_id = ? AND domain = ? AND candidate_id = ? ORDER BY created_at DESC, id DESC", (tenant_id, domain, candidate_id))]

    def recommendations_page(self, tenant_id: str, domain: str, limit: int, after=None, outcome: str | None = None) -> list:
        """The next `limit` recommendations, newest first, after `after` = (created_at, id) of the last one seen. outcome: 'open' (no person has decided yet), or an outcome."""
        sql, params = "SELECT * FROM agent_recommendations WHERE tenant_id = ? AND domain = ?", [tenant_id, domain]
        if outcome == "open":
            sql += " AND outcome IS NULL"
        elif outcome:
            sql, params = sql + " AND outcome = ?", params + [outcome]
        if after is not None:
            sql, params = sql + " AND " + get_dialect("sqlite").keyset_after(["created_at", "id"], descending=True), params + [after[0], after[1]]
        with self._conn() as conn:
            return [self._recommendation_from_row(r) for r in conn.execute(sql + " ORDER BY created_at DESC, id DESC LIMIT ?", (*params, limit))]

    def record_recommendation_outcomes(self, tenant_id: str, domain: str, candidate_id: str, decision: str, decided_by: str, decided_at: str, suggestion_viewed: bool | None = None) -> int:
        """A person decided this item: every recommendation still open for it gets its outcome (agreed, disagreed, or human_decided if it said needs_human). Returns how many."""
        with self._conn() as conn:
            return conn.execute("UPDATE agent_recommendations SET outcome = CASE WHEN recommendation = 'needs_human' THEN 'human_decided' WHEN recommendation = ? THEN 'agreed' ELSE 'disagreed' END, "
                                "outcome_decision = ?, outcome_by = ?, outcome_at = ?, outcome_seen = ? WHERE tenant_id = ? AND domain = ? AND candidate_id = ? AND outcome IS NULL",
                                (decision, decision, decided_by, decided_at, None if suggestion_viewed is None else int(bool(suggestion_viewed)), tenant_id, domain, candidate_id)).rowcount

    def recommendation_counts(self, tenant_id: str, domain: str, limit: int = 20000) -> list:
        """[(recommendation, outcome, confidence, how many)]: what the agreement statistics are made from."""
        with self._conn() as conn:
            return [(r["recommendation"], r["outcome"], r["confidence"], None if r["outcome_seen"] is None else bool(r["outcome_seen"]), r["n"]) for r in conn.execute(
                "SELECT recommendation, outcome, confidence, outcome_seen, COUNT(*) AS n FROM agent_recommendations WHERE tenant_id = ? AND domain = ? "
                "GROUP BY recommendation, outcome, confidence, outcome_seen LIMIT ?", (tenant_id, domain, limit))]

    def delete_recommendations(self, tenant_id: str, domain: str, candidate_ids) -> int:
        """Right to be forgotten: an agent's reasons may quote the data, so they go with it. Returns how many were deleted."""
        d, n = get_dialect("sqlite"), 0
        with self._conn() as conn:
            for chunk in d.chunks(list(candidate_ids)):
                cond, params = d.in_list("candidate_id", chunk)
                n += conn.execute(f"DELETE FROM agent_recommendations WHERE tenant_id = ? AND domain = ? AND {cond}", (tenant_id, domain, *params)).rowcount
        return n

    def get_source_records_batch(self, tenant_id: str, domain: str, record_ids) -> dict:
        """{record id: SourceRecord}: one query per 500, instead of one per record."""
        ids, out = list(dict.fromkeys(record_ids)), {}
        with self._conn() as conn:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                for r in conn.execute(f"SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND id IN ({','.join('?' for _ in chunk)})", (tenant_id, domain, *chunk)):
                    out[r["id"]] = self._row_to_source_record(r)
        return out

    def entities_page(self, tenant_id: str, domain: str, limit: int, after_id: str | None = None, status: str | None = None) -> list:
        """The next `limit` entities of this tenant after `after_id`, in id order, WITH their source record ids: three queries for the page, not three per entity."""
        self._ensure_entity_table(domain)
        sql, params = f"SELECT entity_id FROM {golden.q(golden.table_name(domain))} WHERE tenant_id = ?", [tenant_id]
        if after_id is not None:
            sql, params = sql + " AND entity_id > ?", params + [after_id]
        if status:
            sql, params = sql + " AND status = ?", params + [status]
        try:
            with self._conn() as conn:
                ids = [r["entity_id"] for r in conn.execute(sql + " ORDER BY entity_id LIMIT ?", (*params, limit))]
        except sqlite3.OperationalError:
            return []
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
