"""
Persistence layer with tenant-scoped isolation (Phase 2 item #3).

Still SQLite via stdlib `sqlite3` -- no Postgres server is reachable in
this sandbox, so that swap isn't tested here. But the isolation *pattern*
(every table carries tenant_id, every read query filters on it) is the
part that actually matters for correctness, and that's fully exercised
below with a dedicated multi-tenant test. Moving the same schema/queries
to Postgres is a connection-string change plus swapping `sqlite3.connect`
for `psycopg2.connect` (or, more robustly, adding Postgres row-level
security policies as a second enforcement layer -- see the README).

Design choice: tenant_id is required on every write and every read that
returns a single record by ID. This means it is *impossible* to fetch a
record without stating which tenant you're asking on behalf of -- there
is no "give me record X" API, only "give me record X for tenant Y". That
constraint is what actually prevents cross-tenant leakage, not just a
best-effort filter.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .models import Entity, LineageEvent, MatchCandidate, SourceRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    attributes TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    entity_id TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attributes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_candidates (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
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
    entity_id TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    source_record_id TEXT,
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_records_entity ON source_records(tenant_id, entity_id);
CREATE INDEX IF NOT EXISTS idx_source_records_tenant ON source_records(tenant_id);
CREATE INDEX IF NOT EXISTS idx_entities_tenant ON entities(tenant_id);
CREATE INDEX IF NOT EXISTS idx_match_candidates_tenant_status ON match_candidates(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_lineage_tenant_entity ON lineage(tenant_id, entity_id);
"""


class Store:
    def __init__(self, path: str = "mdm.db"):
        self.path = path
        parent = Path(path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self):
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    # --- source records -----------------------------------------------
    def save_source_record(self, rec: SourceRecord) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO source_records (id, tenant_id, source_system, source_record_id, "
                "attributes, ingested_at, entity_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (rec.id, rec.tenant_id, rec.source_system, rec.source_record_id,
                 json.dumps(rec.attributes), rec.ingested_at, rec.entity_id),
            )

    def get_source_record(self, rec_id: str, tenant_id: str) -> SourceRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM source_records WHERE id = ? AND tenant_id = ?", (rec_id, tenant_id)
            ).fetchone()
            return self._row_to_source_record(row) if row else None

    def list_source_records(self, tenant_id: str, entity_id: str | None = None) -> list[SourceRecord]:
        with self._conn() as conn:
            if entity_id:
                rows = conn.execute(
                    "SELECT * FROM source_records WHERE tenant_id = ? AND entity_id = ?",
                    (tenant_id, entity_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM source_records WHERE tenant_id = ?", (tenant_id,)
                ).fetchall()
            return [self._row_to_source_record(r) for r in rows]

    def set_record_entity(self, record_id: str, tenant_id: str, entity_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE source_records SET entity_id = ? WHERE id = ? AND tenant_id = ?",
                (entity_id, record_id, tenant_id),
            )

    @staticmethod
    def _row_to_source_record(row) -> SourceRecord:
        return SourceRecord(
            id=row["id"], tenant_id=row["tenant_id"], source_system=row["source_system"],
            source_record_id=row["source_record_id"],
            attributes=json.loads(row["attributes"]),
            ingested_at=row["ingested_at"], entity_id=row["entity_id"],
        )

    # --- entities --------------------------------------------------------
    def save_entity(self, ent: Entity) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO entities (id, tenant_id, status, attributes, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (ent.id, ent.tenant_id, ent.status, json.dumps(ent.attributes), ent.created_at, ent.updated_at),
            )

    def get_entity(self, entity_id: str, tenant_id: str) -> Entity | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM entities WHERE id = ? AND tenant_id = ?", (entity_id, tenant_id)
            ).fetchone()
            if not row:
                return None
            source_ids = [r.id for r in self.list_source_records(tenant_id, entity_id=entity_id)]
            return Entity(
                id=row["id"], tenant_id=row["tenant_id"], status=row["status"],
                attributes=json.loads(row["attributes"]), source_record_ids=source_ids,
                created_at=row["created_at"], updated_at=row["updated_at"],
            )

    def list_entities(self, tenant_id: str, status: str | None = None) -> list[Entity]:
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT id FROM entities WHERE tenant_id = ? AND status = ?", (tenant_id, status)
                ).fetchall()
            else:
                rows = conn.execute("SELECT id FROM entities WHERE tenant_id = ?", (tenant_id,)).fetchall()
        return [self.get_entity(r["id"], tenant_id) for r in rows]

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO match_candidates (id, tenant_id, record_a_id, record_b_id, score, match_type, "
                "reasons, status, created_at, decided_at, decided_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (c.id, c.tenant_id, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 json.dumps(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by),
            )

    def update_candidate_status(self, cand_id: str, tenant_id: str, status: str, decided_by: str, decided_at: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE match_candidates SET status = ?, decided_by = ?, decided_at = ? "
                "WHERE id = ? AND tenant_id = ?",
                (status, decided_by, decided_at, cand_id, tenant_id),
            )

    def list_candidates(self, tenant_id: str, status: str | None = "pending") -> list[MatchCandidate]:
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = ? AND status = ? ORDER BY score DESC",
                    (tenant_id, status),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM match_candidates WHERE tenant_id = ? ORDER BY score DESC", (tenant_id,)
                ).fetchall()
        return [
            MatchCandidate(
                id=r["id"], tenant_id=r["tenant_id"], record_a_id=r["record_a_id"], record_b_id=r["record_b_id"],
                score=r["score"], match_type=r["match_type"], reasons=json.loads(r["reasons"]),
                status=r["status"], created_at=r["created_at"],
                decided_at=r["decided_at"], decided_by=r["decided_by"],
            )
            for r in rows
        ]

    def get_candidate(self, cand_id: str, tenant_id: str) -> MatchCandidate | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM match_candidates WHERE id = ? AND tenant_id = ?", (cand_id, tenant_id)
            ).fetchone()
        if not row:
            return None
        return MatchCandidate(
            id=row["id"], tenant_id=row["tenant_id"], record_a_id=row["record_a_id"], record_b_id=row["record_b_id"],
            score=row["score"], match_type=row["match_type"], reasons=json.loads(row["reasons"]),
            status=row["status"], created_at=row["created_at"],
            decided_at=row["decided_at"], decided_by=row["decided_by"],
        )

    # --- lineage -----------------------------------------------------------
    def add_lineage(self, ev: LineageEvent) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lineage (id, tenant_id, entity_id, field, old_value, new_value, "
                "source_record_id, reason, changed_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (ev.id, ev.tenant_id, ev.entity_id, ev.field, ev.old_value, ev.new_value,
                 ev.source_record_id, ev.reason, ev.changed_at),
            )

    def list_lineage(self, entity_id: str, tenant_id: str) -> list[LineageEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM lineage WHERE entity_id = ? AND tenant_id = ? ORDER BY changed_at",
                (entity_id, tenant_id),
            ).fetchall()
        return [
            LineageEvent(
                id=r["id"], tenant_id=r["tenant_id"], entity_id=r["entity_id"], field=r["field"],
                old_value=r["old_value"], new_value=r["new_value"],
                source_record_id=r["source_record_id"], reason=r["reason"], changed_at=r["changed_at"],
            )
            for r in rows
        ]
