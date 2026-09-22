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

from .models import Entity, LineageEvent, MatchCandidate, SourceRecord

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

CREATE INDEX IF NOT EXISTS idx_source_records_entity ON source_records(tenant_id, domain, entity_id);
CREATE INDEX IF NOT EXISTS idx_source_records_scope ON source_records(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_entities_scope ON entities(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_match_candidates_scope_status ON match_candidates(tenant_id, domain, status);
CREATE INDEX IF NOT EXISTS idx_lineage_scope_entity ON lineage(tenant_id, domain, entity_id);
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
                "INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, "
                "attributes, ingested_at, entity_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (rec.id, rec.tenant_id, rec.domain, rec.source_system, rec.source_record_id,
                 json.dumps(rec.attributes), rec.ingested_at, rec.entity_id),
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
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO entities (id, tenant_id, domain, status, attributes, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ent.id, ent.tenant_id, ent.domain, ent.status, json.dumps(ent.attributes),
                 ent.created_at, ent.updated_at),
            )

    def get_entity(self, entity_id: str, tenant_id: str, domain: str) -> Entity | None:
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

    # --- match candidates --------------------------------------------------
    def save_candidate(self, c: MatchCandidate) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, "
                "match_type, reasons, status, created_at, decided_at, decided_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (c.id, c.tenant_id, c.domain, c.record_a_id, c.record_b_id, c.score, c.match_type,
                 json.dumps(c.reasons), c.status, c.created_at, c.decided_at, c.decided_by),
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
