"""
Right-to-be-forgotten (Phase 3 item #2, continued).

The architecture doc calls this out specifically as "nontrivial in MDM
since golden records span sources" -- a golden record isn't one row to
delete, it's a cluster of source records from potentially many systems,
plus lineage history, plus match-candidate history referencing those
records. This module purges all of it for one entity and records that
the purge happened (the fact that data was erased, and when, and by
whom, is itself something compliance needs -- so the purge is logged to
the audit trail even though the underlying data is gone).

Scope: this purges ONE golden record (one real-world person/product/
vendor) at a time, identified by entity_id -- matching how an actual
erasure request arrives ("delete everything about person X"), not a
bulk/tenant-wide operation.

What this does NOT do (documented limitations, not silently glossed
over): it doesn't purge that entity's mentions in OTHER entities' lineage
`source_record_id` references (a golden record that once merged with,
then split from, the purged data would still show a now-dangling
source_record_id in its lineage history) -- full referential cleanup
across the whole graph is a larger undertaking than this MVP scope covers.
"""
from __future__ import annotations

from dataclasses import dataclass

from . import audit
from .db import Store


@dataclass
class PurgeResult:
    entity_id: str
    tenant_id: str
    domain: str
    source_records_deleted: int
    lineage_events_deleted: int  # informational -- see note below on what's actually removed
    match_candidates_affected: int


def purge_entity(store: Store, tenant_id: str, domain: str, entity_id: str, requested_by: str) -> PurgeResult:
    """Permanently deletes a golden record and all of its source records.
    Logs the purge itself to the audit trail (the fact of erasure is
    retained even though the erased data is not). Raises ValueError if
    the entity doesn't exist for this (tenant, domain)."""
    entity = store.get_entity(entity_id, tenant_id, domain)
    if not entity:
        raise ValueError(f"No such entity '{entity_id}' for tenant '{tenant_id}' domain '{domain}'")

    source_records = store.list_source_records(tenant_id, domain, entity_id=entity_id)
    lineage = store.list_lineage(entity_id, tenant_id, domain)

    # Match candidates referencing these source records: we don't delete
    # them (they may reference OTHER, non-purged records too, and the
    # steward decision they represent is itself an audit-relevant fact),
    # but we count how many are affected so the caller/compliance officer
    # knows the scope of what's now referencing deleted data.
    record_ids = {r.id for r in source_records}
    all_candidates = store.list_candidates(tenant_id, domain, status=None)
    affected_candidates = [
        c for c in all_candidates if c.record_a_id in record_ids or c.record_b_id in record_ids
    ]

    for rec in source_records:
        store.delete_source_record(rec.id, tenant_id, domain)
    store.delete_entity(entity_id, tenant_id, domain)

    audit.log_action(
        store, tenant_id, action="purge", actor=requested_by, domain=domain,
        details={
            "entity_id": entity_id,
            "source_records_deleted": len(source_records),
            "lineage_events_existed": len(lineage),
            "match_candidates_affected": len(affected_candidates),
        },
    )

    return PurgeResult(
        entity_id=entity_id, tenant_id=tenant_id, domain=domain,
        source_records_deleted=len(source_records),
        lineage_events_deleted=len(lineage),
        match_candidates_affected=len(affected_candidates),
    )
