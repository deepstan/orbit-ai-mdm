"""
EVIDENCE for one review item, group-aware: everything a person (or an agent working next to one) needs to decide, in one call.

With the kernel engine a review item is a record compared with a GROUP, shown against the group's closest member, so two records are not enough: the decision depends on who else is in
each group and on what each group's golden record looks like. This returns both records, both GROUPS (members, golden record, size), whether a person ever separated these records or
their entities, the history of what each record used to say, and the record's other pairs. For a group of one (every pair the older engines made) it is simply two records.

Read-only, and everything that leaves is masked with the domain's protected-field rules (the same masking the review queue uses).
"""
from __future__ import annotations

from typing import Callable

MAX_MEMBERS = 25        # members listed per group (member_count says how many there are)
MAX_HISTORY = 10        # latest versions listed per record
MAX_OTHER_PAIRS = 20


def _record(rec, mask: Callable) -> dict | None:
    if rec is None:
        return None
    return {"id": rec.id, "source_system": rec.source_system, "source_record_id": rec.source_record_id, "version": rec.version, "entity_id": rec.entity_id,
            "attributes": mask(rec.attributes), "ingested_at": rec.ingested_at, "updated_at": rec.updated_at, "processing_status": rec.processing_status}


def _group(entity, members: list, mask: Callable) -> dict | None:
    if entity is None:
        return None
    return {"entity_id": entity.id, "status": entity.status, "golden": mask(entity.attributes), "member_count": len(members),
            "members": [{"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id, "attributes": mask(r.attributes)} for r in members[:MAX_MEMBERS]],
            "truncated": len(members) > MAX_MEMBERS}


def _history(store, tenant_id: str, domain: str, record_id: str | None, mask: Callable) -> list:
    if record_id is None:
        return []
    rows = store.list_record_history(record_id, tenant_id, domain)[-MAX_HISTORY:]
    return [{"version": r["version"], "attributes": mask(r["attributes"]), "superseded_at": r["superseded_at"]} for r in rows]


def record_history(store, tenant_id: str, domain: str, record_id: str, mask: Callable) -> dict | None:
    """The record as it is now and every earlier version a change replaced. None if the tenant has no such record."""
    rec = store.get_source_records_batch(tenant_id, domain, [record_id]).get(record_id)
    if rec is None:
        return None
    rows = store.list_record_history(record_id, tenant_id, domain)
    return {"record": _record(rec, mask),
            "history": [{"version": r["version"], "attributes": mask(r["attributes"]), "superseded_at": r["superseded_at"]} for r in rows]}


def build_pair_evidence(store, tenant_id: str, domain: str, candidate_id: str, mask: Callable) -> dict | None:
    """None if this tenant has no such candidate in this domain (the same answer for 'does not exist' and 'belongs to someone else')."""
    c = store.get_candidate(candidate_id, tenant_id, domain)
    if c is None:
        return None
    records = store.get_source_records_batch(tenant_id, domain, [c.record_a_id, c.record_b_id])
    a, b = records.get(c.record_a_id), records.get(c.record_b_id)
    entity_ids = sorted({r.entity_id for r in (a, b) if r is not None and r.entity_id})
    entities = {eid: store.get_entity(eid, tenant_id, domain) for eid in entity_ids}
    members: dict = {eid: [] for eid in entity_ids}
    for r in (store.list_records_of_entities(tenant_id, domain, entity_ids) if entity_ids else []):
        members.setdefault(r.entity_id, []).append(r)
    ea, eb = (a.entity_id if a else None), (b.entity_id if b else None)

    partners = store.separated_partners(tenant_id, domain, a.id) if a is not None else set()
    group_b_ids = {r.id for r in members.get(eb, [])} if eb else set()
    entities_separated = store.entities_separated(tenant_id, domain, ea, eb) if (ea and eb and ea != eb) else None

    others = []
    if a is not None:
        for other_id, pair in list(store.list_candidate_pairs(tenant_id, domain, a.id).items())[:MAX_OTHER_PAIRS]:
            if pair["id"] != c.id:
                others.append({"candidate_id": pair["id"], "other_record_id": other_id, "status": pair["status"], "score": pair["score"]})
    return {
        "candidate": {"candidate_id": c.id, "status": c.status, "score": c.score, "match_type": c.match_type, "reasons": c.reasons, "contributions": c.contributions,
                      "decided_by": c.decided_by, "created_at": c.created_at},
        "record_a": _record(a, mask),            # the record that was being matched
        "record_b": _record(b, mask),            # the member of the other group it resembles most
        "group_a": _group(entities.get(ea), members.get(ea, []), mask),
        "group_b": _group(entities.get(eb), members.get(eb, []), mask),
        "same_entity": bool(ea and ea == eb),
        "separations": {"records_separated_by_a_person": bool(b is not None and b.id in partners),
                        "a_was_separated_from_a_record_in_group_b": bool(partners & group_b_ids),
                        "entities_separated_by_a_person": bool(entities_separated)},
        "history": {"record_a": _history(store, tenant_id, domain, a.id if a else None, mask), "record_b": _history(store, tenant_id, domain, b.id if b else None, mask)},
        "other_pairs_of_record_a": others,
    }
