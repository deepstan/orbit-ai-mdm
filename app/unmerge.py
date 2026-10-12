"""
Unmerge: a person says two records that were merged are NOT the same, and the entity is split to match.

WHAT IT UNDOES. One confirmed pair (an automatic merge or a steward's confirmation), chosen by a person. The pair becomes a rejected candidate decided by
"unmerge:<who>": a separation. That is the system's only other kind of human "these are different" besides a steward's rejection of a review pair, and it is
a training label like any human decision.

HOW IT SPLITS. Take the entity's records and the OTHER confirmed pairs between them. Follow those from each of the two records.
  * If the two records still reach each other (A-C-B), the entity does not split and unmerge REFUSES, showing the path: the person has to say which
    merge on that path is the wrong one.
  * Otherwise each record has a side. The side holding the OLDEST record keeps the entity id (systems downstream hold entity ids; the established one stays),
    the other side moves to a NEW entity. Records that no confirmed pair reaches from either side (they are in the entity by some other route) stay where they are.
Both golden records are rebuilt. Entities that were merged into this one earlier stay tombstones: nothing is resurrected.

KEPT TRUE, BY EVERY ENGINE. The regular engine never merges two entities when a person separated records between them (it queues the pair for review instead),
nor re-proposes the separated pair; confirming such a pair by hand is refused. The bulk, onboarding and delta engines do the same inside their clustering
(app/bulk.py): an automatic merge that would join two components holding records on either side of a separation is held for review, and a pair that IS a separated
pair is dropped. `separation_violations` (and `domains_cli.py separation-check`) remains the check that every separation still holds.

CRASH SAFETY. The steps run in an order that leaves a state the same call finishes: new entity, moved records, a durable "golden is stale" marker for both
entities, THEN the rejection, then the rebuilds. A repeat of an interrupted unmerge sees records already apart and a still-confirmed pair, and completes it.
"""
from __future__ import annotations

from collections import defaultdict, deque

from .models import Entity, now_iso
from .pipeline import rebuild_golden_record

SEPARATED_BY = "unmerge:"


class CannotUnmerge(ValueError):
    """The request cannot be done as asked; the message says why and, when the entity stays connected, which merges hold it together."""


def _reach(start: str, adjacency: dict):
    """(set of records reachable from start, parent pointers) over the confirmed pairs that remain."""
    seen, parent, queue = {start}, {start: None}, deque([start])
    while queue:
        node = queue.popleft()
        for other, via in adjacency[node]:
            if other not in seen:
                seen.add(other)
                parent[other] = (node, via)
                queue.append(other)
    return seen, parent


def plan_unmerge(store, tenant_id: str, domain: str, candidate_id: str) -> dict:
    """What unmerging this pair would do, without doing it. Raises CannotUnmerge when it cannot be done."""
    cand = store.get_candidate(candidate_id, tenant_id, domain)
    if cand is None:
        raise CannotUnmerge(f"No such candidate: {candidate_id}")
    if cand.status == "rejected" and (cand.decided_by or "").startswith(SEPARATED_BY):
        raise CannotUnmerge(f"{candidate_id} was already unmerged by {cand.decided_by[len(SEPARATED_BY):]}")
    if cand.status != "confirmed":
        raise CannotUnmerge(f"Only a confirmed merge can be unmerged; {candidate_id} is {cand.status}. (A pending pair is decided with a steward's confirm or reject.)")
    rec_a, rec_b = (store.get_source_record(r, tenant_id, domain) for r in (cand.record_a_id, cand.record_b_id))
    if rec_a is None or rec_b is None:
        raise CannotUnmerge(f"A record of {candidate_id} no longer exists")
    plan = {"candidate_id": candidate_id, "record_a_id": rec_a.id, "record_b_id": rec_b.id, "entity_id": rec_a.entity_id}
    if rec_a.entity_id != rec_b.entity_id:
        return {**plan, "already_apart": True, "entities": [rec_a.entity_id, rec_b.entity_id], "move": [], "keep": [], "left_alone": []}   # an interrupted unmerge: finish it
    members = store.list_source_records(tenant_id, domain, entity_id=rec_a.entity_id)
    adjacency = defaultdict(set)
    for cid, a, b in store.confirmed_pairs_among(tenant_id, domain, [m.id for m in members]):
        if cid != candidate_id:
            adjacency[a].add((b, cid))
            adjacency[b].add((a, cid))
    side_a, _ = _reach(rec_a.id, adjacency)
    side_b, parent = _reach(rec_b.id, adjacency)
    if rec_a.id in side_b:   # still connected: say which merges hold it together
        path, node = [], rec_a.id
        while parent[node] is not None:
            node, via = parent[node]
            path.append(via)
        raise CannotUnmerge(f"{candidate_id} cannot be undone on its own: the same two records are still joined through other confirmed pairs "
                            f"({', '.join(path)}). Unmerge the pair that is actually wrong.")
    age = {m.id: (m.ingested_at or "", m.id) for m in members}
    keeper = side_a if min(age[r] for r in side_a) <= min(age[r] for r in side_b) else side_b
    mover = side_b if keeper is side_a else side_a
    left = [m.id for m in members if m.id not in side_a and m.id not in side_b]
    return {**plan, "already_apart": False, "move": sorted(mover), "keep": sorted(keeper), "left_alone": sorted(left)}


def unmerge_pair(store, tenant_id: str, domain: str, candidate_id: str, by: str, reason: str = "", dry_run: bool = False) -> dict:
    """Unmerge one confirmed pair. Returns {candidate_id, split, kept_entity, new_entity, moved, ...}."""
    if not by or not by.strip():
        raise CannotUnmerge("Say who is unmerging (by): the separation is a person's decision and is recorded as theirs")
    plan = plan_unmerge(store, tenant_id, domain, candidate_id)
    note = f"unmerged by {by}" + (f": {reason}" if reason else "")
    if dry_run:
        return {**plan, "dry_run": True}
    if plan["already_apart"]:   # the records were moved but the call died before it finished: record the separation and rebuild what may be stale
        dirty = [(tenant_id, domain, e) for e in plan["entities"] if e]
        store.mark_golden_dirty(dirty)
        store.update_candidate_status(candidate_id, tenant_id, domain, "rejected", SEPARATED_BY + by, now_iso())
        for _, _, entity_id in dirty:
            rebuild_golden_record(store, tenant_id, domain, entity_id, reason=f"{note} ({candidate_id})")
        store.clear_golden_dirty(dirty)
        store.record_merge_audit(candidate_id, tenant_id, domain, "wrong", by)   # if this merge was in the audit sample, unmerging it IS the verdict (False if it was not flagged)
        return {**plan, "split": False, "kept_entity": plan["entity_id"], "new_entity": None, "moved": [], "note": "the records were already in different entities; the separation is now recorded and both golden records rebuilt"}
    old = plan["entity_id"]
    new = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
    store.save_entity(new)
    for rid in plan["move"]:
        store.set_record_entity(rid, tenant_id, domain, new.id)
    dirty = [(tenant_id, domain, old), (tenant_id, domain, new.id)]
    store.mark_golden_dirty(dirty)                                    # durable: a crash from here on leaves both golden records marked stale, and process-pending rebuilds them
    store.update_candidate_status(candidate_id, tenant_id, domain, "rejected", SEPARATED_BY + by, now_iso())
    for entity_id in (old, new.id):
        rebuild_golden_record(store, tenant_id, domain, entity_id, reason=f"{note} ({candidate_id})")
    store.clear_golden_dirty(dirty)
    store.record_merge_audit(candidate_id, tenant_id, domain, "wrong", by)   # a flagged merge that a person unmerges was wrong: that is the audit's answer (a merge nobody flagged stays out of the sample)
    return {**plan, "split": True, "kept_entity": old, "new_entity": new.id, "moved": plan["move"], "note": note}


def separation_violations(store, tenant_id: str, domain: str) -> list:
    """Separations a person made whose two records have ended up in one entity again (a check that every separation still holds, whatever wrote the data: no engine should leave any)."""
    return store.separation_violations(tenant_id, domain)
