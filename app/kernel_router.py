"""
The kernel as the pipeline's matching DECISION STEP. app/pipeline.py lands, stages, tracks status, resumes, keeps history, publishes golden records and logs runs; this module answers
the one question in the middle, "which group (entity) does this record belong to?", and writes the answer in the product's own tables, so everything built on them keeps working:
the review queue, a steward's confirm and reject, unmerge, audits, lineage, the learned scorer.

NO NEW TABLES. A group is built on demand from what is already stored: the entity's member records, their staged forms, and the blocking index. So the kernel always sees CURRENT
membership: a steward's confirm, an unmerge, a purge or a legacy bulk load cannot leave it stale.

HOW A DECISION IS WRITTEN (the product's model is "an entity is the union of the records its confirmed pairs connect"; the kernel keeps that model):
  * a record that joins a group gets ONE confirmed pair to the group's closest member (the evidence), so an entity is a TREE of pairs and unmerge always has one edge to remove
  * two groups that are merged get one confirmed pair too
  * a record that does not clearly belong anywhere is its own entity, with pending pairs for a steward to look at
  * a record whose data CHANGED is matched again like a new one (Senzing and Reltio: the policy the owner chose), so it can leave its group; it never leaves if a PERSON tied it to
    that group (a human decision is a constraint), and the members it leaves behind are kept connected
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

from . import active_learning, kernel, learned_scorer, llm_adjudication, pipeline
from .matching import classify, score_pair
from .models import Entity, MatchCandidate, now_iso
from .staging import stage_record

logger = logging.getLogger("mdm.matching")
PSEUDO = "rec:"          # a record that has no entity yet (an interrupted run) is a group of its own, named by its record id


@dataclass
class Plan:
    gid: str
    group: kernel.GroupView
    view: dict
    res: kernel.Result
    rep: object = None                                      # the member record closest to the new one
    decision: str = "no_match"
    promoted_p: float | None = None
    llm: object = None
    candidate: MatchCandidate | None = None
    blocked: str | None = None
    notes: list = field(default_factory=list)
    skey: tuple = ()                                        # what this group's earliest-named member IS (source system, source record id): breaks ties between equally good groups


# ------------------------------------------------------------------------------------------------ small helpers
def _held_by_a_person(store, tenant_id: str, domain: str, record_id: str) -> bool:
    """Did a PERSON confirm a pair for this record? Then it stays where they put it, whatever its data now says."""
    for other, pair in store.list_candidate_pairs(tenant_id, domain, record_id).items():
        if pair["status"] == "confirmed":
            c = store.get_candidate(pair["id"], tenant_id, domain)
            if c and not (c.decided_by or "").startswith("system:"):
                return True
    return False


def _supersede_system_edges(store, tenant_id: str, domain: str, record_id: str) -> int:
    n = 0
    for other, pair in store.list_candidate_pairs(tenant_id, domain, record_id).items():
        if pair["status"] == "confirmed":
            store.update_candidate_status(pair["id"], tenant_id, domain, "superseded", "system:kernel_update", now_iso())
            n += 1
    return n


def _nat(r) -> tuple:
    """What a record IS: its source system and its id there. Ties are settled by this and never by an id the run made up (a random one gives a different answer in every run)."""
    return (r.source_system or "", r.source_record_id or "")


def _plan_order(p) -> tuple:
    """Best first. Equal scores are ordered by what the groups' records ARE, and only then (a tie of the tie) by the group id: used for both the groups to join and the groups to review."""
    return (-round(p.res.score, 9), p.skey, p.gid)


def _group_keys(store, tenant_id: str, domain: str, gids) -> dict:
    """gid -> the smallest (source system, source record id) among the group's members, for the groups whose order matters."""
    real = [g for g in gids if not g.startswith(PSEUDO)]
    out: dict = {}
    for r in (store.list_records_of_entities(tenant_id, domain, real) if real else []):
        out[r.entity_id] = min(out.get(r.entity_id, _nat(r)), _nat(r))
    for rid, r in store.get_source_records_batch(tenant_id, domain, [g[len(PSEUDO):] for g in gids if g.startswith(PSEUDO)]).items():
        out[f"{PSEUDO}{rid}"] = _nat(r)
    return out


def _best_n(store, tenant_id: str, domain: str, ordered: list, weight: dict, n: int) -> list:
    """The best n groups. A tie ACROSS the cut-off must not be settled by an id: the tied groups are ordered by their records' natural keys and the first ones kept."""
    boundary = round(weight[ordered[n - 1]], 9)
    keep = [g for g in ordered if round(weight[g], 9) > boundary]
    tied = [g for g in ordered if round(weight[g], 9) == boundary]
    need = n - len(keep)
    if len(tied) <= need:
        return keep + tied
    keys = _group_keys(store, tenant_id, domain, tied)
    tied.sort(key=lambda g: (keys.get(g, ("", "")), g))
    return keep + tied[:need]


def _closest_member(config, rec_std: dict, group: kernel.GroupView, matching_by_id: dict, never: frozenset = frozenset(), order: dict | None = None):
    """The member the new record resembles most, and NEVER one a person separated it from: the pair that is stored and shown as the evidence must not be a pair a steward
    said is not the same. None if every member is such a partner."""
    best_id, best_score = None, -1.0
    for mid in group.members:
        if mid in never:
            continue
        s = score_pair(rec_std, matching_by_id[mid], config).score if mid in matching_by_id else 0.0
        if s > best_score or (s == best_score and (best_id is None or (order or {}).get(mid, (mid,)) < (order or {}).get(best_id, (best_id,)))):
            best_id, best_score = mid, s
    return best_id


def _separation_block(store, tenant_id: str, domain: str, mine: str | None, plan: Plan, partners: set) -> str | None:
    """Why this group may not be joined automatically, or None."""
    hit = partners & set(plan.group.members)
    if hit:
        return "a person separated records in these two entities (this record and a record of that group)"
    if mine and not plan.gid.startswith(PSEUDO) and mine != plan.gid:
        blocked_by = store.entities_separated(tenant_id, domain, mine, plan.gid)
        if blocked_by:
            return f"a person separated records in these two entities ({blocked_by})"
    return None


def _relink(store, tenant_id: str, domain: str, config, entity_id: str) -> int:
    """After a member left an entity: if the confirmed pairs no longer connect the members who stayed, add system pairs that do. An entity is the union of what its pairs connect,
    and unmerge works on those pairs, so the members who stay must still be ONE connected piece."""
    members = store.list_source_records(tenant_id, domain, entity_id=entity_id)
    ids = [m.id for m in members]
    if len(ids) < 2:
        return 0
    nat = {m.id: _nat(m) for m in members}
    parent = {i: i for i in ids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for _cid, a, b in store.confirmed_pairs_among(tenant_id, domain, ids):
        if a in parent and b in parent:
            parent[find(a)] = find(b)
    pieces = defaultdict(list)
    for i in sorted(ids, key=lambda i: nat[i]):
        pieces[find(i)].append(i)
    if len(pieces) < 2:
        return 0
    roots = sorted(pieces.values(), key=lambda p: nat[p[0]])
    std = store.get_matching_many(tenant_id, domain, [p[0] for p in roots])
    anchor, added = roots[0][0], 0
    for piece in roots[1:]:
        other = piece[0]
        sb = score_pair(std[anchor], std[other], config) if anchor in std and other in std else None
        store.save_candidate(MatchCandidate(
            record_a_id=other, record_b_id=anchor, tenant_id=tenant_id, domain=domain, score=sb.score if sb else 0.0, match_type="relink",
            reasons=["kept together: the record that connected these two parts of the entity has left it"], contributions=sb.contributions if sb else [],
            status="confirmed", decided_at=now_iso(), decided_by="system:kernel_relink"))
        added += 1
    return added


def _separations(store, tenant_id: str, domain: str, ctx) -> bool:
    """Has a person separated anything in this domain? Realtime asks every time (a person may separate records between two calls); a batch asks once per transaction (the
    answer cannot change inside it on SQLite, and on PostgreSQL the window is one transaction)."""
    if ctx.defer_golden is None:
        return store.has_separations(tenant_id, domain)
    if ctx.has_separations is None:
        ctx.has_separations = store.has_separations(tenant_id, domain)
    return ctx.has_separations


def _create_entity(store, tenant_id: str, domain: str, config, rec, cleansed: dict | None, ctx) -> str:
    """A record that belongs to no group becomes an entity of its own. Its golden record is computed from the record we already hold and written WITH the entity: one write.
    (Creating an empty entity and rebuilding it afterwards took six statements: write it, mark it stale, then read its records, its row and its staging, write its lineage, write it again.)"""
    if cleansed is None:                                  # a caller that cannot give the cleansed form: the long way
        entity_id = _new_entity(store, tenant_id, domain)
        store.set_record_entity(rec.id, tenant_id, domain, entity_id)
        pipeline._publish_golden(store, tenant_id, domain, entity_id, "recomputed after ingestion/merge", ctx)
        return entity_id
    entity = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
    decisions, issues = pipeline.compute_golden(config, domain, [rec], {rec.id: cleansed})
    reason = "rebuilt at the end of a batch run" if ctx.defer_golden is not None else "recomputed after ingestion/merge"      # the words the old path wrote
    entity.attributes, lineage = pipeline.golden_values(decisions, {}, entity.id, tenant_id, domain, reason)
    entity.quality_issues = issues
    store.save_entity(entity)
    store.set_record_entity(rec.id, tenant_id, domain, entity.id)
    if ctx.lineage_rows is not None:
        ctx.lineage_rows.extend(lineage)                  # written once, with the rest of this transaction's
    else:
        store.add_lineage_batch(lineage)
    return entity.id


def _new_entity(store, tenant_id: str, domain: str) -> str:
    entity = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
    store.save_entity(entity)
    return entity.id


# ------------------------------------------------------------------------------------------------ the decision step
def route_record(store, tenant_id: str, domain: str, config, rec, std_new: dict, new_keys: set, learned_model, existing_entity: str | None = None,
                 ctx=None, known_pairs: dict | None = None, cleansed: dict | None = None, maybe_shared: set | None = None) -> dict:
    """Same contract as the pairwise step it replaces (pipeline._route_record_legacy): `rec` and its blocking keys are already stored; returns the same result dict."""
    ctx = ctx or pipeline.REALTIME
    cfg = kernel.settings()
    scorer = kernel.make_scorer(cfg.scorer, config)
    result = {"record_id": rec.id, "auto_merged_with": [], "queued_for_review": [], "matched_entity_id": existing_entity, "skipped_overpopulated_keys": []}

    # ---- is this an UPDATE of a record in a multi-member entity (it may leave), or a record that is held where it is?
    is_update = existing_entity is not None and known_pairs is None
    own_members = store.list_source_records(tenant_id, domain, entity_id=existing_entity) if existing_entity else []
    leaving = existing_entity if (is_update and len(own_members) > 1 and not _held_by_a_person(store, tenant_id, domain, rec.id)) else None

    # ---- resume: pairs already decided before an interruption are not decided twice
    for other_id, known in (known_pairs or {}).items():
        if known["status"] == "confirmed":
            result["matched_entity_id"] = pipeline.merge_two_records(store, tenant_id, domain, rec.id, other_id, reason="auto-merge (resumed)", ctx=ctx)
            result["auto_merged_with"].append({"record_id": other_id, "score": known["score"]})
        elif known["status"] == "pending":
            result["queued_for_review"].append({"candidate_id": known["id"], "record_id": other_id, "score": known["score"], "reasons": []})

    # ---- candidate RECORDS from the blocking index (a key held by too many records finds everyone and no one: it is skipped, and reported)
    # With a key filter (a single-writer bulk load) a key it says NO other record holds is not looked up: asking would only find this record's own entry (size 1: never over the cap, and
    # no other member to compare with), so leaving it out changes nothing. Without one, every key is looked up, as always.
    lookup = new_keys if maybe_shared is None else {k for k in new_keys if k in maybe_shared}
    sizes = store.blocking_bucket_sizes(tenant_id, domain, lookup) if lookup else {}
    skipped = sorted(k for k in lookup if k != pipeline.NO_BLOCK_KEY and sizes.get(k, 0) - 1 > config.max_bucket_size)
    usable = {k for k in new_keys if k != pipeline.NO_BLOCK_KEY and k not in set(skipped)}
    to_ask = usable if maybe_shared is None else usable & lookup
    result["skipped_overpopulated_keys"] = skipped
    if skipped:
        logger.info("domain %s: %d blocking key(s) over the %d-record cap were not used for lookup: %s", domain, len(skipped), config.max_bucket_size, skipped[:5])
    separated = _separations(store, tenant_id, domain, ctx)
    partners = store.separated_partners(tenant_id, domain, rec.id) if separated else set()
    banned = {rec.id} | partners
    if existing_entity and leaving is None:                # held where it is, or being resumed: its own entity's members are not candidates
        banned |= {m.id for m in own_members}
    asked = store.blocking_members(tenant_id, domain, to_ask) if (to_ask or maybe_shared is None) else {}          # without a filter always asked, even with no usable key: one lookup per record
    by_key = {k: [r for r in rids if r not in banned] for k, rids in asked.items()}
    candidate_ids = {r for rids in by_key.values() for r in rids}

    # ---- map them to GROUPS (entities)
    ents = store.get_record_entities(tenant_id, domain, list(candidate_ids | {rec.id})) if candidate_ids else {}
    group_of = {r: (ents.get(r) or f"{PSEUDO}{r}") for r in candidate_ids}
    own_now = ents.get(rec.id) if candidate_ids else None
    if known_pairs:                                         # the groups the resumed pairs already settled
        settled = set(store.get_record_entities(tenant_id, domain, list(known_pairs)).values()) | ({own_now} if own_now else set())
    else:
        settled = {own_now} if (own_now and leaving is None) else set()
    group_ids = {g for g in group_of.values() if g not in settled}

    # ---- rank the groups by the evidence that found them, then keep as many as the settings allow
    weight: dict = defaultdict(float)
    for rids in by_key.values():
        gs = {group_of[r] for r in rids if group_of[r] in group_ids}
        for g in gs:
            weight[g] += 1.0 / len(gs) if cfg.rank == "idf" else 1.0
    ordered = sorted(weight, key=lambda g: (-round(weight[g], 9), g))     # rounded: sums that are equal on paper can differ in the last bit with the order they were added in
    if cfg.max_candidates and len(ordered) > cfg.max_candidates:
        ordered = _best_n(store, tenant_id, domain, ordered, weight, cfg.max_candidates)

    plans: list[Plan] = []
    groups: dict = {}
    matching_by_id: dict = {}
    cleansed_by_id: dict = {}
    members_of: dict = defaultdict(list)
    if ordered:
        real = [g for g in ordered if not g.startswith(PSEUDO)]
        for r in (store.list_records_of_entities(tenant_id, domain, real) if real else []):
            if r.id != rec.id:
                members_of[r.entity_id].append(r)
        for rid, r in store.get_source_records_batch(tenant_id, domain, [g[len(PSEUDO):] for g in ordered if g.startswith(PSEUDO)]).items():
            members_of[f"{PSEUDO}{rid}"].append(r)
        ids = [r.id for rs in members_of.values() for r in rs]
        staged = store.get_staged_many(tenant_id, domain, ids)
        for rs in members_of.values():
            for r in rs:
                m, c = staged[r.id] if r.id in staged else stage_record(domain, config, r.attributes)
                matching_by_id[r.id], cleansed_by_id[r.id] = m, c
        for g in ordered:
            if members_of.get(g):
                groups[g] = kernel.build_group_view(config, domain, g, members_of[g], matching_by_id, cleansed_by_id)

        # who holds each identifier: counted over EVERY candidate record, not only the groups that were kept for scoring
        holders: dict = defaultdict(set)
        for r, m in store.get_matching_many(tenant_id, domain, list(candidate_ids)).items():
            for f in config.deterministic_fields:
                if m.get(f):
                    holders[(f, m[f])].add(group_of[r])

        lscorer = None                                       # None = not looked up yet; False = the tenant has no live learned scorer
        for gid in ordered:
            grp = groups.get(gid)
            if grp is None:
                continue
            view = kernel.view_of(grp, std_new, config)
            res = scorer.score(std_new, view, kernel.Context(holders, {gid}))
            if learned_model is not None:
                blended = active_learning.blend_scores(res.score, learned_model, active_learning.extract_features(std_new, view, config))
                if abs(blended - res.score) > 0.01:
                    res.reasons = res.reasons + [f"blended with learned model ({learned_model.trained_on_examples} past decisions): {res.score:.2f} -> {blended:.2f}"]
                res.score = round(blended, 4)
            decision = classify(res.score)
            if decision == "no_match":
                continue
            rep_id = _closest_member(config, std_new, grp, matching_by_id, frozenset(partners), {r.id: _nat(r) for r in members_of[gid]})
            if rep_id is None:                               # every member of this group is one a person separated the record from: it is not even proposed
                continue
            plan = Plan(gid, grp, view, res, decision=decision, skey=min(_nat(r) for r in members_of[gid]))
            plan.rep = next(r for r in members_of[gid] if r.id == rep_id)
            if decision == "review":
                if lscorer is None:
                    lscorer = learned_scorer.active_scorer(tenant_id, domain) or False
                if lscorer:                                  # the tenant's learned scorer, if one is LIVE (and not rolled back), may promote a group the rule score sent to review
                    promoted, p = learned_scorer.promote(lscorer, res.score, res.match_type, res.contributions)
                    if promoted:
                        plan.decision, plan.promoted_p = "auto_merge", p
                        res.reasons = res.reasons + [f"promoted out of review by the learned scorer (p = {p:.3f})"]
            reasons = list(res.reasons) + [f"compared with a group of {len(grp.members)} record(s); closest member {plan.rep.source_system}/{plan.rep.source_record_id}"]
            plan.candidate = MatchCandidate(record_a_id=rec.id, record_b_id=plan.rep.id, tenant_id=tenant_id, domain=domain, score=res.score, match_type=res.match_type,
                                            reasons=reasons, contributions=res.contributions)
            if plan.decision == "review" and ctx.allow_llm and llm_adjudication.is_enabled() and llm_adjudication.in_adjudication_band(res.score):
                verdict = llm_adjudication.adjudicate(rec.attributes, plan.rep.attributes, res.reasons, domain=domain)
                if verdict is not None:
                    plan.llm = verdict
                    plan.candidate.reasons = plan.candidate.reasons + [f"LLM adjudication: {verdict.explanation}"]
                    if verdict.confidence >= 0.85:
                        plan.decision = "auto_merge" if verdict.is_match else "reject"
                        plan.candidate.match_type = "llm_adjudicated"
            if plan.decision == "auto_merge" and (separated or partners):
                plan.blocked = _separation_block(store, tenant_id, domain, existing_entity if leaving is None else None, plan, partners)
                if plan.blocked:
                    plan.decision = "review"
                    plan.candidate.reasons = plan.candidate.reasons + [f"not merged automatically: {plan.blocked}"]
            plans.append(plan)

    autos = sorted([p for p in plans if p.decision == "auto_merge"], key=_plan_order)
    for p in plans:
        if p.decision == "reject":
            p.candidate.status, p.candidate.decided_at, p.candidate.decided_by = "rejected", now_iso(), "system:llm_adjudicated"
            store.save_candidate(p.candidate)

    # ---- JOIN the best group
    joined = None
    if autos:
        joined = autos[0]
        result["matched_entity_id"] = _confirm(store, tenant_id, domain, rec, joined, ctx, result, leaving, own_members, config)
        # ---- other groups the record also matched: merged if their golden records match each other, otherwise queued for a person
        target_records = list(members_of[joined.gid]) + [rec]
        std_with_rec = dict(matching_by_id)
        std_with_rec[rec.id] = std_new
        staged_new = store.get_staged_many(tenant_id, domain, [rec.id])
        cleansed_with_rec = dict(cleansed_by_id)
        cleansed_with_rec[rec.id] = staged_new[rec.id][1] if rec.id in staged_new else stage_record(domain, config, rec.attributes)[1]
        target = kernel.build_group_view(config, domain, result["matched_entity_id"], target_records, std_with_rec, cleansed_with_rec)
        for other in autos[1:]:
            if cfg.merge_groups == "off":                    # off: two groups are never merged, and not even proposed
                continue
            merged, block = False, None
            if cfg.merge_groups == "auto":
                gvg = kernel.golden_vs_golden(config, scorer, target, other.group, holders)
                if classify(gvg.score) == "auto_merge":
                    mine = result["matched_entity_id"]
                    block = _separation_block(store, tenant_id, domain, mine, other, partners)
                    if not block:
                        edge = MatchCandidate(record_a_id=rec.id, record_b_id=other.rep.id, tenant_id=tenant_id, domain=domain, score=gvg.score, match_type="group_merge",
                                              reasons=list(gvg.reasons) + [f"the two groups were merged: their golden records match each other ({gvg.score:.2f})"],
                                              contributions=gvg.contributions, status="confirmed", decided_at=now_iso(), decided_by="system:group_merge")
                        store.save_candidate(edge)
                        if learned_scorer.audit_sampled(edge.id):
                            store.flag_merge_audits(tenant_id, domain, [edge.id])
                        result["matched_entity_id"] = pipeline.merge_two_records(store, tenant_id, domain, rec.id, other.rep.id, reason="two groups merged: their golden records match", ctx=ctx)
                        result["auto_merged_with"].append({"record_id": other.rep.id, "score": gvg.score})
                        target_records += list(members_of[other.gid])
                        target = kernel.build_group_view(config, domain, result["matched_entity_id"], target_records, std_with_rec, cleansed_with_rec)
                        merged = True
            if not merged:
                note = f"this record matched TWO groups strongly (this one and {joined.gid}); the groups were not merged automatically"
                other.candidate.reasons = other.candidate.reasons + [note + (f": {block}" if block else "")]
                _queue(store, other, result)

    # ---- the rest: a person looks at them
    queued = 0
    for p in sorted([p for p in plans if p.decision == "review"], key=_plan_order):
        if queued >= cfg.review_cap:
            break
        _queue(store, p, result)
        queued += 1

    # ---- EVERY record belongs to exactly one entity
    if joined is None and not result["auto_merged_with"]:      # (a resumed record may already have been united with its group above)
        if leaving is not None:                            # an updated record that matched nothing: it leaves the group it was in, alone
            result["matched_entity_id"] = _leave_alone(store, tenant_id, domain, config, rec, leaving, ctx)
        elif existing_entity is None:
            result["matched_entity_id"] = _create_entity(store, tenant_id, domain, config, rec, cleansed, ctx)
    return result


def _confirm(store, tenant_id: str, domain: str, rec, plan: Plan, ctx, result: dict, leaving: str | None, own_members: list, config) -> str:
    """The record joins the plan's group: the evidence pair is saved as confirmed, audit-sampled, and the entities are united (or the record is moved)."""
    c = plan.candidate
    c.status, c.decided_at = "confirmed", now_iso()
    c.decided_by = learned_scorer.PROMOTED_BY if plan.promoted_p is not None else "system:llm_adjudicated" if plan.llm else "system:auto_merge"
    if leaving is not None and plan.gid == leaving:         # an updated record that still belongs where it was: nothing moves, its old pairs still hold the entity together
        result["stayed"] = True                              # (it is not reported as merged: it was already there)
        return leaving
    result["auto_merged_with"].append({"record_id": plan.rep.id, "score": plan.res.score})
    store.save_candidate(c)
    if learned_scorer.audit_sampled(c.id, promoted=plan.promoted_p is not None):   # 2% of automatic merges, the rule's and the model's alike (the model's can be audited more), are flagged for a person to check
        store.flag_merge_audits(tenant_id, domain, [c.id])
    if leaving is not None and not plan.gid.startswith(PSEUDO):
        # an updated record that belongs to a DIFFERENT group now: it MOVES (the group it left is not merged into the new one)
        _supersede_system_edges_except(store, tenant_id, domain, rec.id, c.id)
        store.set_record_entity(rec.id, tenant_id, domain, plan.gid)
        _relink(store, tenant_id, domain, config, leaving)
        pipeline._publish_golden(store, tenant_id, domain, leaving, "a member left after its data changed", pipeline.REALTIME)
        pipeline._publish_golden(store, tenant_id, domain, plan.gid, f"record {rec.id} joined after its data changed", ctx)
        return plan.gid
    if leaving is not None:                                  # joining a group that has no entity yet: detach first, then unite
        _leave_alone(store, tenant_id, domain, config, rec, leaving, ctx)
    return pipeline.merge_two_records(store, tenant_id, domain, rec.id, plan.rep.id, reason=f"auto-merge ({c.reasons})", ctx=ctx)


def _supersede_system_edges_except(store, tenant_id: str, domain: str, record_id: str, keep_id: str) -> None:
    for other, pair in store.list_candidate_pairs(tenant_id, domain, record_id).items():
        if pair["status"] == "confirmed" and pair["id"] != keep_id:
            store.update_candidate_status(pair["id"], tenant_id, domain, "superseded", "system:kernel_update", now_iso())


def _leave_alone(store, tenant_id: str, domain: str, config, rec, leaving: str, ctx) -> str:
    """An updated record that matches nothing leaves its entity and becomes one of its own; the members who stay are kept connected."""
    _supersede_system_edges(store, tenant_id, domain, rec.id)
    entity_id = _new_entity(store, tenant_id, domain)
    store.set_record_entity(rec.id, tenant_id, domain, entity_id)
    _relink(store, tenant_id, domain, config, leaving)
    pipeline._publish_golden(store, tenant_id, domain, leaving, "a member left after its data changed", ctx)
    pipeline._publish_golden(store, tenant_id, domain, entity_id, "recomputed after ingestion/merge", ctx)
    return entity_id


def _queue(store, plan: Plan, result: dict) -> None:
    store.save_candidate(plan.candidate)
    result["queued_for_review"].append({"candidate_id": plan.candidate.id, "record_id": plan.rep.id, "score": plan.res.score, "reasons": plan.candidate.reasons})
