"""
Ties standardize -> match -> route -> survive -> lineage together.
Domain-agnostic: every function takes (tenant_id, domain) explicitly, and
domain-specific behavior comes entirely from `domains.get_domain_config()`.
No function in this module hardcodes a customer or product field name.
"""
from __future__ import annotations

from . import active_learning, llm_adjudication
from .db import Store
from .domains import get_domain_config
from .indexing import ensure_index_ready
from .matching import blocking_keys, classify, score_pair
from .models import Entity, LineageEvent, MatchCandidate, SourceRecord, now_iso
from .standardize import standardize
from .survivorship import build_golden_attributes


def ingest_record(store: Store, tenant_id: str, domain: str, source_system: str,
                   source_record_id: str, attributes: dict) -> dict:
    """Ingests one raw record, runs it against this (tenant, domain)'s
    existing records for matches, and either auto-merges, queues for
    steward review, or leaves it standalone.

    Candidate lookup is INDEXED, not a full-table scan: the new record's
    blocking keys (see matching.py's blocking_keys()) are computed once,
    persisted, and used to query the blocking_keys table directly for
    other records sharing at least one key -- store.find_candidate_record_ids()
    hits an index (tenant_id, domain, blocking_key), so this stays fast
    as the dataset grows, instead of re-fetching and re-standardizing
    every existing record on every single ingest call (the previous
    behavior, which made bulk loads effectively O(n^2)).

    For ingesting many records at once, see ingest_batch() below -- it
    batches the WRITE side (fewer commits) while still routing each
    record through this same matching logic one at a time, since match
    decisions are inherently sequential (record #500 might match record
    #2, but only because of what #2 became earlier in the same run).
    """
    config = get_domain_config(domain)  # raises KeyError early for an unknown domain
    ensure_index_ready(store, domain, config)  # refuses if the domain's config changed and the index wasn't rebuilt
    rec = SourceRecord(source_system=source_system, source_record_id=source_record_id,
                        attributes=attributes, tenant_id=tenant_id, domain=domain)
    store.save_source_record(rec)

    std_new = standardize(domain, rec.attributes)
    new_keys = blocking_keys(std_new, config)
    store.save_blocking_keys(tenant_id, domain, rec.id, new_keys)

    learned_model = active_learning.load_model(tenant_id, domain)
    return _route_record(store, tenant_id, domain, config, rec, std_new, new_keys, learned_model)


def ingest_batch(store: Store, tenant_id: str, domain: str,
                  records: list[tuple[str, str, dict]]) -> list[dict]:
    """Bulk-ingest version of ingest_record(): records is a list of
    (source_system, source_record_id, attributes) tuples. Returns one
    result dict per input record, in the same order, IDENTICAL to what
    calling ingest_record() that many times would produce.

    Only the raw SourceRecord rows are batched (one bulk INSERT, one
    commit, instead of one commit per record) -- blocking keys and
    matching stay strictly per-record, in order, same as
    ingest_record().

    This is a deliberate correctness choice, not a missed optimization:
    an earlier version of this function also batch-wrote ALL records'
    blocking keys upfront, before matching any of them. That let record
    #3's candidate search see record #500's keys -- which hasn't
    "happened yet" in ingestion order -- producing MORE merges than
    calling ingest_record() N times would, not just faster ones (caught
    by comparing entity counts between the two paths on identical
    input: 327 vs 79 entities for the same 1000 records). Batching
    reads/writes that don't affect matching order is safe; batching the
    ones that do changes what the algorithm computes, not just how fast
    it computes it. If you need a genuinely different (batch-complete,
    order-independent) matching algorithm, that's a different piece of
    work -- an offline clustering pass, not a faster version of this
    one -- and isn't what's built here.
    """
    config = get_domain_config(domain)  # raises KeyError early for an unknown domain
    ensure_index_ready(store, domain, config)

    recs = [
        SourceRecord(source_system=ss, source_record_id=srid, attributes=attrs,
                     tenant_id=tenant_id, domain=domain)
        for ss, srid, attrs in records
    ]

    # Safe to batch: raw record storage doesn't affect matching at all
    # (matching only ever reads the blocking_keys table), so there's no
    # ordering hazard here -- unlike blocking keys, below.
    store.save_source_records_batch(recs)

    # Loaded once for the whole batch, not once per record -- safe, since
    # the model itself doesn't change mid-batch.
    learned_model = active_learning.load_model(tenant_id, domain)

    results = []
    for rec in recs:
        std_new = standardize(domain, rec.attributes)
        new_keys = blocking_keys(std_new, config)
        # Deliberately ONE record's keys at a time, right before that
        # record's own matching step -- see the docstring above for why
        # batching this part isn't safe.
        store.save_blocking_keys(tenant_id, domain, rec.id, new_keys)
        result = _route_record(store, tenant_id, domain, config, rec, std_new, new_keys, learned_model)
        results.append(result)
    return results


def _route_record(store: Store, tenant_id: str, domain: str, config, rec: SourceRecord,
                   std_new: dict, new_keys: set[str], learned_model) -> dict:
    """The matching/routing logic shared by ingest_record() and
    ingest_batch() -- assumes `rec` and its blocking keys are ALREADY
    persisted (both callers handle that themselves, one record at a
    time or in a batch respectively) and just does the candidate
    lookup, scoring, and merge/queue/standalone decision."""
    # Isolation point: scoped by BOTH tenant_id and domain, so a Product
    # ingest can never be compared against Customer records, or another
    # tenant's records of the same domain -- find_candidate_record_ids()
    # only ever queries within this exact (tenant_id, domain) pair.
    candidate_ids = store.find_candidate_record_ids(tenant_id, domain, new_keys, exclude_record_id=rec.id)

    std_by_id = {rec.id: std_new}
    for cid in candidate_ids:
        candidate_rec = store.get_source_record(cid, tenant_id, domain)
        if candidate_rec:  # guards against a rare race with a concurrent purge
            std_by_id[cid] = standardize(domain, candidate_rec.attributes)

    result = {"record_id": rec.id, "auto_merged_with": [], "queued_for_review": [], "matched_entity_id": None}

    for other_id in candidate_ids:
        if other_id not in std_by_id:
            continue  # the race-guard above skipped a now-deleted record
        breakdown = score_pair(std_by_id[rec.id], std_by_id[other_id], config)

        if learned_model is not None:
            features = active_learning.extract_features(std_by_id[rec.id], std_by_id[other_id], config)
            blended = active_learning.blend_scores(breakdown.score, learned_model, features)
            if abs(blended - breakdown.score) > 0.01:
                breakdown.reasons = breakdown.reasons + [
                    f"blended with learned model ({learned_model.trained_on_examples} past decisions): "
                    f"{breakdown.score:.2f} -> {blended:.2f}"
                ]
            breakdown.score = round(blended, 4)

        decision = classify(breakdown.score)
        if decision == "no_match":
            continue

        candidate = MatchCandidate(
            record_a_id=rec.id, record_b_id=other_id, tenant_id=tenant_id, domain=domain,
            score=breakdown.score, match_type=breakdown.match_type, reasons=breakdown.reasons,
        )

        # --- Optional LLM adjudication for the ambiguous middle band ---
        llm_verdict = None
        if decision == "review" and llm_adjudication.is_enabled() and llm_adjudication.in_adjudication_band(breakdown.score):
            other_rec = store.get_source_record(other_id, tenant_id, domain)
            llm_verdict = llm_adjudication.adjudicate(rec.attributes, other_rec.attributes, breakdown.reasons, domain=domain)
            if llm_verdict is not None:
                candidate.reasons = candidate.reasons + [f"LLM adjudication: {llm_verdict.explanation}"]
                if llm_verdict.is_match and llm_verdict.confidence >= 0.85:
                    decision = "auto_merge"
                    candidate.match_type = "llm_adjudicated"
                elif not llm_verdict.is_match and llm_verdict.confidence >= 0.85:
                    decision = "reject"
                    candidate.match_type = "llm_adjudicated"

        if decision == "auto_merge":
            candidate.status = "confirmed"
            candidate.decided_at = now_iso()
            candidate.decided_by = "system:llm_adjudicated" if llm_verdict else "system:auto_merge"
            store.save_candidate(candidate)
            entity_id = merge_two_records(store, tenant_id, domain, rec.id, other_id,
                                           reason=f"auto-merge ({candidate.reasons})")
            result["auto_merged_with"].append({"record_id": other_id, "score": breakdown.score})
            result["matched_entity_id"] = entity_id
        elif decision == "reject":
            candidate.status = "rejected"
            candidate.decided_at = now_iso()
            candidate.decided_by = "system:llm_adjudicated"
            store.save_candidate(candidate)
        else:  # review
            store.save_candidate(candidate)
            result["queued_for_review"].append(
                {"candidate_id": candidate.id, "record_id": other_id, "score": breakdown.score, "reasons": candidate.reasons}
            )

    if not result["auto_merged_with"] and not result["queued_for_review"]:
        entity = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
        store.save_entity(entity)
        store.set_record_entity(rec.id, tenant_id, domain, entity.id)
        rebuild_golden_record(store, tenant_id, domain, entity.id)
        result["matched_entity_id"] = entity.id

    return result


def _entity_for_record(store: Store, record_id: str, tenant_id: str, domain: str) -> str | None:
    rec = store.get_source_record(record_id, tenant_id, domain)
    return rec.entity_id if rec else None


def merge_two_records(store: Store, tenant_id: str, domain: str, record_id_a: str, record_id_b: str, reason: str) -> str:
    entity_a = _entity_for_record(store, record_id_a, tenant_id, domain)
    entity_b = _entity_for_record(store, record_id_b, tenant_id, domain)

    if entity_a and entity_b and entity_a != entity_b:
        survivor, loser = entity_a, entity_b
        for r in store.list_source_records(tenant_id, domain, entity_id=loser):
            store.set_record_entity(r.id, tenant_id, domain, survivor)
        loser_entity = store.get_entity(loser, tenant_id, domain)
        loser_entity.status = "merged"
        store.save_entity(loser_entity)
        target = survivor
    elif entity_a:
        store.set_record_entity(record_id_b, tenant_id, domain, entity_a)
        target = entity_a
    elif entity_b:
        store.set_record_entity(record_id_a, tenant_id, domain, entity_b)
        target = entity_b
    else:
        entity = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
        store.save_entity(entity)
        store.set_record_entity(record_id_a, tenant_id, domain, entity.id)
        store.set_record_entity(record_id_b, tenant_id, domain, entity.id)
        target = entity.id

    rebuild_golden_record(store, tenant_id, domain, target, reason=reason)
    return target


def rebuild_golden_record(store: Store, tenant_id: str, domain: str, entity_id: str,
                           reason: str = "recomputed after ingestion/merge") -> Entity:
    config = get_domain_config(domain)
    records = store.list_source_records(tenant_id, domain, entity_id=entity_id)
    entity = store.get_entity(entity_id, tenant_id, domain)
    decisions = build_golden_attributes(
        records, config.schema_fields, config.field_strategies, config.default_strategy,
    )

    new_attrs = {}
    for f, decision in decisions.items():
        old_val = entity.attributes.get(f)
        new_attrs[f] = decision.value
        if decision.value != old_val:
            store.add_lineage(LineageEvent(
                entity_id=entity_id, field=f, old_value=old_val, new_value=decision.value,
                source_record_id=decision.winning_source_record_id, tenant_id=tenant_id, domain=domain,
                reason=f"{decision.strategy}: {decision.reason} ({reason})",
            ))

    entity.attributes = new_attrs
    entity.updated_at = now_iso()
    store.save_entity(entity)
    return entity


def resolve_candidate(store: Store, tenant_id: str, domain: str, candidate_id: str,
                       decision: str, decided_by: str) -> dict:
    candidate = store.get_candidate(candidate_id, tenant_id, domain)
    if not candidate:
        raise ValueError(f"No such candidate: {candidate_id}")
    if candidate.status != "pending":
        raise ValueError(f"Candidate {candidate_id} already resolved ({candidate.status})")
    if decision not in ("confirm", "reject"):
        raise ValueError("decision must be 'confirm' or 'reject'")

    status = "confirmed" if decision == "confirm" else "rejected"
    store.update_candidate_status(candidate_id, tenant_id, domain, status, decided_by, now_iso())

    entity_id = None
    if decision == "confirm":
        entity_id = merge_two_records(
            store, tenant_id, domain, candidate.record_a_id, candidate.record_b_id,
            reason=f"steward-confirmed match by {decided_by}",
        )
    return {"candidate_id": candidate_id, "status": status, "entity_id": entity_id}
