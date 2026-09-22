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
from .matching import classify, find_candidate_pairs, score_pair
from .models import Entity, LineageEvent, MatchCandidate, SourceRecord, now_iso
from .standardize import standardize
from .survivorship import build_golden_attributes


def ingest_record(store: Store, tenant_id: str, domain: str, source_system: str,
                   source_record_id: str, attributes: dict) -> dict:
    """Ingests one raw record, runs it against this (tenant, domain)'s
    existing records for matches, and either auto-merges, queues for
    steward review, or leaves it standalone."""
    config = get_domain_config(domain)  # raises KeyError early for an unknown domain
    rec = SourceRecord(source_system=source_system, source_record_id=source_record_id,
                        attributes=attributes, tenant_id=tenant_id, domain=domain)
    store.save_source_record(rec)

    # Isolation point: scoped by BOTH tenant_id and domain, so a Product
    # ingest can never be compared against Customer records, or another
    # tenant's records of the same domain.
    existing = [r for r in store.list_source_records(tenant_id, domain) if r.id != rec.id]
    std_new = standardize(domain, rec.attributes)

    pool = [(r.id, standardize(domain, r.attributes)) for r in existing] + [(rec.id, std_new)]
    pairs = find_candidate_pairs(pool, config)
    std_by_id = dict(pool)

    result = {"record_id": rec.id, "auto_merged_with": [], "queued_for_review": [], "matched_entity_id": None}
    learned_model = active_learning.load_model(tenant_id, domain)

    for id_a, id_b in pairs:
        if rec.id not in (id_a, id_b):
            continue
        other_id = id_b if id_a == rec.id else id_a
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
