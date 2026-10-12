"""
Ties standardize -> match -> route -> survive -> lineage together.
Domain-agnostic: every function takes (tenant_id, domain) explicitly, and
domain-specific behavior comes entirely from `domains.get_domain_config()`.
No function in this module hardcodes a customer or product field name.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass, replace

from . import active_learning, learned_scorer, llm_adjudication
from .db import Store
from .domains import get_domain_config
from .indexing import ensure_index_ready
from .staging import cast_record, stage_record, cleansing_issues
from .matching import blocking_keys, classify, score_pair
from .models import Entity, LineageEvent, MatchCandidate, SourceRecord, new_id, now_iso
from .standardize import standardize
from .survivorship import build_golden_attributes


def content_hash(attributes: dict) -> str:
    """Hash of the canonical payload (key order and formatting of the JSON don't matter)."""
    return hashlib.sha256(json.dumps(attributes, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()


# ===========================================================================================
# How a record gets from raw to golden, and what differs between REALTIME and BATCH
# ===========================================================================================
#   landed   raw saved, nothing else
#   staged   standardized and in the blocking index; not yet matched
#   matched  the matching decision is made and recorded (merged, queued for review, or a new entity). The ONLY finished state.
# A record is only ever "already ingested" if it is `matched`. Anything else is RESUMED when it is sent again, or by
# `domains_cli.py process-pending`, so an interrupted run can never leave a record silently unfinished.
@dataclass
class Ctx:
    mode: str = "realtime"             # realtime | batch | resume
    defer_golden: set | None = None    # batch/resume: golden rebuilds wait until the end of the run (None = rebuild immediately)
    allow_llm: bool = True             # LLM adjudication inline? (a batch of thousands would mean thousands of slow calls)
    # Batch/resume only: progress is written in GROUPS (flush_progress), not one commit per record. Safe by construction: if the process
    # dies between flushes, those records are still `staged`, so they are resumed, and resuming is idempotent (see _process_record).
    done_ids: list | None = None       # records matched but not yet marked 'matched'
    dirty_rows: list | None = None     # golden-dirty marks not yet saved


REALTIME = Ctx()
MODES = ("realtime", "batch")
_UNSET = object()


def _llm_in_batch() -> bool:
    return os.environ.get("MDM_LLM_IN_BATCH", "").strip().lower() in ("1", "true", "yes")


def batch_ctx(mode: str = "batch") -> Ctx:
    return Ctx(mode=mode, defer_golden=set(), allow_llm=_llm_in_batch(), done_ids=[], dirty_rows=[])


def _publish_golden(store: Store, tenant_id: str, domain: str, entity_id: str, reason: str, ctx: Ctx) -> None:
    """Realtime: rebuild the golden record now, so the caller sees it. Batch: note that it is stale and rebuild it ONCE at the
    end of the run (an entity that 50 records merge into is rebuilt once, not 50 times). The note is persisted (golden_dirty), so
    a run that dies before the end does not leave the golden record stale for ever: `process-pending` finishes the job."""
    if ctx.defer_golden is None:
        rebuild_golden_record(store, tenant_id, domain, entity_id, reason=reason)
        return
    if (tenant_id, entity_id) not in ctx.defer_golden:
        ctx.defer_golden.add((tenant_id, entity_id))
        if ctx.dirty_rows is not None:
            ctx.dirty_rows.append((tenant_id, domain, entity_id))
        else:
            store.mark_golden_dirty([(tenant_id, domain, entity_id)])


FLUSH_EVERY = 100


def flush_progress(store: Store, tenant_id: str, domain: str, ctx: Ctx) -> None:
    """Writes the batched progress. Golden-dirty marks go FIRST: a record is never marked finished while the note that its golden
    record needs rebuilding is still unsaved."""
    if ctx.dirty_rows:
        store.mark_golden_dirty(ctx.dirty_rows)
        ctx.dirty_rows.clear()
    if ctx.done_ids:
        store.set_processing_status(ctx.done_ids, tenant_id, domain, "matched")
        ctx.done_ids.clear()


def flush_golden(store: Store, domain: str, dirty: set) -> int:
    rebuilt = 0
    for tenant_id, entity_id in sorted(dirty):
        rebuild_golden_record(store, tenant_id, domain, entity_id, reason="rebuilt at the end of a batch run")  # skips merged entities
        rebuilt += 1
    store.clear_golden_dirty([(t, domain, e) for t, e in dirty])
    dirty.clear()
    return rebuilt


class Run:
    """One row in pipeline_runs: what a batch run (or a process-pending pass, or a reindex) received and what became of it.
    Realtime requests are NOT logged here (one row per API call would swamp it); the audit log has an entry per request."""
    COUNTS = ("received", "new", "unchanged", "updated", "resumed", "deferred", "errors", "auto_merged", "queued", "standalone", "golden_rebuilt")

    def __init__(self, store: Store, tenant_id: str, domain: str, mode: str, triggered_by: str):
        self.store, self.id, self.tenant_id, self.domain, self.mode = store, new_id("run_"), tenant_id, domain, mode
        self.triggered_by, self.status, self.aborted = triggered_by, "running", False
        self.counts, self.errors = {k: 0 for k in self.COUNTS}, []

    @classmethod
    def start(cls, store: Store, tenant_id: str, domain: str, mode: str, triggered_by: str = "api") -> "Run":
        run = cls(store, tenant_id, domain, mode, triggered_by)
        store.create_run(run.id, tenant_id, domain, mode, triggered_by, now_iso())
        return run

    def tally(self, result: dict) -> None:
        c = self.counts
        c["received"] += 1
        if "error" in result:
            c["errors"] += 1
        elif result.get("unchanged"):
            c["unchanged"] += 1
        elif result.get("deferred"):
            c["deferred"] += 1
        elif result.get("updated"):
            c["updated"] += 1
        else:
            c["resumed" if result.get("resumed") else "new"] += 1
            c["auto_merged" if result.get("auto_merged_with") else "queued" if result.get("queued_for_review") else "standalone"] += 1

    def error(self, source_system: str, source_record_id: str, message: str) -> None:
        if len(self.errors) < 20:
            self.errors.append({"source_system": source_system, "source_record_id": source_record_id, "error": message})

    def finish(self) -> dict:
        self.status = "failed" if self.aborted else ("completed_with_errors" if self.counts["errors"] else "completed")
        self.store.finish_run(self.id, self.status, self.counts, self.errors)
        return self.summary()

    def summary(self) -> dict:
        return {"run_id": self.id, "status": self.status, "mode": self.mode, "counts": dict(self.counts), "errors": list(self.errors)}


def _process_record(store: Store, tenant_id: str, domain: str, config, rec: SourceRecord, fingerprint: str, ctx: Ctx, *,
                    staged: tuple | None = None, resume: bool = False, learned_model=_UNSET) -> dict:
    """Everything after raw landing: stage, index, match, merge/queue/standalone. Safe to run again on a record that stopped
    partway (staging and keys are replaced, and with resume=True pairs already decided are not decided twice). If it fails the
    failure is recorded on the record and re-raised; the record stays unfinished and is picked up again."""
    try:
        if staged is None:
            std_new, cleansed = stage_record(domain, config, rec.attributes)
            store.save_staged_batch([(tenant_id, domain, rec.id, std_new, cleansed, fingerprint)])
        else:
            std_new = staged[0]
        new_keys = blocking_keys(std_new, config)
        # ONE record's keys at a time, right before that record's own matching step: a record must never see the keys of
        # records that arrive AFTER it (see run_batch). Replacing, not inserting, is what makes resuming safe.
        store.replace_blocking_keys_batch([(tenant_id, domain, rec.id, new_keys)])
        if staged is None:  # (a batch has already marked all its new records 'staged' in one write)
            store.set_processing_status([rec.id], tenant_id, domain, "staged")
        model = active_learning.load_model(tenant_id, domain) if learned_model is _UNSET else learned_model
        known = store.list_candidate_pairs(tenant_id, domain, rec.id) if resume else None
        result = _route_record(store, tenant_id, domain, config, rec, std_new, new_keys, model,
                               existing_entity=rec.entity_id, ctx=ctx, known_pairs=known)
        if ctx.done_ids is not None:
            ctx.done_ids.append(rec.id)
        else:
            store.set_processing_status([rec.id], tenant_id, domain, "matched")
        return result
    except Exception as e:
        store.record_processing_failure(rec.id, tenant_id, domain, f"{type(e).__name__}: {e}"[:500])
        raise


def _accepted(rec: SourceRecord) -> dict:
    return {"record_id": rec.id, "accepted": True, "deferred": True, "status": "staged", "matched_entity_id": None,
            "auto_merged_with": [], "queued_for_review": [], "skipped_overpopulated_keys": []}


def _ingest_existing(store: Store, tenant_id: str, domain: str, config, existing: SourceRecord, attributes: dict,
                     fingerprint: str, ctx: Ctx) -> dict:
    """The source system sent a record we already hold. Idempotent:
      * same payload, and it FINISHED -> nothing happens (re-running a file is safe)
      * same payload, but it did NOT finish -> it is resumed from where it stopped (it is not a finished duplicate)
      * changed -> the superseded payload goes to source_record_history, the row is updated IN PLACE (same record id, same entity),
                   and everything derived from it is refreshed: staging, blocking keys, review items scored on the old data, any
                   NEW matches, and the golden record.
    Known limit: a change can ADD merges but never splits an entity (there is no unmerge)."""
    new_hash = content_hash(attributes)
    old_hash = existing.content_hash or content_hash(existing.attributes)
    if new_hash == old_hash:
        if existing.processing_status == "matched":
            return {"record_id": existing.id, "auto_merged_with": [], "queued_for_review": [],
                    "matched_entity_id": existing.entity_id, "skipped_overpopulated_keys": [], "unchanged": True}
        result = _process_record(store, tenant_id, domain, config, existing, fingerprint, ctx, resume=True)
        result["resumed"] = True
        return result

    version = existing.version + 1
    store.add_record_history([(tenant_id, domain, existing.id, existing.version, old_hash, existing.attributes, now_iso())])
    store.update_source_record(existing.id, tenant_id, domain, attributes, new_hash, version)  # also resets its processing state
    rec = replace(existing, attributes=attributes, content_hash=new_hash, version=version,
                  processing_status="landed", processing_attempts=0, processing_error=None)
    store.supersede_pending_candidates(tenant_id, domain, rec.id)
    result = _process_record(store, tenant_id, domain, config, rec, fingerprint, ctx)
    current = store.get_source_record(rec.id, tenant_id, domain)
    if current and current.entity_id:
        _publish_golden(store, tenant_id, domain, current.entity_id, f"source record changed (version {version})", ctx)
        result["matched_entity_id"] = current.entity_id
    result["updated"], result["version"] = True, version
    return result


def ingest_record(store: Store, tenant_id: str, domain: str, source_system: str,
                  source_record_id: str, attributes: dict, mode: str = "realtime") -> dict:
    """REALTIME ingest of one record: matched, merged and published as golden before this returns, so the caller sees the
    outcome. An error is raised to the caller, and the record stays unfinished (resumable).

    mode="batch" is for high-volume intake: the record is landed and staged, and the call returns at once with
    `accepted: True`. Matching happens later, in order, in a batch run (`domains_cli.py process-pending`). It applies to NEW
    records; one we already hold is handled exactly as in realtime."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    config = get_domain_config(domain)  # raises KeyError early for an unknown domain
    fingerprint = ensure_index_ready(store, domain, config)  # refuses if the domain's config changed and derived data wasn't rebuilt
    existing = store.find_source_record(tenant_id, domain, source_system, source_record_id)
    if existing is not None:  # idempotent: the same source record again is a no-op, a resume, or a new version, never a duplicate row
        return _ingest_existing(store, tenant_id, domain, config, existing, attributes, fingerprint, REALTIME)
    rec = SourceRecord(source_system=source_system, source_record_id=source_record_id, attributes=attributes,
                       tenant_id=tenant_id, domain=domain, content_hash=content_hash(attributes))
    store.save_source_record(rec)  # RAW landing: exactly as received, never rewritten
    if mode == "batch":
        std_new, cleansed = stage_record(domain, config, rec.attributes)
        store.save_staged_batch([(tenant_id, domain, rec.id, std_new, cleansed, fingerprint)])
        store.set_processing_status([rec.id], tenant_id, domain, "staged")
        return _accepted(rec)
    return _process_record(store, tenant_id, domain, config, rec, fingerprint, REALTIME)


@dataclass
class BatchRun:
    results: list
    run: dict


def run_batch(store: Store, tenant_id: str, domain: str, records: list[tuple[str, str, dict]], *, max_errors: int | None = None,
              defer: bool = False, triggered_by: str = "api", run: Run | None = None) -> BatchRun:
    """BATCH ingest of many records of one tenant+domain, as one logged run (pipeline_runs).

    What differs from realtime, on purpose:
      * errors are collected per record and the run continues (`max_errors`: abort once exceeded; the rest stay unfinished
        and are completed by process-pending), where realtime raises to its caller
      * golden records are rebuilt once per affected entity at the end, not after every merge
      * LLM adjudication is not done inline (ambiguous pairs go to review) unless MDM_LLM_IN_BATCH is set
      * `defer=True` only lands and stages; matching waits for process-pending

    What does NOT differ: the matching itself, and its order. Only RAW storage and staging are batched (each depends on nothing
    but its own record). Blocking keys are written one record at a time right before that record is matched: writing them all
    up front let record #3 see record #500's keys, which merged records that had not "arrived" yet and gave different, wrong
    clusters (327 entities instead of 79 for the same 1000 records).

    Idempotent, like ingest_record(): a key already held (or repeated inside the batch) is a no-op, a resume, or a new version."""
    config = get_domain_config(domain)  # raises KeyError early for an unknown domain
    fingerprint = ensure_index_ready(store, domain, config)
    own = run is None
    if own:
        run = Run.start(store, tenant_id, domain, "batch", triggered_by)
    ctx = batch_ctx("batch")
    results: list = []
    try:
        held = store.find_source_records_by_keys(tenant_id, domain, [(ss, srid) for ss, srid, _ in records])
        seen, plan = set(), []
        for ss, srid, attrs in records:
            if (ss, srid) in held or (ss, srid) in seen:
                plan.append(None)
            else:
                seen.add((ss, srid))
                plan.append(SourceRecord(source_system=ss, source_record_id=srid, attributes=attrs, tenant_id=tenant_id,
                                         domain=domain, content_hash=content_hash(attrs)))
        recs = [r for r in plan if r is not None]
        store.save_source_records_batch(recs)
        staged = {rec.id: stage_record(domain, config, rec.attributes) for rec in recs}
        store.save_staged_batch([(tenant_id, domain, rid, m, c, fingerprint) for rid, (m, c) in staged.items()])
        store.set_processing_status([r.id for r in recs], tenant_id, domain, "staged")
        learned_model = active_learning.load_model(tenant_id, domain)  # once for the batch: the model doesn't change mid-run

        for (ss, srid, attrs), rec in zip(records, plan):
            if run.aborted:
                result = {"record_id": rec.id if rec else None, "deferred": True, "skipped": "the run was aborted"}
            else:
                try:
                    if rec is None:
                        existing = store.find_source_record(tenant_id, domain, ss, srid)  # fresh: an earlier item may have changed it
                        result = _ingest_existing(store, tenant_id, domain, config, existing, attrs, fingerprint, ctx)
                    elif defer:
                        result = _accepted(rec)
                    else:
                        result = _process_record(store, tenant_id, domain, config, rec, fingerprint, ctx,
                                                 staged=staged[rec.id], learned_model=learned_model)
                except Exception as e:
                    result = {"record_id": rec.id if rec else None, "error": f"{type(e).__name__}: {e}"[:300]}
                    run.error(ss, srid, result["error"])
                    if max_errors is not None and run.counts["errors"] + 1 > max_errors:
                        run.aborted = True
            run.tally(result)
            results.append(result)
            if len(ctx.done_ids) >= FLUSH_EVERY:
                flush_progress(store, tenant_id, domain, ctx)
    except BaseException:
        run.aborted = True
        raise
    finally:
        flush_progress(store, tenant_id, domain, ctx)
        run.counts["golden_rebuilt"] += flush_golden(store, domain, ctx.defer_golden)
        if own:
            run.finish()
    return BatchRun(results, run.summary())


def ingest_batch(store: Store, tenant_id: str, domain: str, records: list[tuple[str, str, dict]], **options) -> list[dict]:
    """run_batch(), returning just the per-record results (one dict per input record, in order; a failed record has an `error`)."""
    return run_batch(store, tenant_id, domain, records, **options).results


def process_pending(store: Store, domain: str, *, limit: int = 1000, max_attempts: int = 5, triggered_by: str = "cli") -> dict:
    """Finishes whatever is unfinished, oldest first (matching is order-sensitive): records that were only accepted (mode=batch)
    or landed, records an interrupted run or an error left partway, and golden rebuilds a crashed batch run never got to. Safe to
    run any time and as often as you like (cron, Airflow, ...). A record that fails `max_attempts` times is left alone and shown
    by `domains_cli.py status`; it needs a human, not another retry."""
    config = get_domain_config(domain)
    fingerprint = ensure_index_ready(store, domain, config)
    by_tenant: dict = {}
    for rec in store.list_unfinished_records(domain, limit, max_attempts):
        by_tenant.setdefault(rec.tenant_id, []).append(rec)
    leftovers = store.list_golden_dirty(domain)
    summaries = []
    for tenant_id in sorted(set(by_tenant) | {t for t, _ in leftovers}):
        run = Run.start(store, tenant_id, domain, "resume", triggered_by)
        ctx = batch_ctx("resume")
        ctx.defer_golden.update((t, e) for t, e in leftovers if t == tenant_id)  # already marked dirty in the database
        model = active_learning.load_model(tenant_id, domain)
        try:
            for rec in by_tenant.get(tenant_id, []):
                try:
                    result = _process_record(store, tenant_id, domain, config, rec, fingerprint, ctx, resume=True, learned_model=model)
                    result["resumed"] = True
                except Exception as e:
                    result = {"record_id": rec.id, "error": f"{type(e).__name__}: {e}"[:300]}
                    run.error(rec.source_system, rec.source_record_id, result["error"])
                run.tally(result)
                if len(ctx.done_ids) >= FLUSH_EVERY:
                    flush_progress(store, tenant_id, domain, ctx)
        except BaseException:
            run.aborted = True
            raise
        finally:
            flush_progress(store, tenant_id, domain, ctx)
            run.counts["golden_rebuilt"] += flush_golden(store, domain, ctx.defer_golden)
            summaries.append(run.finish())
    return {"finished": sum(s["counts"]["resumed"] for s in summaries), "errors": sum(s["counts"]["errors"] for s in summaries),
            "golden_rebuilt": sum(s["counts"]["golden_rebuilt"] for s in summaries), "runs": summaries,
            "remaining": store.count_unfinished(domain)[0]}  # more than `limit` were waiting? run it again


logger = logging.getLogger("mdm.matching")
NO_BLOCK_KEY = "__no_block__"


def _route_record(store: Store, tenant_id: str, domain: str, config, rec: SourceRecord,
                   std_new: dict, new_keys: set[str], learned_model, existing_entity: str | None = None,
                   ctx: Ctx | None = None, known_pairs: dict | None = None) -> dict:
    """The matching/routing logic shared by ingest_record() and
    ingest_batch() -- assumes `rec` and its blocking keys are ALREADY
    persisted (both callers handle that themselves, one record at a
    time or in a batch respectively) and just does the candidate
    lookup, scoring, and merge/queue/standalone decision."""
    ctx = ctx or REALTIME
    # Isolation point: scoped by BOTH tenant_id and domain, so a Product
    # ingest can never be compared against Customer records, or another
    # tenant's records of the same domain -- find_candidate_record_ids()
    # only ever queries within this exact (tenant_id, domain) pair.
    # A blocking key held by more than max_bucket_size OTHER records (a placeholder, a very common name) finds
    # everyone and no one, and every ingest would pay to score all of them. Such keys aren't used for lookup;
    # the record is still findable through its other keys, and the skip is reported, never silent.
    # (`__no_block__` marks "no usable key at all" and is never a lookup key.)
    sizes = store.blocking_bucket_sizes(tenant_id, domain, new_keys)
    skipped = sorted(k for k in new_keys if k != NO_BLOCK_KEY and sizes.get(k, 0) - 1 > config.max_bucket_size)
    usable = {k for k in new_keys if k != NO_BLOCK_KEY and k not in set(skipped)}
    if skipped:
        logger.info("domain %s: %d blocking key(s) over the %d-record cap were not used for lookup: %s",
                    domain, len(skipped), config.max_bucket_size, skipped[:5])
    candidate_ids = store.find_candidate_record_ids(tenant_id, domain, usable, exclude_record_id=rec.id)
    separated = store.has_separations(tenant_id, domain)  # has a person unmerged anything here? (one tiny query)
    if separated:  # a pair a person separated is never proposed again
        candidate_ids -= store.separated_partners(tenant_id, domain, rec.id)
    if existing_entity:  # a changed record: records already in its own entity are not candidates for it
        candidate_ids -= {r.id for r in store.list_source_records(tenant_id, domain, entity_id=existing_entity)}

    # Candidates come from the staging layer: one query for all of them, no re-standardizing, no per-candidate fetch.
    std_by_id = {rec.id: std_new}
    staged = store.get_staged_many(tenant_id, domain, candidate_ids)
    for cid in candidate_ids:
        if cid in staged:
            std_by_id[cid] = staged[cid][0]
        else:  # not staged (a record from before the staging layer, or a race with a purge): derive it from raw
            candidate_rec = store.get_source_record(cid, tenant_id, domain)
            if candidate_rec:
                std_by_id[cid] = standardize(domain, candidate_rec.attributes)

    result = {"record_id": rec.id, "auto_merged_with": [], "queued_for_review": [], "matched_entity_id": existing_entity,
              "skipped_overpopulated_keys": skipped}

    for other_id in candidate_ids:
        if other_id not in std_by_id:
            continue  # the race-guard above skipped a now-deleted record
        known = (known_pairs or {}).get(other_id)
        if known:  # resuming: this pair was already decided (or queued) before the interruption; don't decide it twice
            if known["status"] == "confirmed":
                result["matched_entity_id"] = merge_two_records(store, tenant_id, domain, rec.id, other_id, reason="auto-merge (resumed)", ctx=ctx)
                result["auto_merged_with"].append({"record_id": other_id, "score": known["score"]})
            elif known["status"] == "pending":
                result["queued_for_review"].append({"candidate_id": known["id"], "record_id": other_id, "score": known["score"], "reasons": []})
            continue
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

        promoted_p = None
        if decision == "review":  # the tenant's learned scorer, if one is LIVE (and not rolled back), may promote a pair the rule score sent to review
            scorer = learned_scorer.active_scorer(tenant_id, domain)
            if scorer is not None:
                promoted, p = learned_scorer.promote(scorer, breakdown.score, breakdown.match_type, breakdown.contributions)
                if promoted:
                    decision, promoted_p = "auto_merge", p
                    breakdown.reasons = breakdown.reasons + [f"promoted out of review by the learned scorer (p = {p:.3f})"]

        candidate = MatchCandidate(
            record_a_id=rec.id, record_b_id=other_id, tenant_id=tenant_id, domain=domain,
            score=breakdown.score, match_type=breakdown.match_type, reasons=breakdown.reasons,
            contributions=breakdown.contributions,
        )

        # --- Optional LLM adjudication for the ambiguous middle band ---
        llm_verdict = None
        if decision == "review" and ctx.allow_llm and llm_adjudication.is_enabled() and llm_adjudication.in_adjudication_band(breakdown.score):
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

        if decision == "auto_merge" and separated:  # never join two entities that a person separated records between: queue the pair for review instead
            mine, theirs = existing_entity or _entity_for_record(store, rec.id, tenant_id, domain), _entity_for_record(store, other_id, tenant_id, domain)
            blocked_by = store.entities_separated(tenant_id, domain, mine, theirs) if mine and theirs and mine != theirs else None
            if blocked_by:
                decision = "review"
                candidate.reasons = candidate.reasons + [f"not merged automatically: a person separated records in these two entities ({blocked_by})"]
        if decision == "auto_merge":
            candidate.status = "confirmed"
            candidate.decided_at = now_iso()
            candidate.decided_by = learned_scorer.PROMOTED_BY if promoted_p is not None else "system:llm_adjudicated" if llm_verdict else "system:auto_merge"
            store.save_candidate(candidate)
            if learned_scorer.audit_sampled(candidate.id, promoted=promoted_p is not None):  # 2% of automatic merges, the rule's and the model's alike (the model's can be audited more), are flagged for a person to check
                store.flag_merge_audits(tenant_id, domain, [candidate.id])
            entity_id = merge_two_records(store, tenant_id, domain, rec.id, other_id,
                                           reason=f"auto-merge ({candidate.reasons})", ctx=ctx)
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

    # EVERY record belongs to exactly one entity. A record whose only matches are waiting for a steward is still its own entity
    # until one is confirmed (confirming unions the two entities; rejecting changes nothing). Before, such a record had NO entity at
    # all: it was missing from the golden layer, rejecting never gave it one, and WHICH of two review-pair records was entity-less
    # depended on arrival order (28% of benchmark records, different ones in every order).
    if not result["auto_merged_with"] and existing_entity is None:
        entity = Entity(attributes={}, status="active", tenant_id=tenant_id, domain=domain)
        store.save_entity(entity)
        store.set_record_entity(rec.id, tenant_id, domain, entity.id)
        _publish_golden(store, tenant_id, domain, entity.id, "recomputed after ingestion/merge", ctx)
        result["matched_entity_id"] = entity.id

    return result


def _entity_for_record(store: Store, record_id: str, tenant_id: str, domain: str) -> str | None:
    rec = store.get_source_record(record_id, tenant_id, domain)
    return rec.entity_id if rec else None


def merge_two_records(store: Store, tenant_id: str, domain: str, record_id_a: str, record_id_b: str, reason: str,
                      ctx: Ctx | None = None) -> str:
    ctx = ctx or REALTIME
    entity_a = _entity_for_record(store, record_id_a, tenant_id, domain)
    entity_b = _entity_for_record(store, record_id_b, tenant_id, domain)

    if entity_a and entity_b and entity_a != entity_b:
        # The OLDEST entity survives, whichever record is "a". Systems downstream hold entity ids, so the established one must keep
        # its id; letting the newcomer's brand-new entity win (what "a's entity survives" did once every record had its own entity)
        # would retire the id everyone already uses, on every steward confirmation.
        age = lambda eid: ((store.get_entity(eid, tenant_id, domain).created_at or ""), eid)
        survivor, loser = (entity_a, entity_b) if age(entity_a) <= age(entity_b) else (entity_b, entity_a)
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

    _publish_golden(store, tenant_id, domain, target, reason, ctx)
    return target


def compute_golden(config, domain: str, records: list, cleansed_by_id: dict):
    """The golden record's field decisions and quality issues for ONE entity's records, as a pure function (no store). Shared by the
    one-record-at-a-time engine (rebuild_golden_record) and the bulk engine (app/bulk.py), so they cannot drift apart.
    cleansed_by_id: record id -> its CLEANSED form from staging; a record without one is standardized from raw."""
    typed_records, issues = [], []
    for r in records:
        cleansed = cleansed_by_id[r.id] if r.id in cleansed_by_id else stage_record(domain, config, r.attributes)[1]
        # Cast BEFORE survivorship, so a newer "N/A" cannot beat an older valid number: an uncastable value is simply absent.
        typed, found = cast_record(config, cleansed, r.id)
        found += cleansing_issues(config, r.attributes, r.id)  # ssn/dob values the rules discarded, with the reason and never the value
        typed_records.append(replace(r, attributes=typed))
        issues += found
    decisions = build_golden_attributes(typed_records, config.schema_fields, config.field_strategies, config.default_strategy)
    seen, kept = set(), []
    for issue in issues:
        k = (issue["field"], issue["value"])
        if k not in seen and len(kept) < 20:
            seen.add(k)
            kept.append(issue)
    return decisions, kept


def golden_values(decisions: dict, old_attrs: dict, entity_id: str, tenant_id: str, domain: str, reason: str):
    """(the golden attribute values, one lineage event per field whose value changed)."""
    new_attrs, lineage = {}, []
    for f, decision in decisions.items():
        old_val = old_attrs.get(f)
        new_attrs[f] = decision.value
        if decision.value != old_val:
            lineage.append(LineageEvent(
                entity_id=entity_id, field=f, old_value=None if old_val is None else str(old_val),
                new_value=None if decision.value is None else str(decision.value),
                source_record_id=decision.winning_source_record_id, tenant_id=tenant_id, domain=domain,
                reason=f"{decision.strategy}: {decision.reason} ({reason})",
            ))
    return new_attrs, lineage


def rebuild_golden_record(store: Store, tenant_id: str, domain: str, entity_id: str,
                           reason: str = "recomputed after ingestion/merge") -> Entity:
    config = get_domain_config(domain)
    records = store.list_source_records(tenant_id, domain, entity_id=entity_id)
    entity = store.get_entity(entity_id, tenant_id, domain)
    if entity is None or entity.status == "merged":
        return entity  # a merged entity is a tombstone with no records left: rebuilding it would BLANK its values
    # The golden record is derived from the CLEANSED form (staging), never from raw: raw is untrusted and unformatted
    # (whitespace, casing, placeholders). Same records, same timestamps and sources; only the values differ.
    staged = store.get_staged_many(tenant_id, domain, [r.id for r in records])
    decisions, issues = compute_golden(config, domain, records, {rid: v[1] for rid, v in staged.items()})
    new_attrs, lineage = golden_values(decisions, entity.attributes, entity_id, tenant_id, domain, reason)
    store.add_lineage_batch(lineage)  # one transaction for the whole rebuild

    entity.attributes = new_attrs
    entity.quality_issues = issues
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

    if decision == "confirm" and store.has_separations(tenant_id, domain):
        ea, eb = (_entity_for_record(store, r, tenant_id, domain) for r in (candidate.record_a_id, candidate.record_b_id))
        blocked_by = store.entities_separated(tenant_id, domain, ea, eb) if ea and eb and ea != eb else None
        if blocked_by:
            raise ValueError(f"Cannot confirm {candidate_id}: a person separated records in these two entities ({blocked_by}), and merging them would undo that")

    status = "confirmed" if decision == "confirm" else "rejected"
    store.update_candidate_status(candidate_id, tenant_id, domain, status, decided_by, now_iso())

    entity_id = None
    if decision == "confirm":
        entity_id = merge_two_records(
            store, tenant_id, domain, candidate.record_a_id, candidate.record_b_id,
            reason=f"steward-confirmed match by {decided_by}",
        )
    return {"candidate_id": candidate_id, "status": status, "entity_id": entity_id}
