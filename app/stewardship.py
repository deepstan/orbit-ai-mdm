"""
The audit page's logic, kept apart from the web layer so it can be tested without a web server.

A person is shown a random sample of the system's AUTOMATIC merges (the rule score's, and the model's promotions, which come first because they are the
ones the rule score would not have made) and says whether each is right. "Wrong" can also unmerge it on the spot. Whoever is acting is passed in by the
caller from the authenticated key; nothing here trusts a name sent by a browser.
"""
from __future__ import annotations

import os
import threading
import time

from . import learned_scorer as ls
from . import monitor
from .unmerge import CannotUnmerge, plan_unmerge, unmerge_pair

VERDICTS = ("correct", "wrong")
DEFAULT_TARGET = 0.0005
SUMMARY_CACHE_ENV, DEFAULT_SUMMARY_CACHE_SECONDS = "MDM_SUMMARY_CACHE_SECONDS", 30.0

_merge_totals: dict = {}
_merge_totals_lock = threading.Lock()


def _cache_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(SUMMARY_CACHE_ENV, DEFAULT_SUMMARY_CACHE_SECONDS)))
    except ValueError:
        return DEFAULT_SUMMARY_CACHE_SECONDS


def _database_of(store):
    """What the store POINTS AT: the SQLite file or the Postgres URL. Not id(store): Python reuses the address of a freed object (50 of 60 fresh stores in a
    measurement), so a cache keyed on it could serve one database's count for another."""
    return getattr(store, "path", None) or getattr(store, "database_url", None) or id(store)


def merge_totals(store, tenant_id: str, domain: str, clock=time.monotonic) -> dict:
    """How many automatic merges each kind has made. Counting them reads every one (the single cost of the page that grows with the data), they change slowly and
    only weight what the page DISPLAYS, so the page may use a total up to MDM_SUMMARY_CACHE_SECONDS old (30; 0 turns the cache off). The audit counts, which a person's
    verdict changes at once, are never cached, and neither is anything the rollback decides on."""
    ttl, key, now = _cache_seconds(), (_database_of(store), tenant_id, domain), clock()
    with _merge_totals_lock:
        hit = _merge_totals.get(key)
        if ttl > 0 and hit and now - hit[0] < ttl:
            return hit[1]
    counts = store.count_auto_merges(tenant_id, domain)
    with _merge_totals_lock:
        _merge_totals[key] = (now, counts)
        if len(_merge_totals) > 256:   # a long-running server sees few (tenant, domain) pairs; never let this grow without bound
            _merge_totals.pop(next(iter(_merge_totals)))
    return counts


def _kind(made_by) -> str:
    return "model" if made_by == "promoted" else "rule"


def _record(rec, mask) -> dict | None:
    return None if rec is None else {"id": rec.id, "source_system": rec.source_system, "attributes": mask(rec.attributes)}


def audit_queue(store, tenant_id: str, domain: str, mask=lambda attributes: attributes, limit: int = 50) -> dict:
    """The merges waiting for a person: model promotions first, then the rest by when they were flagged. Only a page of them is ever read from the database, and a
    merge that was unmerged since it was flagged is not offered (the query asks for confirmed ones)."""
    items = []
    for a in store.pending_audits(tenant_id, domain, limit):
        cand = store.get_candidate(a["candidate_id"], tenant_id, domain)
        if cand is None or cand.status != "confirmed":
            continue
        rec_a, rec_b = (store.get_source_record(r, tenant_id, domain) for r in (cand.record_a_id, cand.record_b_id))
        try:
            plan = plan_unmerge(store, tenant_id, domain, cand.id)
            unmerge = {"possible": True, "moves": len(plan["move"]), "stays": len(plan["keep"]) + len(plan["left_alone"])}
        except CannotUnmerge as e:
            unmerge = {"possible": False, "why": str(e)}
        items.append({"candidate_id": cand.id, "made_by": _kind(a["made_by"]), "score": cand.score, "match_type": cand.match_type, "reasons": cand.reasons,
                      "contributions": cand.contributions, "merged_at": cand.decided_at, "entity_id": rec_a.entity_id if rec_a else None,
                      "record_a": _record(rec_a, mask), "record_b": _record(rec_b, mask), "unmerge": unmerge})
    return {"waiting": store.audit_counts(tenant_id, domain)["waiting"], "shown": len(items), "items": items}


def audit_summary(store, tenant_id: str, domain: str) -> dict:
    """What the page shows above the queue: how much has been checked, how it came out for each kind of merge, and whether the model is live or rolled back."""
    scorer = ls.load_scorer(tenant_id, domain)
    policy = scorer.policy if scorer else {}
    result = monitor.assess(store, tenant_id, domain, policy.get("target_false_rate", DEFAULT_TARGET), policy.get("confidence", 0.95), with_pairs=False,
                            merge_counts=merge_totals(store, tenant_id, domain))
    state = ls.read_state(tenant_id, domain) if scorer else {"status": "none"}
    waiting = store.audit_counts(tenant_id, domain)["waiting"]
    return {"waiting": waiting, "strata": {k: {f: v[f] for f in ("merges", "audited", "wrong", "estimate", "lower_bound", "upper_bound")} for k, v in result["strata"].items()},
            "pooled_estimate": result["pooled_estimate"], "target": result["target"], "verdict": result["verdict"], "why": result["why"],
            "scorer": {"present": scorer is not None, "state": state.get("status", "active"), "reason": state.get("reason", ""),
                       "promotion_live": bool(scorer and policy.get("promote_threshold") is not None and state.get("status", "active") == "active")}}


def give_verdict(store, tenant_id: str, domain: str, candidate_id: str, verdict: str, by: str, unmerge: bool = False, reason: str = "") -> dict:
    """A person's verdict on a flagged merge. With unmerge=True a 'wrong' verdict also splits the entity; if that cannot be done (the records are still joined another
    way) the verdict is kept and the answer says why, so the person can choose which merge is actually wrong."""
    if verdict not in VERDICTS:
        raise ValueError(f"verdict must be one of {VERDICTS}")
    if unmerge and verdict != "wrong":
        raise ValueError("only a merge judged wrong can be unmerged")
    if not store.record_merge_audit(candidate_id, tenant_id, domain, verdict, by):
        raise ValueError("this merge was not flagged for audit, or does not exist")
    out = {"candidate_id": candidate_id, "verdict": verdict, "audited_by": by, "unmerge": None}
    if unmerge:
        try:
            r = unmerge_pair(store, tenant_id, domain, candidate_id, by, reason)
            out["unmerge"] = {"ok": True, "split": r["split"], "kept_entity": r["kept_entity"], "new_entity": r["new_entity"], "moved": len(r["moved"])}
        except CannotUnmerge as e:
            out["unmerge"] = {"ok": False, "why": str(e)}
    return out
