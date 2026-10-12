"""
Shadow replay: what the learned scorer WOULD have decided on the pairs already in the database, set beside what actually happened and what people said.

It is read-only. It rescores every stored candidate from the evidence scoring already stored on it (no records are re-read), so it can be run on a
production database at any time and merges nothing. Three views:

  * the rule score's decision against the model's, as a table of counts: where would it agree, where would it hold an automatic merge for review, where
    would it promote a pair out of review;
  * for each of those sets, what the HUMAN labels say: how many labelled pairs, how many were wrong, and the upper confidence bound on the false rate;
  * the same bound on FRESH audits only (made after the model was trained), the one unbiased test of a threshold that was chosen on earlier labels.

Labels are a random sample only where they are audits; steward decisions are what stewards chose to look at. The report says which is which.
"""
from __future__ import annotations

from . import learned_scorer as ls
from .matching import AUTO_MERGE_THRESHOLD


def rule_decision(candidate) -> str:
    return "auto_merge" if candidate.score >= AUTO_MERGE_THRESHOLD else "review"


def shadow_report(store, tenant_id: str, domain: str, scorer: ls.TreeScorer) -> dict:
    labeled = {c.id: (label, source) for c, label, source in store.human_labeled_candidates(tenant_id, domain)}
    audit_rows = store.list_merge_audits(tenant_id, domain)
    audited_at = {a["candidate_id"]: a["audited_at"] for a in audit_rows if a["verdict"]}
    trained_at = (scorer.doc.get("trained") or {}).get("at") or ""
    candidates = store.list_candidates(tenant_id, domain, status="confirmed") + store.list_candidates(tenant_id, domain, status="pending")
    confidence = scorer.policy.get("confidence", 0.95)
    blank = lambda: {"pairs": 0, "labeled": 0, "wrong": 0, "audited": 0, "audited_wrong": 0, "fresh": 0, "fresh_wrong": 0}
    sets = {"rule_auto": blank(), "promoted_certified": blank(), "promoted_uncertified": blank()}
    review_pairs = promoted_certified = promoted_uncertified = 0
    for c in candidates:
        rule = rule_decision(c)
        p = scorer.score_candidate(c.score, c.match_type, c.contributions)
        members = ["rule_auto"] if rule == "auto_merge" else []
        if rule == "review":
            review_pairs += 1
            if scorer.decide(p, rule) == "auto_merge":
                members.append("promoted_certified")
                promoted_certified += 1
            if scorer.decide(p, rule, uncertified_ok=True) == "auto_merge":
                members.append("promoted_uncertified")
                promoted_uncertified += 1
        label = labeled.get(c.id)
        for name in members:
            s = sets[name]
            s["pairs"] += 1
            if label is not None:
                wrong = label[0] == 0
                s["labeled"] += 1
                s["wrong"] += wrong
                if label[1] == "audit":
                    s["audited"] += 1
                    s["audited_wrong"] += wrong
                    if audited_at.get(c.id, "") > trained_at:
                        s["fresh"] += 1
                        s["fresh_wrong"] += wrong
    for s in sets.values():
        s["upper_bound_audited"] = ls.upper_bound(s["audited_wrong"], s["audited"], confidence) if s["audited"] else None
        s["upper_bound_fresh"] = ls.upper_bound(s["fresh_wrong"], s["fresh"], confidence) if s["fresh"] else None
    return {"model_id": scorer.model_id, "policy": scorer.policy, "candidates": len(candidates), "rule_auto": sets["rule_auto"]["pairs"], "review_pairs": review_pairs,
            "would_promote_certified": promoted_certified, "would_promote_uncertified": promoted_uncertified, "sets": sets, "confidence": confidence,
            "audits": {"flagged": len(audit_rows), "checked": sum(1 for a in audit_rows if a["verdict"]), "pending": sum(1 for a in audit_rows if not a["verdict"])}}


def format_shadow_report(r: dict) -> str:
    pol, target = r["policy"], r["policy"].get("target_false_rate")
    ub = lambda v: "-" if v is None else f"{v:.3%}"
    lines = [f"SHADOW REPORT (read-only: nothing was changed). Model {r['model_id']}, {r['candidates']:,} stored pairs.",
             f"  The rule score auto-merged {r['rule_auto']:,} of them; the model leaves those alone. {r['review_pairs']:,} went to review."]
    if pol.get("promote_threshold") is None:
        lines.append("  The model has NO live promotion threshold, so it promotes nothing: today's decisions stand.")
    else:
        lines.append(f"  LIVE: promote a review pair to an automatic merge at p >= {pol['promote_threshold']:.4f}.")
    lines += [f"  Of the review pairs: the model would promote {r['would_promote_certified']:,} under the LIVE policy"
              + (f", and {r['would_promote_uncertified']:,} under the PROPOSED threshold (p >= {pol.get('promote_threshold_uncertified')}; not live)." if pol.get("promote_threshold_uncertified") is not None else "."),
              "", "  WHAT PEOPLE SAID about each set (labelled pairs only; 'audited' pairs are a random sample, steward decisions are not)",
              f"    {'set':26s}{'pairs':>9}{'labelled':>10}{'wrong':>7}{'audited':>9}{'wrong':>7}{'upper bound (audited)':>24}{'fresh audits':>14}{'upper bound':>13}"]
    names = {"rule_auto": "rule score auto-merges", "promoted_certified": "promoted (live)", "promoted_uncertified": "promoted (proposed)"}
    for key, s in r["sets"].items():
        lines.append(f"    {names[key]:26s}{s['pairs']:>9,}{s['labeled']:>10,}{s['wrong']:>7,}{s['audited']:>9,}{s['audited_wrong']:>7,}{ub(s['upper_bound_audited']):>24}{s['fresh']:>14,}{ub(s['upper_bound_fresh']):>13}")
    a = r["audits"]
    lines.append("")
    lines.append(f"  Audits: {a['flagged']:,} flagged, {a['checked']:,} checked, {a['pending']:,} waiting for a person.")
    if target:
        lines.append(f"  Target: at most {target:.4%} false merges (1 in {1 / target:,.0f}); {r['confidence']:.0%} upper bounds. 'Fresh' audits were made after the model was trained: the unbiased test of its threshold.")
    return "\n".join(lines)
