"""
Policy B's guard: ongoing audits, and an automatic rollback.

The model promotes review pairs to automatic merges at a threshold chosen so the POOLED estimated false rate of all automatic merges stays within the target.
That is a point estimate, so it is guarded by what keeps arriving: every automatic merge, the rule score's and the model's, is flagged for a person to check at
2% (learned_scorer.audit_sampled), and `check_scorer` re-estimates the pooled false rate from those audits. If promotions are pushing it over the target, it
switches the model off: new pairs go back to the rule score alone.

WHAT A ROLLBACK DOES NOT DO: it stops FUTURE promotions. Pairs already promoted stay merged, because there is no unmerge yet; the report names the promoted pairs
audited wrong so a person can fix them.
"""
from __future__ import annotations

from . import learned_scorer as ls

MIN_PROMOTED_AUDITS = 100   # fewer checked promotions than this and the estimate is mostly noise: report it, do not act on it


def assess(store, tenant_id: str, domain: str, target: float, confidence: float = 0.95, min_promoted_audits: int = MIN_PROMOTED_AUDITS, with_pairs: bool = True, merge_counts: dict | None = None) -> dict:
    """Two kinds of automatic merge, each audited at random: the rule score's, and the model's promotions. Rolls back only on EVIDENCE OF HARM: when the lower
    confidence bound of the pooled false rate is over the target (so even the optimistic reading is over), and the rule score's own merges are not the cause. Zero errors
    never trigger it, however few audits there are: absence of evidence of safety is not evidence of harm.

    Everything is counted IN SQL (the cost does not grow with the number of audits), and each audit is attributed to who MADE the merge as recorded when it was flagged,
    not to the candidate's current decided_by: unmerging a pair rewrites that, and a promotion a person unmerged must still count against the model.
    with_pairs=False skips listing the pairs judged wrong (the page does not need them; the command line does)."""
    counts = merge_counts if merge_counts is not None else store.count_auto_merges(tenant_id, domain)   # (the page passes a briefly cached total: counting every merge is the one cost that grows with them)
    audited = store.audit_counts(tenant_id, domain)
    wrong_pairs = {"rule": [], "promoted": []}
    if with_pairs:
        for candidate_id, kind in store.audited_wrong(tenant_id, domain):
            wrong_pairs[kind].append(candidate_id)
    side = 1 - (1 - confidence) / 2          # two-sided bounds: half the error on each side
    strata = {}
    for name in ("rule", "promoted"):
        n, k = audited[name]["audited"], audited[name]["wrong"]
        strata[name] = {"merges": counts[name], "audited": n, "wrong": k, "estimate": (k + 0.5) / (n + 1), "lower_bound": ls.lower_bound(k, n, side),
                        "upper_bound": ls.upper_bound(k, n, side), "wrong_pairs": wrong_pairs[name]}
    total = counts["rule"] + counts["promoted"]
    weighted = lambda key: sum(s["merges"] * s[key] for s in strata.values()) / total if total else None
    pooled, pooled_lower, pooled_upper = weighted("estimate"), weighted("lower_bound"), weighted("upper_bound")
    rule_alone_over = strata["rule"]["lower_bound"] > target
    if strata["promoted"]["audited"] < min_promoted_audits:
        verdict, why = "ok", (f"only {strata['promoted']['audited']} promoted merges audited so far (need {min_promoted_audits} before acting on the evidence; at the current audit rate of "
                               f"{ls.audit_rate(promoted=True):.0%} for promotions that takes about {int(min_promoted_audits / max(ls.audit_rate(promoted=True), 1e-9)):,} of them)")
    elif pooled_lower is not None and pooled_lower > target and not rule_alone_over:
        verdict, why = "rollback", (f"even the optimistic reading of the pooled false-merge rate ({pooled_lower:.4%}) is over the target {target:.4%}: "
                                    f"{strata['promoted']['wrong']} of {strata['promoted']['audited']} audited promotions were wrong")
    elif rule_alone_over:
        verdict, why = "ok", (f"the rule score's own automatic merges are over the target ({strata['rule']['wrong']} wrong of {strata['rule']['audited']} audited): "
                              f"that is not the model's doing, so it is not rolled back. Look at the rule score.")
    else:
        verdict, why = "ok", "no evidence that the promotions are pushing the false-merge rate over the target"
    return {"verdict": verdict, "why": why, "target": target, "pooled_estimate": pooled, "pooled_lower_bound": pooled_lower, "pooled_upper_bound": pooled_upper,
            "strata": strata, "confidence": confidence}


def check_scorer(store, tenant_id: str, domain: str) -> dict:
    """Re-estimate from the audits and, if promotions are pushing the false rate over the target, roll the model back (a state file the engines read on every record).
    Run it on a schedule. A rollback stays until a person resets it."""
    scorer = ls.load_scorer(tenant_id, domain)
    if scorer is None:
        return {"verdict": "no_scorer", "why": "no exported scorer for this tenant and domain"}
    state = ls.read_state(tenant_id, domain)
    result = assess(store, tenant_id, domain, scorer.policy.get("target_false_rate", 0.0005), scorer.policy.get("confidence", 0.95))
    result["state"] = state.get("status", "active")
    if state.get("status") == "rolled_back":
        result["why"] = f"already rolled back ({state.get('reason', '')}); a person must reset it"
    elif result["verdict"] == "rollback":
        ls.write_state(tenant_id, domain, "rolled_back", result["why"], {k: result[k] for k in ("pooled_estimate", "target")})
        result["state"] = "rolled_back"
    return result


def format_check(r: dict) -> str:
    if r["verdict"] == "no_scorer":
        return f"No scorer: {r['why']}."
    lines = [f"Scorer state: {r['state'].upper()}. Verdict: {r['verdict'].upper()}: {r['why']}.", ""]
    lines.append(f"  {'automatic merges':20s}{'made':>10}{'audited':>9}{'wrong':>7}{'estimate':>11}{'range (95%)':>22}")
    for name, label in (("rule", "by the rule score"), ("promoted", "promoted by model")):
        s = r["strata"][name]
        span = f"{s['lower_bound']:.3%} to {s['upper_bound']:.3%}"
        lines.append(f"  {label:20s}{s['merges']:>10,}{s['audited']:>9,}{s['wrong']:>7,}{s['estimate']:>11.4%}{span:>22}")
    if r["pooled_estimate"] is not None:
        lines.append(f"  pooled estimate {r['pooled_estimate']:.4%}, plausibly between {r['pooled_lower_bound']:.4%} and {r['pooled_upper_bound']:.4%}; target {r['target']:.4%} = 1 in {1 / r['target']:,.0f}. Rolls back only if even the low end is over it.")
    wrong = r["strata"]["promoted"]["wrong_pairs"]
    if wrong:
        lines.append(f"  Promoted merges audited WRONG ({len(wrong)}), to fix by hand (there is no unmerge yet): " + ", ".join(wrong[:10]) + (" ..." if len(wrong) > 10 else ""))
    return "\n".join(lines)
