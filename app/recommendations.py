"""
What a stewardship agent recommends, and how its recommendations are judged.

A recommendation CHANGES NOTHING. It is stored (agent_recommendations), shown to the steward next to the review item, and when a person decides the item the recommendation gets its
OUTCOME: agreed, disagreed, or human_decided (the agent had said needs_human). Those outcomes are the evidence for how far the agent can be trusted: how often it agrees with people,
and whether its confidence means anything (an agent that is 90% sure should be right about 90% of the time). Nothing here lets an agent act; that is a separate, later decision.
"""
from __future__ import annotations

import math

RECOMMENDATIONS = ("confirm", "reject", "needs_human")
OUTCOMES = ("agreed", "disagreed", "human_decided")
MAX_REASONS, MAX_REASON_CHARS, MAX_EVIDENCE, MAX_EVIDENCE_CHARS, MAX_MODEL, MAX_PROMPT_VERSION = 10, 300, 20, 120, 80, 40
BUCKETS = ((0.0, 0.5), (0.5, 0.7), (0.7, 0.9), (0.9, 1.0))


def wilson_lower(k: int, n: int, z: float = 1.96):
    """The lower end of a 95% interval for a proportion: what agreement we can claim given only n cases. None for n = 0."""
    if n == 0:
        return None
    p = k / n
    return round((p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n), 4)


def wilson_upper(k: int, n: int, z: float = 1.96):
    """The upper end of the same 95% interval: how bad the true rate could still be. With no failures in n cases it is still not zero (0 of 40 can be as much as 8.8%). None for n = 0."""
    if n == 0:
        return None
    p = k / n
    return round(min(1.0, (p + z * z / (2 * n) + z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)), 4)


def check_proposal(recommendation, confidence, reasons, evidence, model, prompt_version) -> list:
    """The problems with a proposal (empty if it is fine). Checked here, whatever the web framework checks, so that every caller gets the same rules."""
    problems = []
    if recommendation not in RECOMMENDATIONS:
        problems.append(f"recommendation must be one of {list(RECOMMENDATIONS)}")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        problems.append("confidence must be a number from 0 to 1")
    if not isinstance(reasons, list) or len(reasons) > MAX_REASONS or not all(isinstance(r, str) and 0 < len(r) <= MAX_REASON_CHARS for r in reasons):
        problems.append(f"reasons must be at most {MAX_REASONS} non-empty strings of at most {MAX_REASON_CHARS} characters")
    if not isinstance(evidence, list) or len(evidence) > MAX_EVIDENCE or not all(isinstance(e, str) and 0 < len(e) <= MAX_EVIDENCE_CHARS for e in evidence):
        problems.append(f"evidence must be at most {MAX_EVIDENCE} non-empty strings of at most {MAX_EVIDENCE_CHARS} characters")
    if not isinstance(model, str) or len(model) > MAX_MODEL:
        problems.append(f"model must be text of at most {MAX_MODEL} characters")
    if not isinstance(prompt_version, str) or len(prompt_version) > MAX_PROMPT_VERSION:
        problems.append(f"prompt_version must be text of at most {MAX_PROMPT_VERSION} characters")
    return problems


def as_dict(rec) -> dict:
    return {"recommendation_id": rec.id, "candidate_id": rec.candidate_id, "recommendation": rec.recommendation, "confidence": rec.confidence, "reasons": rec.reasons, "evidence": rec.evidence,
            "agent": rec.agent, "model": rec.model, "prompt_version": rec.prompt_version, "created_at": rec.created_at,
            "outcome": rec.outcome, "outcome_decision": rec.outcome_decision, "outcome_by": rec.outcome_by, "outcome_at": rec.outcome_at,
            "outcome_seen": rec.outcome_seen}


def summarize(rows) -> dict:
    """rows: [(recommendation, outcome, confidence, how many)] or [(recommendation, outcome, confidence, seen, how many)], seen being whether the person had opened the suggestion before deciding
    (True), had it available and did not (False), or it is not known (None). How often the agent agreed with the person who decided, by what it recommended, by how sure it was, and by whether
    the person had seen the suggestion: ONLY the decisions made without seeing it measure the agent unbiased, because a person who read it first may have been influenced by it.
    needs_human is not an opinion on the pair, so it is counted separately and left out of the agreement rate. A rate is None where nothing was decided yet: never a made-up 0 or 1."""
    by = {r: {"agreed": 0, "disagreed": 0, "human_decided": 0, "open": 0} for r in RECOMMENDATIONS}
    buckets = [{"confidence": f"{lo:.1f}-{hi:.1f}", "decided": 0, "agreed": 0} for lo, hi in BUCKETS]
    viewing = {"decided_blind": [0, 0], "decided_after_viewing": [0, 0], "viewing_not_known": [0, 0]}      # [decided, agreed]
    total = 0
    for row in rows:
        recommendation, outcome, confidence, seen, n = (row if len(row) == 5 else (*row[:3], None, row[3]))
        total += n
        by[recommendation]["open" if outcome is None else outcome] += n
        if outcome in ("agreed", "disagreed"):
            i = next(i for i, (lo, hi) in enumerate(BUCKETS) if lo <= confidence < hi or (hi == 1.0 and confidence == 1.0))
            buckets[i]["decided"] += n
            buckets[i]["agreed"] += n if outcome == "agreed" else 0
            bucket = viewing["viewing_not_known" if seen is None else ("decided_after_viewing" if seen else "decided_blind")]
            bucket[0] += n
            bucket[1] += n if outcome == "agreed" else 0
    agreed = sum(v["agreed"] for v in by.values())
    decided = agreed + sum(v["disagreed"] for v in by.values())
    for b in buckets:
        b["agreement_rate"] = round(b["agreed"] / b["decided"], 4) if b["decided"] else None
    return {"total": total, "open": sum(v["open"] for v in by.values()), "by_recommendation": by, "decided": decided, "agreed": agreed,
            "agreement_rate": round(agreed / decided, 4) if decided else None, "calibration": buckets,
            "by_viewing": {name: {"decided": d, "agreed": a, "agreement_rate": round(a / d, 4) if d else None, "lower_bound_95": wilson_lower(a, d)} for name, (d, a) in viewing.items()},
            "note": "agreement is with the person who decided; calibration compares how sure the agent said it was with how often it agreed. Small counts mean little. "
                    "by_viewing: only decided_blind (a suggestion was there and the person did not open it) is an unbiased measure of the agent; decided_after_viewing may partly measure the suggestion influencing the person."}
