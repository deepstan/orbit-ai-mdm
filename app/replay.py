"""
REPLAY: how would an agent have done on the review items people already decided?

Shadow-mode evaluation of a stewardship agent (docs/replay.md). It takes pairs a PERSON confirmed or rejected in review, rebuilds the evidence as it was when the person decided, hands
that to an agent, and compares the agent's recommendation with what the person decided. It READS the database and writes nothing, so shadow results never mix with live recommendations.

THE TRAP this module is built around: looked at today, a decided pair gives away its own answer. A confirmed pair's two groups have been merged into one; the candidate carries its
verdict, who decided it and when; the record may have changed since. An agent shown that would score perfectly and prove nothing. So the evidence is REBUILT AS OF THE DECISION:
  * the two groups are the connected components of the confirmed pairs decided BEFORE the decision, minus the pair itself (removing the edge a confirm added splits the merge again)
  * every record is shown as it was at that moment (record history), and the golden records are recomputed from those versions
  * the candidate is shown as pending, with no verdict, no decider and no decision time; separations only if a person made them before
  * other pairs of the record are left out (they carry other decisions)
When that cannot be done exactly (a record in the groups changed or was unmerged AFTER the decision, so its past membership is not fully recorded) the item is refused with the reason, unless
the caller allows approximate reconstruction, and the report keeps exact and approximate apart.
"""
from __future__ import annotations

import copy
import math
from collections import Counter
from dataclasses import dataclass
from typing import Callable

from . import pipeline, recommendations
from .recommendations import wilson_lower, wilson_upper      # defined with the statistics they serve; re-exported here, where the replay and its tests use them
from .domains import get_domain_config

MAX_MEMBERS = 25
MAX_HISTORY = 10
THRESHOLDS = (0.5, 0.7, 0.8, 0.9, 0.95, 0.99)


class Unreconstructable(Exception):
    """The evidence as of the decision cannot be rebuilt; the message says why."""


class TooManyErrors(Exception):
    """The agent failed more times than allowed: a broken setup should not keep spending (time, or a model's money). `results` are the ones made so far."""

    def __init__(self, results: list, errors: int, limit: int):
        super().__init__(f"{errors} agent errors, more than the {limit} allowed")
        self.results, self.errors, self.limit = results, errors, limit


@dataclass
class Item:
    candidate: object
    label: str                       # confirm | reject: what the person decided
    source: str = "review"           # review: a person decided a pending pair | audit: a person checked an automatic merge


def labelled_items(store, tenant_id: str, domain: str, include_audits: bool = False) -> list:
    """The pairs a PERSON decided in review. With include_audits, also the automatic merges a person checked (verdict correct = they should merge, wrong = they should not): the agent is
    then judged on the merge as it stood when the system made it. Never a pair a person unmerged (the decision time was overwritten), and never what is still pending."""
    items = []
    for candidate, label, source in store.human_labeled_candidates(tenant_id, domain):
        if (candidate.decided_by or "").startswith("unmerge:") or not candidate.decided_at:
            continue
        if source == "steward":
            items.append(Item(candidate, "confirm" if label == 1 else "reject", "review"))
        elif source == "audit" and include_audits:
            items.append(Item(candidate, "confirm" if label == 1 else "reject", "audit"))
    return sorted(items, key=lambda i: i.candidate.id)


def sample_items(items: list, limit: int | None, seed: int = 1) -> list:
    import random
    if limit is None or limit >= len(items):
        return list(items)
    return sorted(random.Random(seed).sample(items, limit), key=lambda i: i.candidate.id)


# ------------------------------------------------------------------------------------------------ the evidence, as of the decision
def attributes_as_of(record, history: list, t: str):
    """(attributes, version) of a record at time t: the version that was current then (history is oldest first; each row says when its version was replaced)."""
    for row in history:
        if row["superseded_at"] > t:
            return row["attributes"], row["version"]
    return record.attributes, record.version


def _side(store, tenant_id: str, domain: str, start_id: str, exclude_candidate_id: str, t: str, notes: set) -> set:
    """The records connected to start_id by confirmed pairs decided before t, not counting the pair under decision. Notes anything that makes this inexact."""
    seen, frontier = {start_id}, [start_id]
    while frontier:
        record_id = frontier.pop()
        for other, pair in store.list_candidate_pairs(tenant_id, domain, record_id).items():
            if pair["id"] == exclude_candidate_id:
                continue
            if pair["status"] == "confirmed":
                c = store.get_candidate(pair["id"], tenant_id, domain)
                if c is not None and (c.decided_at or c.created_at) == t:
                    notes.add("another pair touching these records was decided at the very same moment, so which of them came first cannot be told")
                if c is not None and (c.decided_at or c.created_at) < t and other not in seen:
                    seen.add(other)
                    frontier.append(other)
            elif pair["status"] == "rejected":
                c = store.get_candidate(pair["id"], tenant_id, domain)
                if c is not None and (c.decided_by or "").startswith("unmerge:") and (c.decided_at or "") > t:
                    notes.add("a pair touching these records was unmerged after the decision, so what it joined before is not recorded")
    return seen


def build_blinded_evidence(store, tenant_id: str, domain: str, candidate, mask: Callable, allow_approximate: bool = False) -> dict:
    """The evidence for one decided pair as it stood when it was decided, with the answer left out. Same shape as the live evidence endpoint, so an agent built for one works on the other."""
    t = candidate.decided_at
    pair_records = store.get_source_records_batch(tenant_id, domain, [candidate.record_a_id, candidate.record_b_id])
    a, b = pair_records.get(candidate.record_a_id), pair_records.get(candidate.record_b_id)
    if a is None or b is None:
        raise Unreconstructable("a record of the pair no longer exists (it was purged)")
    notes: set = set()
    side_a = _side(store, tenant_id, domain, a.id, candidate.id, t, notes)
    side_b = _side(store, tenant_id, domain, b.id, candidate.id, t, notes)
    if side_a & side_b:
        raise Unreconstructable("the two records are connected by other confirmed pairs, so the two groups cannot be told apart")
    wanted = sorted(side_a | side_b)
    members = store.get_source_records_batch(tenant_id, domain, wanted)
    if len(members) != len(wanted):
        raise Unreconstructable("a member of one of the groups no longer exists (it was purged)")
    histories = {rid: store.list_record_history(rid, tenant_id, domain) for rid in members}
    if any(row["superseded_at"] > t for rows in histories.values() for row in rows):
        notes.add("a record in these groups was changed after the decision, so the groups as they were may differ from what the pairs now show")
    if notes and not allow_approximate:
        raise Unreconstructable("; ".join(sorted(notes)))

    asof = {rid: attributes_as_of(members[rid], histories[rid], t) for rid in members}
    config = get_domain_config(domain)

    def view(rid):
        r = members[rid]
        return {"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id, "attributes": mask(asof[rid][0])}

    def group(ids):
        ordered = sorted(ids, key=lambda rid: (members[rid].source_system, members[rid].source_record_id))
        versions = []
        for rid in ordered:
            clone = copy.copy(members[rid])
            clone.attributes = asof[rid][0]
            versions.append(clone)
        decisions, _ = pipeline.compute_golden(config, domain, versions, {})
        golden, _ = pipeline.golden_values(decisions, {}, "replay", tenant_id, domain, "replay")
        return {"entity_id": None, "status": "active", "golden": mask(golden), "member_count": len(ordered), "members": [view(rid) for rid in ordered[:MAX_MEMBERS]],
                "truncated": len(ordered) > MAX_MEMBERS}

    def record(rec):
        attrs, version = asof[rec.id]
        return {"id": rec.id, "source_system": rec.source_system, "source_record_id": rec.source_record_id, "version": version, "entity_id": None, "attributes": mask(attrs),
                "ingested_at": rec.ingested_at, "updated_at": None, "processing_status": None}

    def past(rid):
        return [{"version": row["version"], "attributes": mask(row["attributes"]), "superseded_at": row["superseded_at"]} for row in histories[rid] if row["superseded_at"] <= t][-MAX_HISTORY:]

    # separations a person had made BEFORE this decision, between the record and anyone in the other group
    separated_before = False
    for other, pair in store.list_candidate_pairs(tenant_id, domain, a.id).items():
        if other in side_b and pair["status"] == "rejected":
            c = store.get_candidate(pair["id"], tenant_id, domain)
            if c is not None and (c.decided_by or "").startswith("unmerge:") and (c.decided_at or "") < t:
                separated_before = True
    return {
        "candidate": {"candidate_id": candidate.id, "status": "pending", "score": candidate.score, "match_type": candidate.match_type, "reasons": candidate.reasons,
                      "contributions": candidate.contributions, "decided_by": None, "created_at": candidate.created_at},
        "record_a": record(a), "record_b": record(b), "group_a": group(side_a), "group_b": group(side_b), "same_entity": False,
        "separations": {"records_separated_by_a_person": False, "a_was_separated_from_a_record_in_group_b": separated_before, "entities_separated_by_a_person": separated_before},
        "history": {"record_a": past(a.id), "record_b": past(b.id)},
        "other_pairs_of_record_a": [],
        "as_of": t, "reconstruction": "approximate" if notes else "exact", "notes": sorted(notes),
    }


# ------------------------------------------------------------------------------------------------ the run
def run_replay(store, tenant_id: str, domain: str, items: list, agent, mask: Callable, allow_approximate: bool = False, done: dict | None = None,
               on_result: Callable | None = None, max_errors: int | None = None) -> list:
    """One result per item: skipped (with the reason), an error from the agent, or its recommendation.
    `done` = results already made (by candidate id). Only the ones that HAVE a recommendation are reused: an error is usually a hiccup and is tried again, a skip costs nothing to recompute.
    max_errors: stop, raising TooManyErrors with what is done so far, once the agent has failed MORE than this many times in this run (None = never stop)."""
    from .replay_agents import AgentError
    results, errors = [], 0
    for item in items:
        cid = item.candidate.id
        if done and cid in done and "recommendation" in done[cid]:
            results.append(done[cid])
            continue
        base = {"candidate_id": cid, "label": item.label, "source": item.source, "match_type": item.candidate.match_type, "score": item.candidate.score, "agent": agent.name}
        try:
            evidence = build_blinded_evidence(store, tenant_id, domain, item.candidate, mask, allow_approximate)
        except Unreconstructable as e:
            result = {**base, "skipped": str(e)}
        else:
            base.update(quality=evidence["reconstruction"], group_sizes=[evidence["group_a"]["member_count"], evidence["group_b"]["member_count"]])
            try:
                p = agent.recommend(evidence)
                problems = recommendations.check_proposal(p.get("recommendation"), p.get("confidence"), p.get("reasons", []), p.get("evidence", []), p.get("model", ""), p.get("prompt_version", ""))
                result = {**base, "error": "the agent's answer broke the rules: " + "; ".join(problems)} if problems else \
                    {**base, "recommendation": p["recommendation"], "confidence": float(p["confidence"]), "reasons": p.get("reasons", []), "model": p.get("model", ""), "prompt_version": p.get("prompt_version", "")}
            except AgentError as e:
                result = {**base, "error": str(e)}
        results.append(result)
        if on_result:
            on_result(result)
        if result.get("error"):
            errors += 1
            if max_errors is not None and errors > max_errors:
                raise TooManyErrors(results, errors, max_errors)
    return results


# ------------------------------------------------------------------------------------------------ the report
def _rate(k: int, n: int):
    return round(k / n, 4) if n else None


def _score_band(score):
    if score is None:
        return "no score"
    low = min(int(score * 10) / 10, 0.9)
    return f"{low:.1f}-{min(low + 0.1, 1.0):.1f}"


def summarize_replay(results: list, stopped_early: bool = False) -> dict:
    skipped = Counter(r["skipped"] for r in results if "skipped" in r)
    errors = [r for r in results if r.get("error")]
    answered = [r for r in results if r.get("recommendation")]
    decided = [r for r in answered if r["recommendation"] in ("confirm", "reject")]
    right = [r for r in decided if r["recommendation"] == r["label"]]
    confusion = {rec: {"confirm": 0, "reject": 0} for rec in recommendations.RECOMMENDATIONS}
    rows = Counter()
    for r in answered:
        confusion[r["recommendation"]][r["label"]] += 1
        outcome = "human_decided" if r["recommendation"] == "needs_human" else ("agreed" if r["recommendation"] == r["label"] else "disagreed")
        rows[(r["recommendation"], outcome, r["confidence"])] += 1
    said_confirm = [r for r in decided if r["recommendation"] == "confirm"]
    truly_confirm = [r for r in answered if r["label"] == "confirm"]
    truly_different = [r for r in answered if r["label"] == "reject"]           # failing to answer must never improve a rate: only pairs it answered about
    false_merges = [r for r in said_confirm if r["label"] == "reject"]
    missed = [r for r in decided if r["recommendation"] == "reject" and r["label"] == "confirm"]

    selective = []
    for t in THRESHOLDS:
        chosen = [r for r in decided if r["confidence"] >= t]
        k = sum(1 for r in chosen if r["recommendation"] == r["label"])
        confirms = [r for r in chosen if r["recommendation"] == "confirm"]
        wrong_confirms = sum(1 for r in confirms if r["label"] == "reject")
        selective.append({"confidence_at_least": t, "decided": len(chosen), "share_of_answered": _rate(len(chosen), len(answered)), "agreed": k, "agreement_rate": _rate(k, len(chosen)),
                          "agreement_lower_bound_95": wilson_lower(k, len(chosen)), "confirms": len(confirms), "false_merges": wrong_confirms,
                          "false_merge_upper_bound_95": wilson_upper(wrong_confirms, len(confirms))})

    def slice_by(key):
        out = {}
        for name in sorted({str(key(r)) for r in decided}):
            group = [r for r in decided if str(key(r)) == name]
            k = sum(1 for r in group if r["recommendation"] == r["label"])
            out[name] = {"decided": len(group), "agreed": k, "agreement_rate": _rate(k, len(group))}
        return out

    labelled = [r for r in results if "label" in r and "skipped" not in r]
    confirm_share = _rate(sum(1 for r in labelled if r["label"] == "confirm"), len(labelled))
    scored = [r for r in labelled if r.get("score") is not None]
    best = None
    for t in sorted({r["score"] for r in scored}):                       # the best single score cut, tuned on THESE very items: an optimistic bar for 'just use the score'
        k = sum(1 for r in scored if (r["score"] >= t) == (r["label"] == "confirm"))
        if best is None or k > best["correct"]:
            best = {"threshold": t, "correct": k}
    return {
        "items": len(results), "stopped_early": stopped_early, "skipped": dict(skipped), "errors": len(errors), "answered": len(answered), "needs_human": len(answered) - len(decided),
        "coverage": _rate(len(decided), len(answered)),
        "agreement": {"decided": len(decided), "agreed": len(right), "rate": _rate(len(right), len(decided)), "lower_bound_95": wilson_lower(len(right), len(decided))},
        "confusion": confusion,
        "confirm": {"precision": _rate(sum(1 for r in said_confirm if r["label"] == "confirm"), len(said_confirm)),
                    "recall": _rate(sum(1 for r in truly_confirm if r["recommendation"] == "confirm"), len(truly_confirm))},
        # The error that costs most is a FALSE MERGE (it joins two different real-world entities, and undoing it is work). Two views of it, each with how bad it could still be:
        "false_merges": {"count": len(false_merges), "of_its_confirms": len(said_confirm), "share_of_its_confirms": _rate(len(false_merges), len(said_confirm)),
                         "share_upper_bound_95": wilson_upper(len(false_merges), len(said_confirm)),
                         "pairs_that_were_different": len(truly_different), "rate_on_different_pairs": _rate(len(false_merges), len(truly_different)),
                         "rate_upper_bound_95": wilson_upper(len(false_merges), len(truly_different)),
                         "missed_merges": len(missed), "missed_merge_rate": _rate(len(missed), len(truly_confirm))},
        "if_it_acted_only_above": selective,
        "calibration": recommendations.summarize([(rec, outcome, conf, n) for (rec, outcome, conf), n in rows.items()])["calibration"],
        "slices": {"source": slice_by(lambda r: r.get("source", "review")), "reconstruction": slice_by(lambda r: r.get("quality")), "match_type": slice_by(lambda r: r.get("match_type")),
                   "score_band": slice_by(lambda r: _score_band(r.get("score"))),
                   "group_sizes": slice_by(lambda r: "both groups are single records" if r.get("group_sizes") == [1, 1] else "a group has several records")},
        "baselines": {"people_confirmed_share": confirm_share, "always_the_commoner_answer": None if confirm_share is None else round(max(confirm_share, 1 - confirm_share), 4),
                      "best_score_cut_tuned_on_these_items": None if best is None else {"threshold": best["threshold"], "accuracy": _rate(best["correct"], len(scored))}},
        "notes": ["Agreement is with the PERSON who decided; people disagree and err, so 100% is not the goal and the labels are noisy.",
                  "The lower bound is the 95% Wilson bound: what can be claimed given only that many cases. A high rate on few cases means little.",
                  "'if it acted only above' shows what agreement it would have had if it had acted only on its most confident answers, and on how many: the evidence a later tier would need.",
                  "The best-score-cut baseline is tuned on these same items, so it flatters the score: an agent that cannot beat it adds nothing over the score."],
    }


def format_report(report: dict) -> str:
    def pct(x):
        return "n/a" if x is None else f"{x * 100:.1f}%"
    a = report["agreement"]
    lines = (["STOPPED EARLY: the agent failed more often than allowed, so this covers only what was done before that. The results so far are kept; fix the agent and run again.", ""]
             if report.get("stopped_early") else [])
    lines += [f"Replayed {report['items']} decided review items: {report['answered']} answered, {report['errors']} agent errors, {sum(report['skipped'].values())} skipped.",
             f"It gave a verdict (confirm or reject) on {a['decided']} and said needs_human on {report['needs_human']} (coverage {pct(report['coverage'])})."]
    for reason, n in sorted(report["skipped"].items(), key=lambda kv: -kv[1]):
        lines.append(f"  skipped {n}: {reason}")
    lines += ["", f"AGREEMENT with the person who decided: {a['agreed']} of {a['decided']} = {pct(a['rate'])} (at least {pct(a['lower_bound_95'])} with 95% confidence)",
              f"  confirm: precision {pct(report['confirm']['precision'])}, recall {pct(report['confirm']['recall'])}",
              f"  people confirmed {pct(report['baselines']['people_confirmed_share'])} of these items; always giving the commoner answer would agree {pct(report['baselines']['always_the_commoner_answer'])}"]
    fm = report["false_merges"]
    lines += ["", "FALSE MERGES (it said confirm; the person said these were different: the costly error):",
              f"  {fm['count']} of its {fm['of_its_confirms']} confirms = {pct(fm['share_of_its_confirms'])} (could be as high as {pct(fm['share_upper_bound_95'])} with 95% confidence)",
              f"  {fm['count']} of the {fm['pairs_that_were_different']} pairs that really were different = {pct(fm['rate_on_different_pairs'])} (as high as {pct(fm['rate_upper_bound_95'])})",
              f"  missed merges (it said reject; the person confirmed): {fm['missed_merges']} = {pct(fm['missed_merge_rate'])} of the pairs that really matched"]
    best = report["baselines"]["best_score_cut_tuned_on_these_items"]
    if best:
        lines.append(f"  the best single score cut ({best['threshold']}), tuned on these very items, would agree {pct(best['accuracy'])}: the bar to beat")
    lines += ["", "If it had acted only on its most confident answers:"]
    for s in report["if_it_acted_only_above"]:
        lines.append(f"  confidence >= {s['confidence_at_least']:<4}: {s['decided']:>5} items ({pct(s['share_of_answered'])} of answered), agreement {pct(s['agreement_rate'])}, at least {pct(s['agreement_lower_bound_95'])}; "
                     f"{s['false_merges']} false merges in {s['confirms']} confirms (as high as {pct(s['false_merge_upper_bound_95'])})")
    lines += ["", "Is its confidence honest? (agreement by how sure it said it was)"]
    for b in report["calibration"]:
        lines.append(f"  said {b['confidence']}: {b['decided']:>5} decided, agreed {pct(b['agreement_rate'])}")
    for title, rows in report["slices"].items():
        if rows:
            lines += ["", f"By {title.replace('_', ' ')}:"] + [f"  {name}: {r['decided']} decided, agreement {pct(r['agreement_rate'])}" for name, r in rows.items()]
    return "\n".join(lines + [""] + ["* " + n for n in report["notes"]])
