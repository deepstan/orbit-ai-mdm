"""
The reference matcher, and the check that every engine agrees with it.

WHY THIS EXISTS. Matching is a function of PAIRS: whether two records link depends only on those two records' standardized
values, the config and the model. It does not depend on which arrived first. So the right answer for a set of records is:

    1. candidate pairs   every two records that share a usable blocking key
    2. score             score_pair() for each pair (pure)
    3. auto-merge edges  pairs classified auto_merge; review pairs  pairs classified review
    4. entities          connected components of the auto-merge edges (every record is in exactly one; a record with no
                         edge is its own entity)

`resolve_offline` does exactly that, with no database and no ordering. The engines (realtime, batch, accept-now-process-later)
reach the same answer incrementally, one record at a time. `check_equivalence` runs them in several arrival orders and compares
each to this reference. A faster engine (multi-process, DuckDB, Spark) is correct if, and only if, it passes the same check, which
is what makes it safe to build one: "is the cluster result right?" has an answer.

The one place they may legitimately differ: a blocking key shared by more than `max_bucket_size` other records is skipped. The
incremental engines decide that from the bucket size AT THAT MOMENT, this reference from the FINAL size, so the two can differ
when a bucket crosses the cap mid-load. `OfflineResult.capped_keys` says whether that happened; when it is non-zero the check
reports "not comparable" instead of a verdict.

Not covered: the learned (active-learning) model and LLM adjudication. The first can be passed in; the second is not deterministic.
"""
from __future__ import annotations

import os
import random
import tempfile
from collections import defaultdict
from dataclasses import dataclass

from . import active_learning
from .bulk import bulk_load
from .db import Store
from .domains import get_domain_config
from . import learned_scorer
from .matching import blocking_keys, classify, score_pair
from .pipeline import ingest_record, process_pending, run_batch
from .staging import stage_record

NO_BLOCK_KEY = "__no_block__"
TENANT = "equivalence"


@dataclass
class OfflineResult:
    partition: set        # {frozenset(record index)}: every record is in exactly one group
    review_pairs: set     # {frozenset({i, j})}
    auto_pairs: int
    candidate_pairs: int
    capped_keys: int      # keys skipped for being over the cap (see the module docstring)


def resolve_offline(records: list, domain: str, model=None, scorer=None) -> OfflineResult:
    config = get_domain_config(domain)
    std = [stage_record(domain, config, r)[0] for r in records]
    buckets = defaultdict(list)
    for i, s in enumerate(std):
        for k in blocking_keys(s, config):
            if k != NO_BLOCK_KEY:
                buckets[k].append(i)
    pairs, capped = set(), 0
    for members in buckets.values():
        if len(members) - 1 > config.max_bucket_size:  # the engine's rule: the OTHER records on a key may number at most the cap
            capped += 1
            continue
        for a in range(len(members)):
            for b in range(a + 1, len(members)):
                pairs.add((members[a], members[b]))

    parent = list(range(len(records)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    review, auto = set(), 0
    for a, b in pairs:
        breakdown = score_pair(std[a], std[b], config)
        score = breakdown.score
        if model is not None:
            score = round(active_learning.blend_scores(score, model, active_learning.extract_features(std[a], std[b], config)), 4)
        decision = classify(score)
        if decision == "review" and scorer is not None and learned_scorer.promote(scorer, score, breakdown.match_type, breakdown.contributions)[0]:
            decision = "auto_merge"   # the learned scorer's promotion, from the same function the engines call
        if decision == "auto_merge":
            auto += 1
            parent[find(a)] = find(b)
        elif decision == "review":
            review.add(frozenset((a, b)))
    groups = defaultdict(set)
    for i in range(len(records)):
        groups[find(i)].add(i)
    return OfflineResult({frozenset(g) for g in groups.values()}, review, auto, len(pairs), capped)


# --------------------------------------------------------------------------- the engines under test
def read_back(store: Store, domain: str):
    """(entity partition, pending review pairs, indexes of records with NO entity) as the engine left them."""
    recs = store.list_source_records(TENANT, domain)
    idx = {r.id: int(r.source_record_id[1:]) for r in recs}
    groups, entityless = defaultdict(set), []
    for r in recs:
        if r.entity_id is None:
            entityless.append(idx[r.id])
            groups[f"solo:{r.id}"].add(idx[r.id])
        else:
            groups[r.entity_id].add(idx[r.id])
    pending = {frozenset((idx[c.record_a_id], idx[c.record_b_id])) for c in store.list_candidates(TENANT, domain, "pending")}
    return {frozenset(g) for g in groups.values()}, pending, sorted(entityless)


def _fresh() -> Store:
    return Store(os.path.join(tempfile.mkdtemp(), "eq.db"))


def engine_realtime(records, order, domain):
    store = _fresh()
    for i in order:
        ingest_record(store, TENANT, domain, "src", f"r{i}", records[i])
    return read_back(store, domain)


def engine_batch(records, order, domain, chunk=40):
    store = _fresh()
    for lo in range(0, len(order), chunk):
        run_batch(store, TENANT, domain, [("src", f"r{i}", records[i]) for i in order[lo:lo + chunk]])
    return read_back(store, domain)


def engine_deferred(records, order, domain):
    """Accepted now (mode=batch), matched later by process-pending."""
    store = _fresh()
    for i in order:
        ingest_record(store, TENANT, domain, "src", f"r{i}", records[i], mode="batch")
    process_pending(store, domain, limit=10 ** 9)
    return read_back(store, domain)


def engine_bulk(records, order, domain):
    """The bulk pair-based engine (app/bulk.py): everything standardized, paired, scored and clustered set-wise, then written in batches."""
    store = _fresh()
    bulk_load(store, TENANT, domain, [("src", f"r{i}", records[i]) for i in order], chunk=25)
    return read_back(store, domain)


def engine_onboard(records, order, domain, share=0.6, base="bulk"):
    """A new source onboarded onto existing data: the first `share` of the records are loaded (by the bulk engine, or one by one with
    base="realtime"), then the rest are ADDED as a new source system. It must end exactly where one engine given everything would."""
    store = _fresh()
    cut = max(1, int(len(order) * share))
    if base == "realtime":
        for i in order[:cut]:
            ingest_record(store, TENANT, domain, "src", f"r{i}", records[i])
    else:
        bulk_load(store, TENANT, domain, [("src", f"r{i}", records[i]) for i in order[:cut]], chunk=25)
    bulk_load(store, TENANT, domain, [("newsource", f"r{i}", records[i]) for i in order[cut:]], chunk=25, onboard=True)
    return read_back(store, domain)


def engine_onboard_onto_realtime(records, order, domain):
    return engine_onboard(records, order, domain, share=0.4, base="realtime")


ENGINES = {"realtime": engine_realtime, "batch": engine_batch, "deferred": engine_deferred, "bulk": engine_bulk,
           "onboard": engine_onboard, "onboard_onto_realtime": engine_onboard_onto_realtime}


def check_equivalence(records: list, domain: str, *, orders: int = 3, seed: int = 1, engines: dict | None = None, scorer=None) -> dict:
    """Runs each engine in `orders` arrival orders (the given order, then shuffles) and compares entities and review pairs with
    the offline reference. `engines` maps a name to engine(records, order, domain) -> (partition, review_pairs, entityless)."""
    ref = resolve_offline(records, domain, scorer=scorer)
    rng, runs = random.Random(seed), []
    for name, engine in (engines or ENGINES).items():
        for o in range(orders):
            order = list(range(len(records)))
            if o:
                rng.shuffle(order)
            part, pending, entityless = engine(records, order, domain)
            runs.append({"engine": name, "order": o, "same_entities": part == ref.partition, "same_review_pairs": pending == ref.review_pairs,
                         "entities": len(part), "entity_groups_that_differ": len(part ^ ref.partition), "review_pairs": len(pending),
                         "records_without_entity": len(entityless)})
    comparable = ref.capped_keys == 0
    return {"records": len(records), "domain": domain, "comparable": comparable,
            "reference": {"entities": len(ref.partition), "review_pairs": len(ref.review_pairs), "auto_pairs": ref.auto_pairs,
                          "candidate_pairs": ref.candidate_pairs, "capped_keys": ref.capped_keys},
            "runs": runs,
            "ok": (all(r["same_entities"] and r["same_review_pairs"] and r["records_without_entity"] == 0 for r in runs) if comparable else None)}


def format_equivalence(rep: dict) -> str:
    ref = rep["reference"]
    lines = [f"Equivalence check, domain '{rep['domain']}', {rep['records']} records",
             f"  reference (offline): {ref['entities']} entities, {ref['review_pairs']} review pairs, {ref['auto_pairs']} auto-merge pairs "
             f"from {ref['candidate_pairs']} candidate pairs; keys over the cap: {ref['capped_keys']}", "",
             f"  {'engine':10s} {'order':>5s} {'entities':>9s} {'same entities':>14s} {'same review':>12s} {'no entity':>10s}"]
    for r in rep["runs"]:
        lines.append(f"  {r['engine']:10s} {r['order']:>5d} {r['entities']:>9d} {'yes' if r['same_entities'] else 'NO (%d differ)' % r['entity_groups_that_differ']:>14s} "
                     f"{'yes' if r['same_review_pairs'] else 'NO':>12s} {r['records_without_entity']:>10d}")
    verdict = {True: "PASS: every engine, in every order, matches the reference", False: "FAIL",
               None: "NOT COMPARABLE: a blocking key crossed the cap, where incremental and offline legitimately differ"}[rep["ok"]]
    return "\n".join(lines + ["", verdict])
