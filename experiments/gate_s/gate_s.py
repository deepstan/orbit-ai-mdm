"""
GATE S, STEP 1: can ONE engine run by MANY workers give nearly the answers it gives one record at a time? (docs/kernel-design.md, section 7; results in docs/gate-s.md)

A deterministic SIMULATION on the in-memory kernel (experiments/kernel_v2.py), as the design asks, before any machinery is built into the application.

HOW IT WORKS. Records are taken in WAVES of W. All W of a wave READ the same state first: for each, the groups it would look at (its candidates, found through the blocking index), what each
group looked like (a signature), and what it would decide (join the best group, or start a new one). Then the W "finish" in a random order and each COMMITS in turn: its read is VALIDATED against the
state as it is by then (same candidate groups? each unchanged?). If so it commits (the kernel's real `arrive`, run against the current state). If not it is a CONFLICT: it goes to the redo queue and
is read again in a later wave. A record redone more than `max_rounds` times is committed alone, as if under a lock (counted as `serialised`: the design's fallback for a hot group).

WHAT THAT PROVES, AND WHAT IT CANNOT. Everything that commits runs against the state it commits into, so the final result is exactly what running the kernel one record at a time in COMMIT ORDER
would give (the tests replay the commit order and require identical groups). Concurrency can therefore cost nothing but ORDER (which the kernel is mildly sensitive to) and the WORK of redoing;
this measures both: the conflict rate, the share of conflicts that were FALSE ALARMS (the group changed but the record would have decided the same), how many rounds records need, and the accuracy
against the sequential run. It does NOT measure a database, locks, or real timing, and its waves are synchronous (all W read together), which is the pessimistic case for conflicts.
Whether a real shared store gives these numbers is step 2 (real threads on PostgreSQL), and is decided only if this passes.

TWO ways to say "a group changed": `version` (any save of the group bumps it: simple, and what a database row version gives) and `content` (only a change to what scoring reads: the golden and the
feature sets). If `content` has far fewer conflicts than `version`, a plain row version is too pessimistic for hot groups and the signature is what the real design should validate.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))
os.environ.setdefault("MDM_AUDIT_RATE", "0")

import hard_cases as hc  # noqa: E402
import kernel_v2 as kv  # noqa: E402
from kernel_v2 import Kernel2  # noqa: E402


def signature(group, mode: str):
    """What a reader depended on. `version`: the group's save counter. `content`: the golden and the feature sets, which are all scoring reads."""
    if mode == "version":
        return group.version
    return hash((tuple(sorted(group.std.items())), tuple((f, tuple(sorted(c.items()))) for f, c in sorted(group.features.items())), len(group.members)))


def derive(k: Kernel2, raw: dict, mode: str):
    """READ-ONLY: the groups this record would look at, and a signature of each. This is what validation compares. Mirrors the first half of Kernel2.arrive."""
    st = k.state
    matching, _ = kv.stage_record(k.domain, k.config, raw)
    weight = {}
    for key in sorted(kv.blocking_keys(matching, k.config)):
        bucket = st.bucket(key, k.cap + 1)
        if not bucket or len(bucket) > k.cap:
            continue
        w = 1.0 / len(bucket) if k.rank == "idf" else 1.0
        for gid in bucket:
            weight[gid] = weight.get(gid, 0.0) + w
    candidates = sorted(weight, key=lambda g: (-weight[g], g))
    if k.max_candidates is not None:
        candidates = candidates[: k.max_candidates]
    return matching, candidates, {g: signature(st.get_group(g), mode) for g in candidates}


def decide(k: Kernel2, matching: dict, candidates: list, known: dict | None = None):
    """READ-ONLY: (the group this record would join, None = it would start a new one; the score of each candidate). The core of Kernel2.arrive's choice; the tests prove it agrees with `arrive`.
    `known` = scores already worked out against groups that have not changed since: they are reused, and only the rest are scored (the cost of validating by re-deciding)."""
    st = k.state
    scored, scores, rescored = [], {}, 0
    for gid in candidates:
        if known is not None and gid in known:
            score = known[gid]
        else:
            score = k.scorer.score(matching, k._view(matching, st.get_group(gid)), kv.Context(st.holder_view(), frozenset({gid}))).score
            rescored += 1
        scores[gid] = score
        scored.append((score, gid))
    scored.sort(key=lambda x: (-x[0], x[1]))
    autos = [t for t in scored if kv.classify(t[0]) == "auto_merge"]
    return (autos[0][1] if autos else None), scores, rescored


class VersionedState(kv.MemoryState):
    def save_group(self, g):
        g.version += 1          # the prototype never bumps a group's version ("the object IS the state"); a real store bumps a row version on EVERY write, so the simulation does too


def new_kernel() -> Kernel2:
    return Kernel2(cap=30, relationships=False, rank="idf", max_candidates=10, state=VersionedState())


def sequential(raw: dict, order: list, updates: list):
    k = new_kernel()
    for rid in order:
        k.arrive(rid, raw[rid])
    for u in updates:
        k.update(u["rid"], u["raw"])
    return k


def simulate(raw: dict, order: list, updates: list, workers: int, mode: str = "content", seed: int = 5, max_rounds: int = 12, prefix: int = 0):
    """One run. Returns (kernel, stats, commit_order). `prefix`: the first records are committed one at a time first (a base that already exists), and the rest are resolved by the workers."""
    k = new_kernel()
    for rid in order[:prefix]:
        k.arrive(rid, raw[rid])
    order = order[prefix:]
    queue, redo = collections.deque(order), collections.deque()
    rng = random.Random(seed)
    tries, committed = collections.Counter(), []
    s = collections.Counter({key: 0 for key in ("attempts", "conflicts", "false_alarms", "necessary", "serialised", "rescored_groups", "reused_scores")})
    conflict_group_sizes = collections.Counter()
    waves = 0
    while queue or redo:
        wave = []
        while len(wave) < workers and (redo or queue):          # a worker takes a retry before new work
            wave.append(redo.popleft() if redo else queue.popleft())
        waves += 1
        reads = {}
        strict = "content" if mode == "redecide" else mode                             # "redecide" tracks content signatures, to know which scores are still good
        for rid in wave:                                                               # every one of the wave reads the SAME state, and decides from it
            matching, cands, sigs = derive(k, raw[rid], strict)
            target, scores, _ = decide(k, matching, cands)
            reads[rid] = (matching, cands, sigs, target, scores)
        rng.shuffle(wave)                                                              # they finish in any order
        for rid in wave:
            matching, cands, sigs, stale_target, stale_scores = reads[rid]
            s["attempts"] += 1
            tries[rid] += 1
            _, now_cands, now_sigs = derive(k, raw[rid], strict)
            same_read = set(now_cands) == set(cands) and all(now_sigs[g] == sigs[g] for g in cands)
            if same_read:
                valid, now_target = True, stale_target
            else:
                good = {g: stale_scores[g] for g in now_cands if g in stale_scores and now_sigs[g] == sigs.get(g)}      # scores against groups that have NOT changed are still good
                now_target, _, rescored = decide(k, matching, now_cands, good) if now_cands else (None, {}, 0)
                s["rescored_groups"] += rescored
                s["reused_scores"] += len(good)
                same = (k._resolve(stale_target) == k._resolve(now_target)) if (stale_target and now_target) else (stale_target is None and now_target is None)
                valid = same if mode == "redecide" else False                          # by re-deciding, only a changed DECISION is a conflict; the strict modes call any change a conflict
            if not valid and tries[rid] <= max_rounds:
                s["conflicts"] += 1
                changed = [g for g in cands if g in now_sigs and now_sigs[g] != sigs[g]]
                for g in changed[:1]:
                    conflict_group_sizes[min(len(k.state.get_group(g).members), 1000)] += 1
                same_decision = (k._resolve(stale_target) == k._resolve(now_target)) if (stale_target and now_target) else (stale_target is None and now_target is None)
                s["false_alarms"] += 1 if same_decision else 0                         # the read was invalidated but the record would have decided the same
                s["necessary"] += 0 if same_decision else 1
                redo.append(rid)
                continue
            if not valid:
                s["serialised"] += 1                                                   # out of rounds: committed alone, as if under a lock
            k.arrive(rid, raw[rid])
            committed.append(rid)
    for u in updates:
        k.update(u["rid"], u["raw"])
    s["waves"], s["records"] = waves, len(order)
    committed = list(committed)
    s["max_tries"] = max(tries.values()) if tries else 0
    s["retried_records"] = sum(1 for n in tries.values() if n > 1)
    s["hot_group_conflicts_over_20_members"] = sum(n for size, n in conflict_group_sizes.items() if size > 20)
    return k, dict(s), committed


def group_sets(k: Kernel2) -> dict:
    a = k.assign()
    members = collections.defaultdict(set)
    for rid, g in a.items():
        members[g].add(rid)
    return {rid: frozenset(members[g]) for rid, g in a.items()}


def evaluate(d: dict, workers_list, modes, n_label: str, seed: int = 5, max_rounds: int = 12):
    raw = {r["rid"]: r["raw"] for r in d["rows"]}
    order = [r["rid"] for r in d["rows"]]
    random.Random(11).shuffle(order)
    t = time.perf_counter()
    base = sequential(raw, order, d["updates"])
    base_secs = time.perf_counter() - t
    base_f1 = hc.metrics(base.assign(), d)["f1"]
    base_groups = group_sets(base)
    out = [{"workers": 1, "mode": "sequential", "f1": base_f1, "seconds": base_secs, "records": len(order)}]
    print(f"{n_label}: {len(order):,} records, sequential F1 {base_f1:.4f} in {base_secs:.1f}s", flush=True)
    for w in workers_list:
        for mode in modes:
            t = time.perf_counter()
            k, s, _ = simulate(raw, order, d["updates"], w, mode, seed, max_rounds)
            secs = time.perf_counter() - t
            f1 = hc.metrics(k.assign(), d)["f1"]
            mine = group_sets(k)
            differ = sum(1 for rid in order if mine[rid] != base_groups[rid])
            row = {"workers": w, "mode": mode, "f1": f1, "delta_f1_points": round((f1 - base_f1) * 100, 3), "attempts": s["attempts"], "conflicts": s["conflicts"],
                   "conflict_rate": round(s["conflicts"] / s["attempts"], 4), "false_alarm_share": round(s["false_alarms"] / s["conflicts"], 3) if s["conflicts"] else 0.0,
                   "waves": s["waves"], "max_tries": s["max_tries"], "retried_records": s["retried_records"], "serialised": s.get("serialised", 0), "rescored_groups": s.get("rescored_groups", 0), "reused_scores": s.get("reused_scores", 0),
                   "records_in_a_different_group_than_sequential": differ, "hot_group_conflicts": s["hot_group_conflicts_over_20_members"],
                   "effective_parallelism": round(w * (1 - s["conflicts"] / s["attempts"]), 1), "seconds": round(secs, 1), "records": len(order)}
            out.append(row)
            print(f"  W={w:>5} {mode:<8} conflicts {row['conflict_rate']:>6.1%} (false alarms {row['false_alarm_share']:>5.0%}), max tries {row['max_tries']:>2}, serialised {row['serialised']:>4}, "
                  f"F1 {f1:.4f} ({row['delta_f1_points']:+.3f} pts), {differ} records grouped differently, effective parallelism {row['effective_parallelism']}", flush=True)
    return out


def main():
    p = argparse.ArgumentParser(description="Gate S step 1: simulate W workers with optimistic validation and a redo queue.")
    p.add_argument("--scale", type=float, default=6, help="benchmark scale (6 is about 7,400 records; 20 about 25,000)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--workers", default="8,64,512,4096")
    p.add_argument("--modes", default="version,content,redecide")
    p.add_argument("--max-rounds", type=int, default=12)
    p.add_argument("--out", default=os.path.join(HERE, "out", "gate_s.json"))
    args = p.parse_args()
    d = hc.build(args.seed, args.scale)
    rows = evaluate(d, [int(w) for w in args.workers.split(",")], args.modes.split(","), f"seed {args.seed} scale {args.scale}", max_rounds=args.max_rounds)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(rows, open(args.out, "w"), indent=2)
    base = rows[0]["f1"]
    print("\nGATE (the design's): F1 within 0.5 points of sequential, and the redo queue drains:")
    for r in rows[1:]:
        ok = abs(r["f1"] - base) * 100 <= 0.5 and r["max_tries"] <= args.max_rounds + 1
        print(f"  W={r['workers']:>5} {r['mode']:<8} {'PASS' if ok else 'FAIL'}  (delta {r['delta_f1_points']:+.3f} pts, max tries {r['max_tries']}, serialised {r['serialised']})")


if __name__ == "__main__":
    main()
