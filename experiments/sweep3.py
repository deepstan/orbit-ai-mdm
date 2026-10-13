"""Candidate ranking and the key cap: accuracy, scored groups per arrival and time, on several seeds. Usage: sweep3.py SEED [SEED ...]  (SCALE env, default 0.6)"""
import json, os, random, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
os.environ["MDM_AUDIT_RATE"] = "0"
import hard_cases as hc
from kernel_v2 import Kernel2

SCALE = float(os.environ.get("SCALE", 0.6))
OUT = os.path.join(HERE, "out", "sweep3.json")
CAPS = [int(c) for c in os.environ.get("CAPS", "10,30,100,200").split(",")]
RANKINGS = [("none", None), ("keys", 3), ("keys", 6), ("keys", 12), ("idf", 3), ("idf", 6), ("idf", 12)]

def run(seed, cap, rank, k):
    d = hc.build(seed, SCALE); raw = {r["rid"]: r["raw"] for r in d["rows"]}
    order = [r["rid"] for r in d["rows"]]; random.Random(11).shuffle(order)
    kern = Kernel2(cap=cap, relationships=False, rank=("keys" if rank == "none" else rank), max_candidates=k)
    t = time.perf_counter()
    for rid in order: kern.arrive(rid, raw[rid])
    for u in d["updates"]: kern.update(u["rid"], u["raw"])
    secs = time.perf_counter() - t
    n = len(order) + len(d["updates"]); m = hc.metrics(kern.assign(), d)
    return {"seed": seed, "cap": cap, "rank": rank, "k": k, "f1": m["f1"], "precision": m["precision"], "recall": m["recall"], "fn": m["fn"], "fp": m["fp"], "seconds": secs, "n": n,
            "retrieved": kern.stats["candidate_groups"] / n, "scored": kern.stats["scored_groups"] / n}

results = json.load(open(OUT)) if os.path.exists(OUT) else []
for seed in map(int, sys.argv[1:]):
    for cap in CAPS:
        for rank, k in RANKINGS:
            results.append(run(seed, cap, rank, k))
        json.dump(results, open(OUT, "w"))
    print(f"seed {seed} done", flush=True)
