"""Version 1 against version 2 and its ablations, on several seeds. Appends metrics (not assignments) to out/sweep2.json. Usage: sweep2.py SEED [SEED ...]  (SCALE env, default 0.6)"""
import json, os, random, shutil, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
os.environ["MDM_AUDIT_RATE"] = "0"
import hard_cases as hc
from golden_kernel import Kernel
from kernel_v2 import Kernel2, replay
from app.db import Store
from app.bulk import bulk_load

SCALE = float(os.environ.get("SCALE", 0.6))
OUT = os.path.join(HERE, "out", "sweep2.json")
VARIANTS = {
    "v2": dict(),                              # features + frequency rule + cap 200 + relationships
    "v2_no_features": dict(use_features=False),
    "v2_no_frequency": dict(scorer="rule"),
    "v2_cap30": dict(cap=30),
}

def order(d, seed):
    rows = [r["rid"] for r in d["rows"]]
    random.Random(seed).shuffle(rows)
    return rows

def drive(k, d, raw, seed):
    t = time.perf_counter()
    for rid in order(d, seed): k.arrive(rid, raw[rid])
    for u in d["updates"]: k.update(u["rid"], u["raw"])
    return time.perf_counter() - t

def current(d, raw):
    tmp = tempfile.mkdtemp(); store = Store(os.path.join(tmp, "x.db"))
    try:
        t = time.perf_counter()
        bulk_load(store, "t", "customer", [("src", rid, raw[rid]) for rid in order(d, 11)])
        bulk_load(store, "t", "customer", [("src", u["rid"], u["raw"]) for u in d["updates"]], update=True)
        return {r.source_record_id: r.entity_id for r in store.list_source_records("t", "customer")}, time.perf_counter() - t
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

results = json.load(open(OUT)) if os.path.exists(OUT) else {}
for seed in map(int, sys.argv[1:]):
    d = hc.build(seed, SCALE); raw = {r["rid"]: r["raw"] for r in d["rows"]}
    n = len(d["rows"]) + len(d["updates"])
    entry = {}
    t0 = time.perf_counter()
    assign, secs = current(d, raw)
    entry["current"] = {**{k: v for k, v in hc.metrics(assign, d).items() if k != "by_scenario"}, "seconds": secs, "by_scenario": hc.metrics(assign, d)["by_scenario"]}
    k = Kernel(bridge=True); secs = drive(k, d, raw, 11)
    m = hc.metrics(k.assign(), d)
    entry["v1"] = {**{x: y for x, y in m.items() if x != "by_scenario"}, "seconds": secs, "cand_per_rec": k.stats["candidate_groups"] / n, "by_scenario": m["by_scenario"]}
    for name, kw in VARIANTS.items():
        k = Kernel2(**kw); secs = drive(k, d, raw, 11)
        m = hc.metrics(k.assign(), d); h = hc.household_metrics(k.links("household"), k.assign(), d)
        ra, rl = replay(k.events)
        entry[name] = {**{x: y for x, y in m.items() if x != "by_scenario"}, "seconds": secs, "cand_per_rec": k.stats["candidate_groups"] / n, "skipped_keys": k.stats["skipped_keys"],
                       "household": h, "replay_ok": ra == k.assign() and rl == k.links(), "events": len(k.events), "by_scenario": m["by_scenario"]}
    results[str(seed)] = entry
    json.dump(results, open(OUT, "w"))
    print(f"seed {seed}: {len(d['rows'])} records + {len(d['updates'])} updates, done in {time.perf_counter() - t0:.0f}s", flush=True)
