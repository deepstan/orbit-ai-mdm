"""Run the current engine and both kernel variants on several seeds of the benchmark; append the results to out/sweep.json. Usage: sweep.py SEED [SEED ...]  (SCALE env, default 0.6)"""
import json, os, random, shutil, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
os.environ["MDM_AUDIT_RATE"] = "0"
import hard_cases as hc
from golden_kernel import Kernel
from app.db import Store
from app.bulk import bulk_load

SCALE = float(os.environ.get("SCALE", 0.6))
OUT = os.path.join(HERE, "out", "sweep.json")

def shuffled(data, seed):
    rows = [r["rid"] for r in data["rows"]]
    random.Random(seed).shuffle(rows)
    return rows

def current(data):
    raw = {r["rid"]: r["raw"] for r in data["rows"]}
    tmp = tempfile.mkdtemp(); store = Store(os.path.join(tmp, "x.db"))
    try:
        bulk_load(store, "t", "customer", [("src", rid, raw[rid]) for rid in shuffled(data, 11)])
        bulk_load(store, "t", "customer", [("src", u["rid"], u["raw"]) for u in data["updates"]], update=True)
        return {r.source_record_id: r.entity_id for r in store.list_source_records("t", "customer")}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def kernel(data, order_seed, bridge, margin=0.05):
    raw = {r["rid"]: r["raw"] for r in data["rows"]}
    k = Kernel(margin=margin, bridge=bridge)
    for rid in shuffled(data, order_seed): k.arrive(rid, raw[rid])
    for u in data["updates"]: k.update(u["rid"], u["raw"])
    return k.assign()

results = json.load(open(OUT)) if os.path.exists(OUT) else {}
for seed in map(int, sys.argv[1:]):
    t = time.perf_counter()
    d = hc.build(seed, SCALE)
    entry = {"records": len(d["rows"]), "updates": len(d["updates"])}
    entry["current"] = current(d)
    for name, bridge in (("kernel", False), ("kernelB", True)):
        entry[name + "_r11"] = kernel(d, 11, bridge)
        entry[name + "_r12"] = kernel(d, 12, bridge)
    results[str(seed)] = entry
    json.dump(results, open(OUT, "w"))
    print(f"seed {seed}: {entry['records']} records, done in {time.perf_counter() - t:.0f}s", flush=True)
