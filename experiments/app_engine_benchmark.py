"""The hard-cases benchmark on the APPLICATION's engine (pipeline.ingest_record, a real SQLite store), kernel against legacy.

The prototype (experiments/kernel_v2.py) measured the design in memory. This measures what the application actually does with it: same data (with life-event updates), same arrival order, the
real entry point. Usage:  python3 experiments/app_engine_benchmark.py [SEED ...]     (SCALE env, default 0.6; about 25 s per seed and engine)
"""
import os
import random
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
os.environ["MDM_AUDIT_RATE"] = "0"

import hard_cases as hc  # noqa: E402
from app.db import Store  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402

SCENARIOS = ("upd_life", "upd_split", "family_shared", "junior_senior", "multi_ident")


def run(engine: str, seed: int, scale: float) -> dict:
    os.environ["MDM_ENGINE"] = engine
    data = hc.build(seed, scale, life_events=True)
    raw = {r["rid"]: r["raw"] for r in data["rows"]}
    order = [r["rid"] for r in data["rows"]]
    random.Random(11).shuffle(order)
    store = Store(os.path.join(tempfile.mkdtemp(), "b.db"))
    started = time.perf_counter()
    for rid in order:
        ingest_record(store, "t", "customer", "src", rid, raw[rid])
    for u in data["updates"]:
        ingest_record(store, "t", "customer", "src", u["rid"], u["raw"])
    elapsed = time.perf_counter() - started
    assign = {r.source_record_id: r.entity_id for r in store.list_source_records("t", "customer")}
    m = hc.metrics(assign, data)
    return {"records": len(raw), "updates": len(data["updates"]), "seconds": elapsed, "precision": m["precision"], "recall": m["recall"], "f1": m["f1"],
            "scenarios": {s: m["by_scenario"].get(s, {"tp": 0, "fn": 0, "fp": 0}) for s in SCENARIOS}}


def main(seeds):
    scale = float(os.environ.get("SCALE", "0.6"))
    totals = {e: {"p": [], "r": [], "f": [], "s": 0.0, **{k: [0, 0] for k in SCENARIOS}} for e in ("legacy", "kernel")}
    for seed in seeds:
        for engine in ("legacy", "kernel"):
            r = run(engine, seed, scale)
            t = totals[engine]
            t["p"].append(r["precision"]); t["r"].append(r["recall"]); t["f"].append(r["f1"]); t["s"] += r["seconds"]
            for k, v in r["scenarios"].items():
                t[k][0] += v["fn"]; t[k][1] += v["fp"]
            print(f"seed {seed} {engine:6s} {r['records']} records + {r['updates']} updates: P {r['precision']:.3f} R {r['recall']:.3f} F1 {r['f1']:.3f} ({r['seconds']:.0f} s)", flush=True)
    mean = lambda v: sum(v) / len(v)
    print(f"\nmean over {len(seeds)} seed(s), scale {scale}. Cells: pairs missed / wrong merges.")
    print(f"{'engine':8s}{'precision':>10s}{'recall':>8s}{'F1':>8s}" + "".join(f"{k:>16s}" for k in SCENARIOS))
    for engine, t in totals.items():
        print(f"{engine:8s}{mean(t['p']):10.3f}{mean(t['r']):8.3f}{mean(t['f']):8.3f}" + "".join(f"{t[k][0]:>10d}/{t[k][1]:<5d}" for k in SCENARIOS))


if __name__ == "__main__":
    main([int(a) for a in sys.argv[1:]] or [1, 2])
