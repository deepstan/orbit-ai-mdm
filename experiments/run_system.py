"""Run one system on the hard-cases benchmark in one arrival order and save who ended up with whom. Usage: run_system.py SYSTEM ORDER [margin]"""
import json, os, random, shutil, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
os.environ["MDM_AUDIT_RATE"] = "0"
import hard_cases as hc

def order_of(data, name):
    rows = [r["rid"] for r in data["rows"]]
    if name[1:].isdigit():                                     # r11, r12, ...: a random shuffle with that seed
        random.Random(int(name[1:])).shuffle(rows)
    elif name == "grouped":                                    # copies of a person arrive together
        first = {r["rid"]: r["true"] for r in data["rows"]}
        rows.sort(key=lambda x: (first[x], x))
    elif name == "reversed":
        first = {r["rid"]: r["true"] for r in data["rows"]}
        rows.sort(key=lambda x: (-first[x], x), reverse=False)
    return rows

def run(system, order_name, margin=0.05):
    data = hc.build(1)
    raw = {r["rid"]: r["raw"] for r in data["rows"]}
    order = order_of(data, order_name)
    t = time.perf_counter()
    if system in ("kernel", "kernelB"):
        from golden_kernel import Kernel
        k = Kernel(margin=margin, bridge=(system == "kernelB"))
        for rid in order: k.arrive(rid, raw[rid])
        for u in data["updates"]: k.update(u["rid"], u["raw"])
        assign, extra = k.assign(), {"reviews": len(k.reviews), **dict(k.stats)}
    else:
        from app.db import Store
        from app.pipeline import ingest_record
        from app.bulk import bulk_load
        tmp = tempfile.mkdtemp()
        store = Store(os.path.join(tmp, "x.db"))
        try:
            if system == "regular":
                for rid in order: ingest_record(store, "t", "customer", "src", rid, raw[rid])
                for u in data["updates"]: ingest_record(store, "t", "customer", "src", u["rid"], u["raw"])
            else:  # bulk: everything at once, then the updates as one delta
                bulk_load(store, "t", "customer", [("src", rid, raw[rid]) for rid in order])
                bulk_load(store, "t", "customer", [("src", u["rid"], u["raw"]) for u in data["updates"]], update=True)
            assign = {r.source_record_id: r.entity_id for r in store.list_source_records("t", "customer")}
            extra = {"pending_review_pairs": len(store.list_candidates("t", "customer", status="pending"))}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    secs = time.perf_counter() - t
    m = hc.metrics(assign, data)
    out = {"system": system, "order": order_name, "margin": margin, "seconds": round(secs, 1), "assign": assign, "extra": extra, "f1": m["f1"], "precision": m["precision"], "recall": m["recall"]}
    json.dump(out, open(os.path.join(HERE, "out", f"{system}__{order_name}__{margin}.json"), "w"))
    print(f"{system:8s} {order_name:9s} F1 {m['f1']:.3f}  P {m['precision']:.3f}  R {m['recall']:.3f}  ({secs:.0f}s)", flush=True)

if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 0.05)
