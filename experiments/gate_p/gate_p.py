#!/usr/bin/env python3
"""
GATE P (Persistence): how many records per second can ONE worker resolve when the matching kernel's state lives in a real database?

    python3 experiments/gate_p/gate_p.py --check    --db postgresql://USER:PASSWORD@HOST:5432/DBNAME
    python3 experiments/gate_p/gate_p.py            --db postgresql://USER:PASSWORD@HOST:5432/DBNAME --records 100000
    python3 experiments/gate_p/gate_p.py            --db sqlite:///gate_p.db --records 20000          (here, as a proxy only)

WHAT IT RUNS. The same kernel (experiments/kernel_v2.py) on the same generated data, in three modes, each in a tenant of its own in the same database:
    naive    nothing remembered between arrivals; every read goes to the database; one transaction per arrival      (what a straightforward implementation costs)
    cache1   a hot cache, but still a transaction per arrival                                                        (what the cache alone buys)
    hot      a hot cache and the writes of --batch arrivals flushed in ONE transaction with multi-row statements     (the design)
It then applies the dataset's updates (records that change) as a separate measurement.

WHAT IT REPORTS, per mode: records per second overall and per window (so you SEE the rate decay as the tables grow), the share of the time spent inside the database against in Python, statements per
record, latency percentiles of one arrival, flush times, cache hit rates, the memory the process used, and a PROJECTION: the workers needed for --target-rate on --cores cores.

IT CREATES gp_* tables, in schema mdm_gate_p on PostgreSQL (--schema), and removes only its own tenants' rows at the end. Your own tables are never touched. --drop removes the schema or tables.
THE POSTGRESQL ADAPTER (experiments/gate_p/dbs.py: PostgresDb) HAS NEVER RUN AGAINST A SERVER. If a run fails, send the traceback: that is the first place to look.
"""
from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import platform
import random
import resource
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))
os.environ.setdefault("MDM_AUDIT_RATE", "0")


def open_db(url: str, schema: str):
    if url.startswith("sqlite:///"):
        from dbs import SqliteDb
        return SqliteDb(url[len("sqlite:///"):])
    if url.startswith(("postgres://", "postgresql://")):
        from dbs import PostgresDb
        return PostgresDb(url, schema=schema)
    raise SystemExit(f"--db must start with sqlite:/// or postgresql:// (got {url[:20]!r})")


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(math.ceil(p / 100 * len(s))) - 1)]


def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)


def round_trip_ms(db, n=200):
    times = []
    for _ in range(n):
        t = time.perf_counter()
        db.query("SELECT 1")
        times.append((time.perf_counter() - t) * 1000)
    return {"p50_ms": round(pct(times, 50), 3), "p95_ms": round(pct(times, 95), 3)}


def run_mode(cfg: dict) -> dict:
    """One mode, one tenant. A top-level function so a worker process can run it."""
    import hard_cases as hc
    from kernel_v2 import Kernel2
    from sqlstate import SqlState, ensure_schema

    db = open_db(cfg["db"], cfg["schema"])
    ensure_schema(db)
    scale = cfg["records"] / 1227.0
    data = hc.build(cfg["seed"], scale)
    raw = {r["rid"]: r["raw"] for r in data["rows"]}
    order = [r["rid"] for r in data["rows"]]
    random.Random(cfg["seed"] + 1).shuffle(order)
    n = len(order)
    mode = cfg["mode"]
    state = SqlState(db, cfg["tenant"], cache=(mode != "naive"), batch=(cfg["batch"] if mode == "hot" else 1), cache_groups=cfg["cache_groups"], cache_buckets=cfg["cache_buckets"],
                     cache_records=cfg["cache_records"], hub_ttl=cfg["hub_ttl"])
    kernel = Kernel2(cap=cfg["cap"], rank=cfg["rank"], max_candidates=cfg["max_candidates"], relationships=False, state=state)
    window = max(1, n // 10)
    latencies, windows = [], []
    s0, dbs0 = db.statements, db.db_seconds
    t_start = t_win = time.perf_counter()
    for i, rid in enumerate(order, 1):
        t = time.perf_counter()
        kernel.arrive(rid, raw[rid])
        latencies.append(time.perf_counter() - t)
        if i % window == 0:
            now = time.perf_counter()
            windows.append({"arrivals": i, "records_per_second": round(window / (now - t_win), 1)})
            t_win = now
    state.flush()
    wall = time.perf_counter() - t_start
    statements, db_seconds = db.statements - s0, db.db_seconds - dbs0
    out = {"mode": mode, "tenant": cfg["tenant"], "records": n, "seconds": round(wall, 2), "records_per_second": round(n / wall, 1), "windows": windows,
           "statements": statements, "statements_per_record": round(statements / n, 2), "database_seconds": round(db_seconds, 2), "database_share": round(db_seconds / wall, 3),
           "python_seconds": round(wall - db_seconds, 2), "mean_statement_ms": round(db_seconds / max(1, statements) * 1000, 3),
           "arrival_latency_ms": {"p50": round(pct(latencies, 50) * 1000, 3), "p95": round(pct(latencies, 95) * 1000, 3), "p99": round(pct(latencies, 99) * 1000, 3), "max": round(max(latencies) * 1000, 2)},
           "flushes": state.stats["flushes"], "flush_ms": ({"p50": round(pct(state.flush_times, 50) * 1000, 2), "p95": round(pct(state.flush_times, 95) * 1000, 2)} if state.flush_times else None),
           "cache": {k: state.stats[k] for k in ("group_hits", "group_loads", "bucket_hits", "bucket_loads", "record_hits", "record_loads", "holder_loads")},
           "scored_groups_per_record": round(kernel.stats["scored_groups"] / n, 2), "peak_rss_mb": peak_rss_mb()}
    if cfg["updates"] and data["updates"]:
        s1, d1, t1 = db.statements, db.db_seconds, time.perf_counter()
        for u in data["updates"]:
            kernel.update(u["rid"], u["raw"])
        state.flush()
        secs = time.perf_counter() - t1
        out["updates"] = {"count": len(data["updates"]), "seconds": round(secs, 2), "per_second": round(len(data["updates"]) / secs, 1), "statements_per_update": round((db.statements - s1) / len(data["updates"]), 2)}
    if cfg["verify"] and n <= 20_000:
        mem = Kernel2(cap=cfg["cap"], rank=cfg["rank"], max_candidates=cfg["max_candidates"], relationships=False)
        for rid in order:
            mem.arrive(rid, raw[rid])
        if cfg["updates"]:
            for u in data["updates"]:
                mem.update(u["rid"], u["raw"])
        out["identical_to_in_memory"] = bool(mem.assign() == kernel.assign() and mem.events == kernel.events)
    out["groups"] = len(set(kernel.assign().values()))
    db.close()
    return out


def cleanup(db_url, schema, tenants, drop):
    db = open_db(db_url, schema)
    from sqlstate import TABLES, drop_schema
    if drop:
        if db_url.startswith("sqlite"):
            drop_schema(db)
        else:
            db.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    else:
        with db.transaction():
            for tenant in tenants:
                for t in TABLES:
                    db.execute(db.dialect.sql(f"DELETE FROM {t} WHERE tenant_id = ?"), [tenant])
    db.close()


def environment(db):
    info = {"python": platform.python_version(), "platform": platform.platform(), "cpu_count": os.cpu_count(), "database": db.kind}
    try:
        info["database_version"] = db.query("SELECT version()")[0][0] if db.kind == "postgres" else db.query("SELECT sqlite_version()")[0][0]
    except Exception as e:  # noqa: BLE001
        info["database_version"] = f"unknown ({e})"
    return info


def check(args):
    db = open_db(args.db, args.schema)
    from sqlstate import drop_schema, ensure_schema
    print("connected:", environment(db))
    ensure_schema(db)
    print("created the gp_* tables (schema", args.schema + ")" if db.kind == "postgres" else ")")
    with db.transaction():
        db.execute("DELETE FROM gp_meta WHERE tenant_id = 'check'")
        db.insert_many("gp_meta", ["tenant_id", "name", "value"], [["check", "a", 1], ["check", "b", 2]], db.dialect.conflict_suffix(["tenant_id", "name", "value"], ["tenant_id", "name"]))
    rows = db.query(db.dialect.sql("SELECT name, value FROM gp_meta WHERE tenant_id = ? ORDER BY name"), ["check"])
    assert rows == [("a", 1), ("b", 2)], rows
    sql, params = db.dialect.in_list("name", ["a", "zz"])
    assert db.query(db.dialect.sql("SELECT name FROM gp_meta WHERE tenant_id = ? AND ") + sql, ["check"] + params) == [("a",)]
    db.execute(db.dialect.sql("DELETE FROM gp_meta WHERE tenant_id = ?"), ["check"])
    print("insert / read / IN-list / delete work; round trip for a trivial query:", round_trip_ms(db))
    if args.drop:
        cleanup(args.db, args.schema, [], True)
        print("dropped")
    print("OK: the real run can start.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.environ.get("GATE_P_DATABASE_URL"), help="sqlite:///path or postgresql://user:password@host:port/dbname (or set GATE_P_DATABASE_URL)")
    ap.add_argument("--schema", default="mdm_gate_p", help="PostgreSQL schema the experiment lives in (default mdm_gate_p)")
    ap.add_argument("--records", type=int, default=50_000, help="arrivals per mode (default 50000; the generated data has about 5%% updates on top)")
    ap.add_argument("--modes", default="naive,cache1,hot", help="comma list of naive, cache1, hot")
    ap.add_argument("--batch", type=int, default=1000, help="arrivals per flush in hot mode (default 1000)")
    ap.add_argument("--naive-records", type=int, default=None, help="arrivals for naive mode (default: min(--records, 10000), because it is slow)")
    ap.add_argument("--cap", type=int, default=30, help="blocking-key cap (default 30)")
    ap.add_argument("--rank", default="idf", choices=["keys", "idf"])
    ap.add_argument("--max-candidates", type=int, default=10, help="score only the best N candidate groups (default 10; 0 = all)")
    ap.add_argument("--cache-groups", type=int, default=200_000)
    ap.add_argument("--cache-buckets", type=int, default=600_000)
    ap.add_argument("--cache-records", type=int, default=600_000)
    ap.add_argument("--hub-ttl", type=int, default=5000)
    ap.add_argument("--no-updates", action="store_true")
    ap.add_argument("--verify", action="store_true", help="also run the in-memory kernel and compare (only when records <= 20000)")
    ap.add_argument("--workers", type=int, default=1, help="W parallel processes, each with its own tenant and data on the SAME database (hot mode): the database's scaling")
    ap.add_argument("--target-rate", type=float, default=2315.0, help="records/second the whole system must sustain (100M in 12h = 2315)")
    ap.add_argument("--cores", type=int, default=8, help="cores you can give the load")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="gate_p_report.json")
    ap.add_argument("--check", action="store_true", help="connect, create the tables, test one write/read, time a trivial round trip, and stop")
    ap.add_argument("--drop", action="store_true", help="remove the experiment's tables (SQLite) or schema (PostgreSQL) at the end")
    args = ap.parse_args()
    if not args.db:
        raise SystemExit("give --db (or set GATE_P_DATABASE_URL)")
    if args.check:
        return check(args)

    tag = f"{int(time.time())}"
    db = open_db(args.db, args.schema)
    env = environment(db)
    rtt = round_trip_ms(db)
    db.close()
    print("environment:", env)
    print("round trip of a trivial query:", rtt, "(if this is large, it dominates everything below)\n")
    base = dict(db=args.db, schema=args.schema, seed=args.seed, batch=args.batch, cap=args.cap, rank=args.rank, max_candidates=(args.max_candidates or None), hub_ttl=args.hub_ttl,
                cache_groups=args.cache_groups, cache_buckets=args.cache_buckets, cache_records=args.cache_records, updates=not args.no_updates, verify=args.verify)
    results, tenants = [], []
    for mode in [m.strip() for m in args.modes.split(",") if m.strip()]:
        records = args.records if mode != "naive" else (args.naive_records or min(args.records, 10_000))
        cfg = {**base, "mode": mode, "records": records, "tenant": f"gp_{tag}_{mode}"}
        tenants.append(cfg["tenant"])
        print(f"--- {mode}: {records} arrivals ...", flush=True)
        r = run_mode(cfg)
        results.append(r)
        print(f"    {r['records_per_second']} records/s | {r['statements_per_record']} statements/record | database {r['database_share'] * 100:.0f}% of the time | "
              f"mean statement {r['mean_statement_ms']} ms | p99 arrival {r['arrival_latency_ms']['p99']} ms | rate by window: {[w['records_per_second'] for w in r['windows']]}", flush=True)
        if "identical_to_in_memory" in r:
            print("    identical to the in-memory kernel:", r["identical_to_in_memory"])
    scaling = None
    if args.workers > 1:
        print(f"\n--- {args.workers} parallel workers (hot mode, a tenant each, the same database) ...", flush=True)
        cfgs = []
        for w in range(args.workers):
            c = {**base, "mode": "hot", "records": args.records, "tenant": f"gp_{tag}_w{w}", "seed": args.seed + w, "verify": False}
            cfgs.append(c)
            tenants.append(c["tenant"])
        t = time.perf_counter()
        with multiprocessing.get_context("spawn").Pool(args.workers) as pool:
            outs = pool.map(run_mode, cfgs)
        wall = time.perf_counter() - t
        total = sum(o["records"] for o in outs)
        scaling = {"workers": args.workers, "records": total, "wall_seconds": round(wall, 2), "aggregate_records_per_second": round(total / wall, 1),
                   "per_worker_records_per_second": [o["records_per_second"] for o in outs]}
        print(f"    aggregate {scaling['aggregate_records_per_second']} records/s; per worker {scaling['per_worker_records_per_second']}")
    hot = next((r for r in results if r["mode"] == "hot"), None)
    projection = None
    if hot:
        last = hot["windows"][-1]["records_per_second"] if hot["windows"] else hot["records_per_second"]
        rate = min(last, hot["records_per_second"])
        needed = math.ceil(args.target_rate / rate)
        projection = {"per_worker_rate_used": rate, "basis": "the lower of the overall rate and the last window's rate", "workers_needed_for_target": needed, "target_rate": args.target_rate, "cores_available": args.cores,
                      "verdict": ("PASS: fits in the cores available, IF workers scale linearly (check --workers)" if needed <= args.cores else f"DOES NOT FIT: needs about {needed} workers, {args.cores} available")}
        if scaling:
            projection["measured_scaling_efficiency"] = round(scaling["aggregate_records_per_second"] / (scaling["workers"] * rate), 2)
        print("\nPROJECTION:", json.dumps(projection, indent=1))
    report = {"environment": env, "round_trip": rtt, "config": {k: v for k, v in base.items() if k != "db"}, "records": args.records, "results": results, "scaling": scaling, "projection": projection}
    with open(args.out, "w") as f:
        json.dump(report, f, indent=1)
    print(f"\nreport written to {args.out}: please send that file back")
    cleanup(args.db, args.schema, tenants, args.drop)


if __name__ == "__main__":
    main()
