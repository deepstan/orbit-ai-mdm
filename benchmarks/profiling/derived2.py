import sqlite3, time, os, random
DB = "/tmp/delta500k/trial.db"
cold = lambda: os.system("sync; echo 3 > /proc/sys/vm/drop_caches")
random.seed(11)
src = sqlite3.connect(DB)
maxrow = src.execute("SELECT MAX(rowid) FROM standardized_records").fetchone()[0]
def real_rows(n_records, tag):
    start = random.randint(1, maxrow - n_records - 10)
    ids = [r[0] for r in src.execute("SELECT record_id FROM standardized_records WHERE rowid >= ? LIMIT ?", (start, n_records))]
    out = []
    for i, rid in enumerate(ids):
        for (k,) in src.execute("SELECT blocking_key FROM blocking_keys WHERE tenant_id = 'trial' AND domain = 'person' AND record_id = ?", (rid,)):
            out.append(("trial", "person", f"bench{tag}_{i}", k))
    return out
SQL = "INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?, ?, ?, ?)"
for n_records, tag in ((2000, "a"), (10000, "b")):
    rows = real_rows(n_records, tag)
    for label, ordered in (("engine order (by record)", rows), ("sorted by blocking key", sorted(rows, key=lambda r: r[3]))):
        cold()
        con = sqlite3.connect(DB); con.execute("BEGIN")
        t = time.perf_counter(); con.executemany(SQL, ordered); dt = time.perf_counter() - t
        con.execute("ROLLBACK"); con.close()
        print(f"  {n_records:>6,} records, {len(rows):>8,} real keys, {label:26s}: {dt:6.2f}s = {dt / len(rows) * 1e6:5.0f} microseconds per key")
