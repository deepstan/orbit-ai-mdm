import sqlite3, time, os, random
DB = "/tmp/delta500k/trial.db"
cold = lambda: os.system("sync; echo 3 > /proc/sys/vm/drop_caches")
con = sqlite3.connect(DB)
marks = lambda n: ",".join("?" * n)
print("QUERY PLANS of the lookups that were slow (3 placeholders stand in for 500):")
for label, sql in (
    ("get_entities_batch   ", f"SELECT * FROM entity_person WHERE tenant_id = ? AND entity_id IN ({marks(3)})"),
    ("get_record_entities  ", f"SELECT id, entity_id FROM source_records WHERE tenant_id = ? AND domain = ? AND id IN ({marks(3)})"),
    ("get_matching_many    ", f"SELECT record_id, matching FROM standardized_records WHERE tenant_id = ? AND domain = ? AND record_id IN ({marks(3)})"),
    ("blocking_bucket_sizes", f"SELECT blocking_key, COUNT(*) FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND blocking_key IN ({marks(3)}) GROUP BY blocking_key"),
    ("blocking_members     ", f"SELECT blocking_key, record_id FROM blocking_keys WHERE tenant_id = ? AND domain = ? AND blocking_key IN ({marks(3)})"),
    ("list_records_of_ents ", f"SELECT * FROM source_records WHERE tenant_id = ? AND domain = ? AND entity_id IN ({marks(3)})")):
    params = ["t", "p"][:sql.count("?") - 3] + ["a", "b", "c"] if "tenant_id = ? AND domain" not in sql else ["t", "p", "a", "b", "c"]
    params = (["t"] if sql.startswith("SELECT * FROM entity_person") else ["t", "p"]) + ["a", "b", "c"]
    print(f"  {label}: {' | '.join(r[3] for r in con.execute('EXPLAIN QUERY PLAN ' + sql, params))}")
print("  entity_person indexes:", [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'entity_person'")], "| primary key:", [r[1] for r in con.execute("PRAGMA table_info(entity_person)") if r[5]])
# ---- the commit hypothesis: the same 24,000-row insert, COMMITTED, under three journal settings (rows are deleted again afterwards)
random.seed(9)
def run(label, pragmas):
    cold()
    c = sqlite3.connect(DB)
    for p in pragmas: c.execute(p)
    rows = [("trial", "person", f"benchcommit{label[:2]}_{i // 12}", f"k:{random.getrandbits(52):013x}") for i in range(24000)]
    t = time.perf_counter()
    c.executemany("INSERT INTO blocking_keys (tenant_id, domain, record_id, blocking_key) VALUES (?,?,?,?)", rows); c.commit()
    if "WAL" in " ".join(pragmas): c.execute("PRAGMA wal_checkpoint(TRUNCATE)")   # include the cost of writing the pages back
    dt = time.perf_counter() - t
    c.execute("DELETE FROM blocking_keys WHERE record_id LIKE ?", (f"benchcommit{label[:2]}_%",)); c.commit()
    if "WAL" in " ".join(pragmas): c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    c.close()
    print(f"  insert 24,000 key rows and COMMIT, {label:46s}: {dt:5.2f}s (cold cache)")
run("rollback journal, synchronous FULL (the default)", [])
run("rollback journal, synchronous OFF (no fsync)", ["PRAGMA synchronous = OFF"])
run("WAL, synchronous NORMAL (incl. writing pages back)", ["PRAGMA journal_mode = WAL", "PRAGMA synchronous = NORMAL"])
c = sqlite3.connect(DB); print("journal mode restored to:", c.execute("PRAGMA journal_mode = DELETE").fetchone()[0], "| rows left behind:", c.execute("SELECT COUNT(*) FROM blocking_keys WHERE record_id LIKE 'benchcommit%'").fetchone()[0])
