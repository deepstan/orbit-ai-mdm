"""
Build a synthetic population with the given rates, load it with the bulk engine into a scratch SQLite store, and write every scored pair with the generator's truth to a pickle.
    python extract_pairs.py OUT.pkl N SEED DUP_RATE ssn_rate dob_rate phone_rate email_rate
"""
import json, os, pickle, sqlite3, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
os.environ.setdefault("MDM_TOKEN_KEY", "a-test-key-that-is-long-enough-0123456789")
from app import benchmark_person as bp
from app.bulk import bulk_load
from app.db import Store
out, n, seed, dup = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
rates = dict(zip(("ssn_rate", "dob_rate", "phone_rate", "email_rate"), map(float, sys.argv[5:9])))
bp.register_person("scalable_tight_plus")
T = time.time()
d = tempfile.mkdtemp(); store = Store(os.path.join(d, "s.db"))
records = [("synthetic", f"r{i}", bp.record_at(seed, i, dup, **rates)[0]) for i in range(n)]
print(f"generated {n:,} people in {time.time() - T:.0f}s", flush=True)
r = bulk_load(store, "t", "person", records)
print(f"loaded in {time.time() - T:.0f}s: {r.entities:,} entities, {r.candidate_pairs:,} pairs, {r.auto_pairs:,} auto, {r.review_pairs:,} review", flush=True)
c = sqlite3.connect(os.path.join(d, "s.db"))
rows = c.execute("""SELECT c.score, c.status, c.match_type, c.contributions, ra.source_record_id, rb.source_record_id FROM match_candidates c
                    JOIN source_records ra ON ra.id = c.record_a_id JOIN source_records rb ON rb.id = c.record_b_id WHERE c.status IN ('confirmed','pending')""").fetchall()
idxs = {int(r[4][1:]) for r in rows} | {int(r[5][1:]) for r in rows}
base = {i: bp.record_at(seed, i, dup, **rates)[2] for i in idxs if bp.is_duplicate(seed, i, dup)}
truth = lambda i: base.get(i, i)
data = [{"a": int(r[4][1:]), "b": int(r[5][1:]), "score": r[0], "status": r[1], "type": r[2], "contrib": json.loads(r[3]) if r[3] else [],
         "same": truth(int(r[4][1:])) == truth(int(r[5][1:])), "ident": truth(int(r[4][1:]))} for r in rows]
pickle.dump(data, open(out, "wb"))
auto = [x for x in data if x["status"] == "confirmed"]; rev = [x for x in data if x["status"] == "pending"]
fp = sum(not x["same"] for x in auto)
print(f"saved {out}: {len(data):,} pairs. Fixed 0.92 cutoff: {len(auto):,} auto-merged, {fp} false (1 in {len(auto) / max(fp, 1):,.0f}); review {len(rev):,} at {sum(x['same'] for x in rev) / max(len(rev), 1):.1%} precision; done in {time.time() - T:.0f}s")
