import sys, os, time, tempfile, shutil, functools, collections
proj = sys.argv[1]; sys.path.insert(0, proj)
os.environ["MDM_TOKEN_KEY"] = "a-test-key-that-is-long-enough-0123456789"
from app import benchmark_person as bp
from app.bulk import bulk_load
from app.db import Store
bp.register_person("scalable_tight_plus")
rates = bp.DEFAULT_RATES
raw = lambda i: bp.record_at(7, i, 0.08, **rates)[0]
spent, calls = collections.Counter(), collections.Counter()
def timed(name, fn):
    @functools.wraps(fn)
    def inner(*a, **k):
        t = time.perf_counter()
        try: return fn(*a, **k)
        finally: spent[name] += time.perf_counter() - t; calls[name] += 1
    return inner
for name in ("blocking_bucket_sizes", "blocking_members", "get_staged_many", "get_matching_many", "get_record_entities", "count_tenant_records", "unfinished_in"):
    if hasattr(Store, name): setattr(Store, name, timed(name, getattr(Store, name)))
store = Store("/tmp/delta500k/trial.db")
changed = lambda c: ("synthetic", f"r{1 + 5 * c}", {**raw(1 + 5 * c), "email": f"profile{c}@newmail.test"})
feed = [changed(c) for c in range(5000)] + [("synthetic", f"r{i}", raw(i)) for i in range(900000, 920000)]
d = tempfile.mkdtemp(); T = time.time()
r = bulk_load(store, "trial", "person", feed, update=True, write=False, workdir=d)
total = time.time() - T
s = r.seconds
print(f"{'NEW' if 'new_project' in proj or proj.endswith('customer-mdm-mvp') else 'OLD'} code: total {total:5.1f}s | stage B (pairs) {s['pairs']:5.1f}s | score {s['score']:4.1f}s | standardize {s['standardize']:4.1f}s | {r.candidate_pairs:,} pairs, {r.auto_pairs:,} auto, {r.review_pairs:,} review, {r.existing_entities_touched:,} entities touched, {r.existing_entities_merged:,} merged")
print("   store calls (seconds, calls): " + ", ".join(f"{n} {spent[n]:.1f}s/{calls[n]}" for n, _ in spent.most_common()))
shutil.rmtree(d, True)
