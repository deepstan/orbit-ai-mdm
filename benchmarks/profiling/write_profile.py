import sys, os, time, tempfile, shutil, functools, collections
proj, new_start, changed_offset = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]); sys.path.insert(0, proj)
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
for name in ("save_source_records_batch", "replace_derived_batch", "save_entities_batch", "add_lineage_batch", "save_candidates_batch", "apply_onboarding",
             "list_records_of_entities", "get_staged_many", "get_entities_batch", "get_record_entities", "get_matching_many", "blocking_bucket_sizes", "blocking_members", "find_source_records_by_keys", "blocking_keys_of"):
    if hasattr(Store, name): setattr(Store, name, timed(name, getattr(Store, name)))
store = Store("/tmp/delta500k/trial.db")
changed = lambda c: ("synthetic", f"r{changed_offset + 5 * c}", {**raw(changed_offset + 5 * c), "email": f"w{changed_offset}x{c}@newmail.test"})
feed = [changed(c) for c in range(5000)] + [("synthetic", f"r{i}", raw(i)) for i in range(new_start, new_start + 20000)]
T = time.time()
r = bulk_load(store, "trial", "person", feed, update=True)
total = time.time() - T
s = r.seconds
label = "OLD" if "old_project" in proj else "NEW"
print(f"{label} code, a real 25,000-record delta (5,000 changed + 20,000 new): total {total:5.1f}s | classify {s['classify']:.1f} standardize {s['standardize']:.1f} pairs {s['pairs']:.1f} score {s['score']:.1f} cluster {s['cluster']:.2f} WRITE {s['write']:.1f}")
print(f"   {r.new_records:,} new, {r.changed_records:,} changed; {r.candidate_pairs:,} pairs, {r.auto_pairs:,} auto, {r.review_pairs:,} review; {r.existing_entities_touched:,} entities rebuilt, {r.existing_entities_merged:,} merged, {r.entities:,} new entities")
print("   store calls, seconds/calls: " + ", ".join(f"{n} {spent[n]:.1f}/{calls[n]}" for n, _ in spent.most_common()))
