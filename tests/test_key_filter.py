"""
THE KEY FILTER (app/keyfilter.py, `bulk_ingest.py --bulk --key-filter`): skip the blocking lookups that would find nothing.

What must NEVER happen is a wrong "this key is new": it would lose a candidate, and two records of one person would become two entities. So most of this file is about what the filter must not change:
  * the Bloom filter itself: no false negatives, ever; a plausible false-positive rate; it gives up cleanly when it fills
  * THE RESULT: the same groups, the same golden records and the same pairs (by status, type and score) with the filter as without it, on random streams with duplicates, typos and updates,
    on a base that ALREADY holds records, with a filter too small for the load, and under the old engine, which ignores it
  * a NEGATIVE CONTROL: a filter that skipped the scan of the existing index would lose candidates, and the test that says so must fail for it
  * THE SAVING: it really does skip the lookups (counted at the database)
  * the loader's flag

THE COMPARISON IS EXACT. (Writing it found that two identical loads WITHOUT the filter could disagree about which of two tied groups a record joined: one run joined P-00226 to P-00047,
another to P-00005, because ties were broken by random ids. That is fixed, see tests/test_determinism.py; ids are still issued in a fixed order here, which is now belt and braces.)
Run with:  python3 -m unittest tests.test_key_filter -v
"""
import collections
import csv
import itertools
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

from app import benchmark_person as bp  # noqa: E402
from app import domains  # noqa: E402
from app import domain_config  # noqa: E402
from app.db import Store  # noqa: E402
from app.keyfilter import KeyFilter, KeyFilterTooBig, build_for_load  # noqa: E402
from app.pipeline import run_batch  # noqa: E402

TOKEN_KEY = "a-long-secret-of-at-least-32-characters-0123456789"
FIELDS = ["first_name", "last_name", "dob", "ssn", "phone", "email", "address_line1", "postal_code"]
TENANT, DOMAIN = "t", "person"


# ================================================================================================ the filter itself
class TestTheBloomFilter(unittest.TestCase):
    def keys(self, n, seed=1):
        rng = random.Random(seed)
        return [f"k:{rng.getrandbits(64):016x}" for _ in range(n)]

    def test_it_never_forgets_a_key_it_was_given(self):
        f = KeyFilter(20_000)
        keys = self.keys(20_000)
        f.add(keys)
        self.assertEqual(f.check_and_add(keys), set(keys))                                          # every one is reported as maybe-held: no false negative, whatever the load

    def test_the_false_maybe_rate_is_about_what_ten_bits_a_key_buys(self):
        f = KeyFilter(20_000)
        f.add(self.keys(20_000, seed=1))
        probes = self.keys(20_000, seed=2)                                                           # keys it was never given, asked WITHOUT adding them (check_and_add adds, which would fill it as it measures)
        rate = sum(1 for k in probes if f._present_and_set(k, False)) / len(probes)
        self.assertLess(rate, 0.02)                                                                  # designed for about 1% (theory: 0.8%)
        self.assertGreater(rate, 0.001)

    def test_a_record_sees_only_other_records_keys_never_its_own(self):
        f = KeyFilter(1000)
        self.assertEqual(f.check_and_add({"k:a", "k:b", "k:c"}), set())                              # the first record: nothing else holds anything
        self.assertEqual(f.check_and_add({"k:b", "k:c", "k:d"}), {"k:b", "k:c"})                     # the second shares b and c with the first, and only those
        self.assertEqual(f.check_and_add({"k:d", "k:e"}), {"k:d"})                                   # and the third shares d with the second

    def test_the_load_asked_about_and_how_much_was_skipped_is_counted(self):
        f = KeyFilter(1000)
        f.check_and_add({"k:a", "k:b"})
        f.check_and_add({"k:b", "k:c"})
        self.assertEqual((f.asked, f.maybe), (4, 1))
        self.assertIn("(75%)", f.summary())

    def test_a_filter_that_has_filled_up_stops_filtering_instead_of_lying(self):
        f = KeyFilter(1, bits_per_key=1)                                                             # the 4,096-bit minimum: far too small for what follows
        with self.assertLogs("mdm.keyfilter", level="WARNING"):
            for chunk in range(0, 3000, 10):
                f.check_and_add({f"k:{i}" for i in range(chunk, chunk + 10)})
        fresh = {f"never:{i}" for i in range(10)}
        self.assertTrue(f.giving_up)
        self.assertEqual(f.check_and_add(fresh), fresh)                                              # every key is a maybe: slow, never wrong

    def test_its_size_is_sane_and_a_whole_number_of_bytes(self):
        for expected in (0, 1, 7, 1000, 123_457):
            f = KeyFilter(expected)
            self.assertGreaterEqual(f.bits, 4096)
            self.assertEqual(f.bits % 8, 0)
        self.assertEqual(KeyFilter(1_000_000, bits_per_key=10).bits, 10_000_000)


# ================================================================================================ the data
def people(n, seed=7, dup_rate=0.15):
    """Realistic people: duplicates of earlier records with typos, and records that share a hub name or place. Returns [(source_system, source_record_id, attributes)]."""
    out = []
    for i in range(n):
        raw, _truth, base = bp.record_at(seed, i, dup_rate, **bp.DEFAULT_RATES)
        out.append(("crm", f"P-{i:05d}", {k: v for k, v in raw.items() if v}))
    return out


def snapshot(store):
    """What a load MEANS: the groups, each group's golden record, and the pairs by status, type and score."""
    records = store.list_source_records(TENANT, DOMAIN)
    by_entity = collections.defaultdict(set)
    for r in records:
        by_entity[r.entity_id].add(r.source_record_id)
    key = {e: frozenset(m) for e, m in by_entity.items()}
    golden = {key[e.id]: tuple(sorted((k, str(v)) for k, v in e.attributes.items())) for e in store.list_entities(TENANT, DOMAIN, "active")}
    natural = {r.id: r.source_record_id for r in records}
    pairs = collections.Counter((tuple(sorted((natural[c.record_a_id], natural[c.record_b_id]))), c.status, c.match_type, round(c.score, 6))      # which two records, not just how many
                                for c in store.list_candidates(TENANT, DOMAIN, status=None))
    return {"groups": set(key.values()), "golden": golden, "pairs": pairs}


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._registry = dict(domains.REGISTRY)
        cls._env = mock.patch.dict(os.environ, {"MDM_TOKEN_KEY": TOKEN_KEY, "MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel",
                                                "MDM_DOMAINS_DIR": os.path.join(ROOT, "domain_templates")})
        cls._env.start()
        domain_config.reset()                                                                        # built-ins only, and forget what was loaded (the loader remembers directories)
        domain_config.load_configured_domains()                                                      # the PRODUCTION person configuration (its blocking is the one the 92% was measured on)

    @classmethod
    def tearDownClass(cls):
        domain_config.reset()
        domains.REGISTRY.clear()
        domains.REGISTRY.update(cls._registry)                                                       # leave the registry as other tests expect it
        cls._env.stop()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.n = 0
        counter = itertools.count(1)
        patch = mock.patch("app.models.new_id", lambda prefix="": f"{prefix}{next(counter):032x}")    # ids in the order they are made: every tie is broken the same way in every load of this test
        patch.start()
        self.addCleanup(patch.stop)

    def store(self):
        self.n += 1
        return Store(os.path.join(self.tmp, f"s{self.n}.db"))

    def load(self, store, records, key_filter=None, batch=100):
        for i in range(0, len(records), batch):                                                      # in batches, as the loader does
            run_batch(store, TENANT, DOMAIN, records[i:i + batch], key_filter=key_filter)
        return store


# ================================================================================================ it must not change the result
class TestItNeverChangesTheResult(Base):
    def assertSame(self, a, b, msg=None):
        self.assertEqual(a["groups"], b["groups"], msg)
        self.assertEqual(a["golden"], b["golden"], msg)
        self.assertEqual(a["pairs"], b["pairs"], msg)

    def test_on_random_streams_with_duplicates_and_typos(self):
        for seed in (1, 2, 3, 4):
            records = people(500, seed=seed)
            plain = snapshot(self.load(self.store(), records))
            filtered = snapshot(self.load(self.store(), records, KeyFilter(len(records) * 20)))
            self.assertSame(plain, filtered, f"seed {seed}")
            self.assertLess(len(plain["groups"]), len(records))                                      # and the data really did contain duplicates to find

    def test_with_a_filter_far_too_small_for_the_load(self):
        records = people(500)
        plain = snapshot(self.load(self.store(), records))
        tiny = KeyFilter(10, bits_per_key=1)                                                         # fills up almost at once and gives up
        with self.assertLogs("mdm.keyfilter", level="WARNING"):
            filtered = snapshot(self.load(self.store(), records, tiny))
        self.assertSame(plain, filtered)
        self.assertTrue(tiny.giving_up)

    def test_on_a_base_that_already_holds_records_the_filter_starts_from_them(self):
        records = people(600, seed=5)
        first, second = records[:300], records[300:]
        cross = [i for i in range(300, 600) if (bp.record_at(5, i, 0.15, **bp.DEFAULT_RATES)[2] is not None and bp.record_at(5, i, 0.15, **bp.DEFAULT_RATES)[2] < 300)]
        self.assertGreaterEqual(len(cross), 5, "the data must contain second-half duplicates of first-half records, or this proves nothing")
        plain = snapshot(self.load(self.store(), records))
        store = self.load(self.store(), first)                                                       # the first half loaded earlier, with no filter at all
        kf, message = build_for_load(store, TENANT, DOMAIN, len(second))
        self.assertIsNotNone(kf, message)
        self.assertIn("key filter ON", message)
        self.assertGreater(int(message.split(":")[1].split(" keys")[0].replace(",", "")), 0)         # it loaded what was already there
        self.assertSame(plain, snapshot(self.load(store, second, kf)))

    def test_NEGATIVE_CONTROL_a_filter_that_did_not_read_the_existing_index_loses_candidates(self):
        records = people(600, seed=5)
        first, second = records[:300], records[300:]
        plain = snapshot(self.load(self.store(), records))
        store = self.load(self.store(), first)
        broken = KeyFilter(len(records) * 20)                                                       # sized right but NOT loaded from the index: it thinks every key is new
        wrong = snapshot(self.load(store, second, broken))
        self.assertNotEqual(plain["groups"], wrong["groups"])                                        # duplicates of the first half are not found: exactly the failure rule 2 of the contract prevents
        self.assertGreater(len(wrong["groups"]), len(plain["groups"]))

    def test_a_changed_record_is_remembered_so_the_records_after_it_find_it(self):
        base = people(60, seed=9)
        x = ("crm", "X-1", {"first_name": "Zephyrine", "last_name": "Quillfeather", "email": "zq@example.com", "phone": "404-555-0100", "postal_code": "30301"})
        x_changed = ("crm", "X-1", {"first_name": "Marguerite", "last_name": "Oberholtzer", "email": "mo@example.org", "phone": "706-555-0111",      # the SAME record, corrected entirely:
                                    "postal_code": "31201"})                                         # EVERY key is new, so the new keys are the only way to find it again
        y = ("crm", "Y-2", {**x_changed[2]})                                                         # a new record identical to the CHANGED version
        results = []
        for kf in (None, KeyFilter(2000)):
            store = self.store()
            for batch in (base, [x], [x_changed], [y]):                                              # three separate batches, one filter across them, as the loader does
                run_batch(store, TENANT, DOMAIN, batch, key_filter=kf)
            results.append(snapshot(store))
        self.assertSame(results[0], results[1])
        joined = [g for g in results[1]["groups"] if {"X-1", "Y-2"} <= g]
        self.assertEqual(len(joined), 1)                                                             # Y found X's NEW keys: the changed record's keys went into the filter

    def test_the_old_engine_ignores_it_entirely(self):
        records = people(150)
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"}):
            plain = snapshot(self.load(self.store(), records))
            kf = KeyFilter(10_000)
            filtered = snapshot(self.load(self.store(), records, kf))
        self.assertSame(plain, filtered)
        self.assertEqual(kf.asked, 0)                                                                # it was never consulted

    def test_the_realtime_path_has_no_filter_to_consult(self):
        from app.pipeline import ingest_record
        store = self.store()
        for ss, sid, attrs in people(40):
            ingest_record(store, TENANT, DOMAIN, ss, sid, attrs)                                     # takes no key_filter argument: one record at a time, other writers possible
        self.assertGreater(len(store.list_source_records(TENANT, DOMAIN)), 0)


# ================================================================================================ it saves what it claims
class TestItReallySkipsTheLookups(Base):
    def lookups_during(self, fn):
        asked = []
        real = sqlite3.connect

        class Counting(sqlite3.Connection):
            def execute(inner, sql, *a):
                if "FROM blocking_keys" in sql and sql.lstrip().upper().startswith("SELECT"):
                    asked.append(1)
                return super().execute(sql, *a)
        with mock.patch.object(sqlite3, "connect", lambda *a, **k: real(*a, factory=Counting, **k)):
            fn()
        return len(asked)

    def test_a_stream_of_unrelated_people_needs_no_blocking_lookup_at_all(self):
        records = people(300, seed=11, dup_rate=0.0)                                                 # nobody is a duplicate of anybody
        plain = self.lookups_during(lambda: self.load(self.store(), records))
        filtered = self.lookups_during(lambda: self.load(self.store(), records, KeyFilter(len(records) * 20)))
        self.assertGreater(plain, 2 * len(records) - 20)                                             # two lookups for every record
        self.assertLess(filtered, 0.25 * plain)                                                      # and with the filter only the few that share a key with someone
        self.assertEqual(snapshot(self.load(self.store(), records)), snapshot(self.load(self.store(), records, KeyFilter(len(records) * 20))))

    def test_with_duplicates_only_the_records_that_have_a_maybe_are_looked_up(self):
        records = people(400, seed=3)
        kf = KeyFilter(len(records) * 20)
        filtered = self.lookups_during(lambda: self.load(self.store(), records, kf))
        plain = self.lookups_during(lambda: self.load(self.store(), records))
        self.assertLess(filtered, plain)
        self.assertGreater(kf.asked - kf.maybe, 0.5 * kf.asked)                                      # most keys were known to be held by nobody else


# ================================================================================================ building it, and the loader's flag
class TestBuildingIt(Base):
    def test_an_empty_index_gives_an_empty_filter_sized_for_the_load(self):
        kf, message = build_for_load(self.store(), TENANT, DOMAIN, 1000)
        self.assertIn("0 keys already in the index loaded", message)
        self.assertGreaterEqual(kf.bits, 1000 * 20 * 10)

    def test_too_many_keys_already_there_means_no_filter_and_a_reason(self):
        store = self.load(self.store(), people(100))
        kf, message = build_for_load(store, TENANT, DOMAIN, 100, max_existing_keys=50)
        self.assertIsNone(kf)
        self.assertIn("not used", message)
        self.assertIn("--key-filter-max-keys", message)

    def test_the_scan_reads_every_page_not_just_the_first(self):
        store = self.load(self.store(), people(120))
        kf = KeyFilter(10_000)
        total = kf.load_existing(store, TENANT, DOMAIN, 10_000_000, page=7)                         # many small pages
        self.assertEqual(total, store.count_blocking_keys(TENANT, DOMAIN) and len({k for p in store.iter_blocking_keys(TENANT, DOMAIN, 1000) for k in p}))
        for page in store.iter_blocking_keys(TENANT, DOMAIN, 1000):
            self.assertEqual(kf.check_and_add(set(page)), set(page))                                 # every key in the index is "maybe held": none was missed
        with self.assertRaises(KeyFilterTooBig):
            KeyFilter(10).load_existing(store, TENANT, DOMAIN, 5, page=7)


class TestTheLoaderFlag(Base):
    def setUp(self):
        super().setUp()
        self.csv = os.path.join(self.tmp, "people.csv")
        with open(self.csv, "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["PersonID"] + FIELDS)
            for _ss, sid, attrs in people(250, seed=13):
                w.writerow([sid] + [attrs.get(k, "") for k in FIELDS])
        self.env = {**os.environ, "MDM_TOKEN_KEY": TOKEN_KEY, "MDM_AUDIT_RATE": "0", "MDM_DOMAINS_DIR": os.path.join(ROOT, "domain_templates"), "MDM_ENGINE": "kernel"}

    def load(self, db, *extra, env=None):
        mapping = ",".join(f"{k}={k}" for k in FIELDS)
        return subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", "people", "--domain", "person", "--file", self.csv, "--mapping", mapping, "--id-column",
                               "PersonID", "--source-system", "crm", "--bulk", "--db", db, *extra], capture_output=True, text=True, env=env or self.env, timeout=120)

    def shape(self, db):
        c = sqlite3.connect(db)
        groups = collections.defaultdict(set)
        for ent, sid in c.execute("SELECT entity_id, source_record_id FROM source_records"):
            groups[ent].add(sid)
        return len(groups), collections.Counter(c.execute("SELECT status, match_type, ROUND(score, 6) FROM match_candidates").fetchall())      # counts: separate processes break ties by random ids

    def test_the_flag_gives_the_same_result_says_what_it_promises_and_reports_what_it_saved(self):
        off, on = os.path.join(self.tmp, "off.db"), os.path.join(self.tmp, "on.db")
        a, b = self.load(off), self.load(on, "--key-filter")
        self.assertEqual((a.returncode, b.returncode), (0, 0), a.stderr + b.stderr)
        self.assertEqual(self.shape(off), self.shape(on))
        self.assertIn("key filter ON", b.stdout)
        self.assertIn("YOU ARE PROMISING that nothing else writes tenant 'people' domain 'person'", b.stdout)
        self.assertIn("known to be held by no other record and not looked up", b.stdout)
        self.assertNotIn("key filter", a.stdout)                                                     # off by default

    def test_it_is_not_used_when_too_much_is_already_indexed_and_the_load_still_succeeds(self):
        db = os.path.join(self.tmp, "x.db")
        self.assertEqual(self.load(db).returncode, 0)
        again = self.load(db, "--key-filter", "--key-filter-max-keys", "10")
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn("the key filter was not used", again.stdout)
        self.assertNotIn("PROMISING", again.stdout)

    def test_the_set_based_engine_refuses_it_like_its_other_kernel_only_options_and_a_plain_old_engine_load_ignores_it_out_loud(self):
        legacy = {**self.env, "MDM_ENGINE": "legacy"}
        refused = self.load(os.path.join(self.tmp, "y.db"), "--key-filter", env=legacy)                 # --bulk under the old engine is the set-based engine
        self.assertEqual(refused.returncode, 2)
        self.assertIn("--key-filter belongs to the kernel", refused.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "y.db")) and sqlite3.connect(os.path.join(self.tmp, "y.db")).execute("SELECT COUNT(*) FROM source_records").fetchone()[0])   # and it loaded nothing
        mapping = ",".join(f"{k}={k}" for k in FIELDS)
        plain = subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", "people", "--domain", "person", "--file", self.csv, "--mapping", mapping, "--id-column",
                                "PersonID", "--source-system", "crm", "--key-filter", "--db", os.path.join(self.tmp, "z.db")], capture_output=True, text=True, env=legacy, timeout=120)
        self.assertEqual(plain.returncode, 0, plain.stderr)                                              # a normal (not --bulk) load goes through the pipeline
        self.assertIn("--key-filter belongs to the kernel and MDM_ENGINE=legacy is set: it is ignored", plain.stderr)


if __name__ == "__main__":
    unittest.main()
