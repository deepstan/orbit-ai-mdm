"""
Every record belongs to exactly one entity, and the answer does not depend on arrival order.

Before: a record whose only matches were waiting for review had NO entity (so no golden record), rejecting the review never gave it
one, and WHICH record of a review pair was entity-less depended on which arrived first (28% of benchmark records).

Run with:  python3 -m unittest tests.test_every_record_has_an_entity -v
"""
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark, domain_config, offline
from app.db import Store
from app.domains import get_domain_config
from app.migrations import SQLiteMigrationBackend, run_migrations
from app.migrations.versions import MIGRATIONS
from app.pipeline import ingest_batch, ingest_record, merge_two_records, process_pending, resolve_candidate

ROOT = os.path.join(os.path.dirname(__file__), "..")
HOME = {"first_name": "Jon", "address_line1": "12 Oak St", "city": "Austin", "postal_code": "73301"}


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.dbpath = os.path.join(self.tmp, "t.db")
        self.store = Store(self.dbpath)

    def sql(self, query, params=()):
        c = sqlite3.connect(self.dbpath)
        try:
            return c.execute(query, params).fetchall()
        finally:
            c.close()

    def ingest(self, rid, attrs, system="crm", **kw):
        return ingest_record(self.store, "acme", "customer", system, rid, attrs, **kw)


class TestAReviewPairIsTwoEntities(Base):
    def setUp(self):
        super().setUp()
        self.a = self.ingest("A", {**HOME, "last_name": "Park Sr"})
        self.b = self.ingest("B", {**HOME, "last_name": "Park Jr"})

    def test_a_record_waiting_for_review_still_has_its_own_entity_and_golden_record(self):
        self.assertEqual(len(self.b["queued_for_review"]), 1)
        self.assertIsNotNone(self.b["matched_entity_id"])
        self.assertNotEqual(self.a["matched_entity_id"], self.b["matched_entity_id"])
        rows = self.sql("SELECT entity_id, last_name, status FROM entity_customer ORDER BY created_at")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r[1] and r[2] == "active" for r in rows))        # both have real golden values
        self.assertEqual(self.sql("SELECT COUNT(*) FROM source_records WHERE entity_id IS NULL")[0][0], 0)

    def test_rejecting_the_review_changes_nothing_and_both_keep_their_entities(self):
        resolve_candidate(self.store, "acme", "customer", self.b["queued_for_review"][0]["candidate_id"], "reject", "steward")
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 2)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM source_records WHERE entity_id IS NULL")[0][0], 0)

    def test_confirming_the_review_unions_the_two_and_the_established_entity_keeps_its_id(self):
        resolve_candidate(self.store, "acme", "customer", self.b["queued_for_review"][0]["candidate_id"], "confirm", "steward")
        active = self.sql("SELECT entity_id FROM entity_customer WHERE status = 'active'")
        self.assertEqual(active, [(self.a["matched_entity_id"],)])           # the OLDER entity survives: its id is the one downstream systems hold
        self.assertEqual(self.sql("SELECT status FROM entity_customer WHERE entity_id = ?", (self.b["matched_entity_id"],))[0][0], "merged")
        self.assertEqual(len({r[0] for r in self.sql("SELECT entity_id FROM source_records")}), 1)

    def test_which_entity_survives_does_not_depend_on_which_record_is_named_first(self):
        for first, second in (("A", "B"), ("B", "A")):
            store = Store(os.path.join(self.tmp, f"{first}.db"))
            ra = ingest_record(store, "acme", "customer", "crm", "A", {**HOME, "last_name": "Park Sr"})
            rb = ingest_record(store, "acme", "customer", "crm", "B", {**HOME, "last_name": "Park Jr"})
            ids = {"A": ra["record_id"], "B": rb["record_id"]}
            self.assertEqual(merge_two_records(store, "acme", "customer", ids[first], ids[second], "t"), ra["matched_entity_id"])


class TestNobodyIsLeftWithoutAnEntity(Base):
    def test_in_every_engine_for_both_profiles(self):
        for profile in ("customer", "person"):
            prof = benchmark.get_profile(profile, "full" if profile == "person" else None)
            rows = [r["raw"] for r in prof.generate(2, 2)]
            for name, engine in offline.ENGINES.items():
                _, _, entityless = engine(rows, list(range(len(rows))), prof.domain)
                self.assertEqual(entityless, [], f"{profile}/{name}")

    def test_a_record_that_only_waits_for_review_is_not_given_a_second_entity_when_resumed(self):
        self.ingest("A", {**HOME, "last_name": "Park Sr"})
        real, fired = Store.set_processing_status, {"done": False}

        def flaky(store, ids, tenant, domain, status, error=None):
            if status == "matched" and not fired["done"]:
                fired["done"] = True
                raise RuntimeError("simulated crash")
            return real(store, ids, tenant, domain, status, error)
        with mock.patch.object(Store, "set_processing_status", flaky):
            self.assertRaises(RuntimeError, self.ingest, "B", {**HOME, "last_name": "Park Jr"})
        again = self.ingest("B", {**HOME, "last_name": "Park Jr"})
        self.assertTrue(again["resumed"])
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 2)  # still exactly one each
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates")[0][0], 1)


class TestReferenceMatcher(Base):
    RECORDS = [
        {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com", "postal_code": "94107"},                      # 0
        {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com", "phone": "512-555-0001"},                     # 1  same email as 0
        {"first_name": "Ann", "last_name": "Lee", "email": "other@y.com", "phone": "512-555-0001"},                 # 2  same phone as 1 (transitive)
        {"first_name": "Zed", "last_name": "Quill", "email": "z@q.com"},                                            # 3  alone
        {**HOME, "last_name": "Park Sr"},                                                                           # 4 \\ a review pair
        {**HOME, "last_name": "Park Jr"},                                                                           # 5 /
    ]

    def test_it_finds_transitive_chains_singletons_and_review_pairs(self):
        r = offline.resolve_offline(self.RECORDS, "customer")
        self.assertEqual(r.partition, {frozenset({0, 1, 2}), frozenset({3}), frozenset({4}), frozenset({5})})
        self.assertIn(frozenset({4, 5}), r.review_pairs)
        self.assertEqual(r.capped_keys, 0)

    def test_the_order_of_the_input_does_not_change_the_answer(self):
        base = offline.resolve_offline(self.RECORDS, "customer")
        order = [5, 2, 4, 0, 3, 1]
        shuffled = offline.resolve_offline([self.RECORDS[i] for i in order], "customer")
        back = {frozenset(order[i] for i in g) for g in shuffled.partition}
        self.assertEqual(back, base.partition)

    def test_every_engine_in_every_order_agrees_with_it(self):
        rep = offline.check_equivalence(self.RECORDS, "customer", orders=4)
        self.assertTrue(rep["ok"], offline.format_equivalence(rep))
        self.assertEqual({r["engine"] for r in rep["runs"]}, {"realtime", "batch", "deferred", "bulk", "onboard", "onboard_onto_realtime"})

    def test_a_key_over_the_cap_makes_the_comparison_say_so_instead_of_giving_a_wrong_verdict(self):
        cfg = get_domain_config("customer")
        saved, cfg.max_bucket_size = cfg.max_bucket_size, 2
        self.addCleanup(setattr, cfg, "max_bucket_size", saved)
        crowd = [{"first_name": f"N{i}", "last_name": "Smith", "postal_code": "94107"} for i in range(6)]
        rep = offline.check_equivalence(crowd, "customer", orders=1)
        self.assertGreater(rep["reference"]["capped_keys"], 0)
        self.assertIsNone(rep["ok"])
        self.assertIn("NOT COMPARABLE", offline.format_equivalence(rep))

    def test_a_deliberately_wrong_engine_fails_the_check(self):
        """The check must be able to fail, or a green result means nothing."""
        def forgetful(records, order, domain):  # never merges anything
            return {frozenset({i}) for i in range(len(records))}, set(), []
        rep = offline.check_equivalence(self.RECORDS, "customer", orders=1, engines={"forgetful": forgetful})
        self.assertFalse(rep["ok"])
        self.assertFalse(rep["runs"][0]["same_entities"])

    def test_the_benchmark_data_passes_for_both_profiles(self):
        for profile in ("customer", "person"):
            prof = benchmark.get_profile(profile, "full" if profile == "person" else None)
            rep = offline.check_equivalence([r["raw"] for r in prof.generate(2, 2)], prof.domain, orders=3)
            self.assertTrue(rep["ok"], f"{profile}\n" + offline.format_equivalence(rep))
            self.assertGreater(rep["reference"]["review_pairs"], 0)  # the data really exercises review pairs, so the check has teeth


class TestUpgrade013(Base):
    def test_entity_less_records_become_unfinished_and_are_given_an_entity_without_touching_their_reviews(self):
        path = os.path.join(self.tmp, "old.db")
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:12])
        c = sqlite3.connect(path)
        ts = "2026-01-01T00:00:00+00:00"
        people = {"r_pending": ("Ann", "Lee", "ann@x.com"), "r_rejected": ("Cy", "Wu", "cy@z.com"), "r_other": ("Zed", "Quill", "zed@q.com")}
        for rid, (f, l, e) in people.items():
            c.execute("INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, ingested_at, entity_id) "
                      "VALUES (?, 'acme', 'customer', 'crm', ?, ?, ?, NULL)", (rid, rid, '{"first_name": "%s", "last_name": "%s", "email": "%s"}' % (f, l, e), ts))
        for cid, a, b, status in (("c_pending", "r_pending", "r_other", "pending"), ("c_rejected", "r_rejected", "r_other", "rejected")):
            c.execute("INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at) "
                      "VALUES (?, 'acme', 'customer', ?, ?, 0.7, 'fuzzy', '[]', ?, ?)", (cid, a, b, status, ts))
        c.execute("UPDATE source_records SET processing_status = 'matched'")
        c.commit()
        c.close()
        store = Store(path)  # upgrades to the latest
        self.assertEqual(store.count_unfinished("customer"), (3, 0))
        out = process_pending(store, "customer")
        self.assertEqual((out["finished"], out["remaining"]), (3, 0))
        recs = store.list_source_records("acme", "customer")
        self.assertTrue(all(r.entity_id for r in recs))
        self.assertEqual(len({r.entity_id for r in recs}), 3)  # three different people, three entities (a rejected review gave r_rejected one too)
        self.assertEqual({c_.id: c_.status for c_ in store.list_candidates("acme", "customer", "pending") + store.list_candidates("acme", "customer", "rejected")},
                         {"c_pending": "pending", "c_rejected": "rejected"})
        self.assertEqual(run_migrations(SQLiteMigrationBackend(path)), [])


class TestLineageIsWrittenOncePerRebuild(Base):
    def test_one_store_call_per_rebuild_and_the_same_events_as_before(self):
        with mock.patch.object(Store, "add_lineage_batch", autospec=True, side_effect=Store.add_lineage_batch) as batch, \
             mock.patch.object(Store, "add_lineage", autospec=True) as single:
            r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com", "city": "Reno", "postal_code": "89501"})
            self.assertEqual(batch.call_count, 1)   # one transaction for the whole golden record...
            self.assertEqual(single.call_count, 0)  # ...not one commit per field
            events = batch.call_args[0][1]
        self.assertEqual({e.field for e in events}, {"first_name", "last_name", "email", "city", "postal_code", "country"})
        stored = self.store.list_lineage(r["matched_entity_id"], "acme", "customer")
        self.assertEqual(sorted(e.field for e in stored), sorted(e.field for e in events))
        self.assertTrue(all(e.reason and e.entity_id == r["matched_entity_id"] for e in stored))
        self.ingest("B", {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com", "city": "Sparks"}, "erp")   # a merge that changes one value
        changed = [e for e in self.store.list_lineage(r["matched_entity_id"], "acme", "customer") if e.field == "city"]
        self.assertEqual([(e.old_value, e.new_value) for e in changed], [(None, "Reno"), ("Reno", "Sparks")])
        self.assertEqual(self.store.add_lineage_batch([]), None)  # an empty rebuild writes nothing and does not fail


class TestProcessPendingSaysWhenThereIsMore(Base):
    def test_a_limited_pass_reports_what_remains(self):
        for i in range(5):
            self.ingest(f"D{i}", {"first_name": f"P{i}", "last_name": "Wu", "email": f"p{i}@y.com"}, mode="batch")
        out = process_pending(self.store, "customer", limit=2)
        self.assertEqual((out["finished"], out["remaining"]), (2, 3))
        r = subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "process-pending", "customer", "--db", self.dbpath, "--limit", "2"],
                           capture_output=True, text=True, cwd=ROOT)
        self.assertIn("1 more record(s) are still unfinished", r.stdout)


class TestCli(Base):
    def test_equivalence_command_passes(self):
        r = subprocess.run([sys.executable, os.path.join(ROOT, "benchmark.py"), "equivalence", "--profile", "customer", "--scale", "1", "--orders", "2"],
                           capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("PASS", r.stdout)


if __name__ == "__main__":
    unittest.main()
