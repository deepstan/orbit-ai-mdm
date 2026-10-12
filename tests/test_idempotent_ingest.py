"""
Idempotent raw ingest: the same source record sent again is a no-op, or a new VERSION updated in place; never a
duplicate row. The superseded payload is kept in source_record_history.

Run with:  python3 -m unittest tests.test_idempotent_ingest -v
"""
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.compliance import purge_entity
from app.db import Store
from app.migrations import SQLiteMigrationBackend, run_migrations
from app.migrations.versions import MIGRATIONS
from app.pipeline import content_hash, ingest_batch, ingest_record

ROOT = os.path.join(os.path.dirname(__file__), "..")
ANN = {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "postal_code": "94107"}


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
            rows = c.execute(query, params).fetchall()
            c.commit()
            return rows
        finally:
            c.close()

    def ingest(self, rid, attrs, system="crm", tenant="acme", domain="customer"):
        return ingest_record(self.store, tenant, domain, system, rid, attrs)

    def count(self, table):
        return self.sql(f"SELECT COUNT(*) FROM {table}")[0][0]


class TestSameRecordAgain(Base):
    def test_resending_an_identical_record_changes_nothing(self):
        first = self.ingest("C1", ANN)
        snapshot = {t: self.count(t) for t in ("source_records", "standardized_records", "blocking_keys", "entity_customer", "lineage", "match_candidates")}
        again = self.ingest("C1", ANN)
        self.assertTrue(again["unchanged"])
        self.assertEqual((again["record_id"], again["matched_entity_id"]), (first["record_id"], first["matched_entity_id"]))
        self.assertEqual({t: self.count(t) for t in snapshot}, snapshot)

    def test_key_order_is_not_a_change(self):
        self.ingest("C1", ANN)
        self.assertTrue(self.ingest("C1", dict(reversed(list(ANN.items()))))["unchanged"])

    def test_it_is_keyed_by_tenant_domain_and_source_system_too(self):
        for kwargs in ({}, {"tenant": "other"}, {"system": "erp"}):
            self.ingest("C1", ANN, **kwargs)
        self.ingest("C1", {"company_name": "Acme", "tax_id": "12-3456789"}, domain="vendor")
        self.assertEqual(self.count("source_records"), 4)  # four different records that happen to share an id

    def test_rows_from_before_this_feature_have_no_hash_and_are_still_recognized(self):
        self.ingest("C1", ANN)
        self.sql("UPDATE source_records SET content_hash = NULL")
        self.assertTrue(self.ingest("C1", ANN)["unchanged"])

    def test_the_hash_ignores_nothing_that_matters(self):
        self.assertEqual(content_hash({"a": 1, "b": 2}), content_hash({"b": 2, "a": 1}))
        self.assertNotEqual(content_hash({"a": 1}), content_hash({"a": 2}))
        self.assertNotEqual(content_hash({"a": "x"}), content_hash({"a": "X"}))  # raw is raw: case is a change


class TestChangedRecord(Base):
    def test_a_change_updates_in_place_and_keeps_the_ids(self):
        first = self.ingest("C1", ANN)
        changed = self.ingest("C1", {**ANN, "city": "Reno"})
        self.assertEqual((changed["updated"], changed["version"], changed["record_id"], changed["matched_entity_id"]),
                         (True, 2, first["record_id"], first["matched_entity_id"]))
        self.assertEqual(self.count("source_records"), 1)
        self.assertEqual(self.sql("SELECT version, updated_at IS NOT NULL FROM source_records")[0], (2, 1))
        rec = self.store.get_source_record(first["record_id"], "acme", "customer")
        self.assertEqual(rec.attributes["city"], "Reno")

    def test_the_superseded_payload_is_kept_in_history(self):
        first = self.ingest("C1", ANN)
        self.ingest("C1", {**ANN, "city": "Reno"})
        self.ingest("C1", {**ANN, "city": "Reno", "phone": "512-555-0123"})
        history = self.store.list_record_history(first["record_id"], "acme", "customer")
        self.assertEqual([h["version"] for h in history], [1, 2])
        self.assertEqual(history[0]["attributes"], ANN)
        self.assertEqual(history[1]["attributes"], {**ANN, "city": "Reno"})
        self.assertEqual(self.sql("SELECT version FROM source_records")[0][0], 3)

    def test_everything_derived_from_it_is_refreshed(self):
        r = self.ingest("C1", ANN)
        self.ingest("C1", {**ANN, "email": "ann.new@x.com", "city": "reno"})
        self.assertEqual(self.sql("SELECT cleansed FROM standardized_records")[0][0].count("Reno"), 1)       # staging
        self.assertEqual(self.sql("SELECT city, email FROM entity_customer")[0], ("Reno", "ann.new@x.com"))  # golden
        keys = {k for (k,) in self.sql("SELECT blocking_key FROM blocking_keys WHERE record_id = ?", (r["record_id"],))}
        self.assertIn("g2:ann.new@x.com", keys)
        self.assertNotIn("g2:ann@x.com", keys)                                                                # the old key is gone

    def test_golden_recency_follows_the_update_time_not_the_first_arrival(self):
        self.ingest("A", {"first_name": "Zed", "last_name": "Quill", "phone": "212-555-0000", "city": "Austin"}, system="crm")
        self.ingest("B", {"first_name": "Zed", "last_name": "Quill", "phone": "212-555-0000", "city": "Dallas"}, system="erp")
        self.assertEqual(self.sql("SELECT city FROM entity_customer WHERE status = 'active'")[0][0], "Dallas")
        self.ingest("A", {"first_name": "Zed", "last_name": "Quill", "phone": "212-555-0000", "city": "Reno"}, system="crm")  # A is OLDER but just changed
        self.assertEqual(self.sql("SELECT city FROM entity_customer WHERE status = 'active'")[0][0], "Reno")

    def test_a_correction_can_find_the_match_it_could_not_before(self):
        a = self.ingest("A", {"first_name": "Cara", "last_name": "Diaz", "email": "c@x.com", "postal_code": "78701"})
        b = self.ingest("B", {"first_name": "Cara", "last_name": "Diaz", "email": "c@xx.com", "postal_code": "78701"})  # typo'd email
        self.assertEqual(len(b["queued_for_review"]), 1)
        self.assertNotEqual(a["matched_entity_id"], b["matched_entity_id"])
        fixed = self.ingest("B", {"first_name": "Cara", "last_name": "Diaz", "email": "c@x.com", "postal_code": "78701"})
        self.assertEqual([m["record_id"] for m in fixed["auto_merged_with"]], [a["record_id"]])
        self.assertEqual(fixed["matched_entity_id"], a["matched_entity_id"])
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates WHERE status = 'pending'")[0][0], 0)

    def test_review_items_scored_on_the_old_data_are_superseded(self):
        home = {"first_name": "Jon", "address_line1": "12 Oak St", "city": "Austin", "postal_code": "73301"}
        self.ingest("A", {**home, "last_name": "Park Sr"})
        b = self.ingest("B", {**home, "last_name": "Park Jr"})
        self.assertEqual(len(b["queued_for_review"]), 1)
        self.ingest("B", {"first_name": "Jon", "last_name": "Park Jr", "address_line1": "9 Elm St", "city": "Dallas", "postal_code": "75201"})
        statuses = [r[0] for r in self.sql("SELECT status FROM match_candidates ORDER BY created_at")]
        self.assertEqual(statuses[0], "superseded")
        self.assertNotIn("pending", statuses[:1])

    def test_a_change_does_not_make_a_record_a_candidate_for_its_own_entity(self):
        self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "phone": "512-555-0123"})
        self.ingest("B", {"first_name": "Ann", "last_name": "Lee", "phone": "512-555-0123"}, system="erp")
        before = self.count("match_candidates")
        r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "phone": "512-555-0123", "city": "Reno"})
        self.assertEqual((r["auto_merged_with"], r["queued_for_review"]), ([], []))
        self.assertEqual(self.count("match_candidates"), before)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 1)

    def test_purge_removes_the_history_too(self):
        r = self.ingest("C1", ANN)
        self.ingest("C1", {**ANN, "city": "Reno"})
        self.assertEqual(self.count("source_record_history"), 1)
        purge_entity(self.store, "acme", "customer", r["matched_entity_id"], requested_by="dpo")
        self.assertEqual((self.count("source_record_history"), self.count("source_records")), (0, 0))


class TestBatch(Base):
    def run_batch(self, items):
        return ingest_batch(self.store, "acme", "customer", items)

    def test_resending_a_batch_is_a_no_op(self):
        items = [("crm", f"C{i}", {"first_name": f"P{i}", "last_name": "Lee", "email": f"p{i}@x.com"}) for i in range(5)]
        self.run_batch(items)
        again = self.run_batch(items)
        self.assertTrue(all(r.get("unchanged") for r in again))
        self.assertEqual(self.count("source_records"), 5)

    def test_a_key_repeated_inside_one_batch_becomes_a_new_version(self):
        r = self.run_batch([("crm", "C3", {"first_name": "Ann", "last_name": "Lee", "city": "Austin"}),
                            ("crm", "C3", {"first_name": "Ann", "last_name": "Lee", "city": "Reno"})])
        self.assertTrue(r[1]["updated"])
        self.assertEqual(self.sql("SELECT version FROM source_records")[0][0], 2)
        self.assertEqual(self.count("source_record_history"), 1)
        self.assertEqual(self.sql("SELECT city FROM entity_customer")[0][0], "Reno")

    def test_a_batch_may_mix_new_unchanged_and_changed(self):
        self.run_batch([("crm", "A", ANN), ("crm", "B", {**ANN, "email": "b@x.com", "first_name": "Bea"})])
        r = self.run_batch([("crm", "A", ANN), ("crm", "B", {**ANN, "email": "b@x.com", "first_name": "Bea", "city": "Reno"}),
                            ("crm", "C", {"first_name": "Cy", "last_name": "Wu", "email": "c@y.com"})])
        unchanged, changed, new = r
        self.assertEqual([bool(unchanged.get("unchanged")), bool(changed.get("updated")), "unchanged" not in new and "updated" not in new],
                         [True, True, True])
        self.assertEqual(self.count("source_records"), 3)

    def test_one_at_a_time_and_batch_agree(self):
        a, b = Store(os.path.join(self.tmp, "a.db")), Store(os.path.join(self.tmp, "b.db"))
        seq = [("crm", "A", ANN), ("crm", "A", ANN), ("crm", "A", {**ANN, "city": "Reno"}), ("erp", "B", ANN)]
        for ss, rid, attrs in seq:
            ingest_record(a, "acme", "customer", ss, rid, attrs)
        ingest_batch(b, "acme", "customer", seq)
        shape = lambda st: sorted((r.source_system, r.source_record_id, r.version) for r in st.list_source_records("acme", "customer"))
        self.assertEqual(shape(a), shape(b))


class TestExistingDatabases(Base):
    def test_old_duplicates_are_reported_and_the_newest_copy_is_current(self):
        r = self.ingest("C1", ANN)
        self.sql("INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, ingested_at, entity_id, version) "
                 "SELECT 'rec_dupe', tenant_id, domain, source_system, source_record_id, attributes, '2999-01-01T00:00:00+00:00', NULL, 1 FROM source_records")
        self.assertEqual(self.store.count_duplicate_raw_keys("customer"), 1)
        self.assertEqual(self.store.find_source_record("acme", "customer", "crm", "C1").id, "rec_dupe")
        self.assertNotEqual(r["record_id"], "rec_dupe")

    def test_a_database_from_before_migration_010_is_upgraded_in_place(self):
        path = os.path.join(self.tmp, "old.db")
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:9])
        c = sqlite3.connect(path)
        self.assertNotIn("content_hash", [r[1] for r in c.execute("PRAGMA table_info(source_records)")])
        c.execute("INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, ingested_at) "
                  "VALUES ('rec_old','acme','customer','crm','C1','{\"first_name\": \"Ann\", \"last_name\": \"Lee\", \"email\": \"ann@x.com\", \"postal_code\": \"94107\"}','2026-01-01T00:00:00+00:00')")
        c.commit()
        c.close()
        store = Store(path)
        cols = {r[1] for r in sqlite3.connect(path).execute("PRAGMA table_info(source_records)")}
        self.assertTrue({"content_hash", "version", "updated_at"} <= cols)
        self.assertEqual(store.find_source_record("acme", "customer", "crm", "C1").version, 1)
        self.assertEqual(sqlite3.connect(path).execute("SELECT \"unique\" FROM pragma_index_list('source_records') WHERE name = 'idx_source_records_natural'").fetchone(), (0,))  # deliberately not unique


class TestBulkIngestCli(Base):
    def test_running_the_same_file_twice_reports_unchanged(self):
        csv_path = os.path.join(self.tmp, "in.csv")
        with open(csv_path, "w") as f:
            f.write("id,first,last,email\n1,Ann,Lee,a@x.com\n2,Bob,Chen,b@y.com\n3,Cy,Wu,c@z.com\n")
        cmd = [sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", "acme", "--domain", "customer", "--file", csv_path,
               "--db", self.dbpath, "--id-column", "id", "--mapping", "first_name=first,last_name=last,email=email"]
        first = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
        second = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("Standalone (new):   3", first.stdout)
        self.assertIn("Already held, unchanged: 3", second.stdout)
        self.assertEqual(self.count("source_records"), 3)


if __name__ == "__main__":
    unittest.main()
