"""
Onboarding (app/bulk.py, bulk_load(..., onboard=True)): a NEW source system added to data that already exists. The oracle is the regular
engine: the same existing data, the new records ingested one at a time. Nothing here measures time.

Run with:  python3 -m unittest tests.test_onboarding -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_bulk_engine import ROOT, TENANT, Base  # noqa: E402
from app.bulk import BulkLoadError, bulk_load  # noqa: E402
from app.db import Store  # noqa: E402
from app.domains import get_domain_config  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate  # noqa: E402

ANN_A = {"first_name": "Ann", "last_name": "Lee", "email": "ann.lee@example.com"}
ANN_B = {"first_name": "Ann", "last_name": "Lee", "phone": "512-555-0142"}
ANN_BRIDGE = {"first_name": "Ann", "last_name": "Lee", "email": "ann.lee@example.com", "phone": "512-555-0142", "city": "Austin"}


class Onb(Base):
    def existing_data(self, rows, cut, base="bulk"):
        store, path = self.store()
        if base == "bulk":
            bulk_load(store, TENANT, self.domain, [("src", f"r{i}", rows[i]) for i in range(cut)])
        else:
            for i in range(cut):
                ingest_record(store, TENANT, self.domain, "src", f"r{i}", rows[i])
        return store, path

    def clone(self, path):
        self.n += 1
        copy = f"{path}.copy{self.n}"
        shutil.copy(path, copy)
        return Store(copy), copy

    def both(self, rows, share=0.6, base="bulk", prepare=None, **kw):
        """The same existing data, twice: the oracle takes the new records one at a time, the other has them onboarded."""
        cut = int(len(rows) * share)
        store, path = self.existing_data(rows, cut, base)
        if prepare:
            prepare(store)
        self.base_entities = {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records")}
        oracle, oracle_path = self.clone(path)
        for i in range(cut, len(rows)):
            ingest_record(oracle, TENANT, self.domain, "new", f"r{i}", rows[i])
        result = bulk_load(store, TENANT, self.domain, [("new", f"r{i}", rows[i]) for i in range(cut, len(rows))], onboard=True, **kw)
        return store, path, oracle_path, result, cut

    def same(self, got, want, cut):
        self.assertEqual(self.golden(got), self.golden(want))          # every golden value and quality issue, entity by entity
        self.assertEqual(self.candidates(got), self.candidates(want))  # the same pairs, scores and statuses (steward decisions included)
        self.assertEqual(self.staging(got), self.staging(want))
        self.assertEqual(self.index(got), self.index(want))
        existing = lambda p: {r["source_record_id"]: r["entity_id"] for r in self.q(
            p, "SELECT source_record_id, entity_id FROM source_records WHERE CAST(SUBSTR(source_record_id, 2) AS INTEGER) < ?", (cut,))}
        self.assertEqual(existing(got), existing(want))                # every EXISTING record is still in the same entity id: the oldest survived
        # an existing entity merged away is a tombstone in both. (Only those are comparable: the regular engine also leaves tombstones for the
        # transient entities a new record has before it is merged, which an onboarding never creates.)
        tombstones = lambda p: {r["entity_id"] for r in self.q(p, f"SELECT entity_id FROM entity_{self.domain} WHERE status = 'merged'")} & self.base_entities
        self.assertEqual(tombstones(got), tombstones(want))

    def snapshot(self, path):
        out = {}
        for table, cols in (("source_records", "id, entity_id, content_hash, processing_status, version, attributes, updated_at"), ("source_record_history", "record_id, version, content_hash"), (f"entity_{self.domain}", "*"), ("match_candidates", "id, status, score"),
                            ("lineage", "id"), ("blocking_keys", "record_id, blocking_key"), ("standardized_records", "record_id")):
            out[table] = sorted(json.dumps(r, sort_keys=True, default=str) for r in self.q(path, f"SELECT {cols} FROM {table}"))
        return out


class TestMatchesTheRegularEngine(Onb):
    def test_customers_onto_a_bulk_loaded_base(self):
        store, path, oracle, result, cut = self.both(self.rows())
        self.same(path, oracle, cut)
        self.assertGreater(result.existing_entities_touched, 0)       # the new records really did reach existing entities
        self.assertGreater(result.auto_pairs, 0)
        self.assertGreater(result.review_pairs, 0)

    def test_customers_onto_a_base_built_record_by_record(self):
        store, path, oracle, result, cut = self.both(self.rows(seed=4), base="realtime", share=0.4)
        self.same(path, oracle, cut)

    def test_people_with_a_protected_ssn_and_conditional_keys(self):
        store, path, oracle, result, cut = self.both(self.rows("person", scale=1))
        self.same(path, oracle, cut)

    def test_workers_and_chunk_size_change_nothing(self):
        rows = self.rows()
        cut = int(len(rows) * 0.6)
        base, base_path = self.existing_data(rows, cut)
        self.base_entities = {r["entity_id"] for r in self.q(base_path, "SELECT entity_id FROM source_records")}
        new = [("new", f"r{i}", rows[i]) for i in range(cut, len(rows))]
        ones, ones_path = self.clone(base_path)
        many, many_path = self.clone(base_path)
        oracle, oracle_path = self.clone(base_path)
        for _, name, raw in new:
            ingest_record(oracle, TENANT, self.domain, "new", name, raw)
        bulk_load(ones, TENANT, self.domain, new, onboard=True)
        bulk_load(many, TENANT, self.domain, new, onboard=True, workers=2, chunk=7)
        self.same(ones_path, oracle_path, cut)
        self.same(many_path, oracle_path, cut)

    def test_steward_decisions_in_the_existing_data_stand(self):
        decided = {}

        def prepare(store):  # a steward confirms one review pair and rejects another BEFORE the new source arrives
            pending = [r["id"] for r in self.q(store.path, "SELECT id FROM match_candidates WHERE status = 'pending' ORDER BY id")]
            self.assertGreaterEqual(len(pending), 2)
            resolve_candidate(store, TENANT, self.domain, pending[0], "confirm", "steward")
            resolve_candidate(store, TENANT, self.domain, pending[1], "reject", "steward")
            decided.update(confirmed=pending[0], rejected=pending[1])
        store, path, oracle, result, cut = self.both(self.rows(), prepare=prepare)
        self.same(path, oracle, cut)
        statuses = {r["id"]: r["status"] for r in self.q(path, "SELECT id, status FROM match_candidates")}
        self.assertEqual(statuses[decided["confirmed"]], "confirmed")  # onboarding did not reopen, or rescore, what was decided
        self.assertEqual(statuses[decided["rejected"]], "rejected")

    def test_the_cap_counts_the_existing_records_that_hold_a_key(self):
        """A key held by 3 existing records and 2 new ones is a bucket of 5, usable only when the cap allows 5, as in the reference matcher:
        a bucket is counted over ALL the records. The new records share nothing else with anyone, so the only way they can be compared is through
        that key (the two new records still meet through a name key, which is not the point). (A base that merged while the key was still uncommon is not undone when the key later grows: there is no unmerge.)"""
        existing = [{"first_name": "Sam", "last_name": f"Hill{i}", "email": "shared@example.com"} for i in range(3)]
        new = [{"first_name": "Pat", "last_name": f"Quinn{i}", "email": "shared@example.com"} for i in range(2)]
        self.domain = "customer"
        store, path = self.existing_data(existing, 3)
        config, results = get_domain_config("customer"), {}
        for cap in (3, 4):
            with mock.patch.object(config, "max_bucket_size", cap):
                results[cap] = bulk_load(store, TENANT, "customer", [("new", f"n{i}", raw) for i, raw in enumerate(new)], onboard=True, write=False,
                                         workdir=os.path.join(self.tmp, f"cap{cap}"))
        with_existing = lambda cap: self.q(os.path.join(self.tmp, f"cap{cap}", "pairs.db"), "SELECT COUNT(*) AS n FROM pairs WHERE a < 0")[0]["n"]   # existing records are numbered below 0
        self.assertEqual((results[3].capped_keys, with_existing(3)), (1, 0))   # 3 existing + 2 new = 5 > cap + 1: too common to compare on, so no existing record is compared
        self.assertEqual((results[4].capped_keys, with_existing(4)), (0, 2 * 3))   # a cap of 4 allows 5: each new record is compared with each existing one


class TestExistingEntitiesMerge(Onb):
    def setup_ann(self):
        store, path = self.store()
        ingest_record(store, TENANT, "customer", "src", "a", ANN_A)
        ingest_record(store, TENANT, "customer", "src", "b", ANN_B)
        self.domain = "customer"
        self.assertEqual(len(self.golden(path)), 2, "the two existing Anns must start as two entities for this test to mean anything")
        return store, path

    def test_a_new_record_that_links_two_existing_entities_merges_them_and_the_oldest_survives(self):
        store, path = self.setup_ann()
        older = self.q(path, "SELECT entity_id FROM source_records WHERE source_record_id = 'a'")[0]["entity_id"]
        younger = self.q(path, "SELECT entity_id FROM source_records WHERE source_record_id = 'b'")[0]["entity_id"]
        born = self.q(path, "SELECT created_at FROM entity_customer WHERE entity_id = ?", (older,))[0]["created_at"]
        oracle, oracle_path = self.clone(path)
        ingest_record(oracle, TENANT, "customer", "new", "n", ANN_BRIDGE)
        result = bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True)
        self.assertEqual(self.q(path, "SELECT created_at FROM entity_customer WHERE entity_id = ?", (older,))[0]["created_at"], born)   # it keeps its age: later merges depend on it
        self.assertEqual((result.existing_entities_touched, result.existing_entities_merged, result.entities), (2, 1, 0))
        entities = {r["source_record_id"]: r["entity_id"] for r in self.q(path, "SELECT source_record_id, entity_id FROM source_records")}
        self.assertEqual(set(entities.values()), {older})                                 # all three records are in the OLDER entity
        status = {r["entity_id"]: r["status"] for r in self.q(path, "SELECT entity_id, status FROM entity_customer")}
        self.assertEqual((status[older], status[younger]), ("active", "merged"))        # and the younger is a tombstone, as the regular engine leaves it
        golden = list(self.golden(path).values())[0]
        self.assertEqual((golden["email"], golden["phone"], golden["city"]), ("ann.lee@example.com", "(512) 555-0142", "Austin"))
        self.assertEqual(self.golden(path), self.golden(oracle_path))
        self.assertEqual(self.candidates(path), self.candidates(oracle_path))
        events = self.q(path, "SELECT reason, field FROM lineage WHERE entity_id = ?", (older,))
        self.assertTrue(any("onboarding load" in e["reason"] for e in events))                  # the rebuild is explained in lineage

    def test_the_new_source_is_the_most_recent_so_its_value_wins_a_conflict(self):
        existing = {**ANN_A, "city": "Reno", "state": "NV"}
        new = {**ANN_A, "city": "Austin", "state": "TX"}
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, TENANT, "customer", "src", "a", existing)
        oracle, oracle_path = self.clone(path)
        ingest_record(oracle, TENANT, "customer", "new", "n", new)
        bulk_load(store, TENANT, "customer", [("new", "n", new)], onboard=True)
        golden = list(self.golden(path).values())
        self.assertEqual(len(golden), 1)
        self.assertEqual((golden[0]["city"], golden[0]["state"]), ("Austin", "TX"))       # the record that arrived last wins, as in the regular engine
        self.assertEqual(self.golden(path), self.golden(oracle_path))

    def test_a_dry_run_reports_it_and_writes_nothing(self):
        store, path = self.setup_ann()
        before = self.snapshot(path)
        dry = bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True, write=False, workdir=os.path.join(self.tmp, "dry"))
        self.assertTrue(dry.dry_run)
        self.assertEqual((dry.existing_records, dry.existing_entities_touched, dry.existing_entities_merged, dry.entities), (2, 2, 1, 0))
        self.assertEqual(self.snapshot(path), before)

    def test_a_failure_while_changing_existing_entities_leaves_everything_exactly_as_it_was(self):
        store, path = self.setup_ann()
        before = self.snapshot(path)
        real = Store.apply_onboarding

        def fail_at_the_very_end(self_, tenant_id, domain, reassign, entities, lineage, **rest):
            # everything but the last statement runs, then a duplicate lineage id breaks the transaction: records were already reassigned
            # and golden records already rewritten inside it
            self.assertTrue(reassign and entities and lineage)
            return real(self_, tenant_id, domain, reassign, entities, [*lineage, lineage[0]], **rest)
        with mock.patch.object(Store, "apply_onboarding", fail_at_the_very_end), self.assertRaises(sqlite3.IntegrityError):
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True)
        self.assertEqual(self.snapshot(path), before)                                    # the new rows are gone and the existing entities untouched
        oracle, oracle_path = self.clone(path)
        ingest_record(oracle, TENANT, "customer", "new", "n", ANN_BRIDGE)
        bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True)   # and simply running it again works
        self.assertEqual(self.golden(path), self.golden(oracle_path))

    def test_a_failure_while_writing_the_new_rows_also_leaves_everything_as_it_was(self):
        store, path = self.setup_ann()
        before = self.snapshot(path)
        with mock.patch.object(Store, "save_candidates_batch", side_effect=RuntimeError("disk full")), self.assertRaises(RuntimeError):
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True)
        self.assertEqual(self.snapshot(path), before)

    def test_existing_data_that_changes_between_planning_and_writing_is_detected(self):
        store, path = self.setup_ann()
        work = os.path.join(self.tmp, "plan")
        bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True, write=False, workdir=work)
        ingest_record(store, TENANT, "customer", "src", "c", {"first_name": "Bo", "last_name": "Yu", "email": "bo@example.com"})   # someone else wrote meanwhile
        before = self.snapshot(path)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True, resume=True, workdir=work)
        self.assertIn("changed", str(cm.exception))
        self.assertEqual(self.snapshot(path), before)


class TestManyBridges(Onb):
    """The generated datasets never make a new record link two existing entities, so this one is built to: many people whose existing records
    are separate (one knows only an email, another only a phone) and whose new record knows both. Compared with the regular engine record by record."""

    def people(self):
        existing, new, two_way, three_way = [], [], 0, 0
        for k in range(25):
            first, last = f"Pat{k}", f"Zorn{k}x"
            email, phone, email2 = f"p{k}@mail.test", f"512-555-{1000 + k}", f"p{k}.alt@mail.test"
            existing.append({"first_name": first, "last_name": last, "email": email})
            existing.append({"first_name": first, "last_name": last, "phone": phone})
            if k < 5:
                existing.append({"first_name": first, "last_name": last, "email": email})    # a second record that merges with the first: a loser with several records
            if k < 15:
                new.append({"first_name": first, "last_name": last, "email": email, "phone": phone, "city": f"Town{k}"})
                two_way += 1
            else:
                existing.append({"first_name": first, "last_name": last, "email": email2})
                new.append({"first_name": first, "last_name": last, "email": email, "phone": phone, "city": f"Town{k}"})
                new.append({"first_name": first, "last_name": last, "phone": phone, "email": email2, "state": "TX"})
                three_way += 1
        return existing, new, two_way, three_way

    def test_every_bridge_matches_the_regular_engine(self):
        existing, new, two_way, three_way = self.people()
        rows, cut = existing + new, len(existing)
        self.domain = "customer"
        store, path = self.existing_data(rows, cut)
        self.assertEqual(len(self.golden(path)), 25 * 2 + 10, "the existing records must start as separate entities (the 5 duplicates already merged into their first) for this test to mean anything")
        self.base_entities = {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records")}
        oracle, oracle_path = self.clone(path)
        for i in range(cut, len(rows)):
            ingest_record(oracle, TENANT, "customer", "new", f"r{i}", rows[i])
        result = bulk_load(store, TENANT, "customer", [("new", f"r{i}", rows[i]) for i in range(cut, len(rows))], onboard=True)
        self.assertEqual(result.existing_entities_merged, 15 * 1 + 10 * 2)                 # a two-way bridge merges one entity away, a three-way bridge two
        self.assertEqual(result.entities, 0)                                               # and no new entity is needed: every new record joined an existing one
        self.same(path, oracle_path, cut)
        self.assertEqual(len(self.golden(path)), 25)                                       # one person each
        tombstones = [r for r in self.q(path, "SELECT status FROM entity_customer WHERE status = 'merged'")]
        self.assertEqual(len(tombstones), 35)


class TestGuards(Onb):
    def test_existing_data_needs_an_explicit_onboard(self):
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, TENANT, "customer", "src", "a", ANN_A)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_B)])
        self.assertIn("onboard", str(cm.exception))
        self.assertIn("regular ingest", str(cm.exception))

    def test_records_already_held_are_refused_and_nothing_is_written(self):
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, TENANT, "customer", "src", "a", ANN_A)
        before = self.snapshot(path)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_B), ("src", "a", {**ANN_A, "city": "Reno"})], onboard=True)
        self.assertIn("already held", str(cm.exception))
        self.assertEqual(self.snapshot(path), before)

    def test_existing_records_that_are_not_finished_are_refused(self):
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, TENANT, "customer", "src", "a", ANN_A)
        ingest_record(store, TENANT, "customer", "src", "b", ANN_B, mode="batch")   # landed, not matched yet
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", [("new", "n", ANN_BRIDGE)], onboard=True)
        self.assertIn("process-pending", str(cm.exception))

    def test_onboarding_onto_an_empty_tenant_is_just_a_bulk_load(self):
        rows = self.rows()
        _, plain, _ = self.bulk(rows)
        store, onboarded = self.store()
        result = bulk_load(store, TENANT, self.domain, [("src", f"r{i}", r) for i, r in enumerate(rows)], onboard=True)
        self.assertEqual((result.existing_records, result.existing_entities_touched), (0, 0))
        self.assertEqual(self.golden(onboarded), self.golden(plain))

    def test_another_tenants_records_do_not_count_as_existing(self):
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, "someone-else", "customer", "src", "a", ANN_A)
        result = bulk_load(store, TENANT, "customer", [("src", "a", ANN_A)])             # a bulk load, not an onboarding, and the same natural key is fine
        self.assertEqual(result.records, 1)


class TestReadsLessFromTheStore(Onb):
    """The pairing stage used to fetch the entity and BOTH staged forms of every existing record it paired with. Reading is the cost on a store larger than memory."""

    def wanted_by_clustering(self, work):
        con = sqlite3.connect(os.path.join(work, "edges.db"))
        con.execute("ATTACH DATABASE ? AS w", (os.path.join(work, "work.db"),))
        auto = {r[0] for r in con.execute("SELECT DISTINCT x.record_id FROM edges ed JOIN w.ext x ON x.rid = -ed.a WHERE ed.decision = 'auto_merge' AND ed.a < 0")}
        total = con.execute("SELECT COUNT(*) FROM w.ext").fetchone()[0]
        return auto, total

    def test_an_existing_record_is_looked_up_for_its_entity_only_if_an_auto_merge_edge_needs_it(self):
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        store, path = self.existing_data(rows, cut)
        asked, real, work = [], Store.get_record_entities, os.path.join(self.tmp, "lazy")

        def spy(self_, tenant_id, domain, ids):
            ids = list(ids)
            asked.extend(ids)
            return real(self_, tenant_id, domain, ids)
        with mock.patch.object(Store, "get_record_entities", spy):
            bulk_load(store, TENANT, self.domain, [("new", f"r{i}", rows[i]) for i in range(cut, len(rows))], onboard=True, write=False, workdir=work)
        needed, total = self.wanted_by_clustering(work)
        self.assertEqual(set(asked), needed)                 # exactly the existing records on an auto-merge edge
        self.assertEqual(len(asked), len(set(asked)))        # each once
        self.assertLess(len(needed), total, "the data must pair new records with existing ones that no auto-merge needs, or this test proves nothing")

    def test_the_lookups_by_id_use_the_primary_key_and_not_a_scan_of_the_whole_tenant(self):
        """The two queries below were planned as a scan of every record (or entity) of the tenant for each batch of 500 ids."""
        rows = self.rows()
        store, path = self.existing_data(rows, len(rows))
        con = sqlite3.connect(path)
        for label, sql, params, scan in (
                ("get_record_entities", Store._RECORD_ENTITIES_SQL.format(marks="?,?,?"), ["a", "b", "c", TENANT, self.domain], "idx_source_records_"),
                ("get_entities_batch", Store._ENTITIES_BATCH_SQL.format(table=f"entity_{self.domain}", marks="?,?,?"), ["a", "b", "c", TENANT], "idx_entity_")):
            plan = " | ".join(r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql, params))
            self.assertIn("autoindex", plan, f"{label}: {plan}")                   # the primary key
            self.assertNotIn(scan, plan, f"{label}: {plan}")                        # not the tenant's indexes
        some = [r["id"] for r in self.q(path, "SELECT id FROM source_records LIMIT 30")]
        self.assertEqual(len(store.get_record_entities(TENANT, self.domain, some)), 30)
        self.assertEqual(store.get_record_entities("another-tenant", self.domain, some), {})     # the tenant is still enforced
        self.assertEqual(store.get_record_entities(TENANT, "another-domain", some), {})
        entity_ids = [r["entity_id"] for r in self.q(path, f"SELECT entity_id FROM entity_{self.domain} LIMIT 20")]
        self.assertEqual(len(store.get_entities_batch(TENANT, self.domain, entity_ids)), 20)
        self.assertEqual(store.get_entities_batch("another-tenant", self.domain, entity_ids), {})

    def test_the_matching_form_alone_is_what_scoring_gets(self):
        rows = self.rows()
        store, path = self.existing_data(rows, len(rows))
        ids = [r["id"] for r in self.q(path, "SELECT id FROM source_records LIMIT 40")]
        staged, matching = store.get_staged_many(TENANT, self.domain, ids), store.get_matching_many(TENANT, self.domain, ids + ["no-such-record"])
        self.assertEqual(matching, {rid: staged[rid][0] for rid in ids})     # the same matching forms, nothing for a record that is not there

    def test_unfinished_records_are_counted_through_the_index_that_holds_only_them(self):
        store, path = self.store()
        self.domain = "customer"
        ingest_record(store, TENANT, "customer", "src", "a", ANN_A)
        self.assertEqual(store.unfinished_in(TENANT, "customer"), 0)
        ingest_record(store, TENANT, "customer", "src", "b", ANN_B, mode="batch")
        ingest_record(store, "someone-else", "customer", "src", "c", ANN_B, mode="batch")
        self.assertEqual((store.unfinished_in(TENANT, "customer"), store.unfinished_in("someone-else", "customer"), store.unfinished_in("nobody", "customer")), (1, 1, 0))
        plan = " ".join(r[3] for r in sqlite3.connect(path).execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM source_records INDEXED BY idx_source_records_unfinished WHERE domain = 'customer' AND tenant_id = 'x' AND processing_status <> 'matched'"))
        self.assertIn("idx_source_records_unfinished", plan)


class TestStoreOperations(Onb):
    def setUp(self):
        super().setUp()
        self.domain = "customer"
        self.store_, self.path = self.store()
        for i, raw in enumerate([ANN_A, ANN_B, {"first_name": "Bo", "last_name": "Yu", "email": "bo@example.com"}]):
            ingest_record(self.store_, TENANT, "customer", "src", f"r{i}", raw)

    def test_counts_and_lookups(self):
        s = self.store_
        self.assertEqual(s.count_tenant_records(TENANT, "customer"), 3)
        self.assertEqual(s.count_tenant_records("nobody", "customer"), 0)
        self.assertEqual(s.unfinished_in(TENANT, "customer"), 0)
        ids = {r["source_record_id"]: r["id"] for r in self.q(self.path, "SELECT id, source_record_id FROM source_records")}
        entities = s.get_record_entities(TENANT, "customer", list(ids.values()))
        self.assertEqual(len(entities), 3)
        self.assertEqual(s.get_record_entities("nobody", "customer", list(ids.values())), {})
        got = s.get_entities_batch(TENANT, "customer", set(entities.values()))
        self.assertEqual(set(got), set(entities.values()))
        self.assertTrue(all(e.created_at for e in got.values()))
        self.assertEqual([r.source_record_id for r in s.list_records_of_entities(TENANT, "customer", [entities[ids["r2"]]])], ["r2"])
        keys = {r["blocking_key"] for r in self.q(self.path, "SELECT blocking_key FROM blocking_keys WHERE record_id = ?", (ids["r0"],))}
        members = s.blocking_members(TENANT, "customer", keys)
        self.assertTrue(all(ids["r0"] in m for m in members.values()))
        self.assertEqual(s.blocking_members("nobody", "customer", keys), {})
        self.assertEqual(s.blocking_members(TENANT, "customer", ["no-such-key"]), {})

    def test_apply_onboarding_is_all_or_nothing(self):
        s = self.store_
        ids = {r["source_record_id"]: r["entity_id"] for r in self.q(self.path, "SELECT source_record_id, entity_id FROM source_records")}
        before = self.snapshot(self.path)
        entity = s.get_entities_batch(TENANT, "customer", [ids["r0"]])[ids["r0"]]
        entity.attributes = {**entity.attributes, "city": "Reno"}
        broken = mock.Mock(id="x", tenant_id=TENANT, domain="customer", entity_id=ids["r0"], field="city", old_value=None, new_value="Reno",
                           source_record_id=None, reason="t", changed_at="now")
        with self.assertRaises(sqlite3.Error):
            s.apply_onboarding(TENANT, "customer", [(ids["r1"], ids["r0"])], [entity], [broken, broken])   # the second insert repeats the id
        self.assertEqual(self.snapshot(self.path), before)                                              # the reassignment and the rewrite were undone


class TestCommandLine(Onb):
    def test_a_second_source_is_onboarded_with_the_flag(self):
        db = os.path.join(self.tmp, "cli.db")
        for name, rows in (("one.csv", [("1", "Ann", "Lee", "ann.lee@example.com"), ("2", "Bo", "Yu", "bo@example.com")]),
                           ("two.csv", [("9", "Ann", "Lee", "ann.lee@example.com"), ("8", "Cy", "Ox", "cy@example.com")])):
            with open(os.path.join(self.tmp, name), "w") as f:
                f.write("id,first,last,email\n" + "".join(",".join(r) + "\n" for r in rows))
        run = lambda name, *a: subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", TENANT, "--domain", "customer", "--file", os.path.join(self.tmp, name),
                                                "--db", db, "--id-column", "id", "--source-system", name, "--mapping", "first_name=first,last_name=last,email=email", "--bulk", *a],
                                               capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(run("one.csv").returncode, 0)
        refused = run("two.csv")
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("--onboard", refused.stdout + refused.stderr)
        ok = run("two.csv", "--onboard")
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        self.assertIn("existing", ok.stdout)
        con = sqlite3.connect(db)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM source_records").fetchone()[0], 4)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'").fetchone()[0], 3)   # Ann appears in both sources: 4 records, 3 people


if __name__ == "__main__":
    unittest.main()
