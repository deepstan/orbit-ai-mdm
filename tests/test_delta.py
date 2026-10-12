"""
Delta (app/bulk.py, bulk_load(..., update=True)): MORE records from a source already loaded. Each is new, unchanged or changed. The oracle is the
regular engine: the same existing data, the same feed ingested one record at a time. Nothing here measures time.

Run with:  python3 -m unittest tests.test_delta -v
"""
import json
import os
import sqlite3
import subprocess
import sys
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_bulk_engine import ROOT, TENANT  # noqa: E402
from test_onboarding import ANN_A, ANN_B, Onb  # noqa: E402
from app.bulk import BulkLoadError, bulk_load  # noqa: E402
from app.db import Store  # noqa: E402
from app.domains import get_domain_config  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402


class Delta(Onb):
    def records(self, path, cut=None):
        """version, hash, payload and, for the records that were already there (index < cut), their arrival time: it must not change. (A NEW record's arrival time
        differs between the engines on purpose: wall clock against input order.)"""
        existing = lambda sid: cut is None or (sid[1:].isdigit() and int(sid[1:]) < cut)
        return {r["source_record_id"]: (r["version"], r["content_hash"], r["attributes"], r["ingested_at"] if existing(r["source_record_id"]) else None)
                for r in self.q(path, "SELECT source_record_id, version, content_hash, attributes, ingested_at FROM source_records")}

    def history(self, path):
        return {(r["source_record_id"], r["version"], r["content_hash"], r["attributes"]) for r in self.q(
            path, "SELECT s.source_record_id, h.version, h.content_hash, h.attributes FROM source_record_history h JOIN source_records s ON s.id = h.record_id")}

    def apply_feed(self, rows, cut, feed, base="bulk", reverse=False, **kw):
        """rows[:cut] are the existing data (source 'src', ids r0, r1, ...). feed: [(source record id, attributes)] from the same source: new, unchanged or changed."""
        store, path = self.existing_data(rows, cut, base)
        self.base_entities = {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records")}
        oracle, oracle_path = self.clone(path)
        for sid, attrs in (reversed(feed) if reverse else feed):
            ingest_record(oracle, TENANT, self.domain, "src", sid, attrs)
        result = bulk_load(store, TENANT, self.domain, [("src", sid, attrs) for sid, attrs in feed], update=True, **kw)
        return store, path, oracle_path, result

    def same_delta(self, got, want, cut):
        self.same(got, want, cut)
        self.assertEqual(self.records(got, cut), self.records(want, cut))   # versions, hashes, payloads and the original arrival time of every record that was there
        self.assertEqual(self.history(got), self.history(want))   # every payload a change replaced

    def feed(self, rows, cut, new=20, changed=12, unchanged=6):
        """A mixed feed built from generated customers: new records, records of the existing data changed four different ways, and some resent as they were."""
        out, picks = [], list(range(0, cut, max(1, cut // changed)))[:changed]
        for k, i in enumerate(picks):
            attrs = dict(rows[i])
            j = (i + cut // 2) % cut
            if k % 4 == 0:   # takes on another record's identifiers: it may now link another entity
                attrs.update({f: rows[j].get(f) for f in ("email", "phone") if rows[j].get(f)})
            elif k % 4 == 1:   # a corrected name
                attrs["last_name"] = (attrs.get("last_name") or "Doe") + "son"
            elif k % 4 == 2:   # a brand new identifier: the old one is lost
                attrs["email"] = f"fresh{i}@mail.test"
            else:
                attrs["city"] = "Newtown"
            out.append((f"r{i}", attrs))
        out += [(f"r{i}", rows[i]) for i in range(1, cut, max(1, cut // unchanged))][:unchanged]
        out += [(f"r{i}", rows[i]) for i in range(cut, cut + new)]
        return out


class TestMatchesTheRegularEngine(Delta):
    def test_a_mixed_feed_of_new_changed_and_unchanged_records(self):
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        feed = self.feed(rows, cut)
        store, path, oracle, result = self.apply_feed(rows, cut, feed)
        self.assertEqual((result.new_records, result.changed_records, result.unchanged_records), (20, 12, 6))
        self.same_delta(path, oracle, cut)

    def test_the_same_onto_a_base_built_record_by_record(self):
        rows = self.rows(scale=4, seed=3)
        cut = int(len(rows) * 0.5)
        store, path, oracle, result = self.apply_feed(rows, cut, self.feed(rows, cut, new=15, changed=10, unchanged=4), base="realtime")
        self.same_delta(path, oracle, cut)

    def test_people_with_a_protected_ssn(self):
        rows = self.rows("person", scale=1)
        cut = int(len(rows) * 0.6)
        feed = [(f"r{i}", {**rows[i], "first_name": (rows[i].get("first_name") or "Al") + "ex"}) for i in range(0, cut, 7)] + [(f"r{i}", rows[i]) for i in range(cut, len(rows))]
        store, path, oracle, result = self.apply_feed(rows, cut, feed)
        self.assertGreater(result.changed_records, 0)
        self.same_delta(path, oracle, cut)

    def test_the_order_of_the_feed_does_not_change_who_is_grouped_or_which_pairs_are_decided(self):
        """What a golden VALUE is can depend on feed order (a record later in the feed is more recent, as in the regular engine). Which records are
        grouped, and every pair's score and status, cannot."""
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        feed = self.feed(rows, cut)
        _, forward, oracle, _ = self.apply_feed(rows, cut, feed)
        _, backward, _, _ = self.apply_feed(rows, cut, list(reversed(feed)))
        self.same_delta(forward, oracle, cut)
        self.assertEqual(set(self.golden(forward)), set(self.golden(backward)))            # the same groups of records
        self.assertEqual(self.candidates(forward), self.candidates(backward))              # the same pairs, scores and statuses

    def test_workers_and_chunk_size_change_nothing(self):
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        feed = self.feed(rows, cut)
        _, plain, oracle, _ = self.apply_feed(rows, cut, feed)
        _, many, oracle2, _ = self.apply_feed(rows, cut, feed, workers=2, chunk=5)
        self.same_delta(plain, oracle, cut)
        self.same_delta(many, oracle2, cut)


class TestWhatAChangeDoes(Delta):
    def setUp(self):
        super().setUp()
        self.domain = "customer"

    def base(self, base_rows):
        store, path = self.store()
        for i, attrs in enumerate(base_rows):
            ingest_record(store, TENANT, "customer", "src", f"r{i}", attrs)
        self.base_entities = {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records")}
        return store, path

    def delta(self, store, path, feed, **kw):
        oracle, oracle_path = self.clone(path)
        for sid, attrs in feed:
            ingest_record(oracle, TENANT, "customer", "src", sid, attrs)
        return bulk_load(store, TENANT, "customer", [("src", sid, attrs) for sid, attrs in feed], update=True, **kw), oracle_path

    def test_a_change_that_links_two_entities_merges_them_and_the_oldest_survives(self):
        store, path = self.base([ANN_A, ANN_B])
        self.assertEqual(len(self.golden(path)), 2, "the two Anns must start apart")
        older = self.q(path, "SELECT entity_id FROM source_records WHERE source_record_id = 'r0'")[0]["entity_id"]
        result, oracle = self.delta(store, path, [("r1", {**ANN_B, "email": "ann.lee@example.com"})])   # B learns A's email
        self.assertEqual((result.changed_records, result.new_records, result.existing_entities_merged), (1, 0, 1))
        entities = {r["source_record_id"]: r["entity_id"] for r in self.q(path, "SELECT source_record_id, entity_id FROM source_records")}
        self.assertEqual(set(entities.values()), {older})
        self.same_delta(path, oracle, 2)

    def test_the_old_payload_goes_to_history_and_the_record_keeps_its_id_and_its_age(self):
        store, path = self.base([ANN_A])
        before = self.records(path)["r0"]
        rid_before = self.q(path, "SELECT id FROM source_records")[0]["id"]
        result, oracle = self.delta(store, path, [("r0", {**ANN_A, "city": "Reno"})])
        now = self.records(path)["r0"]
        self.assertEqual((before[0], now[0]), (1, 2))                                    # version 1 -> 2
        self.assertEqual(now[3], before[3])                                              # ingested_at is untouched: recency does not move
        self.assertEqual(self.q(path, "SELECT id FROM source_records")[0]["id"], rid_before)   # same record, in place
        history = self.q(path, "SELECT version, content_hash, attributes FROM source_record_history")
        self.assertEqual([(h["version"], h["content_hash"]) for h in history], [(1, before[1])])
        self.assertEqual(json.loads(history[0]["attributes"]), ANN_A)                    # the payload that was replaced, whole
        self.same_delta(path, oracle, 1)

    def test_a_changed_record_forgets_the_keys_it_gave_up(self):
        """X and R share an email; R changes to another. A new record with the shared email must meet X, and must NOT meet R through a key R no longer holds."""
        x = {"first_name": "Zed", "last_name": "Moss", "email": "shared@mail.test"}
        r = {"first_name": "Ray", "last_name": "Orr", "email": "shared@mail.test"}
        store, path = self.base([x, r])
        feed = [("r1", {**r, "email": "ray.orr@mail.test"}), ("n", {"first_name": "Zed", "last_name": "Moss", "email": "shared@mail.test", "city": "Reno"})]
        result, oracle = self.delta(store, path, feed, write=False, workdir=os.path.join(self.tmp, "w"))   # a dry run, to look at the pairs
        with_new_record = lambda: self.q(os.path.join(self.tmp, "w", "pairs.db"), "SELECT a, b FROM pairs")
        pairs = with_new_record()
        self.assertNotIn((0, 1), {(p["a"], p["b"]) for p in pairs}, "R (node 0) and the new record (node 1) must not be paired through R's old key")
        # and the real thing agrees with the regular engine, candidates included
        store2, path2 = self.base([x, r])
        result2, oracle2 = self.delta(store2, path2, feed)
        self.same_delta(path2, oracle2, 2)

    def test_the_bucket_cap_does_not_count_the_keys_a_change_gives_up(self):
        """Three existing records hold a key; two of them change away from it; one new record brings it again. The bucket is 2, not 4."""
        base = [{"first_name": f"N{i}", "last_name": f"Q{i}x", "email": "crowd@mail.test"} for i in range(3)]
        store, path = self.base(base)
        feed = [("r0", {**base[0], "email": "a@mail.test"}), ("r1", {**base[1], "email": "b@mail.test"}), ("n", {"first_name": "New", "last_name": "Person", "email": "crowd@mail.test"})]
        config = get_domain_config("customer")
        with mock.patch.object(config, "max_bucket_size", 2):   # a bucket of up to 3 may be compared on
            result = bulk_load(store, TENANT, "customer", [("src", s, a) for s, a in feed], update=True, write=False, workdir=os.path.join(self.tmp, "cap"))
        self.assertEqual(result.capped_keys, 0)                  # counted naively it would be 4 (3 existing + 1 new) and skipped
        with_existing = self.q(os.path.join(self.tmp, "cap", "pairs.db"), "SELECT COUNT(*) AS n FROM pairs WHERE a < 0")[0]["n"]
        self.assertEqual(with_existing, 1)                       # the new record meets the one record still holding the key

    def test_a_changed_record_is_not_rescored_against_its_own_entity(self):
        a = {"first_name": "Ann", "last_name": "Lee", "email": "ann.lee@example.com", "phone": "512-555-0142"}
        store, path = self.base([a, {**a, "city": "Austin"}])
        self.assertEqual(len(self.golden(path)), 1, "the two records must start as one entity")
        before = self.candidates(path)
        result, oracle = self.delta(store, path, [("r0", {**a, "city": "Dallas"})])
        self.assertEqual(result.candidate_pairs, 0)
        self.assertEqual(self.candidates(path), before)                                  # no new row for a pair inside one entity
        self.same_delta(path, oracle, 2)

    def test_the_review_items_scored_on_the_old_data_are_superseded(self):
        rows = self.rows(scale=4)
        store, path = self.store()
        for i, attrs in enumerate(rows):
            ingest_record(store, TENANT, "customer", "src", f"r{i}", attrs)
        self.base_entities = {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records")}
        pending = self.q(path, "SELECT a.source_record_id AS sid FROM match_candidates c JOIN source_records a ON a.id = c.record_a_id WHERE c.status = 'pending' LIMIT 1")
        self.assertTrue(pending, "the data must hold a review item for this test to mean anything")
        sid = pending[0]["sid"]
        result, oracle = self.delta(store, path, [(sid, {**rows[int(sid[1:])], "city": "Elsewhere"})])
        self.assertGreater(self.q(path, "SELECT COUNT(*) AS n FROM match_candidates WHERE status = 'superseded'")[0]["n"], 0)
        self.same_delta(path, oracle, len(rows))

    def test_unchanged_records_cost_nothing_and_write_nothing(self):
        store, path = self.base([ANN_A, ANN_B])
        before = self.snapshot(path)
        result = bulk_load(store, TENANT, "customer", [("src", "r0", ANN_A), ("src", "r1", ANN_B)], update=True)
        self.assertEqual((result.unchanged_records, result.changed_records, result.new_records), (2, 0, 0))
        self.assertEqual(self.snapshot(path), before)


class TestDeltaReadsLessFromTheStore(Delta):
    def test_for_a_delta_an_entity_is_also_looked_up_for_existing_records_paired_with_a_changed_record(self):
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        store, path = self.existing_data(rows, cut)
        feed = [("src", sid, attrs) for sid, attrs in self.feed(rows, cut)]
        asked, real, work = [], Store.get_record_entities, os.path.join(self.tmp, "lazy")

        def spy(self_, tenant_id, domain, ids):
            ids = list(ids)
            asked.extend(ids)
            return real(self_, tenant_id, domain, ids)
        with mock.patch.object(Store, "get_record_entities", spy):
            result = bulk_load(store, TENANT, self.domain, feed, update=True, write=False, workdir=work)
        con = sqlite3.connect(os.path.join(work, "edges.db"))
        con.execute("ATTACH DATABASE ? AS w", (os.path.join(work, "work.db"),))
        con.execute("ATTACH DATABASE ? AS p", (os.path.join(work, "pairs.db"),))
        auto = {r[0] for r in con.execute("SELECT DISTINCT x.record_id FROM edges ed JOIN w.ext x ON x.rid = -ed.a WHERE ed.decision = 'auto_merge' AND ed.a < 0")}
        # pairs.db has had the same-entity pairs deleted, so ask the work table instead: an existing record was paired with a changed one if `ent` knows its entity
        with_changed = {r[0] for r in con.execute("SELECT x.record_id FROM w.ext x JOIN w.ent e ON e.idx = -x.rid")}
        total = con.execute("SELECT COUNT(*) FROM w.ext").fetchone()[0]
        self.assertEqual(set(asked), auto | with_changed)
        self.assertLess(len(set(asked)), total)
        self.assertGreater(result.changed_records, 0)


class TestSafety(Delta):
    def setUp(self):
        super().setUp()
        self.domain = "customer"

    def bridge(self):
        store, path = self.store()
        ingest_record(store, TENANT, "customer", "src", "r0", ANN_A)
        ingest_record(store, TENANT, "customer", "src", "r1", ANN_B)
        return store, path, [("src", "r1", {**ANN_B, "email": "ann.lee@example.com"}), ("src", "r9", {"first_name": "Bo", "last_name": "Yu", "email": "bo@mail.test"})]

    def test_a_failure_while_changing_existing_rows_leaves_everything_exactly_as_it_was(self):
        store, path, feed = self.bridge()
        before = self.snapshot(path)
        real = Store.apply_onboarding

        def fail_at_the_end(self_, tenant_id, domain, reassign, entities, lineage, **rest):
            self.assertTrue(rest["history"] and rest["record_updates"] and rest["derived"] and lineage)   # the changed record's update is in this transaction
            return real(self_, tenant_id, domain, reassign, entities, [*lineage, lineage[0]], **rest)
        with mock.patch.object(Store, "apply_onboarding", fail_at_the_end), self.assertRaises(sqlite3.IntegrityError):
            bulk_load(store, TENANT, "customer", feed, update=True)
        self.assertEqual(self.snapshot(path), before)   # the payload, version, history, keys, staging, candidates, entities, and the new record: all as they were
        bulk_load(store, TENANT, "customer", feed, update=True)   # and it simply runs again
        self.assertEqual(self.records(path)["r1"][0], 2)

    def test_a_failure_while_writing_the_new_rows_leaves_the_changed_records_untouched(self):
        store, path, feed = self.bridge()
        before = self.snapshot(path)
        with mock.patch.object(Store, "save_candidates_batch", side_effect=RuntimeError("disk full")), self.assertRaises(RuntimeError):
            bulk_load(store, TENANT, "customer", feed, update=True)
        self.assertEqual(self.snapshot(path), before)

    def test_a_dry_run_classifies_and_writes_nothing(self):
        store, path, feed = self.bridge()
        before = self.snapshot(path)
        dry = bulk_load(store, TENANT, "customer", feed + [("src", "r0", ANN_A)], update=True, write=False, workdir=os.path.join(self.tmp, "dry"))
        self.assertEqual((dry.new_records, dry.changed_records, dry.unchanged_records, dry.existing_entities_merged), (1, 1, 1, 1))
        self.assertEqual(self.snapshot(path), before)

    def test_without_the_delta_flag_a_held_record_is_refused_and_the_message_says_what_to_do(self):
        store, path, feed = self.bridge()
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", feed, onboard=True)
        self.assertIn("delta", str(cm.exception))

    def test_a_held_record_that_is_not_finished_is_refused(self):
        store, path, feed = self.bridge()
        ingest_record(store, TENANT, "customer", "src", "r7", {"first_name": "Cy", "last_name": "Ox", "email": "cy@mail.test"}, mode="batch")
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", [("src", "r7", {"first_name": "Cy", "last_name": "Ox", "email": "cy2@mail.test"})], update=True)
        self.assertIn("process-pending", str(cm.exception))

    def test_a_feed_that_repeats_a_record_is_refused(self):
        store, path, feed = self.bridge()
        before = self.snapshot(path)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, "customer", feed + [feed[0]], update=True)
        self.assertIn("twice", str(cm.exception))
        self.assertEqual(self.snapshot(path), before)

    def test_a_delta_onto_an_empty_tenant_is_just_a_bulk_load(self):
        rows = self.rows()
        store, path = self.store()
        result = bulk_load(store, TENANT, "customer", [("src", f"r{i}", r) for i, r in enumerate(rows)], update=True)
        self.assertEqual((result.new_records, result.changed_records, result.unchanged_records), (len(rows), 0, 0))
        _, plain, _ = self.bulk(rows)
        self.assertEqual(self.golden(path), self.golden(plain))

    def test_a_paused_and_resumed_delta_ends_where_an_uninterrupted_one_does(self):
        from app.bulk import BulkPaused
        rows = self.rows(scale=4)
        cut = int(len(rows) * 0.6)
        feed = self.feed(rows, cut)
        _, whole, oracle, _ = self.apply_feed(rows, cut, feed)
        store, path = self.existing_data(rows, cut)
        work, pauses = os.path.join(self.tmp, "resume"), 0
        while True:
            try:
                bulk_load(store, TENANT, "customer", lambda start: iter([("src", s, a) for s, a in feed][start:]), update=True, workdir=work, resume=True, budget=0, chunk=7)
                break
            except BulkPaused:
                pauses += 1
        self.assertGreater(pauses, 3)
        self.assertEqual(self.golden(path), self.golden(oracle))
        self.assertEqual(self.history(path), self.history(oracle))
        self.assertEqual({k: v[:3] for k, v in self.records(path).items()}, {k: v[:3] for k, v in self.records(oracle).items()})


class TestCommandLine(Delta):
    def test_the_delta_flag(self):
        db = os.path.join(self.tmp, "cli.db")
        files = {"one.csv": [("1", "Ann", "Lee", "ann.lee@example.com"), ("2", "Bo", "Yu", "bo@example.com")],
                 "two.csv": [("1", "Ann", "Lee", "ann.lee@example.com"), ("2", "Bo", "Yu", "bo.yu@example.com"), ("3", "Cy", "Ox", "cy@example.com")]}
        for name, rows in files.items():
            with open(os.path.join(self.tmp, name), "w") as f:
                f.write("id,first,last,email\n" + "".join(",".join(r) + "\n" for r in rows))
        run = lambda name, *a: subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", TENANT, "--domain", "customer", "--file", os.path.join(self.tmp, name),
                                                "--db", db, "--id-column", "id", "--source-system", "crm", "--mapping", "first_name=first,last_name=last,email=email", "--bulk", *a],
                                               capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(run("one.csv").returncode, 0)
        refused = run("two.csv", "--onboard")
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("delta", refused.stdout + refused.stderr)
        ok = run("two.csv", "--delta")
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        self.assertIn("1 new, 1 changed, 1 unchanged", ok.stdout)
        con = sqlite3.connect(db)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM source_records").fetchone()[0], 3)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM source_record_history").fetchone()[0], 1)


class TestTheMeasurementHarness(Delta):
    def test_a_small_trial_runs_pauses_resumes_and_adds_up(self):
        from app import benchmark_person as bp
        work, pauses = os.path.join(self.tmp, "trial"), 0
        while True:
            try:
                report = bp.delta_trial(300, 200, work, piece=100, budget=0, log=lambda m: None)
                break
            except bp.DeltaPaused as e:
                pauses += 1
                self.assertLess(e.done, e.total)
        self.assertGreaterEqual(pauses, 3)                                       # base and feed were really loaded across several calls
        d, inv = report["delta"], report["invariants"]
        self.assertEqual((d["new"], d["changed"], d["unchanged"]), (160, 40, 0))   # every fifth feed record is a change
        self.assertEqual(inv["records"], 300 + 160)
        self.assertEqual((inv["history_rows"], inv["records_at_version_2"]), (40, 40))
        self.assertEqual((inv["records_without_an_entity"], inv["records_not_finished"], inv["records_in_a_merged_entity"], inv["active_entities_without_records"]), (0, 0, 0, 0))
        self.assertIn("classify", d["stages"])
        self.assertIn("THE DELTA", bp.format_delta_trial(report))
        with self.assertRaises(ValueError):
            bp.delta_trial(300, 250, work, piece=100, log=lambda m: None)         # another trial's directory

    def test_a_call_that_died_mid_write_is_not_built_on(self):
        from app import benchmark_person as bp
        work = os.path.join(self.tmp, "died")
        os.makedirs(work, exist_ok=True)
        bp_params = {"base": 100, "feed": 50, "seed": 7, "dup_rate": 0.08, "rates": bp.DEFAULT_RATES, "piece": 50, "variant": "scalable_tight_plus"}
        with open(os.path.join(work, "delta.json"), "w") as f:
            json.dump(bp_params, f)
        with open(os.path.join(work, "pieces.json"), "w") as f:
            json.dump({"pieces": [], "in_progress": 0}, f)
        with self.assertRaises(RuntimeError) as cm:
            bp.delta_trial(100, 50, work, piece=50, log=lambda m: None)
        self.assertIn("partly written", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
