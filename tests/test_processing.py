"""
From raw to golden: per-record processing state and resume, the run log, and what differs between REALTIME and BATCH.

Run with:  python3 -m unittest tests.test_processing -v
"""
import importlib
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))
import fake_web  # noqa: E402

from app import domain_config, pipeline
from app.db import Store
from app.migrations import SQLiteMigrationBackend, run_migrations
from app.migrations.versions import MIGRATIONS
from app.pipeline import MODES, Run, batch_ctx, flush_golden, ingest_batch, ingest_record, merge_two_records, process_pending, run_batch

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

    def ingest(self, rid, attrs, system="crm", **kw):
        return ingest_record(self.store, "acme", "customer", system, rid, attrs, **kw)

    def state(self, rid="C1"):
        return self.sql("SELECT processing_status, processing_attempts, processing_error FROM source_records WHERE source_record_id = ?", (rid,))[0]

    def crash_on_nth_lookup(self, n):
        real, calls = Store.find_candidate_record_ids, {"n": 0}

        def flaky(store, *a, **k):
            calls["n"] += 1
            if calls["n"] == n:
                raise RuntimeError("simulated crash")
            return real(store, *a, **k)
        return mock.patch.object(Store, "find_candidate_record_ids", flaky)

    def crash_when_marking_matched_once(self):
        real, fired = Store.set_processing_status, {"done": False}

        def flaky(store, ids, tenant, domain, status, error=None):
            if status == "matched" and not fired["done"]:
                fired["done"] = True
                raise RuntimeError("simulated crash")
            return real(store, ids, tenant, domain, status, error)
        return mock.patch.object(Store, "set_processing_status", flaky)


class TestEveryRecordKnowsHowFarItGot(Base):
    def test_a_crash_leaves_the_record_unfinished_and_says_why(self):
        with self.crash_on_nth_lookup(1):
            with self.assertRaises(RuntimeError):
                self.ingest("C1", ANN)  # realtime: the error reaches the caller
        status, attempts, error = self.state()
        self.assertEqual((status, attempts), ("staged", 1))
        self.assertIn("simulated crash", error)

    def test_sending_it_again_resumes_it_instead_of_calling_it_a_duplicate(self):
        """The bug this fixes: idempotent ingest used to answer 'unchanged' for a record that never finished, and nothing completed it."""
        with self.crash_on_nth_lookup(1):
            self.assertRaises(RuntimeError, self.ingest, "C1", ANN)
        again = self.ingest("C1", ANN)
        self.assertTrue(again["resumed"])
        self.assertFalse(again.get("unchanged"))
        self.assertEqual(self.state(), ("matched", 1, None))
        self.assertEqual(self.sql("SELECT COUNT(*) FROM source_records")[0][0], 1)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 1)
        self.assertTrue(self.ingest("C1", ANN)["unchanged"])  # and NOW it is a finished duplicate

    def test_a_crash_after_the_merge_but_before_the_record_was_marked_done_duplicates_nothing(self):
        a = self.ingest("A", ANN)
        with self.crash_when_marking_matched_once():
            self.assertRaises(RuntimeError, self.ingest, "B", {**ANN, "city": "Reno"}, "erp")
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates")[0][0], 1)  # the merge happened
        again = self.ingest("B", {**ANN, "city": "Reno"}, "erp")
        self.assertTrue(again["resumed"])
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates")[0][0], 1)   # not decided twice
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 1)
        self.assertEqual(again["matched_entity_id"], a["matched_entity_id"])

    def test_a_crash_after_the_candidate_was_saved_but_before_the_merge_is_finished_on_resume(self):
        a = self.ingest("A", ANN)
        real, fired = pipeline.merge_two_records, {"done": False}

        def flaky(*args, **kwargs):
            if not fired["done"]:
                fired["done"] = True
                raise RuntimeError("simulated crash")
            return real(*args, **kwargs)
        with mock.patch.object(pipeline, "merge_two_records", flaky):
            self.assertRaises(RuntimeError, self.ingest, "B", {**ANN, "city": "Reno"}, "erp")
        self.assertEqual(self.sql("SELECT status FROM match_candidates")[0][0], "confirmed")  # decided, but the merge never ran
        again = self.ingest("B", {**ANN, "city": "Reno"}, "erp")
        self.assertEqual(again["matched_entity_id"], a["matched_entity_id"])                   # now it has
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates")[0][0], 1)

    def test_a_review_item_is_not_queued_twice_by_a_resume(self):
        home = {"first_name": "Jon", "address_line1": "12 Oak St", "city": "Austin", "postal_code": "73301"}
        self.ingest("A", {**home, "last_name": "Park Sr"})
        with self.crash_when_marking_matched_once():
            self.assertRaises(RuntimeError, self.ingest, "B", {**home, "last_name": "Park Jr"})
        again = self.ingest("B", {**home, "last_name": "Park Jr"})
        self.assertEqual(len(again["queued_for_review"]), 1)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM match_candidates WHERE status = 'pending'")[0][0], 1)

    def test_resuming_does_not_duplicate_the_blocking_keys(self):
        clean = Store(os.path.join(self.tmp, "clean.db"))
        ingest_record(clean, "acme", "customer", "crm", "C1", ANN)
        expected = sqlite3.connect(os.path.join(self.tmp, "clean.db")).execute("SELECT COUNT(*) FROM blocking_keys").fetchone()[0]
        with self.crash_on_nth_lookup(1):
            self.assertRaises(RuntimeError, self.ingest, "C1", ANN)
        self.ingest("C1", ANN)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM blocking_keys")[0][0], expected)

    def test_a_changed_payload_starts_the_record_over(self):
        self.ingest("C1", ANN)
        self.ingest("C1", {**ANN, "city": "Reno"})
        self.assertEqual(self.state()[0], "matched")

    def test_a_record_that_keeps_failing_is_left_alone_after_max_attempts_and_shown(self):
        with self.crash_on_nth_lookup(1):
            self.assertRaises(RuntimeError, self.ingest, "C1", ANN)
        always = mock.patch.object(Store, "find_candidate_record_ids", side_effect=RuntimeError("still broken"))
        with always:
            for _ in range(3):
                process_pending(self.store, "customer", max_attempts=3)
        self.assertEqual(self.store.list_unfinished_records("customer", 10, max_attempts=3), [])  # given up on: it needs a human
        self.assertEqual(self.store.count_unfinished("customer"), (1, 1))                          # ...and status still reports it
        self.assertEqual(self.state()[1], 3)


class TestBatchRuns(Base):
    def items(self, n=4):
        return [("crm", f"B{i}", {"first_name": f"P{i}", "last_name": "Wu", "email": f"p{i}@y.com"}) for i in range(n)]

    def test_one_bad_record_does_not_stop_the_run(self):
        with self.crash_on_nth_lookup(3):
            batch = run_batch(self.store, "acme", "customer", self.items())
        self.assertEqual([("error" in r) for r in batch.results], [False, False, True, False])
        self.assertEqual((batch.run["status"], batch.run["counts"]["errors"], batch.run["counts"]["new"]), ("completed_with_errors", 1, 3))
        self.assertIn("simulated crash", batch.run["errors"][0]["error"])
        self.assertEqual(self.sql("SELECT COUNT(*) FROM source_records")[0][0], 4)     # all four are held...
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 3)    # ...three finished, one waits
        self.assertEqual(self.store.count_unfinished("customer"), (1, 1))

    def test_re_running_the_same_batch_finishes_the_failed_record_and_leaves_the_rest_alone(self):
        with self.crash_on_nth_lookup(3):
            run_batch(self.store, "acme", "customer", self.items())
        again = run_batch(self.store, "acme", "customer", self.items())
        self.assertEqual({k: v for k, v in again.run["counts"].items() if v}, {"received": 4, "unchanged": 3, "resumed": 1, "standalone": 1, "golden_rebuilt": 1})
        self.assertEqual((self.sql("SELECT COUNT(*) FROM source_records")[0][0], self.sql("SELECT COUNT(*) FROM entity_customer")[0][0]), (4, 4))
        self.assertEqual(self.store.count_unfinished("customer"), (0, 0))

    def test_an_error_budget_aborts_the_run_and_process_pending_finishes_the_rest(self):
        with self.crash_on_nth_lookup(1):
            batch = run_batch(self.store, "acme", "customer", self.items(), max_errors=0)
        self.assertEqual(batch.run["status"], "failed")
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 0)   # it stopped
        self.assertEqual(self.store.count_unfinished("customer")[0], 4)                # nothing is lost
        out = process_pending(self.store, "customer")
        self.assertEqual((out["finished"], out["errors"]), (4, 0))
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 4)

    def test_a_batch_is_logged_and_realtime_is_not(self):
        for i in range(3):
            self.ingest(f"R{i}", {**ANN, "email": f"r{i}@x.com"})
        self.assertEqual(self.sql("SELECT COUNT(*) FROM pipeline_runs")[0][0], 0)       # one row per API call would swamp the log
        run_batch(self.store, "acme", "customer", self.items())
        (row,) = self.store.list_runs(domain="customer")
        self.assertEqual((row["mode"], row["status"], row["counts"]["new"], row["counts"]["received"]), ("batch", "completed", 4, 4))

    def test_the_run_log_is_the_arrival_log_for_unchanged_re_sends(self):
        run_batch(self.store, "acme", "customer", self.items())
        run_batch(self.store, "acme", "customer", self.items())  # the source sends the same file again
        newest, oldest = self.store.list_runs(domain="customer")
        self.assertEqual((newest["counts"]["unchanged"], newest["counts"]["new"]), (4, 0))
        self.assertEqual((oldest["counts"]["unchanged"], oldest["counts"]["new"]), (0, 4))

    def test_runs_can_be_filtered_by_domain_and_tenant(self):
        run_batch(self.store, "acme", "customer", self.items(2))
        run_batch(self.store, "other", "customer", self.items(2))
        run_batch(self.store, "acme", "vendor", [("erp", "V1", {"company_name": "Acme", "tax_id": "12-3456789"})])
        self.assertEqual(len(self.store.list_runs(domain="customer")), 2)
        self.assertEqual(len(self.store.list_runs(tenant_id="acme")), 2)
        self.assertEqual(len(self.store.list_runs(domain="vendor", tenant_id="acme")), 1)

    def test_a_reindex_is_logged_too(self):
        from app.indexing import reindex_domain
        self.ingest("C1", ANN)
        reindex_domain(self.store, "customer")
        (row,) = [r for r in self.store.list_runs(domain="customer") if r["mode"] == "reindex"]
        self.assertEqual((row["status"], row["counts"]["received"], row["counts"]["golden_rebuilt"]), ("completed", 1, 1))


class TestRealtimeAndBatchDiffer(Base):
    def chain(self, n=6):
        return [("crm", f"M{i}", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "phone": "512-555-0123", "city": f"City{i}"}) for i in range(n)]

    def test_realtime_publishes_golden_after_every_merge_batch_rebuilds_each_entity_once(self):
        real = pipeline.rebuild_golden_record
        with mock.patch.object(pipeline, "rebuild_golden_record", wraps=real) as spy:
            for ss, rid, attrs in self.chain():
                self.ingest(rid, attrs, ss)
            realtime_calls = spy.call_count
        other = Store(os.path.join(self.tmp, "b.db"))
        with mock.patch.object(pipeline, "rebuild_golden_record", wraps=real) as spy:
            ingest_batch(other, "acme", "customer", self.chain())
            batch_calls = spy.call_count
        self.assertGreaterEqual(realtime_calls, 6)
        self.assertEqual(batch_calls, 1)  # six records, one entity, one rebuild

    def test_the_golden_records_are_identical_either_way(self):
        seq = self.chain(5) + [("crm", "X1", {"first_name": "Bo", "last_name": "Chen", "email": "bo@y.com"}),
                               ("erp", "X2", {"first_name": "Bob", "last_name": "Chen", "email": "bo@y.com", "city": "Reno"}),
                               ("crm", "X3", {"first_name": "Cy", "last_name": "Wu", "phone": "212-555-0000"})]
        other = Store(os.path.join(self.tmp, "b.db"))
        for ss, rid, attrs in seq:
            ingest_record(self.store, "acme", "customer", ss, rid, attrs)
        ingest_batch(other, "acme", "customer", seq)
        rows = lambda path: sorted(sqlite3.connect(path).execute(
            "SELECT first_name, last_name, email, phone, city FROM entity_customer WHERE status = 'active'").fetchall())
        self.assertEqual(rows(self.dbpath), rows(os.path.join(self.tmp, "b.db")))
        self.assertEqual(len(rows(self.dbpath)), 3)

    def test_a_run_killed_before_its_final_flush_leaves_a_durable_marker_that_process_pending_clears(self):
        with mock.patch.object(pipeline, "flush_golden", return_value=0):  # as if the process died before the end of the run
            run_batch(self.store, "acme", "customer", self.chain(3))
        self.assertEqual(self.sql("SELECT COUNT(*) FROM golden_dirty")[0][0], 1)
        self.assertIsNone(self.sql("SELECT first_name FROM entity_customer")[0][0])  # the golden record is stale: nothing was published
        out = process_pending(self.store, "customer")
        self.assertEqual((out["golden_rebuilt"], self.sql("SELECT COUNT(*) FROM golden_dirty")[0][0]), (1, 0))
        self.assertEqual(self.sql("SELECT first_name FROM entity_customer")[0][0], "Ann")

    def test_a_crash_between_progress_flushes_is_repaired_by_process_pending_without_duplicating_anything(self):
        """Progress is written in groups. If the process dies before a group is saved, those records are still `staged` although they
        were matched, and finishing them must not decide anything twice."""
        seq = self.chain(4) + [("crm", "Z1", {"first_name": "Zed", "last_name": "Quill", "email": "z@q.com"})]
        clean = Store(os.path.join(self.tmp, "clean.db"))
        ingest_batch(clean, "acme", "customer", seq)
        counts = lambda path: [sqlite3.connect(path).execute(q).fetchone()[0] for q in (
            "SELECT COUNT(*) FROM match_candidates", "SELECT COUNT(*) FROM entity_customer WHERE status = 'active'",
            "SELECT COUNT(*) FROM entity_customer WHERE status = 'merged'", "SELECT COUNT(*) FROM blocking_keys")]
        real = Store.set_processing_status
        lost = lambda store, ids, tenant, domain, status, error=None: None if status == "matched" else real(store, ids, tenant, domain, status, error)
        with mock.patch.object(Store, "set_processing_status", lost):  # the progress write never reaches the database
            run_batch(self.store, "acme", "customer", seq)
        self.assertEqual(self.store.count_unfinished("customer")[0], 5)   # all look unfinished...
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], len(set(r[0] for r in self.sql("SELECT entity_id FROM source_records"))))  # ...though matched
        out = process_pending(self.store, "customer")
        self.assertEqual((out["finished"], out["errors"], self.store.count_unfinished("customer")[0]), (5, 0, 0))
        self.assertEqual(counts(self.dbpath), counts(os.path.join(self.tmp, "clean.db")))  # the same candidates, entities and keys as a clean run

    def test_an_entity_merged_away_during_a_run_is_not_rebuilt_into_blankness(self):
        a = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com"})
        b = self.ingest("B", {"first_name": "Bob", "last_name": "Chen", "email": "bob@y.com"})
        ctx = batch_ctx()
        merge_two_records(self.store, "acme", "customer", a["record_id"], b["record_id"], "test", ctx=ctx)
        ctx.defer_golden.add(("acme", b["matched_entity_id"]))  # the loser is in the dirty set too
        flush_golden(self.store, "customer", ctx.defer_golden)
        loser = self.sql("SELECT status, first_name FROM entity_customer WHERE entity_id = ?", (b["matched_entity_id"],))[0]
        self.assertEqual(loser, ("merged", "Bob"))  # untouched

    def test_llm_adjudication_runs_inline_in_realtime_but_not_in_batch(self):
        pair = [("crm", "A", {"first_name": "Jennifer", "last_name": "Nguyen", "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301"}),
                ("crm", "B", {"first_name": "Jenna", "last_name": "Nguyen", "address_line1": "9 Elm Street", "city": "Austin", "postal_code": "73344"})]

        def count_calls(runner):
            with mock.patch.object(pipeline.llm_adjudication, "is_enabled", return_value=True), \
                 mock.patch.object(pipeline.llm_adjudication, "adjudicate", return_value=None) as adjudicate:
                runner()
            return adjudicate.call_count
        self.assertEqual(count_calls(lambda: [self.ingest(rid, a, ss) for ss, rid, a in pair]), 1)
        other = Store(os.path.join(self.tmp, "b.db"))
        self.assertEqual(count_calls(lambda: ingest_batch(other, "acme", "customer", pair)), 0)
        third = Store(os.path.join(self.tmp, "c.db"))
        with mock.patch.dict(os.environ, {"MDM_LLM_IN_BATCH": "true"}):
            self.assertEqual(count_calls(lambda: ingest_batch(third, "acme", "customer", pair)), 1)  # opt-in

    def test_mode_batch_accepts_now_and_matches_later_in_arrival_order(self):
        seq = [("A", {**ANN}), ("B", {**ANN, "city": "Reno"}), ("C", {"first_name": "Cy", "last_name": "Wu", "email": "c@z.com"})]
        accepted = [self.ingest(rid, attrs, mode="batch") for rid, attrs in seq]
        self.assertTrue(all(r["accepted"] and r["deferred"] and r["matched_entity_id"] is None for r in accepted))
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 0)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM blocking_keys")[0][0], 0)       # not indexed yet: later records can't see it early
        self.assertEqual(self.sql("SELECT DISTINCT processing_status FROM source_records")[0][0], "staged")
        out = process_pending(self.store, "customer")
        self.assertEqual(out["finished"], 3)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 2)  # A+B merged, C alone
        self.assertEqual([r["mode"] for r in self.store.list_runs(domain="customer")], ["resume"])

    def test_deferred_processing_gives_the_same_entities_as_realtime(self):
        seq = self.chain(4) + [("crm", "Z", {"first_name": "Zed", "last_name": "Q", "email": "z@q.com"})]
        other = Store(os.path.join(self.tmp, "b.db"))
        for ss, rid, attrs in seq:
            ingest_record(other, "acme", "customer", ss, rid, attrs)
            ingest_record(self.store, "acme", "customer", ss, rid, attrs, mode="batch")
        process_pending(self.store, "customer")
        groups = lambda path: sorted(r[0] for r in sqlite3.connect(path).execute(
            "SELECT COUNT(*) FROM source_records WHERE entity_id IS NOT NULL GROUP BY entity_id").fetchall())
        self.assertEqual(groups(self.dbpath), groups(os.path.join(self.tmp, "b.db")))

    def test_an_invalid_mode_is_refused(self):
        self.assertEqual(MODES, ("realtime", "batch"))
        with self.assertRaises(ValueError):
            self.ingest("C1", ANN, mode="whenever")


class TestApiHandlers(unittest.TestCase):
    """The API functions, called directly under a stubbed FastAPI (the real HTTP layer still can't run here)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.saved = {k: sys.modules.get(k) for k in ("fastapi", "fastapi.responses", "fastapi.staticfiles", "pydantic", "app.api")}
        cls.env = {k: os.environ.get(k) for k in ("DATABASE_URL", "MDM_DOMAINS_DIR", "MDM_ENABLE_AUTH")}
        os.environ.update(DATABASE_URL=os.path.join(cls.tmp, "api.db"), MDM_DOMAINS_DIR=cls.tmp)
        os.environ.pop("MDM_ENABLE_AUTH", None)

        fake_web.install(force=True)   # the shared stand-in (tests/fake_web.py), put back in tearDownClass
        sys.modules.pop("app.api", None)
        cls.api = importlib.import_module("app.api")
        cls.HTTPException = sys.modules["fastapi"].HTTPException
        from app.auth import Principal
        cls.who = Principal(tenant_id="acme", role="tenant_admin", key_id="k", label="test")

    @classmethod
    def tearDownClass(cls):
        for k, v in cls.saved.items():
            sys.modules.pop(k, None) if v is None else sys.modules.__setitem__(k, v)
        for k, v in cls.env.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        domain_config.reset()
        shutil.rmtree(cls.tmp, True)

    def req(self, rid, **attrs):
        return types.SimpleNamespace(source_system="crm", source_record_id=rid, attributes={"first_name": "Ann", "last_name": "Lee", **attrs})

    def audits(self, action):
        return [e for e in self.api.store.list_audit_log("acme") if e.action == action]

    def metered(self):
        return sum(e.quantity for e in self.api.store.list_usage_events("acme") if e.event_type == "record_ingested")

    def test_realtime_is_audited_and_metered_per_request_and_an_unchanged_resend_is_not_metered(self):
        before_audit, before_usage = len(self.audits("ingest")), self.metered()
        self.api.ingest("customer", self.req("RT1", email="rt1@x.com"), principal=self.who)
        self.api.ingest("customer", self.req("RT1", email="rt1@x.com"), principal=self.who)
        self.assertEqual((len(self.audits("ingest")) - before_audit, self.metered() - before_usage), (2, 1))

    def test_mode_batch_on_one_record_answers_202_without_matching(self):
        r = self.api.ingest("customer", self.req("DEF1", email="def1@x.com"), mode="batch", principal=self.who)
        self.assertEqual((r.status_code, r.content["accepted"], r.content["matched_entity_id"]), (202, True, None))

    def test_an_unknown_mode_is_a_422(self):
        with self.assertRaises(self.HTTPException) as cm:
            self.api.ingest("customer", self.req("X"), mode="eventually", principal=self.who)
        self.assertEqual(cm.exception.status_code, 422)

    def test_the_batch_endpoint_logs_one_run_writes_one_audit_entry_and_meters_the_total(self):
        body = types.SimpleNamespace(records=[self.req(f"BT{i}", email=f"bt{i}@x.com") for i in range(3)], max_errors=None, defer=False)
        a0, u0 = len(self.audits("ingest_batch")), self.metered()
        first = self.api.ingest_batch_endpoint("customer", body, principal=self.who)
        again = self.api.ingest_batch_endpoint("customer", body, principal=self.who)
        self.assertEqual((first.status_code, first.content["counts"]["new"], first.content["status"]), (200, 3, "completed"))
        self.assertEqual(again.content["counts"]["unchanged"], 3)
        self.assertEqual(len(self.audits("ingest_batch")) - a0, 2)   # one per RUN, not one per record
        self.assertEqual(self.metered() - u0, 3)                      # the re-send added nothing
        self.assertTrue(self.api.list_runs("customer", principal=self.who))

    def test_defer_answers_202(self):
        body = types.SimpleNamespace(records=[self.req("DB1", email="db1@x.com")], max_errors=None, defer=True)
        self.assertEqual(self.api.ingest_batch_endpoint("customer", body, principal=self.who).status_code, 202)


class TestCli(Base):
    def cli(self, script, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, script), *args], capture_output=True, text=True, cwd=ROOT)

    def test_defer_then_status_then_process_pending_then_runs(self):
        csv_path = os.path.join(self.tmp, "in.csv")
        with open(csv_path, "w") as f:
            f.write("id,first,last,email\n1,Ann,Lee,a@x.com\n2,Ann,Lee,a@x.com\n3,Cy,Wu,c@z.com\n")
        load = self.cli("bulk_ingest.py", "--tenant", "acme", "--domain", "customer", "--file", csv_path, "--db", self.dbpath,
                        "--id-column", "id", "--mapping", "first_name=first,last_name=last,email=email", "--defer")
        self.assertEqual(load.returncode, 0, load.stderr)
        self.assertIn("Accepted, not yet matched: 3", load.stdout)
        status = self.cli("domains_cli.py", "status", "--db", self.dbpath)
        self.assertRegex(status.stdout, r"customer\s+\w+\s+3\s+3\s+0\s+3\s+0")                     # records staged dupes pending failing
        self.assertIn("process-pending customer", status.stdout)
        done = self.cli("domains_cli.py", "process-pending", "customer", "--db", self.dbpath)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("Finished 3 record(s), 0 failed", done.stdout)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'")[0][0], 2)
        self.assertIn("Nothing to finish", self.cli("domains_cli.py", "process-pending", "customer", "--db", self.dbpath).stdout)
        runs = self.cli("domains_cli.py", "runs", "customer", "--db", self.dbpath).stdout
        self.assertRegex(runs, r"batch\s+completed\s+received=3, deferred=3")
        self.assertRegex(runs, r"resume\s+completed")

    def test_status_flags_failing_records(self):
        with self.crash_on_nth_lookup(1):
            self.assertRaises(RuntimeError, self.ingest, "C1", ANN)
        status = self.cli("domains_cli.py", "status", "--db", self.dbpath)
        self.assertEqual(status.returncode, 1)  # something needs a person
        self.assertIn("1 record(s) failed", status.stdout)


class TestMigration012(Base):
    def legacy_db(self, upto):
        path = os.path.join(self.tmp, "old.db")
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:upto])
        c = sqlite3.connect(path)
        ts = "2026-01-01T00:00:00+00:00"
        c.execute("INSERT INTO entity_customer (entity_id, tenant_id, status, created_at, updated_at) VALUES ('ent_ok','acme','active',?,?)", (ts, ts))
        people = {"rec_done": "Zed Quill zed@q.com", "rec_stranded": "Ann Lee ann@x.com", "rec_waiting": "Cy Wu cy@z.com"}  # three different people
        for rid, ent, system in (("rec_done", "ent_ok", "a"), ("rec_stranded", None, "b"), ("rec_waiting", None, "c")):
            first, last, email = people[rid].split()
            c.execute("INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, ingested_at, entity_id) "
                      "VALUES (?, 'acme', 'customer', ?, ?, ?, ?, ?)", (rid, system, rid, '{"first_name": "%s", "last_name": "%s", "email": "%s"}' % (first, last, email), ts, ent))
        c.execute("INSERT INTO match_candidates (id, tenant_id, domain, record_a_id, record_b_id, score, match_type, reasons, status, created_at) "
                  "VALUES ('cand1','acme','customer','rec_waiting','rec_done',0.8,'fuzzy','[]','pending',?)", (ts,))
        c.commit()
        c.close()
        return path

    def test_records_the_previous_release_stranded_are_found_by_migration_012(self):
        path = self.legacy_db(11)
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:12])  # stop at 012: this is what 012 alone does
        status = dict(sqlite3.connect(path).execute("SELECT id, processing_status FROM source_records").fetchall())
        self.assertEqual(status, {"rec_done": "matched", "rec_waiting": "matched", "rec_stranded": "staged"})  # only the stranded one

    def test_upgrading_all_the_way_finishes_the_stranded_record_and_gives_the_waiting_one_its_own_entity(self):
        path = self.legacy_db(11)
        store = Store(path)  # migrates to the latest version: 013 also marks the entity-less record that is merely waiting for review
        self.assertEqual(store.count_unfinished("customer"), (2, 0))
        out = process_pending(store, "customer")
        self.assertEqual((out["finished"], out["remaining"]), (2, 0))
        entities = {r.id: r.entity_id for r in store.list_source_records("acme", "customer")}
        self.assertTrue(all(entities.values()))                                            # nobody is left without an entity
        self.assertEqual(len({entities["rec_stranded"], entities["rec_waiting"]}), 2)      # each its own
        self.assertEqual(store.list_candidates("acme", "customer", "pending")[0].id, "cand1")  # the review item is untouched
        self.assertEqual(run_migrations(SQLiteMigrationBackend(path)), [])                 # and it is idempotent


if __name__ == "__main__":
    unittest.main()
