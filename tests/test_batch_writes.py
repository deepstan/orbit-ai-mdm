"""
Writing in batches: one transaction for many records (store.batch), a rollback scope for one record (store.savepoint), and what that must never change.

  * the RESULT: the same groups whatever the batch size, and whether records arrive one at a time or in a batch
  * ATOMICITY: a batch that dies leaves none of its matching behind; what was landed is durable; finishing it gives the groups an uninterrupted run gives
  * ISOLATION: a failing record rolls back only itself; another thread never sees a batch half written
  * THE POINT: far fewer commits (and connections) per record
The same unit of work is tested against PostgreSQL in tests/test_store_contract.py (it runs there when TEST_DATABASE_URL is set).
"""
import csv
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
import unittest.mock as mock

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

from app import pipeline  # noqa: E402
from app.db import Store  # noqa: E402
from app.pipeline import ingest_record, process_pending, run_batch  # noqa: E402

TENANT, DOMAIN = "acme", "customer"


def sample(n=200):
    with open(os.path.join(ROOT, "samples", "sample_customers_200.csv"), newline="") as f:
        rows = list(csv.DictReader(f))[:n]
    m = {"first_name": "First Name", "last_name": "Last Name", "email": "Email", "phone": "Phone", "address_line1": "Address", "city": "City", "state": "State", "postal_code": "Zip"}
    return [("src", r["CustomerID"], {k: r[v] for k, v in m.items()}) for r in rows]


class Crash(BaseException):
    """A process dying: not an Exception, so nothing in the pipeline may swallow it."""


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel"})
        env.start()
        self.addCleanup(env.stop)
        self.path = os.path.join(self.tmp, "t.db")
        self.store = Store(self.path)

    def fresh(self, name):
        return Store(os.path.join(self.tmp, name))

    def groups(self, store=None):
        by = {}
        for r in (store or self.store).list_source_records(TENANT, DOMAIN):
            by.setdefault(r.entity_id, set()).add(r.source_record_id)
        return {frozenset(v) for v in by.values()}

    def statuses(self, store=None):
        return {r.source_record_id: r.processing_status for r in (store or self.store).list_source_records(TENANT, DOMAIN)}

    def counts(self, store=None):
        s = store or self.store
        return len(s.list_entities(TENANT, DOMAIN, "active")), len(s.list_candidates(TENANT, DOMAIN, status="confirmed")), len(s.list_candidates(TENANT, DOMAIN, status="pending"))


class TestTheSameResult(Base):
    def test_every_batch_size_and_the_one_at_a_time_path_give_the_same_groups(self):
        recs = sample()
        reference = self.fresh("one.db")
        for ss, sid, a in recs:
            ingest_record(reference, TENANT, DOMAIN, ss, sid, a)
        expect = (self.groups(reference), self.counts(reference))
        for size in (1, 7, 64, 200):
            store = self.fresh(f"b{size}.db")
            with mock.patch.object(pipeline, "COMMIT_EVERY", size):
                run_batch(store, TENANT, DOMAIN, recs)
            self.assertEqual((self.groups(store), self.counts(store)), expect, f"commit size {size}")

    def test_a_record_that_changes_inside_a_batch_is_handled_in_order(self):
        recs = sample(40)
        changed = [(ss, sid, {**a, "city": "Reno"}) for ss, sid, a in recs[:10]]
        one = self.fresh("one.db")
        for ss, sid, a in recs + changed:
            ingest_record(one, TENANT, DOMAIN, ss, sid, a)
        batch = self.fresh("batch.db")
        with mock.patch.object(pipeline, "COMMIT_EVERY", 13):
            run_batch(batch, TENANT, DOMAIN, recs)
            run_batch(batch, TENANT, DOMAIN, changed)
        self.assertEqual((self.groups(batch), self.counts(batch)), (self.groups(one), self.counts(one)))


class TestAtomicity(Base):
    def run_with_crash(self, store, recs, at, commit_every):
        calls = {"n": 0}
        real = pipeline._route_record

        def dying(*a, **k):
            calls["n"] += 1
            if calls["n"] == at:
                raise Crash()
            return real(*a, **k)
        with mock.patch.object(pipeline, "COMMIT_EVERY", commit_every), mock.patch.object(pipeline, "_route_record", dying):
            with self.assertRaises(Crash):
                run_batch(store, TENANT, DOMAIN, recs)

    def test_a_process_that_dies_mid_batch_loses_only_the_batch_it_was_in_and_the_rest_finishes(self):
        recs = sample(60)
        clean = self.fresh("clean.db")
        run_batch(clean, TENANT, DOMAIN, recs)
        self.run_with_crash(self.store, recs, at=25, commit_every=10)     # dies on the 25th record: chunks 1 and 2 (records 1-20) committed, chunk 3 was rolled back
        status = self.statuses()
        self.assertEqual(len(status), 60)                                  # landing is durable: nothing has to be sent again
        self.assertEqual(sum(1 for s in status.values() if s == "matched"), 20)
        unfinished = [sid for sid, s in status.items() if s != "matched"]
        self.assertEqual(len(unfinished), 40)
        self.assertEqual({status[sid] for sid in unfinished}, {"staged"})
        entities_before = len(self.store.list_entities(TENANT, DOMAIN, "active"))
        self.assertLessEqual(entities_before, 20)                           # nothing of the dead chunk's matching is left behind
        result = process_pending(self.store, DOMAIN)
        self.assertEqual((result["finished"], result["errors"], result["remaining"]), (40, 0, 0))
        self.assertEqual((self.groups(), self.counts()), (self.groups(clean), self.counts(clean)))      # the same answer an uninterrupted run gives

    def test_a_dead_chunk_leaves_no_entity_without_records_and_no_record_without_its_entity(self):
        recs = sample(30)
        self.run_with_crash(self.store, recs, at=15, commit_every=10)
        active = {e.id for e in self.store.list_entities(TENANT, DOMAIN, "active")}
        owners = {r.entity_id for r in self.store.list_source_records(TENANT, DOMAIN) if r.entity_id}
        self.assertEqual(owners, active)                                    # every entity has its records, every record's entity exists

    def test_one_failing_record_rolls_back_only_itself(self):
        recs = sample(30)
        calls = {"n": 0}
        real = pipeline._route_record

        def flaky(*a, **k):
            calls["n"] += 1
            if calls["n"] == 5:
                raise RuntimeError("simulated crash")
            return real(*a, **k)
        with mock.patch.object(pipeline, "_route_record", flaky):
            out = run_batch(self.store, TENANT, DOMAIN, recs)
        self.assertEqual(sum(1 for r in out.results if "error" in r), 1)
        status = self.statuses()
        failed = [r for r in self.store.list_source_records(TENANT, DOMAIN) if r.processing_status != "matched"]
        self.assertEqual(len(failed), 1)
        self.assertIn("simulated crash", failed[0].processing_error)
        self.assertEqual(failed[0].processing_attempts, 1)
        keys = sqlite3.connect(self.path).execute("SELECT COUNT(*) FROM blocking_keys WHERE record_id = ?", (failed[0].id,)).fetchone()[0]
        self.assertEqual(keys, 0)                                           # what that record had written was undone with it
        self.assertEqual(sum(1 for s in status.values() if s == "matched"), 29)      # the rest of the batch is untouched and committed
        process_pending(self.store, DOMAIN)                                 # and the failed one finishes later
        self.assertEqual(set(self.statuses().values()), {"matched"})

    def test_a_realtime_failure_still_records_why_and_reaches_the_caller(self):
        with mock.patch.object(pipeline, "_route_record", side_effect=RuntimeError("simulated crash")):
            with self.assertRaises(RuntimeError):
                ingest_record(self.store, TENANT, DOMAIN, "src", "X1", {"first_name": "Ann", "last_name": "Lee", "email": "a@b.com"})
        rec = self.store.list_source_records(TENANT, DOMAIN)[0]
        self.assertEqual((rec.processing_status, rec.processing_attempts), ("landed", 1))
        self.assertIn("simulated crash", rec.processing_error)


class TestTheUnitOfWork(Base):
    def test_a_batch_commits_at_the_end_and_a_failed_one_leaves_nothing(self):
        with self.store.batch(DOMAIN):
            ingest_record(self.store, TENANT, DOMAIN, "src", "A", {"first_name": "Ann", "last_name": "Lee", "email": "a@b.com"})
            self.assertEqual(len(self.store.list_source_records(TENANT, DOMAIN)), 1)      # it reads its own writes
        self.assertEqual(len(self.store.list_source_records(TENANT, DOMAIN)), 1)
        with self.assertRaises(RuntimeError):
            with self.store.batch(DOMAIN):
                ingest_record(self.store, TENANT, DOMAIN, "src", "B", {"first_name": "Bob", "last_name": "Cole", "email": "b@c.com"})
                raise RuntimeError("boom")
        self.assertEqual([r.source_record_id for r in self.store.list_source_records(TENANT, DOMAIN)], ["A"])

    def test_a_savepoint_undoes_only_its_own_block(self):
        with self.store.batch(DOMAIN):
            ingest_record(self.store, TENANT, DOMAIN, "src", "A", {"first_name": "Ann", "last_name": "Lee", "email": "a@b.com"})
            with self.assertRaises(RuntimeError):
                with self.store.savepoint():
                    ingest_record(self.store, TENANT, DOMAIN, "src", "B", {"first_name": "Bob", "last_name": "Cole", "email": "b@c.com"})
                    raise RuntimeError("boom")
            ingest_record(self.store, TENANT, DOMAIN, "src", "C", {"first_name": "Cy", "last_name": "Dunn", "email": "c@d.com"})
        self.assertEqual(sorted(r.source_record_id for r in self.store.list_source_records(TENANT, DOMAIN)), ["A", "C"])

    def test_a_savepoint_outside_a_batch_does_nothing_and_a_batch_inside_a_batch_joins_it(self):
        with self.store.savepoint():
            self.assertFalse(self.store.in_batch())
        with self.store.batch(DOMAIN):
            with self.store.batch(DOMAIN):
                self.assertTrue(self.store.in_batch())
            self.assertTrue(self.store.in_batch())                          # the inner one did not end the outer transaction
        self.assertFalse(self.store.in_batch())

    def test_the_golden_table_check_cannot_run_inside_a_batch_and_says_how_to_fix_it(self):
        fresh = Store(self.path)
        with self.assertRaises(RuntimeError) as caught:
            with fresh.batch():                                             # no domain: the table check is still to come
                fresh.save_entity(__import__("app.models", fromlist=["Entity"]).Entity(attributes={}, status="active", tenant_id=TENANT, domain=DOMAIN))
        self.assertIn("store.batch(domain=", str(caught.exception))
        with Store(self.path).batch(DOMAIN):                                # with the domain named, it is checked BEFORE the transaction opens
            pass

    def test_another_thread_never_sees_a_batch_half_written(self):
        inside, release, seen = threading.Event(), threading.Event(), {}

        def writer():
            with self.store.batch(DOMAIN):
                ingest_record(self.store, TENANT, DOMAIN, "src", "A", {"first_name": "Ann", "last_name": "Lee", "email": "a@b.com"})
                inside.set()
                release.wait(10)

        t = threading.Thread(target=writer)
        t.start()
        self.assertTrue(inside.wait(10))
        seen["during"] = len(self.store.list_source_records(TENANT, DOMAIN))      # this thread has its OWN connection, not the writer's
        self.assertFalse(self.store.in_batch())
        release.set()
        t.join(10)
        seen["after"] = len(self.store.list_source_records(TENANT, DOMAIN))
        self.assertEqual((seen["during"], seen["after"]), (0, 1))


class TestThePoint(Base):
    def connects(self, fn):
        real, n = sqlite3.connect, {"n": 0}

        def counting(*a, **k):
            n["n"] += 1
            return real(*a, **k)
        with mock.patch.object(sqlite3, "connect", counting):
            fn()
        return n["n"]

    def test_a_batch_of_200_opens_a_handful_of_connections_not_thousands(self):
        recs = sample(200)
        used = self.connects(lambda: run_batch(self.store, TENANT, DOMAIN, recs))
        self.assertLess(used, 15)                  # measured: 9. Before: about 25 per record, so about 5,000 for these 200 (and 12.6 commits per record)

    def test_one_record_is_a_batch_of_one_a_few_connections_not_dozens(self):
        recs = sample(30)
        for ss, sid, a in recs[:20]:
            ingest_record(self.store, TENANT, DOMAIN, ss, sid, a)
        used = self.connects(lambda: ingest_record(self.store, TENANT, DOMAIN, *recs[25]))
        self.assertLess(used, 7)                   # measured: 4 (landing, then one transaction for the matching). Before: about 25


class TestFewerStatements(Base):
    """Every statement is a network round trip on PostgreSQL. A record used to cost 16.8 statements (person) and now costs about half; these keep it there, and keep the results the same."""

    def counting(self):
        seen = {"all": [], }

        class Counting(sqlite3.Connection):
            def execute(inner, sql, *a):
                seen["all"].append(sql.strip())
                return super().execute(sql, *a)

            def executemany(inner, sql, *a):
                seen["all"].append(sql.strip())
                return super().executemany(sql, *a)
        real = sqlite3.connect
        patcher = mock.patch.object(sqlite3, "connect", lambda *a, **k: real(*a, factory=Counting, **k))
        return seen, patcher

    def test_a_batch_of_200_customers_costs_at_most_14_statements_a_record_and_deletes_no_blocking_keys(self):
        recs = sample(200)
        seen, patcher = self.counting()
        with patcher:
            run_batch(self.store, TENANT, DOMAIN, recs)
        self.assertLessEqual(len(seen["all"]) / 200, 14)                    # measured 12.7
        self.assertEqual([q for q in seen["all"] if q.startswith("DELETE FROM blocking_keys")], [])    # a NEW record has no keys to delete

    def test_asking_which_of_200_records_are_held_is_a_handful_of_queries_not_200(self):
        recs = sample(200)
        run_batch(self.store, TENANT, DOMAIN, recs[:100])
        seen, patcher = self.counting()
        with patcher:
            held = self.store.find_source_records_by_keys(TENANT, DOMAIN, [(ss, sid) for ss, sid, _ in recs])
        self.assertEqual(len(held), 100)
        self.assertLessEqual(len([q for q in seen["all"] if q.startswith("SELECT * FROM source_records")]), 2)

    def test_separations_are_asked_once_per_transaction_in_a_batch_and_every_time_in_realtime(self):
        recs = sample(60)
        asked = {"n": 0}
        real = Store.has_separations

        def counting(store, *a, **k):
            asked["n"] += 1
            return real(store, *a, **k)
        with mock.patch.object(Store, "has_separations", counting), mock.patch.object(pipeline, "COMMIT_EVERY", 20):
            run_batch(self.store, TENANT, DOMAIN, recs)
        self.assertLessEqual(asked["n"], 4)                                  # 60 records in 3 transactions (it was 60)
        asked["n"] = 0
        with mock.patch.object(Store, "has_separations", counting):
            for ss, sid, a in sample(70)[60:]:
                ingest_record(self.store, TENANT, DOMAIN, ss, sid, a)
        self.assertGreaterEqual(asked["n"], 10)                              # realtime asks for every record: a person may separate records between two calls

    def test_a_new_entity_has_its_golden_record_and_lineage_at_once_in_a_batch_and_in_realtime(self):
        one = ("src", "N1", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "phone": "404-555-0188", "city": "Atlanta", "state": "GA", "postal_code": "30301"})
        batch, realtime = self.fresh("b.db"), self.fresh("r.db")
        run_batch(batch, TENANT, DOMAIN, [one])
        ingest_record(realtime, TENANT, DOMAIN, *one)
        for store in (batch, realtime):
            entity = store.list_entities(TENANT, DOMAIN, "active")[0]
            self.assertEqual((entity.attributes["first_name"], entity.attributes["email"]), ("Ann", "ann@x.com"))
            self.assertEqual(sorted(l.field for l in store.list_lineage(entity.id, TENANT, DOMAIN)), sorted(f for f, v in entity.attributes.items() if v))
        self.assertEqual({k: v for k, v in batch.list_entities(TENANT, DOMAIN, "active")[0].attributes.items()},
                         {k: v for k, v in realtime.list_entities(TENANT, DOMAIN, "active")[0].attributes.items()})

    def test_a_dead_chunk_leaves_no_lineage_for_entities_that_do_not_exist(self):
        recs = sample(30)
        calls = {"n": 0}
        real = pipeline._route_record

        def dying(*a, **k):
            calls["n"] += 1
            if calls["n"] == 15:
                raise Crash()
            return real(*a, **k)
        with mock.patch.object(pipeline, "COMMIT_EVERY", 10), mock.patch.object(pipeline, "_route_record", dying):
            with self.assertRaises(Crash):
                run_batch(self.store, TENANT, DOMAIN, recs)
        orphans = sqlite3.connect(self.path).execute("SELECT COUNT(*) FROM lineage WHERE entity_id NOT IN (SELECT entity_id FROM entity_customer)").fetchone()[0]
        self.assertEqual(orphans, 0)                                          # the pooled lineage of the rolled-back chunk was dropped with it
        process_pending(self.store, DOMAIN)
        clean = self.fresh("clean.db")
        run_batch(clean, TENANT, DOMAIN, recs)
        self.assertEqual((self.groups(), self.counts()), (self.groups(clean), self.counts(clean)))


if __name__ == "__main__":
    unittest.main()
