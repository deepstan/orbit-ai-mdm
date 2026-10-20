"""
Pages of the review queue and of the entities: exact (nothing repeated, nothing skipped, even when many pairs tie on score), stable when decisions are made between two
requests, tenant-isolated, served by the indexes that make a page cost the same however deep it is, and a constant number of connections per page.

Run with:  python3 -m unittest tests.test_pagination -v
"""
import contextlib
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import paging  # noqa: E402
from app.db import Store  # noqa: E402
from app.migrations.versions import MIGRATIONS  # noqa: E402
from app.models import MatchCandidate  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402

DOMAIN = "customer"


def candidate(i, score, tenant="acme", status="pending"):
    return MatchCandidate(id=f"cand_{i:05d}", record_a_id=f"a{i}", record_b_id=f"b{i}", tenant_id=tenant, domain=DOMAIN, score=score, match_type="fuzzy", status=status)


class Fresh(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.path = os.path.join(self.tmp, "t.db")
        self.store = Store(self.path)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0"})
        env.start()
        self.addCleanup(env.stop)

    def recorded(self, fn):
        """Run fn and return every (sql, params) the store sent to the database, UNEXPANDED: exactly what the planner sees, which is what decides the index."""
        log, real = [], Store._conn

        @contextlib.contextmanager
        def recording(store):
            with real(store) as conn:
                class Proxy:
                    def __getattr__(self, name):
                        return getattr(conn, name)

                    def execute(self, sql, params=()):
                        log.append((sql, tuple(params)))
                        return conn.execute(sql, params)
                yield Proxy()
        with mock.patch.object(Store, "_conn", recording):
            fn()
        return log

    def plan_of(self, log, table_hint):
        """The plan(s) of the recorded queries that read `table_hint`."""
        plans = []
        for sql, params in log:
            if sql.lstrip().upper().startswith("SELECT") and table_hint in sql and "ORDER BY" in sql:
                plans.append(self.plan(sql, params))
        self.assertTrue(plans, f"no recorded query reads {table_hint} with an ORDER BY: {[q[0][:60] for q in log]}")
        return plans

    def plan(self, sql, params=()):
        conn = sqlite3.connect(self.path)
        try:
            return " | ".join(r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params))
        finally:
            conn.close()


class TestThePendingQueue(Fresh):
    def seed(self, n=237):
        rng = random.Random(5)
        scores = [0.5, 0.6, 0.7, 0.8]                                       # few distinct scores: most pairs TIE, which is where keyset paging usually goes wrong
        cands = [candidate(i, rng.choice(scores)) for i in range(n)]
        self.store.save_candidates_batch(cands + [candidate(900 + i, 0.99, tenant="other") for i in range(30)] + [candidate(800 + i, 0.9, status="confirmed") for i in range(10)])
        return sorted(cands, key=lambda c: (c.score, c.id), reverse=True)   # best score first, ties by id descending

    def pages(self, size, tenant="acme"):
        out, after = [], None
        while True:
            page = self.store.pending_page(tenant, DOMAIN, size, after)
            out.append(page)
            if len(page) < size:
                return out
            after = (page[-1].score, page[-1].id)

    def test_the_pages_are_the_whole_queue_in_order_exactly_once(self):
        expected = self.seed()
        for size in (1, 7, 25, 100, 237, 1000):
            ids = [c.id for page in self.pages(size) for c in page]
            self.assertEqual(ids, [c.id for c in expected], f"page size {size}")
        self.assertEqual(self.store.count_pending("acme", DOMAIN), 237)       # confirmed pairs and the other tenant's are not in it

    def test_other_tenants_and_decided_pairs_never_appear(self):
        self.seed()
        seen = {c.id for page in self.pages(50) for c in page}
        self.assertTrue(all(not i.startswith(("cand_009", "cand_008")) for i in seen))
        self.assertEqual({c.tenant_id for page in self.pages(50) for c in page}, {"acme"})
        self.assertEqual(len([c for page in self.pages(50, "other") for c in page]), 30)

    def test_a_cursor_from_another_tenant_only_moves_the_start_within_the_callers_own_data(self):
        self.seed()
        foreign = self.store.pending_page("other", DOMAIN, 5)[-1]
        page = self.store.pending_page("acme", DOMAIN, 10, (foreign.score, foreign.id))
        self.assertTrue(page and all(c.tenant_id == "acme" for c in page))

    def test_decisions_made_between_two_requests_do_not_repeat_or_skip_anything_else(self):
        expected = self.seed()
        first = self.store.pending_page("acme", DOMAIN, 50)
        after = (first[-1].score, first[-1].id)
        gone_seen, gone_ahead = first[3], expected[120]                       # one pair already shown is decided, and so is one not yet shown
        for c in (gone_seen, gone_ahead):
            self.store.update_candidate_status(c.id, "acme", DOMAIN, "rejected", "steward:x", "2026-01-01T00:00:00Z")
        rest = [c.id for page in self.pages_after(after) for c in page]
        self.assertEqual(rest, [c.id for c in expected[50:] if c.id != gone_ahead.id])
        self.assertEqual(self.store.count_pending("acme", DOMAIN), 237 - 2)   # and the count agrees: both decided pairs are gone

    def pages_after(self, after, size=40):
        out = []
        while True:
            page = self.store.pending_page("acme", DOMAIN, size, after)
            out.append(page)
            if len(page) < size:
                return out
            after = (page[-1].score, page[-1].id)

    def test_the_real_page_query_uses_the_partial_index_and_needs_no_sort(self):
        self.seed()
        for after in (None, (0.7, "cand_00010")):
            log = self.recorded(lambda: self.store.pending_page("acme", DOMAIN, 51, after))
            for plan in self.plan_of(log, "match_candidates"):
                self.assertIn("idx_match_candidates_pending", plan)
                self.assertNotIn("TEMP B-TREE", plan, plan)                 # the index delivers the rows in order ...
                if after is not None:                                       # ... and the cursor is part of the index SEARCH (a seek to the position), not a filter over a walk from the start:
                    self.assertIn("(score,id)<(?,?)", plan, plan)           # that is what keeps a page at depth 199,950 as cheap as the first (an OR of two conditions is not a seek)
        log = self.recorded(lambda: self.store.count_pending("acme", DOMAIN))
        self.assertIn("COVERING INDEX", self.plan(*log[-1]))                # the count never reads the table's rows

    def test_the_pages_are_exact_even_where_the_index_is_missing(self):
        """The ORDER BY must be right by itself, not only because an index happens to deliver it: an older database, or a changed plan, has no such luck."""
        rng = random.Random(9)
        cands = [candidate(i, rng.choice([0.5, 0.6, 0.7])) for i in range(120)]
        shuffled = cands[:]
        rng.shuffle(shuffled)                                                # inserted in no particular order, so rowid order is not id order
        self.store.save_candidates_batch(shuffled)
        conn = sqlite3.connect(self.path)
        conn.execute("DROP INDEX idx_match_candidates_pending")
        conn.commit()
        conn.close()
        expected = [c.id for c in sorted(cands, key=lambda c: (c.score, c.id), reverse=True)]
        for size in (1, 13, 120):
            self.assertEqual([c.id for page in self.pages(size) for c in page], expected, f"page size {size}")

    def test_the_partial_index_holds_only_pending_pairs(self):
        self.seed()
        conn = sqlite3.connect(self.path)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM match_candidates WHERE status = 'pending'").fetchone()[0], 237 + 30)
        conn.close()
        self.store.update_candidate_status("cand_00001", "acme", DOMAIN, "confirmed", "steward:x", "2026-01-01T00:00:00Z")
        self.assertNotIn("cand_00001", [c.id for page in self.pages(500) for c in page])   # a decided pair leaves the queue (and so its index)


class TestRecordsInBatches(Fresh):
    def test_a_batch_returns_what_exists_for_this_tenant_and_domain_only(self):
        a = ingest_record(self.store, "acme", DOMAIN, "src", "1", {"first_name": "Ann", "last_name": "Lee"})["record_id"]
        b = ingest_record(self.store, "acme", DOMAIN, "src", "2", {"first_name": "Bo", "last_name": "Ng"})["record_id"]
        other = ingest_record(self.store, "other", DOMAIN, "src", "1", {"first_name": "Cy", "last_name": "Poe"})["record_id"]
        got = self.store.get_source_records_batch("acme", DOMAIN, [a, b, other, "rec_nope", a])
        self.assertEqual(set(got), {a, b})
        self.assertEqual(got[a].attributes["first_name"], "Ann")
        self.assertEqual(self.store.get_source_records_batch("acme", DOMAIN, []), {})

    def test_a_large_batch_costs_a_few_queries_not_one_per_record(self):
        ids = [ingest_record(self.store, "acme", DOMAIN, "src", str(i), {"first_name": f"N{i}", "last_name": f"L{i}", "city": f"C{i}"})["record_id"] for i in range(120)]
        connects = []
        real = sqlite3.connect
        with mock.patch.object(sqlite3, "connect", lambda *a, **k: (connects.append(1), real(*a, **k))[1]):
            got = self.store.get_source_records_batch("acme", DOMAIN, ids)
        self.assertEqual(len(got), 120)
        self.assertEqual(len(connects), 1)


class TestPagesOfEntities(Fresh):
    def seed(self, n=60):
        for i in range(n):
            ingest_record(self.store, "acme", DOMAIN, "src", f"p{i}", {"first_name": f"Name{i}", "last_name": f"Surname{i}", "email": f"p{i}@example.com", "city": f"City{i}"})
        ingest_record(self.store, "acme", DOMAIN, "src2", "dup", {"first_name": "Name3", "last_name": "Surname3", "email": "p3@example.com", "city": "City3"})   # joins one: 2 sources
        for i in range(8):
            ingest_record(self.store, "other", DOMAIN, "src", f"o{i}", {"first_name": f"Zed{i}", "last_name": f"Quill{i}", "email": f"z{i}@example.com", "city": f"Z{i}"})

    def all_pages(self, size, **kw):
        out, after = [], None
        while True:
            page = self.store.entities_page("acme", DOMAIN, size, after_id=after, **kw)
            out.append(page)
            if len(page) < size:
                return out
            after = page[-1].id

    def test_the_pages_are_every_entity_of_the_tenant_in_id_order_exactly_once(self):
        self.seed()
        every = sorted(e.id for e in self.store.list_entities("acme", DOMAIN))
        self.assertEqual(len(every), 60)
        for size in (1, 9, 20, 60, 500):
            self.assertEqual([e.id for page in self.all_pages(size) for e in page], every, f"page size {size}")

    def test_each_entity_carries_its_source_records_as_the_one_by_one_read_does(self):
        self.seed()
        for page in self.all_pages(25):
            for e in page:
                one = self.store.get_entity(e.id, "acme", DOMAIN)
                self.assertEqual(sorted(e.source_record_ids), sorted(one.source_record_ids))
                self.assertEqual((e.attributes, e.status), (one.attributes, one.status))
        sizes = sorted(len(e.source_record_ids) for page in self.all_pages(100) for e in page)
        self.assertEqual(sizes[-1], 2)

    def test_the_status_filter_and_the_tenant_boundary(self):
        self.seed()
        self.assertEqual(len([e for p in self.all_pages(10, status="active") for e in p]), 60)
        self.assertEqual([e for p in self.all_pages(10, status="merged") for e in p], [])
        self.assertEqual(len([e for p in self.all_pages(10) for e in p if e.tenant_id != "acme"]), 0)

    def test_a_page_costs_a_constant_number_of_connections_not_a_few_per_entity(self):
        self.seed()
        connects = []
        real = sqlite3.connect
        with mock.patch.object(sqlite3, "connect", lambda *a, **k: (connects.append(1), real(*a, **k))[1]):
            page = self.store.entities_page("acme", DOMAIN, 50)
        self.assertEqual(len(page), 50)
        self.assertLessEqual(len(connects), 6, f"{len(connects)} connections for one page of 50")

    def test_the_real_entity_query_is_a_range_of_the_tenant_entity_index_with_no_sort(self):
        self.seed()
        for after in (None, "ent_5"):
            for status in (None, "active"):
                log = self.recorded(lambda: self.store.entities_page("acme", DOMAIN, 51, after_id=after, status=status))
                for plan in self.plan_of(log, "entity_customer"):
                    self.assertIn("idx_entity_customer_tenant_entity", plan, plan)
                    self.assertNotIn("TEMP B-TREE", plan, plan)

    def test_asking_for_a_domain_with_no_table_is_an_empty_page_not_an_error(self):
        self.assertEqual(self.store.entities_page("acme", "vendor", 10), [])


class TestTheMigration(Fresh):
    def test_it_adds_the_indexes_to_tables_that_already_exist_and_can_run_again(self):
        conn = sqlite3.connect(self.path)
        conn.execute("DROP INDEX idx_match_candidates_pending")
        conn.execute("DROP INDEX idx_entity_customer_tenant_entity")
        conn.execute("DROP INDEX idx_entity_product_tenant_entity")
        conn.execute("DELETE FROM schema_migrations WHERE version = 16")
        conn.commit()
        conn.close()
        Store(self.path)                                                     # opening applies migration 16 to a database that has the tables but not the indexes
        for _ in range(2):                                                   # ... and a second time changes nothing and fails nothing
            conn = sqlite3.connect(self.path)
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
            conn.close()
            self.assertTrue({"idx_match_candidates_pending", "idx_entity_customer_tenant_entity", "idx_entity_product_tenant_entity", "idx_entity_vendor_tenant_entity"} <= names)
            Store(self.path)

    def test_a_golden_table_made_later_gets_its_index_when_it_is_made(self):
        conn = sqlite3.connect(self.path)
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        conn.close()
        self.assertIn("idx_entity_vendor_tenant_entity", names)
        self.assertEqual([m.version for m in MIGRATIONS if m.name == "paging_indexes"], [16])        # the migration that makes the indexes for the tables that exist then


if __name__ == "__main__":
    unittest.main()
