"""
One set of scenarios, run against EVERY database backend: the same code, the same assertions, so whatever SQLite does here is what Postgres must do too.

It covers what was added after the Postgres runbook was written: migrations 014 to 016 and their indexes, unmerge and separations, the audit counts and queue, the pages of
the review queue and the entities, and a bulk load that must keep separated records apart.

  * SQLite runs always. That is what proves the TESTS are right.
  * Postgres runs only with `pip install psycopg2-binary` and TEST_DATABASE_URL pointing at a SCRATCH database (every test uses a tenant of its own, but it does leave rows):

        export TEST_DATABASE_URL=postgresql://postgres:PASSWORD@localhost:5432/mdm_scratch
        python3 -m unittest tests.test_store_contract -v

A Postgres failure here is the first time that SQL met a real server: read it as a bug report on db_postgres.py (or on migrations 014 to 016) rather than on the test, unless
the SQLite twin of the same test fails too.
"""
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock as mock
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_unmerge import A, B, C, D  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app import monitor  # noqa: E402
from app.bulk import bulk_load  # noqa: E402
from app.models import MatchCandidate, now_iso  # noqa: E402
from app.pipeline import ingest_record, merge_two_records  # noqa: E402
from app.stewardship import audit_queue, audit_summary, give_verdict  # noqa: E402
from app.unmerge import CannotUnmerge, plan_unmerge, separation_violations, unmerge_pair  # noqa: E402

DOMAIN = "customer"
TEST_DB_URL = os.environ.get("TEST_DATABASE_URL")
try:
    import psycopg2  # noqa: F401
    HAVE_PSYCOPG2 = True
except ImportError:
    HAVE_PSYCOPG2 = False


class Contract:
    """The scenarios. A backend supplies make_store(), index_names() and migration_versions()."""

    def setUp(self):
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0"})
        env.start()
        self.addCleanup(env.stop)
        self.store, self.tenant = self.make_store()

    # ---------------------------------------------------------------- helpers
    def ingest(self, sid, attrs, system="src"):
        return ingest_record(self.store, self.tenant, DOMAIN, system, sid, attrs)

    def chain(self):
        """A-B share an email, B-C a phone: one entity, two confirmed pairs, no A-C pair. Returns {name: record id}."""
        return {n: self.ingest(n, a)["record_id"] for n, a in (("A", A), ("B", B), ("C", C))}

    def entity_of(self, record_id):
        return self.store.get_source_record(record_id, self.tenant, DOMAIN).entity_id

    def pair(self, ids, x, y):
        found = [c for c, a, b in self.store.confirmed_pairs_among(self.tenant, DOMAIN, [ids[x], ids[y]])]
        self.assertEqual(len(found), 1, f"{x}-{y}")
        return found[0]

    def pending_candidates(self, n, tenant=None):
        rng = random.Random(4)
        tag = uuid.uuid4().hex[:8]
        cands = [MatchCandidate(id=f"cand_{tag}_{i:05d}", record_a_id=f"a_{tag}_{i}", record_b_id=f"b_{tag}_{i}", tenant_id=tenant or self.tenant, domain=DOMAIN,
                                score=rng.choice([0.5, 0.6, 0.7, 0.8]), match_type="fuzzy") for i in range(n)]     # few distinct scores: most TIE
        self.store.save_candidates_batch(cands)
        return cands

    # ---------------------------------------------------------------- the schema
    def test_every_migration_is_applied_and_the_new_indexes_exist(self):
        self.assertTrue(set(range(1, 17)) <= set(self.migration_versions()), self.migration_versions())
        names = self.index_names()
        for wanted in ("idx_match_candidates_pending", "idx_merge_audits_queue", "idx_entity_customer_tenant_entity", "idx_entity_product_tenant_entity",
                       "idx_entity_vendor_tenant_entity"):
            self.assertIn(wanted, names)
        self.assertNotIn("idx_merge_audits_scope", names)                   # migration 015 replaced it

    def test_migration_016_puts_back_the_indexes_on_tables_that_already_exist(self):
        """On a fresh database the golden tables are made WITH their index, so only this exercises the migration's own loop over existing tables: on Postgres, the
        information_schema query that finds them."""
        gone = ("idx_entity_customer_tenant_entity", "idx_entity_product_tenant_entity", "idx_match_candidates_pending")
        for name in gone:
            self.run_sql(f"DROP INDEX IF EXISTS {name}")
        self.run_sql("DELETE FROM schema_migrations WHERE version = 16")
        self.assertFalse(set(gone) & self.index_names())
        self.reopen()                                                          # opening the store applies the migration that is now missing
        self.assertTrue(set(gone) <= self.index_names(), self.index_names())
        self.assertIn(16, self.migration_versions())
        self.reopen()                                                          # and once more: nothing to do, nothing fails

    # ---------------------------------------------------------------- unmerge and separations
    def test_unmerge_splits_the_entity_and_the_separation_queries_agree(self):
        ids = self.chain()
        self.assertEqual(len({self.entity_of(i) for i in ids.values()}), 1)
        self.assertFalse(self.store.has_separations(self.tenant, DOMAIN))
        ab = self.pair(ids, "A", "B")
        self.assertEqual(len(plan_unmerge(self.store, self.tenant, DOMAIN, ab)["move"]), 2)
        result = unmerge_pair(self.store, self.tenant, DOMAIN, ab, "jane", "different people")
        self.assertTrue(result["split"])
        e_a, e_bc = self.entity_of(ids["A"]), self.entity_of(ids["B"])
        self.assertNotEqual(e_a, e_bc)
        self.assertEqual(self.entity_of(ids["C"]), e_bc)
        self.assertEqual(self.store.get_candidate(ab, self.tenant, DOMAIN).decided_by, "unmerge:jane")
        self.assertTrue(self.store.has_separations(self.tenant, DOMAIN))
        self.assertEqual(self.store.separated_partners(self.tenant, DOMAIN, ids["A"]), {ids["B"]})
        self.assertEqual(self.store.entities_separated(self.tenant, DOMAIN, e_a, e_bc), ab)
        self.assertEqual(self.store.entities_separated(self.tenant, DOMAIN, e_bc, e_a), ab)
        self.assertEqual([frozenset(p) for p in self.store.separation_pairs(self.tenant, DOMAIN)], [frozenset((ids["A"], ids["B"]))])   # (in either order: which record came first is the engine's)
        self.assertEqual(separation_violations(self.store, self.tenant, DOMAIN), [])
        self.assertTrue(self.store.get_entity(e_bc, self.tenant, DOMAIN).attributes)                  # the new entity has a golden record
        with self.assertRaises(CannotUnmerge):
            unmerge_pair(self.store, self.tenant, DOMAIN, ab, "sam")                                   # not twice
        merge_two_records(self.store, self.tenant, DOMAIN, ids["A"], ids["B"], reason="an engine that does not know")
        self.assertEqual([(c, frozenset((a, b))) for c, a, b, e in separation_violations(self.store, self.tenant, DOMAIN)], [(ab, frozenset((ids["A"], ids["B"])))])

    def test_a_bulk_load_keeps_separated_records_apart(self):
        ids = self.chain()
        unmerge_pair(self.store, self.tenant, DOMAIN, self.pair(ids, "A", "B"), "jane")
        result = bulk_load(self.store, self.tenant, DOMAIN, [("src2", "D1", D)], onboard=True)       # D matches A (email) and B, C (phone)
        self.assertGreaterEqual(result.separations_held, 1)
        self.assertEqual(separation_violations(self.store, self.tenant, DOMAIN), [])
        self.assertNotEqual(self.entity_of(ids["A"]), self.entity_of(ids["B"]))
        changed = bulk_load(self.store, self.tenant, DOMAIN, [("src", "A", {**A, "phone": "404-555-0102"})], update=True)
        self.assertEqual((changed.changed_records, changed.separations_dropped), (1, 1))
        self.assertEqual(separation_violations(self.store, self.tenant, DOMAIN), [])

    # ---------------------------------------------------------------- audits
    def test_audits_are_attributed_to_who_made_the_merge_and_counted_in_sql(self):
        ids = self.chain()
        ab, bc = self.pair(ids, "A", "B"), self.pair(ids, "B", "C")
        self.store.update_candidate_status(bc, self.tenant, DOMAIN, "confirmed", ls.PROMOTED_BY, now_iso())      # B-C was made by the model
        self.assertEqual(self.store.flag_merge_audits(self.tenant, DOMAIN, [ab, bc]), 2)
        self.assertEqual(self.store.flag_merge_audits(self.tenant, DOMAIN, [ab, bc]), 0)                          # already flagged
        by = {a["candidate_id"]: a["made_by"] for a in self.store.list_merge_audits(self.tenant, DOMAIN)}
        self.assertEqual(by, {ab: "rule", bc: "promoted"})
        self.assertEqual([p["candidate_id"] for p in self.store.pending_audits(self.tenant, DOMAIN, 10)], [bc, ab])   # the model's promotions first
        self.assertEqual(len(self.store.pending_audits(self.tenant, DOMAIN, 1)), 1)
        self.assertEqual(self.store.count_auto_merges(self.tenant, DOMAIN), {"rule": 1, "promoted": 1})
        counts = self.store.audit_counts(self.tenant, DOMAIN)
        self.assertEqual((counts["waiting"], counts["rule"]["audited"], counts["promoted"]["audited"]), (2, 0, 0))
        out = give_verdict(self.store, self.tenant, DOMAIN, bc, "wrong", "jane", unmerge=True)              # a person unmerges the model's merge
        self.assertTrue(out["unmerge"]["ok"])
        self.assertEqual(self.store.get_candidate(bc, self.tenant, DOMAIN).decided_by, "unmerge:jane")      # the candidate no longer says the model made it ...
        counts = self.store.audit_counts(self.tenant, DOMAIN)
        self.assertEqual((counts["promoted"]["audited"], counts["promoted"]["wrong"], counts["rule"]["wrong"], counts["waiting"]), (1, 1, 0, 1))   # ... the audit does
        self.assertEqual(self.store.audited_wrong(self.tenant, DOMAIN), [(bc, "promoted")])
        self.assertEqual(self.store.count_auto_merges(self.tenant, DOMAIN), {"rule": 1, "promoted": 0})
        self.assertEqual(self.store.human_labeled_candidates(self.tenant, DOMAIN)[0][1:], (0, "steward"))   # an unmerge is a human label of 'different'

    def test_the_audit_page_and_the_monitor_read_the_same_numbers(self):
        ids = self.chain()
        ab = self.pair(ids, "A", "B")
        self.store.flag_merge_audits(self.tenant, DOMAIN, [ab])
        page = audit_queue(self.store, self.tenant, DOMAIN)
        self.assertEqual((page["waiting"], [i["candidate_id"] for i in page["items"]], page["items"][0]["made_by"]), (1, [ab], "rule"))
        self.assertTrue(page["items"][0]["unmerge"]["possible"])
        self.store.record_merge_audit(ab, self.tenant, DOMAIN, "correct", "pat")
        summary = audit_summary(self.store, self.tenant, DOMAIN)
        self.assertEqual((summary["waiting"], summary["strata"]["rule"]["audited"], summary["strata"]["rule"]["wrong"]), (0, 1, 0))
        assessed = monitor.assess(self.store, self.tenant, DOMAIN, 0.0005)
        self.assertEqual((assessed["verdict"], assessed["strata"]["rule"]["merges"]), ("ok", 2))

    # ---------------------------------------------------------------- pages
    def test_the_review_queue_pages_are_exact_with_many_ties(self):
        cands = self.pending_candidates(137)
        foreign = self.pending_candidates(20, tenant=f"{self.tenant}_other")
        expected = [c.id for c in sorted(cands, key=lambda c: (c.score, c.id), reverse=True)]
        self.assertEqual(self.store.count_pending(self.tenant, DOMAIN), 137)
        for size in (1, 13, 137, 500):
            walked, after = [], None
            while True:
                page = self.store.pending_page(self.tenant, DOMAIN, size, after)
                walked += [c.id for c in page]
                if len(page) < size:
                    break
                after = (page[-1].score, page[-1].id)
            self.assertEqual(walked, expected, f"page size {size}")
        self.assertTrue(not set(expected) & {c.id for c in foreign})
        self.store.update_candidate_status(expected[40], self.tenant, DOMAIN, "rejected", "steward:x", now_iso())
        self.assertEqual(self.store.count_pending(self.tenant, DOMAIN), 136)
        self.assertNotIn(expected[40], [c.id for c in self.store.pending_page(self.tenant, DOMAIN, 500)])

    def test_the_entity_pages_and_the_record_batch(self):
        ids = [self.ingest(f"p{i}", {"first_name": f"Name{i}", "last_name": f"Surname{i}", "email": f"p{i}@example.com", "city": f"City{i}"})["record_id"] for i in range(40)]
        self.ingest("dup", {"first_name": "Name3", "last_name": "Surname3", "email": "p3@example.com", "city": "City3"}, system="src2")
        every = sorted(e.id for e in self.store.list_entities(self.tenant, DOMAIN))
        self.assertEqual(len(every), 40)
        for size in (1, 9, 40, 500):
            walked, after = [], None
            while True:
                page = self.store.entities_page(self.tenant, DOMAIN, size, after_id=after)
                walked += [e.id for e in page]
                if len(page) < size:
                    break
                after = page[-1].id
            self.assertEqual(walked, every, f"page size {size}")
        page = self.store.entities_page(self.tenant, DOMAIN, 100)
        self.assertEqual(max(len(e.source_record_ids) for e in page), 2)
        one = self.store.get_entity(page[0].id, self.tenant, DOMAIN)
        self.assertEqual((sorted(page[0].source_record_ids), page[0].attributes), (sorted(one.source_record_ids), one.attributes))
        self.assertEqual(self.store.entities_page(self.tenant, DOMAIN, 10, status="merged"), [])
        got = self.store.get_source_records_batch(self.tenant, DOMAIN, ids[:30] + ["rec_nope"] + ids[:5])
        self.assertEqual(set(got), set(ids[:30]))
        self.assertEqual(self.store.get_source_records_batch(f"{self.tenant}_other", DOMAIN, ids[:3]), {})     # another tenant's key sees nothing


class TestSqliteContract(Contract, unittest.TestCase):
    def make_store(self):
        from app.db import Store
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        self.path = os.path.join(tmp, "contract.db")
        return Store(self.path), "acme_contract"

    def _query(self, sql):
        conn = sqlite3.connect(self.path)
        try:
            return [r[0] for r in conn.execute(sql)]
        finally:
            conn.close()

    def index_names(self):
        return set(self._query("SELECT name FROM sqlite_master WHERE type = 'index'"))

    def run_sql(self, sql):
        conn = sqlite3.connect(self.path)
        conn.execute(sql)
        conn.commit()
        conn.close()

    def reopen(self):
        from app.db import Store
        Store(self.path)

    def migration_versions(self):
        return self._query("SELECT version FROM schema_migrations ORDER BY version")


@unittest.skipUnless(HAVE_PSYCOPG2 and TEST_DB_URL, "Set TEST_DATABASE_URL to a SCRATCH Postgres database and `pip install psycopg2-binary` to run the contract on Postgres.")
class TestPostgresContract(Contract, unittest.TestCase):
    def make_store(self):
        from app.db_postgres import PostgresStore
        return PostgresStore(TEST_DB_URL), f"contract_{uuid.uuid4().hex[:10]}"

    def _query(self, sql):
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return [list(r.values())[0] if hasattr(r, "values") else r[0] for r in cur.fetchall()]

    def index_names(self):
        return set(self._query("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"))

    def run_sql(self, sql):
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute(sql)

    def reopen(self):
        from app.db_postgres import PostgresStore
        PostgresStore(TEST_DB_URL)

    def migration_versions(self):
        return self._query("SELECT version FROM schema_migrations ORDER BY version")

    def test_the_queue_and_entity_queries_can_use_their_indexes(self):
        """A tiny table makes Postgres prefer a sequential scan whatever the indexes are, so switch that off to ask the real question: CAN these queries use them?
        (Whether the planner WILL on a big table is for EXPLAIN ANALYZE on your own data; see the runbook.)"""
        self.pending_candidates(30)
        with self.store._conn(tenant_id=self.tenant) as conn, conn.cursor() as cur:
            cur.execute("SET LOCAL enable_seqscan = off")
            cur.execute("EXPLAIN SELECT * FROM match_candidates WHERE tenant_id = %s AND domain = %s AND status = 'pending' AND (score, id) < (%s, %s) "
                        "ORDER BY score DESC, id DESC LIMIT %s", (self.tenant, DOMAIN, 0.7, "x", 51))
            queue_plan = " ".join(str(list(r.values())[0] if hasattr(r, "values") else r[0]) for r in cur.fetchall())
            cur.execute('EXPLAIN SELECT entity_id FROM "entity_customer" WHERE tenant_id = %s AND entity_id > %s ORDER BY entity_id LIMIT %s', (self.tenant, "ent_5", 51))
            entity_plan = " ".join(str(list(r.values())[0] if hasattr(r, "values") else r[0]) for r in cur.fetchall())
        self.assertIn("idx_match_candidates_pending", queue_plan, queue_plan)
        self.assertNotIn("Sort", queue_plan, queue_plan)
        self.assertIn("idx_entity_customer_tenant_entity", entity_plan, entity_plan)


if __name__ == "__main__":
    unittest.main()
