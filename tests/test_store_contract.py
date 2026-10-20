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
from app import audit  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app import monitor  # noqa: E402
from app.bulk import bulk_load  # noqa: E402
from app.models import ApiKeyRecord, Entity, MatchCandidate, Recommendation, UsageEvent, now_iso  # noqa: E402
from app.pipeline import ingest_record, merge_two_records  # noqa: E402
from app.sql_dialect import get_dialect  # noqa: E402
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


    # ---------------------------------------------------------------- tenant isolation, which must NOT depend on the database's row-level security
    # Another database may have no such feature, or a different one. So every read is asked for ANOTHER tenant's data using that tenant's REAL ids, and must come back empty.
    # The tenant-taking methods the isolation tests do NOT exercise. A visible list of debt: a new method must be probed or added here with a reason, and removing a name is progress.
    NOT_YET_PROBED = frozenset({
        # three, each needing a state this test cannot reach by the ordinary routes:
        "separation_violations",        # reports records a person separated that are NOW in one entity: it returns something only if the engine's own guarantee has been broken
        "apply_onboarding",             # applies a whole legacy-engine onboarding plan (reassignments, entities, lineage, history, updates): it needs a plan that engine produced
        "rollback_bulk",                # deletes what ONE legacy bulk load (a token) wrote: it needs such a load to roll back
    })

    CREATES_UNDER_THE_ACTING_TENANT = frozenset({"create_run", "save_blocking_keys"})      # these write the ACTING tenant's own rows by design (a run of its own, keys of its own)

    @staticmethod
    def empty(x):
        if x is None or x is False:
            return True
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            return x == 0
        if isinstance(x, (list, tuple, set, frozenset)):
            return all(Contract.empty(v) for v in x)                                       # a tuple of empty answers (one per variant of a call) is empty
        if isinstance(x, dict):
            return all(Contract.empty(v) for v in x.values())
        return False

    def seed_one_tenant(self):
        ids = self.chain()
        bc = self.pair(ids, "B", "C")
        ab = self.pair(ids, "A", "B")
        self.pending = self.pending_candidates(5)
        history = {"first_name": "Hilda", "last_name": "Zander", "email": "hilda@z.example"}
        h = self.ingest("H", history)["record_id"]
        self.ingest("H", {**history, "city": "Macon"})                                      # a changed record keeps its earlier version in the record history
        audit.log_action(self.store, self.tenant, action="probe", actor="test", domain=DOMAIN, details={})
        unmerge_pair(self.store, self.tenant, DOMAIN, ab, "jane", "different people")
        self.rec_candidate = self.pending[1].id                                              # an item with a recommendation waiting (the first pending one is used by a write probe)
        self.store.save_recommendation(Recommendation(candidate_id=self.rec_candidate, recommendation="confirm", confidence=0.9, reasons=["probe"], agent="agent:probe", model="m", prompt_version="v",
                                                      tenant_id=self.tenant, domain=DOMAIN))
        # what the audit, key, usage, run and processing-state methods need to have something to return
        tag = uuid.uuid4().hex[:8]
        system_merge = lambda n: MatchCandidate(id=f"cand_aud_{tag}_{n}", record_a_id=ids["B"], record_b_id=ids["C"], tenant_id=self.tenant, domain=DOMAIN, score=0.95, match_type="deterministic",
                                                status="confirmed", decided_by="system:rule", decided_at=now_iso())
        self.audit_pending, self.audit_wrong, self.audit_unflagged = system_merge("pending"), system_merge("wrong"), system_merge("unflagged")
        self.store.save_candidates_batch([self.audit_pending, self.audit_wrong, self.audit_unflagged])
        self.store.flag_merge_audits(self.tenant, DOMAIN, [self.audit_pending.id, self.audit_wrong.id])        # two merges a person is asked to check, one of them judged wrong; a third nobody asked about
        self.store.record_merge_audit(self.audit_wrong.id, self.tenant, DOMAIN, "wrong", "steward:probe")
        key = ApiKeyRecord(tenant_id=self.tenant, role="data_steward", key_hash=f"hash_{tag}", label="probe")
        self.store.save_api_key(key)
        self.key_id = key.id
        self.store.record_usage_event(UsageEvent(tenant_id=self.tenant, event_type="probe", domain=DOMAIN))
        self.store.create_run(f"run_{tag}", self.tenant, DOMAIN, "batch", "probe", now_iso())
        self.store.set_processing_status([h], self.tenant, DOMAIN, "staged")                                    # one record landed but not matched: unfinished work
        self.seeded = {"ids": ids, "ab": ab, "bc": bc, "h": h}
        return ids, ab, bc

    def probes(self):
        ids, ab, bc = self.seed_one_tenant()
        s, rids = self.store, list(ids.values())
        rid, e_a, e_bc = ids["A"], self.entity_of(ids["A"]), self.entity_of(ids["B"])
        keys = set()
        for ks in s.blocking_keys_of(self.tenant, DOMAIN, rids).values():
            keys |= set(ks)
        self.assertTrue(keys, "the probe data has blocking keys")
        pairs = [("src", n) for n in ids]
        return {
            "get_source_record": lambda t: s.get_source_record(rid, t, DOMAIN),
            "list_source_records": lambda t: (s.list_source_records(t, DOMAIN), s.list_source_records(t, DOMAIN, e_bc)),            # with and without the entity filter
            "get_source_records_batch": lambda t: s.get_source_records_batch(t, DOMAIN, rids),
            "find_source_record": lambda t: s.find_source_record(t, DOMAIN, "src", "A"),
            "find_source_records_by_keys": lambda t: s.find_source_records_by_keys(t, DOMAIN, pairs),
            "get_record_entities": lambda t: s.get_record_entities(t, DOMAIN, rids),
            "has_source_records": lambda t: s.has_source_records(t, DOMAIN),
            "count_tenant_records": lambda t: s.count_tenant_records(t, DOMAIN),
            "get_entity": lambda t: s.get_entity(e_bc, t, DOMAIN),
            "get_entities_batch": lambda t: s.get_entities_batch(t, DOMAIN, [e_a, e_bc]),
            "entities_page": lambda t: (s.entities_page(t, DOMAIN, 100), s.entities_page(t, DOMAIN, 100, after_id="!"), s.entities_page(t, DOMAIN, 100, status="active")),
            "list_entities": lambda t: (s.list_entities(t, DOMAIN), s.list_entities(t, DOMAIN, "active")),
            "list_records_of_entities": lambda t: s.list_records_of_entities(t, DOMAIN, [e_a, e_bc]),
            "pending_page": lambda t: s.pending_page(t, DOMAIN, 50),
            "count_pending": lambda t: s.count_pending(t, DOMAIN),
            "list_candidates": lambda t: (s.list_candidates(t, DOMAIN, None), s.list_candidates(t, DOMAIN), s.list_candidates(t, DOMAIN, "confirmed")),
            "get_candidate": lambda t: s.get_candidate(bc, t, DOMAIN),
            "list_candidate_pairs": lambda t: s.list_candidate_pairs(t, DOMAIN, ids["B"]),
            "confirmed_pairs_among": lambda t: s.confirmed_pairs_among(t, DOMAIN, [ids["B"], ids["C"]]),
            "list_lineage": lambda t: s.list_lineage(e_bc, t, DOMAIN),
            "list_record_history": lambda t: s.list_record_history(self.seeded["h"], t, DOMAIN),
            "list_audit_log": lambda t: (s.list_audit_log(t), s.list_audit_log(t, DOMAIN)),
            "get_last_audit_entry": lambda t: s.get_last_audit_entry(t),
            "separation_pairs": lambda t: s.separation_pairs(t, DOMAIN),
            "has_separations": lambda t: s.has_separations(t, DOMAIN),
            "separated_partners": lambda t: s.separated_partners(t, DOMAIN, rid),
            "entities_separated": lambda t: s.entities_separated(t, DOMAIN, e_a, e_bc),
            "blocking_bucket_sizes": lambda t: s.blocking_bucket_sizes(t, DOMAIN, keys),
            "blocking_keys_of": lambda t: s.blocking_keys_of(t, DOMAIN, rids),
            "blocking_members": lambda t: s.blocking_members(t, DOMAIN, keys),
            "find_candidate_record_ids": lambda t: s.find_candidate_record_ids(t, DOMAIN, keys, "no-such-record"),
            "get_staged_many": lambda t: s.get_staged_many(t, DOMAIN, rids),
            "get_matching_many": lambda t: s.get_matching_many(t, DOMAIN, rids),
            "count_auto_merges": lambda t: s.count_auto_merges(t, DOMAIN),
            "human_labeled_candidates": lambda t: s.human_labeled_candidates(t, DOMAIN),
            "list_recommendations_for": lambda t: s.list_recommendations_for(t, DOMAIN, self.rec_candidate),
            "recommendations_page": lambda t: (s.recommendations_page(t, DOMAIN, 50), s.recommendations_page(t, DOMAIN, 50, outcome="open")),
            "recommendation_counts": lambda t: s.recommendation_counts(t, DOMAIN),
            "iter_blocking_keys": lambda t: list(s.iter_blocking_keys(t, DOMAIN, 3)),
            "count_blocking_keys": lambda t: s.count_blocking_keys(t, DOMAIN),
            "audit_counts": lambda t: s.audit_counts(t, DOMAIN),
            "audited_wrong": lambda t: s.audited_wrong(t, DOMAIN),
            "pending_audits": lambda t: s.pending_audits(t, DOMAIN, 10),
            "list_merge_audits": lambda t: (s.list_merge_audits(t, DOMAIN), s.list_merge_audits(t, DOMAIN, pending_only=True)),
            "list_api_keys": lambda t: s.list_api_keys(t),
            "list_usage_events": lambda t: s.list_usage_events(t),
            "list_runs": lambda t: s.list_runs(DOMAIN, t),
            "iter_raw_attributes": lambda t: [batch for batch in s.iter_raw_attributes(DOMAIN, t)],
            "unfinished_in": lambda t: s.unfinished_in(t, DOMAIN),
        }

    def write_probes(self):
        """Writes that must do NOTHING when made under another tenant, with the real ids of this one. Call after probes()."""
        s, ids, bc = self.store, self.seeded["ids"], self.seeded["bc"]
        rid, e_bc, pending_record = ids["B"], self.entity_of(ids["B"]), self.pending[0].record_a_id
        return {
            "delete_entity": lambda t: s.delete_entity(e_bc, t, DOMAIN),
            "delete_source_record": lambda t: s.delete_source_record(rid, t, DOMAIN),
            "update_candidate_status": lambda t: s.update_candidate_status(bc, t, DOMAIN, "rejected", "intruder", now_iso()),
            "set_record_entity": lambda t: s.set_record_entity(rid, t, DOMAIN, "an-entity-of-the-intruder"),
            "supersede_pending_candidates": lambda t: s.supersede_pending_candidates(t, DOMAIN, pending_record),
            "delete_staged": lambda t: s.delete_staged(rid, t, DOMAIN),
            "delete_blocking_keys": lambda t: s.delete_blocking_keys(rid, t, DOMAIN),
            "delete_record_history": lambda t: s.delete_record_history(self.seeded["h"], t, DOMAIN),
            "update_source_record": lambda t: s.update_source_record(rid, t, DOMAIN, {"first_name": "Intruder"}, "intruder-hash", 99),
            "set_processing_status": lambda t: s.set_processing_status([rid], t, DOMAIN, "failed", "intruder"),
            "record_recommendation_outcomes": lambda t: s.record_recommendation_outcomes(t, DOMAIN, self.rec_candidate, "reject", "intruder", now_iso()),
            "delete_recommendations": lambda t: s.delete_recommendations(t, DOMAIN, [self.rec_candidate]),
            "create_run": lambda t: s.create_run(f"run_intruder_{uuid.uuid4().hex[:6]}", t, DOMAIN, "batch", "intruder", now_iso()),
            "flag_merge_audits": lambda t: s.flag_merge_audits(t, DOMAIN, [self.audit_unflagged.id]),                  # must not flag the owner's merge
            "record_merge_audit": lambda t: s.record_merge_audit(self.audit_pending.id, t, DOMAIN, "wrong", "steward:intruder"),     # must not judge the owner's flagged merge
            "sample_merge_audit": lambda t: s.sample_merge_audit(t, DOMAIN, 1.0, seed=1),                             # sampling every automatic merge must not reach the owner's
            "record_processing_failure": lambda t: s.record_processing_failure(self.seeded["h"], t, DOMAIN, "intruder"),
            "revoke_api_key": lambda t: s.revoke_api_key(self.key_id, t, now_iso()),
            "save_blocking_keys": lambda t: s.save_blocking_keys(t, DOMAIN, self.seeded["ids"]["A"], {"k:intruder"}),
        }

    def fingerprint(self, probes):
        return {name: repr(call(self.tenant)) for name, call in probes.items()}

    def test_no_write_made_under_another_tenant_changes_anything(self):
        other = self.tenant + "_other"
        reads = self.probes()
        writes = self.write_probes()
        before = self.fingerprint(reads)
        other_before = {n: repr(read(other)) for n, read in reads.items()}
        for name, call in writes.items():
            try:
                call(other)
            except Exception:                                                              # refusing is as good as ignoring: what matters is that nothing changed
                pass
            self.assertEqual(self.fingerprint(reads), before, f"{name}, made under another tenant, changed this tenant's data")
            now_other = {n: repr(read(other)) for n, read in reads.items()}
            if name not in self.CREATES_UNDER_THE_ACTING_TENANT:             # and it must not leave junk behind UNDER the other tenant either: a row for this tenant's id, filed under another, can block this tenant's own later write
                changed = sorted(n for n in now_other if now_other[n] != other_before[n])
                self.assertEqual(changed, [], f"{name}, made under another tenant with this tenant's ids, left something behind under that other tenant")
            other_before = now_other
        # the control: the same KIND of write by the OWNER does change what the fingerprint sees, so "unchanged" above means something
        writes["update_candidate_status"](self.tenant)
        self.assertNotEqual(self.fingerprint(reads), before, "the fingerprint cannot see a status change, so it proved nothing")

    def test_another_tenants_entities_with_lower_ids_do_not_use_up_this_tenants_pages(self):
        """The entity pages select ids and THEN load them tenant-filtered. If the first step ignored the tenant, other tenants' ids would fill the page limit and be dropped, and this tenant
        would see short or empty pages. Nothing leaks, but it is wrong. The other tenant's ids here sort BEFORE every id of this one."""
        other = self.tenant + "_other"
        self.seed_one_tenant()
        mine = sorted(e.id for e in self.store.list_entities(self.tenant, DOMAIN))
        self.assertGreaterEqual(len(mine), 3)
        tag = uuid.uuid4().hex[:8]
        for i in range(8):
            self.store.save_entity(Entity(id=f"!!{tag}-{i}", tenant_id=other, domain=DOMAIN, status="active", attributes={"first_name": "Zed"}, source_record_ids=[],
                                          created_at=now_iso(), updated_at=now_iso()))
        self.assertEqual(sorted(e.id for e in self.store.entities_page(self.tenant, DOMAIN, len(mine))), mine)       # a page of exactly this tenant's size
        walked, after = [], None
        while True:                                                                                                  # and walked one at a time
            page = self.store.entities_page(self.tenant, DOMAIN, 1, after_id=after)
            if not page:
                break
            walked.append(page[0].id)
            after = page[0].id
        self.assertEqual(walked, mine)
        self.assertEqual(sorted(e.id for e in self.store.list_entities(self.tenant, DOMAIN)), mine)
        self.assertEqual(sorted(e.id for e in self.store.list_entities(self.tenant, DOMAIN, "active")), mine)
        self.assertEqual(len(self.store.list_entities(other, DOMAIN)), 8)

    def test_no_read_returns_another_tenants_data_even_given_the_right_ids(self):
        other = self.tenant + "_other"
        for name, call in self.probes().items():
            self.assertFalse(self.empty(call(self.tenant)), f"{name}: even the owner gets nothing back, so this probe proves nothing")
            self.assertTrue(self.empty(call(other)), f"{name} returned another tenant's data")

    def test_every_store_method_that_takes_a_tenant_is_probed_or_listed_as_not_yet_probed(self):
        import inspect
        tenant_methods = {n for n, f in inspect.getmembers(type(self.store), inspect.isfunction) if not n.startswith("_") and "tenant_id" in inspect.signature(f).parameters}
        reads = self.probes()
        probed = set(reads) | set(self.write_probes())
        self.assertEqual(sorted(probed - tenant_methods), [], "a probe names a method that does not take a tenant (or no longer exists)")
        self.assertEqual(sorted(tenant_methods - probed - self.NOT_YET_PROBED), [],
                         "a store method takes a tenant but is neither probed for isolation nor listed in NOT_YET_PROBED: probe it, or list it with a reason")
        self.assertEqual(sorted(self.NOT_YET_PROBED - tenant_methods), [], "NOT_YET_PROBED lists a method that no longer exists or no longer takes a tenant")
        self.assertEqual(sorted(self.NOT_YET_PROBED & probed), [], "NOT_YET_PROBED lists a method that IS probed")

    # ---------------------------------------------------------------- the dialect helpers the kernel's tables will use, run on THIS backend
    def test_the_dialect_helpers_behave_on_this_backend(self):
        d = get_dialect(self.dialect_name)
        table = f"probe_{uuid.uuid4().hex[:10]}"
        self.exec_sql(f"CREATE TABLE {table} (k TEXT PRIMARY KEY, v INTEGER, w INTEGER, version INTEGER NOT NULL DEFAULT 0)")
        self.addCleanup(lambda: self.exec_sql(f"DROP TABLE IF EXISTS {table}"))
        read = lambda k: self.exec_sql(d.sql(f"SELECT v, w, version FROM {table} WHERE k = ?"), (k,))[0]
        up = d.upsert(table, ["k", "v", "w"], ["k"])
        self.exec_sql(up, ("a", 1, 1)); self.exec_sql(up, ("a", 2, 2))
        self.assertEqual(read("a"), [(2, 2, 0)])                                                   # inserted, then updated
        self.exec_sql(d.upsert(table, ["k", "v", "w"], ["k"], update=[]), ("a", 9, 9))
        self.assertEqual(read("a"), [(2, 2, 0)])                                                   # DO NOTHING
        for i in range(40):                                                                         # rows with many ties, for paging
            self.exec_sql(d.sql(f"INSERT INTO {table} (k, v, w) VALUES (?, ?, ?)"), (f"k{i:03d}", i % 3, 0))
        sql, params = d.in_list("k", ["k001", "k002", "zz"])
        self.assertEqual(sorted(r[0] for r in self.exec_sql(f"SELECT k FROM {table} WHERE {sql}", params)[0]), ["k001", "k002"])
        sql, params = d.in_list("k", [])
        self.assertEqual(self.exec_sql(f"SELECT COUNT(*) FROM {table} WHERE {sql}", params)[0][0][0], 0)
        want = [r[0] for r in self.exec_sql(f"SELECT k FROM {table} ORDER BY v DESC, k DESC")[0]]
        seen, after = [], None
        while True:                                                                                 # keyset paging, one row-value comparison, over ties
            where = "" if after is None else "WHERE " + d.keyset_after(["v", "k"], descending=True)
            page = self.exec_sql(f"SELECT k, v FROM {table} {where} ORDER BY v DESC, k DESC {d.limit_clause()}", ((after[1], after[0]) if after else ()) + (7,))[0]
            if not page:
                break
            seen += [r[0] for r in page]
            after = page[-1]
        self.assertEqual(seen, want)
        sql = d.versioned_update(table, ["v"], ["k"])                                              # two writers read version 0; only the first may write
        first, second = self.exec_sql(sql, (10, "a", 0))[1], self.exec_sql(sql, (20, "a", 0))[1]
        self.assertEqual((first, second), (1, 0))
        self.assertEqual(read("a"), [(10, 2, 1)])
        rows = self.exec_sql(f"SELECT k FROM {table} ORDER BY k {d.limit_clause()}{d.claim_suffix()}", (3,))[0]       # a claiming read is valid SQL here
        self.assertEqual(len(rows), 3)


    # ---------------------------------------------------------------- the unit of work: store.batch() and store.savepoint()
    def test_a_batch_commits_at_the_end_and_a_failed_one_leaves_nothing(self):
        mine = lambda: sorted(r.source_record_id for r in self.store.list_source_records(self.tenant, DOMAIN))
        with self.store.batch(DOMAIN):
            self.ingest("U1", A)
            self.assertEqual(mine(), ["U1"])                                  # inside the batch it reads its own writes
        self.assertEqual(mine(), ["U1"])
        with self.assertRaises(RuntimeError):
            with self.store.batch(DOMAIN):
                self.ingest("U2", B)
                raise RuntimeError("boom")
        self.assertEqual(mine(), ["U1"])                                      # the failed batch left NOTHING, landing included

    def test_a_savepoint_undoes_only_its_own_block_and_the_batch_carries_on(self):
        with self.store.batch(DOMAIN):
            self.ingest("V1", A)
            with self.assertRaises(RuntimeError):
                with self.store.savepoint():
                    self.ingest("V2", B)
                    raise RuntimeError("boom")
            self.ingest("V3", C)
        self.assertEqual(sorted(r.source_record_id for r in self.store.list_source_records(self.tenant, DOMAIN)), ["V1", "V3"])

    def test_a_savepoint_outside_a_batch_does_nothing_and_batches_nest(self):
        with self.store.savepoint():
            self.assertFalse(self.store.in_batch())
        with self.store.batch(DOMAIN):
            with self.store.batch(DOMAIN):
                self.assertTrue(self.store.in_batch())
            self.assertTrue(self.store.in_batch())
        self.assertFalse(self.store.in_batch())

    def test_the_golden_table_check_cannot_run_inside_a_batch_and_says_how_to_fix_it(self):
        fresh, tenant = self.make_store()                                      # a store object that has not checked the golden table yet
        with self.assertRaises(RuntimeError) as caught:
            with fresh.batch():
                fresh.save_entity(Entity(attributes={}, status="active", tenant_id=tenant, domain=DOMAIN))
        self.assertIn("store.batch(domain=", str(caught.exception))
        with fresh.batch(DOMAIN):                                              # naming the domain checks the table BEFORE the transaction opens
            fresh.save_entity(Entity(attributes={}, status="active", tenant_id=tenant, domain=DOMAIN))
        self.assertEqual(len(fresh.list_entities(tenant, DOMAIN, "active")), 1)

    def test_the_lookup_of_held_records_gives_the_latest_for_each_key_across_systems_and_chunks(self):
        for i in range(30):
            self.ingest(f"K{i}", {**A, "email": f"k{i}@x.com"}, system="crm")
        self.ingest("K1", {**A, "email": "k1@other.com"}, system="erp")                    # the same id in ANOTHER system is another record
        keys = [("crm", f"K{i}") for i in range(30)] + [("erp", "K1"), ("crm", "NOPE"), ("erp", "NOPE")]
        held = self.store.find_source_records_by_keys(self.tenant, DOMAIN, keys)
        self.assertEqual(len(held), 31)                                                      # missing keys are simply absent
        self.assertEqual(held[("crm", "K1")].attributes["email"], "k1@x.com")
        self.assertEqual(held[("erp", "K1")].attributes["email"], "k1@other.com")
        many = self.store.find_source_records_by_keys(self.tenant, DOMAIN, [("crm", f"X{i}") for i in range(2500)] + [("crm", "K5")])    # more keys than one IN list may carry
        self.assertEqual(list(many), [("crm", "K5")])
        self.assertEqual(self.store.find_source_records_by_keys(self.tenant, DOMAIN, []), {})

    def test_new_records_keys_can_be_added_without_a_delete_and_old_ones_still_replaced(self):
        rec = self.ingest("F1", A)
        rid = rec["record_id"]
        keys = lambda: sorted(k for k in self.store.blocking_keys_of(self.tenant, DOMAIN, [rid]).get(rid, set()))
        before = keys()
        self.store.replace_blocking_keys_batch([(self.tenant, DOMAIN, rid, {"zz:one", "zz:two"})])               # replace: the old keys go
        self.assertEqual(keys(), ["zz:one", "zz:two"])
        self.store.replace_blocking_keys_batch([(self.tenant, DOMAIN, rid, {"zz:three"})], fresh=True)           # fresh: nothing is deleted, the new key is added
        self.assertEqual(keys(), ["zz:one", "zz:three", "zz:two"])
        self.assertTrue(before)


    # ---------------------------------------------------------------- agent recommendations
    def rec(self, candidate_id, recommendation="confirm", confidence=0.9, agent="agent:a", model="m", prompt_version="v1", tenant=None):
        return Recommendation(candidate_id=candidate_id, recommendation=recommendation, confidence=confidence, reasons=["r1"], evidence=["group_b.members"], agent=agent, model=model,
                              prompt_version=prompt_version, tenant_id=tenant or self.tenant, domain=DOMAIN)

    def test_a_recommendation_is_stored_replaced_by_the_same_agent_and_kept_apart_from_another(self):
        cid = self.pending_candidates(1)[0].id
        first = self.store.save_recommendation(self.rec(cid, "confirm", 0.9))
        again = self.store.save_recommendation(self.rec(cid, "reject", 0.6))                       # the same agent, model and prompt version: replaced
        self.store.save_recommendation(self.rec(cid, "needs_human", 0.3, prompt_version="v2"))      # another prompt version: a second opinion
        self.store.save_recommendation(self.rec(cid, "confirm", 0.8, agent="agent:b"))              # another agent
        self.assertEqual((again.id, again.recommendation, again.confidence), (first.id, "reject", 0.6))
        got = self.store.list_recommendations_for(self.tenant, DOMAIN, cid)
        self.assertEqual(sorted((r.agent, r.prompt_version, r.recommendation) for r in got), [("agent:a", "v1", "reject"), ("agent:a", "v2", "needs_human"), ("agent:b", "v1", "confirm")])
        self.assertEqual(got[0].reasons, ["r1"])
        self.assertEqual(got[0].evidence, ["group_b.members"])

    def test_the_table_itself_refuses_a_recommendation_that_is_not_one_and_a_confidence_outside_0_to_1(self):
        cid = self.pending_candidates(1)[0].id
        for bad in (self.rec(cid, "merge_them_all"), self.rec(cid, "confirm", 1.5), self.rec(cid, "confirm", -0.1)):
            with self.assertRaises(Exception):
                self.store.save_recommendation(bad)

    def test_a_decision_gives_every_open_recommendation_its_outcome_once(self):
        cid, other = (c.id for c in self.pending_candidates(2))
        self.store.save_recommendation(self.rec(cid, "confirm", agent="agent:a"))
        self.store.save_recommendation(self.rec(cid, "reject", agent="agent:b"))
        self.store.save_recommendation(self.rec(cid, "needs_human", agent="agent:c"))
        self.store.save_recommendation(self.rec(other, "confirm", agent="agent:a"))
        self.assertEqual(self.store.record_recommendation_outcomes(self.tenant, DOMAIN, cid, "confirm", "steward:Pat", now_iso()), 3)
        by_agent = {r.agent: (r.outcome, r.outcome_decision, r.outcome_by) for r in self.store.list_recommendations_for(self.tenant, DOMAIN, cid)}
        self.assertEqual(by_agent, {"agent:a": ("agreed", "confirm", "steward:Pat"), "agent:b": ("disagreed", "confirm", "steward:Pat"), "agent:c": ("human_decided", "confirm", "steward:Pat")})
        self.assertEqual([r.outcome for r in self.store.list_recommendations_for(self.tenant, DOMAIN, other)], [None])           # another item: untouched
        self.assertEqual(self.store.record_recommendation_outcomes(self.tenant, DOMAIN, cid, "reject", "steward:Lee", now_iso()), 0)   # decided once: a second decision changes nothing
        self.assertEqual({r.outcome for r in self.store.list_recommendations_for(self.tenant, DOMAIN, cid)}, {"agreed", "disagreed", "human_decided"})

    def test_the_page_walks_newest_first_exactly_once_and_filters_by_outcome(self):
        cands = self.pending_candidates(4)
        for i, c in enumerate(cands):
            self.store.save_recommendation(self.rec(c.id, agent=f"agent:{i}"))
        self.store.record_recommendation_outcomes(self.tenant, DOMAIN, cands[0].id, "confirm", "steward:Pat", now_iso())
        everything = self.store.recommendations_page(self.tenant, DOMAIN, 100)
        self.assertEqual(len(everything), 4)
        self.assertEqual([r.id for r in everything], [r.id for r in sorted(everything, key=lambda r: (r.created_at, r.id), reverse=True)])
        walked, after = [], None
        while True:
            page = self.store.recommendations_page(self.tenant, DOMAIN, 1, after=after)
            if not page:
                break
            walked.append(page[0].id)
            after = (page[0].created_at, page[0].id)
        self.assertEqual(walked, [r.id for r in everything])
        self.assertEqual(len(self.store.recommendations_page(self.tenant, DOMAIN, 100, outcome="open")), 3)
        self.assertEqual(len(self.store.recommendations_page(self.tenant, DOMAIN, 100, outcome="agreed")), 1)

    def test_the_counts_group_by_what_was_said_what_happened_and_how_sure(self):
        a, b = (c.id for c in self.pending_candidates(2))
        self.store.save_recommendation(self.rec(a, "confirm", 0.9))
        self.store.save_recommendation(self.rec(b, "reject", 0.4))
        self.store.record_recommendation_outcomes(self.tenant, DOMAIN, a, "confirm", "steward:Pat", now_iso())
        self.assertEqual(sorted(self.store.recommendation_counts(self.tenant, DOMAIN), key=str), sorted([("confirm", "agreed", 0.9, None, 1), ("reject", None, 0.4, None, 1)], key=str))

    def test_whether_the_person_had_opened_the_suggestion_is_kept_with_the_outcome_and_counted_apart(self):
        a, b, c = (x.id for x in self.pending_candidates(3))
        for cid in (a, b, c):
            self.store.save_recommendation(self.rec(cid, "confirm", 0.9))
        for cid, viewed in ((a, True), (b, False), (c, None)):
            self.store.record_recommendation_outcomes(self.tenant, DOMAIN, cid, "confirm", "steward:Pat", now_iso(), viewed)
        seen = {cid: self.store.list_recommendations_for(self.tenant, DOMAIN, cid)[0].outcome_seen for cid in (a, b, c)}
        self.assertEqual(seen, {a: True, b: False, c: None})                              # three different answers: opened it, did not, not known
        self.assertEqual(sorted(self.store.recommendation_counts(self.tenant, DOMAIN), key=str), sorted([("confirm", "agreed", 0.9, True, 1), ("confirm", "agreed", 0.9, False, 1),
                                                                                                          ("confirm", "agreed", 0.9, None, 1)], key=str))
        self.assertEqual(self.store.record_recommendation_outcomes(self.tenant, DOMAIN, a, "reject", "steward:Lee", now_iso(), False), 0)       # decided once: the first answer stands
        self.assertTrue(self.store.list_recommendations_for(self.tenant, DOMAIN, a)[0].outcome_seen)

    def test_the_index_can_be_read_a_page_at_a_time_in_key_order_distinct_and_counted(self):
        a, b, c = "rec_idx_a", "rec_idx_b", "rec_idx_c"                                           # the index is just keys against record ids
        entries = [(self.tenant, DOMAIN, a, {"k:b", "k:a", "k:shared"}), (self.tenant, DOMAIN, b, {"k:c", "k:shared"}), (self.tenant, DOMAIN, c, {"k:d"})]
        self.store.replace_blocking_keys_batch(entries)
        everything = [k for page in self.store.iter_blocking_keys(self.tenant, DOMAIN, 1_000) for k in page]
        mine = [k for k in everything if k.startswith("k:")]
        self.assertEqual(mine, ["k:a", "k:b", "k:c", "k:d", "k:shared"])                           # distinct (k:shared once), in key order
        paged = list(self.store.iter_blocking_keys(self.tenant, DOMAIN, 2))
        self.assertTrue(all(len(p) <= 2 for p in paged) and len(paged) > 1)                          # really a page at a time
        self.assertEqual([k for p in paged for k in p], everything)                                  # and the same keys in the same order whatever the page size
        self.assertGreaterEqual(self.store.count_blocking_keys(self.tenant, DOMAIN), 6)
        self.assertEqual(list(self.store.iter_blocking_keys("a-tenant-with-nothing", DOMAIN, 2)), [])
        self.assertEqual(self.store.count_blocking_keys("a-tenant-with-nothing", DOMAIN), 0)

    def test_deleting_recommendations_for_some_candidates_leaves_the_rest(self):
        a, b = (c.id for c in self.pending_candidates(2))
        self.store.save_recommendation(self.rec(a))
        self.store.save_recommendation(self.rec(b))
        self.assertEqual(self.store.delete_recommendations(self.tenant, DOMAIN, [a]), 1)
        self.assertEqual((len(self.store.list_recommendations_for(self.tenant, DOMAIN, a)), len(self.store.list_recommendations_for(self.tenant, DOMAIN, b))), (0, 1))
        self.assertEqual(self.store.delete_recommendations(self.tenant, DOMAIN, []), 0)
        self.assertEqual(self.store.delete_recommendations(self.tenant, DOMAIN, [f"c{i}" for i in range(1200)] + [b]), 1)         # more ids than one IN list may carry

class TestSqliteContract(Contract, unittest.TestCase):
    dialect_name = "sqlite"

    def exec_sql(self, sql, params=()):
        """(rows as tuples, rows changed) on a fresh connection."""
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.execute(sql, params)
            rows = [tuple(r) for r in cur.fetchall()] if cur.description else []
            conn.commit()
            return rows, cur.rowcount
        finally:
            conn.close()

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
    dialect_name = "postgres"

    def exec_sql(self, sql, params=()):
        """(rows as tuples, rows changed). NOT YET RUN against a server: this is the first thing to check if the dialect test fails there."""
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params if params else None)
            rows = [tuple(r.values()) if hasattr(r, "values") else tuple(r) for r in cur.fetchall()] if cur.description else []
            return rows, cur.rowcount

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
