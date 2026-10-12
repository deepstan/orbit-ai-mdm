"""
The audit page and the monitor count in SQL, and attribute each audit to who MADE the merge (fixed when it was flagged).

Run with:  python3 -m unittest tests.test_audit_sql -v
"""
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_stewardship import Audited, DOMAIN, TENANT  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app import monitor  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import MatchCandidate  # noqa: E402
from app.stewardship import audit_queue, audit_summary, give_verdict  # noqa: E402
from app.unmerge import unmerge_pair  # noqa: E402


class TestWhoMadeTheMerge(Audited):
    def test_a_promotion_a_person_unmerged_still_counts_against_the_model(self):
        """The bug this guards: unmerging rewrites the candidate's decided_by, so attributing by it would have counted the model's mistake against the rule score."""
        give_verdict(self.store, TENANT, DOMAIN, self.bc, "wrong", "jane", unmerge=True)            # B-C was made by the model
        self.assertEqual(self.store.get_candidate(self.bc, TENANT, DOMAIN).decided_by, "unmerge:jane")   # the candidate no longer says so ...
        counts = self.store.audit_counts(TENANT, DOMAIN)
        self.assertEqual((counts["promoted"]["audited"], counts["promoted"]["wrong"]), (1, 1))       # ... the audit does
        self.assertEqual((counts["rule"]["audited"], counts["rule"]["wrong"]), (0, 0))
        summary = audit_summary(self.store, TENANT, DOMAIN)
        self.assertEqual((summary["strata"]["promoted"]["wrong"], summary["strata"]["rule"]["wrong"]), (1, 0))

    def test_the_monitor_names_the_unmerged_promotion_among_the_pairs_to_fix(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.bc, "jane")
        r = monitor.assess(self.store, TENANT, DOMAIN, 0.0005)
        self.assertEqual(r["strata"]["promoted"]["wrong_pairs"], [self.bc])
        self.assertEqual(r["strata"]["rule"]["wrong_pairs"], [])
        self.assertEqual(monitor.assess(self.store, TENANT, DOMAIN, 0.0005, with_pairs=False)["strata"]["promoted"]["wrong_pairs"], [])

    def test_the_maker_is_recorded_when_the_merge_is_flagged(self):
        by = {a["candidate_id"]: a["made_by"] for a in self.store.list_merge_audits(TENANT, DOMAIN)}
        self.assertEqual(by, {self.ab: "rule", self.bc: "promoted"})


class TestTheMigration(Audited):
    def test_it_fills_in_who_made_the_audits_that_already_existed_and_can_run_again(self):
        conn = sqlite3.connect(self.path)
        conn.execute("UPDATE merge_audits SET made_by = NULL")                                          # a database from before the column was filled
        conn.execute("DELETE FROM schema_migrations WHERE version = 15")
        conn.commit()
        conn.close()
        store = Store(self.path)                                                                         # opening it applies migration 15 again: the column exists, so it only fills
        by = {a["candidate_id"]: a["made_by"] for a in store.list_merge_audits(TENANT, DOMAIN)}
        self.assertEqual(by, {self.ab: "rule", self.bc: "promoted"})
        Store(self.path)                                                                                 # and once more: nothing changes, nothing fails
        self.assertEqual({a["candidate_id"]: a["made_by"] for a in store.list_merge_audits(TENANT, DOMAIN)}, by)


class TestTheSqlAgreesWithAPlainTally(unittest.TestCase):
    def test_counts_wrong_pairs_and_the_pending_page_on_random_data(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        store = Store(os.path.join(tmp, "t.db"))
        rng = random.Random(11)
        cands = []
        for i in range(400):
            cands.append(MatchCandidate(record_a_id=f"a{i}", record_b_id=f"b{i}", tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy", status="confirmed",
                                        decided_by=ls.PROMOTED_BY if rng.random() < 0.3 else "system:bulk_load", decided_at=f"2026-01-01T00:{i // 60:02d}:{i % 60:02d}Z"))
        store.save_candidates_batch(cands)
        flagged = [c for c in cands if rng.random() < 0.5]
        for c in flagged:   # flag in a few batches with distinct timestamps, so the order of the queue is well defined
            store.flag_merge_audits(TENANT, DOMAIN, [c.id])
        verdicts = {}
        for c in flagged:
            v = rng.choice([None, None, "correct", "wrong"])
            if v:
                store.record_merge_audit(c.id, TENANT, DOMAIN, v, "jane")
                verdicts[c.id] = v
        for c in rng.sample(flagged, 15):                                                                # a few flagged merges later stop being merges (unmerged elsewhere)
            store.update_candidate_status(c.id, TENANT, DOMAIN, "rejected", "unmerge:sam", "2026-02-01T00:00:00Z")
        made = {c.id: "promoted" if c.decided_by == ls.PROMOTED_BY else "rule" for c in cands}
        expected = {k: {"audited": 0, "wrong": 0} for k in ("rule", "promoted")}
        waiting = 0
        for c in flagged:
            if c.id in verdicts:
                expected[made[c.id]]["audited"] += 1
                expected[made[c.id]]["wrong"] += verdicts[c.id] == "wrong"
            else:
                waiting += 1
        got = store.audit_counts(TENANT, DOMAIN)
        self.assertEqual({k: got[k] for k in ("rule", "promoted")}, expected)
        self.assertEqual(got["waiting"], waiting)
        self.assertEqual(sorted(store.audited_wrong(TENANT, DOMAIN)), sorted((c, made[c]) for c, v in verdicts.items() if v == "wrong"))
        for limit in (1, 7, 10_000):
            page = store.pending_audits(TENANT, DOMAIN, limit)
            statuses = {c.id: c.status for c in store.list_candidates(TENANT, DOMAIN, status="confirmed")}
            should = [c for c in flagged if c.id not in verdicts and c.id in statuses]
            should.sort(key=lambda c: (made[c.id] != "promoted", c.id))                                   # (the page orders by flag time, then id: compare as sets of kinds + length)
            self.assertEqual(len(page), min(limit, len(should)))
            kinds = [p["made_by"] for p in page]
            self.assertEqual(kinds, sorted(kinds, key=lambda k: k != "promoted"))                         # the model's promotions come first
            self.assertTrue(all(p["candidate_id"] in statuses for p in page))                             # and none is a merge that no longer exists
            self.assertEqual(len({p["candidate_id"] for p in page}), len(page))
            if limit >= len(should):
                self.assertEqual({p["candidate_id"] for p in page}, {c.id for c in should})


class TestTheMergeTotalIsCachedBrieflyAndNothingElseIs(Audited):
    def test_the_total_is_counted_once_within_the_ttl_and_again_after_it(self):
        from app import stewardship
        clock = [100.0]
        calls = []
        real = Store.count_auto_merges

        def counting(store, *a, **k):
            calls.append(1)
            return real(store, *a, **k)
        with mock.patch.dict(os.environ, {"MDM_SUMMARY_CACHE_SECONDS": "30"}), mock.patch.object(Store, "count_auto_merges", counting):
            for t in (100.0, 110.0, 129.0):
                clock[0] = t
                stewardship.merge_totals(self.store, TENANT, DOMAIN, clock=lambda: clock[0])
            self.assertEqual(len(calls), 1)                                  # three page loads inside 30 s: one count
            clock[0] = 131.0
            stewardship.merge_totals(self.store, TENANT, DOMAIN, clock=lambda: clock[0])
            self.assertEqual(len(calls), 2)                                  # and a fresh one once it is stale
        with mock.patch.dict(os.environ, {"MDM_SUMMARY_CACHE_SECONDS": "0"}), mock.patch.object(Store, "count_auto_merges", counting):
            stewardship.merge_totals(self.store, TENANT, DOMAIN)
            stewardship.merge_totals(self.store, TENANT, DOMAIN)
            self.assertEqual(len(calls), 4)                                  # 0 turns the cache off

    def test_a_verdict_shows_in_the_summary_at_once_even_with_the_cache_on(self):
        with mock.patch.dict(os.environ, {"MDM_SUMMARY_CACHE_SECONDS": "3600"}):
            before = audit_summary(self.store, TENANT, DOMAIN)
            give_verdict(self.store, TENANT, DOMAIN, self.ab, "wrong", "jane")
            after = audit_summary(self.store, TENANT, DOMAIN)
        self.assertEqual((before["waiting"], before["strata"]["rule"]["wrong"]), (2, 0))
        self.assertEqual((after["waiting"], after["strata"]["rule"]["wrong"]), (1, 1))      # the audit counts are live; only the merge total is cached

    def test_the_rollback_check_never_uses_the_cache(self):
        calls = []
        real = Store.count_auto_merges
        with mock.patch.dict(os.environ, {"MDM_SUMMARY_CACHE_SECONDS": "3600"}), mock.patch.object(Store, "count_auto_merges", lambda s, *a, **k: (calls.append(1), real(s, *a, **k))[1]):
            monitor.assess(self.store, TENANT, DOMAIN, 0.0005)
            monitor.assess(self.store, TENANT, DOMAIN, 0.0005)
        self.assertEqual(len(calls), 2)


class TestTheCostDoesNotGrowWithTheAudits(Audited):
    def test_neither_the_page_nor_the_monitor_ever_reads_the_whole_audit_table(self):
        with mock.patch.object(Store, "list_merge_audits", side_effect=AssertionError("loaded every audit row")):
            audit_summary(self.store, TENANT, DOMAIN)
            audit_queue(self.store, TENANT, DOMAIN)
            monitor.assess(self.store, TENANT, DOMAIN, 0.0005)
            monitor.check_scorer(self.store, TENANT, DOMAIN)

    def test_the_queue_reads_only_a_page_of_candidates(self):
        calls = []
        real = Store.get_candidate

        def counting(store, *a, **k):
            calls.append(1)
            return real(store, *a, **k)
        with mock.patch.object(Store, "get_candidate", counting):
            audit_queue(self.store, TENANT, DOMAIN, limit=1)
        self.assertLessEqual(len(calls), 3)   # one for the page, plus what unmerge planning reads: never one per waiting audit


if __name__ == "__main__":
    unittest.main()
