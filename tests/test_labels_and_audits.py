"""
Label hygiene and merge audits. A model may learn only from what a PERSON decided: a steward's confirm or reject, and the verdict of someone who checked an
automatic merge. The system's own merges are stored as `confirmed` but are not labels.

Run with:  python3 -m unittest tests.test_labels_and_audits -v
"""
import os
import sqlite3
import sys
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_bulk_engine import TENANT  # noqa: E402
from test_onboarding import Onb  # noqa: E402
from app.active_learning import build_training_set  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate  # noqa: E402


class Labels(Onb):
    def setUp(self):
        super().setUp()
        # These tests are about sampling BY HAND. The engines flag 2% of automatic merges as they write them (MDM_AUDIT_RATE), which would pre-flag some: a test
        # that wants the automatic flagging sets the rate itself.
        patcher = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def loaded(self, scale=4):
        """A store with automatic merges AND review items, built by the regular engine so decided_by is what production writes."""
        rows = self.rows(scale=scale)
        store, path = self.store()
        for i, attrs in enumerate(rows):
            ingest_record(store, TENANT, self.domain, "src", f"r{i}", attrs)
        return store, path

    def ids(self, path, sql):
        return [r["id"] for r in self.q(path, sql)]


class TestLabelHygiene(Labels):
    def test_a_decision_the_system_made_is_not_a_label(self):
        store, path = self.loaded()
        auto = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'confirmed'")
        pending = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'pending' ORDER BY id")
        self.assertTrue(auto and len(pending) >= 3, "the data must hold automatic merges and review items")
        self.assertEqual(store.human_labeled_candidates(TENANT, self.domain), [])          # nobody has decided anything yet: no labels at all
        resolve_candidate(store, TENANT, self.domain, pending[0], "confirm", "steward")
        resolve_candidate(store, TENANT, self.domain, pending[1], "reject", "steward")
        got = {c.id: (label, source) for c, label, source in store.human_labeled_candidates(TENANT, self.domain)}
        self.assertEqual(got, {pending[0]: (1, "steward"), pending[1]: (0, "steward")})    # the automatic merges are absent, the two human decisions are there
        self.assertEqual(len(build_training_set(store, TENANT, self.domain)), 2)           # and the training set is exactly those two (it used to be every confirmed pair)

    def test_a_decision_by_the_language_model_is_not_a_label_either(self):
        store, path = self.loaded()
        pending = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'pending' ORDER BY id")
        con = sqlite3.connect(path)
        con.execute("UPDATE match_candidates SET status = 'confirmed', decided_by = 'system:llm_adjudicated' WHERE id = ?", (pending[0],))
        con.commit()
        con.close()
        self.assertNotIn(pending[0], {c.id for c, _l, _s in store.human_labeled_candidates(TENANT, self.domain)})

    def test_another_tenants_labels_are_not_mine(self):
        store, path = self.loaded()
        pending = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'pending'")
        resolve_candidate(store, TENANT, self.domain, pending[0], "confirm", "steward")
        self.assertEqual(store.human_labeled_candidates("someone-else", self.domain), [])


class TestMergeAudits(Labels):
    def test_a_random_share_of_the_systems_automatic_merges_is_flagged_and_only_those(self):
        store, path = self.loaded()
        auto = set(self.ids(path, "SELECT id FROM match_candidates WHERE status = 'confirmed' AND decided_by LIKE 'system:%'"))
        pending = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'pending' ORDER BY id")
        resolve_candidate(store, TENANT, self.domain, pending[0], "confirm", "steward")      # a steward-confirmed pair is a label already: never sampled
        n = store.sample_merge_audit(TENANT, self.domain, 0.5, seed=3)
        flagged = {r["candidate_id"] for r in store.list_merge_audits(TENANT, self.domain)}
        self.assertEqual(n, len(flagged))
        self.assertTrue(flagged <= auto and pending[0] not in flagged)
        self.assertTrue(0 < n < len(auto), f"a half sample of {len(auto)} merges gave {n}")
        self.assertEqual(store.sample_merge_audit(TENANT, self.domain, 1.0, seed=3), len(auto) - n)   # asking again flags only what is not flagged yet
        self.assertEqual(store.sample_merge_audit(TENANT, self.domain, 1.0, seed=3), 0)
        for bad in (0, -0.1, 1.5):
            with self.assertRaises(ValueError):
                store.sample_merge_audit(TENANT, self.domain, bad)

    def test_the_sample_is_the_same_for_the_same_seed_on_the_same_data(self):
        store, path = self.loaded()
        flagged = lambda: {r["candidate_id"] for r in store.list_merge_audits(TENANT, self.domain)}
        store.sample_merge_audit(TENANT, self.domain, 0.4, seed=11)
        first = flagged()
        con = sqlite3.connect(path)
        con.execute("DELETE FROM merge_audits")
        con.commit()
        con.close()
        store.sample_merge_audit(TENANT, self.domain, 0.4, seed=11)
        self.assertEqual(flagged(), first)                                                   # the same seed on the same data picks the same pairs
        con = sqlite3.connect(path)
        con.execute("DELETE FROM merge_audits")
        con.commit()
        con.close()
        store.sample_merge_audit(TENANT, self.domain, 0.4, seed=12)
        self.assertNotEqual(flagged(), first)                                                # and another seed picks others

    def test_a_verdict_becomes_a_label_and_only_a_person_may_give_it(self):
        store, path = self.loaded()
        store.sample_merge_audit(TENANT, self.domain, 1.0, seed=1)
        flagged = [r["candidate_id"] for r in store.list_merge_audits(TENANT, self.domain)]
        self.assertGreaterEqual(len(flagged), 2)
        self.assertEqual(len(store.list_merge_audits(TENANT, self.domain, pending_only=True)), len(flagged))
        self.assertEqual(store.human_labeled_candidates(TENANT, self.domain), [])           # flagged is not checked: still no label
        self.assertTrue(store.record_merge_audit(flagged[0], TENANT, self.domain, "correct", "jane"))
        self.assertTrue(store.record_merge_audit(flagged[1], TENANT, self.domain, "wrong", "jane"))
        got = {c.id: (label, source) for c, label, source in store.human_labeled_candidates(TENANT, self.domain)}
        self.assertEqual(got, {flagged[0]: (1, "audit"), flagged[1]: (0, "audit")})
        self.assertEqual(len(store.list_merge_audits(TENANT, self.domain, pending_only=True)), len(flagged) - 2)
        with self.assertRaises(ValueError):
            store.record_merge_audit(flagged[2 % len(flagged)], TENANT, self.domain, "maybe", "jane")      # not a verdict
        with self.assertRaises(ValueError):
            store.record_merge_audit(flagged[2 % len(flagged)], TENANT, self.domain, "correct", "system:auto_merge")   # the system cannot audit itself
        with self.assertRaises(ValueError):
            store.record_merge_audit(flagged[2 % len(flagged)], TENANT, self.domain, "correct", "")
        self.assertFalse(store.record_merge_audit("cand_unknown", TENANT, self.domain, "correct", "jane"))   # never flagged
        self.assertEqual(len(build_training_set(store, TENANT, self.domain)), 2)

    def test_a_pair_cannot_be_flagged_or_judged_from_another_tenant(self):
        store, path = self.loaded()
        auto = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'confirmed' LIMIT 3")
        self.assertEqual(store.flag_merge_audits("someone-else", self.domain, auto), 0)
        store.flag_merge_audits(TENANT, self.domain, auto)
        self.assertFalse(store.record_merge_audit(auto[0], "someone-else", self.domain, "correct", "jane"))
        self.assertEqual(store.list_merge_audits("someone-else", self.domain), [])

    def test_flagging_the_same_pair_twice_counts_once(self):
        store, path = self.loaded()
        auto = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'confirmed' LIMIT 4")
        self.assertEqual(store.flag_merge_audits(TENANT, self.domain, auto), len(auto))
        self.assertEqual(store.flag_merge_audits(TENANT, self.domain, auto), 0)


if __name__ == "__main__":
    unittest.main()
