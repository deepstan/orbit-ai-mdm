"""
The audit page's logic: what a person is shown, in what order, what a verdict does, and what unmerging from it does.

Run with:  python3 -m unittest tests.test_stewardship -v
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_unmerge import A, B, C, D, DOMAIN, TENANT, Chain  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app.models import now_iso  # noqa: E402
from app.stewardship import audit_queue, audit_summary, give_verdict  # noqa: E402
from app.unmerge import unmerge_pair  # noqa: E402


class Audited(Chain):
    """The chain A-B-C with both of its merges flagged for audit; B-C is a promotion by the model, A-B the rule score's."""

    def setUp(self):
        super().setUp()
        self.ab, self.bc = self.pair("A", "B"), self.pair("B", "C")
        self.store.update_candidate_status(self.bc, TENANT, DOMAIN, "confirmed", ls.PROMOTED_BY, now_iso())   # B-C was made by the model ...
        self.store.flag_merge_audits(TENANT, DOMAIN, [self.ab, self.bc])                                      # ... and who made it is recorded when it is flagged

    def queue(self, **kw):
        return audit_queue(self.store, TENANT, DOMAIN, **kw)


class TestTheQueue(Audited):
    def test_the_models_promotions_come_first_and_each_merge_says_who_made_it(self):
        q = self.queue()
        self.assertEqual([(i["candidate_id"], i["made_by"]) for i in q["items"]], [(self.bc, "model"), (self.ab, "rule")])
        self.assertEqual((q["waiting"], q["shown"]), (2, 2))
        first = q["items"][0]
        for key in ("score", "match_type", "reasons", "contributions", "merged_at", "entity_id", "record_a", "record_b", "unmerge"):
            self.assertIn(key, first)
        self.assertEqual({first["record_a"]["id"], first["record_b"]["id"]}, {self.rid("B"), self.rid("C")})

    def test_attributes_pass_through_the_masking_function_the_caller_gives(self):
        q = self.queue(mask=lambda attrs: {k: ("***" if k in ("email", "phone") else v) for k, v in attrs.items()})
        for item in q["items"]:
            for side in ("record_a", "record_b"):
                self.assertEqual((item[side]["attributes"]["email"], item[side]["attributes"]["phone"]), ("***", "***"))
                self.assertEqual(item[side]["attributes"]["city"], "Atlanta")

    def test_the_limit_caps_what_is_shown_but_not_the_count_of_what_waits(self):
        q = self.queue(limit=1)
        self.assertEqual((q["waiting"], q["shown"], q["items"][0]["made_by"]), (2, 1, "model"))

    def test_it_says_in_advance_whether_a_merge_can_be_unmerged_and_what_would_move(self):
        by_id = {i["candidate_id"]: i for i in self.queue()["items"]}
        self.assertEqual(by_id[self.ab]["unmerge"], {"possible": True, "moves": 2, "stays": 1})
        self.ingest("D", D)                                                                         # D joins A, B and C: A-B is now also joined through D
        by_id = {i["candidate_id"]: i for i in self.queue()["items"]}
        self.assertFalse(by_id[self.ab]["unmerge"]["possible"])
        self.assertIn("still joined through other confirmed pairs", by_id[self.ab]["unmerge"]["why"])

    def test_a_merge_unmerged_since_it_was_flagged_is_no_longer_offered(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.ab, "jane")
        q = self.queue()
        self.assertEqual([i["candidate_id"] for i in q["items"]], [self.bc])
        self.assertEqual(q["waiting"], 1)                                                           # unmerging a flagged merge IS the audit's verdict, so it is no longer waiting


class TestVerdicts(Audited):
    def test_correct_and_wrong_are_recorded_under_the_name_given(self):
        self.assertEqual(give_verdict(self.store, TENANT, DOMAIN, self.ab, "correct", "pat")["verdict"], "correct")
        give_verdict(self.store, TENANT, DOMAIN, self.bc, "wrong", "sam")
        verdicts = {a["candidate_id"]: (a["verdict"], a["audited_by"]) for a in self.store.list_merge_audits(TENANT, DOMAIN)}
        self.assertEqual(verdicts, {self.ab: ("correct", "pat"), self.bc: ("wrong", "sam")})
        self.assertEqual(len({self.entity(n) for n in "ABC"}), 1)                                   # a verdict alone does not change any entity

    def test_wrong_with_unmerge_splits_the_entity_and_says_how(self):
        r = give_verdict(self.store, TENANT, DOMAIN, self.ab, "wrong", "jane", unmerge=True, reason="two people")
        self.assertEqual(r["unmerge"]["ok"], True)
        self.assertEqual((r["unmerge"]["split"], r["unmerge"]["moved"]), (True, 2))
        self.assertNotEqual(self.entity("A"), self.entity("B"))
        self.assertEqual(self.store.get_candidate(self.ab, TENANT, DOMAIN).decided_by, "unmerge:jane")

    def test_when_it_cannot_unmerge_the_verdict_is_kept_and_the_reason_given(self):
        self.ingest("D", D)
        r = give_verdict(self.store, TENANT, DOMAIN, self.ab, "wrong", "jane", unmerge=True)
        self.assertEqual(r["verdict"], "wrong")
        self.assertFalse(r["unmerge"]["ok"])
        self.assertIn("still joined", r["unmerge"]["why"])
        self.assertEqual(len({self.entity(n) for n in "ABCD"}), 1)
        self.assertEqual([a["verdict"] for a in self.store.list_merge_audits(TENANT, DOMAIN) if a["candidate_id"] == self.ab], ["wrong"])

    def test_what_it_refuses(self):
        for args, text in (((self.ab, "maybe", "pat"), "verdict must be"), (("cand_nope", "correct", "pat"), "not flagged"),
                           ((self.ab, "correct", "system:auto"), "from a person")):
            with self.assertRaises(ValueError) as raised:
                give_verdict(self.store, TENANT, DOMAIN, *args)
            self.assertIn(text, str(raised.exception))
        with self.assertRaises(ValueError) as raised:
            give_verdict(self.store, TENANT, DOMAIN, self.ab, "correct", "pat", unmerge=True)
        self.assertIn("only a merge judged wrong", str(raised.exception))
        self.assertEqual([a["verdict"] for a in self.store.list_merge_audits(TENANT, DOMAIN)], [None, None])

    def test_a_merge_nobody_flagged_cannot_be_given_a_verdict_and_stays_out_of_the_sample(self):
        zed = {"first_name": "Zed", "last_name": "Quill", "email": "zed@quill.example", "phone": "404-555-0777", "city": "Macon"}
        self.ingest("X", zed)
        self.ingest("Y", {**zed, "first_name": "Zedd", "phone": "404-555-0778"})                      # an independent pair, merged by the engine, never flagged (the audit rate is 0 here)
        unflagged = next(c for c, a, b in self.store.confirmed_pairs_among(TENANT, DOMAIN, [self.rid("X"), self.rid("Y")]))
        with self.assertRaises(ValueError):
            give_verdict(self.store, TENANT, DOMAIN, unflagged, "wrong", "jane")
        r = unmerge_pair(self.store, TENANT, DOMAIN, unflagged, "jane")                              # a person can still unmerge it ...
        self.assertTrue(r["split"])
        self.assertEqual({a["candidate_id"] for a in self.store.list_merge_audits(TENANT, DOMAIN)}, {self.ab, self.bc})   # ... but it does not enter the audit sample

    def test_unmerging_a_flagged_merge_by_command_is_recorded_as_a_wrong_verdict_by_the_person(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.ab, "jane")
        audit = next(a for a in self.store.list_merge_audits(TENANT, DOMAIN) if a["candidate_id"] == self.ab)
        self.assertEqual((audit["verdict"], audit["audited_by"]), ("wrong", "jane"))


class TestTheSummary(Audited):
    def test_it_counts_what_was_checked_for_each_kind_of_merge(self):
        give_verdict(self.store, TENANT, DOMAIN, self.ab, "correct", "pat")
        give_verdict(self.store, TENANT, DOMAIN, self.bc, "wrong", "sam")
        s = audit_summary(self.store, TENANT, DOMAIN)
        self.assertEqual(s["waiting"], 0)
        self.assertEqual((s["strata"]["rule"]["merges"], s["strata"]["rule"]["audited"], s["strata"]["rule"]["wrong"]), (1, 1, 0))
        self.assertEqual((s["strata"]["model"] if "model" in s["strata"] else s["strata"]["promoted"])["wrong"], 1)
        self.assertEqual(s["scorer"], {"present": False, "state": "none", "reason": "", "promotion_live": False})

    def test_it_shows_whether_the_model_is_live_or_has_been_rolled_back(self):
        models = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, models, True)
        patcher = mock.patch.dict(os.environ, {"MDM_MODEL_DIR": models})
        patcher.start()
        self.addCleanup(patcher.stop)
        doc = {"format": 1, "kind": "gradient_boosted_trees", "feature_names": ["score"], "init_raw": 0.0, "learning_rate": 1.0, "model_id": "stump",
               "trees": [{"left": [-1], "right": [-1], "feature": [-2], "threshold": [-2.0], "value": [0.0]}], "trained": {"at": "2000-01-01T00:00:00Z"},
               "policy": {"mode": "monitored", "target_false_rate": 0.0005, "confidence": 0.95, "promote_threshold": 0.9, "promote_threshold_uncertified": 0.9, "drop_threshold": None}}
        path = ls.model_path(TENANT, DOMAIN)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc))
        self.assertEqual(audit_summary(self.store, TENANT, DOMAIN)["scorer"], {"present": True, "state": "active", "reason": "", "promotion_live": True})
        ls.write_state(TENANT, DOMAIN, "rolled_back", "too many wrong")
        s = audit_summary(self.store, TENANT, DOMAIN)["scorer"]
        self.assertEqual((s["state"], s["reason"], s["promotion_live"]), ("rolled_back", "too many wrong", False))


if __name__ == "__main__":
    unittest.main()
