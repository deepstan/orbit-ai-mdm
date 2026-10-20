"""
Policy B in the engines: the learned scorer promotes pairs out of review (the same function in every engine), 2% of automatic merges are flagged for audit, and a
rollback switches the promotion off. Nothing here needs scikit-learn: the scorer is a hand-built one-tree model, so every number is known.

Run with:  python3 -m unittest tests.test_promotion -v
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))
from legacy_engine import legacy_engine  # noqa: E402

from test_bulk_engine import ROOT, TENANT  # noqa: E402
from test_labels_and_audits import Labels  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app import monitor, offline  # noqa: E402
from app.bulk import bulk_load  # noqa: E402
from app.models import MatchCandidate  # noqa: E402
from app.pipeline import Run  # noqa: E402


class Promotion(Labels):
    def setUp(self):
        super().setUp()
        self.domain = "customer"

    def install(self, promote=0.9, split=0.6, target=0.0005, tenant=TENANT):
        """A one-tree scorer in a temporary model directory: p ~ 0.9975 for a pair whose rule score is above `split`, ~ 0.0025 otherwise."""
        models = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, models, True)
        patcher = mock.patch.dict(os.environ, {"MDM_MODEL_DIR": models})
        patcher.start()
        self.addCleanup(patcher.stop)
        ls._active_cache.clear()
        self.addCleanup(ls._active_cache.clear)
        body = {"format": 1, "kind": "gradient_boosted_trees", "feature_names": ["score"], "init_raw": 0.0, "learning_rate": 1.0,
                "trees": [{"left": [1, -1, -1], "right": [2, -1, -1], "feature": [0, -2, -2], "threshold": [split, -2.0, -2.0], "value": [0.0, -6.0, 6.0]}]}
        doc = {**body, "model_id": "stump", "trained": {"at": "2000-01-01T00:00:00Z"}, "policy": {
            "mode": "monitored", "target_false_rate": target, "confidence": 0.95, "promote_threshold": promote, "promote_threshold_uncertified": promote, "drop_threshold": None}}
        path = ls.model_path(tenant, self.domain)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc))
        return ls.TreeScorer(doc), models

    def promoted(self, path):
        return self.q(path, "SELECT id, status, reasons FROM match_candidates WHERE decided_by = 'system:learned_scorer'")

    def audits(self, path):
        return self.q(path, "SELECT candidate_id FROM merge_audits")


@legacy_engine   # compares the LEGACY engines with each other (the kernel has its own tests: tests/test_kernel_engine.py)
class TestPromotionIsTheSameEverywhere(Promotion):
    def test_every_engine_agrees_with_the_reference_when_a_scorer_is_live(self):
        scorer, _ = self.install(tenant=offline.TENANT)                                          # the equivalence engines run under their own tenant
        rows = self.rows(scale=3)
        without = offline.resolve_offline(rows, self.domain)
        report = offline.check_equivalence(rows, self.domain, orders=2, scorer=scorer)
        self.assertTrue(report["comparable"])
        bad = [r for r in report["runs"] if not (r["same_entities"] and r["same_review_pairs"])]
        self.assertEqual(bad, [], bad)                                                          # realtime, batch, deferred, bulk, onboard, onboard-onto-realtime
        self.assertGreater(report["reference"]["auto_pairs"], without.auto_pairs)               # and the scorer really did promote some pairs
        self.assertLess(report["reference"]["review_pairs"], len(without.review_pairs))

    def test_a_promotion_is_recorded_with_who_made_it_and_why(self):
        self.install()
        store, path = self.loaded()
        promoted = self.promoted(path)
        self.assertGreater(len(promoted), 0)
        self.assertTrue(all(r["status"] == "confirmed" and "promoted out of review by the learned scorer" in r["reasons"] for r in promoted))
        rows = self.rows()
        _, bulk_path, result = self.bulk(rows)
        self.assertGreater(result.promoted_pairs, 0)
        self.assertEqual(len(self.promoted(bulk_path)), result.promoted_pairs)                  # the bulk engine records it the same way
        self.assertEqual(store.count_auto_merges(TENANT, self.domain)["promoted"], len(promoted))
        self.assertEqual(store.human_labeled_candidates(TENANT, self.domain), [])               # a promotion is the system's decision: not a label

    def test_without_a_scorer_nothing_is_promoted(self):
        store, path = self.loaded()
        self.assertEqual(self.promoted(path), [])
        _, bulk_path, result = self.bulk(self.rows())
        self.assertEqual((self.promoted(bulk_path), result.promoted_pairs), ([], 0))

    def test_a_model_without_a_live_threshold_promotes_nothing(self):
        self.install(promote=None)
        self.assertIsNone(ls.active_scorer(TENANT, self.domain))
        store, path = self.loaded()
        self.assertEqual(self.promoted(path), [])

    def test_promotion_stops_at_once_when_the_model_is_rolled_back_and_resumes_on_reset(self):
        self.install()
        self.assertIsNotNone(ls.active_scorer(TENANT, self.domain))
        ls.write_state(TENANT, self.domain, "rolled_back", "test")
        self.assertIsNone(ls.active_scorer(TENANT, self.domain))
        store, path = self.loaded()
        self.assertEqual(self.promoted(path), [])
        _, bulk_path, result = self.bulk(self.rows())
        self.assertEqual(result.promoted_pairs, 0)
        ls.write_state(TENANT, self.domain, "active", "reset")
        self.assertIsNotNone(ls.active_scorer(TENANT, self.domain))
        _, again, result = self.bulk(self.rows())
        self.assertGreater(result.promoted_pairs, 0)

    def test_the_rule_scores_own_merges_are_untouched(self):
        store, plain = self.loaded()
        rule_auto = sorted(r["score"] for r in self.q(plain, "SELECT score FROM match_candidates WHERE status = 'confirmed'"))
        self.install()
        store2, with_scorer = self.loaded()
        kept = sorted(r["score"] for r in self.q(with_scorer, "SELECT score FROM match_candidates WHERE status = 'confirmed' AND decided_by <> 'system:learned_scorer'"))
        self.assertEqual(kept, rule_auto)                                                         # every automatic merge the rule score made is still there, unchanged


class TestTheTwoPercentAudit(Promotion):
    def test_the_sample_is_uniform_deterministic_and_a_function_of_the_id_alone(self):
        ids = [f"cand_{i}" for i in range(100000)]
        hit = [i for i in ids if ls.audit_sampled(i, 0.02)]
        self.assertTrue(1780 <= len(hit) <= 2220, len(hit))                                       # 2% of 100,000 is 2,000, give or take 5 standard deviations
        halves = (sum(1 for i in hit if int(i[5:]) < 50000), sum(1 for i in hit if int(i[5:]) >= 50000))
        self.assertLess(abs(halves[0] - halves[1]), 250)                                          # and spread evenly over the id range, not clumped
        self.assertEqual(hit, [i for i in ids if ls.audit_sampled(i, 0.02)])
        self.assertEqual([i for i in ids if ls.audit_sampled(i, 0)], [])
        self.assertEqual(len([i for i in ids if ls.audit_sampled(i, 1.0)]), 100000)

    def test_promotions_are_audited_at_ten_percent_and_zero_switches_everything_off(self):
        def rates(**env):
            with mock.patch.dict(os.environ, env):
                for name in ("MDM_AUDIT_RATE", "MDM_AUDIT_RATE_PROMOTED"):
                    if name not in env:
                        os.environ.pop(name, None)
                return ls.audit_rate(), ls.audit_rate(promoted=True)
        self.assertEqual(rates(), (0.02, 0.10))                                                      # the rule's merges 2%, the model's promotions 10%
        self.assertEqual(rates(MDM_AUDIT_RATE="0.3"), (0.3, 0.3))                                    # a higher general rate lifts the promotions' with it
        self.assertEqual(rates(MDM_AUDIT_RATE="0"), (0.0, 0.0))                                      # 0 means NO auditing at all, promotions included
        self.assertEqual(rates(MDM_AUDIT_RATE="0", MDM_AUDIT_RATE_PROMOTED="1"), (0.0, 1.0))         # unless the promotions' rate is set on purpose
        self.assertEqual(rates(MDM_AUDIT_RATE_PROMOTED="0.5"), (0.02, 0.5))
        self.assertEqual(rates(MDM_AUDIT_RATE_PROMOTED="lots"), (0.02, 0.10))                        # nonsense falls back to the default

    def test_the_rate_is_two_percent_unless_the_environment_says_otherwise(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MDM_AUDIT_RATE", None)
            self.assertEqual(ls.audit_rate(), 0.02)
        for value, expected in (("0.1", 0.1), ("0", 0.0), ("7", 1.0), ("-1", 0.0), ("lots", 0.02)):
            with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": value}):
                self.assertEqual(ls.audit_rate(), expected, value)

    def test_every_automatic_merge_is_flagged_at_a_rate_of_one_and_none_at_zero_in_every_engine(self):
        self.install()
        rows = self.rows()
        for rate, everything in (("1", True), ("0", False)):
            with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": rate, "MDM_AUDIT_RATE_PROMOTED": rate}):
                store, path = self.loaded()
                _, bulk_path, _ = self.bulk(rows)
            for label, p in (("regular", path), ("bulk", bulk_path)):
                auto = {r["id"] for r in self.q(p, "SELECT id FROM match_candidates WHERE status = 'confirmed' AND decided_by LIKE 'system:%'")}
                flagged = {r["candidate_id"] for r in self.audits(p)}
                self.assertTrue(auto, label)
                self.assertEqual(flagged, auto if everything else set(), f"{label} at rate {rate}")        # the rule's merges AND the model's promotions

    def test_promotions_can_be_audited_at_a_higher_rate_than_the_rules_merges(self):
        self.install()
        rows = self.rows()
        with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "1"}):
            store, path = self.loaded()
            _, bulk_path, _ = self.bulk(rows)
        for label, p in (("regular", path), ("bulk", bulk_path)):
            promoted = {r["id"] for r in self.promoted(p)}
            self.assertTrue(promoted, label)
            self.assertEqual({r["candidate_id"] for r in self.audits(p)}, promoted, label)        # every promotion flagged, none of the rule's merges
        with mock.patch.dict(os.environ, {}):                                                      # and by default the promotions are audited at the same rate as the rest
            os.environ.pop("MDM_AUDIT_RATE", None)
            os.environ.pop("MDM_AUDIT_RATE_PROMOTED", None)
            self.assertEqual((ls.audit_rate(), ls.audit_rate(promoted=True)), (0.02, 0.10))

    def test_a_sample_at_two_percent_is_in_the_right_range_on_a_real_load(self):
        self.install()
        rows = self.rows(scale=8)
        with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0.2", "MDM_AUDIT_RATE_PROMOTED": "0.2"}):
            _, path, result = self.bulk(rows)
        auto = len(self.q(path, "SELECT id FROM match_candidates WHERE status = 'confirmed' AND decided_by LIKE 'system:%'"))
        flagged = len(self.audits(path))
        self.assertGreater(auto, 100)
        self.assertTrue(0.05 * auto < flagged < 0.40 * auto, (flagged, auto))

    def test_a_failed_bulk_load_leaves_no_audits_behind(self):
        self.install()
        rows = self.rows()
        store, path = self.store()
        calls = []
        real = Run.finish

        def finish_once_broken(run):
            if not calls:
                calls.append(1)
                raise RuntimeError("boom")
            return real(run)
        with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "1"}), mock.patch.object(Run, "finish", finish_once_broken), self.assertRaises(RuntimeError):
            bulk_load(store, TENANT, self.domain, [("src", f"r{i}", r) for i, r in enumerate(rows)])
        self.assertEqual(self.audits(path), [])
        self.assertEqual(self.q(path, "SELECT id FROM match_candidates"), [])
        self.assertEqual(self.q(path, "SELECT id FROM source_records"), [])


class TestTheRollback(Promotion):
    def seed(self, store, rule, promoted):
        """Automatic merges of each kind, a share of them audited. rule / promoted: (made, audited, wrong)."""
        for kind, (made, audited, wrong) in (("rule", rule), ("promoted", promoted)):
            decided_by = ls.PROMOTED_BY if kind == "promoted" else "system:auto_merge"
            cands = [MatchCandidate(record_a_id=f"{kind}a{i}", record_b_id=f"{kind}b{i}", tenant_id=TENANT, domain=self.domain, score=0.95, match_type="fuzzy",
                                    status="confirmed", decided_by=decided_by) for i in range(made)]
            store.save_candidates_batch(cands)
            store.flag_merge_audits(TENANT, self.domain, [c.id for c in cands[:audited]])
            for i, c in enumerate(cands[:audited]):
                store.record_merge_audit(c.id, TENANT, self.domain, "wrong" if i < wrong else "correct", "jane")

    def setUp(self):
        super().setUp()
        self.install()
        self.store_, self.path_ = self.store()

    def test_it_rolls_back_when_the_promotions_are_clearly_pushing_the_false_rate_over_the_target(self):
        self.seed(self.store_, rule=(1000, 600, 0), promoted=(300, 150, 6))                    # 4% of the audited promotions were wrong
        result = monitor.check_scorer(self.store_, TENANT, self.domain)
        self.assertEqual((result["verdict"], result["state"]), ("rollback", "rolled_back"))
        self.assertGreater(result["pooled_lower_bound"], 0.0005)
        self.assertEqual(ls.read_state(TENANT, self.domain)["status"], "rolled_back")
        self.assertIsNone(ls.active_scorer(TENANT, self.domain))                                 # the engines stop promoting from the next record
        self.assertEqual(len(result["strata"]["promoted"]["wrong_pairs"]), 6)                     # and the report names the promoted pairs to fix by hand
        again = monitor.check_scorer(self.store_, TENANT, self.domain)
        self.assertEqual(again["state"], "rolled_back")                                           # it stays off until a person resets it
        self.assertIn("a person must reset", again["why"])

    def test_zero_errors_never_trigger_it_however_few_audits_there_are(self):
        self.seed(self.store_, rule=(1000, 600, 0), promoted=(300, 150, 0))
        result = monitor.check_scorer(self.store_, TENANT, self.domain)
        self.assertEqual((result["verdict"], result["state"]), ("ok", "active"))
        self.assertEqual(result["pooled_lower_bound"], 0.0)                                       # absence of evidence of safety is not evidence of harm
        self.assertIsNotNone(ls.active_scorer(TENANT, self.domain))

    def test_it_waits_for_enough_audited_promotions_before_acting(self):
        self.seed(self.store_, rule=(1000, 600, 0), promoted=(300, 50, 10))                       # a terrible rate, but only 50 audits
        result = monitor.check_scorer(self.store_, TENANT, self.domain)
        self.assertEqual((result["verdict"], result["state"]), ("ok", "active"))
        self.assertIn("only 50 promoted", result["why"])

    def test_a_bad_rule_score_is_not_blamed_on_the_model(self):
        self.seed(self.store_, rule=(1000, 600, 20), promoted=(300, 150, 0))                      # the RULE's merges are 3% wrong; the promotions are clean
        result = monitor.check_scorer(self.store_, TENANT, self.domain)
        self.assertEqual((result["verdict"], result["state"]), ("ok", "active"))
        self.assertIn("rule score's own", result["why"])

    def test_one_error_among_many_audits_is_not_enough(self):
        self.seed(self.store_, rule=(1000, 600, 0), promoted=(300, 150, 1))
        self.assertEqual(monitor.check_scorer(self.store_, TENANT, self.domain)["verdict"], "ok")

    def test_no_scorer_means_nothing_to_check(self):
        os.remove(ls.model_path(TENANT, self.domain))
        self.assertEqual(monitor.check_scorer(self.store_, TENANT, self.domain)["verdict"], "no_scorer")

    def test_the_command_line_checks_rolls_back_and_resets(self):
        self.seed(self.store_, rule=(1000, 600, 0), promoted=(300, 150, 6))
        run = lambda *a: subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "scorer", *a, self.domain, "--db", self.path_, "--tenant", TENANT],
                                        capture_output=True, text=True, cwd=ROOT, env=dict(os.environ))
        status = run("status")
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertIn("LIVE at p >= 0.9000", status.stdout)
        check = run("check")
        self.assertEqual(check.returncode, 3, check.stdout + check.stderr)                       # exit 3: rolled back, for a scheduler to alert on
        self.assertIn("ROLLED_BACK", check.stdout)
        self.assertIn("fix by hand", check.stdout)
        self.assertEqual(run("status").returncode, 0)
        self.assertIn("rolled_back".upper(), run("status").stdout)
        reset = run("reset", "--by", "jane")
        self.assertEqual(reset.returncode, 0)
        self.assertEqual(ls.read_state(TENANT, self.domain)["status"], "active")


if __name__ == "__main__":
    unittest.main()
