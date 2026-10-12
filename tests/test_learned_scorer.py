"""
The learned scorer: pure-Python evaluation of exported trees (checked against scikit-learn), the shared feature builder, the exact upper confidence bound,
certification and its scope, the offline trainer, the shadow replay, and the audit command line.

The tests that train a model need scikit-learn and numpy (the project itself does not): they skip without them.
Run with:  python3 -m unittest tests.test_learned_scorer -v
"""
import json
import math
import os
import random
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_bulk_engine import ROOT, TENANT  # noqa: E402
from test_labels_and_audits import Labels  # noqa: E402
from app import learned_scorer as ls  # noqa: E402
from app.pipeline import resolve_candidate  # noqa: E402
from app.shadow import format_shadow_report, shadow_report  # noqa: E402

try:
    import numpy as np  # noqa: F401
    import sklearn  # noqa: F401
    HAVE_SKLEARN = True
except ImportError:
    HAVE_SKLEARN = False
try:
    import scipy.stats as st
except ImportError:
    st = None
needs_sklearn = unittest.skipUnless(HAVE_SKLEARN, "scikit-learn and numpy are needed to train a model offline")


def evidence(score, **states):
    """A pair's evidence as scoring stores it: states = {field: state}."""
    return [{"field": f, "state": s, "similarity": 1.0 if s == "agree" else (None if s == "missing" else 0.2), "points": 0.3 if s == "agree" else -0.2 if s == "disagree" else 0.0}
            for f, s in states.items()]


class TestEvidenceFeatures(unittest.TestCase):
    def test_what_the_model_sees(self):
        f = ls.evidence_features(0.81, "fuzzy", [{"field": "email", "state": "agree", "similarity": 1.0, "points": 0.4},
                                                  {"field": "dob", "state": "missing", "similarity": None, "points": 0.0},
                                                  {"field": "last_name", "state": "partial", "similarity": 0.88, "points": -0.05}])
        self.assertEqual(f, {"score": 0.81, "type:fuzzy": 1.0, "email:agree": 1.0, "email:sim": 1.0, "email:pts": 0.4, "dob:missing": 1.0, "dob:sim": -1.0, "dob:pts": 0.0,
                             "last_name:partial": 1.0, "last_name:sim": 0.88, "last_name:pts": -0.05})
        self.assertEqual(ls.evidence_features(0.5, "x", None), {"score": 0.5, "type:x": 1.0})     # a pair with no stored evidence still has features


@unittest.skipIf(st is None, "scipy is needed to cross-check the bound")
class TestUpperBound(unittest.TestCase):
    def test_it_is_the_exact_clopper_pearson_bound(self):
        for false, n in ((0, 100), (0, 6000), (1, 50), (3, 1000), (28, 67370), (39, 78405), (150, 5000)):
            exact = float(st.beta.ppf(0.95, false + 1, n - false))
            self.assertAlmostEqual(ls.upper_bound(false, n), exact, places=7, msg=(false, n))

    def test_no_errors_needs_about_three_over_n(self):
        self.assertAlmostEqual(ls.upper_bound(0, 5990), 0.0005, delta=2e-6)                   # the number behind "about 6,000 audited merges for 1 in 2,000"
        self.assertEqual(ls.upper_bound(0, 0), 1.0)
        self.assertEqual(ls.upper_bound(10, 10), 1.0)


class TestCertification(unittest.TestCase):
    def test_a_threshold_is_certified_only_with_enough_clean_audits(self):
        rng = random.Random(1)
        clean = [(0.99 - rng.random() * 0.01, True) for _ in range(6500)]
        threshold, details = ls.certify_threshold(clean, 0.0005)
        self.assertIsNotNone(threshold)
        self.assertLessEqual(details["upper_bound"], 0.0005)
        threshold, details = ls.certify_threshold(clean[:1000], 0.0005)                 # too few: 1,000 clean audits prove only about 1 in 340
        self.assertIsNone(threshold)
        self.assertIn("about 5990", details["needed"])

    def test_an_error_pushes_the_threshold_up_and_never_splits_a_tie(self):
        pairs = [(0.99, True)] * 4000 + [(0.98, False)] * 5 + [(0.97, True)] * 4000
        threshold, details = ls.certify_threshold(pairs, 0.001)
        self.assertEqual(threshold, 0.99)                                                     # five wrong pairs at 0.98 keep the certified region above them
        one = [(0.99, True)] * 4000 + [(0.98, False)] + [(0.97, True)] * 4000
        threshold, details = ls.certify_threshold(one, 0.001)
        self.assertEqual((threshold, details["false"]), (0.97, 1))                            # one error among 8,001 still fits: the LOWEST threshold that fits is returned
        tied = [(0.9, True)] * 7000 + [(0.9, False)] * 10        # the 7,000 correct ones alone would certify (bound 0.043%); the 10 wrong ones tie with them
        self.assertLess(ls.upper_bound(0, 7000), 0.0005)
        self.assertIsNone(ls.certify_threshold(tied, 0.0005)[0])                         # a threshold cannot split pairs that tie: it takes all 7,010 or none


class TestPolicy(unittest.TestCase):
    def test_the_rule_scores_automatic_merges_stay_whatever_the_model_says(self):
        for policy in ({}, {"promote_threshold": 0.9}, {"promote_threshold": None, "promote_threshold_uncertified": 0.8}):
            self.assertEqual(ls.decide(0.01, policy, "auto_merge"), "auto_merge")            # a model with no certificate behaves exactly like the rule score

    def test_a_review_pair_is_promoted_only_past_a_certified_threshold(self):
        policy = {"promote_threshold": 0.9}
        self.assertEqual(ls.decide(0.95, policy, "review"), "auto_merge")
        self.assertEqual(ls.decide(0.89, policy, "review"), "review")
        self.assertEqual(ls.decide(0.999, {}, "review"), "review")                           # no certificate: nothing is promoted
        self.assertEqual(ls.decide(0.999, {"promote_threshold": None}, "review"), "review")

    def test_only_a_shadow_report_may_ask_for_the_uncertified_estimate(self):
        policy = {"promote_threshold": None, "promote_threshold_uncertified": 0.8}
        self.assertEqual(ls.decide(0.85, policy, "review"), "review")
        self.assertEqual(ls.decide(0.85, policy, "review", uncertified_ok=True), "auto_merge")
        self.assertEqual(ls.decide(0.7, policy, "review", uncertified_ok=True), "review")

    def test_nothing_is_dropped_unless_a_drop_threshold_is_set(self):
        self.assertEqual(ls.decide(0.001, {"promote_threshold": 0.9}, "review"), "review")
        self.assertEqual(ls.decide(0.001, {"drop_threshold": 0.1}, "review"), "drop")


@needs_sklearn
class TestTreeScorerMatchesScikitLearn(unittest.TestCase):
    def test_the_exported_trees_give_scikit_learns_answers_exactly(self):
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from train_scorer import export_body
        rng = np.random.default_rng(0)
        X = rng.random((3000, 12)) * [1, 5, 1, 1, 100, 1, 1, 1, 1, 1, 1, 1]
        y = ((X[:, 0] + 0.1 * X[:, 1] + rng.normal(0, 0.15, 3000) + (X[:, 4] > 60)) > 1.2).astype(int)
        model = GradientBoostingClassifier(n_estimators=60, max_depth=4, random_state=0).fit(X, y)
        names = [f"f{i}" for i in range(12)]
        body = export_body(model, names, 0.1)
        scorer = ls.TreeScorer({**body, "model_id": ls.model_id_of(body), "policy": {}})
        test = rng.random((2000, 12)) * [1, 5, 1, 1, 100, 1, 1, 1, 1, 1, 1, 1]                 # NOT pre-rounded to 32 bits: the scorer rounds them as scikit-learn does
        raw = model.decision_function(test)
        for i in range(0, 2000):
            self.assertAlmostEqual(scorer.raw({n: float(v) for n, v in zip(names, test[i])}), float(raw[i]), places=9)
        p = model.predict_proba(test)[:, 1]
        self.assertAlmostEqual(scorer.probability({n: float(v) for n, v in zip(names, test[0])}), float(p[0]), places=9)
        # The sharp case: values exactly AT a split threshold, where rounding to 32 bits decides which side they fall on. Random inputs almost never land there.
        edge_rng, rows = random.Random(3), []
        thresholds = [(f, t) for est in model.estimators_[:, 0] for f, t in zip(est.tree_.feature, est.tree_.threshold) if f >= 0]
        for f, t in edge_rng.sample(thresholds, 400):
            row = test[edge_rng.randrange(2000)].copy()
            row[f] = t
            rows.append(row)
        edge = np.array(rows)
        edge_raw = model.decision_function(edge)
        for i in range(len(edge)):
            self.assertAlmostEqual(scorer.raw({n: float(v) for n, v in zip(names, edge[i])}), float(edge_raw[i]), places=9, msg=f"row {i}")

    def test_a_feature_the_model_never_saw_is_ignored_and_an_absent_one_is_zero(self):
        body = {"format": 1, "kind": "gradient_boosted_trees", "feature_names": ["a", "b"], "init_raw": 0.0, "learning_rate": 1.0,
                "trees": [{"left": [1, -1, -1], "right": [2, -1, -1], "feature": [0, -2, -2], "threshold": [0.5, -2.0, -2.0], "value": [0.0, -3.0, 3.0]}]}
        scorer = ls.TreeScorer({**body, "model_id": "t", "policy": {}})
        self.assertEqual(scorer.raw({"a": 1.0}), 3.0)                  # a > 0.5 goes right
        self.assertEqual(scorer.raw({}), -3.0)                         # absent is 0, which goes left
        self.assertEqual(scorer.raw({"a": 1.0, "never_seen": 99.0}), 3.0)
        with self.assertRaises(ValueError):
            ls.TreeScorer({**body, "format": 2, "model_id": "t"})


def fabricated(n, seed, audit_review=0, review_clean=True):
    """Labelled pairs with a learnable pattern. Rule-auto pairs (score >= 0.92) are audited; some review-band pairs are audited too."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        auto = rng.random() < 0.5
        same = rng.random() < (0.995 if auto else 0.45)
        ssn = "agree" if (same and rng.random() < 0.8) or (not same and rng.random() < 0.02) else "disagree"
        score = rng.uniform(0.93, 1.0) if auto else rng.uniform(0.56, 0.9)
        email = "agree" if same else "disagree"
        if not review_clean and not auto:                  # the review band's pairs look alike whatever the truth: evidence that does not tell the two apart
            same = rng.random() < 0.6
            ssn, email = ("agree" if rng.random() < 0.5 else "disagree"), ("agree" if rng.random() < 0.5 else "disagree")
        audited = auto or (not auto and i < audit_review)
        out.append({"features": ls.evidence_features(score, "deterministic", evidence(score, ssn=ssn, email=email)), "label": int(same),
                    "source": "audit" if audited else "steward", "group": i})
    return out


class TestSelectingAThreshold(unittest.TestCase):
    """Policy B in arithmetic: the lowest probability at which promoting keeps the POOLED estimated false rate of all automatic merges within the target."""

    def test_the_pooled_estimate_is_weighted_by_how_many_merges_each_kind_has(self):
        labeled = [(0.99, True)] * 400 + [(0.95, True)] * 300 + [(0.95, False)] * 2 + [(0.6, True)] * 100 + [(0.6, False)] * 100
        population = [0.99] * 500 + [0.95] * 400 + [0.6] * 5000
        t, d = ls.select_promotion_threshold(labeled, population, n_rule_auto=60000, rule_false=0, rule_audited=1500, target=0.0005, margin=1.0)
        self.assertEqual(t, 0.95)                                            # the 0.6 band is far too error-prone; 0.95 and above is not
        r_rule, r_prom = 0.5 / 1501, (2 + 0.5) / (702 + 1)
        self.assertAlmostEqual(d["pooled_estimate"], (60000 * r_rule + 900 * r_prom) / 60900, places=12)
        self.assertEqual((d["labeled_at_or_above"], d["false"], d["promoted_population"]), (702, 2, 900))
        self.assertLessEqual(d["pooled_estimate"], 0.0005)

    def test_a_tighter_margin_promotes_less_or_nothing(self):
        labeled = [(0.99, True)] * 800 + [(0.9, True)] * 150 + [(0.9, False)] * 2
        population = [0.99] * 400 + [0.9] * 600
        loose, _ = ls.select_promotion_threshold(labeled, population, 60000, 0, 1500, 0.0005, margin=1.0)
        tight, d = ls.select_promotion_threshold(labeled, population, 60000, 0, 1500, 0.0005, margin=0.5)
        self.assertEqual(loose, 0.9)
        self.assertIsNone(tight)                                             # half the target is below the rule score's own estimated rate
        self.assertIn("pooled_with_no_promotion", d)

    def test_it_will_not_choose_without_audits_of_the_rule_scores_own_merges(self):
        labeled = [(0.99, True)] * 500
        for audited, made in ((0, 60000), (1500, 0)):
            t, d = ls.select_promotion_threshold(labeled, [0.99] * 100, made, 0, audited, 0.0005)
            self.assertIsNone(t)
            self.assertIn("why", d)

    def test_a_few_very_bad_promotions_cannot_hide_among_many_good_rule_merges(self):
        bad = [(0.99, True)] * 120 + [(0.99, False)] * 80                       # the promoted pairs are wrong 40% of the time
        pooled_only, d = ls.select_promotion_threshold(bad, [0.99] * 20, n_rule_auto=60000, rule_false=0, rule_audited=1500, target=0.0005, promoted_cap=1e9)
        self.assertIsNotNone(pooled_only)                                       # the POOLED average alone allows it: 8 wrong promotions hide among 60,000 good merges
        self.assertLessEqual(d["pooled_estimate"], 0.0005)
        t, d = ls.select_promotion_threshold(bad, [0.99] * 20, n_rule_auto=60000, rule_false=0, rule_audited=1500, target=0.0005)
        self.assertIsNone(t)                                                    # with the promoted pairs held to 10x the target on their own, it is refused

    def test_a_threshold_needs_enough_labelled_pairs_behind_it(self):
        t, _ = ls.select_promotion_threshold([(0.99, True)] * 40, [0.99] * 10, 60000, 0, 1500, 0.0005, min_labeled=100)
        self.assertIsNone(t)

    def test_a_rule_score_already_over_the_target_leaves_no_room(self):
        t, d = ls.select_promotion_threshold([(0.99, True)] * 900, [0.99] * 100, 60000, 60, 1500, 0.0005)   # the rule's merges are audited at 4% wrong
        self.assertIsNone(t)
        self.assertGreater(d["pooled_with_no_promotion"], 0.0005)


class TestTheLowerBound(unittest.TestCase):
    @unittest.skipIf(st is None, "scipy is needed to cross-check the bound")
    def test_it_is_the_exact_clopper_pearson_lower_bound(self):
        for k, n in ((1, 50), (6, 150), (20, 600), (3, 5000)):
            self.assertAlmostEqual(ls.lower_bound(k, n, 0.975), float(st.beta.ppf(0.025, k, n - k + 1)), places=7)

    def test_no_errors_never_give_a_lower_bound_above_zero(self):
        for n in (1, 50, 100000):
            self.assertEqual(ls.lower_bound(0, n), 0.0)
        self.assertEqual(ls.lower_bound(0, 0), 0.0)


@needs_sklearn
class TestTrainer(unittest.TestCase):
    def pool(self, examples):
        return [e["features"] for e in examples if e["features"]["score"] < 0.92]

    def test_it_refuses_to_train_on_too_few_labels(self):
        from train_scorer import NotEnoughLabels, train
        with self.assertRaises(NotEnoughLabels):
            train(fabricated(100, 1))
        few_positive = [e for e in fabricated(400, 1) if e["label"] == 0 or random.Random(e["group"]).random() < 0.02]
        with self.assertRaises(NotEnoughLabels):
            train(few_positive)

    def test_a_trained_model_matches_scikit_learn_and_proposes_nothing_without_the_population_it_is_weighed_against(self):
        from train_scorer import train
        doc, report = train(fabricated(900, 2), folds=3, n_estimators=40)                      # no population, no count of the rule's merges
        self.assertLessEqual(report["worst_export_difference"], 1e-9)
        self.assertGreater(report["out_of_fold_auc"], 0.9)
        policy = doc["policy"]
        self.assertEqual(policy["mode"], "monitored")
        self.assertIsNone(policy["promote_threshold"])
        self.assertIsNone(policy["promote_threshold_uncertified"])
        self.assertIsNone(policy["drop_threshold"])                                             # a drop threshold is never set
        scorer = ls.TreeScorer(doc)
        self.assertEqual(scorer.decide(0.0001, "auto_merge"), "auto_merge")                     # the rule score's merges stay
        self.assertEqual(scorer.decide(0.9999, "review"), "review")
        self.assertEqual(doc["trained"]["audit_labels"] + doc["trained"]["steward_labels"], doc["trained"]["labels"])

    def test_policy_b_proposes_a_threshold_and_makes_it_live_only_when_asked(self):
        from train_scorer import train
        examples = fabricated(2400, 4)
        kw = dict(target=0.02, folds=3, n_estimators=40, population=self.pool(examples), n_rule_auto=5000)
        proposed, _ = train(examples, **kw)
        sel = proposed["policy"]["selection"]
        self.assertIsNotNone(proposed["policy"]["promote_threshold_uncertified"], sel)
        self.assertIsNone(proposed["policy"]["promote_threshold"])                              # PROPOSED only: the engines act on promote_threshold
        self.assertLessEqual(sel["pooled_estimate"], 0.02)
        self.assertEqual(ls.TreeScorer(proposed).decide(0.99999, "review"), "review")
        live, _ = train(examples, activate=True, **kw)
        self.assertEqual(live["policy"]["promote_threshold"], live["policy"]["promote_threshold_uncertified"])
        scorer = ls.TreeScorer(live)
        self.assertEqual(scorer.decide(0.99999, "review"), "auto_merge")
        self.assertEqual(scorer.decide(0.00001, "review"), "review")
        self.assertEqual(scorer.decide(0.00001, "auto_merge"), "auto_merge")

    def test_a_review_band_the_evidence_cannot_tell_apart_proposes_nothing(self):
        from train_scorer import train
        examples = fabricated(2400, 5, review_clean=False)
        doc, _ = train(examples, target=0.02, folds=3, n_estimators=40, population=self.pool(examples), n_rule_auto=5000)
        self.assertIsNone(doc["policy"]["promote_threshold_uncertified"])

    def test_without_audits_of_the_rule_scores_own_merges_it_proposes_nothing(self):
        from train_scorer import train
        examples = [dict(e, source="steward") for e in fabricated(2400, 4)]                      # nobody audited an automatic merge
        doc, _ = train(examples, target=0.02, folds=3, n_estimators=40, population=self.pool(examples), n_rule_auto=5000)
        self.assertIsNone(doc["policy"]["promote_threshold_uncertified"])
        self.assertIn("no audited automatic merges", doc["policy"]["selection"]["why"])


@needs_sklearn
class TestShadowReplayAndAuditCommands(Labels):
    """On a real store: a hand-built one-tree scorer (so the numbers are known) replayed over the stored pairs."""

    def stump(self, promote, uncertified=None, split=0.6):
        # one tree on the 'score' feature: above `split` -> p of about 0.9975, otherwise about 0.0025
        body = {"format": 1, "kind": "gradient_boosted_trees", "feature_names": ["score"], "init_raw": 0.0, "learning_rate": 1.0,
                "trees": [{"left": [1, -1, -1], "right": [2, -1, -1], "feature": [0, -2, -2], "threshold": [split, -2.0, -2.0], "value": [0.0, -6.0, 6.0]}]}
        return ls.TreeScorer({**body, "model_id": "stump", "trained": {"at": "2000-01-01T00:00:00Z"}, "policy": {
            "target_false_rate": 0.0005, "confidence": 0.95, "promote_threshold": promote, "promote_threshold_uncertified": uncertified, "drop_threshold": None}})

    def test_the_report_counts_what_would_change_and_reads_the_labels(self):
        from app.matching import AUTO_MERGE_THRESHOLD
        store, path = self.loaded()
        high = self.ids(path, "SELECT id FROM match_candidates WHERE status = 'pending' AND score > 0.6 ORDER BY id")
        self.assertTrue(high, "the data must hold a review pair the stump scores high")
        resolve_candidate(store, TENANT, self.domain, high[0], "confirm", "steward")
        store.sample_merge_audit(TENANT, self.domain, 1.0, seed=1)
        audits = [a["candidate_id"] for a in store.list_merge_audits(TENANT, self.domain)]
        for i, cid in enumerate(audits):
            store.record_merge_audit(cid, TENANT, self.domain, "wrong" if i == 0 else "correct", "jane")
        review = [c for c in store.list_candidates(TENANT, self.domain, "pending") + store.list_candidates(TENANT, self.domain, "confirmed") if c.score < AUTO_MERGE_THRESHOLD]
        none = shadow_report(store, TENANT, self.domain, self.stump(None))
        rule_auto = none["sets"]["rule_auto"]
        self.assertEqual((rule_auto["audited"], rule_auto["audited_wrong"], rule_auto["fresh"]), (len(audits), 1, len(audits)))     # audited after the model's (2000) training date
        self.assertAlmostEqual(rule_auto["upper_bound_fresh"], ls.upper_bound(1, len(audits)), places=9)
        self.assertEqual((none["would_promote_certified"], none["review_pairs"]), (0, len(review)))       # no certificate: it promotes nothing
        self.assertEqual(none["rule_auto"] + none["review_pairs"], none["candidates"])
        promoting = shadow_report(store, TENANT, self.domain, self.stump(0.9))
        expected = sum(1 for c in review if c.score > 0.6)
        self.assertGreater(expected, 0)
        self.assertEqual(promoting["would_promote_certified"], expected)
        self.assertEqual(promoting["sets"]["promoted_certified"]["pairs"], expected)
        self.assertEqual(promoting["sets"]["promoted_certified"]["labeled"], 1)                           # the steward-confirmed pair is among them: a label, not an audit
        self.assertEqual(promoting["sets"]["promoted_certified"]["audited"], 0)
        text = format_shadow_report(promoting)
        self.assertIn("read-only", text)
        self.assertIn("stump", text)

    def test_an_uncertified_model_would_promote_nothing_and_the_report_says_so(self):
        store, path = self.loaded()
        report = shadow_report(store, TENANT, self.domain, self.stump(None, uncertified=0.9))
        self.assertEqual(report["would_promote_certified"], 0)
        self.assertGreater(report["would_promote_uncertified"], 0)
        self.assertIn("NO live promotion threshold", format_shadow_report(report))

    def test_the_audit_command_line(self):
        store, path = self.loaded()
        run = lambda *a: subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "audit", *a, "--db", path, "--tenant", TENANT], capture_output=True, text=True, cwd=ROOT)
        flagged = run("sample", self.domain, "--rate", "1", "--seed", "1")
        self.assertEqual(flagged.returncode, 0, flagged.stderr)
        self.assertIn("about 6,000 checked merges", flagged.stdout)
        listing = run("list", self.domain, "--pending")
        first = listing.stdout.split()[0]
        self.assertTrue(first.startswith("cand_"))
        self.assertEqual(run("verdict", self.domain, "--candidate", first, "--verdict", "correct", "--by", "jane").returncode, 0)
        self.assertEqual(run("verdict", self.domain, "--candidate", first, "--verdict", "correct", "--by", "system:auto").returncode, 2)   # the system cannot audit itself
        self.assertEqual(run("verdict", self.domain, "--candidate", "cand_nope", "--verdict", "wrong", "--by", "jane").returncode, 1)
        self.assertEqual(len(store.human_labeled_candidates(TENANT, self.domain)), 1)
        self.assertEqual(run("sample-review", self.domain).returncode, 2)                          # no scorer exported yet: it says so


if __name__ == "__main__":
    unittest.main()
