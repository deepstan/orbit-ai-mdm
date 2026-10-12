"""
The benchmark is the instrument behind every claimed improvement, so it gets tested: deterministic data, ground
truth that is actually what each scenario says it is, and arithmetic that can be checked by hand.
"""
import os
import sys
import unittest
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark as bm


class TestGenerator(unittest.TestCase):
    def test_same_seed_same_data_different_seed_different_data(self):
        self.assertEqual(bm.generate(5, 4), bm.generate(5, 4))
        self.assertNotEqual(bm.generate(5, 4), bm.generate(6, 4))

    def test_every_scenario_has_the_ground_truth_it_claims(self):
        cases = defaultdict(list)
        for row in bm.generate(2, 6):
            cases[row["case"]].append(row)
        per_scenario = defaultdict(int)
        for case, rows in cases.items():
            scenario = rows[0]["scenario"]
            per_scenario[scenario] += 1
            identities = {r["true"] for r in rows}
            if scenario in bm.POSITIVE:
                self.assertEqual(len(identities), 1, case)       # one person...
                self.assertGreaterEqual(len(rows), 2, case)       # ...in at least two records
            elif scenario in bm.NEGATIVE:
                self.assertGreaterEqual(len(identities), 2, case)  # different people, who must not be merged
        for scenario in bm.POSITIVE + bm.NEGATIVE:
            self.assertEqual(per_scenario[scenario], 6, scenario)
        self.assertEqual(per_scenario["background"], 60)

    def test_true_identities_never_leak_between_cases(self):
        owner = {}
        for row in bm.generate(3, 5):
            self.assertEqual(owner.setdefault(row["true"], row["case"]), row["case"])

    def test_the_hard_negatives_really_are_hard(self):
        """If these stopped sharing the identifiers they're built around, the benchmark would be measuring nothing."""
        rows = [r for r in bm.generate(4, 8) if r["scenario"] in ("household", "jr_sr", "placeholder")]
        by_case = defaultdict(list)
        for r in rows:
            by_case[r["case"]].append(r["raw"])
        for case, (a, b) in ((c, v) for c, v in by_case.items()):
            if case.startswith("household"):
                self.assertEqual((a["email"], a["phone"], a["last_name"], a["address_line1"]), (b["email"], b["phone"], b["last_name"], b["address_line1"]))
                self.assertNotEqual(a["first_name"], b["first_name"])
            if case.startswith("jr_sr"):
                self.assertEqual({a["last_name"].split()[-1], b["last_name"].split()[-1]}, {"Sr", "Jr"})  # (records are shuffled: either order)
            if case.startswith("placeholder"):
                self.assertTrue(a["email"] == b["email"] != "" or a["phone"] == b["phone"] != "")


class TestMetrics(unittest.TestCase):
    def test_summary_arithmetic_on_a_hand_checkable_aggregate(self):
        agg = {"label": "t", "seeds": [1], "scale": 1, "records": 20, "records_per_sec": 10.0,
               "overall": {"true_pairs": 16, "pred_pairs": 10, "tp": 8, "fp": 2, "pending": 5, "pending_true": 1,
                           "candidate_pairs": 19, "all_pairs": 190, "bucket_max": 7, "bucket_p99": 4},
               "scenarios": {"nickname": {"pairs": 10, "merged": 6, "queued": 2, "shares_key": 9, "is_positive": 1},
                             "household": {"pairs": 10, "merged": 1, "queued": 3, "shares_key": 10, "is_positive": 0}}}
        s = bm.summarize(agg)
        self.assertAlmostEqual(s["auto_precision"], 0.8)
        self.assertAlmostEqual(s["auto_recall"], 0.5)
        self.assertAlmostEqual(s["pos_auto_recall"], 0.6)
        self.assertAlmostEqual(s["pos_recall_with_review"], 0.8)
        self.assertAlmostEqual(s["pos_blocking_recall"], 0.9)
        self.assertAlmostEqual(s["neg_wrong_merge_rate"], 0.1)
        self.assertAlmostEqual(s["neg_queued_rate"], 0.3)
        self.assertAlmostEqual(s["review_precision"], 0.2)
        self.assertAlmostEqual(s["reduction_ratio"], 0.9)          # 19 of 190 pairs were ever compared
        self.assertAlmostEqual(s["avg_candidates_per_record"], 1.9)

    def test_aggregation_pools_counts_rather_than_averaging_rates(self):
        def run(pairs, merged):
            return {"seed": 0, "scale": 1, "label": "x", "records": 10, "seconds": 1.0,
                    "overall": {k: 0 for k in ("true_pairs", "pred_pairs", "tp", "fp", "pending", "pending_true", "candidate_pairs", "all_pairs", "bucket_max", "bucket_p99")},
                    "scenarios": {"nickname": {"pairs": pairs, "merged": merged, "queued": 0, "shares_key": pairs, "is_positive": 1}}}
        agg = bm.aggregate([run(100, 100), run(10, 0)])   # 100% on a big sample, 0% on a small one
        c = agg["scenarios"]["nickname"]
        self.assertEqual((c["pairs"], c["merged"], c["is_positive"]), (110, 100, 1))  # pooled: 91%, not the 50% an average of rates gives


class TestEndToEnd(unittest.TestCase):
    def test_a_real_run_produces_sane_internally_consistent_numbers(self):
        r = bm.run_one(seed=3, scale=3)
        o = r["overall"]
        self.assertEqual(o["fp"], o["pred_pairs"] - o["tp"])
        self.assertLessEqual(o["tp"], min(o["pred_pairs"], o["true_pairs"]))
        self.assertLessEqual(o["pending_true"], o["pending"])
        for name, c in r["scenarios"].items():
            self.assertLessEqual(c["merged"], c["pairs"], name)
            self.assertLessEqual(c["shares_key"], c["pairs"], name)
        self.assertEqual(set(r["scenarios"]), set(bm.POSITIVE + bm.NEGATIVE))

    def test_the_report_and_comparison_render(self):
        a = bm.run(seeds=[3], scale=2, label="first")
        b = bm.run(seeds=[4], scale=2, label="second")
        self.assertIn("HARD NEGATIVES", bm.format_report(a))
        both = bm.compare(a, b)
        self.assertIn("first", both)
        self.assertIn("second", both)
        self.assertIn("WRONG merges", both)


if __name__ == "__main__":
    unittest.main()
