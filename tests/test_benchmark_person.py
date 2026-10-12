"""
The person benchmark: SSN/DOB scenarios and the scale harness.

The scale harness is checked against a brute-force computation of the same quantity, because its whole value is that the
numbers can be trusted.

Run with:  python3 -m unittest tests.test_benchmark_person -v
"""
import datetime
import os
import random
import subprocess
import sys
import tempfile
import shutil
import unittest
from collections import Counter, defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark, benchmark_person as bp, domain_config
from app.standardize import standardize

ROOT = os.path.join(os.path.dirname(__file__), "..")


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)


class TestGenerator(Base):
    def test_a_record_is_a_pure_function_of_seed_and_index(self):
        first = [bp.record_at(5, i) for i in (123, 7, 99999, 7)]
        self.assertEqual(first[1], first[3])
        self.assertEqual(bp.record_at(5, 123), first[0])
        self.assertNotEqual(bp.record_at(6, 123), first[0])  # and the seed matters

    def test_a_duplicate_is_a_changed_copy_of_an_earlier_record_that_is_not_itself_a_duplicate(self):
        seen = 0
        for i in range(4000):
            raw, truth, base = bp.record_at(9, i)
            if base is None:
                self.assertEqual(truth, i)
                continue
            seen += 1
            self.assertLess(base, i)
            self.assertEqual(truth, base)
            self.assertFalse(bp.is_duplicate(9, base, 0.08))  # no chains: truth is just the base index
            self.assertNotEqual({k: v.lower() for k, v in raw.items()}, {k: v.lower() for k, v in bp.record_at(9, base)[0].items()})  # it really differs
        self.assertTrue(250 < seen < 400, seen)  # ~8% of 4000

    def test_the_share_of_records_with_an_ssn_or_dob_follows_the_requested_rates(self):
        rows = [bp.record_at(3, i, 0.0, ssn_rate=0.5, dob_rate=0.8)[0] for i in range(6000)]
        self.assertAlmostEqual(sum(bool(r["ssn"]) for r in rows) / 6000, 0.5, delta=0.03)
        self.assertAlmostEqual(sum(bool(r["dob"]) for r in rows) / 6000, 0.8, delta=0.03)

    def test_names_have_a_realistic_head_and_a_long_tail(self):
        """The old vocabularies were so small that 'partners per record' said nothing about 1M records. Guard against that coming back."""
        rows = [bp.record_at(3, i, 0.0)[0] for i in range(30000)]
        last = Counter(r["last_name"] for r in rows)
        self.assertLess(last.most_common(1)[0][1] / 30000, 0.02)   # the commonest surname is ~1%, not 10% or 50%
        self.assertGreater(len(last), 4000)                         # and there are thousands of distinct surnames
        self.assertGreater(len({r["postal_code"] for r in rows}), 4000)

    def test_defects_stack_without_breaking_each_other(self):
        """A year-only DOB followed by 'swap day and month' used to crash the generator."""
        for seed in range(4000):
            rng = random.Random(seed)
            bp.corrupt(rng, bp.make_person(rng, 1, 1, 1, 1))
        self.assertEqual(bp._swap_day_month("1985"), "1985")
        self.assertEqual(bp._swap_day_month("1900-01-01"), "1900-01-01")   # day == month
        self.assertEqual(bp._swap_day_month("1985-03-07"), "1985-07-03")
        self.assertEqual(bp._swap_day_month("1985-03-27"), "1985-03-27")   # 27 is not a month
        self.assertEqual(bp._shift_year("0001-01-01", 1), "0001-01-01")    # a placeholder is left alone
        self.assertEqual(bp._shift_year("2000-02-29", 1), "2001-02-28")


class TestScenarios(Base):
    def cases(self, name, n=40):
        rng = random.Random(1)
        return [bp._case(name, rng) for _ in range(n)]

    def test_every_scenario_says_which_records_are_the_same_person(self):
        for name in bp.POSITIVE:
            for case in self.cases(name, 15):
                self.assertEqual([who for _, who in case], [0, 0], name)
        for name in bp.NEGATIVE:
            for case in self.cases(name, 15):
                self.assertEqual([who for _, who in case], [0, 1], name)

    def test_the_scenarios_do_what_their_names_say(self):
        for (a, _), (b, _) in self.cases("ssn_exact_name_variants"):
            self.assertEqual((a["ssn"], a["dob"]), (b["ssn"], b["dob"]))
            self.assertNotEqual((a["phone"], a["email"], a["address_line1"], a["first_name"]), (b["phone"], b["email"], b["address_line1"], b["first_name"]))
        for (a, _), (b, _) in self.cases("ssn_typo"):
            self.assertEqual(sum(x != y for x, y in zip(a["ssn"], b["ssn"])), 1)
        for (a, _), (b, _) in self.cases("dob_swap"):
            self.assertEqual((a["dob"][:4], a["dob"][5:7], a["dob"][8:]), (b["dob"][:4], b["dob"][8:], b["dob"][5:7]))
            self.assertNotEqual(a["dob"], b["dob"])
        for (a, _), (b, _) in self.cases("dob_year_only"):
            self.assertEqual(b["dob"], a["dob"][:4])
        for (a, _), (b, _) in self.cases("ssn_placeholder"):
            self.assertEqual(a["ssn"], b["ssn"])
            self.assertIn(a["ssn"], bp.SSN_PLACEHOLDERS)
            self.assertNotEqual(a["dob"], b["dob"])
        for (a, _), (b, _) in self.cases("dob_placeholder_same_name"):
            self.assertEqual((a["dob"], a["first_name"], a["last_name"], a["postal_code"]), (b["dob"], b["first_name"], b["last_name"], b["postal_code"]))
            self.assertIn(a["dob"], bp.DOB_PLACEHOLDERS)
            self.assertNotEqual(a["address_line1"], b["address_line1"])
        for (a, _), (b, _) in self.cases("twins"):
            self.assertEqual((a["dob"], a["last_name"], a["address_line1"]), (b["dob"], b["last_name"], b["address_line1"]))
            self.assertNotEqual(a["first_name"], b["first_name"])
        for (a, _), (b, _) in self.cases("jr_sr_dob"):
            self.assertTrue(a["last_name"].endswith("Sr") and b["last_name"].endswith("Jr"))
            self.assertGreater(b["dob"], a["dob"])
            self.assertLessEqual(b["dob"], datetime.date.today().isoformat())  # a son born in the future is impossible data, not a hard negative
        for (a, _), (b, _) in self.cases("ssn_shared_error"):
            self.assertEqual(a["ssn"], b["ssn"])
            self.assertNotEqual((a["first_name"], a["dob"]), (b["first_name"], b["dob"]))

    def test_generate_is_deterministic_and_labels_truth(self):
        a, b = bp.generate(4, 3), bp.generate(4, 3)
        self.assertEqual(a, b)
        self.assertEqual(len(a), 2 * len(bp.POSITIVE) * 3 + 2 * len(bp.NEGATIVE) * 3 + 30)
        by_case = defaultdict(list)
        for r in a:
            by_case[r["case"]].append(r["true"])
        for case, truths in by_case.items():
            name = case.split(":")[0]
            if name in bp.POSITIVE:
                self.assertEqual(len(set(truths)), 1, case)
            elif name in bp.NEGATIVE:
                self.assertEqual(len(set(truths)), 2, case)


class TestVariants(Base):
    def test_variants_name_real_groups(self):
        n_groups = len(bp.CANDIDATES)
        self.assertEqual(n_groups, len(bp.GROUP_LABELS))
        for name, groups in bp.VARIANTS.items():
            self.assertTrue(groups and all(0 <= g < n_groups for g in groups), name)
        self.assertEqual(bp.VARIANTS["full"], list(range(n_groups)))
        self.assertNotIn(0, bp.VARIANTS["dob_phone_no_ssn"])   # no SSN key
        self.assertNotIn(3, bp.VARIANTS["dob_phone_no_ssn"])   # and no phone windows
        self.assertTrue({2, 3} & set(bp.VARIANTS["ssn_no_phone"]) == set())  # no phone keys at all
        self.assertIn(0, bp.VARIANTS["ssn_no_phone"])

    def test_registering_a_variant_changes_the_live_blocking_keys(self):
        from app.domains import get_domain_config
        self.assertEqual(len(bp.register_person("full").blocking_field_groups), len(bp.CANDIDATES))
        self.assertEqual(len(get_domain_config("person").blocking_field_groups), len(bp.CANDIDATES))
        self.assertEqual(len(bp.register_person("ssn_no_phone").blocking_field_groups), len(bp.VARIANTS["ssn_no_phone"]))
        with self.assertRaises(ValueError):
            bp.register_person("nope")

    def test_the_ssn_key_actually_links_records_that_share_nothing_else(self):
        a = standardize if False else None  # (clarity: uses the real standardizer below)
        cfg = bp.register_person("full")
        from app.matching import blocking_keys
        p = {"first_name": "Rob", "last_name": "Smith", "ssn": "512-45-6789", "dob": "1985-03-07"}  # (123-45-6789 would be cleansed away: it is a published example)
        q = {"first_name": "Zed", "last_name": "Quill", "ssn": "512 45 6789", "dob": "1990-01-01"}
        shared = set(blocking_keys(standardize("person", p), cfg)) & set(blocking_keys(standardize("person", q), cfg))
        self.assertEqual({bp._group_of(k) for k in shared}, {0})  # only the SSN group, despite the formatting difference


class TestScaleHarness(Base):
    def brute_force(self, n, cap, groups, seed=7, dup_rate=0.08):
        """The quantity measure_size claims to compute, computed the slow obvious way, for EVERY record."""
        cfg = bp.register_person("full")
        keys = [[k for k in bp.keys_of_record(bp.record_at(seed, i, dup_rate)[0], cfg) if bp._group_of(k) in groups] for i in range(n)]
        buckets = defaultdict(list)
        for i, ks in enumerate(keys):
            for k in ks:
                buckets[k].append(i)
        partners, no_key = [], 0
        for i, ks in enumerate(keys):
            s = set()
            live = 0
            for k in ks:
                if len(buckets[k]) - 1 <= cap:  # the engine's rule: the OTHER records on a key may number at most `cap`
                    live += 1
                    s.update(buckets[k])
            s.discard(i)
            partners.append(len(s))
            no_key += live == 0
        return sum(partners) / n, max(partners), 100 * no_key / n

    def test_partners_per_record_match_a_brute_force_computation(self):
        n = 1500
        for cap in (500, 4):  # a tight cap makes keys unusable, which is the behaviour that matters at scale
            m = bp.measure_size(n, sample=n, cap=cap, dup_sample=50)  # sampling EVERY record, so the sample is the population
            for name, groups in bp.VARIANTS.items():
                mean, mx, no_key = self.brute_force(n, cap, set(groups))
                v = m["variants"][name]
                self.assertAlmostEqual(v["partners_mean"], mean, delta=0.06, msg=f"{name} cap={cap}")
                self.assertEqual(v["partners_max"], mx, f"{name} cap={cap}")
                self.assertAlmostEqual(v["unreachable_pct"], no_key, delta=0.01, msg=f"{name} cap={cap}")

    def test_phone_windows_are_what_grows_linearly(self):
        """The measured reason 4-digit windows stop working: partners grow in proportion to the data, and nothing else does (yet)."""
        r = bp.scale_profile([2000, 8000], sample=400, dup_sample=100, log=lambda *_: None)
        windows = r["sizes"][1]["variants"]["dob_phone_windows_no_ssn"]
        exact = r["sizes"][1]["variants"]["dob_phone_no_ssn"]
        self.assertGreater(windows["growth_exponent"], 0.8)
        self.assertLess(exact["partners_mean"], windows["partners_mean"] / 5)
        self.assertGreater(r["sizes"][1]["by_group"]["phone windows(4)"], 5 * max(v for k, v in r["sizes"][1]["by_group"].items() if k != "phone windows(4)"))

    def test_extending_a_report_measures_only_the_new_sizes_and_recomputes_growth(self):
        quiet = lambda *_: None
        first = bp.scale_profile([1500, 3000], sample=60, dup_sample=30, log=quiet)
        measured = []
        real = bp.measure_size
        bp.measure_size = lambda n, **kw: (measured.append(n), real(n, **kw))[1]
        self.addCleanup(setattr, bp, "measure_size", real)
        both = bp.scale_profile([3000, 6000], base=first, sample=60, dup_sample=30, log=quiet)
        self.assertEqual(measured, [6000])                                  # 3000 was not measured again
        self.assertEqual([s["records"] for s in both["sizes"]], [1500, 3000, 6000])
        for name in both["sizes"][0]["variants"]:
            self.assertNotIn("growth_exponent", both["sizes"][0]["variants"][name])
            if both["sizes"][2]["variants"][name]["partners_mean"] > 0 and both["sizes"][1]["variants"][name]["partners_mean"] > 0:
                self.assertIn("growth_exponent", both["sizes"][2]["variants"][name])
        with self.assertRaises(ValueError):                                  # a different seed would make the sizes incomparable
            bp.scale_profile([9000], base=first, seed=99, sample=60, log=quiet)

    def test_a_measurement_that_runs_out_of_time_resumes_to_exactly_the_same_numbers(self):
        """A 1M-record measurement does not fit in one sitting; it must be able to stop, keep its progress, and carry on without any drift."""
        whole = bp.measure_size(4000, sample=200, dup_sample=100)
        ckpt = os.path.join(tempfile.mkdtemp(), "progress.pkl")
        self.addCleanup(shutil.rmtree, os.path.dirname(ckpt), True)
        pauses = 0
        while True:
            try:
                resumed = bp.measure_size(4000, sample=200, dup_sample=100, checkpoint=ckpt, budget=0)  # budget 0: stop after every chunk
                break
            except bp.MeasurementPaused as e:
                pauses += 1
                self.assertTrue(os.path.exists(ckpt))
                self.assertEqual(e.total, 4000)
        self.assertGreaterEqual(pauses, 2)                    # it really was interrupted, more than once
        self.assertFalse(os.path.exists(ckpt))                # and the checkpoint is gone once it finished
        for key in ("variants", "by_group"):
            self.assertEqual(resumed[key], whole[key])

    def test_a_checkpoint_from_a_different_measurement_is_ignored(self):
        ckpt = os.path.join(tempfile.mkdtemp(), "progress.pkl")
        self.addCleanup(shutil.rmtree, os.path.dirname(ckpt), True)
        with self.assertRaises(bp.MeasurementPaused):
            bp.measure_size(4000, sample=200, dup_sample=100, checkpoint=ckpt, budget=0)
        other = bp.measure_size(3000, sample=200, dup_sample=100, checkpoint=ckpt)   # another size: starts from scratch
        self.assertEqual(other["variants"], bp.measure_size(3000, sample=200, dup_sample=100)["variants"])

    def test_more_workers_give_the_same_numbers(self):
        one = bp.measure_size(3000, sample=300, dup_sample=60, workers=1)
        two = bp.measure_size(3000, sample=300, dup_sample=60, workers=2)
        for key in ("variants", "by_group"):
            self.assertEqual(one[key], two[key])

    def test_blocking_recall_is_measured_on_real_duplicates(self):
        m = bp.measure_size(3000, sample=100, dup_sample=150)
        self.assertEqual(m["duplicates_in_sample"], 150)
        for v in m["variants"].values():
            self.assertTrue(0.5 < v["blocking_recall"] <= 1.0)
        # a variant with fewer keys can only find fewer duplicates
        self.assertLessEqual(m["variants"]["ssn_no_phone"]["blocking_recall"], m["variants"]["full"]["blocking_recall"])

    def test_the_report_is_readable(self):
        text = bp.format_scale(bp.scale_profile([1500, 3000], sample=100, dup_sample=50, log=lambda *_: None))
        for fragment in ("customer_like", "ssn_no_phone", "dob_phone_no_ssn", "growth a", "Each group ALONE", "phone windows(4)", "SSN present 70%"):
            self.assertIn(fragment, text)


class TestPersonProfileRuns(Base):
    def test_the_person_profile_runs_through_the_real_pipeline(self):
        agg = benchmark.run([1], scale=3, label="t", profile="person", variant="full")
        self.assertEqual((agg["profile"], agg["variant"]), ("person", "full"))
        self.assertEqual(set(agg["scenarios"]) - {"background"}, set(bp.POSITIVE + bp.NEGATIVE))
        text = benchmark.format_report(agg)
        self.assertIn("profile=person / full", text)
        self.assertIn("ssn_exact_name_variants", text)

    def test_an_exact_ssn_links_people_with_nothing_else_in_common_and_a_junk_ssn_does_not_merge_strangers(self):
        agg = benchmark.run([1, 2], scale=8, label="t", profile="person", variant="full")
        sc = agg["scenarios"]
        self.assertGreaterEqual(sc["ssn_exact_name_variants"]["merged"], 0.9 * sc["ssn_exact_name_variants"]["pairs"])
        self.assertEqual(sc["ssn_placeholder"]["merged"], 0)   # the mismatch penalties already protect against strangers
        self.assertEqual(sc["ssn_shared_error"]["merged"], 0)
        self.assertEqual(sc["jr_sr_dob"]["merged"], 0)

    def test_the_customer_profile_is_untouched(self):
        agg = benchmark.run([1], scale=3, label="t")
        self.assertEqual((agg["profile"], agg["variant"]), ("customer", None))
        self.assertEqual(set(agg["scenarios"]) - {"background"}, set(benchmark.POSITIVE + benchmark.NEGATIVE))

    def test_old_saved_results_without_a_profile_still_compare(self):
        agg = benchmark.run([1], scale=2, label="t")
        agg.pop("profile"), agg.pop("variant")
        self.assertIn("auto-link precision", benchmark.compare(agg, agg))
        self.assertIn("profile=customer", benchmark.format_report(agg))


class TestCli(Base):
    def cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "benchmark.py"), *args], capture_output=True, text=True, cwd=ROOT)

    def test_scale_and_person_run(self):
        out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out, True)
        r = self.cli("scale", "--sizes", "1000", "2000", "--sample", "80", "--dup-sample", "40", "--ssn-rate", "0.5", "--out", os.path.join(out, "s.json"))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("SSN present 50%", r.stdout)
        self.assertTrue(os.path.exists(os.path.join(out, "s.json")))
        r = self.cli("run", "--profile", "person", "--variant", "ssn_no_phone", "--seeds", "1", "--scale", "2")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("profile=person / ssn_no_phone", r.stdout)

    def test_the_person_template_validates_as_a_domain_file(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        shutil.copy(os.path.join(ROOT, "domain_templates", "person.json"), d)
        r = subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "validate", "--domains-dir", d], capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
