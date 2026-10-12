"""
Step 4, scalable blocking: the `transpose` key kind, conditional key groups (`unless_any`), date-aware DOB comparison, and near-equal SSNs.
Unit and functional tests; nothing here measures time.

Run with:  python3 -m unittest tests.test_scalable_blocking -v
"""
import json
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark, benchmark_person as bp, domain_config, offline
from app.db import Store
from app.domain_config import DomainConfigError, config_to_document, parse_domain_document
from app.indexing import derived_fingerprint
from app.matching import blocking_keys, classify, date_compare, score_pair
from app.normalizers import FieldSpec, KeySpec
from app.pipeline import ingest_record
from app.protect import near_token, token
from app.standardize import standardize

ROOT = os.path.join(os.path.dirname(__file__), "..")
SHIPPED = "scalable_tight_plus"  # the key set domain_templates/person.json ships
ENV = {"MDM_TOKEN_KEY": "a-test-key-that-is-long-enough-0123456789", "MDM_TOKEN_KEY_ID": "v1"}
PERSON = {"first_name": "Ann", "last_name": "Lee", "address_line1": "12 Oak St", "city": "Reno", "state": "NV", "postal_code": "89501", "dob": "1985-03-07"}


def person_doc():
    with open(os.path.join(ROOT, "domain_templates", "person.json")) as f:
        return json.load(f)


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        patcher = mock.patch.dict(os.environ, ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.cfg = bp.register_person(SHIPPED)

    def score(self, a, b):
        return score_pair(standardize("person", a), standardize("person", b), self.cfg)

    def groups_of(self, rec):
        return {int(k[1:].split(":")[0]) for k in blocking_keys(standardize("person", rec), self.cfg)}


class TestTransposeKeys(Base):
    def test_the_value_and_every_adjacent_swap_of_it(self):
        keys = KeySpec("phone", "transpose").keys("5125550123", False)
        self.assertEqual(keys[0], "5125550123")
        self.assertIn("5152550123", keys)                    # positions 2 and 3 swapped
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(keys), 8)                       # 9 adjacent pairs, minus the two where both digits are 5 (a swap changes nothing)
        self.assertEqual(KeySpec("phone", "transpose").keys("", False), [])
        self.assertEqual(KeySpec("phone", "transpose").keys("7", False), ["7"])

    def test_two_numbers_one_adjacent_swap_apart_share_a_key_and_anything_else_does_not(self):
        def shares(a, b):
            ga, gb = (blocking_keys(standardize("person", {"phone": p}), self.cfg) for p in (a, b))
            return bool({k for k in ga & gb if k.startswith("g2:")})   # group 2 of the scalable set is the phone group
        self.assertTrue(shares("512-555-0123", "512-555-0132"))      # the last two digits swapped
        self.assertTrue(shares("512-555-0123", "152-555-0123"))
        self.assertFalse(shares("512-555-0123", "512-555-0124"))     # one digit different: not a swap
        self.assertFalse(shares("512-555-0123", "512-550-0123"[:7] + "5-0123") or False)
        self.assertFalse(shares("512-555-0123", "312-555-1023"))     # two edits

    def test_a_bucket_stays_tiny_however_many_numbers_there_are(self):
        rng, buckets = random.Random(1), {}
        for i in range(6000):
            for k in KeySpec("phone", "transpose").keys(f"{rng.randint(201, 989)}{rng.randint(201, 989)}{rng.randint(1000, 9999)}", False):
                buckets[k] = buckets.get(k, 0) + 1
        self.assertLessEqual(max(buckets.values()), 3)               # 4-digit windows put thousands of records in one bucket at this size

    def test_the_config_accepts_it_round_trips_and_refuses_a_size(self):
        d = person_doc()
        self.assertTrue(any(isinstance(g, list) and g and g[0] == {"field": "phone", "as": "transpose"} for g in d["blocking"]))
        cfg = parse_domain_document(d)
        self.assertEqual(derived_fingerprint(cfg), derived_fingerprint(parse_domain_document(config_to_document(cfg))))
        d["blocking"].append([{"field": "phone", "as": "transpose", "size": 3}])
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("only applies to 'prefix' and 'windows'", str(cm.exception))


class TestConditionalGroups(Base):
    SPARSE_GROUPS = {9, 10, 11}  # positions of the three conditional groups in the shipped set

    def test_the_two_whole_name_keys_win_back_what_a_missing_identifier_loses(self):
        """No SSN, email or phone on either side, and the DOB is off: only the whole-name keys can still bring these two together."""
        base = {"first_name": "Ann", "last_name": "Lee", "postal_code": "89501", "dob": "1985-03-07"}
        typo = {**base, "last_name": "Kee", "dob": "1986-03-07"}               # surname mistyped so it no longer sounds the same, and the birth year is off
        moved = {**base, "postal_code": "10001", "dob": "1986-03-07"}          # moved, and the birth year is off
        shared = lambda a, b: {bp.GROUP_LABELS[bp.VARIANTS[SHIPPED][int(k[1:].split(":")[0])]] for k in
                               blocking_keys(standardize("person", a), self.cfg) & blocking_keys(standardize("person", b), self.cfg)}
        self.assertIn("first (exact)+zip5", shared(base, typo))
        self.assertIn("last (exact)+first (exact)", shared(base, moved))
        tight = bp.register_person("scalable_tight")                           # without them the same two pairs meet nowhere
        for other in (typo, moved):
            self.assertEqual(blocking_keys(standardize("person", base), tight) & blocking_keys(standardize("person", other), tight), set())

    def test_the_shipped_template_has_three_conditional_groups_and_no_phone_windows(self):
        shipped = bp.load_config()
        self.assertEqual([i for i, u in enumerate(shipped.blocking_unless) if u], sorted(self.SPARSE_GROUPS))
        self.assertTrue(all(u == ("ssn", "dob", "email", "phone") for u in shipped.blocking_unless if u))
        self.assertFalse(any(spec.kind == "windows" for g in shipped.keyspecs() for spec in g))
        self.assertEqual(derived_fingerprint(shipped), derived_fingerprint(self.cfg))   # the template IS the benchmark's "scalable" variant

    def test_loose_keys_exist_only_for_a_record_with_no_selective_field(self):
        sparse = {"first_name": "Ann", "last_name": "Lee", "postal_code": "89501"}
        self.assertTrue(self.SPARSE_GROUPS <= self.groups_of(sparse))
        for field, value in (("email", "a@x.com"), ("phone", "512-555-0123"), ("dob", "1985-03-07"), ("ssn", "512-45-6789")):
            self.assertFalse(self.SPARSE_GROUPS & self.groups_of({**sparse, field: value}), field)

    def test_a_junk_ssn_or_dob_does_not_make_a_record_rich(self):
        """Cleansing turns them into missing, so the record is sparse, which is exactly what it is."""
        junk = {"first_name": "Ann", "last_name": "Lee", "postal_code": "89501", "ssn": "000-00-0000", "dob": "1900-01-01"}
        self.assertTrue(self.SPARSE_GROUPS <= self.groups_of(junk))

    def test_discovery_is_symmetric_so_it_cannot_depend_on_arrival_order(self):
        sparse_a = {"first_name": "Ann", "last_name": "Lee", "postal_code": "89501"}
        sparse_b = {"first_name": "Ann", "last_name": "Lee", "postal_code": "90210"}      # same name, another zip
        rich = {"first_name": "Ann", "last_name": "Lee", "postal_code": "90210", "email": "a@x.com"}
        shared = lambda a, b: blocking_keys(standardize("person", a), self.cfg) & blocking_keys(standardize("person", b), self.cfg)
        conditional = lambda keys: {k for k in keys if int(k[1:].split(":")[0]) in self.SPARSE_GROUPS}
        self.assertTrue(conditional(shared(sparse_a, sparse_b)))                         # two sparse records meet on the loose keys
        self.assertEqual(conditional(shared(sparse_a, rich)), set())                     # a sparse and a rich record never do, whichever arrives first
        self.assertEqual(conditional(shared(rich, sparse_a)), set())
        self.assertTrue(shared(sparse_a, rich))                                          # (they still meet through the unconditional whole-name key)

    def test_config_parsing_validation_and_export(self):
        d = person_doc()
        d["blocking"].append({"fields": ["last_name"], "unless_any": ["email"], "also": 1})
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("allowed: fields, unless_any", str(cm.exception))
        for bad, fragment in ((["nope"], "must be a list of field names"), ("email", "must be a list of field names")):
            d = person_doc()
            d["blocking"].append({"fields": ["last_name"], "unless_any": bad})
            with self.assertRaises(DomainConfigError, msg=fragment) as cm:
                parse_domain_document(d)
            self.assertIn(fragment, str(cm.exception))
        d = person_doc()
        d["blocking"].append(["last_name", "first_name"])  # the same fields as an existing conditional group but unconditional: allowed
        parse_domain_document(d)
        d["blocking"].append(["last_name", "first_name"])  # ...twice: a duplicate
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("duplicate", str(cm.exception))
        exported = config_to_document(parse_domain_document(person_doc()))["blocking"]
        self.assertEqual([g for g in exported if isinstance(g, dict)][0]["unless_any"], ["ssn", "dob", "email", "phone"])

    def test_the_condition_is_part_of_the_derived_data_fingerprint(self):
        base = derived_fingerprint(parse_domain_document(person_doc()))
        d = person_doc()
        next(g for g in d["blocking"] if isinstance(g, dict))["unless_any"] = ["ssn", "dob", "email"]
        self.assertNotEqual(base, derived_fingerprint(parse_domain_document(d)))

    def test_every_engine_in_every_order_still_agrees_with_the_reference(self):
        rows = [r["raw"] for r in bp.generate(1, 1)]
        report = offline.check_equivalence(rows, "person", orders=2)
        self.assertTrue(report["ok"], offline.format_equivalence(report))
        self.assertGreater(report["reference"]["review_pairs"], 0)


class TestDates(Base):
    def test_the_date_comparison_table(self):
        table = [("1985-03-07", "1985-03-07", "agree"), ("1985-03-07", "1985-07-03", "near"), ("1985-03-07", "1986-03-07", "near"),
                 ("1985-03-07", "1987-03-07", "disagree"), ("1985-03-07", "1991-11-23", "disagree"), ("1985-03-07", "1985", "agree"),
                 ("1985", "1985", "agree"), ("1985", "1986", "disagree"), ("1985-03-07", "1986", "disagree"), ("1985-03-03", "1985-03-03", "agree"),
                 ("1985-03-07", "", "missing"), ("", "", "missing")]
        for a, b, state in table:
            self.assertEqual(date_compare(a, b)[0], state, (a, b))
        self.assertNotEqual(date_compare("1985-03-07", "garbage")[0], "agree")
        self.assertEqual(date_compare("1985-03-07", "1985-07-03"), date_compare("1985-07-03", "1985-03-07"))  # symmetric

    def test_a_swapped_day_and_month_no_longer_turns_an_otherwise_identical_pair_into_a_review_item(self):
        swapped = self.score(PERSON, {**PERSON, "dob": "1985-07-03"})
        self.assertEqual(classify(swapped.score), "auto_merge")
        self.assertIn("dob nearly agrees", " ".join(swapped.reasons))
        self.assertNotIn("1985", " ".join(r for r in swapped.reasons if "nearly" in r))   # no values in the reason
        doc = person_doc()
        doc["mismatch_penalties"]["dob"] = {"penalty": 0.3, "compare": "exact"}           # the old behaviour: any difference is a contradiction
        from app.domains import register_domain
        exact = parse_domain_document(doc)
        register_domain(exact, replace=True)
        old = score_pair(standardize("person", PERSON), standardize("person", {**PERSON, "dob": "1985-07-03"}), exact)
        self.assertEqual(classify(old.score), "review")

    def test_a_bare_year_agrees_with_a_date_in_that_year_and_not_with_another(self):
        self.assertEqual(classify(self.score(PERSON, {**PERSON, "dob": "1985"}).score), "auto_merge")
        other = self.score(PERSON, {**PERSON, "dob": "1990"})
        self.assertNotEqual(classify(other.score), "auto_merge")
        self.assertIn("dob disagrees", " ".join(other.reasons))

    def test_a_year_out_by_one_is_near_and_a_different_date_still_disagrees(self):
        near = self.score(PERSON, {**PERSON, "dob": "1986-03-07"})
        self.assertIn("dob nearly agrees", " ".join(near.reasons))
        far = self.score(PERSON, {**PERSON, "dob": "1991-11-23"})
        self.assertNotEqual(classify(far.score), "auto_merge")
        self.assertIn("dob disagrees", " ".join(far.reasons))

    def test_the_config_checks_what_a_date_comparison_is_applied_to(self):
        d = person_doc()
        d["mismatch_penalties"]["first_name"] = {"penalty": 0.1, "compare": "date"}
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("needs a field of type date", str(cm.exception))
        d = person_doc()
        d["mismatch_penalties"]["dob"] = {"penalty": 0.3, "compare": "weekly"}
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("'exact', 'similarity' or 'date'", str(cm.exception))
        cfg = parse_domain_document(person_doc())
        self.assertEqual(cfg.mismatch_date, ["dob"])
        self.assertEqual(config_to_document(cfg)["mismatch_penalties"]["dob"], {"penalty": 0.3, "compare": "date"})


class TestNearSsn(Base):
    SPEC = FieldSpec(("ssn",), present=("present_ssn",), protect="hash", options=(("near", True),))

    def shared(self, a, b):
        return set(self.SPEC.near_tokens(a).split(",")) & set(self.SPEC.near_tokens(b).split(","))

    def test_one_substitution_or_one_adjacent_swap_share_a_token_and_nothing_else_does(self):
        self.assertTrue(self.shared("512-45-6789", "512-45-6780"))     # last digit different
        self.assertTrue(self.shared("512-45-6789", "612-45-6789"))     # first digit different
        self.assertTrue(self.shared("512-45-6789", "512-45-6798"))     # last two digits swapped
        self.assertFalse(self.shared("512-45-6789", "512-45-6700"))    # two digits different
        self.assertFalse(self.shared("512-45-6789", "345-67-8901"))
        self.assertEqual(len(self.shared("512-45-6789", "5124-56789")), len(self.SPEC.near_tokens("512-45-6789").split(",")))  # the same SSN shares them all

    def test_tokens_are_short_keyed_deterministic_and_hold_no_digits_of_the_value(self):
        tokens = self.SPEC.near_tokens("512-45-6789").split(",")
        self.assertLessEqual(len(tokens), 18)
        self.assertTrue(all(len(t) == 10 and set(t) <= set("0123456789abcdef") for t in tokens))
        self.assertEqual(tokens, self.SPEC.near_tokens("512 45 6789").split(","))
        self.assertNotIn(token("512456789", "ssn"), tokens)               # distinct from the exact hash
        with mock.patch.dict(os.environ, {"MDM_TOKEN_KEY": "another-key-that-is-also-long-enough-9876543210"}):
            self.assertNotEqual(self.SPEC.near_tokens("512-45-6789").split(","), tokens)

    def test_it_is_off_unless_asked_for_and_never_for_an_unprotected_or_empty_field(self):
        self.assertEqual(FieldSpec(("ssn",), protect="hash").near_tokens("512-45-6789"), "")
        self.assertEqual(FieldSpec(("ssn",), options=(("near", True),)).near_tokens("512-45-6789"), "")
        self.assertEqual(self.SPEC.near_tokens("000-00-0000"), "")        # a placeholder has no variants
        self.assertEqual(self.SPEC.near_tokens(""), "")

    def test_the_tokens_live_in_the_matching_form_only_and_are_never_a_blocking_key(self):
        std = standardize("person", {**PERSON, "ssn": "512-45-6789"})
        self.assertIn("ssn~near", std)
        tokens = std["ssn~near"].split(",")
        keys = " ".join(blocking_keys(std, self.cfg))
        self.assertFalse(any(t in keys for t in tokens))
        matching, cleansed = self.cfg.standardizer.standardize_both({**PERSON, "ssn": "512-45-6789"})
        self.assertNotIn("ssn~near", cleansed)
        self.assertNotIn("ssn~near", standardize("person", PERSON))       # no SSN, nothing stored
        self.assertNotIn("ssn~near", standardize("person", {**PERSON, "ssn": "000-00-0000"}))  # a placeholder has no variants either

    def test_a_one_typo_ssn_is_a_near_match_not_a_contradiction(self):
        a = {**PERSON, "ssn": "512-45-6789"}
        sub = self.score(a, {**a, "ssn": "512-45-6780"})
        swap = self.score(a, {**a, "ssn": "512-45-6798"})
        two = self.score(a, {**a, "ssn": "512-45-6700"})
        for r in (sub, swap):
            self.assertEqual(classify(r.score), "auto_merge")
            self.assertIn("ssn nearly agrees", " ".join(r.reasons))
        self.assertEqual(classify(two.score), "review")
        self.assertIn("ssn disagrees", " ".join(two.reasons))
        text = " ".join(sub.reasons + swap.reasons + two.reasons)
        for leaked in ("512", "6789", "6780", "v1:"):
            self.assertNotIn(leaked, text)

    def test_exact_ssns_and_unrelated_ssns_behave_as_before(self):
        a = {**PERSON, "ssn": "512-45-6789"}
        self.assertEqual(classify(self.score(a, {**a, "ssn": "512 45 6789"}).score), "auto_merge")
        different_people = self.score(a, {**a, "first_name": "Zed", "ssn": "345-67-8901"})
        self.assertNotEqual(classify(different_people.score), "auto_merge")

    def test_config_refuses_near_without_protection_or_with_a_wrong_value(self):
        d = person_doc()
        del d["fields"]["ssn"]["protect"]
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("needs \"protect\": \"hash\"", str(cm.exception))
        d = person_doc()
        d["fields"]["ssn"]["options"]["near"] = "yes"
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(d)
        self.assertIn("near must be true or false", str(cm.exception))
        d = person_doc()
        d["fields"]["dob"]["options"]["near"] = True
        with self.assertRaises(DomainConfigError):
            parse_domain_document(d)

    def test_the_database_holds_the_tokens_in_staging_only_and_the_plain_ssn_in_raw_only(self):
        db = os.path.join(self.tmp, "t.db")
        store = Store(db)
        ingest_record(store, "acme", "person", "crm", "A", {**PERSON, "ssn": "512-45-6789"})
        r = ingest_record(store, "acme", "person", "crm", "B", {**PERSON, "ssn": "512-45-6780"})
        self.assertEqual(len(r["auto_merged_with"]), 1)                   # linked despite the typo
        tokens = self.cfg.field_specs["ssn"].near_tokens("512-45-6789").split(",")
        found, plain = set(), set()
        con = sqlite3.connect(db)
        for line in con.iterdump():
            if line.startswith("INSERT INTO"):
                table = line.split()[2].strip('"')
                if any(t in line for t in tokens):
                    found.add(table)
                if any(x in line for x in ("512-45-6789", "512456789", "512-45-6780", "512456780")):
                    plain.add(table)
        self.assertEqual(found, {"standardized_records"})
        self.assertEqual(plain, {"source_records"})


class TestScenarios(Base):
    """The labelled cases through the real pipeline, with the shipped (scalable) keys."""

    def test_the_gaps_step_4_set_out_to_close_are_closed_and_nothing_got_worse(self):
        sc = benchmark.run([1, 2], scale=8, label="t", profile="person", variant=SHIPPED)["scenarios"]
        for name in ("ssn_typo", "dob_swap", "dob_year_only", "ssn_exact_name_variants", "nickname_dob", "no_ssn_name_dob_address"):
            self.assertEqual(sc[name]["merged"], sc[name]["pairs"], name)
        for name in ("jr_sr_dob", "ssn_shared_error", "ssn_placeholder", "same_name_diff_dob"):
            self.assertEqual(sc[name]["merged"], 0, name)
        self.assertEqual(sc["ssn_placeholder_same_name"]["queued"], 0)
        self.assertLessEqual(sc["twins"]["merged"], max(1, sc["twins"]["pairs"] // 10))
        self.assertLessEqual(sc["dob_placeholder_same_name"]["merged"], 0.4 * sc["dob_placeholder_same_name"]["pairs"])

    def test_the_scalable_keys_find_every_pair_the_full_set_finds_on_these_cases(self):
        run = lambda variant: benchmark.run([1, 2], scale=8, label="t", profile="person", variant=variant)["scenarios"]
        full, scalable = run("full"), run(SHIPPED)
        for name in bp.POSITIVE + bp.NEGATIVE:
            self.assertEqual((scalable[name]["merged"], scalable[name]["queued"]), (full[name]["merged"], full[name]["queued"]), name)


class TestScaleHarness(Base):
    def test_the_scalable_keys_need_far_fewer_comparisons_without_losing_duplicates(self):
        m = bp.measure_size(8000, sample=400, dup_sample=300, variants=["customer_like", SHIPPED])
        old, new = m["variants"]["customer_like"], m["variants"][SHIPPED]
        self.assertLess(new["partners_mean"], old["partners_mean"] / 2)
        self.assertGreaterEqual(new["blocking_recall"], old["blocking_recall"] - 0.01)
        self.assertEqual(len(m["by_group"]), len(bp.GROUP_LABELS))
        self.assertLess(m["by_group"]["phone + adjacent swaps"], 1.0)
        self.assertLess(m["by_group"]["last+first (sparse only)"], m["by_group"]["last+first"])  # a conditional group is far smaller than its unconditional twin


if __name__ == "__main__":
    unittest.main()
