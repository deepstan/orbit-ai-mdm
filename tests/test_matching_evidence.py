"""
Matching evidence: placeholders, disagreement, contradiction, the generational suffix, nicknames, phonetic and
window blocking keys, the bucket cap, and the per-field score breakdown. Every behavior here was a measured
defect in the matcher (see benchmarks/ and README): these tests lock the fixes in.

Run with:  python3 -m unittest tests.test_matching_evidence -v
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.db import Store
from app.domain_config import DomainConfigError, config_to_document, parse_domain_document
from app.domains import BUILTIN_DOMAINS, REGISTRY, get_domain_config, register_domain
from app.indexing import blocking_fingerprint
from app.matching import blocking_keys, classify, score_pair
from app.normalizers import FieldSpec, KeySpec, group_keys, is_placeholder, soundex
from app.pipeline import ingest_record
from app.standardize import normalize_given_name, normalize_suffix, standardize

ROOT = os.path.join(os.path.dirname(__file__), "..")


def customer(**kw):
    return {"first_name": "", "last_name": "", "email": "", "phone": "", "address_line1": "", "city": "",
            "postal_code": "", **kw}


def score(domain, a, b):
    cfg = get_domain_config(domain)
    return score_pair(standardize(domain, a), standardize(domain, b), cfg)


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.dbpath = os.path.join(self.tmp, "t.db")


# ----------------------------------------------------------------- the vocabulary
class TestVocabulary(Base):
    def test_soundex_matches_the_published_test_vectors(self):
        vectors = {"Robert": "R163", "Rupert": "R163", "Rubin": "R150", "Ashcraft": "A261", "Ashcroft": "A261",
                   "Tymczak": "T522", "Pfister": "P236", "Honeyman": "H555", "Smith": "S530", "Smyth": "S530"}
        for name, code in vectors.items():
            self.assertEqual(soundex(name), code, name)
        self.assertEqual(soundex(""), "")
        self.assertEqual(soundex("123"), "")

    def test_windows_survive_a_transposition_that_breaks_an_exact_key(self):
        spec = KeySpec("phone", "windows", 4)
        a, b = set(spec.keys("5125550123", False)), set(spec.keys("5125550132", False))
        self.assertEqual(len(a), 7)
        self.assertTrue(a & b)
        self.assertNotEqual("5125550123", "5125550132")  # an exact key would not have matched

    def test_key_kinds(self):
        self.assertEqual(KeySpec("x").keys("smith", multi=True), ["smi"])
        self.assertEqual(KeySpec("x").keys("smith", multi=False), ["smith"])
        self.assertEqual(KeySpec("x", "prefix", 2).keys("smith", True), ["sm"])
        self.assertEqual(KeySpec("x", "exact").keys("smith", True), ["smith"])
        self.assertEqual(KeySpec("x", "soundex").keys("smith", True), ["S530"])
        self.assertEqual(KeySpec("x", "windows", 3).keys("ab", False), ["ab"])  # shorter than the window: the value itself

    def test_group_keys_is_a_cartesian_product_and_bounded(self):
        keys = group_keys([KeySpec("p", "windows", 4), KeySpec("z", "prefix", 3)], ["5125550123", "94107"])
        self.assertEqual(len(keys), 7)
        self.assertTrue(all(k.endswith(":941") for k in keys))
        many = group_keys([KeySpec("a", "windows", 1), KeySpec("b", "windows", 1)], ["abcdefghij", "klmnopqrst"])
        self.assertLessEqual(len(many), 25)

    def test_generational_suffix(self):
        self.assertEqual([normalize_suffix(x) for x in ("Park Jr.", "Park, Sr", "Smith III", "Park", "", None)],
                         ["jr", "sr", "iii", "", "", ""])

    def test_nicknames_map_the_first_word_only_and_ambiguous_ones_are_left_alone(self):
        self.assertEqual(normalize_given_name("Bob"), "robert")
        self.assertEqual(normalize_given_name("Jen"), "jennifer")
        self.assertEqual(normalize_given_name("Mary Beth"), "mary beth")  # Beth is a middle name here
        for ambiguous in ("Pat", "Chris", "Sam", "Jamie", "Terry", "Alex"):
            self.assertEqual(normalize_given_name(ambiguous), ambiguous.lower())

    def test_placeholder_rules_are_conservative(self):
        for kind, value in [("email", "none@none.com"), ("email", "noemail@x.com"), ("phone", "0000000000"),
                            ("phone", "1234567890"), ("phone", "12345"), ("name", "unknown"), ("identifier", "0000"),
                            ("alnum_id", "111111111")]:
            self.assertTrue(is_placeholder(kind, value), (kind, value))
        for kind, value in [("email", "bob@x.com"), ("email", "ann.lee@gmail.com"), ("phone", "5125550123"),
                            ("name", "robert"), ("alnum_id", "123456789"),  # a real-looking tax ID must survive
                            ("identifier", "AB1234")]:
            self.assertFalse(is_placeholder(kind, value), (kind, value))

    def test_field_spec_placeholders_become_missing_before_the_default(self):
        spec = FieldSpec(("name",), default="n/a-default", anonymous=("acme",), anonymous_defaults=True)
        self.assertEqual(spec.apply("Unknown"), "n/a-default")  # default placeholder -> missing -> default
        self.assertEqual(spec.apply("ACME"), "n/a-default")     # explicit, compared after normalization
        self.assertEqual(spec.apply("Robert"), "robert")

    def test_sources_are_tried_in_order(self):
        cfg = BUILTIN_DOMAINS["customer"]
        self.assertEqual(cfg.standardizer({"last_name": "Park Jr"})["name_suffix"], "jr")
        self.assertEqual(cfg.standardizer({"last_name": "Park Jr", "name_suffix": "Sr"})["name_suffix"], "sr")  # explicit field wins


# ------------------------------------------------------- the measured defects, one by one
class TestEvidence(Base):
    def test_placeholders_never_create_a_match(self):
        for field, value in (("email", "none@none.com"), ("phone", "000-000-0000"), ("email", "unknown")):
            r = score("customer", customer(first_name="Ann", last_name="Lee", city="Reno", **{field: value}),
                      customer(first_name="Bob", last_name="Chen", city="Miami", **{field: value}))
            self.assertEqual(r.match_type, "fuzzy", (field, value))
            self.assertEqual(classify(r.score), "no_match", (field, value, r.score))

    def test_a_shared_family_email_with_different_first_names_goes_to_review_not_auto_merge(self):
        r = score("customer", customer(first_name="Ann", last_name="Lee", email="lee.family@x.com"),
                  customer(first_name="Bob", last_name="Lee", email="lee.family@x.com"))
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "review")
        self.assertTrue(any("first_name disagrees" in x for x in r.reasons))

    def test_same_name_and_address_but_different_email_and_phone_is_not_auto_merged(self):
        base = dict(first_name="Jon", last_name="Park", address_line1="12 Oak St", city="Austin", postal_code="73301")
        r = score("customer", customer(email="jon@x.com", phone="512-555-0001", **base),
                  customer(email="other@y.com", phone="512-555-0002", **base))
        self.assertEqual(classify(r.score), "review")

    def test_jr_and_sr_at_one_address_are_not_auto_merged(self):
        base = dict(first_name="Jon", address_line1="12 Oak St", city="Austin", postal_code="73301")
        r = score("customer", customer(last_name="Park Sr", **base), customer(last_name="Park Jr", **base))
        self.assertEqual(classify(r.score), "review")
        self.assertTrue(any("name_suffix disagrees" in x for x in r.reasons))

    def test_one_record_with_a_suffix_and_one_without_is_not_a_contradiction(self):
        base = dict(first_name="Jon", address_line1="12 Oak St", city="Austin", postal_code="73301")
        r = score("customer", customer(last_name="Park Jr", **base), customer(last_name="Park", **base))
        self.assertEqual(classify(r.score), "auto_merge")

    def test_names_a_string_matcher_cannot_tell_from_a_nickname_still_count_as_different(self):
        """Jaro-Winkler puts many genuinely different names between 0.4 and 0.6, so a low cutoff would call them
        'weak matches' and never penalize them. Take a real pair from that zone and check the disagreement fires."""
        from app.benchmark import FIRST_COMMON
        from app.matching import similarity
        names = [n.lower() for n in FIRST_COMMON]
        a, b = next((x, y) for x in names for y in names if x[0] != y[0] and 0.4 <= similarity(x, y) < 0.6)
        r = score("customer", customer(first_name=a, last_name="Lee", email="x@y.com"),
                  customer(first_name=b, last_name="Lee", email="x@y.com"))
        self.assertTrue(any("first_name disagrees" in x for x in r.reasons), (a, b))

    def test_a_nickname_with_a_shared_email_still_auto_merges(self):
        r = score("customer", customer(first_name="Bob", last_name="Smith", email="bob@x.com"),
                  customer(first_name="Robert", last_name="Smith", email="bob@x.com"))
        self.assertEqual(classify(r.score), "auto_merge")

    def test_a_second_identifier_differing_is_normal_once_one_has_matched(self):
        """People have several emails. A phone match with a different email must stay an auto-merge."""
        r = score("customer", customer(first_name="Ann", last_name="Lee", phone="512-555-0123", email="a@x.com"),
                  customer(first_name="Ann", last_name="Lee", phone="512-555-0123", email="b@y.com"))
        self.assertEqual(classify(r.score), "auto_merge")

    def test_missing_is_not_disagreement(self):
        base = dict(first_name="Ann", last_name="Lee", address_line1="12 Oak St", city="Austin", postal_code="73301")
        r = score("customer", customer(email="a@x.com", phone="512-555-0123", **base), customer(**base))
        self.assertEqual(classify(r.score), "auto_merge")
        self.assertFalse(any("disagrees" in x for x in r.reasons))

    def test_vendors_with_different_tax_ids_are_not_auto_merged_even_when_everything_else_matches(self):
        base = dict(company_name="Acme Supply LLC", address_line1="1 Dock Rd", city="Dallas", postal_code="75201", contact_phone="214-555-0100")
        r = score("vendor", {**base, "tax_id": "12-3456789"}, {**base, "tax_id": "98-7654321"})
        self.assertEqual(classify(r.score), "review")

    def test_exclusive_identifiers_count_even_when_another_identifier_matched(self):
        r = score("vendor", {"company_name": "Acme", "tax_id": "12-3456789", "contact_email": "ap@acme.com"},
                  {"company_name": "Acme", "tax_id": "98-7654321", "contact_email": "ap@acme.com"})
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "review")

    def test_a_product_with_the_same_sku_but_a_different_upc_is_a_different_product(self):
        r = score("product", {"sku": "W-1", "upc": "012345678905", "product_name": "Widget", "brand": "Acme"},
                  {"sku": "W-1", "upc": "012345678912", "product_name": "Widget", "brand": "Acme"})
        self.assertNotEqual(classify(r.score), "auto_merge")


class TestContributions(Base):
    PAIRS = [
        (customer(first_name="Jennifer", last_name="Nguyen", address_line1="45 Birch Ln", city="Austin", postal_code="73301"),
         customer(first_name="Jenna", last_name="Nguyen", address_line1="9 Elm Street", city="Austin", postal_code="73344")),
        (customer(first_name="Ann", last_name="Lee", email="x@y.com"), customer(first_name="Bob", last_name="Lee", email="x@y.com")),
        (customer(first_name="Jon", last_name="Park Sr", city="Austin"), customer(first_name="Jon", last_name="Park Jr", city="Austin")),
    ]

    def test_points_sum_to_the_score(self):
        for a, b in self.PAIRS:
            r = score("customer", a, b)
            self.assertAlmostEqual(sum(c["points"] for c in r.contributions), r.score, places=2, msg=(a, b))

    def test_every_contribution_is_complete_and_serializable(self):
        for a, b in self.PAIRS:
            for c in score("customer", a, b).contributions:
                self.assertEqual(set(c), {"field", "state", "similarity", "points"})
                self.assertIn(c["state"], {"agree", "partial", "disagree", "missing"})
        json.dumps([score("customer", *self.PAIRS[0]).contributions])

    def test_disagreements_appear_as_negative_points(self):
        r = score("customer", *self.PAIRS[2])
        self.assertTrue(any(c["field"] == "name_suffix" and c["points"] < 0 for c in r.contributions))

    def test_the_breakdown_is_stored_with_the_review_candidate(self):
        store = Store(self.dbpath)
        base = dict(first_name="Jon", address_line1="12 Oak St", city="Austin", postal_code="73301")
        ingest_record(store, "acme", "customer", "crm", "A", {"last_name": "Park Sr", **base})
        r = ingest_record(store, "acme", "customer", "crm", "B", {"last_name": "Park Jr", **base})
        self.assertEqual(len(r["queued_for_review"]), 1)
        (cand,) = store.list_candidates("acme", "customer", "pending")
        self.assertTrue(any(c["field"] == "name_suffix" and c["state"] == "disagree" for c in cand.contributions))
        self.assertEqual(store.get_candidate(cand.id, "acme", "customer").contributions, cand.contributions)


# ------------------------------------------------------------------------- blocking
class TestBlocking(Base):
    def keys(self, **kw):
        return blocking_keys(standardize("customer", kw), get_domain_config("customer"))

    def test_a_spelling_variant_and_a_surname_typo_still_share_a_key(self):
        a = self.keys(first_name="Robert", last_name="Smith", postal_code="94107")
        self.assertTrue(a & self.keys(first_name="Robert", last_name="Smyth", postal_code="94107"))  # phonetic
        self.assertTrue(a & self.keys(first_name="Robert", last_name="Zmith", postal_code="94107"))  # any typo: first name + zip

    def test_the_phonetic_key_works_on_its_own(self):
        """Different first names, so the first-name keys can't be what connects these: only the Soundex key can."""
        shared = self.keys(first_name="Ann", last_name="Smith", postal_code="94107") & \
            self.keys(first_name="Bea", last_name="Smyth", postal_code="94107")
        self.assertTrue(any("S530" in k for k in shared), shared)

    def test_a_transposed_phone_still_shares_a_key(self):
        self.assertTrue(self.keys(phone="512-555-0123") & self.keys(phone="512-555-0132"))

    def test_unrelated_people_do_not_share_keys(self):
        self.assertFalse(self.keys(first_name="Ann", last_name="Lee", postal_code="94107", phone="512-555-0123") &
                         self.keys(first_name="Zed", last_name="Wu", postal_code="10001", phone="917-555-8888"))

    def test_a_record_with_nothing_to_block_on_gets_the_fallback_key(self):
        self.assertEqual(self.keys(), {"__no_block__"})

    def test_placeholders_produce_no_blocking_key(self):
        self.assertFalse(any("none@none.com" in k for k in self.keys(email="none@none.com", first_name="A", last_name="B")))


class TestBucketCap(Base):
    def make_domain(self, **extra):
        doc = {"name": "capdom", "fields": {"email": "email", "name": "name"}, "deterministic_fields": ["email"],
               "fuzzy_weights": {"name": 1}, "blocking": [["email"]], "max_bucket_size": 10, **extra}
        register_domain(parse_domain_document(doc, "test"))

    def test_a_key_held_by_too_many_records_stops_being_used_and_says_so(self):
        self.make_domain()
        store = Store(self.dbpath)
        results = [ingest_record(store, "acme", "capdom", "s", f"R{i}", {"email": "shared@x.com", "name": f"n{i}"}) for i in range(13)]
        self.assertEqual(results[10]["skipped_overpopulated_keys"], [])           # 10 others: at the cap, still used
        self.assertEqual(results[11]["skipped_overpopulated_keys"], ["g0:shared@x.com"])  # 11 others: over it
        self.assertEqual(results[12]["auto_merged_with"], [])                      # ...so it is no longer searched

    def test_a_cap_never_hides_a_record_that_has_other_keys(self):
        self.make_domain(blocking=[["email"], ["name"]])
        store = Store(self.dbpath)
        for i in range(12):
            ingest_record(store, "acme", "capdom", "s", f"R{i}", {"email": "shared@x.com", "name": f"someone{i}"})
        r = ingest_record(store, "acme", "capdom", "s", "Z", {"email": "shared@x.com", "name": "someone3"})
        self.assertTrue(r["skipped_overpopulated_keys"])
        self.assertTrue(r["auto_merged_with"] or r["queued_for_review"])  # found through the name key instead

    def test_the_fallback_key_is_never_used_for_lookup(self):
        self.make_domain()
        store = Store(self.dbpath)
        ingest_record(store, "acme", "capdom", "s", "A", {})
        with unittest.mock.patch("app.pipeline.score_pair", wraps=score_pair) as spy:
            r = ingest_record(store, "acme", "capdom", "s", "B", {})
        self.assertEqual(spy.call_count, 0)  # nothing to look up, so nothing is even scored
        self.assertEqual((r["auto_merged_with"], r["queued_for_review"], r["skipped_overpopulated_keys"]), ([], [], []))

    def test_bucket_sizes_and_stats(self):
        self.make_domain(max_bucket_size=500)
        store = Store(self.dbpath)
        for i in range(5):
            ingest_record(store, "acme", "capdom", "s", f"R{i}", {"email": "shared@x.com", "name": f"n{i}"})
        ingest_record(store, "acme", "capdom", "s", "U", {"email": "unique@x.com", "name": "u"})
        self.assertEqual(store.blocking_bucket_sizes("acme", "capdom", {"g0:shared@x.com", "g0:nope"}), {"g0:shared@x.com": 5})
        st = store.blocking_key_stats("capdom", top_n=3)
        self.assertEqual((st["buckets"], st["key_rows"], st["biggest"]), (2, 6, 5))
        self.assertEqual(st["top"][0], ("acme", "g0:shared@x.com", 5))
        self.assertEqual(st["bands"], {"1": 1, "2-10": 1})


# --------------------------------------------------------------- the config vocabulary
class TestConfigVocabulary(Base):
    def doc(self, **extra):
        d = {"name": "thing", "fields": {"code": "identifier", "name": "name", "phone": "phone"},
             "deterministic_fields": ["code"], "fuzzy_weights": {"name": 1}, "blocking": [["code"]]}
        d.update(extra)
        return d

    def errors(self, doc):
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(doc, "t.json")
        return str(cm.exception)

    def test_anonymous_values_are_normalized_so_they_can_be_written_naturally(self):
        cfg = parse_domain_document(self.doc(fields={"code": {"normalize": "identifier", "anonymous": ["n-a ", "0 0"]},
                                                     "name": "name"}, fuzzy_weights={"name": 1}))
        self.assertEqual(cfg.field_specs["code"].anonymous, ("00", "NA"))
        self.assertEqual(cfg.standardizer({"code": "n-a"})["code"], "")

    def test_anonymous_defaults_need_a_kind_that_has_them(self):
        self.assertIn("no built-in placeholders", self.errors(self.doc(fields={"code": {"normalize": "upper", "anonymous_defaults": True}, "name": "name"})))

    def test_sources_must_be_raw_field_names(self):
        self.assertIn("sources", self.errors(self.doc(fields={"code": {"normalize": "identifier", "sources": ["Bad Name"]}, "name": "name"})))

    def test_mismatch_penalties_are_validated(self):
        self.assertIn("unknown field", self.errors(self.doc(mismatch_penalties={"nope": 0.2})))
        self.assertIn("greater than 0", self.errors(self.doc(mismatch_penalties={"name": 0})))
        self.assertIn("greater than 0", self.errors(self.doc(mismatch_penalties={"name": 1.5})))
        self.assertIn("'exact', 'similarity' or 'date'", self.errors(self.doc(mismatch_penalties={"name": {"penalty": 0.2, "compare": "fuzzy"}})))
        cfg = parse_domain_document(self.doc(mismatch_penalties={"name": {"penalty": 0.2, "compare": "exact"}, "phone": 0.1}))
        self.assertEqual((cfg.mismatch_penalties, cfg.mismatch_exact), ({"name": 0.2, "phone": 0.1}, ["name"]))

    def test_exclusive_fields_must_be_identifiers(self):
        self.assertIn("must also be listed in deterministic_fields", self.errors(self.doc(exclusive_fields=["name"])))

    def test_bucket_cap_bounds(self):
        for bad in (5, 2_000_000, True, "big"):
            self.assertIn("max_bucket_size", self.errors(self.doc(max_bucket_size=bad)))
        self.assertEqual(parse_domain_document(self.doc(max_bucket_size=50)).max_bucket_size, 50)

    def test_blocking_key_function_forms_and_errors(self):
        cfg = parse_domain_document(self.doc(blocking=[["code"], [{"field": "name", "as": "soundex"}, {"field": "phone", "as": "windows", "size": 4}]]))
        self.assertEqual(cfg.keyspecs()[1], [KeySpec("name", "soundex"), KeySpec("phone", "windows", 4)])
        self.assertEqual(cfg.blocking_field_groups, [["code"], ["name", "phone"]])
        self.assertIn("must be one of", self.errors(self.doc(blocking=[[{"field": "name", "as": "metaphone"}]])))
        self.assertIn("only applies to 'prefix' and 'windows'", self.errors(self.doc(blocking=[[{"field": "name", "as": "soundex", "size": 3}]])))
        self.assertIn("unknown key", self.errors(self.doc(blocking=[[{"field": "name", "kind": "soundex"}]])))
        self.assertIn("duplicate", self.errors(self.doc(blocking=[[{"field": "name", "as": "soundex"}], [{"field": "name", "as": "soundex"}]])))

    def test_the_same_field_can_appear_in_two_groups_with_different_key_functions(self):
        cfg = parse_domain_document(self.doc(blocking=[["name"], [{"field": "name", "as": "soundex"}]]))
        self.assertEqual(len(cfg.keyspecs()), 2)

    def test_warning_when_an_identifier_has_no_blocking_group_of_its_own(self):
        cfg = parse_domain_document(self.doc(blocking=[["name"]]))
        self.assertTrue(any("'code'" in w for w in cfg.warnings))
        self.assertEqual(parse_domain_document(self.doc()).warnings, [])

    def test_a_full_featured_document_round_trips_through_export(self):
        cfg = parse_domain_document(self.doc(
            fields={"code": {"normalize": "identifier", "anonymous_defaults": True, "anonymous": ["x"]}, "name": "name",
                    "phone": {"normalize": "phone", "sources": ["phone", "mobile"]}},
            exclusive_fields=["code"], mismatch_penalties={"name": {"penalty": 0.2, "compare": "exact"}, "phone": 0.1},
            max_bucket_size=77, blocking=[["code"], [{"field": "name", "as": "soundex"}, {"field": "phone", "as": "windows", "size": 4}]]))
        again = parse_domain_document(config_to_document(cfg))
        for attr in ("field_specs", "mismatch_penalties", "mismatch_exact", "exclusive_fields", "max_bucket_size", "blocking_field_groups"):
            self.assertEqual(getattr(again, attr), getattr(cfg, attr), attr)
        self.assertEqual(again.keyspecs(), cfg.keyspecs())


class TestFingerprintCoversKeyFunctions(Base):
    def cfg(self, blocking, **fields):
        d = {"name": "thing", "fields": {"code": "identifier", "name": "name", **fields}, "deterministic_fields": ["code"],
             "fuzzy_weights": {"name": 1}, "blocking": blocking}
        return parse_domain_document(d)

    def test_key_functions_change_the_fingerprint_and_auto_does_not(self):
        auto = self.cfg([["code"], ["name"]])
        self.assertEqual(blocking_fingerprint(auto), blocking_fingerprint(self.cfg([["code"], ["name"]])))
        self.assertNotEqual(blocking_fingerprint(auto), blocking_fingerprint(self.cfg([["code"], [{"field": "name", "as": "soundex"}]])))
        w4 = self.cfg([["code"], [{"field": "code", "as": "windows", "size": 4}]])
        w5 = self.cfg([["code"], [{"field": "code", "as": "windows", "size": 5}]])
        self.assertNotEqual(blocking_fingerprint(w4), blocking_fingerprint(w5))

    def test_placeholder_settings_matter_for_every_field_because_staging_stores_them(self):
        base = self.cfg([["code"]])
        d = {"name": "thing", "fields": {"code": {"normalize": "identifier", "anonymous_defaults": True}, "name": "name"},
             "deterministic_fields": ["code"], "fuzzy_weights": {"name": 1}, "blocking": [["code"]]}
        self.assertNotEqual(blocking_fingerprint(parse_domain_document(d)), blocking_fingerprint(base))
        d2 = {"name": "thing", "fields": {"code": "identifier", "name": {"normalize": "name", "anonymous_defaults": True}},
              "deterministic_fields": ["code"], "fuzzy_weights": {"name": 1}, "blocking": [["code"]]}
        self.assertNotEqual(blocking_fingerprint(parse_domain_document(d2)), blocking_fingerprint(base))  # not a blocking field: still staged


# --------------------------------------------------------------------------- the CLI
class TestCli(Base):
    def cli(self, *args, env=None):
        return subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), *args], capture_output=True, text=True,
                              cwd=ROOT, env=dict(os.environ, **(env or {})))

    def test_blocking_stats_flags_a_suspiciously_shared_value(self):
        store = Store(self.dbpath)
        for i in range(15):
            ingest_record(store, "acme", "customer", "crm", f"R{i}", {"first_name": f"P{i}", "last_name": f"Q{i}", "email": "weird@corp-mail.com"})
        out = self.cli("blocking-stats", "customer", "--db", self.dbpath)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("weird@corp-mail.com", out.stdout)
        self.assertIn("`anonymous` list", out.stdout)
        self.assertIn("records with NO usable blocking key", out.stdout)

    def test_blocking_stats_on_an_empty_domain_and_an_unknown_one(self):
        Store(self.dbpath)
        self.assertIn("no records yet", self.cli("blocking-stats", "vendor", "--db", self.dbpath).stdout)
        self.assertEqual(self.cli("blocking-stats", "nope", "--db", self.dbpath).returncode, 2)

    def test_validate_prints_warnings(self):
        path = os.path.join(self.tmp, "thing.json")
        with open(path, "w") as f:
            json.dump({"name": "thing", "fields": {"code": "identifier", "name": "name"}, "deterministic_fields": ["code"],
                       "fuzzy_weights": {"name": 1}, "blocking": [["name"]]}, f)
        out = self.cli("validate", path)
        self.assertEqual(out.returncode, 0)
        self.assertIn("warning:", out.stdout)
        self.assertIn("'code'", out.stdout)


if __name__ == "__main__":
    unittest.main()
