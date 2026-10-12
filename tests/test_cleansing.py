"""
SSN and date-of-birth cleansing: unit tests of the rules, and functional tests of what they do to matching and golden records.
No performance or scale tests here.

Run with:  python3 -m unittest tests.test_cleansing -v
"""
import datetime
import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark, benchmark_person as bp, domain_config
from app.db import Store
from app.domain_config import DomainConfigError, config_to_document, parse_domain_document
from app.domains import register_domain
from app.indexing import IndexStaleError, derived_fingerprint, reindex_domain
from app.normalizers import DIAGNOSERS, NORMALIZERS, PRESENTERS, FieldSpec, check_dob, check_ssn
from app.matching import classify, score_pair
from app.pipeline import ingest_batch, ingest_record
from app.profile import profile_records
from app.standardize import standardize
from app.staging import cleansing_issues

ROOT = os.path.join(os.path.dirname(__file__), "..")
TODAY = datetime.date(2026, 10, 7)


def person_doc():
    with open(os.path.join(ROOT, "domain_templates", "person.json")) as f:
        return json.load(f)


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)


class TestSsnRules(Base):
    def test_a_plausible_ssn_is_nine_digits_however_it_was_written(self):
        for raw in ("512-45-6789", "512 45 6789", "512456789", " 512-45-6789 "):
            self.assertEqual(check_ssn(raw), ("512456789", None), raw)
        self.assertEqual(check_ssn("001-01-0001"), ("001010001", None))  # leading zeros are real

    def test_each_structural_rule_rejects_with_its_own_reason(self):
        cases = {"000-12-3456": "area 000", "666-12-3456": "area 666", "900-12-3456": "area 9xx (not a valid SSN)", "123-00-4567": "group 00",
                 "123-45-0000": "serial 0000", "111-11-1111": "all the same digit", "999999999": "all the same digit", "000000000": "all the same digit",
                 "123-45-6789": "a widely published example number", "987654321": "a widely published example number",
                 "078-05-1120": "a widely published example number", "219-09-9999": "a widely published example number",
                 "457-55-5462": "a widely published example number", "12345678": "not 9 digits", "1234567890": "not 9 digits",
                 "unknown": "no digits", "N/A": "no digits"}
        for raw, reason in cases.items():
            self.assertEqual(check_ssn(raw), ("", reason), raw)

    def test_nothing_there_is_not_a_rejection(self):
        for raw in ("", "   ", None):
            self.assertEqual(check_ssn(raw), ("", None))

    def test_a_short_number_is_never_padded_into_a_valid_one(self):
        self.assertEqual(check_ssn("12345678")[0], "")  # a spreadsheet dropped a leading zero: we do not guess which

    def test_itins_are_rejected_unless_the_field_allows_them(self):
        self.assertEqual(check_ssn("912-70-1234")[1], "area 9xx (not a valid SSN)")
        self.assertEqual(check_ssn("912-70-1234", itin_ok=True), ("912701234", None))
        self.assertEqual(check_ssn("912-65-1234", itin_ok=True)[0], "912651234")
        self.assertEqual(check_ssn("912-69-1234", itin_ok=True)[1], "area 9xx (not a valid SSN)")  # group 69 is not an ITIN group
        self.assertEqual(check_ssn("000-70-1234", itin_ok=True)[1], "area 000")


class TestDobRules(Base):
    def dob(self, raw, parse=(), **kw):
        return check_dob(raw, parse, today=TODAY, **kw)

    def test_iso_is_always_read_and_other_formats_only_when_the_config_declares_them(self):
        self.assertEqual(self.dob("1985-03-07"), ("1985-03-07", None))
        self.assertEqual(self.dob("03/07/1985"), ("", "not a real date in a declared format"))
        self.assertEqual(self.dob("03/07/1985", ("%m/%d/%Y",)), ("1985-03-07", None))
        self.assertEqual(self.dob("03/07/1985", ("%d/%m/%Y",)), ("1985-07-03", None))  # the config decides which, not a guess
        self.assertEqual(self.dob("Mar 7, 1985", ("%b %d, %Y",)), ("1985-03-07", None))

    def test_impossible_dates_are_rejected(self):
        for raw in ("2023-02-30", "1985-13-01", "1985-00-10", "not a date"):
            self.assertEqual(self.dob(raw)[0], "", raw)
        self.assertEqual(self.dob("2000-02-29")[0], "2000-02-29")
        self.assertEqual(self.dob("2001-02-29")[0], "")

    def test_system_default_dates_are_rejected(self):
        for raw in ("1900-01-01", "1899-12-30", "1970-01-01", "1800-01-01", "1753-01-01", "0001-01-01", "1111-11-11", "9999-12-31"):
            self.assertEqual(self.dob(raw), ("", "a default or system date" if raw not in ("1800-01-01",) else "a default or system date"), raw)

    def test_future_and_implausible_dates_are_rejected_and_today_is_not(self):
        self.assertEqual(self.dob("2026-10-08"), ("", "in the future"))
        self.assertEqual(self.dob("2026-10-07")[0], "2026-10-07")
        self.assertEqual(self.dob("1890-05-05"), ("", "implausible age"))               # 120-year default: 1906 is the earliest year
        self.assertEqual(self.dob("1890-05-05", max_age_years=150)[0], "1890-05-05")
        self.assertEqual(self.dob("1949-12-31", min_year=1950), ("", "implausible age"))

    def test_the_config_can_add_its_own_default_dates(self):
        self.assertEqual(self.dob("1960-06-15")[0], "1960-06-15")
        self.assertEqual(self.dob("1960-06-15", defaults=("1960-06-15",)), ("", "a default or system date"))

    def test_a_bare_year_is_kept_for_matching_with_a_note(self):
        value, note = self.dob("1985")
        self.assertEqual(value, "1985")
        self.assertIn("year only", note)
        self.assertEqual(self.dob("1850"), ("", "implausible year"))
        self.assertEqual(self.dob("2030"), ("", "implausible year"))

    def test_nothing_there_is_not_a_rejection(self):
        for raw in ("", "  ", None):
            self.assertEqual(self.dob(raw), ("", None))


class TestFieldSpecs(Base):
    def test_an_ssn_field_standardizes_and_presents(self):
        spec = FieldSpec(("ssn",), present=("present_ssn",))
        self.assertEqual(spec.apply_both("512 45 6789"), ("512456789", "512-45-6789"))
        self.assertEqual(spec.apply_both("000-00-0000"), ("", ""))     # a placeholder is missing in BOTH forms: it never reaches a golden record
        self.assertEqual(spec.apply("123-45-6789"), "")

    def test_a_dob_field_reads_declared_formats_and_presents_iso(self):
        spec = FieldSpec(("dob",), present=("present_date",), parse=("%m/%d/%Y",))
        self.assertEqual(spec.apply_both("03/07/1985"), ("1985-03-07", "1985-03-07"))
        self.assertEqual(spec.apply_both("1900-01-01"), ("", ""))
        self.assertEqual(spec.apply_both("1985"), ("1985", ""))        # matched on, but not a golden date

    def test_options_reach_the_rules(self):
        self.assertEqual(FieldSpec(("ssn",)).apply("912-70-1234"), "")
        self.assertEqual(FieldSpec(("ssn",), options=(("itin", "allow"),)).apply("912-70-1234"), "912701234")
        self.assertEqual(FieldSpec(("dob",), options=(("defaults", ("1960-06-15",)),)).apply("1960-06-15"), "")

    def test_diagnose_explains_a_discard_without_the_value(self):
        spec = FieldSpec(("ssn",))
        self.assertEqual(spec.diagnose("000-00-0000"), "all the same digit")
        self.assertIsNone(spec.diagnose("512-45-6789"))
        self.assertIsNone(spec.diagnose(""))
        self.assertTrue(spec.reports_rejections)
        self.assertFalse(FieldSpec(("text",)).reports_rejections)
        self.assertEqual(FieldSpec(("ssn",), anonymous=("512456789",)).diagnose("512-45-6789"), "listed as a placeholder in the domain config")

    def test_the_vocabulary_still_works_one_string_at_a_time(self):
        for name in ("ssn", "dob"):
            self.assertIsInstance(NORMALIZERS[name]("  Foo-Bar_1  "), str)
        for name in ("present_ssn", "present_date"):
            self.assertIsInstance(PRESENTERS[name]("  Foo-Bar_1  "), str)
        self.assertEqual(set(DIAGNOSERS), {"ssn", "dob"})

    def test_built_in_domains_report_nothing_new(self):
        from app.domains import get_domain_config
        cfg = get_domain_config("customer")
        self.assertEqual(cleansing_issues(cfg, {"first_name": "Ann", "email": "none@none.com", "phone": "000-000-0000"}, "r1"), [])
        self.assertFalse(any(s.reports_rejections for s in cfg.field_specs.values()))


class TestConfig(Base):
    def doc(self, **field_overrides):
        d = person_doc()
        d["fields"].update(field_overrides)
        return d

    def test_the_person_template_parses_with_its_rules(self):
        cfg = parse_domain_document(self.doc())
        self.assertEqual(cfg.field_specs["ssn"].normalize, ("ssn",))
        self.assertEqual(cfg.field_specs["dob"].parse[0], "%m/%d/%Y")
        self.assertEqual(cfg.field_specs["dob"].opt("max_age_years"), 120)

    def test_it_round_trips_through_export_and_back(self):
        cfg = parse_domain_document(self.doc())
        again = parse_domain_document(config_to_document(cfg))
        self.assertEqual(derived_fingerprint(cfg), derived_fingerprint(again))
        self.assertEqual(again.field_specs["dob"].parse, cfg.field_specs["dob"].parse)
        self.assertEqual(again.field_specs["ssn"].options, cfg.field_specs["ssn"].options)

    def test_bad_options_are_refused_with_a_clear_message(self):
        bad = {
            "unknown option": {"normalize": ["ssn"], "options": {"strict": True}},
            "itin must be": {"normalize": ["ssn"], "options": {"itin": "maybe"}},
            "max_age_years must": {"normalize": ["dob"], "options": {"max_age_years": 0}},
            "defaults must be": {"normalize": ["dob"], "options": {"defaults": ["1950-1-1"]}},
            "take no options": {"normalize": ["text"], "options": {"itin": "allow"}},
            "only applies to a field with the 'dob'": {"normalize": ["ssn"], "parse": ["%m/%d/%Y"]},
            "not a valid strptime": {"normalize": ["dob"], "parse": ["%Q-nonsense"]},
            "must be an object": {"normalize": ["ssn"], "options": ["itin"]},
        }
        for fragment, spec in bad.items():
            with self.assertRaises(DomainConfigError, msg=fragment) as cm:
                parse_domain_document(self.doc(ssn=spec))
            self.assertIn(fragment, str(cm.exception))

    def test_changing_a_rule_makes_the_derived_data_stale(self):
        """So the stale-index guard stops ingestion until `reindex` re-derives staging and golden records from raw."""
        base = derived_fingerprint(parse_domain_document(self.doc()))
        itin = self.doc(ssn={"normalize": ["ssn"], "present": ["present_ssn"], "options": {"itin": "allow"}})
        self.assertNotEqual(base, derived_fingerprint(parse_domain_document(itin)))
        d = self.doc()
        d["fields"]["dob"]["parse"] = ["%d/%m/%Y"]
        self.assertNotEqual(base, derived_fingerprint(parse_domain_document(d)))

    def test_the_vocabulary_lists_the_new_steps(self):
        out = subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "normalizers"], capture_output=True, text=True, cwd=ROOT).stdout
        for word in ("ssn ", "dob ", "present_ssn", "present_date"):
            self.assertIn(word, out)


class TestGoldenRecords(Base):
    def setUp(self):
        super().setUp()
        bp.register_person("full")
        self.store = Store(os.path.join(self.tmp, "t.db"))

    def ingest(self, rid, attrs):
        return ingest_record(self.store, "acme", "person", "crm", rid, attrs)

    def entity(self, result):
        c = sqlite3.connect(os.path.join(self.tmp, "t.db"))
        c.row_factory = sqlite3.Row
        try:
            return dict(c.execute("SELECT * FROM entity_person WHERE entity_id = ?", (result["matched_entity_id"],)).fetchone())
        finally:
            c.close()

    def test_placeholders_never_reach_the_golden_record_and_the_reason_is_recorded_without_the_value(self):
        r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "000-00-0000", "dob": "1900-01-01", "postal_code": "94107"})
        e = self.entity(r)
        self.assertIsNone(e["ssn"])
        self.assertIsNone(e["dob"])
        issues = json.loads(e["quality_issues"])
        self.assertEqual({(i["field"], i["type"]) for i in issues}, {("ssn", "cleansing"), ("dob", "cleansing")})
        self.assertEqual({i["problem"] for i in issues}, {"all the same digit", "a default or system date"})
        text = json.dumps(issues)
        for leaked in ("000", "1900", "0000"):
            self.assertNotIn(leaked, text)

    def test_a_real_ssn_and_dob_are_cleansed_and_typed(self):
        r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "512 45 6789", "dob": "03/07/1985"})
        e = self.entity(r)
        self.assertEqual((e["ssn"], e["dob"]), ("***-**-6789", "1985-03-07"))  # the SSN is protected (masked in golden); the date, written in a declared format, is ISO
        self.assertEqual(json.loads(e["quality_issues"] or "[]"), [])

    def test_raw_is_never_rewritten(self):
        self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "000-00-0000", "dob": "1900-01-01"})
        attrs = json.loads(sqlite3.connect(os.path.join(self.tmp, "t.db")).execute("SELECT attributes FROM source_records").fetchone()[0])
        self.assertEqual((attrs["ssn"], attrs["dob"]), ("000-00-0000", "1900-01-01"))

    def test_a_year_only_dob_is_matched_on_but_is_not_a_golden_date(self):
        r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "dob": "1985"})
        e = self.entity(r)
        self.assertIsNone(e["dob"])
        issues = json.loads(e["quality_issues"])
        self.assertEqual([i["field"] for i in issues], ["dob"])
        self.assertIn("year only", issues[0]["problem"])
        self.assertNotIn("1985", json.dumps(issues))
        staged = sqlite3.connect(os.path.join(self.tmp, "t.db")).execute("SELECT matching FROM standardized_records").fetchone()[0]
        self.assertEqual(json.loads(staged)["dob"], "1985")

    def test_the_same_person_written_two_ways_is_still_the_same_person(self):
        a = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "dob": "03/07/1985", "address_line1": "12 Oak St", "postal_code": "94107", "ssn": "512-45-6789"})
        b = self.ingest("B", {"first_name": "Ann", "last_name": "Lee", "dob": "1985-03-07", "address_line1": "12 Oak Street", "postal_code": "94107", "ssn": "512456789"})
        self.assertEqual(a["matched_entity_id"], b["matched_entity_id"])

    def test_changing_a_rule_is_applied_to_data_already_loaded_by_reindex(self):
        r = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "dob": "1960-06-15"})
        self.assertEqual(self.entity(r)["dob"], "1960-06-15")
        doc = person_doc()
        doc["fields"]["dob"]["options"]["defaults"] = ["1960-06-15"]  # this source stamps 1960-06-15 on every record nobody filled in
        register_domain(parse_domain_document(doc), replace=True)
        with self.assertRaises(IndexStaleError):
            self.ingest("B", {"first_name": "Bo", "last_name": "Chen"})
        reindex_domain(self.store, "person")
        e = self.entity(r)
        self.assertIsNone(e["dob"])
        self.assertIn("a default or system date", e["quality_issues"])


class TestMatchingBeforeAndAfter(Base):
    """The point of cleansing, shown on the benchmark's own labelled cases: the same data through the same engine, with and without it."""

    def cases(self, scenario, variant, n=12):
        bp.register_person(variant)
        out = []
        for seed in range(n):
            (a, _), (b, _) = bp._case(scenario, random.Random(seed))
            store = Store(os.path.join(self.tmp, f"{variant.replace(':', '-')}-{scenario}-{seed}.db"))  # one fresh store per case: cases must not meet
            ra = ingest_record(store, "t", "person", "crm", "A", a)
            rb = ingest_record(store, "t", "person", "crm", "B", b)
            out.append({"merged": ra["matched_entity_id"] == rb["matched_entity_id"], "queued": len(rb["queued_for_review"])})
        return out

    BUILT_IN_DEFAULTS = {"1900-01-01", "1970-01-01", "0001-01-01", "9999-12-31"}

    def pairs(self, n=40):
        return [bp._case("dob_placeholder_same_name", random.Random(seed)) for seed in range(n)]

    def auto_merged(self, cases):
        """Which of the labelled different-people pairs the engine would merge on its own (score >= the auto-merge threshold)."""
        from app.domains import get_domain_config
        cfg = get_domain_config("person")
        return [classify(score_pair(standardize("person", a), standardize("person", b), cfg).score) == "auto_merge" for (a, _), (b, _) in cases]

    def test_strangers_with_a_structurally_defaulted_dob_no_longer_merge(self):
        cases = self.pairs()
        bp.register_person("full:baseline")
        before = self.auto_merged(cases)
        bp.register_person("full")
        after = self.auto_merged(cases)
        self.assertGreaterEqual(sum(before), 25)  # measured: 31 of 40 were wrongly merged without cleansing
        self.assertLess(sum(after), sum(before) // 3)
        for case, merged in zip(cases, after):
            if case[0][0]["dob"] in self.BUILT_IN_DEFAULTS:
                self.assertFalse(merged, case[0][0]["dob"])  # not one of them survives

    def test_what_remains_is_a_default_that_looks_like_a_real_date_and_profiling_finds_it(self):
        """2000-01-01 is deliberately NOT a built-in default (a real person can be born that day, and a false positive silently
        discards real data). A source that stamps it on every unfilled record is exactly what `profile` is for."""
        cases = self.pairs()
        bp.register_person("full")
        merged = self.auto_merged(cases)
        leftovers = {case[0][0]["dob"] for case, m in zip(cases, merged) if m}
        self.assertEqual(leftovers, {"2000-01-01"})

        rng = random.Random(7)  # the source's file: ordinary people, and 40 different people who all carry the date nobody filled in
        records = [bp.make_person(rng, 1, 1, 1, 1) for _ in range(300)] + [dict(bp.make_person(rng, 1, 1, 1, 1), dob="2000-01-01") for _ in range(40)]
        report = profile_records("person", records, fields=["dob"], min_count=10, reveal=True)
        self.assertEqual([x["value"] for x in report["fields"]["dob"]["suspects"]], ["2000-01-01"])
        snippet = report["config_snippet"]["fields"]["dob"]
        self.assertEqual(snippet, {"anonymous": ["2000-01-01"]})

        doc = person_doc()
        doc["fields"]["dob"].update(snippet)  # the person reviews the suspect and pastes the suggestion
        register_domain(parse_domain_document(doc), replace=True)
        self.assertEqual(sum(self.auto_merged(cases)), 0)  # and the false merges are gone

    def test_strangers_sharing_a_junk_ssn_stop_cluttering_the_review_queue(self):
        before = self.cases("ssn_placeholder_same_name", "full:baseline")
        after = self.cases("ssn_placeholder_same_name", "full")
        self.assertGreater(sum(c["queued"] for c in before), 0)
        self.assertEqual(sum(c["queued"] for c in after), 0)
        self.assertEqual(sum(c["merged"] for c in after), 0)

    def test_real_matches_are_not_lost(self):
        for scenario in ("ssn_exact_name_variants", "nickname_dob", "no_ssn_name_dob_address"):
            before = self.cases(scenario, "full:baseline", 8)
            after = self.cases(scenario, "full", 8)
            self.assertEqual([c["merged"] for c in after], [c["merged"] for c in before], scenario)
            self.assertTrue(all(c["merged"] for c in after), scenario)

    def test_the_benchmark_scenarios_agree(self):
        agg = benchmark.run([1], scale=6, label="t", profile="person", variant="full")["scenarios"]
        self.assertEqual(agg["ssn_placeholder_same_name"]["queued"], 0)
        self.assertEqual(agg["ssn_exact_name_variants"]["merged"], agg["ssn_exact_name_variants"]["pairs"])
        self.assertEqual(agg["jr_sr_dob"]["merged"], 0)
        self.assertEqual(agg["ssn_shared_error"]["merged"], 0)

    def test_a_batch_cleanses_exactly_like_one_record_at_a_time(self):
        bp.register_person("full")
        rows = [("crm", f"R{i}", {"first_name": f"P{i}", "last_name": "Wu", "ssn": "000-00-0000" if i % 2 else f"512-45-{1000 + i}", "dob": "1900-01-01" if i % 3 else "1985-03-07"}) for i in range(6)]
        store = Store(os.path.join(self.tmp, "b.db"))
        ingest_batch(store, "t", "person", rows)
        c = sqlite3.connect(os.path.join(self.tmp, "b.db"))
        golden = c.execute("SELECT COUNT(*) FROM entity_person WHERE ssn IS NOT NULL").fetchone()[0], c.execute("SELECT COUNT(*) FROM entity_person WHERE dob IS NOT NULL").fetchone()[0]
        self.assertEqual(golden, (3, 2))


if __name__ == "__main__":
    unittest.main()
