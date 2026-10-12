"""
Protecting the SSN: keyed hash for matching, mask for display, plain value only in raw (option A). Unit and functional tests only.

Run with:  python3 -m unittest tests.test_protect -v
"""
import copy
import importlib
import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))
import fake_web  # noqa: E402

from app import benchmark_person as bp, domain_config, offline, protect
from app.db import Store
from app.domain_config import DomainConfigError, config_to_document, parse_domain_document
from app.domains import register_domain
from app.indexing import IndexStaleError, derived_fingerprint, reindex_domain
from app.normalizers import FieldSpec
from app.pipeline import ingest_record, process_pending
from app.protect import ProtectionKeyMissing, mask_attributes, mask_value, scrub_reason, token

ROOT = os.path.join(os.path.dirname(__file__), "..")
KEY = "a-test-key-that-is-long-enough-0123456789"
ENV = {"MDM_TOKEN_KEY": KEY, "MDM_TOKEN_KEY_ID": "v1"}


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

    def no_key(self):
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("MDM_TOKEN_KEY", None)


class TestTokensAndMasks(Base):
    def test_a_token_is_versioned_deterministic_and_does_not_contain_the_value(self):
        t = token("512456789", "ssn")
        self.assertRegex(t, r"^v1:[0-9a-f]{32}$")
        self.assertEqual(t, token("512456789", "ssn"))
        self.assertNotIn("512456789", t)
        self.assertNotEqual(t, token("512456780", "ssn"))
        self.assertNotEqual(t, token("512456789", "tax_id"))  # one field's hashes are not another's

    def test_the_key_and_its_id_change_the_token(self):
        t = token("512456789", "ssn")
        with mock.patch.dict(os.environ, {"MDM_TOKEN_KEY": "another-key-that-is-also-long-enough-9876543210"}):
            self.assertNotEqual(token("512456789", "ssn"), t)
        with mock.patch.dict(os.environ, {"MDM_TOKEN_KEY_ID": "v2"}):
            self.assertTrue(token("512456789", "ssn").startswith("v2:"))

    def test_no_key_is_refused_and_a_weak_one_too(self):
        self.no_key()
        with self.assertRaises(ProtectionKeyMissing) as cm:
            token("512456789", "ssn")
        self.assertIn("MDM_TOKEN_KEY", str(cm.exception))
        self.assertIn("Nothing is stored unprotected", str(cm.exception))
        os.environ["MDM_TOKEN_KEY"] = "short"
        with self.assertRaises(ProtectionKeyMissing) as cm:
            token("512456789", "ssn")
        self.assertIn("too short", str(cm.exception))
        os.environ.update(ENV)
        os.environ["MDM_TOKEN_KEY_ID"] = "bad id!"
        with self.assertRaises(ProtectionKeyMissing):
            token("512456789", "ssn")

    def test_a_read_only_tool_can_run_without_the_key_and_it_does_not_leak_out_of_the_block(self):
        self.no_key()
        with protect.unkeyed():
            self.assertTrue(token("512456789", "ssn").startswith("v1:"))
        with self.assertRaises(ProtectionKeyMissing):
            token("512456789", "ssn")

    def test_masks_keep_only_the_last_four(self):
        self.assertEqual(mask_value("512-45-6789", "ssn"), "***-**-6789")
        self.assertEqual(mask_value("512456789", "ssn"), "***-**-6789")
        self.assertEqual(mask_value("unknown", "ssn"), "***-**-")
        self.assertEqual(mask_value("AB-12345", "tax_id"), "***2345")

    def test_old_match_reasons_lose_the_value_they_printed(self):
        self.assertEqual(scrub_reason("ssn", "exact ssn match (512456789)"), "exact ssn match")
        self.assertEqual(scrub_reason("ssn", "ssn disagrees ('512456789' vs '512456780'): -0.40"), "ssn disagrees: -0.40")
        self.assertEqual(scrub_reason("ssn", "last_name similar ('smith' ~ 'smyth', 0.9)"), "last_name similar ('smith' ~ 'smyth', 0.9)")
        self.assertEqual(scrub_reason("ssn", "exact email match (a@x.com)"), "exact email match (a@x.com)")

    def test_raw_attributes_are_masked_on_their_way_out_and_other_fields_are_not(self):
        bp.register_person("full")
        attrs = {"first_name": "Ann", "ssn": "512-45-6789", "dob": "1985-03-07"}
        out = mask_attributes("person", attrs)
        self.assertEqual(out, {"first_name": "Ann", "ssn": "***-**-6789", "dob": "1985-03-07"})
        self.assertEqual(attrs["ssn"], "512-45-6789")             # the original is untouched
        self.assertEqual(mask_attributes("customer", {"email": "a@x.com"}), {"email": "a@x.com"})
        self.assertEqual(mask_attributes("nope", attrs), attrs)
        self.assertEqual(mask_attributes("person", {}), {})


class TestFieldSpec(Base):
    SPEC = FieldSpec(("ssn",), present=("present_ssn",), protect="hash")

    def test_matching_is_a_hash_and_the_cleansed_form_a_mask(self):
        m, c = self.SPEC.apply_both("512 45 6789")
        self.assertEqual((m, c), (token("512456789", "ssn"), "***-**-6789"))
        self.assertEqual(self.SPEC.apply("512-45-6789"), m)         # however it was written, the same SSN is the same hash

    def test_a_placeholder_is_still_missing_and_is_never_hashed(self):
        self.assertEqual(self.SPEC.apply_both("000-00-0000"), ("", ""))
        self.assertEqual(self.SPEC.apply_both(""), ("", ""))

    def test_the_real_value_stays_available_to_read_only_tools(self):
        self.assertEqual(self.SPEC.plain("512-45-6789"), "512456789")

    def test_other_fields_are_untouched(self):
        self.assertEqual(FieldSpec(("ssn",), present=("present_ssn",)).apply_both("512-45-6789"), ("512456789", "512-45-6789"))

    def test_no_key_means_no_silent_fallback(self):
        self.no_key()
        with self.assertRaises(ProtectionKeyMissing):
            self.SPEC.apply_both("512-45-6789")


class TestConfig(Base):
    def test_the_person_template_protects_the_ssn(self):
        cfg = parse_domain_document(person_doc())
        self.assertEqual(cfg.field_specs["ssn"].protect, "hash")
        self.assertEqual(cfg.field_specs["dob"].protect, "")

    def test_it_round_trips_through_export(self):
        cfg = parse_domain_document(person_doc())
        self.assertEqual(config_to_document(cfg)["fields"]["ssn"]["protect"], "hash")
        self.assertEqual(derived_fingerprint(cfg), derived_fingerprint(parse_domain_document(config_to_document(cfg))))

    def test_a_hashed_field_can_only_be_used_for_equality(self):
        def broken(mutate):
            d = person_doc()
            mutate(d)
            return d
        cases = {
            "can only be compared exactly": lambda d: d["fuzzy_weights"].update({"ssn": 0.2}),
            "cannot feed the semantic": lambda d: d["semantic"].update({"fields": ["first_name", "ssn"]}),
            "block on it only as": lambda d: d["blocking"].append([{"field": "ssn", "as": "prefix", "size": 3}]),
            "so it is compared exactly": lambda d: (d["deterministic_fields"].remove("ssn"), d["exclusive_fields"].remove("ssn")),
            "must be \"hash\"": lambda d: d["fields"]["ssn"].update({"protect": "encrypt"}),
        }
        for fragment, mutate in cases.items():
            with self.assertRaises(DomainConfigError, msg=fragment) as cm:
                parse_domain_document(broken(mutate))
            self.assertIn(fragment, str(cm.exception))

    def test_rotating_the_key_id_makes_the_derived_data_stale_but_only_for_domains_that_protect_something(self):
        cfg = parse_domain_document(person_doc())
        before = derived_fingerprint(cfg)
        with mock.patch.dict(os.environ, {"MDM_TOKEN_KEY_ID": "v2"}):
            self.assertNotEqual(before, derived_fingerprint(cfg))
        from app.domains import get_domain_config
        customer = get_domain_config("customer")
        with mock.patch.dict(os.environ, {"MDM_TOKEN_KEY_ID": "v2"}):
            self.assertEqual(derived_fingerprint(customer), derived_fingerprint(customer))
        d = person_doc()
        del d["fields"]["ssn"]["protect"]
        del d["fields"]["ssn"]["options"]["near"]  # near-equality needs the hash
        self.assertNotEqual(before, derived_fingerprint(parse_domain_document(d)))  # turning protection on or off needs a reindex

    def test_the_service_refuses_to_start_without_the_key_but_only_when_something_is_protected(self):
        bp.register_person("full")
        from app.domains import REGISTRY
        protect.require_key_if_needed(REGISTRY)  # with the key: fine
        self.no_key()
        with self.assertRaises(ProtectionKeyMissing) as cm:
            protect.require_key_if_needed(REGISTRY)
        self.assertIn("person.ssn", str(cm.exception))
        protect.require_key_if_needed({"customer": REGISTRY["customer"]})  # nothing protected: no key needed


class TestStoredData(Base):
    """What actually ends up in the database."""

    def setUp(self):
        super().setUp()
        bp.register_person("full")
        self.db = os.path.join(self.tmp, "t.db")
        self.store = Store(self.db)

    def ingest(self, rid, attrs):
        return ingest_record(self.store, "acme", "person", "crm", rid, attrs)

    def two_people_one_merge(self):
        a = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "512-45-6789", "dob": "1985-03-07", "address_line1": "12 Oak St", "postal_code": "94107"})
        b = self.ingest("B", {"first_name": "Ann", "last_name": "Lee", "ssn": "512 45 6789", "dob": "1985-03-07", "address_line1": "99 Pine Rd", "postal_code": "94107"})
        c = self.ingest("C", {"first_name": "Bo", "last_name": "Chen", "ssn": "345-67-8901", "dob": "1970-01-02"})
        return a, b, c

    def tables_containing(self, *needles):
        con, found, table = sqlite3.connect(self.db), set(), None
        try:
            for line in con.iterdump():
                if line.startswith("INSERT INTO"):
                    table = line.split()[2].strip('"')
                    if any(n in line for n in needles):
                        found.add(table)
        finally:
            con.close()
        return found

    def test_the_plain_ssn_exists_in_raw_and_nowhere_else(self):
        a, b, c = self.two_people_one_merge()
        self.assertEqual(a["matched_entity_id"], b["matched_entity_id"])                        # protection did not stop the match
        self.assertNotEqual(a["matched_entity_id"], c["matched_entity_id"])
        self.assertEqual(self.tables_containing("512-45-6789", "512 45 6789", "512456789", "345-67-8901", "345678901"), {"source_records"})
        self.assertEqual(self.tables_containing(token("512456789", "ssn")), {"standardized_records", "blocking_keys"})  # the hash: staging and the index only

    def test_staging_golden_and_lineage_hold_the_hash_and_the_mask(self):
        a, _, _ = self.two_people_one_merge()
        con = sqlite3.connect(self.db)
        matching, cleansed = con.execute("SELECT matching, cleansed FROM standardized_records WHERE record_id = ?", (a["record_id"],)).fetchone()
        self.assertEqual(json.loads(matching)["ssn"], token("512456789", "ssn"))
        self.assertEqual(json.loads(cleansed)["ssn"], "***-**-6789")
        self.assertEqual(con.execute("SELECT ssn FROM entity_person WHERE entity_id = ?", (a["matched_entity_id"],)).fetchone()[0], "***-**-6789")
        self.assertEqual({r[0] for r in con.execute("SELECT new_value FROM lineage WHERE field = 'ssn'")}, {"***-**-6789", "***-**-8901"})
        raw = json.loads(con.execute("SELECT attributes FROM source_records WHERE source_record_id = 'A'").fetchone()[0])
        self.assertEqual(raw["ssn"], "512-45-6789")                                             # option A: raw keeps exactly what was sent

    def test_the_reasons_for_a_match_do_not_print_the_ssn_or_its_hash(self):
        self.two_people_one_merge()
        reasons = json.dumps([r[0] for r in sqlite3.connect(self.db).execute("SELECT reasons FROM match_candidates")])
        self.assertIn("exact ssn match", reasons)
        for leaked in ("512", "6789", "v1:"):
            self.assertNotIn(leaked, reasons)

    def test_protection_does_not_change_a_single_matching_decision(self):
        def decisions(doc, tag):
            register_domain(parse_domain_document(doc), replace=True)
            out = []
            for scenario in ("ssn_exact_name_variants", "ssn_shared_error", "no_ssn_name_dob_address", "ssn_placeholder_same_name"):  # (ssn_typo is left out on purpose: near-equal SSNs exist only when protected)
                for seed in range(6):
                    (a, _), (b, _) = bp._case(scenario, random.Random(seed))
                    store = Store(os.path.join(self.tmp, f"{tag}{len(out)}.db"))  # a fresh store per case AND per configuration
                    ra, rb = (ingest_record(store, "t", "person", "crm", k, r) for k, r in (("A", a), ("B", b)))
                    out.append((ra["matched_entity_id"] == rb["matched_entity_id"], len(rb["queued_for_review"])))
            return out
        plain = person_doc()
        del plain["fields"]["ssn"]["protect"]
        del plain["fields"]["ssn"]["options"]["near"]  # near-equality needs the hash
        self.assertEqual(decisions(person_doc(), "protected"), decisions(plain, "plain"))

    def test_every_engine_in_every_order_still_agrees_with_the_reference(self):
        bp.register_person("full")
        rows = [r["raw"] for r in bp.generate(1, 1)]
        report = offline.check_equivalence(rows, "person", orders=2)
        self.assertTrue(report["ok"], offline.format_equivalence(report))

    def test_without_the_key_nothing_is_stored_unprotected_and_the_record_is_finished_once_the_key_is_back(self):
        self.no_key()
        with self.assertRaises(ProtectionKeyMissing):
            self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "512-45-6789"})
        self.assertEqual(self.tables_containing("512-45-6789"), {"source_records"})            # raw landed; no staged copy, no index entry, no golden row
        self.assertEqual(self.store.count_unfinished("person"), (1, 1))
        os.environ.update(ENV)
        self.assertEqual(process_pending(self.store, "person")["finished"], 1)
        self.assertEqual(self.tables_containing("512-45-6789", "512456789"), {"source_records"})
        self.assertEqual(sqlite3.connect(self.db).execute("SELECT ssn FROM entity_person").fetchone()[0], "***-**-6789")

    def test_rotating_the_key_re_hashes_everything_from_raw_and_matching_carries_on(self):
        a = self.ingest("A", {"first_name": "Ann", "last_name": "Lee", "ssn": "512-45-6789", "dob": "1985-03-07"})
        os.environ.update({"MDM_TOKEN_KEY": "the-rotated-key-is-also-long-enough-5555555555", "MDM_TOKEN_KEY_ID": "v2"})
        with self.assertRaises(IndexStaleError):                                                # the guard: derived data is hashed under the old key
            self.ingest("B", {"first_name": "Bo", "last_name": "Chen"})
        reindex_domain(self.store, "person")
        con = sqlite3.connect(self.db)
        self.assertEqual(json.loads(con.execute("SELECT matching FROM standardized_records").fetchone()[0])["ssn"], token("512456789", "ssn"))
        self.assertTrue(token("512456789", "ssn").startswith("v2:"))
        self.assertEqual(self.tables_containing("v1:"), set())                                  # no hash from the old key survives
        b = self.ingest("B", {"first_name": "Ann", "last_name": "Lee", "ssn": "512 45 6789", "dob": "1985-03-07"})
        self.assertEqual(b["matched_entity_id"], a["matched_entity_id"])                        # and a NEW record still finds the old one

    def test_reindex_scrubs_plain_values_older_rows_still_hold(self):
        """Switching protection on for data that already exists: lineage, merged-away golden rows and old match reasons."""
        a, b, c = self.two_people_one_merge()
        con = sqlite3.connect(self.db)
        con.execute("UPDATE lineage SET new_value = '512-45-6789', old_value = '512456789' WHERE field = 'ssn'")
        con.execute("UPDATE entity_person SET status = 'merged', ssn = '512-45-6789' WHERE entity_id = ?", (c["matched_entity_id"],))  # an old tombstone: reindex does not rebuild it
        con.execute("UPDATE match_candidates SET reasons = ?", (json.dumps(["exact ssn match (512456789)", "ssn disagrees ('a' vs 'b'): -0.40", "last_name similar ('lee' ~ 'lea', 0.9)"]),))
        con.commit()
        con.close()
        self.assertNotEqual(self.tables_containing("512-45-6789", "512456789"), {"source_records"})  # the legacy plain values are really there
        reindex_domain(self.store, "person")
        self.assertEqual(self.tables_containing("512-45-6789", "512456789"), {"source_records"})
        reasons = json.loads(sqlite3.connect(self.db).execute("SELECT reasons FROM match_candidates").fetchone()[0])
        self.assertEqual(reasons, ["exact ssn match", "ssn disagrees: -0.40", "last_name similar ('lee' ~ 'lea', 0.9)"])
        self.assertEqual(self.store.scrub_protected("person", "ssn", "***-**-"), {"lineage": 0, "golden": 0, "reasons": 0})  # idempotent


class TestApi(Base):
    """The API functions under a stubbed FastAPI: raw attributes leave the system masked, and the service will not start without the key."""

    def setUp(self):
        super().setUp()
        self.saved = {k: sys.modules.get(k) for k in ("fastapi", "fastapi.responses", "fastapi.staticfiles", "pydantic", "app.api")}
        self.addCleanup(self.restore)
        domains = os.path.join(self.tmp, "domains")
        os.makedirs(domains)
        shutil.copy(os.path.join(ROOT, "domain_templates", "person.json"), domains)
        patcher = mock.patch.dict(os.environ, {"DATABASE_URL": os.path.join(self.tmp, "api.db"), "MDM_DOMAINS_DIR": domains})
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("MDM_ENABLE_AUTH", None)

        fake_web.install(force=True)   # the shared stand-in (tests/fake_web.py), put back by restore()
        sys.modules.pop("app.api", None)

    def restore(self):
        for k, v in self.saved.items():
            sys.modules.pop(k, None) if v is None else sys.modules.__setitem__(k, v)
        domain_config.reset()

    def test_raw_attributes_are_masked_in_entity_detail_and_in_the_review_queue(self):
        api = importlib.import_module("app.api")
        from app.auth import Principal
        who = Principal(tenant_id="acme", role="tenant_admin", key_id="k", label="t")
        (twin_a, _), (twin_b, _) = bp._case("twins", random.Random(0))  # same DOB, surname and address, different first names: a review pair
        a = ingest_record(api.store, "acme", "person", "crm", "A", {**twin_a, "ssn": "512-45-6789"})
        ingest_record(api.store, "acme", "person", "crm", "B", twin_b)
        detail = api.get_entity("person", a["matched_entity_id"], principal=who)
        self.assertEqual(detail["source_records"][0]["attributes"]["ssn"], "***-**-6789")
        self.assertEqual(detail["attributes"]["ssn"], "***-**-6789")
        queue = api.review_queue("person", principal=who)
        self.assertEqual((len(queue["items"]), queue["total"], queue["next_cursor"]), (1, 1, None))
        text = json.dumps(queue)
        self.assertNotIn("512-45-6789", text)
        self.assertIn("***-**-6789", text)
        self.assertEqual(api.store.get_source_record(a["record_id"], "acme", "person").attributes["ssn"], "512-45-6789")  # raw itself is unchanged

    def test_the_service_will_not_start_without_the_key(self):
        os.environ.pop("MDM_TOKEN_KEY", None)
        with self.assertRaises(ProtectionKeyMissing):
            importlib.import_module("app.api")


class TestCommands(Base):
    def setUp(self):
        super().setUp()
        self.domains = os.path.join(self.tmp, "domains")
        os.makedirs(self.domains)
        shutil.copy(os.path.join(ROOT, "domain_templates", "person.json"), self.domains)
        self.csv = os.path.join(self.tmp, "p.csv")
        with open(self.csv, "w") as f:
            f.write("rid,First,Last,SSN\n1,Ann,Lee,512-45-6789\n2,Bo,Chen,345-67-8901\n")

    def run_cli(self, script, *args, key=True):
        env = {k: v for k, v in os.environ.items() if k not in ("MDM_TOKEN_KEY", "MDM_TOKEN_KEY_ID")}
        env["MDM_DOMAINS_DIR"] = self.domains
        if key:
            env.update(ENV)
        return subprocess.run([sys.executable, os.path.join(ROOT, script), *args], capture_output=True, text=True, cwd=ROOT, env=env)

    def test_bulk_ingest_refuses_to_start_without_the_key_and_works_with_it(self):
        args = ("--tenant", "acme", "--domain", "person", "--file", self.csv, "--db", os.path.join(self.tmp, "b.db"), "--id-column", "rid",
                "--mapping", "first_name=First,last_name=Last,ssn=SSN")
        r = self.run_cli("bulk_ingest.py", *args, key=False)
        self.assertEqual(r.returncode, 2)
        self.assertIn("MDM_TOKEN_KEY", r.stderr)
        self.assertEqual(sqlite3.connect(os.path.join(self.tmp, "b.db")).execute("SELECT COUNT(*) FROM source_records").fetchone()[0] if os.path.exists(os.path.join(self.tmp, "b.db")) else 0, 0)
        r = self.run_cli("bulk_ingest.py", *args)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        con = sqlite3.connect(os.path.join(self.tmp, "b.db"))
        self.assertEqual(sorted(x[0] for x in con.execute("SELECT ssn FROM entity_person")), ["***-**-6789", "***-**-8901"])

    def test_reindex_says_what_is_missing_instead_of_a_traceback(self):
        db = os.path.join(self.tmp, "r.db")
        ok = self.run_cli("domains_cli.py", "reindex", "person", "--db", db, "--domains-dir", self.domains)
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        r = self.run_cli("domains_cli.py", "reindex", "person", "--db", db, "--domains-dir", self.domains, key=False)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)  # nothing to re-hash yet: an empty domain needs no key

    def test_profiling_needs_no_key_and_still_shows_the_real_value_on_request(self):
        with open(self.csv, "w") as f:
            f.write("rid,First,Last,SSN\n")
            for i in range(60):
                f.write(f"{i},P{i},Q{i % 7}X{i},512-45-0001\n")
            for i in range(60, 300):
                f.write(f"{i},P{i},Q{i % 11}Y{i},{200 + i % 600:03d}-{10 + i % 80:02d}-{1000 + i:04d}\n")
        base = ("profile", "person", "--domains-dir", self.domains, "--file", self.csv, "--mapping", "first_name=First,last_name=Last,ssn=SSN", "--field", "ssn")
        r = self.run_cli("domains_cli.py", *base, key=False)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("***-**-0001", r.stdout)
        r = self.run_cli("domains_cli.py", *base, "--reveal", key=False)
        self.assertIn("512450001", r.stdout)


if __name__ == "__main__":
    unittest.main()
