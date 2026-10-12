"""
The staging layer: raw -> standardized (matching + cleansed) -> matching -> golden. The point of this layer is that
golden records are derived from CLEANSED data, never raw, and that everything above raw can be rebuilt.

Run with:  python3 -m unittest tests.test_staging -v
"""
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.compliance import purge_entity
from app.db import Store
from app.domain_config import config_to_document, parse_domain_document
from app.domains import BUILTIN_DOMAINS, get_domain_config, register_domain
from app.indexing import IndexStaleError, derived_fingerprint, index_state, reindex_domain
from app.normalizers import PRESENTERS, smart_title
from app.pipeline import ingest_batch, ingest_record
from app.staging import stage_record
from app.standardize import standardize

MESSY = {"first_name": "  bob ", "last_name": "SMITH Jr.", "email": " Bob@X.COM ", "phone": "512.555.0123",
         "address_line1": "123 MAIN STREET", "city": "SAN  FRANCISCO", "state": "ca", "postal_code": "941071234"}


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.dbpath = os.path.join(self.tmp, "t.db")
        self.store = Store(self.dbpath)
        self.cfg = get_domain_config("customer")

    def sql(self, query, params=()):
        c = sqlite3.connect(self.dbpath)
        try:
            rows = c.execute(query, params).fetchall()
            c.commit()
            return rows
        finally:
            c.close()

    def golden(self, result):
        return self.store.get_entity(result["matched_entity_id"], "acme", "customer").attributes


class TestPresenters(Base):
    def test_re_casing_only_happens_to_all_upper_or_all_lower_input(self):
        for typed, shown in [("SMITH", "Smith"), ("smith", "Smith"), ("McDonald", "McDonald"), ("DeShawn", "DeShawn"),
                             ("mcdonald", "McDonald"), ("o'neil", "O'Neil"), ("D'ANGELO", "D'Angelo"), ("mary-ann", "Mary-Ann"),
                             ("JOSÉ", "José"), ("  san   francisco ", "San Francisco"), ("iPhone Case", "iPhone Case"), ("", "")]:
            self.assertEqual(smart_title(typed), shown, typed)

    def test_names_drop_the_suffix_because_it_has_its_own_field(self):
        self.assertEqual(PRESENTERS["present_name"]("SMITH Jr."), "Smith")
        self.assertEqual(PRESENTERS["present_name"]("Park, Sr"), "Park")
        self.assertEqual(PRESENTERS["present_suffix"]("SMITH Jr."), "Jr")
        self.assertEqual(PRESENTERS["present_suffix"]("Smith III"), "III")
        self.assertEqual(PRESENTERS["present_suffix"]("Smith"), "")

    def test_phone_postal_address_email(self):
        self.assertEqual(PRESENTERS["present_phone"]("512.555.0123"), "(512) 555-0123")
        self.assertEqual(PRESENTERS["present_phone"]("+1 (512) 555-0123"), "(512) 555-0123")
        self.assertEqual(PRESENTERS["present_phone"]("555-0123"), "555-0123")  # not a full US number: left as typed
        self.assertEqual(PRESENTERS["present_postal"]("941071234"), "94107-1234")
        self.assertEqual(PRESENTERS["present_postal"]("k1a 0b1"), "K1A 0B1")
        self.assertEqual(PRESENTERS["present_address"]("123 MAIN STREET NE"), "123 Main Street NE")
        self.assertEqual(PRESENTERS["present_address"]("45 1ST AVE"), "45 1st Ave")
        self.assertEqual(PRESENTERS["present_address"]("po box 12"), "PO Box 12")
        self.assertEqual(PRESENTERS["present_email"]("  Bob@X.COM "), "bob@x.com")

    def test_config_accepts_and_round_trips_presentation_steps_and_rejects_unknown_ones(self):
        doc = config_to_document(BUILTIN_DOMAINS["customer"])
        self.assertEqual(parse_domain_document(doc).field_specs, BUILTIN_DOMAINS["customer"].field_specs)
        doc["fields"]["email"]["present"] = ["present_emial"]
        from app.domain_config import DomainConfigError
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(doc, "t.json")
        self.assertIn("Did you mean 'present_email'", str(cm.exception))


class TestStagingLayer(Base):
    def test_both_forms_are_derived_and_placeholders_are_empty_in_both(self):
        matching, cleansed = stage_record("customer", self.cfg, MESSY)
        self.assertEqual((matching["first_name"], matching["email"], matching["phone"]), ("robert", "bob@x.com", "5125550123"))
        self.assertEqual((cleansed["first_name"], cleansed["email"], cleansed["phone"]), ("Bob", "bob@x.com", "(512) 555-0123"))
        m2, c2 = stage_record("customer", self.cfg, {"first_name": "Ann", "email": "none@none.com", "phone": "000-000-0000"})
        self.assertEqual((m2["email"], c2["email"], m2["phone"], c2["phone"]), ("", "", "", ""))

    def test_raw_is_stored_exactly_as_received_and_the_staged_row_sits_beside_it(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        self.assertEqual(self.store.list_source_records("acme", "customer")[0].attributes, MESSY)  # untouched: whitespace and all
        staged = self.store.get_staged_many("acme", "customer", [r["record_id"]])
        self.assertEqual(staged[r["record_id"]][1]["last_name"], "Smith")
        self.assertEqual(self.sql("SELECT fingerprint FROM standardized_records")[0][0], derived_fingerprint(self.cfg))

    def test_batch_ingest_stages_every_record(self):
        results = ingest_batch(self.store, "acme", "customer", [("crm", f"C{i}", {**MESSY, "email": f"p{i}@x.com", "phone": f"512-555-01{i:02d}"}) for i in range(7)])
        self.assertEqual(self.store.count_staged("customer"), 7)
        self.assertEqual(len(self.store.get_staged_many("acme", "customer", [r["record_id"] for r in results])), 7)

    def test_matching_reads_candidates_from_staging_instead_of_re_standardizing_them(self):
        for i in range(12):
            ingest_record(self.store, "acme", "customer", "crm", f"S{i}", {"first_name": f"P{i}", "last_name": "Smith", "postal_code": "94107"})
        with unittest.mock.patch("app.pipeline.standardize", wraps=standardize) as spy:
            ingest_record(self.store, "acme", "customer", "crm", "NEW", {"first_name": "Zed", "last_name": "Smith", "postal_code": "94107"})
        self.assertEqual(spy.call_count, 0)  # it was 28 for one such ingest before the staging layer existed

    def test_a_record_that_predates_staging_still_matches_via_a_fallback(self):
        a = ingest_record(self.store, "acme", "customer", "crm", "A", {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com"})
        self.sql("DELETE FROM standardized_records WHERE record_id = ?", (a["record_id"],))
        b = ingest_record(self.store, "acme", "customer", "crm", "B", {"first_name": "Ann", "last_name": "Lee", "email": "a@x.com"})
        self.assertEqual(a["matched_entity_id"], b["matched_entity_id"])

    def test_purge_removes_the_staged_copy_of_personal_data_too(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        purge_entity(self.store, "acme", "customer", r["matched_entity_id"], requested_by="dpo")
        self.assertEqual(self.sql("SELECT COUNT(*) FROM standardized_records")[0][0], 0)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM source_records")[0][0], 0)


class TestGoldenRecordsAreDerivedFromCleansedData(Base):
    def test_golden_values_are_cleansed_not_raw(self):
        g = self.golden(ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY))
        self.assertEqual({k: g[k] for k in ("first_name", "last_name", "name_suffix", "email", "phone", "address_line1",
                                            "city", "state", "postal_code", "country")},
                         {"first_name": "Bob", "last_name": "Smith", "name_suffix": "Jr", "email": "bob@x.com",
                          "phone": "(512) 555-0123", "address_line1": "123 Main Street", "city": "San Francisco",
                          "state": "CA", "postal_code": "94107-1234", "country": "US"})

    def test_a_placeholder_never_reaches_a_golden_record(self):
        g = self.golden(ingest_record(self.store, "acme", "customer", "crm", "C1",
                                      {"first_name": "Ann", "last_name": "Lee", "email": "none@none.com", "phone": "000-000-0000"}))
        self.assertFalse(g.get("email"))
        self.assertFalse(g.get("phone"))

    def test_accents_and_deliberate_casing_survive(self):
        g = self.golden(ingest_record(self.store, "acme", "customer", "crm", "C1", {"first_name": "JOSÉ", "last_name": "McDonald"}))
        self.assertEqual((g["first_name"], g["last_name"]), ("José", "McDonald"))

    def test_survivorship_chooses_among_cleansed_values_across_records(self):
        ingest_record(self.store, "acme", "customer", "crm", "A", {"first_name": "bob", "last_name": "SMITH", "phone": "512-555-0123"})
        r = ingest_record(self.store, "acme", "customer", "erp", "B", {"first_name": "ROBERT", "last_name": "smith", "phone": "(512) 555-0123"})
        g = self.golden(r)
        self.assertEqual((g["first_name"], g["last_name"], g["phone"]), ("Robert", "Smith", "(512) 555-0123"))  # most complete, then cleansed

    def test_unspecified_fields_in_a_custom_domain_are_still_trimmed_and_collapsed(self):
        register_domain(parse_domain_document({"name": "thing", "fields": {"code": "identifier", "label": "text"},
                         "deterministic_fields": ["code"], "fuzzy_weights": {"label": 1}, "blocking": [["code"]]}, "t"))
        r = ingest_record(self.store, "acme", "thing", "s", "1", {"code": "ab-1", "label": "  big    red  box "})
        self.assertEqual(self.store.get_entity(r["matched_entity_id"], "acme", "thing").attributes["label"], "big red box")


class TestRebuildingDerivedLayers(Base):
    def test_reindex_rebuilds_staging_keys_and_golden_records_from_raw(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        # simulate a database that never had the layer: no staging, no keys, golden built from raw
        self.sql("DELETE FROM standardized_records")
        self.sql("DELETE FROM blocking_keys")
        self.sql("UPDATE entity_customer SET first_name = '  bob ', email = ' Bob@X.COM ', phone = NULL")
        result = reindex_domain(self.store, "customer")
        self.assertEqual((result.records, result.entities), (1, 1))
        self.assertEqual(self.store.count_staged("customer"), 1)
        self.assertGreater(self.sql("SELECT COUNT(*) FROM blocking_keys")[0][0], 1)
        g = self.golden(r)
        self.assertEqual((g["first_name"], g["email"], g["phone"]), ("Bob", "bob@x.com", "(512) 555-0123"))

    def test_golden_changes_are_recorded_in_lineage(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        self.sql("UPDATE entity_customer SET first_name = '  bob '")
        before = self.sql("SELECT COUNT(*) FROM lineage")[0][0]
        reindex_domain(self.store, "customer")
        self.assertGreater(self.sql("SELECT COUNT(*) FROM lineage")[0][0], before)
        self.assertTrue(self.sql("SELECT reason FROM lineage WHERE reason LIKE '%rebuilt from the staging layer%'"))

    def test_a_presentation_change_is_picked_up_by_a_rebuild_not_silently(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        self.assertEqual(self.golden(r)["phone"], "(512) 555-0123")
        doc = config_to_document(BUILTIN_DOMAINS["customer"])
        doc["fields"]["phone"]["present"] = ["present_text"]  # "show phones as typed"
        cfg = parse_domain_document(doc, "override")
        register_domain(cfg, replace=True)
        self.assertEqual(index_state(self.store, "customer", cfg), "STALE")
        with self.assertRaises(IndexStaleError):
            ingest_record(self.store, "acme", "customer", "crm", "C2", {"first_name": "X", "last_name": "Y"})
        reindex_domain(self.store, "customer")
        self.assertEqual(self.golden(r)["phone"], "512.555.0123")


class TestUpgradingADatabaseThatHasNoStagingLayer(Base):
    def test_existing_records_are_flagged_for_a_rebuild_not_silently_run_on_an_empty_staging_table(self):
        r = ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        # the database as it was before migration 008: records and keys exist, the staging table does not
        self.sql("DROP TABLE standardized_records")
        self.sql("DELETE FROM schema_migrations WHERE version = 8")
        store = Store(self.dbpath)  # re-applies 008; its data step must notice the unstaged domain
        self.assertEqual(index_state(store, "customer", self.cfg), "STALE")
        with self.assertRaises(IndexStaleError) as cm:
            ingest_record(store, "acme", "customer", "crm", "C2", {"first_name": "X", "last_name": "Y"})
        self.assertIn("reindex customer", str(cm.exception))
        reindex_domain(store, "customer")
        self.assertEqual(index_state(store, "customer", self.cfg), "READY")
        self.assertEqual(store.count_staged("customer"), 1)
        self.assertEqual(store.get_entity(r["matched_entity_id"], "acme", "customer").attributes["last_name"], "Smith")

    def test_a_domain_with_no_records_is_not_flagged(self):
        self.sql("DROP TABLE standardized_records")
        self.sql("DELETE FROM schema_migrations WHERE version = 8")
        store = Store(self.dbpath)
        ingest_record(store, "acme", "customer", "crm", "C1", MESSY)  # must not raise


if __name__ == "__main__":
    unittest.main()
