"""
Customer-defined domains: config parsing and validation, the registry, proof that the built-in
domains are expressible in the same format, the stale-index guard, the reindex job, and the CLI.
SQLite throughout; no network.

Run with:  python3 -m unittest tests.test_domain_config -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.active_learning import feature_names
from app.db import Store
from app.domain_config import (
    DomainConfigError,
    config_to_document,
    load_configured_domains,
    load_domain_file,
    load_domains_dir,
    parse_domain_document,
)
from app.domains import BUILTIN_DOMAINS, REGISTRY, register_domain
from app.indexing import IndexStaleError, blocking_fingerprint, index_state, reindex_domain
from app.normalizers import NORMALIZER_DOCS, NORMALIZERS, DeclarativeStandardizer, FieldSpec
from app.pipeline import ingest_batch, ingest_record
from app.standardize import standardize

try:
    import yaml  # noqa: F401
    HAVE_YAML = True
except ImportError:
    HAVE_YAML = False

ROOT = os.path.join(os.path.dirname(__file__), "..")
TEMPLATES = os.path.join(ROOT, "domain_templates")

BATTERY = {  # deliberately messy inputs; every one must standardize identically both ways
    "customer": [
        {"first_name": "  José ", "last_name": "O'Neil Jr.", "email": " Foo@Bar.COM ", "phone": "+1 (415) 555-0100",
         "address_line1": "123 Main Street, Apt #4", "city": "San  Francisco", "state": "ca",
         "postal_code": "94107-1234", "country": ""},
        {},
        {"first_name": None, "country": "mx", "postal_code": "941071234"},
    ],
    "product": [
        {"sku": "nk-am90_blk 10", "upc": " 0123-4567 ", "product_name": "Nike  Air Max 90 12 OZ 6 PK",
         "brand": "NIKE", "category": "Shoes & Boots", "description": "  Great   SHOE ", "price": 120.5},
        {"price": None},
        {},
    ],
    "vendor": [
        {"company_name": "ACME Supply, Co.", "tax_id": "12-345.6789", "duns_number": "01-234 5678",
         "contact_email": "AP@Acme.com", "contact_phone": "312.555.4400", "address_line1": "90 Lake Shore Drive",
         "city": "Chicago", "state": "il", "postal_code": "606011234", "country": "ca"},
        {},
    ],
}


def people_doc(blocking=None, fuzzy=None, fields=None, **extra):
    doc = {
        "name": "people",
        "fields": fields or {"first_name": "name", "last_name": "name", "email": "email", "postal_code": "postal_code"},
        "deterministic_fields": ["email"],
        "fuzzy_weights": fuzzy or {"first_name": 0.3, "last_name": 0.4, "postal_code": 0.3},
        "blocking": blocking or [["email"]],
    }
    doc.update(extra)
    return doc


class DomainTestCase(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.dbpath = os.path.join(self.tmp, "t.db")

    def register(self, doc):
        cfg = parse_domain_document(doc, "test")
        register_domain(cfg, replace=cfg.name in REGISTRY)
        return cfg

    def write(self, directory, filename, doc):
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, filename)
        with open(path, "w") as f:
            json.dump(doc, f)
        return path

    def errors_for(self, doc):
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(doc, "t.json")
        return str(cm.exception)


# ---------------------------------------------------------------- the vocabulary
class TestNormalizerEngine(DomainTestCase):
    def test_steps_apply_in_order(self):
        self.assertEqual(FieldSpec(("text", "upper")).apply("  ca "), "CA")

    def test_default_applies_only_when_the_result_is_empty(self):
        spec = FieldSpec(("text", "upper"), default="US")
        self.assertEqual((spec.apply(""), spec.apply(None), spec.apply("mx")), ("US", "US", "MX"))

    def test_non_strings_are_coerced_not_crashed_on(self):
        self.assertEqual(FieldSpec(("text",)).apply(12.5), "12.5")

    def test_every_step_is_documented_and_callable(self):
        self.assertEqual(set(NORMALIZERS), set(NORMALIZER_DOCS))
        for name, fn in NORMALIZERS.items():
            self.assertIsInstance(fn("  Foo-Bar_1  "), str, name)


# ------------------------------------- the built-ins are expressible in the same format
class TestBuiltinsAreExpressibleDeclaratively(DomainTestCase):
    def test_builtin_field_specs_reproduce_the_python_standardizers(self):
        """The built-ins' field_specs feed the blocking fingerprint, so they must be TRUE
        descriptions of the Python standardizers -- this is what stops them drifting apart."""
        for name, cfg in BUILTIN_DOMAINS.items():
            declarative = DeclarativeStandardizer(cfg.schema_fields, cfg.field_specs)
            for raw in BATTERY[name]:
                self.assertEqual(declarative(raw), cfg.standardizer(raw), (name, raw))

    def test_shipped_templates_load_back_as_identical_configs(self):
        for name, builtin in BUILTIN_DOMAINS.items():
            cfg = load_domain_file(os.path.join(TEMPLATES, f"{name}.json"))
            for attr in ("schema_fields", "deterministic_fields", "fuzzy_weights", "semantic_fields",
                         "semantic_weight", "blocking_field_groups", "field_strategies", "default_strategy"):
                self.assertEqual(getattr(cfg, attr), getattr(builtin, attr), (name, attr))
            for raw in BATTERY[name]:
                self.assertEqual(cfg.standardizer(raw), builtin.standardizer(raw), (name, raw))
            self.assertEqual(blocking_fingerprint(cfg), blocking_fingerprint(builtin), name)

    def test_shipped_templates_are_exactly_what_export_produces(self):
        for name, builtin in BUILTIN_DOMAINS.items():
            with open(os.path.join(TEMPLATES, f"{name}.json")) as f:
                self.assertEqual(json.load(f), config_to_document(builtin), f"{name}.json is out of date")

    def test_exported_template_is_an_unchanged_override_needing_no_reindex(self):
        domain_dir = os.path.join(self.tmp, "domains")
        os.makedirs(domain_dir)
        shutil.copy(os.path.join(TEMPLATES, "customer.json"), domain_dir)
        load_configured_domains(domain_dir)
        self.assertNotEqual(REGISTRY["customer"].source, "builtin")  # it really did replace the built-in
        store = Store(self.dbpath)
        ingest_record(store, "acme", "customer", "crm", "C1", {"first_name": "A", "last_name": "B", "email": "a@b.com"})
        self.assertEqual(index_state(store, "customer", REGISTRY["customer"]), "READY")


# ---------------------------------------------------------------------- validation
class TestValidation(DomainTestCase):
    def test_a_minimal_valid_document_parses(self):
        cfg = parse_domain_document(people_doc())
        self.assertEqual(cfg.schema_fields, ["first_name", "last_name", "email", "postal_code"])
        self.assertEqual(cfg.blocking_field_groups, [["email"]])

    def test_all_three_field_forms_are_accepted(self):
        cfg = parse_domain_document(people_doc(fields={
            "a": "name", "b": ["text", "upper"], "c": {"normalize": "email", "default": "none"}}, blocking=[["a"]],
            deterministic_fields=["c"], fuzzy_weights={"a": 1}))
        self.assertEqual(cfg.field_specs["b"].normalize, ("text", "upper"))
        self.assertEqual(cfg.field_specs["c"].default, "none")

    def test_unknown_top_level_key_is_rejected_with_a_suggestion(self):
        msg = self.errors_for(people_doc(fuzzy_weight={"email": 1}))
        self.assertIn("unknown key", msg)
        self.assertIn("Did you mean 'fuzzy_weights'", msg)

    def test_every_problem_is_reported_at_once(self):
        doc = people_doc(blocking=[["no_such_field"]], fields={"email": "emial", "x": "name"})
        doc["name"] = "Bad-Name"
        msg = self.errors_for(doc)
        self.assertIn("name:", msg)                       # bad domain name
        self.assertIn("unknown normalizer 'emial'", msg)  # bad normalizer
        self.assertIn("Did you mean 'email'", msg)
        self.assertIn("unknown field 'no_such_field'", msg)
        self.assertRegex(msg, r"\d+ problems")

    def test_blocking_is_required_and_must_name_real_fields(self):
        doc = people_doc()
        del doc["blocking"]
        self.assertIn("blocking: required", self.errors_for(doc))
        self.assertIn("must not be empty", self.errors_for(people_doc(blocking=[[]])))
        self.assertIn("duplicate", self.errors_for(people_doc(blocking=[["email"], ["email"]])))

    def test_weights_must_be_positive_real_numbers_for_real_fields(self):
        self.assertIn("greater than 0", self.errors_for(people_doc(fuzzy={"first_name": -1})))
        self.assertIn("greater than 0", self.errors_for(people_doc(fuzzy={"first_name": True})))
        self.assertIn("unknown field", self.errors_for(people_doc(fuzzy={"nope": 0.5})))

    def test_a_domain_with_no_scoring_signal_is_rejected(self):
        doc = people_doc()
        del doc["deterministic_fields"], doc["fuzzy_weights"]
        self.assertIn("at least one scoring signal", self.errors_for(doc))

    def test_semantic_fields_need_a_weight(self):
        self.assertIn("semantic.weight", self.errors_for(people_doc(semantic={"fields": ["first_name"]})))

    def test_survivorship_strategy_and_fields_are_checked(self):
        self.assertIn("must be one of", self.errors_for(people_doc(survivorship={"default": "newest"})))
        self.assertIn("unknown field", self.errors_for(people_doc(survivorship={"fields": {"nope": "most_recent"}})))

    def test_reserved_and_malformed_domain_names_are_rejected(self):
        self.assertIn("reserved", self.errors_for(people_doc(name="admin")))
        self.assertIn("name:", self.errors_for(people_doc(name="Has Spaces")))

    def test_structural_mistakes_have_clear_messages(self):
        with self.assertRaises(DomainConfigError):
            parse_domain_document([], "t.json")
        self.assertIn("override: must be true or false", self.errors_for(people_doc(override="yes")))
        self.assertIn("fields: required", self.errors_for({**people_doc(), "fields": {}}))

    @unittest.skipUnless(HAVE_YAML, "PyYAML not installed")
    def test_yaml_booleanized_field_names_get_an_actionable_message(self):
        path = os.path.join(self.tmp, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: x\nfields:\n  no: name\nblocking: [[no]]\ndeterministic_fields: []\nfuzzy_weights: {}\n")
        with self.assertRaises(DomainConfigError) as cm:
            load_domain_file(path)
        self.assertIn("quote names like 'no'", str(cm.exception))


# ------------------------------------------------------------- files and the registry
class TestFilesAndRegistry(DomainTestCase):
    def test_json_file_loads_and_registers_a_working_domain(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "people.json", people_doc())
        self.assertEqual(load_configured_domains(d), ["people"])
        self.assertIn("people", REGISTRY)
        self.assertEqual(standardize("people", {"email": " A@B.COM "})["email"], "a@b.com")  # dispatch sees it

    def test_loading_twice_is_a_no_op(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "people.json", people_doc())
        self.assertEqual(load_configured_domains(d), load_configured_domains(d))

    def test_reset_removes_custom_domains_and_their_standardizers(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "people.json", people_doc())
        load_configured_domains(d)
        domain_config.reset()
        self.assertNotIn("people", REGISTRY)
        with self.assertRaises(KeyError):
            standardize("people", {})

    def test_directory_comes_from_the_environment(self):
        d = os.path.join(self.tmp, "envdir")
        self.write(d, "people.json", people_doc())
        with patch.dict(os.environ, {"MDM_DOMAINS_DIR": d}):
            self.assertEqual(load_configured_domains(), ["people"])

    def test_an_explicitly_requested_missing_directory_is_an_error(self):
        with patch.dict(os.environ, {"MDM_DOMAINS_DIR": os.path.join(self.tmp, "nope")}):
            with self.assertRaises(DomainConfigError):
                load_configured_domains()

    def test_absent_default_directory_just_means_no_customization(self):
        here = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, here)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MDM_DOMAINS_DIR", None)
            self.assertEqual(load_configured_domains(), [])

    def test_bad_json_names_the_file(self):
        d = os.path.join(self.tmp, "domains")
        os.makedirs(d)
        with open(os.path.join(d, "broken.json"), "w") as f:
            f.write("{ not json")
        with self.assertRaises(DomainConfigError) as cm:
            load_domains_dir(d)
        self.assertIn("broken.json", str(cm.exception))
        self.assertIn("not valid JSON", str(cm.exception))

    def test_every_broken_file_is_reported_in_one_go_and_nothing_is_half_applied(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "a_good.json", people_doc())
        self.write(d, "b_bad.json", {**people_doc(name="bad_one"), "blocking": []})
        self.write(d, "c_bad.json", {**people_doc(name="bad_two"), "fields": {}})
        with self.assertRaises(DomainConfigError) as cm:
            load_configured_domains(d)
        self.assertIn("b_bad.json", str(cm.exception))
        self.assertIn("c_bad.json", str(cm.exception))
        self.assertNotIn("people", REGISTRY)  # the valid file was NOT applied on its own

    def test_two_files_defining_the_same_domain_are_rejected(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "one.json", people_doc())
        self.write(d, "two.json", people_doc())
        with self.assertRaises(DomainConfigError) as cm:
            load_domains_dir(d)
        self.assertIn("already defined", str(cm.exception))

    def test_unsupported_file_type(self):
        path = os.path.join(self.tmp, "x.toml")
        open(path, "w").close()
        with self.assertRaises(DomainConfigError):
            load_domain_file(path)

    @unittest.skipUnless(HAVE_YAML, "PyYAML not installed")
    def test_yaml_and_json_produce_the_same_domain(self):
        import yaml as _yaml
        doc = people_doc()
        jpath = self.write(self.tmp, "p.json", doc)
        ypath = os.path.join(self.tmp, "p.yaml")
        with open(ypath, "w") as f:
            _yaml.safe_dump(doc, f)
        j, y = load_domain_file(jpath), load_domain_file(ypath)
        self.assertEqual(config_to_document(j), config_to_document(y))

    def test_changing_a_built_in_requires_saying_so(self):
        d = os.path.join(self.tmp, "domains")
        doc = config_to_document(BUILTIN_DOMAINS["customer"])
        del doc["override"]
        self.write(d, "customer.json", doc)
        with self.assertRaises(DomainConfigError) as cm:
            load_configured_domains(d)
        self.assertIn("built-in", str(cm.exception))
        self.assertIn("override", str(cm.exception))

    def test_override_replaces_the_built_in_and_reset_restores_it(self):
        d = os.path.join(self.tmp, "domains")
        doc = config_to_document(BUILTIN_DOMAINS["customer"])
        doc["fuzzy_weights"] = {"first_name": 0.5, "last_name": 0.5}
        self.write(d, "customer.json", doc)
        load_configured_domains(d)
        self.assertEqual(REGISTRY["customer"].fuzzy_weights, {"first_name": 0.5, "last_name": 0.5})
        domain_config.reset()
        self.assertEqual(REGISTRY["customer"].source, "builtin")
        self.assertEqual(REGISTRY["customer"].fuzzy_weights, BUILTIN_DOMAINS["customer"].fuzzy_weights)

    def test_override_on_a_domain_that_is_not_built_in_is_a_likely_typo(self):
        d = os.path.join(self.tmp, "domains")
        self.write(d, "custmer.json", {**config_to_document(BUILTIN_DOMAINS["customer"]), "name": "custmer"})
        with self.assertRaises(DomainConfigError) as cm:
            load_configured_domains(d)
        self.assertIn("not a built-in domain", str(cm.exception))


# ------------------------------------------------- a brand-new domain, end to end
@unittest.skipUnless(HAVE_YAML, "PyYAML not installed (the shipped example is YAML)")
class TestCustomDomainEndToEnd(DomainTestCase):
    def setUp(self):
        super().setUp()
        cfg = load_domain_file(os.path.join(TEMPLATES, "location.yaml"))
        register_domain(cfg)
        self.store = Store(self.dbpath)

    def ingest(self, rid, **attrs):
        return ingest_record(self.store, "acme", "location", "erp", rid, attrs)

    def test_the_shipped_example_matches_on_its_identifier_despite_formatting(self):
        a = self.ingest("L1", location_code="WH-001", name="Main Street Warehouse", postal_code="89501")
        b = self.ingest("L2", location_code="wh 001", name="Main St Warehouse", postal_code="89501")
        self.assertEqual(a["matched_entity_id"], b["matched_entity_id"])
        self.assertEqual(len(b["auto_merged_with"]), 1)

    def test_unrelated_sites_stay_separate(self):
        a = self.ingest("L1", location_code="WH-001", name="Main Street Warehouse", postal_code="89501")
        b = self.ingest("L2", location_code="DC-777", name="Harbor Depot", postal_code="10001")
        self.assertNotEqual(a["matched_entity_id"], b["matched_entity_id"])
        self.assertEqual(len(self.store.list_entities("acme", "location")), 2)

    def test_custom_domain_is_isolated_from_the_built_ins(self):
        self.ingest("L1", location_code="WH-001", name="Main Street Warehouse")
        self.assertEqual(self.store.list_entities("acme", "customer"), [])

    def test_golden_record_uses_the_configured_survivorship(self):
        r = self.ingest("L1", location_code="WH-001", name="Main Street Warehouse")
        self.ingest("L2", location_code="WH-001", name="Main St Warehouse")
        entity = self.store.get_entity(r["matched_entity_id"], "acme", "location")
        self.assertIn(entity.attributes["name"], {"Main Street Warehouse", "Main St Warehouse"})
        self.assertEqual(len(entity.source_record_ids), 2)

    def test_batch_ingest_and_active_learning_features_work_for_a_custom_domain(self):
        results = ingest_batch(self.store, "acme", "location",
                               [("erp", "L1", {"location_code": "WH-001"}), ("wms", "L2", {"location_code": "WH 001"})])
        self.assertEqual(results[0]["matched_entity_id"], results[1]["matched_entity_id"])
        self.assertEqual(feature_names(REGISTRY["location"]),
                         ["location_code_exact", "name_sim", "address_line1_sim", "city_sim", "postal_code_sim", "semantic_sim"])


# ------------------------------------------------------ the stale-index guard + reindex
class TestIndexFreshness(DomainTestCase):
    def ann(self, rid, email, first="Ann", store=None, tenant="acme"):
        return ingest_record(store or self.store, tenant, "people", "crm", rid,
                             {"first_name": first, "last_name": "Lee", "email": email, "postal_code": "94107"})

    def setUp(self):
        super().setUp()
        self.v1 = self.register(people_doc(blocking=[["email"]]))
        self.store = Store(self.dbpath)

    def to_v2(self, **kw):
        return self.register(people_doc(blocking=kw.pop("blocking", [["last_name", "postal_code"]]), **kw))

    def test_first_ingest_into_a_new_domain_records_its_fingerprint(self):
        self.assertEqual(index_state(self.store, "people", self.v1), "EMPTY")
        self.ann("A", "a@x.com")
        self.assertEqual(index_state(self.store, "people", self.v1), "READY")

    def test_changing_blocking_pauses_ingestion_with_an_actionable_error(self):
        self.ann("A", "a@x.com")
        v2 = self.to_v2()
        self.assertEqual(index_state(self.store, "people", v2), "STALE")
        with self.assertRaises(IndexStaleError) as cm:
            self.ann("B", "b@x.com")
        self.assertIn("reindex people", str(cm.exception))
        self.assertEqual(len(self.store.list_source_records("acme", "people")), 1)  # nothing half-written

    def test_ingest_batch_is_guarded_too(self):
        self.ann("A", "a@x.com")
        self.to_v2()
        with self.assertRaises(IndexStaleError):
            ingest_batch(self.store, "acme", "people", [("crm", "B", {"email": "b@x.com"})])

    def test_scoring_only_changes_do_not_need_a_reindex(self):
        self.ann("A", "a@x.com")
        self.register(people_doc(blocking=[["email"]], fuzzy={"first_name": 0.9, "last_name": 0.1},
                                 semantic={"fields": ["first_name"], "weight": 0.5},
                                 survivorship={"default": "most_complete"}))
        self.assertEqual(index_state(self.store, "people", REGISTRY["people"]), "READY")
        self.ann("B", "b@x.com")  # must not raise

    def test_changing_how_a_blocking_field_is_standardized_is_stale(self):
        self.ann("A", "a@x.com")
        cfg = self.register(people_doc(blocking=[["email"]], fields={
            "first_name": "name", "last_name": "name", "email": ["email", "upper"], "postal_code": "postal_code"}))
        self.assertEqual(index_state(self.store, "people", cfg), "STALE")

    def test_changing_a_field_that_is_not_used_for_blocking_is_not_stale(self):
        self.ann("A", "a@x.com")
        cfg = self.register(people_doc(blocking=[["email"]], fields={
            "first_name": ["text", "upper"], "last_name": "name", "email": "email", "postal_code": "postal_code"}))
        self.assertEqual(index_state(self.store, "people", cfg), "READY")

    def test_reindex_makes_existing_records_findable_under_the_new_rules(self):
        a = self.ann("A", "ann1@x.com")
        b = self.ann("B", "ann2@y.com", first="Anne")
        self.assertNotEqual(a["matched_entity_id"], b["matched_entity_id"])  # no shared key: never even compared
        self.to_v2()
        with self.assertRaises(IndexStaleError):
            self.ann("C", "ann3@z.com")

        result = reindex_domain(self.store, "people", batch_size=1)
        self.assertEqual((result.records, result.batches), (2, 2))

        c = self.ann("C", "ann3@z.com")
        found = {m["record_id"] for m in c["auto_merged_with"]} | {q["record_id"] for q in c["queued_for_review"]}
        self.assertEqual(found, {a["record_id"], b["record_id"]})  # the OLD records are reachable now

    def test_reindex_replaces_old_keys_with_the_new_layout_for_every_tenant(self):
        self.ann("A", "a@x.com", tenant="t1")
        self.ann("B", "b@x.com", tenant="t2")
        self.to_v2()
        reindex_domain(self.store, "people")
        conn = sqlite3.connect(self.dbpath)
        keys = {(t, k) for t, k in conn.execute("SELECT tenant_id, blocking_key FROM blocking_keys WHERE domain = 'people'")}
        conn.close()
        self.assertEqual(keys, {("t1", "g0:lee:941"), ("t2", "g0:lee:941")})  # no old email keys left, both tenants done

    def test_an_interrupted_reindex_stays_paused_and_can_simply_be_rerun(self):
        for i in range(3):
            self.ann(f"R{i}", f"r{i}@x.com")
        v2 = self.to_v2()
        real, calls = self.store.replace_blocking_keys_batch, {"n": 0}

        def flaky(entries):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("disk full")
            return real(entries)

        self.store.replace_blocking_keys_batch = flaky
        with self.assertRaises(RuntimeError):
            reindex_domain(self.store, "people", batch_size=1)
        self.assertEqual(index_state(self.store, "people", v2), "REINDEXING")
        with self.assertRaises(IndexStaleError) as cm:
            self.ann("Z", "z@x.com")  # a half-rebuilt index must never be used
        self.assertIn("interrupted", str(cm.exception))

        self.store.replace_blocking_keys_batch = real
        reindex_domain(self.store, "people", batch_size=1)
        self.assertEqual(index_state(self.store, "people", v2), "READY")
        self.ann("Z", "z@x.com")

    def test_keys_with_no_recorded_state_demand_a_reindex(self):
        self.ann("A", "a@x.com")
        conn = sqlite3.connect(self.dbpath)
        conn.execute("DELETE FROM blocking_index_state")
        conn.commit()
        conn.close()
        self.assertEqual(index_state(self.store, "people", self.v1), "UNKNOWN")
        with self.assertRaises(IndexStaleError) as cm:
            self.ann("B", "b@x.com")
        self.assertIn("no recorded index state", str(cm.exception))
        reindex_domain(self.store, "people")
        self.ann("B", "b@x.com")

    def test_fingerprint_changes_only_when_the_blocking_index_would(self):
        base = blocking_fingerprint(self.v1)
        parse = parse_domain_document
        self.assertEqual(blocking_fingerprint(parse(people_doc())), base)
        self.assertEqual(blocking_fingerprint(parse(people_doc(fuzzy={"first_name": 0.5, "last_name": 0.5}))), base)
        self.assertEqual(blocking_fingerprint(parse(people_doc(semantic={"fields": ["first_name"], "weight": 0.9}))), base)
        self.assertNotEqual(blocking_fingerprint(parse(people_doc(blocking=[["email"], ["last_name"]]))), base)
        self.assertNotEqual(blocking_fingerprint(parse(people_doc(blocking=[["email"]], fields={
            "first_name": "name", "last_name": "name", "email": ["email", "upper"], "postal_code": "postal_code"}))), base)
        two_a = parse(people_doc(blocking=[["email"], ["last_name"]]))
        two_b = parse(people_doc(blocking=[["last_name"], ["email"]]))  # group POSITION is part of every key
        self.assertNotEqual(blocking_fingerprint(two_a), blocking_fingerprint(two_b))

    def test_changing_the_key_layout_in_code_is_detected_automatically(self):
        from app import matching
        before = blocking_fingerprint(self.v1)
        with patch.object(matching, "BLOCKING_PREFIX_LEN", 4):
            self.assertNotEqual(blocking_fingerprint(self.v1), before)
        with patch.object(matching, "BLOCKING_KEY_FORMAT", 2):
            self.assertNotEqual(blocking_fingerprint(self.v1), before)


# ------------------------------------------------ upgrading databases that predate this
class TestUpgradingExistingDatabases(DomainTestCase):
    def predate_index_state(self):
        """A database as it was before migration 006: indexed records, no state table."""
        store = Store(self.dbpath)
        ingest_record(store, "acme", "customer", "crm", "C1",
                      {"first_name": "Robert", "last_name": "Smith", "email": "bob@x.com"})
        conn = sqlite3.connect(self.dbpath)
        conn.execute("DROP TABLE blocking_index_state")
        conn.execute("DELETE FROM schema_migrations WHERE version = 6")
        conn.commit()
        conn.close()

    def test_builtin_domains_with_data_are_adopted_without_a_pointless_reindex(self):
        self.predate_index_state()
        store = Store(self.dbpath)  # re-applies 006, whose data step stamps the built-in fingerprint
        self.assertEqual(index_state(store, "customer", BUILTIN_DOMAINS["customer"]), "READY")
        r = ingest_record(store, "acme", "customer", "shop", "S1",
                          {"first_name": "Bob", "last_name": "Smith", "email": "bob@x.com"})
        self.assertEqual(len(r["auto_merged_with"]), 1)

    def test_an_override_already_in_place_at_upgrade_time_is_caught_as_stale(self):
        """The existing index was built under the BUILT-IN rules. If a customer override that changes
        blocking is loaded when the database is upgraded, 006 must stamp the built-in fingerprint
        (the truth), not the override's -- otherwise the stale index would be silently accepted."""
        self.predate_index_state()
        doc = config_to_document(BUILTIN_DOMAINS["customer"])
        doc["blocking"] = [["email"]]
        cfg = parse_domain_document(doc, "override")
        register_domain(cfg, replace=True)
        store = Store(self.dbpath)
        self.assertEqual(index_state(store, "customer", cfg), "STALE")
        with self.assertRaises(IndexStaleError):
            ingest_record(store, "acme", "customer", "shop", "S1", {"first_name": "B", "last_name": "S"})


# ----------------------------------------------------------------------------- CLI
class TestDomainsCli(DomainTestCase):
    def setUp(self):
        super().setUp()
        self.cfgdir = os.path.join(self.tmp, "domains")
        self.cfgfile = self.write(self.cfgdir, "people.json", people_doc(blocking=[["email"]]))

    def cli(self, script, *args):
        env = dict(os.environ, MDM_DOMAINS_DIR=self.cfgdir)
        return subprocess.run([sys.executable, os.path.join(ROOT, script), *args],
                              capture_output=True, text=True, env=env, cwd=ROOT)

    def test_validate_reports_good_and_bad_files(self):
        ok = self.cli("domains_cli.py", "validate")
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertIn("people", ok.stdout)
        self.write(self.cfgdir, "broken.json", {**people_doc(name="broken"), "blocking": []})
        bad = self.cli("domains_cli.py", "validate")
        self.assertEqual(bad.returncode, 1)
        self.assertIn("broken.json", bad.stdout)

    def test_export_and_normalizers(self):
        out = self.cli("domains_cli.py", "export", "customer")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertTrue(json.loads(out.stdout)["override"])
        self.assertIn("postal_code", self.cli("domains_cli.py", "normalizers").stdout)

    def test_unknown_domain_is_an_error_not_a_traceback(self):
        r = self.cli("domains_cli.py", "reindex", "nope", "--db", self.dbpath)
        self.assertEqual(r.returncode, 2)
        self.assertNotIn("Traceback", r.stderr)

    def test_edit_config_then_status_then_reindex_then_ingest_workflow(self):
        # An app instance has been running under v1 config and ingested two people.
        load_configured_domains(self.cfgdir)
        store = Store(self.dbpath)
        for rid, email, first in (("A", "ann1@x.com", "Ann"), ("B", "ann2@y.com", "Anne")):
            ingest_record(store, "acme", "people", "crm", rid,
                          {"first_name": first, "last_name": "Lee", "email": email, "postal_code": "94107"})
        domain_config.reset()

        ready = self.cli("domains_cli.py", "status", "--db", self.dbpath)
        self.assertEqual(ready.returncode, 0, ready.stdout + ready.stderr)
        self.assertRegex(ready.stdout, r"people\s+READY")

        # The customer edits the file: blocking now uses last name + postal code.
        self.write(self.cfgdir, "people.json", people_doc(blocking=[["last_name", "postal_code"]]))
        stale = self.cli("domains_cli.py", "status", "--db", self.dbpath)
        self.assertEqual(stale.returncode, 1)
        self.assertRegex(stale.stdout, r"people\s+STALE")
        self.assertIn("domains_cli.py reindex people", stale.stdout)

        # Bulk ingest refuses, cleanly, and says what to do.
        csv_path = os.path.join(self.tmp, "in.csv")
        with open(csv_path, "w") as f:
            f.write("first_name,last_name,email,postal_code\nAnn,Lee,ann3@z.com,94107\n")
        blocked = self.cli("bulk_ingest.py", "--tenant", "acme", "--domain", "people", "--file", csv_path, "--db", self.dbpath,
                           "--mapping", "first_name=first_name,last_name=last_name,email=email,postal_code=postal_code")
        self.assertEqual(blocked.returncode, 2, blocked.stdout + blocked.stderr)
        self.assertIn("reindex people", blocked.stderr)
        self.assertNotIn("Traceback", blocked.stderr)

        done = self.cli("domains_cli.py", "reindex", "people", "--db", self.dbpath)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("2 record(s)", done.stdout)
        self.assertIn("does NOT re-run matching", done.stdout)

        self.assertEqual(self.cli("domains_cli.py", "status", "--db", self.dbpath).returncode, 0)
        ingested = self.cli("bulk_ingest.py", "--tenant", "acme", "--domain", "people", "--file", csv_path, "--db", self.dbpath,
                            "--mapping", "first_name=first_name,last_name=last_name,email=email,postal_code=postal_code")
        self.assertEqual(ingested.returncode, 0, ingested.stdout + ingested.stderr)
        self.assertIn("Auto-merged:        1", ingested.stdout)  # the new record found the old ones via the rebuilt index


if __name__ == "__main__":
    unittest.main()
