"""
Golden records as real tables: entity_<domain>, one column per field, no JSON blob. Includes the migration that
moved existing golden records out of the old JSON-attributes `entities` table.

Run with:  python3 -m unittest tests.test_entity_tables -v
"""
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.db import Store
from app.domain_config import DomainConfigError, parse_domain_document
from app.domains import BUILTIN_DOMAINS, REGISTRY, register_domain
from app.golden import SYSTEM_COLUMNS
from app.indexing import IndexStaleError, index_state, reindex_domain
from app.migrations import MigrationError, SQLiteMigrationBackend, run_migrations
from app.migrations.versions import MIGRATIONS
from app.models import Entity
from app.pipeline import ingest_record
from app.rls_policies import generate_rls_sql, tenant_scoped_tables

MESSY = {"first_name": "  bob ", "last_name": "SMITH Jr.", "email": " Bob@X.COM ", "phone": "512.555.0123",
         "city": "SAN  FRANCISCO", "state": "ca", "postal_code": "941071234"}


def thing_doc(fields=None, **extra):
    d = {"name": "thing", "fields": fields or {"code": "identifier", "label": "text"}, "deterministic_fields": ["code"],
         "fuzzy_weights": {"label": 1}, "blocking": [["code"]]}
    d.update(extra)
    return d


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.dbpath = os.path.join(self.tmp, "t.db")
        self.store = Store(self.dbpath)

    def sql(self, query, params=()):
        c = sqlite3.connect(self.dbpath)
        try:
            rows = c.execute(query, params).fetchall()
            c.commit()
            return rows
        finally:
            c.close()

    def columns(self, table):
        return [r[1] for r in self.sql(f'PRAGMA table_info("{table}")')]

    def register(self, doc):
        cfg = parse_domain_document(doc, "t")
        register_domain(cfg, replace=cfg.name in REGISTRY)
        return cfg


class TestRealColumns(Base):
    def test_each_builtin_domain_has_its_own_table_with_one_column_per_field(self):
        for domain, cfg in BUILTIN_DOMAINS.items():
            self.assertEqual(self.columns(f"entity_{domain}"), list(SYSTEM_COLUMNS) + cfg.schema_fields, domain)
        self.assertFalse(self.sql("SELECT name FROM sqlite_master WHERE name = 'entities'"))

    def test_there_is_no_json_blob_and_every_column_is_text(self):
        info = self.sql('PRAGMA table_info("entity_customer")')
        self.assertNotIn("attributes", [r[1] for r in info])
        self.assertEqual({r[2] for r in info}, {"TEXT"})

    def test_golden_values_are_plain_columns_a_report_can_select(self):
        ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        row = self.sql("SELECT first_name, last_name, name_suffix, email, phone, state, postal_code, status "
                       "FROM entity_customer WHERE tenant_id = 'acme'")[0]
        self.assertEqual(row, ("Bob", "Smith", "Jr", "bob@x.com", "(512) 555-0123", "CA", "94107-1234", "active"))

    def test_missing_values_are_sql_null_and_filterable(self):
        ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)  # no street address
        self.assertEqual(self.sql("SELECT address_line1 FROM entity_customer")[0][0], None)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer WHERE state = 'CA' AND status = 'active'")[0][0], 1)

    def test_domains_do_not_share_a_table(self):
        ingest_record(self.store, "acme", "customer", "crm", "C1", MESSY)
        ingest_record(self.store, "acme", "vendor", "erp", "V1", {"company_name": "Acme Supply", "tax_id": "12-3456789"})
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 1)
        self.assertEqual(self.sql("SELECT company_name, tax_id FROM entity_vendor")[0], ("Acme Supply", "12-3456789"))  # cleansed form keeps the dash; the matching form strips it

    def test_a_custom_domain_gets_its_own_table_with_exactly_its_fields(self):
        self.register(thing_doc())
        r = ingest_record(self.store, "acme", "thing", "s", "1", {"code": "ab-1", "label": "  big    box "})
        self.assertEqual(self.columns("entity_thing"), list(SYSTEM_COLUMNS) + ["code", "label"])
        self.assertEqual(self.sql("SELECT entity_id, code, label FROM entity_thing")[0], (r["matched_entity_id"], "ab-1", "big box"))  # unspecified presentation: trimmed and collapsed, nothing more
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_customer")[0][0], 0)

    def test_a_merge_keeps_the_losing_entity_as_a_merged_row(self):
        from app.pipeline import merge_two_records
        a = ingest_record(self.store, "acme", "customer", "crm", "A", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com"})
        b = ingest_record(self.store, "acme", "customer", "crm", "B", {"first_name": "Bob", "last_name": "Chen", "email": "bob@y.com"})
        self.assertNotEqual(a["matched_entity_id"], b["matched_entity_id"])
        merge_two_records(self.store, "acme", "customer", a["record_id"], b["record_id"], reason="test")
        self.assertEqual(sorted(r[0] for r in self.sql("SELECT status FROM entity_customer")), ["active", "merged"])
        self.assertTrue(self.sql("SELECT first_name FROM entity_customer WHERE status = 'merged'")[0][0])  # the tombstone keeps its values

    def test_entities_round_trip_through_the_store_with_every_schema_field_present(self):
        ent = Entity(attributes={"first_name": "Ann", "email": "ann@x.com"}, tenant_id="acme", domain="customer")
        self.store.save_entity(ent)
        back = self.store.get_entity(ent.id, "acme", "customer")
        self.assertEqual(set(back.attributes), set(BUILTIN_DOMAINS["customer"].schema_fields))
        self.assertEqual((back.attributes["first_name"], back.attributes["email"], back.attributes["phone"]), ("Ann", "ann@x.com", None))
        self.assertEqual([e.id for e in self.store.list_entities("acme", "customer")], [ent.id])
        self.assertIsNone(self.store.get_entity(ent.id, "other-tenant", "customer"))  # tenant isolation holds
        self.store.delete_entity(ent.id, "acme", "customer")
        self.assertIsNone(self.store.get_entity(ent.id, "acme", "customer"))


class TestSchemaFollowsConfig(Base):
    def test_adding_a_field_adds_a_column_and_a_rebuild_fills_it_from_raw(self):
        self.register(thing_doc())
        r = ingest_record(self.store, "acme", "thing", "s", "1", {"code": "X1", "label": "box", "color": "red"})  # color: not a field yet
        cfg = self.register(thing_doc(fields={"code": "identifier", "label": "text", "color": "text"}))
        self.assertEqual(index_state(self.store, "thing", cfg), "STALE")
        with self.assertRaises(IndexStaleError):
            ingest_record(self.store, "acme", "thing", "s", "2", {"code": "X2"})
        reindex_domain(self.store, "thing")
        self.assertEqual(self.columns("entity_thing"), list(SYSTEM_COLUMNS) + ["code", "label", "color"])
        self.assertEqual(self.sql("SELECT color FROM entity_thing WHERE entity_id = ?", (r["matched_entity_id"],))[0][0], "red")

    def test_removing_a_field_never_drops_its_column(self):
        self.register(thing_doc())
        r = ingest_record(self.store, "acme", "thing", "s", "1", {"code": "X1", "label": "box"})
        cfg = self.register(thing_doc(fields={"code": "identifier"}, fuzzy_weights={}, semantic={"fields": [], "weight": 0}))
        reindex_domain(self.store, "thing")
        self.assertIn("label", self.columns("entity_thing"))                       # the data is still there...
        self.assertEqual(set(self.store.get_entity(r["matched_entity_id"], "acme", "thing").attributes), {"code"})  # ...but not served

    def test_with_auto_migration_off_the_app_only_verifies_and_says_what_to_run(self):
        store = Store(self.dbpath, auto_migrate=False)  # the least-privilege setup: no DDL from the app
        self.register(thing_doc())
        with self.assertRaises(MigrationError) as cm:
            store.save_entity(Entity(attributes={"code": "X"}, tenant_id="acme", domain="thing"))
        self.assertIn("domains_cli.py reindex thing", str(cm.exception))
        self.assertFalse(self.sql("SELECT name FROM sqlite_master WHERE name = 'entity_thing'"))  # and it created nothing
        Store(self.dbpath).sync_entity_table("thing")  # what `reindex` does, with DDL rights
        store.save_entity(Entity(attributes={"code": "X"}, tenant_id="acme", domain="thing"))  # now fine

    def test_reindex_does_the_ddl_even_when_the_app_itself_is_in_check_only_mode(self):
        """The point of `reindex` in a least-privilege deployment: the app role only verifies, and this is the one
        privileged path that creates and extends golden-record tables."""
        self.register(thing_doc())
        verify_only = Store(self.dbpath, auto_migrate=False)
        with self.assertRaises(MigrationError):
            verify_only.list_entities("acme", "thing")           # the app alone can't create it...
        reindex_domain(verify_only, "thing")                      # ...but reindex can
        self.assertEqual(self.columns("entity_thing"), list(SYSTEM_COLUMNS) + ["code", "label"])
        self.assertEqual(verify_only.list_entities("acme", "thing"), [])

    def test_a_rebuild_leaves_merged_tombstones_alone(self):
        """A merged entity has no records left, so re-deriving it from them would BLANK its values."""
        from app.pipeline import merge_two_records
        a = ingest_record(self.store, "acme", "customer", "crm", "A", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com"})
        b = ingest_record(self.store, "acme", "customer", "crm", "B", {"first_name": "Bob", "last_name": "Chen", "email": "bob@y.com"})
        merge_two_records(self.store, "acme", "customer", a["record_id"], b["record_id"], reason="test")
        before = self.sql("SELECT entity_id, first_name, last_name, email FROM entity_customer WHERE status = 'merged'")[0]
        result = reindex_domain(self.store, "customer")
        self.assertEqual(result.entities, 1)  # only the surviving entity was re-derived
        self.assertEqual(self.sql("SELECT entity_id, first_name, last_name, email FROM entity_customer WHERE status = 'merged'")[0], before)
        self.assertTrue(before[1] and before[3])

    def test_field_names_that_would_collide_with_system_columns_or_exceed_the_limit_are_rejected(self):
        for bad in ("status", "entity_id", "tenant_id", "created_at", "id"):
            with self.assertRaises(DomainConfigError) as cm:
                parse_domain_document(thing_doc(fields={"code": "identifier", bad: "text"}, fuzzy_weights={}), "t.json")
            self.assertIn("system column", str(cm.exception), bad)
        with self.assertRaises(DomainConfigError):
            parse_domain_document(thing_doc(fields={"code": "identifier", "x" * 64: "text"}, fuzzy_weights={}), "t.json")  # Postgres' identifier limit is 63
        parse_domain_document(thing_doc(fields={"code": "identifier", "x" * 63: "text"}, fuzzy_weights={}), "t.json")


class TestMigrationFromTheJsonEra(Base):
    def make_json_era_db(self):
        path = os.path.join(self.tmp, "old.db")
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:8])  # everything BEFORE migration 009
        c = sqlite3.connect(path)
        ts = "2026-01-01T00:00:00+00:00"
        for ent in [("ent_a", "acme", "customer", "active", {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "mystery_key": "keep me"}),
                    ("ent_b", "acme", "customer", "merged", {"first_name": "Annie", "last_name": "Lee"}),
                    ("ent_w", "acme", "widgets", "active", {"sku": "W1", "colour": "red"})]:  # a domain whose config is NOT loaded
            c.execute("INSERT INTO entities (id, tenant_id, domain, status, attributes, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                      (ent[0], ent[1], ent[2], ent[3], json.dumps(ent[4]), ts, ts))
        c.execute("INSERT INTO source_records (id, tenant_id, domain, source_system, source_record_id, attributes, ingested_at, entity_id) "
                  "VALUES ('src1','acme','customer','crm','C1','{}',?, 'ent_a')", (ts,))
        c.commit()
        c.close()
        return path

    def test_golden_records_move_across_with_ids_status_and_values_intact(self):
        path = self.make_json_era_db()
        store = Store(path)  # applies migration 009
        c = sqlite3.connect(path)
        names = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        self.assertNotIn("entities", names)
        self.assertEqual(c.execute("SELECT COUNT(*) FROM entities_json_legacy").fetchone()[0], 3)  # safety net kept
        a = store.get_entity("ent_a", "acme", "customer")
        self.assertEqual((a.status, a.attributes["first_name"], a.attributes["email"], a.source_record_ids), ("active", "Ann", "ann@x.com", ["src1"]))
        self.assertEqual(store.get_entity("ent_b", "acme", "customer").status, "merged")
        self.assertEqual(c.execute("SELECT mystery_key FROM entity_customer WHERE entity_id = 'ent_a'").fetchone()[0], "keep me")  # unlisted key kept as a column
        self.assertEqual(c.execute("SELECT sku, colour FROM entity_widgets").fetchone(), ("W1", "red"))  # custom domain, no config loaded: columns from its data
        w = store.get_entity("ent_w", "acme", "widgets")
        self.assertEqual(w.attributes, {"sku": "W1", "colour": "red"})

    def test_running_it_again_changes_nothing(self):
        path = self.make_json_era_db()
        Store(path)
        self.assertEqual(run_migrations(SQLiteMigrationBackend(path)), [])

    def test_a_fresh_database_does_not_keep_an_empty_legacy_table(self):
        self.assertFalse(self.sql("SELECT name FROM sqlite_master WHERE name IN ('entities', 'entities_json_legacy')"))


class TestRowLevelSecurityCoversTheEntityTables(Base):
    def test_every_domain_table_gets_a_policy_and_the_old_table_is_gone(self):
        tables = tenant_scoped_tables()
        self.assertNotIn("entities", tables)
        self.assertTrue({"entity_customer", "entity_product", "entity_vendor"} <= set(tables))
        self.register(thing_doc())
        sql = generate_rls_sql()
        self.assertIn("CREATE POLICY tenant_isolation ON entity_thing", sql)


if __name__ == "__main__":
    unittest.main()
