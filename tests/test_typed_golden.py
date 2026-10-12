"""
Typed golden-record columns. Types are declared per field in the domain config (`"type": "numeric"`, ...); staging stays text;
casting happens at the golden layer, before survivorship; a value that cannot be cast is NULL plus a note, never a wrong value.

Run with:  python3 -m unittest tests.test_typed_golden -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import domain_config
from app.db import Store
from app.domain_config import DomainConfigError, config_to_document, parse_domain_document
from app.domains import BUILTIN_DOMAINS, REGISTRY, register_domain
from app.fieldtypes import canonical_type, cast_value, from_db, to_db
from app.indexing import index_state, reindex_domain
from app.migrations import MigrationError, SQLiteMigrationBackend, run_migrations
from app.migrations.versions import MIGRATIONS
from app.models import Entity
from app.pipeline import ingest_record

ROOT = os.path.join(os.path.dirname(__file__), "..")
UTC = timezone.utc


def asset_doc(**fields):
    base = {"tag": "identifier",
            "qty": {"normalize": "text", "type": "integer"},
            "cost": {"normalize": "text", "type": "numeric"},
            "active": {"normalize": "text", "type": "boolean"},
            "bought": {"normalize": "text", "type": "date", "format": ["%m/%d/%Y"]},
            "seen": {"normalize": "text", "type": "timestamp"}}
    base.update(fields)
    return {"name": "asset", "fields": base, "deterministic_fields": ["tag"], "fuzzy_weights": {}, "blocking": [["tag"]]}


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

    def declared(self, table):
        return {r[1]: r[2] for r in self.sql(f'PRAGMA table_info("{table}")')}

    def register(self, doc):
        cfg = parse_domain_document(doc, "t")
        register_domain(cfg, replace=cfg.name in REGISTRY)
        return cfg

    def golden(self, result, domain):
        return self.store.get_entity(result["matched_entity_id"], "acme", domain)


class TestCasting(unittest.TestCase):
    def test_integer(self):
        self.assertEqual([cast_value(v, "integer")[0] for v in ("1,200", "12.0", "-7", "+3", " 42 ")], [1200, 12, -7, 3, 42])
        for bad in ("12.5", "abc", "1,2", "1e3", "12 000"):
            self.assertIsNotNone(cast_value(bad, "integer")[1], bad)

    def test_numeric_reads_currency_and_thousands_but_not_a_decimal_comma(self):
        self.assertEqual(cast_value("$1,234.50", "numeric")[0], Decimal("1234.50"))
        self.assertEqual(cast_value("€ 5", "numeric")[0], Decimal("5"))
        self.assertEqual(cast_value("-3.2", "numeric")[0], Decimal("-3.2"))
        for bad in ("N/A", "NaN", "Infinity", "1,5", "12abc"):
            value, problem = cast_value(bad, "numeric")
            self.assertEqual((value, bool(problem)), (None, True), bad)

    def test_boolean(self):
        for text, expected in [("Yes", True), ("y", True), ("TRUE", True), ("1", True), ("no", False), ("F", False), ("0", False)]:
            self.assertEqual(cast_value(text, "boolean")[0], expected, text)
        self.assertIsNotNone(cast_value("maybe", "boolean")[1])

    def test_a_date_is_never_guessed(self):
        self.assertEqual(cast_value("2026-03-04", "date")[0], date(2026, 3, 4))
        self.assertIn("%Y-%m-%d", cast_value("03/04/2026", "date")[1])                                 # ambiguous: refused, not guessed
        self.assertEqual(cast_value("03/04/2026", "date", ["%m/%d/%Y"])[0], date(2026, 3, 4))          # you say which
        self.assertEqual(cast_value("03/04/2026", "date", ["%d/%m/%Y"])[0], date(2026, 4, 3))
        self.assertEqual(cast_value("03/04/2026", "date", ["%d/%m/%Y", "%m/%d/%Y"])[0], date(2026, 4, 3))  # first match wins
        self.assertIsNotNone(cast_value("2026-13-45", "date")[1])

    def test_timestamps_are_stored_in_utc(self):
        self.assertEqual(cast_value("2026-01-02T03:04:05Z", "timestamp")[0], datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        self.assertEqual(cast_value("2026-01-02 03:04:05", "timestamp")[0], datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))  # naive = UTC
        self.assertEqual(cast_value("2026-01-02T03:04:05+05:30", "timestamp")[0], datetime(2026, 1, 1, 21, 34, 5, tzinfo=UTC))
        self.assertEqual(cast_value("02/01/2026 03:04", "timestamp", ["%d/%m/%Y %H:%M"])[0], datetime(2026, 1, 2, 3, 4, tzinfo=UTC))

    def test_empty_is_missing_not_a_problem_and_text_is_untouched(self):
        for t in ("integer", "numeric", "boolean", "date", "timestamp"):
            self.assertEqual(cast_value("  ", t), (None, None))
            self.assertEqual(cast_value(None, t), (None, None))
        self.assertEqual(cast_value("  keep  ", "text"), ("  keep  ", None))

    def test_values_survive_the_trip_through_sqlite(self):
        for t, v in [("integer", 5), ("numeric", Decimal("120.50")), ("boolean", False), ("boolean", True), ("date", date(2026, 3, 4)),
                     ("timestamp", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)), ("text", "x")]:
            self.assertEqual(from_db("sqlite", t, to_db("sqlite", t, v)), v, (t, v))
        self.assertEqual(from_db("sqlite", "numeric", 120.5), Decimal("120.5"))
        self.assertEqual(from_db("sqlite", "numeric", "N/A"), "N/A")  # an unreadable stored value is returned, not raised

    def test_declared_types_map_back_to_field_types(self):
        self.assertEqual([canonical_type("sqlite", t) for t in ("TEXT", "INTEGER", "NUMERIC", "BOOLEAN", "DATE", "TIMESTAMP")],
                         ["text", "integer", "numeric", "boolean", "date", "timestamp"])
        self.assertEqual([canonical_type("postgres", t) for t in ("text", "bigint", "numeric", "boolean", "date", "timestamp with time zone")],
                         ["text", "integer", "numeric", "boolean", "date", "timestamp"])


class TestTypesInTheConfig(Base):
    def errors(self, doc):
        with self.assertRaises(DomainConfigError) as cm:
            parse_domain_document(doc, "t.json")
        return str(cm.exception)

    def test_types_and_formats_parse_and_round_trip_through_export(self):
        cfg = parse_domain_document(asset_doc())
        self.assertEqual([cfg.field_specs[f].type for f in ("tag", "qty", "cost", "active", "bought", "seen")],
                         ["text", "integer", "numeric", "boolean", "date", "timestamp"])
        self.assertEqual(cfg.field_specs["bought"].formats, ("%m/%d/%Y",))
        again = parse_domain_document(config_to_document(cfg))
        self.assertEqual(again.field_specs, cfg.field_specs)

    def test_the_shipped_product_template_declares_price_numeric(self):
        with open(os.path.join(ROOT, "domain_templates", "product.json")) as f:
            self.assertEqual(json.load(f)["fields"]["price"]["type"], "numeric")
        self.assertEqual(BUILTIN_DOMAINS["product"].field_specs["price"].type, "numeric")

    def test_mistakes_are_caught_up_front(self):
        self.assertIn("must be one of", self.errors(asset_doc(qty={"normalize": "text", "type": "int"})))
        self.assertIn("only applies to date and timestamp", self.errors(asset_doc(qty={"normalize": "text", "type": "integer", "format": "%d"})))
        self.assertIn("not a valid strptime pattern", self.errors(asset_doc(bought={"normalize": "text", "type": "date", "format": "%Q/%m"})))
        self.assertIn("strptime pattern", self.errors(asset_doc(bought={"normalize": "text", "type": "date", "format": "mm/dd/yyyy"})))
        self.assertIn("is not a valid integer", self.errors(asset_doc(qty={"normalize": "text", "type": "integer", "default": "lots"})))

    def test_changing_a_type_makes_derived_data_stale(self):
        self.register(asset_doc())
        ingest_record(self.store, "acme", "asset", "s", "1", {"tag": "A1", "qty": "5"})
        cfg = self.register(asset_doc(qty={"normalize": "text", "type": "numeric"}))
        self.assertEqual(index_state(self.store, "asset", cfg), "STALE")


class TestTypedColumns(Base):
    def test_the_builtins_are_text_except_product_price(self):
        self.assertEqual(set(self.declared("entity_customer").values()), {"TEXT"})
        self.assertEqual({k: v for k, v in self.declared("entity_product").items() if v != "TEXT"}, {"price": "NUMERIC"})

    def test_a_custom_domain_gets_one_real_type_per_field(self):
        self.register(asset_doc())
        ingest_record(self.store, "acme", "asset", "s", "1", {"tag": "A1"})
        d = self.declared("entity_asset")
        self.assertEqual([d[f] for f in ("tag", "qty", "cost", "active", "bought", "seen")], ["TEXT", "INTEGER", "NUMERIC", "BOOLEAN", "DATE", "TIMESTAMP"])

    def test_values_come_back_as_python_types_and_are_stored_as_real_database_values(self):
        self.register(asset_doc())
        r = ingest_record(self.store, "acme", "asset", "s", "1", {"tag": "a-1", "qty": "1,200", "cost": "$1,234.50", "active": "Yes",
                                                                  "bought": "03/04/2026", "seen": "2026-01-02T03:04:05Z"})
        a = self.golden(r, "asset").attributes
        self.assertEqual((a["qty"], a["cost"], a["active"], a["bought"], a["seen"]),
                         (1200, Decimal("1234.50"), True, date(2026, 3, 4), datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)))
        self.assertEqual([type(a[f]) for f in ("qty", "cost", "active", "bought", "seen")], [int, Decimal, bool, date, datetime])  # True == 1, so check the TYPE
        self.assertEqual(self.sql("SELECT qty, cost, active, bought FROM entity_asset")[0], (1200, 1234.5, 1, "2026-03-04"))

    def test_a_report_can_aggregate_and_filter_without_casting(self):
        register = self.register(parse_domain_dict := asset_doc())  # noqa: F841
        for i, (qty, cost) in enumerate([("10", "$100.00"), ("5", "$1,500.00"), ("1", "$20")]):
            ingest_record(self.store, "acme", "asset", "s", str(i), {"tag": f"T{i}", "qty": qty, "cost": cost})
        self.assertEqual(self.sql("SELECT SUM(qty), COUNT(*) FROM entity_asset WHERE cost > 50")[0], (15, 2))
        ingest_record(self.store, "acme", "product", "erp", "P1", {"sku": "P1", "product_name": "Widget", "price": "$1,299.00"})
        self.assertEqual(self.sql("SELECT COUNT(*) FROM entity_product WHERE price > 1000")[0][0], 1)

    def test_zero_and_false_are_values_not_missing(self):
        self.register(asset_doc())
        a = self.golden(ingest_record(self.store, "acme", "asset", "s", "1", {"tag": "A1", "qty": "0", "active": "no", "cost": "0.00"}), "asset").attributes
        self.assertEqual((a["qty"], a["active"], a["cost"]), (0, False, Decimal("0.00")))
        self.assertEqual(self.sql("SELECT qty, active FROM entity_asset")[0], (0, 0))  # stored as 0, not NULL

    def test_a_typed_entity_round_trips_through_the_store(self):
        self.register(asset_doc())
        ent = Entity(attributes={"tag": "A1", "qty": 7, "cost": Decimal("9.99"), "active": True, "bought": date(2026, 1, 2)}, tenant_id="acme", domain="asset",
                     quality_issues=[{"field": "seen", "value": "x"}])
        self.store.save_entity(ent)
        back = self.store.get_entity(ent.id, "acme", "asset")
        self.assertEqual((back.attributes["qty"], back.attributes["cost"], back.attributes["active"], back.attributes["bought"], back.attributes["seen"]),
                         (7, Decimal("9.99"), True, date(2026, 1, 2), None))
        self.assertEqual(back.quality_issues, [{"field": "seen", "value": "x"}])


class TestQualityAndSurvivorship(Base):
    W = {"sku": "W-1", "product_name": "Widget", "brand": "Acme"}

    def test_a_newer_invalid_value_cannot_beat_an_older_valid_one(self):
        ingest_record(self.store, "acme", "product", "crm", "A", {**self.W, "price": "100"})
        r = ingest_record(self.store, "acme", "product", "erp", "B", {**self.W, "price": "N/A"})  # NEWER, and not a number
        g = self.golden(r, "product")
        self.assertEqual(g.attributes["price"], Decimal("100"))
        self.assertEqual([(i["field"], i["value"]) for i in g.quality_issues], [("price", "N/A")])
        self.assertIn("N/A", self.sql("SELECT quality_issues FROM entity_product")[0][0])

    def test_fixing_the_source_clears_the_issue_and_the_new_value_wins(self):
        ingest_record(self.store, "acme", "product", "crm", "A", {**self.W, "price": "100"})
        ingest_record(self.store, "acme", "product", "erp", "B", {**self.W, "price": "N/A"})
        r = ingest_record(self.store, "acme", "product", "erp", "B", {**self.W, "price": "120"})
        g = self.golden(r, "product")
        self.assertEqual((g.attributes["price"], g.quality_issues), (Decimal("120"), []))

    def test_only_invalid_values_leave_the_column_null_and_say_why(self):
        g = self.golden(ingest_record(self.store, "acme", "product", "erp", "B", {**self.W, "price": "call us"}), "product")
        self.assertIsNone(g.attributes["price"])
        self.assertEqual(g.quality_issues[0]["problem"], "'call us' is not a valid numeric")

    def test_lineage_records_typed_values_as_text(self):
        ingest_record(self.store, "acme", "product", "crm", "A", {**self.W, "price": "100"})
        ingest_record(self.store, "acme", "product", "crm", "A", {**self.W, "price": "$1,299.00"})
        rows = self.sql("SELECT old_value, new_value FROM lineage WHERE field = 'price' ORDER BY changed_at")
        self.assertEqual(rows[-1], ("100", "1299.00"))


class TestChangingATypeLater(Base):
    def thing(self, amount="text", listed=None, **extra):
        fields = {"code": "identifier", "amount": {"normalize": "text", "type": amount}, "listed": listed or "text"}
        fields.update(extra)
        return self.register({"name": "thing", "fields": fields, "deterministic_fields": ["code"], "fuzzy_weights": {}, "blocking": [["code"]]})

    def load(self):
        self.thing()
        for rid, amount, listed in (("A", "12.5", "03/04/2026"), ("B", "oops", "2026-01-02"), ("C", "1,200", "")):
            ingest_record(self.store, "acme", "thing", "s", rid, {"code": rid, "amount": amount, "listed": listed})

    def values(self):
        return {r[0]: r[1:] for r in self.sql("SELECT code, amount, listed FROM entity_thing")}

    def test_retyping_re_casts_existing_values_and_unparseable_ones_become_null(self):
        self.load()
        self.thing(amount="numeric", listed={"normalize": "text", "type": "date", "format": ["%m/%d/%Y", "%Y-%m-%d"]})
        self.store.sync_entity_table("thing")  # the copy-and-swap, on its own
        d = self.declared("entity_thing")
        self.assertEqual((d["amount"], d["listed"]), ("NUMERIC", "DATE"))
        v = self.values()
        self.assertEqual((v["A"][0], v["B"][0], v["C"][0]), (12.5, None, 1200))             # "oops" -> NULL
        self.assertEqual((v["A"][1], v["B"][1]), (None, "2026-01-02"))                      # "03/04/2026" can't be read as ISO by the swap...

    def test_and_a_reindex_then_recovers_what_the_swap_could_not_parse(self):
        self.load()
        self.thing(amount="numeric", listed={"normalize": "text", "type": "date", "format": ["%m/%d/%Y", "%Y-%m-%d"]})
        reindex_domain(self.store, "thing")
        v = self.values()
        self.assertEqual(v["A"][1], "2026-03-04")                                            # ...but the rebuild reads it with YOUR format
        self.assertEqual((v["A"][0], v["B"][0], v["C"][0]), (12.5, None, 1200))
        issues = self.sql("SELECT quality_issues FROM entity_thing WHERE code = 'B'")[0][0]
        self.assertIn("oops", issues)                                                        # and says which value, and why

    def test_the_column_of_a_field_dropped_from_the_config_is_kept_through_a_retype(self):
        self.load()
        self.thing(amount="numeric", listed="text")
        self.register({"name": "thing", "fields": {"code": "identifier", "amount": {"normalize": "text", "type": "numeric"}},
                       "deterministic_fields": ["code"], "fuzzy_weights": {}, "blocking": [["code"]]})
        reindex_domain(self.store, "thing")
        self.assertIn("listed", self.declared("entity_thing"))
        self.assertEqual(self.sql("SELECT listed FROM entity_thing WHERE code = 'A'")[0][0], "03/04/2026")

    def test_a_number_can_be_turned_back_into_text(self):
        self.thing(amount="numeric")
        ingest_record(self.store, "acme", "thing", "s", "A", {"code": "A", "amount": "12.50"})
        self.thing(amount="text")
        reindex_domain(self.store, "thing")
        self.assertEqual(self.declared("entity_thing")["amount"], "TEXT")
        self.assertEqual(self.sql("SELECT amount FROM entity_thing")[0][0], "12.50")

    def test_the_app_alone_refuses_a_type_mismatch_and_says_what_to_run(self):
        self.load()
        self.thing(amount="numeric")
        verify_only = Store(self.dbpath, auto_migrate=False)
        with self.assertRaises(MigrationError) as cm:
            verify_only.list_entities("acme", "thing")
        self.assertIn("column type(s) differ", str(cm.exception))
        self.assertIn("domains_cli.py reindex thing", str(cm.exception))
        self.assertEqual(self.declared("entity_thing")["amount"], "TEXT")  # and it changed nothing


class TestMigration011(Base):
    def test_an_older_database_gets_typed_columns_and_quality_issues_in_place(self):
        path = os.path.join(self.tmp, "old.db")
        run_migrations(SQLiteMigrationBackend(path), MIGRATIONS[:10])
        c = sqlite3.connect(path)
        c.execute("ALTER TABLE entity_product DROP COLUMN quality_issues")  # the table as it was before this migration
        ts = "2026-01-01T00:00:00+00:00"
        for eid, price in (("ent_ok", "120.5"), ("ent_bad", "N/A")):
            c.execute("INSERT INTO entity_product (entity_id, tenant_id, status, created_at, updated_at, sku, price) VALUES (?, 'acme', 'active', ?, ?, ?, ?)",
                      (eid, ts, ts, eid, price))
        c.commit()
        c.close()
        store = Store(path)
        c = sqlite3.connect(path)
        cols = {r[1]: r[2] for r in c.execute('PRAGMA table_info("entity_product")')}
        self.assertEqual((cols["price"], "quality_issues" in cols), ("NUMERIC", True))
        self.assertEqual(c.execute("SELECT entity_id, price FROM entity_product ORDER BY entity_id").fetchall(), [("ent_bad", None), ("ent_ok", 120.5)])
        self.assertEqual(store.get_entity("ent_ok", "acme", "product").attributes["price"], Decimal("120.5"))
        for table in ("entity_customer", "entity_vendor"):
            self.assertIn("quality_issues", [r[1] for r in c.execute(f'PRAGMA table_info("{table}")')])
        self.assertEqual(run_migrations(SQLiteMigrationBackend(path)), [])  # idempotent


class TestCli(Base):
    def cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), *args], capture_output=True, text=True, cwd=ROOT)

    def test_schema_shows_types_and_formats(self):
        out = self.cli("schema", "product", "--db", self.dbpath)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertRegex(out.stdout, r"price\s+numeric")
        self.assertIn("quality_issues", out.stdout)

    def test_normalizers_lists_the_types(self):
        out = self.cli("normalizers")
        self.assertIn("numeric", out.stdout)
        self.assertIn("timestamp", out.stdout)


if __name__ == "__main__":
    unittest.main()
