"""
The dialect module (app/sql_dialect.py) and the two stores' shared interface.

  * The SQL that the schema code and the migration context send to SQLite and to PostgreSQL is pinned, character for character, against tests/data/ddl_snapshot.json, which was captured BEFORE
    that text was gathered into the dialect. This is the only way the PostgreSQL text is checked here, because there is no PostgreSQL server in the places this is developed: a diff
    is a change to what customers' databases are sent, so it has to be deliberate (regenerate with `python3 tests/ddl_snapshot.py`, and say why).
  * Every helper meant for new code (upsert, in_list, keyset_after, versioned_update, claim_suffix) is executed against a real SQLite, and its PostgreSQL text is asserted.
    tests/test_store_contract.py runs the same helpers against PostgreSQL when TEST_DATABASE_URL is set.
  * The two stores expose the same methods with the same parameters. They had drifted by one method; that one is a named exception below.

Run with:  python3 -m unittest tests.test_dialect -v
"""
import inspect
import json
import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)

import ddl_snapshot  # noqa: E402
from app.db import Store  # noqa: E402
from app.db_postgres import PostgresStore  # noqa: E402
from app.fieldtypes import FIELD_TYPES  # noqa: E402
from app.sql_dialect import PostgresDialect, SqliteDialect, get_dialect  # noqa: E402

SQLITE, PG = get_dialect("sqlite"), get_dialect("postgres")
SNAPSHOT = os.path.join(HERE, "data", "ddl_snapshot.json")

# Public methods that exist in only one store. Each needs a reason; a NEW entry is a drift between the databases and should be a decision, not an accident.
ONE_STORE_ONLY = {
    "find_entities_by_attribute": "PostgreSQL only. Nothing in the application calls it (two tests in test_postgres_backend.py do). Add the SQLite twin if the application ever needs it.",
}


class TestThePinnedSql(unittest.TestCase):
    def test_the_sql_sent_to_each_database_is_exactly_what_it_was_before_the_dialect_move(self):
        now = json.loads(json.dumps(ddl_snapshot.record()))
        with open(SNAPSHOT) as f:
            was = json.load(f)
        self.assertEqual(sorted(now), sorted(was))
        for backend in was:
            self.assertEqual(sorted(now[backend]), sorted(was[backend]), backend)
            for scenario in was[backend]:
                self.assertEqual(now[backend][scenario], was[backend][scenario], f"{backend} / {scenario}")

    def test_the_snapshot_really_contains_the_differences_between_the_two_databases(self):
        with open(SNAPSHOT) as f:
            was = json.load(f)
        pg = " ".join(s[1] for s in was["postgres"]["retype"] + was["postgres"]["add_columns"] + was["postgres"]["new_table"])
        lite = " ".join(s[1] for s in was["sqlite"]["retype"] + was["sqlite"]["add_columns"] + was["sqlite"]["new_table"])
        for fragment in ("JSONB", "BIGINT", "TIMESTAMPTZ", "information_schema", 'RENAME TO "entity_customer"', "ADD COLUMN", "CREATE INDEX IF NOT EXISTS"):
            self.assertIn(fragment, pg, fragment)
        self.assertNotIn("JSONB", lite)
        self.assertNotIn("TIMESTAMPTZ", lite)
        self.assertIn("PRAGMA table_info", " ".join(s[1] for s in was["sqlite"]["context_basics"]))
        self.assertIn("%s", " ".join(s[1] for s in was["postgres"]["context_basics"]))     # the placeholder was translated for the driver
        self.assertNotIn("%s", " ".join(s[1] for s in was["sqlite"]["context_basics"]))


class TestTheDialectText(unittest.TestCase):
    def test_the_placeholder_is_translated_for_the_driver(self):
        self.assertEqual(SQLITE.sql("a = ? AND b = ?"), "a = ? AND b = ?")
        self.assertEqual(PG.sql("a = ? AND b = ?"), "a = %s AND b = %s")

    def test_ddl_text(self):
        for d in (SQLITE, PG):
            self.assertEqual(d.create_index_if_not_exists('"i"', '"t"', "a, b"), 'CREATE INDEX IF NOT EXISTS "i" ON "t"(a, b)')
            self.assertEqual(d.add_column('"t"', '"c"', "TEXT"), 'ALTER TABLE "t" ADD COLUMN "c" TEXT')
            self.assertEqual(d.rename_table('"a"', '"b"'), 'ALTER TABLE "a" RENAME TO "b"')
            self.assertEqual(d.drop_table('"t"'), 'DROP TABLE "t"')

    def test_the_two_databases_differ_where_they_should(self):
        self.assertEqual((SQLITE.json_type, PG.json_type), ("TEXT", "JSONB"))
        self.assertEqual((SQLITE.column_type("integer"), PG.column_type("integer")), ("INTEGER", "BIGINT"))
        self.assertEqual((SQLITE.column_type("timestamp"), PG.column_type("timestamp")), ("TIMESTAMP", "TIMESTAMPTZ"))
        self.assertIn("sqlite_master", SQLITE.table_exists_query("x")[0])
        self.assertIn("information_schema.tables", PG.table_exists_query("x")[0])
        self.assertEqual(SQLITE.column_types_query("entity_x"), ("PRAGMA table_info(entity_x)", ()))
        self.assertEqual(PG.column_types_query("entity_x")[1], ("entity_x",))

    def test_every_field_type_has_a_column_type_in_each_database_and_sqlite_reads_it_back(self):
        for t in FIELD_TYPES:
            for d in (SQLITE, PG):
                self.assertTrue(d.column_type(t), (d.name, t))
            self.assertEqual(SQLITE.canonical_type(SQLITE.column_type(t)), t)
        self.assertEqual(PG.canonical_type("bigint"), "integer")                       # information_schema's names, which is what Postgres reports
        self.assertEqual(PG.canonical_type("timestamp with time zone"), "timestamp")

    def test_catalog_answers_are_parsed_per_database(self):
        self.assertEqual(SQLITE.parse_column_types([{"name": "a", "type": "text"}, {"name": "b", "type": None}]), {"a": "TEXT", "b": ""})
        self.assertEqual(PG.parse_column_types([{"column_name": "a", "data_type": "text"}]), {"a": "text"})
        self.assertEqual(SQLITE.parse_columns([{"name": "a"}, {"name": "b"}]), {"a", "b"})
        self.assertEqual(PG.parse_columns([{"column_name": "a"}]), {"a"})

    def test_json_values_for_the_sqlite_driver(self):
        self.assertIsNone(SQLITE.json_param(None))
        self.assertIsNone(SQLITE.json_param(""))
        self.assertEqual(SQLITE.json_param('[{"a": 1}]'), '[{"a": 1}]')
        self.assertEqual(json.loads(SQLITE.json_param([{"a": 1}])), [{"a": 1}])
        self.assertIsNone(PG.json_param(None))                                          # (the JSON adapter itself needs psycopg2; None and "" never reach it)

    def test_an_unknown_database_is_refused_with_the_way_forward(self):
        with self.assertRaises(ValueError) as cm:
            get_dialect("oracle")
        self.assertIn("docs/database-support.md", str(cm.exception))
        self.assertIsInstance(get_dialect("sqlite"), SqliteDialect)
        self.assertIsInstance(get_dialect("postgres"), PostgresDialect)


class TestTheHelpersForNewCode(unittest.TestCase):
    """Executed on a real SQLite. The same helpers run on PostgreSQL in test_store_contract.py when TEST_DATABASE_URL is set."""

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE probe (k TEXT PRIMARY KEY, v INTEGER, w INTEGER, version INTEGER NOT NULL DEFAULT 0)")
        self.addCleanup(self.db.close)

    def row(self, k):
        return self.db.execute("SELECT v, w, version FROM probe WHERE k = ?", (k,)).fetchone()

    def test_upsert_inserts_then_updates_then_can_do_nothing(self):
        sql = SQLITE.upsert("probe", ["k", "v", "w"], ["k"])
        self.db.execute(sql, ("a", 1, 1))
        self.db.execute(sql, ("a", 2, 2))
        self.assertEqual(self.row("a")[:2], (2, 2))
        self.db.execute(SQLITE.upsert("probe", ["k", "v", "w"], ["k"], update=["v"]), ("a", 3, 3))
        self.assertEqual(self.row("a")[:2], (3, 2))                                     # only v was updated
        self.db.execute(SQLITE.upsert("probe", ["k", "v", "w"], ["k"], update=[]), ("a", 9, 9))
        self.assertEqual(self.row("a")[:2], (3, 2))                                     # DO NOTHING

    def test_the_conflict_clause_is_its_own_piece_and_upsert_is_built_from_it(self):
        self.assertEqual(SQLITE.conflict_suffix(["k", "v", "w"], ["k"]), "ON CONFLICT (k) DO UPDATE SET v = excluded.v, w = excluded.w")
        self.assertEqual(SQLITE.conflict_suffix(["k", "v"], ["k"], update=[]), "ON CONFLICT (k) DO NOTHING")
        self.assertEqual(SQLITE.conflict_suffix(["a", "b", "c"], ["a", "b"], update=["c"]), "ON CONFLICT (a, b) DO UPDATE SET c = excluded.c")
        self.assertEqual(SQLITE.upsert("probe", ["k", "v"], ["k"]), f"INSERT INTO probe (k, v) VALUES (?, ?) {SQLITE.conflict_suffix(['k', 'v'], ['k'])}")

    def test_many_composite_keys_are_deleted_by_one_statement(self):
        self.db.execute("CREATE TABLE pairs (tenant TEXT, k TEXT, g TEXT, PRIMARY KEY (tenant, k, g))")
        self.db.executemany("INSERT INTO pairs VALUES (?, ?, ?)", [("t", f"k{i}", f"g{i % 3}") for i in range(30)] + [("u", "k1", "g1")])
        doomed = [("k1", "g1"), ("k2", "g2"), ("k3", "g9"), ("k4", "g1")]                 # k3/g9 does not exist; k4/g1 exists as k4/g1
        sql = f"DELETE FROM pairs WHERE tenant = ? AND {SQLITE.row_in(['k', 'g'], len(doomed))}"
        n = self.db.execute(sql, ("t", *[v for pair in doomed for v in pair])).rowcount
        self.assertEqual(n, 3)                                                              # k1/g1, k2/g2, k4/g1: the ones that exist, for THIS tenant only
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM pairs WHERE tenant = 'u'").fetchone()[0], 1)
        self.assertEqual(PG.row_in(["k", "g"], 2), "(k, g) IN (VALUES (%s, %s), (%s, %s))")

    def test_upsert_text_for_postgres(self):
        self.assertEqual(PG.upsert("probe", ["k", "v", "w"], ["k"]), "INSERT INTO probe (k, v, w) VALUES (%s, %s, %s) ON CONFLICT (k) DO UPDATE SET v = excluded.v, w = excluded.w")
        self.assertTrue(PG.upsert("probe", ["k", "v"], ["k"], update=[]).endswith("ON CONFLICT (k) DO NOTHING"))

    def test_in_list_matches_the_values_and_an_empty_list_matches_nothing(self):
        self.db.executemany("INSERT INTO probe (k, v) VALUES (?, ?)", [(c, i) for i, c in enumerate("abcdef")])
        sql, params = SQLITE.in_list("k", ["b", "d", "zz"])
        self.assertEqual(sorted(r[0] for r in self.db.execute(f"SELECT k FROM probe WHERE {sql}", params)), ["b", "d"])
        sql, params = SQLITE.in_list("k", [])
        self.assertEqual(self.db.execute(f"SELECT COUNT(*) FROM probe WHERE {sql}", params).fetchone()[0], 0)
        self.assertEqual((sql, params), ("1 = 0", []))                                  # SQLite would accept `IN ()`; SQL Server and Oracle do not, so the TEXT is what is pinned
        self.assertEqual(PG.in_list("k", ["b", "d"]), ("k = ANY(%s)", [["b", "d"]]))
        self.assertEqual(PG.in_list("k", []), ("1 = 0", []))

    def test_too_long_an_in_list_is_refused_and_chunks_cover_everything_once(self):
        values = list(range(2000))
        with self.assertRaises(ValueError):
            SQLITE.in_list("k", values)
        chunks = list(SQLITE.chunks(values))
        self.assertTrue(all(len(c) <= SQLITE.max_in_values for c in chunks))
        self.assertEqual([v for c in chunks for v in c], values)

    def test_keyset_paging_visits_every_row_once_even_with_many_ties(self):
        self.assertGreaterEqual(sqlite3.sqlite_version_info, (3, 15), "row-value comparison needs SQLite 3.15")
        rows = [(f"k{i:03d}", i % 3, 0) for i in range(40)]                              # v has only three distinct values: most rows tie
        self.db.executemany("INSERT INTO probe (k, v, w) VALUES (?, ?, ?)", rows)
        want = [r[0] for r in self.db.execute("SELECT k FROM probe ORDER BY v DESC, k DESC")]
        seen, after = [], None
        while True:
            where = "" if after is None else "WHERE " + SQLITE.keyset_after(["v", "k"], descending=True)
            page = self.db.execute(f"SELECT k, v FROM probe {where} ORDER BY v DESC, k DESC {SQLITE.limit_clause()}", ((after[1], after[0]) if after else ()) + (7,)).fetchall()
            if not page:
                break
            seen += [r[0] for r in page]
            after = page[-1]
        self.assertEqual(seen, want)
        self.assertEqual(PG.keyset_after(["score", "id"], descending=True), "(score, id) < (%s, %s)")

    def test_a_versioned_update_loses_to_the_writer_who_got_there_first(self):
        self.db.execute("INSERT INTO probe (k, v, version) VALUES ('a', 1, 0)")
        sql = SQLITE.versioned_update("probe", ["v"], ["k"])
        first = self.db.execute(sql, (10, "a", 0)).rowcount          # both writers read version 0
        second = self.db.execute(sql, (20, "a", 0)).rowcount
        self.assertEqual((first, second), (1, 0))
        self.assertEqual(self.row("a"), (10, None, 1))
        self.assertEqual(PG.versioned_update("probe", ["v", "w"], ["k"]), "UPDATE probe SET v = %s, w = %s, version = version + 1 WHERE k = %s AND version = %s")

    def test_claiming_rows_skips_locked_ones_only_where_the_database_can(self):
        self.assertEqual(SQLITE.claim_suffix(), "")
        self.assertEqual(PG.claim_suffix(), " FOR UPDATE SKIP LOCKED")


class TestTheTwoStoresShareOneInterface(unittest.TestCase):
    @staticmethod
    def public(cls):
        return {n: f for n, f in inspect.getmembers(cls, inspect.isfunction) if not n.startswith("_")}

    @staticmethod
    def params(f):
        return [(p.name, None if p.default is inspect.Parameter.empty else repr(p.default), str(p.kind)) for p in inspect.signature(f).parameters.values()]

    def test_the_same_methods_exist_in_both_stores_apart_from_the_named_exceptions(self):
        a, b = self.public(Store), self.public(PostgresStore)
        self.assertEqual(sorted((set(a) ^ set(b)) - set(ONE_STORE_ONLY)), [], "a method exists in only one store: add it to the other, or name it in ONE_STORE_ONLY with a reason")
        for name in ONE_STORE_ONLY:
            self.assertEqual(len({name} & set(a)) + len({name} & set(b)), 1, f"{name} is no longer in exactly one store: remove it from ONE_STORE_ONLY")

    def test_shared_methods_take_the_same_parameters_with_the_same_defaults(self):
        a, b = self.public(Store), self.public(PostgresStore)
        for name in sorted(set(a) & set(b)):
            self.assertEqual(self.params(a[name]), self.params(b[name]), name)


if __name__ == "__main__":
    unittest.main()
