"""
A RATCHET against database-specific SQL spreading. Supporting another database costs roughly the amount of PostgreSQL-only SQL, raw `execute` calls, driver imports and
`if backend == "postgres"` branches there are outside the places that are SUPPOSED to hold them, so those counts may not grow, and when they shrink the baseline must be lowered
(otherwise slack would hide the next increase).

  * Where they are supposed to be: the two stores (db.py, db_postgres.py), the migrations (versions.py, runner.py), the row-level-security module, app/sql_dialect.py (the one home for
    SQL text that differs between databases), app/fieldtypes.py (the type table the dialect reads), and two modules that are not the customer's database at all: bulk.py (it runs on
    scratch SQLite files in a temp folder, and is retired by the single-engine design) and benchmark_person.py (a development tool).
  * New code for the matching kernel's tables should take its SQL from app/sql_dialect.py (upsert, in_list, keyset_after, versioned_update, claim_suffix), which is what keeps these
    numbers flat. If a test here fails: if the new SQL is deliberate, raise the number in BASELINE below and say why in the change; if it is not, use the dialect.

Run with:  python3 -m unittest tests.test_sql_portability -v
"""
import glob
import os
import re
import unittest

ROOT = os.path.join(os.path.dirname(__file__), "..")

# The patterns are CASE SENSITIVE on purpose: the SQL in this code base is upper case, and Python's own any(...) or the English word "returning" must not count.
FEATURES = {'ANY(array) parameter': '=\\s*ANY\\(',
 'JSONB type or JSON operator': '\\bJSONB\\b|jsonb_\\w+|->>|#>>',
 'ON CONFLICT (upsert)': 'ON CONFLICT',
 'TIMESTAMPTZ': '\\bTIMESTAMPTZ\\b',
 'partial index (CREATE INDEX .. WHERE)': 'CREATE (?:UNIQUE )?INDEX[^;\\"]*\\bWHERE\\b',
 'row locking (FOR UPDATE / SKIP LOCKED / advisory lock)': 'FOR UPDATE|SKIP LOCKED|pg_advisory',
 'row-level security': 'ROW LEVEL SECURITY|CREATE POLICY|set_config\\(|current_setting\\(',
 'row-value comparison': '\\(\\s*[a-z_.]+\\s*,\\s*[a-z_.]+\\s*\\)\\s*[<>]=?\\s*\\(',
 'typed cast ::type': '\\)::[a-z]+|\\w::(?:text|int|integer|bigint|numeric|jsonb|timestamptz|timestamp|date|boolean)\\b'}
CONFINE = {"branches on the backend's name": '(?:backend|backend_name|\\.name)\\s*==\\s*[\\"\'](?:postgres|sqlite)[\\"\']|[\\"\'](?:postgres|sqlite)[\\"\']\\s*==',
 'calls .execute( or .executemany(': '\\.execute(?:many)?\\(',
 'imports a database driver': '^\\s*(?:import|from)\\s+(?:sqlite3|psycopg2)\\b'}
# 'ON CONFLICT (upsert)' in sql_dialect.py went from 3 to 4 on purpose: conflict_suffix() became its own helper (the multi-row inserts of Gate P's SqlState need the clause by itself).
BASELINE = {'FEATURES': {'ON CONFLICT (upsert)': {'db.py': 4, 'db_postgres.py': 12, 'migrations/versions.py': 1, 'sql_dialect.py': 4},
              'JSONB type or JSON operator': {'db_postgres.py': 5, 'migrations/versions.py': 13, 'sql_dialect.py': 2},
              'ANY(array) parameter': {'db_postgres.py': 14, 'sql_dialect.py': 1},
              'typed cast ::type': {'db_postgres.py': 2},
              'row locking (FOR UPDATE / SKIP LOCKED / advisory lock)': {'migrations/runner.py': 2, 'sql_dialect.py': 6},
              'row-level security': {'rls_policies.py': 8},
              'partial index (CREATE INDEX .. WHERE)': {'migrations/versions.py': 2},
              'TIMESTAMPTZ': {'fieldtypes.py': 1},
              'row-value comparison': {'bulk.py': 2, 'db.py': 1, 'db_postgres.py': 1, 'migrations/versions.py': 1, 'sql_dialect.py': 2}},
 'CONFINE': {'imports a database driver': {'benchmark_person.py': 1,
                                           'bulk.py': 1,
                                           'db.py': 1,
                                           'db_postgres.py': 4,
                                           'migrations/runner.py': 3,
                                           'sql_dialect.py': 1},
             'calls .execute( or .executemany(': {'benchmark_person.py': 9,
                                                  'bulk.py': 76,
                                                  'db.py': 126,
                                                  'db_postgres.py': 126,
                                                  'golden.py': 11,
                                                  'migrations/runner.py': 16,
                                                  'migrations/versions.py': 18},
             "branches on the backend's name": {'fieldtypes.py': 2, 'migrations/runner.py': 3, 'migrations/versions.py': 4, 'sql_dialect.py': 1}}}


FLAGS = {name: re.M for name in CONFINE}


def count_everything():
    found = {"FEATURES": {n: {} for n in FEATURES}, "CONFINE": {n: {} for n in CONFINE}}
    for path in sorted(glob.glob(os.path.join(ROOT, "app", "**", "*.py"), recursive=True)):
        rel = os.path.relpath(path, os.path.join(ROOT, "app")).replace(os.sep, "/")
        with open(path) as f:
            text = f.read()
        for kind, table in (("FEATURES", FEATURES), ("CONFINE", CONFINE)):
            for name, rx in table.items():
                n = len(re.findall(rx, text, FLAGS.get(name, 0)))
                if n:
                    found[kind][name][rel] = n
    return found


class TestDatabaseSpecificSqlDoesNotSpread(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.found = count_everything()

    def rows(self):
        for kind in ("FEATURES", "CONFINE"):
            for name in BASELINE[kind]:
                yield kind, name, BASELINE[kind][name], self.found[kind][name]

    def test_nothing_grew_and_nothing_new_appeared(self):
        problems = []
        for kind, name, base, now in self.rows():
            for rel, n in sorted(now.items()):
                if n > base.get(rel, 0):
                    problems.append(f"{rel}: {n} x '{name}' (allowed {base.get(rel, 0)})")
        self.assertEqual(problems, [], "database-specific SQL or driver use grew. Use app/sql_dialect.py for new SQL; if it is deliberate raise the number in BASELINE and say why.")

    def test_the_baseline_has_no_slack(self):
        slack = []
        for kind, name, base, now in self.rows():
            for rel, n in sorted(base.items()):
                if now.get(rel, 0) < n:
                    slack.append(f"{rel}: '{name}' is now {now.get(rel, 0)}, BASELINE says {n}")
        self.assertEqual(slack, [], "this is PROGRESS: lower these numbers in BASELINE so the next increase cannot hide in the slack.")

    def test_the_schema_code_no_longer_branches_on_the_database_or_imports_a_driver(self):
        for rel in ("golden.py", "pipeline.py", "matching.py", "unmerge.py", "stewardship.py", "monitor.py", "learned_scorer.py", "api.py"):
            for kind, name in (("CONFINE", "branches on the backend's name"), ("CONFINE", "imports a database driver")):
                self.assertNotIn(rel, self.found[kind][name], f"{rel} {name}: that belongs in the stores or app/sql_dialect.py")
        self.assertNotIn("golden.py", self.found["CONFINE"]["imports a database driver"])   # golden.py used to import psycopg2 for its JSON type; the dialect owns that now

    def test_the_known_homes_are_still_where_the_sql_is(self):
        """If these were all zero the ratchet would be vacuous (a renamed file, a wrong glob)."""
        self.assertGreater(self.found["FEATURES"]["ON CONFLICT (upsert)"].get("db_postgres.py", 0), 5)
        self.assertGreater(self.found["CONFINE"]["calls .execute( or .executemany("].get("db.py", 0), 50)
        self.assertGreater(sum(self.found["CONFINE"]["imports a database driver"].values()), 3)


if __name__ == "__main__":
    unittest.main()
