"""
Schema migration system. Everything here runs on SQLite with zero setup,
except the Postgres class, which drives the real runner against a fake
psycopg2 to verify control flow (locking, transaction boundaries,
placeholder translation) -- NOT real SQL execution on a live server.

Run with:  python3 -m unittest tests.test_migrations -v
"""
import os
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from contextlib import contextmanager
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import Store
from app.migrations import (
    Migration,
    MigrationError,
    PostgresMigrationBackend,
    SQLiteMigrationBackend,
    apply_or_verify,
    get_status,
    run_migrations,
)
from app.migrations.versions import MIGRATIONS
from app.pipeline import ingest_record
from tests.legacy_schema import LEGACY_SQLITE_SCHEMA


# ---------------------------------------------------------------- helpers
def tmp_db(testcase) -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.remove(path)  # start with no file at all, like a first run
    testcase.addCleanup(lambda: os.path.exists(path) and os.remove(path))
    return path


def rows(path, sql, params=()):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def table_names(path):
    return {r["name"] for r in rows(path, "SELECT name FROM sqlite_master WHERE type = 'table'")}


def describe_schema(path):
    """Columns (name, type, notnull, default, pk) and index signatures per
    table, ignoring index names -- the structure that matters."""
    conn = sqlite3.connect(path)
    try:
        out = {}
        names = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations'")]
        for t in names:
            cols = [(r[1], r[2], r[3], r[4], r[5]) for r in conn.execute(f"PRAGMA table_info({t})")]
            idx = set()
            for r in conn.execute(f"PRAGMA index_list({t})"):
                idx.add((r[2], tuple(x[2] for x in conn.execute(f"PRAGMA index_info({r[1]})"))))
            out[t] = (cols, idx)
        return out
    finally:
        conn.close()


# ------------------------------------------------------------ fresh install
class TestFreshDatabase(unittest.TestCase):
    def test_new_database_gets_every_migration_in_order(self):
        path = tmp_db(self)
        Store(path)
        applied = rows(path, "SELECT namespace, version FROM schema_migrations ORDER BY version")
        self.assertEqual([(r["namespace"], r["version"]) for r in applied],
                         [("platform", m.version) for m in MIGRATIONS])
        self.assertTrue({"source_records", "match_candidates", "lineage", "api_keys", "audit_log", "usage_events", "blocking_keys",
                         "standardized_records", "entity_customer", "entity_product", "entity_vendor"} <= table_names(path))
        self.assertNotIn("entities", table_names(path))  # replaced by one table per domain (migration 009)

    def test_second_startup_applies_nothing(self):
        path = tmp_db(self)
        Store(path)
        self.assertEqual(run_migrations(SQLiteMigrationBackend(path)), [])

    def test_migrated_schema_is_identical_to_what_the_old_code_created(self):
        """The migrations must reproduce the pre-migration schema exactly,
        or databases built the two ways would behave differently."""
        legacy, migrated = tmp_db(self), tmp_db(self)
        conn = sqlite3.connect(legacy)
        conn.executescript(LEGACY_SQLITE_SCHEMA)
        conn.commit()
        conn.close()
        Store(migrated)
        legacy_schema, migrated_schema = describe_schema(legacy), describe_schema(migrated)
        # Everything the old code created must be reproduced exactly. What LATER migrations added is the only
        # permitted difference: a table (006 blocking_index_state) and a trailing column (007 match_candidates.contributions).
        # `entities` (one table, JSON attributes) was replaced by entity_<domain> tables (migration 009)
        self.assertEqual(set(migrated_schema) - set(legacy_schema), {"blocking_index_state", "standardized_records",
                                                                      "entity_customer", "entity_product", "entity_vendor",
                                                                      "source_record_history", "pipeline_runs", "golden_dirty", "merge_audits"})
        self.assertNotIn("entities", migrated_schema)
        for table, (cols, idx) in {t: v for t, v in legacy_schema.items() if t != "entities"}.items():
            m_cols, m_idx = migrated_schema[table]
            self.assertEqual(m_cols[:len(cols)], cols, table)
            later_idx = {"source_records": {(0, ("tenant_id", "domain", "source_system", "source_record_id")),  # 010: lookup by natural key
                                            (0, ("domain", "ingested_at"))},                                    # 012: only the UNFINISHED rows
                         "match_candidates": {(0, ("tenant_id", "domain", "record_a_id")), (0, ("tenant_id", "domain", "record_b_id")),
                                              (0, ("tenant_id", "domain", "score", "id"))}}   # 016: the pending queue, in the order it is shown
            self.assertEqual(m_idx - idx, later_idx.get(table, set()), table)
            self.assertTrue(idx <= m_idx, table)
            extra = [c[0] for c in m_cols[len(cols):]]
            later = {"match_candidates": ["contributions"], "source_records": ["content_hash", "version", "updated_at", "processing_status", "processing_attempts", "processing_error"]}
            self.assertEqual(extra, later.get(table, []), table)  # columns added by LATER migrations (007, 010)

    def test_no_schema_changes_when_already_current(self):
        """An up-to-date database must start without issuing any DDL or
        writes -- this is what lets an app role with no CREATE/ALTER
        privilege boot against a schema someone else already migrated."""
        path = tmp_db(self)
        Store(path)

        class Tracing(SQLiteMigrationBackend):
            def __init__(self, p):
                super().__init__(p)
                self.statements = []

            @contextmanager
            def connection(self):
                with super().connection() as conn:
                    conn.set_trace_callback(self.statements.append)
                    yield conn

        backend = Tracing(path)
        run_migrations(backend)
        self.assertTrue(any(x.strip().upper().startswith("SELECT") for x in backend.statements),
                        "tracing captured nothing, so this test would pass vacuously")
        writes = [s for s in backend.statements
                  if s.strip().upper().split(" ")[0] in ("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE", "BEGIN", "DROP")]
        self.assertEqual(writes, [])


# ------------------------------------------- adopting existing databases
class TestAdoptingExistingDatabases(unittest.TestCase):
    def test_legacy_database_is_adopted_with_its_data_intact(self):
        path = tmp_db(self)
        conn = sqlite3.connect(path)
        conn.executescript(LEGACY_SQLITE_SCHEMA)  # exactly what the old code created
        conn.execute("INSERT INTO entities (id, tenant_id, domain, status, attributes, created_at, updated_at) "
                     "VALUES ('ent_old', 'acme', 'customer', 'active', '{\"email\": \"old@x.com\"}', 't', 't')")
        conn.commit()
        conn.close()

        Store(path)  # previously: nothing could change this DB. Now it is adopted.

        # the golden record moved, id and all, into its domain's table (migration 009); the old table is kept as a safety net
        self.assertEqual([r["entity_id"] for r in rows(path, "SELECT entity_id FROM entity_customer")], ["ent_old"])
        self.assertEqual([r["id"] for r in rows(path, "SELECT id FROM entities_json_legacy")], ["ent_old"])
        self.assertEqual([r["version"] for r in rows(path, "SELECT version FROM schema_migrations ORDER BY version")],
                         [m.version for m in MIGRATIONS])

    def test_backfill_makes_pre_index_records_matchable(self):
        """The scenario the blocking_keys backfill exists for: records ingested
        before the index existed had no index rows, so nothing new could
        ever match them."""
        path = tmp_db(self)
        store = Store(path)
        old = ingest_record(store, "acme", "customer", "crm", "C1",
                            {"first_name": "Robert", "last_name": "Smith", "email": "bob@x.com"})
        # Make it look like a database from before the index and the migration system.
        conn = sqlite3.connect(path)
        conn.execute("DROP TABLE blocking_keys")
        conn.execute("DROP TABLE schema_migrations")
        conn.commit()
        conn.close()

        store2 = Store(path)  # adopts, recreates blocking_keys (003), backfills (005)

        keyed = {r["record_id"] for r in rows(path, "SELECT record_id FROM blocking_keys")}
        self.assertIn(old["record_id"], keyed)
        new = ingest_record(store2, "acme", "customer", "shop", "S1",
                            {"first_name": "Bob", "last_name": "Smith", "email": "bob@x.com"})
        self.assertEqual([m["record_id"] for m in new["auto_merged_with"]], [old["record_id"]])

    def test_backfill_does_not_duplicate_keys_on_an_already_indexed_database(self):
        path = tmp_db(self)
        store = Store(path)
        ingest_record(store, "acme", "customer", "crm", "C1", {"first_name": "A", "last_name": "B", "email": "a@b.com"})
        before = rows(path, "SELECT COUNT(*) AS n FROM blocking_keys")[0]["n"]
        conn = sqlite3.connect(path)
        conn.execute("DROP TABLE schema_migrations")  # force the whole history to re-run over an indexed DB
        conn.commit()
        conn.close()
        Store(path)
        self.assertEqual(rows(path, "SELECT COUNT(*) AS n FROM blocking_keys")[0]["n"], before)

    def test_database_older_than_multi_tenant_support_is_refused_clearly(self):
        path = tmp_db(self)
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE entities (id TEXT PRIMARY KEY, status TEXT, attributes TEXT)")  # no tenant_id/domain
        conn.commit()
        conn.close()
        with self.assertRaises(MigrationError) as cm:
            Store(path)
        self.assertIn("predates", str(cm.exception))
        self.assertEqual(table_names(path), {"entities"})  # nothing created, nothing changed


# ------------------------------------------------------------- safety rails
class TestSafetyRails(unittest.TestCase):
    def setUp(self):
        self.path = tmp_db(self)
        self.backend = SQLiteMigrationBackend(self.path)

    def test_failed_migration_rolls_back_completely(self):
        good = Migration(1, "make_t1", sqlite=("CREATE TABLE t1 (x INTEGER)",))
        broken = Migration(2, "half_broken", sqlite=("CREATE TABLE t2 (x INTEGER)", "THIS IS NOT SQL"))
        with self.assertRaises(sqlite3.OperationalError):
            run_migrations(self.backend, [good, broken], namespace="test")
        names = table_names(self.path)
        self.assertIn("t1", names)          # migration 1 committed on its own
        self.assertNotIn("t2", names)       # migration 2's first statement was rolled back
        self.assertEqual([r["version"] for r in rows(self.path, "SELECT version FROM schema_migrations")], [1])

    def test_failed_data_step_rolls_back_its_sql_too(self):
        def explode(ctx):
            raise RuntimeError("data step failed")
        m = Migration(1, "sql_then_explode", sqlite=("CREATE TABLE t1 (x INTEGER)",), data_step=explode)
        with self.assertRaises(RuntimeError):
            run_migrations(self.backend, [m], namespace="test")
        self.assertNotIn("t1", table_names(self.path))

    def test_editing_an_applied_migration_is_detected(self):
        run_migrations(self.backend, [Migration(1, "t", sqlite=("CREATE TABLE t1 (x INTEGER)",))], namespace="test")
        edited = Migration(1, "t", sqlite=("CREATE TABLE t1 (x INTEGER, y INTEGER)",))
        with self.assertRaises(MigrationError) as cm:
            run_migrations(self.backend, [edited], namespace="test")
        self.assertIn("modified after being applied", str(cm.exception))

    def test_database_newer_than_the_code_is_refused(self):
        v1 = Migration(1, "a", sqlite=("CREATE TABLE a (x INTEGER)",))
        v2 = Migration(2, "b", sqlite=("CREATE TABLE b (x INTEGER)",))
        run_migrations(self.backend, [v1, v2], namespace="test")
        with self.assertRaises(MigrationError) as cm:
            run_migrations(self.backend, [v1], namespace="test")  # old code, newer database
        self.assertIn("NEWER", str(cm.exception))

    def test_migration_versions_must_be_contiguous(self):
        m1 = Migration(1, "a", sqlite=("CREATE TABLE a (x INTEGER)",))
        m3 = Migration(3, "c", sqlite=("CREATE TABLE c (x INTEGER)",))
        with self.assertRaises(MigrationError) as cm:
            run_migrations(self.backend, [m1, m3], namespace="test")
        self.assertIn("contiguous", str(cm.exception))

    def test_namespaces_are_tracked_independently(self):
        run_migrations(self.backend, [Migration(1, "a", sqlite=("CREATE TABLE a (x INTEGER)",))], namespace="one")
        run_migrations(self.backend, [Migration(1, "b", sqlite=("CREATE TABLE b (x INTEGER)",))], namespace="two")
        got = {(r["namespace"], r["version"]) for r in rows(self.path, "SELECT namespace, version FROM schema_migrations")}
        self.assertEqual(got, {("one", 1), ("two", 1)})

    def test_shipped_platform_migrations_are_frozen(self):
        """If this fails you edited a migration that has shipped. Don't:
        every database that already applied it would refuse to start.
        Add a NEW migration instead. (Only update these pins if the
        migration has genuinely never been released to anyone.)"""
        pinned = {
            1: ("ae7098e1d621fa55d55d2410458ddf3cacfdee90ab1f0b59cba55b75ea772086",
                "c9554f88fa70e4d8f176ef909debde8fbd12cbc9cbeea142f88594de0fffb92f"),
            2: ("c94d819a18134935dd4a324d0c98d1146d7ff08bafab0cb25323ce708f36caca",
                "c94d819a18134935dd4a324d0c98d1146d7ff08bafab0cb25323ce708f36caca"),
            3: ("ba1a023db681f743887cc141f3b6f74754031e9cc25c90d7eeb8cf16064a636c",
                "ba1a023db681f743887cc141f3b6f74754031e9cc25c90d7eeb8cf16064a636c"),
            4: ("83913625615ca64c596b8395747856cc3bad9b5147d9205eb8ae79ef04e1b4bd",
                "e10a2d1d4084907d7b45f994daa021771b498c6b84c296215cbd2acb35fbf8f9"),
            5: ("c13fee26ccda817c983ce415ae01c123120423280ebcc3aa85aea1f3afedee0c",
                "c13fee26ccda817c983ce415ae01c123120423280ebcc3aa85aea1f3afedee0c"),
            6: ("2ed44c9e03ac1bdad5ed8b63b146744675063a49517d41d05099571f3827db85",
                "2ed44c9e03ac1bdad5ed8b63b146744675063a49517d41d05099571f3827db85"),
            7: ("5f1457717091da4a58ec529c26c09b8d0a1710ea762f38c6b40db67ba24e54d0",
                "4939c878fe2f266d8fcb88f48046390ef0268fd07b2baa36074c546ce5af4b75"),
            13: ("09ba79bbd2b1b880677af9bc7b7403dca2ab9fe301d988616ff00a5f30b6d9a5",
                 "09ba79bbd2b1b880677af9bc7b7403dca2ab9fe301d988616ff00a5f30b6d9a5"),
            12: ("cccb487f2a1bca07d3f1d8cd5faffbe185f8edfe108568c03efa71eb214e329b",
                 "aaa336ac9c5c4876fd8f848160635c70afc36689d04299301f9123aca84f169c"),
            11: ("4c304eebc4408e086fd5b24bc997b027f5e0d14a6ac93d92787c1588ced4d594",
                 "4c304eebc4408e086fd5b24bc997b027f5e0d14a6ac93d92787c1588ced4d594"),
            10: ("946e29f25a658242973eac8a7826fdb39262344f7987fdd5801241dd1c377023",
                 "340323a04fc9f808e4c6d6ead23a24ba8320c6614eace548d0cc46e2bfeba2b2"),
            9: ("383c5c5c501436e4e13f0cbb986c128f4095226298b2c7cd41eca877f96f3eca",
                "383c5c5c501436e4e13f0cbb986c128f4095226298b2c7cd41eca877f96f3eca"),
            8: ("767e8d329cb06f2ade3374f6d7584c8a440ba54b7b1c83f87154142c43295400",
                "21408dc5d1d8c9456de461d5a639a3573ff227886ca06cacd2de65d5ad954e73"),
                    14: ("2678e25c509949b72ae027e0c90c8276a6fc51ce33e0fc57beef23b6d347be7b",
                 "2678e25c509949b72ae027e0c90c8276a6fc51ce33e0fc57beef23b6d347be7b"),
            15: ("dd386f4b70c95f4a636a09111e8631199385e6f48e723e0ae55d16fcbed641eb",
                 "dd386f4b70c95f4a636a09111e8631199385e6f48e723e0ae55d16fcbed641eb"),
            16: ("4a6119737568721f6832eebfd2f844a91013dcd5d3b5e0d78fcc54a56337be8c",
                 "4a6119737568721f6832eebfd2f844a91013dcd5d3b5e0d78fcc54a56337be8c"),
        }
        self.assertEqual(set(pinned), {m.version for m in MIGRATIONS},
                         "A migration was added or removed: pin the new one's checksums here.")
        for m in MIGRATIONS:
            self.assertEqual((m.checksum("sqlite"), m.checksum("postgres")), pinned[m.version],
                             f"Migration {m.version} ({m.name}) changed after being pinned.")


# ----------------------------------------------------------- check-only mode
class TestAutoMigrateSwitch(unittest.TestCase):
    def test_check_only_mode_refuses_an_unmigrated_database_and_changes_nothing(self):
        path = tmp_db(self)
        with self.assertRaises(MigrationError) as cm:
            Store(path, auto_migrate=False)
        self.assertIn("migrate.py upgrade", str(cm.exception))
        self.assertEqual(table_names(path), set())

    def test_check_only_mode_accepts_a_current_database(self):
        path = tmp_db(self)
        Store(path)  # migrates
        Store(path, auto_migrate=False)  # verifies only; must not raise

    def test_environment_variable_controls_the_default(self):
        path = tmp_db(self)
        with patch.dict(os.environ, {"MDM_AUTO_MIGRATE": "false"}):
            with self.assertRaises(MigrationError):
                Store(path)

    def test_status_is_read_only(self):
        path = tmp_db(self)
        status = get_status(SQLiteMigrationBackend(path))
        self.assertEqual(len(status.pending), len(MIGRATIONS))
        self.assertEqual(table_names(path), set())  # looking must not create schema_migrations
        run_migrations(SQLiteMigrationBackend(path))
        self.assertTrue(get_status(SQLiteMigrationBackend(path)).ok)


# -------------------------------------------------------------- concurrency
class TestConcurrentStartup(unittest.TestCase):
    def test_parallel_startups_apply_each_migration_exactly_once(self):
        """A race is probabilistic: one round of threads can get lucky and
        pass even with the locking removed (measured: a single 4-thread
        round caught that regression only ~40% of the time). So repeat the
        race on several fresh databases to make a lucky pass implausible."""
        for attempt in range(6):
            path = tmp_db(self)
            errors, barrier = [], threading.Barrier(6)

            def start():
                try:
                    barrier.wait()
                    Store(path)
                except Exception as e:  # noqa: BLE001 -- we want to see any failure
                    errors.append(e)

            threads = [threading.Thread(target=start) for _ in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [], f"round {attempt}: a concurrent startup failed")
            self.assertEqual(
                [r["version"] for r in rows(path, "SELECT version FROM schema_migrations ORDER BY version")],
                [m.version for m in MIGRATIONS], f"round {attempt}")


# ----------------------------------------- Postgres control flow (mocked)
class _PgState:
    def __init__(self):
        self.log, self.applied, self.tables = [], [], set()


class _PgCursor:
    def __init__(self, st):
        self.st, self._rows = st, []

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.st.log.append(("sql", flat, params))
        low, self._rows = flat.lower(), []
        if low.startswith("create table if not exists"):
            self.st.tables.add(flat.split()[5])
        elif "from information_schema.tables" in low:
            self._rows = [{"present": 1}] if params and params[0] in self.st.tables else []
        elif low.startswith("select version, name, checksum, applied_at from schema_migrations"):
            self._rows = [dict(version=v, name=n, checksum=c, applied_at=a)
                          for (ns, v, n, c, a) in self.st.applied if ns == params[0]]
        elif low.startswith("insert into schema_migrations"):
            self.st.applied.append(params)

    def executemany(self, sql, rows_):
        self.st.log.append(("sql", " ".join(sql.split()), list(rows_)))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PgConn:
    def __init__(self, st):
        self.st = st

    def cursor(self):
        return _PgCursor(self.st)

    def commit(self):
        self.st.log.append(("commit",))

    def rollback(self):
        self.st.log.append(("rollback",))

    def close(self):
        self.st.log.append(("close",))


def _fake_psycopg2(state):
    mod = types.ModuleType("psycopg2")
    mod.connect = lambda *a, **k: _PgConn(state)
    extras = types.ModuleType("psycopg2.extras")
    extras.RealDictCursor = object()
    mod.extras = extras
    return {"psycopg2": mod, "psycopg2.extras": extras}


class TestPostgresControlFlow(unittest.TestCase):
    """Drives the REAL runner against a fake psycopg2. Verifies ordering,
    locking, transaction boundaries and placeholder translation. It does
    NOT execute SQL on a live Postgres -- that remains unverified until
    you run migrate.py against a real instance."""

    def test_locks_then_one_transaction_per_migration_then_unlocks(self):
        st = _PgState()
        with patch.dict(sys.modules, _fake_psycopg2(st)):
            applied = run_migrations(PostgresMigrationBackend("postgresql://fake/fake"))
        self.assertEqual(applied, [m.version for m in MIGRATIONS])

        sql = [(i, e[1]) for i, e in enumerate(st.log) if e[0] == "sql"]
        lock_i = next(i for i, s in sql if "pg_advisory_lock" in s)
        unlock_i = next(i for i, s in sql if "pg_advisory_unlock" in s)
        first_ddl = next(i for i, s in sql if s.lower().startswith("create table if not exists schema_migrations"))
        inserts = [i for i, s in sql if s.lower().startswith("insert into schema_migrations")]

        self.assertEqual(len(inserts), len(MIGRATIONS))
        self.assertLess(lock_i, first_ddl)          # lock taken before any change
        self.assertLess(inserts[-1], unlock_i)      # released only after the last migration
        for a, b in zip(inserts, inserts[1:]):      # each migration commits on its own
            self.assertTrue(any(e[0] == "commit" for e in st.log[a + 1:b]))

        insert_sql = st.log[inserts[0]][1]
        self.assertIn("%s", insert_sql)             # psycopg2 placeholders, not SQLite's ?
        self.assertNotIn("?", insert_sql)

        everything = " ".join(s for _, s in sql)
        self.assertIn("DOUBLE PRECISION", everything)                 # Postgres' own statements,
        self.assertIn("ALTER COLUMN attributes TYPE JSONB", everything)  # including the JSONB conversion
        self.assertNotIn(" REAL ", everything)

    def test_already_current_database_takes_no_lock_and_runs_no_ddl(self):
        st = _PgState()
        backend = PostgresMigrationBackend("postgresql://fake/fake")
        with patch.dict(sys.modules, _fake_psycopg2(st)):
            run_migrations(backend)
            st.log.clear()
            self.assertEqual(run_migrations(backend), [])
        text = " ".join(e[1] for e in st.log if e[0] == "sql").lower()
        self.assertIn("schema_migrations", text)  # it did look; it just changed nothing
        self.assertNotIn("pg_advisory_lock", text)
        self.assertNotIn("create table", text)
        self.assertNotIn("alter table", text)

    def test_check_only_mode_refuses_when_migrations_are_pending(self):
        st = _PgState()
        with patch.dict(sys.modules, _fake_psycopg2(st)):
            with self.assertRaises(MigrationError):
                apply_or_verify(PostgresMigrationBackend("postgresql://fake/fake"), auto_migrate=False)
        text = " ".join(e[1] for e in st.log if e[0] == "sql").lower()
        self.assertNotIn("create table", text)  # looked, didn't touch


if __name__ == "__main__":
    unittest.main()
