"""
Gate P's correctness: the kernel on a DATABASE must give exactly the answers of the kernel in MEMORY, whatever the mode, cache size or batch size. Speed is what may differ.

  * assignments, the whole event stream, links, reviews and statistics are identical for: naive (no cache, a transaction per arrival), cached, hot batches of several sizes, caches so small
    that groups, records and buckets are evicted and re-read from the database all the time, hub keys, the anchored update policy, relationships and group merging;
  * writes that are not flushed yet are visible to reads, and invisible to the database until the flush;
  * a flush is one transaction: a failure in the middle leaves the database exactly as it was, and a retry succeeds;
  * the database ALONE tells the whole story: its index and holder tables are exactly what the in-memory kernel holds;
  * the PostgreSQL TEXT the state emits (through the dialect) has the right placeholders for its parameters, and the run gives the same answers when that text is executed (on SQLite, by a shim).

Run with:  python3 -m unittest experiments.gate_p.test_gate_p -v      (from the project root)
"""
import os
import random
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", ".."))
os.environ.setdefault("MDM_AUDIT_RATE", "0")

import hard_cases as hc  # noqa: E402
from dbs import ShimPgDb, SqliteDb  # noqa: E402
from kernel_v2 import Group, Kernel2, replay  # noqa: E402
from sqlstate import SqlState, dumps, ensure_schema, loads  # noqa: E402

from app.models import SourceRecord  # noqa: E402


def dataset(seed=1, scale=0.15):
    d = hc.build(seed, scale, life_events=True)
    raw = {r["rid"]: r["raw"] for r in d["rows"]}
    order = [r["rid"] for r in d["rows"]]
    random.Random(seed + 10).shuffle(order)
    return d, raw, order


def drive(kernel, d, raw, order):
    for rid in order:
        kernel.arrive(rid, raw[rid])
    for u in d["updates"]:
        kernel.update(u["rid"], u["raw"])
    return kernel


def snapshot(k):
    return {"assign": k.assign(), "events": k.events, "links": k.links(), "reviews": k.reviews, "stats": dict(k.stats)}


class DbCase(unittest.TestCase):
    shim = False

    def make_db(self):
        path = os.path.join(tempfile.mkdtemp(), "gate_p.db")
        db = (ShimPgDb if self.shim else SqliteDb)(path)
        self.addCleanup(db.close)
        ensure_schema(db)
        return db

    def compare(self, state_kwargs, kernel_kwargs=None, seed=1, scale=0.15):
        kernel_kwargs = kernel_kwargs or {}
        d, raw, order = dataset(seed, scale)
        mem = drive(Kernel2(**kernel_kwargs), d, raw, order)
        db = self.make_db()
        st = SqlState(db, f"tenant{seed}", **state_kwargs)
        sql = drive(Kernel2(state=st, **kernel_kwargs), d, raw, order)
        a, b = snapshot(mem), snapshot(sql)
        for part in ("assign", "events", "links", "reviews", "stats"):
            self.assertEqual(a[part], b[part], f"{part} differ between the in-memory kernel and the database-backed one ({state_kwargs})")
        self.assertGreater(len(a["assign"]), 100)
        return mem, sql, st, db


class TestSameAnswersInEveryMode(DbCase):
    def test_naive_no_cache_a_transaction_per_arrival(self):
        _, _, st, _ = self.compare(dict(cache=False, batch=1, hub_ttl=0))
        self.assertGreater(st.stats["group_loads"], 100, "naive mode must really read the database")

    def test_cached_with_a_flush_per_arrival(self):
        self.compare(dict(cache=True, batch=1, hub_ttl=0))

    def test_hot_batches_of_several_sizes_including_larger_than_the_whole_run(self):
        for batch in (7, 50, 100_000):
            self.compare(dict(cache=True, batch=batch, hub_ttl=0))

    def test_caches_so_small_that_everything_is_evicted_and_read_back(self):
        _, _, st, _ = self.compare(dict(cache=True, batch=25, cache_groups=4, cache_buckets=12, cache_records=6, cache_holders=3, hub_ttl=0))
        self.assertGreater(st.stats["group_loads"], 100)
        self.assertGreater(st.stats["bucket_loads"], 100)
        self.assertGreater(st.stats["record_loads"], 50)

    def test_hub_keys_and_ranking(self):
        mem, _, st, _ = self.compare(dict(cache=True, batch=30, hub_ttl=0), dict(cap=6, rank="idf", max_candidates=4))
        self.assertGreater(mem.stats["skipped_keys"], 50, "a cap of 6 must make hub keys, or this test proves nothing about them")
        self.assertGreater(st.stats["bucket_loads"], 100)

    def test_anchored_updates_relationships_and_group_merging(self):
        mem, sql, _, _ = self.compare(dict(cache=True, batch=40, hub_ttl=0), dict(update_policy="anchored", relationships=True, bridge=True, cap=30, rank="idf", max_candidates=8), seed=2, scale=0.2)
        self.assertTrue(mem.links("household"), "the data has households: the comparison must not be vacuous")
        self.assertGreater(mem.stats["groups_merged"] + mem.stats["kept_after_change"], 0)

    def test_a_longer_run_with_a_more_realistic_hub_ttl_gives_the_same_groups(self):
        d, raw, order = dataset(3, 0.3)
        mem = drive(Kernel2(cap=30), d, raw, order)
        db = self.make_db()
        sql = drive(Kernel2(cap=30, state=SqlState(db, "t3", cache=True, batch=100, hub_ttl=5000)), d, raw, order)
        self.assertEqual(mem.assign(), sql.assign())


class TestPendingWritesAndTheFlush(DbCase):
    def count(self, db, table, tenant):
        return db.query(f"SELECT COUNT(*) FROM {table} WHERE tenant_id = ?", [tenant])[0][0]

    def test_unflushed_writes_are_visible_to_reads_and_invisible_to_the_database(self):
        d, raw, order = dataset()
        db = self.make_db()
        st = SqlState(db, "tp", cache=True, batch=10_000)
        k = Kernel2(state=st)
        for rid in order[:40]:
            k.arrive(rid, raw[rid])
        self.assertEqual((self.count(db, "gp_groups", "tp"), self.count(db, "gp_records", "tp"), self.count(db, "gp_keys", "tp")), (0, 0, 0))     # nothing has reached the database
        self.assertEqual(st.stats["flushes"], 0)
        self.assertTrue(all(k.state.get_group(g) is not None for g in {st.record_group(r) for r in order[:40]}))                                       # but every read sees it
        k.arrive(order[40], raw[order[40]])
        st.flush()
        self.assertEqual(self.count(db, "gp_records", "tp"), 41)
        self.assertGreater(self.count(db, "gp_keys", "tp"), 100)

    def test_a_flush_is_one_transaction_and_batching_cuts_the_statements_a_great_deal(self):
        d, raw, order = dataset()
        counts = {}
        for name, kw in (("naive", dict(cache=False, batch=1)), ("hot", dict(cache=True, batch=50))):
            db = self.make_db()
            st = SqlState(db, "tx", hub_ttl=0, **kw)
            before = db.statements
            drive(Kernel2(state=st), d, raw, order)
            st.flush()
            counts[name] = (db.statements - before, st.stats["flushes"])
        self.assertLess(counts["hot"][0] * 4, counts["naive"][0], counts)
        self.assertGreater(counts["hot"][1], 1)
        self.assertLess(counts["hot"][1], 20)

    def test_a_failure_in_the_middle_of_a_flush_leaves_the_database_as_it_was_and_a_retry_succeeds(self):
        d, raw, order = dataset()
        db = self.make_db()
        st = SqlState(db, "rb", cache=True, batch=10_000, hub_ttl=0)
        k = Kernel2(state=st)
        for rid in order[:60]:
            k.arrive(rid, raw[rid])
        real = db.insert_many

        def boom(table, *a, **kw):
            if table == "gp_keys":
                raise RuntimeError("the connection dropped in the middle of the flush")
            return real(table, *a, **kw)
        db.insert_many = boom
        with self.assertRaises(RuntimeError):
            st.flush()
        db.insert_many = real
        self.assertEqual([self.count(db, t, "rb") for t in ("gp_groups", "gp_records", "gp_keys", "gp_events", "gp_meta")], [0, 0, 0, 0, 0])      # records and groups were written before the failure: rolled back
        for rid in order[60:]:
            k.arrive(rid, raw[rid])
        for u in d["updates"]:
            k.update(u["rid"], u["raw"])
        mem = drive(Kernel2(), d, raw, order)
        self.assertEqual(k.assign(), mem.assign())
        self.assertEqual(k.events, mem.events)

    def test_the_database_alone_holds_exactly_what_the_in_memory_kernel_holds(self):
        mem, sql, st, db = self.compare(dict(cache=True, batch=33, cache_groups=8, cache_buckets=30, hub_ttl=0), dict(cap=30, rank="idf", max_candidates=8))
        st.flush()
        t = "tenant1"
        in_db_keys = {(r[0], r[1]) for r in db.query("SELECT bkey, group_id FROM gp_keys WHERE tenant_id = ?", [t])}
        in_mem_keys = {(k, g) for k, gs in mem.state.index.items() for g in gs}
        self.assertEqual(in_db_keys, in_mem_keys)
        in_db_holders = {((r[0], r[1]), r[2]) for r in db.query("SELECT fname, fvalue, group_id FROM gp_holders WHERE tenant_id = ?", [t])}
        self.assertEqual(in_db_holders, {(k, g) for k, gs in mem.state.holders.items() for g in gs})
        self.assertEqual({r[0] for r in db.query("SELECT group_id FROM gp_groups WHERE tenant_id = ?", [t])}, set(mem.state.groups))
        self.assertEqual(self.count(db, "gp_records", t), len(mem.state.records))
        assign, links = replay(sql.events)
        self.assertEqual(assign, mem.assign())
        self.assertEqual(links, mem.links())
        self.assertGreater(len(in_db_keys), 300)

    def test_a_new_record_never_asks_the_database_for_an_earlier_version(self):
        d, raw, order = dataset()
        db = self.make_db()
        st = SqlState(db, "nv", cache=True, batch=10_000, hub_ttl=0)
        k = Kernel2(state=st)
        before = db.statements
        for rid in order:
            k.arrive(rid, raw[rid])
        self.assertEqual(st.stats["record_loads"], 0)                                 # no record was read at all: nothing had an earlier version, and every member was still pending
        self.assertGreater(db.statements - before, 0)                                  # (the bucket and holder reads, and nothing else)


def a_record(rid):
    sr = SourceRecord(source_system="s", source_record_id=rid, attributes={"a": "b"}, id=rid, tenant_id="t", domain="customer", ingested_at="2026-01-01T00:00:00.000Z", updated_at=None)
    return sr, {"first_name": rid}, {"first_name": rid}


class TestTheMechanicsOfTheCache(DbCase):
    """The things a hot cache with pending writes can get wrong without the end-to-end answers noticing, each tested directly."""

    def state(self, **kw):
        db = self.make_db()
        return SqlState(db, "u", cache=True, batch=10_000, hub_ttl=0, **kw), db

    def test_a_pending_group_change_for_a_record_that_is_not_cached_is_what_a_read_returns(self):
        st, db = self.state()
        st.put_record("r1", *a_record("r1"))
        st.set_record_group("r1", "g1")
        st.flush()
        st.recs.clear()                                                         # r1 is now only in the database, with group g1
        st.set_record_group("r1", "g2")                                         # a merge moves it: pending, and r1's body is not loaded
        self.assertEqual(st.record_group("r1"), "g2")                           # a read must not return what the database still says
        st.flush()
        self.assertEqual(db.query("SELECT group_id FROM gp_records WHERE tenant_id = 'u' AND record_id = 'r1'"), [("g2",)])

    def test_a_holder_set_that_is_already_cached_sees_later_changes_and_so_does_the_database(self):
        st, db = self.state()
        key = ("phone", "4045550100")
        self.assertEqual(st._holders_of(key), set())                            # cached, empty
        st.holders_update("g1", set(), {key})
        st.holders_update("g2", set(), {key})
        self.assertEqual(st._holders_of(key), {"g1", "g2"})
        st.holders_update("g1", {key}, set())
        self.assertEqual(st._holders_of(key), {"g2"})
        st.flush()
        self.assertEqual(db.query("SELECT group_id FROM gp_holders WHERE tenant_id = 'u' AND fname = 'phone'"), [("g2",)])
        st.holders.clear()
        self.assertEqual(st._holders_of(key), {"g2"})                           # and read back from the database

    def test_a_removed_group_is_gone_for_readers_at_once_and_from_the_database_after_the_flush(self):
        st, db = self.state()
        for gid in ("g1", "g2"):
            g = Group(gid); g.members = []; st.add_group(g); st.save_group(g)
        st.flush()
        st.remove_group("g1")
        self.assertIsNone(st.get_group("g1"))                                   # not the cached object, not the database's row (still there until the flush)
        self.assertIsNotNone(st.get_group("g2"))
        g3 = Group("g3"); st.add_group(g3); st.save_group(g3)
        st.remove_group("g3")                                                   # a group that never reached the database at all
        self.assertIsNone(st.get_group("g3"))
        st.flush()
        self.assertEqual(sorted(r[0] for r in db.query("SELECT group_id FROM gp_groups WHERE tenant_id = 'u'")), ["g2"])

    def test_the_cache_drops_the_least_recently_used_group_first(self):
        st, _ = self.state(cache_groups=2)
        for gid in ("g1", "g2", "g3"):
            g = Group(gid); g.members = []; st.add_group(g); st.save_group(g)
        st.get_group("g1")                                                      # g1 is now the most recently used; g2 the least
        st.flush()
        self.assertEqual(set(st.groups), {"g3", "g1"})

    def test_an_evicted_group_is_read_back_unchanged(self):
        st, _ = self.state(cache_groups=1)
        g = Group("g1"); g.members = ["r1", "r2"]; g.golden = {"first_name": "ann", "n": Decimal("1.5")}; g.std = {"first_name": "ann"}
        g.keys = {"k1", "k2"}; g.held = {("email", "a@b.c")}
        g.features = defaultdict(Counter)
        g.features["first_name"]["ann"] += 2
        st.add_group(g); st.save_group(g)
        other = Group("g2"); other.members = []; st.add_group(other); st.save_group(other)
        st.flush()
        self.assertNotIn("g1", st.groups)
        back = st.get_group("g1")
        self.assertEqual((back.members, back.golden, back.std, back.keys, back.held, back.version), (["r1", "r2"], g.golden, g.std, {"k1", "k2"}, {("email", "a@b.c")}, 1))
        self.assertEqual(dict(back.features["first_name"]), {"ann": 2})

    def test_the_counters_are_persisted(self):
        d, raw, order = dataset()
        db = self.make_db()
        st = SqlState(db, "ct", cache=True, batch=40, hub_ttl=0)
        k = drive(Kernel2(state=st), d, raw, order)
        st.flush()
        stored = dict(db.query("SELECT name, value FROM gp_meta WHERE tenant_id = 'ct'"))
        self.assertEqual(stored, st.counters)
        self.assertEqual(stored["event_seq"], len(k.events))
        self.assertEqual(stored["next_group"], sum(1 for e in k.events if e["type"] == "group_created"))       # one id issued per group ever created, merged away or not
        self.assertEqual(stored["clock"], len(order) + len(d["updates"]))                                        # one tick per arrival, an update counts as one
        self.assertGreater(stored["next_group"], 50)


class TestTheCodecAndTheText(DbCase):
    def test_typed_values_survive_the_round_trip(self):
        value = {"a": Decimal("12.50"), "b": date(2026, 1, 2), "c": datetime(2026, 1, 2, 3, 4, 5), "d": [1, "x", None], "e": {"f": Decimal("0.1")}}
        self.assertEqual(loads(dumps(value)), value)
        self.assertIsNone(loads(None))
        with self.assertRaises(TypeError):
            dumps({"x": object()})


class TestThePostgresqlText(DbCase):
    shim = True

    def test_the_run_gives_the_same_answers_when_the_postgresql_text_is_what_is_executed(self):
        mem, sql, st, db = self.compare(dict(cache=True, batch=30, cache_groups=6, cache_buckets=20, cache_records=10, hub_ttl=0), dict(cap=30, rank="idf", max_candidates=8))
        self.assertGreater(len(db.log), 100)

    def test_every_statement_has_exactly_the_placeholders_its_parameters_need_and_none_of_sqlites(self):
        d, raw, order = dataset()
        db = self.make_db()
        drive(Kernel2(state=SqlState(db, "pg", cache=True, batch=40, cache_groups=6, cache_buckets=20, hub_ttl=0)), d, raw, order)
        texts = [sql for sql, _ in db.log]
        for sql, params in db.log:
            self.assertNotIn("?", sql, sql)
            if "VALUES %s" in sql:                                                   # a multi-row insert: execute_values expands the one placeholder
                self.assertEqual(sql.count("%s"), 1, sql)
                continue
            self.assertEqual(sql.count("%s"), len(params), sql)
        joined = "\n".join(texts)
        for fragment in ("= ANY(%s)", "ON CONFLICT (tenant_id, record_id) DO UPDATE SET", "ON CONFLICT (tenant_id, bkey, group_id) DO NOTHING", "IN (VALUES (%s, %s)", "LIMIT %s", "BEGIN", "COMMIT"):
            self.assertIn(fragment, joined, f"the PostgreSQL text never used: {fragment}")
        for sql, params in db.log:
            if "= ANY(%s)" in sql:
                self.assertTrue(any(isinstance(p, list) for p in params), "an ANY lookup must be given a list")


if __name__ == "__main__":
    unittest.main()
