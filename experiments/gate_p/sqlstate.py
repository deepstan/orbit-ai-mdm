"""
SqlState: everything the matching kernel remembers, held in a database, behind the same methods as MemoryState (experiments/kernel_v2.py). The kernel is ONE algorithm; this is what it
costs when its state lives in SQL instead of dictionaries.

TABLES (all keyed by tenant_id, created by ensure_schema, prefixed gp_ so they cannot be mistaken for the product's tables):
    gp_groups    one row per group: members, golden, std, features, blocking keys, identifier holdings (JSON text), and a version
    gp_records   one row per record: its group, raw attributes, standardized and cleansed forms
    gp_keys      (blocking key, group): the blocking index
    gp_holders   (identifier field, value, group): which groups hold an identifier value
    gp_events    the outbox
    gp_links, gp_alias, gp_meta   small: relationships, merged-away group ids, counters

TWO MODES, to measure what each costs:
    cache=False, batch=1    NAIVE: nothing is remembered between arrivals; every read goes to the database; the writes of each arrival are one transaction.
    cache=True,  batch=N    HOT: groups, records, blocking-key buckets and holder sets are cached (LRU, bounded); the writes of N arrivals are held as PENDING (and are visible to every
                            read, so the answers do not change) and flushed in ONE transaction with multi-row statements.

CORRECTNESS RULE: the cache size and the batch size may change SPEED and nothing else. experiments/gate_p/test_gate_p.py asserts that the groups, the whole event stream, the links
and the reviews are identical to the in-memory kernel's, for naive, cached and batched runs, with tiny caches that force eviction.

KNOWN SIMPLIFICATIONS (this is a measurement, not the product): one writer per tenant (no concurrency control yet: the version column is where optimistic commits will go); aliases and
links are loaded fully into memory (small); a hub blocking key is detected the first time its bucket reaches the cap and remembered for `hub_ttl` arrivals; holder sets are read with a
LIMIT; a restart on an existing tenant is not supported (a run always starts fresh).
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import time
from collections import Counter, OrderedDict, defaultdict
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))

from kernel_v2 import Group  # noqa: E402

from app.models import SourceRecord  # noqa: E402

TABLES = ("gp_groups", "gp_records", "gp_keys", "gp_holders", "gp_events", "gp_links", "gp_alias", "gp_meta")

GROUP_COLS = ["tenant_id", "group_id", "version", "members", "golden", "std", "features", "keys", "held"]
RECORD_COLS = ["tenant_id", "record_id", "group_id", "attributes", "std", "cleansed", "ingested_at", "updated_at"]


def ensure_schema(db) -> None:
    d = db.dialect
    ddl = [
        "CREATE TABLE IF NOT EXISTS gp_groups (tenant_id TEXT NOT NULL, group_id TEXT NOT NULL, version BIGINT NOT NULL, members TEXT NOT NULL, golden TEXT, std TEXT, features TEXT, keys TEXT, held TEXT, "
        "PRIMARY KEY (tenant_id, group_id))",
        "CREATE TABLE IF NOT EXISTS gp_records (tenant_id TEXT NOT NULL, record_id TEXT NOT NULL, group_id TEXT, attributes TEXT, std TEXT, cleansed TEXT, ingested_at TEXT, updated_at TEXT, "
        "PRIMARY KEY (tenant_id, record_id))",
        "CREATE TABLE IF NOT EXISTS gp_keys (tenant_id TEXT NOT NULL, bkey TEXT NOT NULL, group_id TEXT NOT NULL, PRIMARY KEY (tenant_id, bkey, group_id))",
        "CREATE TABLE IF NOT EXISTS gp_holders (tenant_id TEXT NOT NULL, fname TEXT NOT NULL, fvalue TEXT NOT NULL, group_id TEXT NOT NULL, PRIMARY KEY (tenant_id, fname, fvalue, group_id))",
        "CREATE TABLE IF NOT EXISTS gp_events (tenant_id TEXT NOT NULL, seq BIGINT NOT NULL, etype TEXT NOT NULL, payload TEXT, PRIMARY KEY (tenant_id, seq))",
        "CREATE TABLE IF NOT EXISTS gp_links (tenant_id TEXT NOT NULL, a TEXT NOT NULL, b TEXT NOT NULL, kind TEXT NOT NULL, PRIMARY KEY (tenant_id, a, b, kind))",
        "CREATE TABLE IF NOT EXISTS gp_alias (tenant_id TEXT NOT NULL, gone TEXT NOT NULL, keep TEXT NOT NULL, PRIMARY KEY (tenant_id, gone))",
        "CREATE TABLE IF NOT EXISTS gp_meta (tenant_id TEXT NOT NULL, name TEXT NOT NULL, value BIGINT NOT NULL, PRIMARY KEY (tenant_id, name))",
    ]
    with db.transaction():
        for sql in ddl:
            db.execute(sql)


def drop_schema(db) -> None:
    with db.transaction():
        for t in TABLES:
            db.execute(f"DROP TABLE IF EXISTS {t}")


# ---------------------------------------------------------------------------------------------- JSON that survives the types a golden record can hold
def _enc(o):
    if isinstance(o, Decimal):
        return {"__t": "dec", "v": str(o)}
    if isinstance(o, _dt.datetime):
        return {"__t": "dt", "v": o.isoformat()}
    if isinstance(o, _dt.date):
        return {"__t": "date", "v": o.isoformat()}
    raise TypeError(f"cannot store {type(o).__name__} in a group row")


def _dec(d):
    t = d.get("__t")
    if t == "dec":
        return Decimal(d["v"])
    if t == "dt":
        return _dt.datetime.fromisoformat(d["v"])
    if t == "date":
        return _dt.date.fromisoformat(d["v"])
    return d


def dumps(o) -> str:
    return json.dumps(o, default=_enc, separators=(",", ":"))


def loads(s):
    return None if s is None else json.loads(s, object_hook=_dec)


class _Rec:
    __slots__ = ("sr", "std", "cleansed", "gid")

    def __init__(self, sr, std, cleansed, gid=None):
        self.sr, self.std, self.cleansed, self.gid = sr, std, cleansed, gid


class _HubBucket:
    """A blocking key held by more groups than the cap: the kernel only needs to know that, never who."""
    __slots__ = ("n",)

    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n

    def __bool__(self):
        return True


class _HolderView:
    def __init__(self, state):
        self._s = state

    def get(self, key, default=None):
        return self._s._holders_of(key)


class SqlState:
    def __init__(self, db, tenant: str, domain: str = "customer", *, cache: bool = True, batch: int = 1000, cache_groups: int = 100_000, cache_buckets: int = 400_000,
                 cache_records: int = 400_000, cache_holders: int = 100_000, hub_ttl: int = 5000, holders_limit: int = 100, fresh: bool = True):
        if batch > 1 and not cache:
            raise ValueError("batch > 1 needs cache=True: unflushed writes must be visible to later reads")
        self.db, self.d, self.tenant, self.domain = db, db.dialect, tenant, domain
        self.cache, self.batch = cache, batch
        self.cap = dict(groups=cache_groups, buckets=cache_buckets, records=cache_records, holders=cache_holders)
        self.hub_ttl, self.holders_limit = hub_ttl, holders_limit
        self.groups: OrderedDict = OrderedDict()
        self.recs: OrderedDict = OrderedDict()
        self.buckets: OrderedDict = OrderedDict()
        self.holders: OrderedDict = OrderedDict()
        self.hubs: dict = {}
        self.alias: dict = {}
        self.links: set = set()
        self.arrivals = 0
        self.pending_arrivals = 0
        self.limit = 1 << 30                                        # the bucket limit the kernel last asked for (its cap + 1)
        self.counters = {"next_group": 0, "clock": 0, "event_seq": 0}
        self.stats = defaultdict(int)
        self.flush_times: list = []
        self._clear_pending()
        if fresh:
            self._delete_tenant()

    # ------------------------------------------------------------------ pending writes
    def _clear_pending(self):
        self.p_groups: dict = {}                                    # group id -> Group to upsert
        self.p_group_del: set = set()
        self.p_recs: dict = {}                                      # record id -> _Rec to upsert
        self.p_rec_group: dict = {}                                 # record id -> new group id (when only the group changed)
        self.p_keys: dict = {}                                      # (key, group id) -> True (add) / False (remove): the net effect
        self.p_keys_by_key: dict = defaultdict(dict)
        self.p_holders: dict = {}
        self.p_holders_by_key: dict = defaultdict(dict)
        self.p_events: list = []
        self.p_links: list = []
        self.p_alias: list = []

    def _delete_tenant(self):
        with self.db.transaction():
            for t in TABLES:
                self.db.execute(self.d.sql(f"DELETE FROM {t} WHERE tenant_id = ?"), [self.tenant])

    # ------------------------------------------------------------------ small helpers
    def _query_in(self, select_sql: str, column: str, values, lead_params):
        out = []
        for chunk in self.d.chunks(values):
            in_sql, in_params = self.d.in_list(column, chunk)
            out.extend(self.db.query(self.d.sql(select_sql) + " AND " + in_sql, list(lead_params) + in_params))
        return out

    @staticmethod
    def _touch(od: OrderedDict, key):
        od.move_to_end(key)

    # ------------------------------------------------------------------ records
    def _rec_from_row(self, r):
        attrs = loads(r[3])
        sr = SourceRecord(source_system="s", source_record_id=r[1], attributes=attrs, id=r[1], tenant_id="t", domain=self.domain, ingested_at=r[6], updated_at=r[7])
        return _Rec(sr, loads(r[4]), loads(r[5]), r[2])

    def _load_records(self, rids):
        need = [r for r in rids if r not in self.recs]
        if need:
            self.stats["record_loads"] += len(need)
            sel = "SELECT " + ", ".join(RECORD_COLS) + " FROM gp_records WHERE tenant_id = ?"
            for row in self._query_in(sel, "record_id", need, [self.tenant]):
                rec = self._rec_from_row(row)
                if rec.sr.id in self.p_rec_group:                           # a group change not yet flushed wins over what the database still says
                    rec.gid = self.p_rec_group[rec.sr.id]
                self.recs[row[1]] = rec
        else:
            self.stats["record_hits"] += len(rids)

    def get_record(self, rid):
        if rid not in self.recs:
            self._load_records([rid])
        rec = self.recs.get(rid)
        return rec.sr if rec else None

    def put_record(self, rid, sr, std, cleansed):
        old = self.recs.get(rid)
        rec = _Rec(sr, std, cleansed, old.gid if old else None)
        self.recs[rid] = rec
        self.p_recs[rid] = rec

    def records_data(self, rids):
        self._load_records(rids)
        out = []
        for r in rids:
            rec = self.recs[r]
            out.append((rec.sr, rec.std, rec.cleansed))
        return out

    def record_group(self, rid):
        if rid not in self.recs:
            self._load_records([rid])
        return self.recs[rid].gid

    def set_record_group(self, rid, gid):
        rec = self.recs.get(rid)
        if rec is not None:
            rec.gid = gid
        if rid in self.p_recs:
            self.p_recs[rid].gid = gid
        else:
            self.p_rec_group[rid] = gid                                 # only the group changed: an UPDATE of one column, the record body is not loaded or rewritten

    # ------------------------------------------------------------------ groups
    def _group_from_row(self, r):
        g = Group(r[1])
        g.version = r[2]
        g.members = loads(r[3])
        g.golden = loads(r[4]) or {}
        g.std = loads(r[5]) or {}
        g.features = defaultdict(Counter, {f: Counter(vals) for f, vals in (loads(r[6]) or {}).items()})
        g.keys = set(loads(r[7]) or [])
        g.held = {tuple(h) for h in (loads(r[8]) or [])}
        return g

    def _group_row(self, g):
        return [self.tenant, g.id, g.version, dumps(g.members), dumps(g.golden), dumps(g.std), dumps({f: dict(c) for f, c in g.features.items()}), dumps(sorted(g.keys)),
                dumps(sorted([list(h) for h in g.held]))]

    def prefetch_groups(self, gids):
        need = [g for g in dict.fromkeys(gids) if g not in self.groups and g not in self.p_group_del]
        if not need:
            return
        self.stats["group_loads"] += len(need)
        sel = "SELECT " + ", ".join(GROUP_COLS) + " FROM gp_groups WHERE tenant_id = ?"
        for row in self._query_in(sel, "group_id", need, [self.tenant]):
            self.groups[row[1]] = self._group_from_row(row)

    def get_group(self, gid):
        g = self.groups.get(gid)
        if g is not None:
            self.stats["group_hits"] += 1
            self._touch(self.groups, gid)
            return g
        if gid in self.p_group_del:
            return None
        self.prefetch_groups([gid])
        return self.groups.get(gid)

    def add_group(self, g):
        self.groups[g.id] = g

    def save_group(self, g):
        g.version += 1
        self.groups[g.id] = g
        self.p_groups[g.id] = g
        self.p_group_del.discard(g.id)

    def remove_group(self, gid):
        self.groups.pop(gid, None)
        self.p_groups.pop(gid, None)
        self.p_group_del.add(gid)

    def new_group_id(self):
        self.counters["next_group"] += 1
        return f"g{self.counters['next_group']:06d}"

    def next_clock(self):
        self.counters["clock"] += 1
        return self.counters["clock"]

    # ------------------------------------------------------------------ the blocking index
    def _is_hub(self, key):
        t = self.hubs.get(key)
        if t is None:
            return False
        if self.arrivals - t < self.hub_ttl:
            return True
        del self.hubs[key]
        return False

    def _mark_hub(self, key):
        self.hubs[key] = self.arrivals
        self.buckets.pop(key, None)

    def prefetch_buckets(self, keys, limit):
        self.limit = limit
        need = [k for k in keys if k not in self.buckets and not self._is_hub(k)]
        self.stats["bucket_hits"] += len(keys) - len(need)
        if not need:
            return
        self.stats["bucket_loads"] += len(need)
        found: dict = defaultdict(set)
        for key, gid in self._query_in("SELECT bkey, group_id FROM gp_keys WHERE tenant_id = ?", "bkey", need, [self.tenant]):
            found[key].add(gid)
        for k in need:
            s = found.get(k, set())
            for gid, add in self.p_keys_by_key.get(k, {}).items():          # the writes not yet flushed are part of the bucket
                (s.add if add else s.discard)(gid)
            if len(s) >= limit:
                self._mark_hub(k)
            else:
                self.buckets[k] = s

    def bucket(self, key, limit):
        b = self.buckets.get(key)
        if b is not None:
            self._touch(self.buckets, key)
            return b
        if self._is_hub(key):
            return _HubBucket(limit)
        self.prefetch_buckets([key], limit)
        return self.buckets.get(key) or (_HubBucket(limit) if key in self.hubs else set())

    def _key_op(self, key, gid, add):
        self.p_keys[(key, gid)] = add
        self.p_keys_by_key[key][gid] = add
        b = self.buckets.get(key)
        if b is not None:
            (b.add if add else b.discard)(gid)
            if len(b) >= self.limit:
                self._mark_hub(key)

    def index_update(self, gid, old_keys, new_keys):
        for k in old_keys - new_keys:
            self._key_op(k, gid, False)
        for k in new_keys - old_keys:
            self._key_op(k, gid, True)

    # ------------------------------------------------------------------ identifier holders
    def _holders_of(self, key):
        s = self.holders.get(key)
        if s is not None:
            self._touch(self.holders, key)
            return s
        self.stats["holder_loads"] += 1
        rows = self.db.query(self.d.sql("SELECT group_id FROM gp_holders WHERE tenant_id = ? AND fname = ? AND fvalue = ? LIMIT ?"), [self.tenant, key[0], key[1], self.holders_limit])
        s = {r[0] for r in rows}
        for gid, add in self.p_holders_by_key.get(key, {}).items():
            (s.add if add else s.discard)(gid)
        self.holders[key] = s
        return s

    def holder_view(self):
        return _HolderView(self)

    def holders_update(self, gid, old_held, new_held):
        for key in old_held - new_held:
            self._holder_op(key, gid, False)
        for key in new_held - old_held:
            self._holder_op(key, gid, True)

    def _holder_op(self, key, gid, add):
        self.p_holders[(key, gid)] = add
        self.p_holders_by_key[key][gid] = add
        s = self.holders.get(key)
        if s is not None:
            (s.add if add else s.discard)(gid)

    # ------------------------------------------------------------------ aliases, links, the outbox
    def resolve(self, gid):
        while gid in self.alias:
            gid = self.alias[gid]
        return gid

    def set_alias(self, gone, keep):
        self.alias[gone] = keep
        self.p_alias.append((gone, keep))

    def add_link(self, a, b, kind):
        if (a, b, kind) in self.links:
            return False
        self.links.add((a, b, kind))
        self.p_links.append((a, b, kind))
        return True

    def link_keys(self):
        return list(self.links)

    def emit(self, event_type, **data):
        seq = self.counters["event_seq"]
        self.counters["event_seq"] += 1
        self.p_events.append((seq, event_type, dumps(data)))

    # ------------------------------------------------------------------ batching
    def end_arrival(self):
        self.arrivals += 1
        self.pending_arrivals += 1
        if self.pending_arrivals >= self.batch:
            self.flush()
        if not self.cache:                                          # naive: remember nothing between arrivals
            self.flush()
            self.groups.clear(); self.recs.clear(); self.buckets.clear(); self.holders.clear(); self.hubs.clear()

    def flush(self):
        """All pending writes, in ONE transaction, as multi-row statements."""
        has = self.p_groups or self.p_group_del or self.p_recs or self.p_rec_group or self.p_keys or self.p_holders or self.p_events or self.p_links or self.p_alias
        if not has and not self.pending_arrivals:
            return
        db, d, t = self.db, self.d, self.tenant
        started = time.perf_counter()
        with db.transaction():
            if self.p_group_del:
                for chunk in d.chunks(sorted(self.p_group_del)):
                    in_sql, in_params = d.in_list("group_id", chunk)
                    db.execute(d.sql("DELETE FROM gp_groups WHERE tenant_id = ?") + " AND " + in_sql, [t] + in_params)
            if self.p_recs:
                rows = [[t, rid, r.gid, dumps(r.sr.attributes), dumps(r.std), dumps(r.cleansed), r.sr.ingested_at, r.sr.updated_at] for rid, r in self.p_recs.items()]
                db.insert_many("gp_records", RECORD_COLS, rows, d.conflict_suffix(RECORD_COLS, ["tenant_id", "record_id"]))
            moved = [[gid, t, rid] for rid, gid in self.p_rec_group.items() if rid not in self.p_recs]
            if moved:
                db.executemany(d.sql("UPDATE gp_records SET group_id = ? WHERE tenant_id = ? AND record_id = ?"), moved)
            if self.p_groups:
                db.insert_many("gp_groups", GROUP_COLS, [self._group_row(g) for g in self.p_groups.values()], d.conflict_suffix(GROUP_COLS, ["tenant_id", "group_id"]))
            self._flush_pairs("gp_keys", ["tenant_id", "bkey", "group_id"], self.p_keys, lambda k: [k[0], k[1]], 300)
            self._flush_pairs("gp_holders", ["tenant_id", "fname", "fvalue", "group_id"], self.p_holders, lambda k: [k[0][0], k[0][1], k[1]], 200)
            if self.p_events:
                cols = ["tenant_id", "seq", "etype", "payload"]
                db.insert_many("gp_events", cols, [[t, s, ty, p] for s, ty, p in self.p_events], d.conflict_suffix(cols, ["tenant_id", "seq"]))
            if self.p_links:
                cols = ["tenant_id", "a", "b", "kind"]
                db.insert_many("gp_links", cols, [[t, a, b, k] for a, b, k in self.p_links], d.conflict_suffix(cols, ["tenant_id", "a", "b", "kind"], update=[]))
            if self.p_alias:
                cols = ["tenant_id", "gone", "keep"]
                db.insert_many("gp_alias", cols, [[t, g, k] for g, k in self.p_alias], d.conflict_suffix(cols, ["tenant_id", "gone"]))
            cols = ["tenant_id", "name", "value"]
            db.insert_many("gp_meta", cols, [[t, n, v] for n, v in self.counters.items()], d.conflict_suffix(cols, ["tenant_id", "name"]))
        self.stats["flushes"] += 1
        self.flush_times.append(time.perf_counter() - started)
        self._clear_pending()
        self.pending_arrivals = 0
        self._evict()

    def _flush_pairs(self, table, cols, ops, tail, chunk_size):
        """gp_keys / gp_holders: the net adds go in as one multi-row insert; the net removals as DELETE .. WHERE (composite key) IN (VALUES ..), a few hundred keys a statement."""
        if not ops:
            return
        t, d, db = self.tenant, self.d, self.db
        adds = [[t] + tail(k) for k, add in ops.items() if add]
        dels = [tail(k) for k, add in ops.items() if not add]
        if adds:
            db.insert_many(table, cols, adds, d.conflict_suffix(cols, cols[:], update=[]))
        for i in range(0, len(dels), chunk_size):
            part = dels[i:i + chunk_size]
            key_cols = cols[1:]
            db.execute(d.sql(f"DELETE FROM {table} WHERE tenant_id = ? AND ") + d.row_in(key_cols, len(part)), [t] + [v for row in part for v in row])

    def _evict(self):
        for od, cap in ((self.groups, self.cap["groups"]), (self.recs, self.cap["records"]), (self.buckets, self.cap["buckets"]), (self.holders, self.cap["holders"])):
            while len(od) > cap:
                od.popitem(last=False)

    # ------------------------------------------------------------------ what the world sees (read from the DATABASE, after a flush)
    def assignments(self):
        self.flush()
        return {r[0]: r[1] for r in self.db.query(self.d.sql("SELECT record_id, group_id FROM gp_records WHERE tenant_id = ?"), [self.tenant])}

    def all_events(self):
        self.flush()
        rows = self.db.query(self.d.sql("SELECT seq, etype, payload FROM gp_events WHERE tenant_id = ? ORDER BY seq"), [self.tenant])
        return [{"seq": s, "type": ty, **(loads(p) or {})} for s, ty, p in rows]
