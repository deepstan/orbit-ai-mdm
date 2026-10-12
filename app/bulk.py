"""
The bulk pair-based engine: load a whole source into an EMPTY tenant+domain in stages, instead of one record at a time.

WHY. The one-record-at-a-time engine spends 83% of its time on the database (about 15 commits per record) and under 4% on matching.
Matching is a function of PAIRS (see app/offline.py): whether two records link depends only on those two records, so the same
answer can be computed set-wise:

    A  standardize     every record, in parallel: matching form, cleansed form, blocking keys          (a map)
    B  pairs           group records by blocking key (a key shared by more than max_bucket_size OTHER records is skipped,
                       exactly as in the incremental engine) and emit each candidate pair once         (a group-by)
    C  score           every pair, in parallel, with the same score_pair and the same learned-model blend (a map)
    D  cluster         union the auto-merge pairs: every record is in exactly one entity               (connected components)
    E  write           raw records, staging, blocking index, candidates, golden records and lineage,
                       all in batches. NOTHING is written to the store before this stage, so a failure
                       in A to D leaves the store untouched, and a failure in E is rolled back.

The result is the same state the regular engine produces (its entities and review pairs equal the reference matcher's in any
order: it is one of the engines `benchmark.py equivalence` checks) so ingestion, review, reindex and process-pending carry on from it.
Golden records are built by the SAME functions as the regular engine's (pipeline.compute_golden).

WHAT IT IS NOT (yet). It loads into an empty tenant+domain: adding a source to data that already exists is a different job (new x new
and new x existing pairs, with existing entities as nodes), not built. LLM adjudication is not used (ambiguous pairs go to review).
The work files live on disk (a temporary SQLite database) so memory does not grow with the number of pairs, but the union-find array
(4-8 bytes per record) and one chunk of rows are in memory. Do not ingest into the same tenant+domain while it runs: the empty-check
is a check, not a lock. v1 is single-machine; the stages are written so each can move to another executor.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from array import array
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from itertools import combinations, islice

from . import active_learning, learned_scorer
from .domains import get_domain_config
from .indexing import ensure_index_ready
from .matching import blocking_keys, classify, score_pair
from .models import Entity, MatchCandidate, SourceRecord
from .pipeline import Run, compute_golden, content_hash, golden_values
from .staging import stage_record

NO_BLOCK_KEY = "__no_block__"
LINEAGE_POLICIES = ("multi_source", "all", "none")


MIN_KEY = -(2 ** 62)  # existing records are numbered -1, -2, ...: below every new record, so (earlier, later) = (a, b) still holds for every pair


class BulkLoadError(RuntimeError):
    pass


class BulkPaused(Exception):
    """The time budget ran out at a chunk boundary and progress is saved in the work directory: run the same call again with resume=True."""

    def __init__(self, stage: str, done: int):
        super().__init__(f"paused in the '{stage}' stage after {done:,} items; progress is saved in the work directory")
        self.stage, self.done = stage, done


@dataclass
class BulkResult:
    records: int
    entities: int
    candidate_pairs: int      # pairs sharing a usable key
    auto_pairs: int           # of those, merged automatically
    review_pairs: int         # of those, queued for a steward
    capped_keys: int          # keys skipped for being shared by too many records
    seconds: dict = field(default_factory=dict)
    run_id: str = ""
    dry_run: bool = False     # stages A to D only: nothing was written to the store
    workdir: str = ""
    existing_records: int = 0             # onboarding: how many records the tenant already held
    existing_entities_touched: int = 0    # onboarding: existing entities that gained records
    existing_entities_merged: int = 0     # onboarding: existing entities this load merged into an older one
    promoted_pairs: int = 0               # pairs the learned scorer promoted out of review (0 unless the tenant has a live scorer)
    separations_present: bool = False     # a person has unmerged something in this domain
    separations_held: int = 0             # automatic merges this load did NOT make, because they would have reunited records a person separated: queued for review instead
    separations_dropped: int = 0          # pairs that ARE a pair a person separated, found again by this load: not proposed at all
    new_records: int = 0                  # delta: records of a source already loaded that are new
    changed_records: int = 0              # delta: records already held whose content changed
    unchanged_records: int = 0            # delta: records already held that did not change (nothing is done for them)


_W: dict = {}  # set before the worker processes are forked, so they inherit it


# ------------------------------------------------------------------------------------------------ the two map functions
def _standardize_chunk(items):
    config, domain = _W["config"], _W["domain"]
    out = []
    for idx, system, source_id, attrs in items:
        matching, cleansed = stage_record(domain, config, attrs)
        out.append((idx, system, source_id, json.dumps(attrs), content_hash(attrs), json.dumps(matching), json.dumps(cleansed),
                    sorted(blocking_keys(matching, config))))
    return out


def _score_chunk(rows):
    config, model = _W["config"], _W["model"]
    out = []
    for a, b, matching_a, matching_b in rows:  # a < b: b ARRIVED later, so it plays the "new record" the regular engine scores
        std_a, std_b = json.loads(matching_a), json.loads(matching_b)
        breakdown = score_pair(std_b, std_a, config)
        if model is not None:
            blended = active_learning.blend_scores(breakdown.score, model, active_learning.extract_features(std_b, std_a, config))
            if abs(blended - breakdown.score) > 0.01:
                breakdown.reasons = breakdown.reasons + [
                    f"blended with learned model ({model.trained_on_examples} past decisions): {breakdown.score:.2f} -> {blended:.2f}"]
            breakdown.score = round(blended, 4)
        decision, promoted = classify(breakdown.score), 0
        if decision == "review" and _W.get("scorer") is not None:  # the same promotion the regular engine makes, from the same function
            yes, p = learned_scorer.promote(_W["scorer"], breakdown.score, breakdown.match_type, breakdown.contributions)
            if yes:
                decision, promoted = "auto_merge", 1
                breakdown.reasons = breakdown.reasons + [f"promoted out of review by the learned scorer (p = {p:.3f})"]
        if decision != "no_match":
            out.append((a, b, breakdown.score, decision, breakdown.match_type, json.dumps(breakdown.reasons), json.dumps(breakdown.contributions), promoted))
    return out


def _pair_chunks(pairs_path: str, work_path: str, size: int, start=(MIN_KEY, MIN_KEY), bounds=None):
    """The pairs, joined to both records' matching forms, `size` at a time (its own connection: it may run in a feeder thread)."""
    con = sqlite3.connect(pairs_path)
    con.execute("ATTACH DATABASE ? AS w", (work_path,))
    last = start
    while True:
        rows = con.execute("SELECT p.a, p.b, ra.matching, rb.matching FROM pairs p JOIN w.rec ra ON ra.idx = p.a JOIN w.rec rb ON rb.idx = p.b "
                           "WHERE (p.a, p.b) > (?, ?) ORDER BY p.a, p.b LIMIT ?", (*last, size)).fetchall()
        if not rows:
            con.close()
            return
        last = (rows[-1][0], rows[-1][1])
        if bounds is not None:
            bounds.append(last)  # imap keeps order, so the consumer pops these in step with the results
        yield rows


def _input_chunks(records, size, start=0):
    it, idx = iter(records), start
    while True:
        part = list(islice(it, size))
        if not part:
            return
        yield [(idx + k, system, source_id, attrs) for k, (system, source_id, attrs) in enumerate(part)]
        idx += len(part)


def _meta_get(db, table, k, default=None):
    row = db.execute(f"SELECT v FROM {table} WHERE k = ?", (k,)).fetchone()
    return row[0] if row else default


def _meta_set(db, table, k, v):
    db.execute(f"INSERT OR REPLACE INTO {table} VALUES (?, ?)", (k, str(v)))


def _drop_separated_pairs(db, edb, record_pairs) -> int:
    """Delete, from the scored edges, every edge between two records a person separated (only a changed record can meet an existing one again: existing
    pairs are never rescored). The number of edges deleted."""
    wanted = sorted({r for pair in record_pairs for r in pair})
    number = {}
    for lo in range(0, len(wanted), 500):
        part = wanted[lo:lo + 500]
        marks = ",".join("?" * len(part))
        for rid_, record_id in db.execute(f"SELECT rid, record_id FROM ext WHERE record_id IN ({marks})", part):
            number[record_id] = -rid_            # an existing record paired in this load: the edges number it -rid
        for idx, record_id in db.execute(f"SELECT idx, record_id FROM changed WHERE record_id IN ({marks})", part):
            number[record_id] = idx
    edges = sorted({tuple(sorted((number[a], number[b]))) for a, b in record_pairs if a in number and b in number})
    if not edges:
        return 0
    edb.execute("CREATE INDEX IF NOT EXISTS edges_ab ON edges (a, b)")
    before = edb.total_changes
    edb.executemany("DELETE FROM edges WHERE a = ? AND b = ?", edges)
    return edb.total_changes - before


def _find(parent, x):
    root = x
    while parent[root] != root:
        root = parent[root]
    while parent[x] != root:  # path compression
        parent[x], x = root, parent[x]
    return root


# ------------------------------------------------------------------------------------------------ the engine
def bulk_load(store, tenant_id: str, domain: str, records, *, workers: int = 1, chunk: int = 2000, workdir: str | None = None,
              lineage: str = "multi_source", triggered_by: str = "cli", log=None, write: bool = True, resume: bool = False,
              budget: float | None = None, onboard: bool = False, update: bool = False) -> BulkResult:
    """records: an iterable of (source_system, source_record_id, attributes), each natural key once; or, to be resumable without re-reading
    the start of the input, a function records(start) returning the iterable from the start-th record on. `lineage`: which golden records
    get one lineage row per field: "all" (what the regular engine does), "multi_source" (only entities of 2+ records: a single-record entity's
    provenance is that one record, and at 100M records 'all' is over a billion rows), or "none".

    By default the tenant must hold no records of this domain. onboard=True ADDS these records to existing data (a new source system): they
    are compared with each other AND with the records already held (through the stored blocking index), and wherever they link existing
    entities those merge, the OLDEST surviving, exactly as the regular engine would have done record by record. Pairs between two existing
    records are not rescored, so steward decisions stand. Every change to existing entities is applied in one transaction, and nothing else
    may write to this tenant and domain while an onboarding runs. The records must be NEW: a natural key already held is refused.

    update=True (implies onboard) is a DELTA: more records from a source already loaded. Each record is classified by its natural key: NEW (onboarded
    as above), UNCHANGED (same content: nothing is done), or CHANGED. A changed record is updated in place (same id, same entity, ingested_at
    untouched), its old payload goes to history, its pending review items are superseded, and it is re-matched with its NEW keys: the keys it is
    losing are ignored, and it is not rescored against its own entity. Like the regular engine, a change can add merges but never splits an entity.

    write=False is a DRY RUN: stages A to D only, the store is not touched, and the work files are kept (so what a load WOULD do can be
    inspected: its pairs, its review pairs, its entities). workdir + resume=True + budget=SECONDS make the long stages (A and C) stop cleanly
    at a chunk boundary (BulkPaused) and carry on where they stopped when called again; B and D are quick and simply redone. The write stage
    is not resumable: it is all-or-nothing (a failure is rolled back)."""
    if lineage not in LINEAGE_POLICIES:
        raise ValueError(f"lineage must be one of {LINEAGE_POLICIES}")
    if (resume or budget is not None) and workdir is None:
        raise ValueError("resume and budget need a workdir to keep their progress in")
    onboard = onboard or update
    config = get_domain_config(domain)  # KeyError for an unknown domain
    fingerprint = ensure_index_ready(store, domain, config)  # refuses if the configuration changed and the derived data was not rebuilt
    existing_count = store.count_tenant_records(tenant_id, domain)
    if existing_count and not onboard:
        raise BulkLoadError(f"tenant '{tenant_id}' already has '{domain}' records. A bulk load builds a whole domain from nothing. To ADD a new source "
                            f"to existing data, onboard it (onboard=True, or --onboard); to change records already held use the regular ingest path "
                            f"(bulk_ingest.py without --bulk).")
    onb = existing_count > 0
    if onb and store.unfinished_in(tenant_id, domain):
        raise BulkLoadError(f"{store.unfinished_in(tenant_id, domain):,} of this tenant's '{domain}' records are unfinished (landed or staged but not matched). Onboarding "
                            f"compares the new records with the existing ones, so finish those first: domains_cli.py process-pending {domain}")
    say = log or (lambda *_: None)
    token, started = uuid.uuid4().hex[:10], datetime.now(timezone.utc)
    rid = lambda idx: f"rec_{token}_{idx}"
    eid = lambda root: f"ent_{token}_{root}"
    ts = lambda idx: (started + timedelta(microseconds=idx)).isoformat()  # arrival order = input order, so "most recent" means "latest in the input"
    own = workdir is None
    work = tempfile.mkdtemp(prefix="mdm_bulk_") if own else workdir
    os.makedirs(work, exist_ok=True)
    work_db, pairs_db, edges_db = (os.path.join(work, n) for n in ("work.db", "pairs.db", "edges.db"))
    resumable = resume or budget is not None
    fresh = not (resume and os.path.exists(work_db))
    if fresh:
        for path in (work_db, pairs_db, edges_db):
            if os.path.exists(path):
                os.remove(path)
    deadline = time.time() + budget if budget is not None else None
    ident = f"{tenant_id}|{domain}|{fingerprint}"
    run = Run.start(store, tenant_id, domain, "bulk", triggered_by) if (write and not resumable) else None
    wrote, pool, finished = False, None, False
    _W.update(config=config, domain=domain, model=active_learning.load_model(tenant_id, domain), scorer=learned_scorer.active_scorer(tenant_id, domain))
    pragma = ("PRAGMA journal_mode = WAL", "PRAGMA synchronous = NORMAL") if resumable else ("PRAGMA journal_mode = OFF", "PRAGMA synchronous = OFF")
    try:
        if workers > 1:
            pool = multiprocessing.get_context("fork").Pool(workers)
        mapper = (lambda fn, it: pool.imap(fn, it)) if pool else (lambda fn, it: map(fn, it))
        db = sqlite3.connect(work_db)
        for pr in pragma:
            db.execute(pr)
        db.executescript("CREATE TABLE IF NOT EXISTS raw (idx INTEGER PRIMARY KEY, system TEXT, source_id TEXT, attributes TEXT, hash TEXT, UNIQUE (system, source_id));"
                         "CREATE TABLE IF NOT EXISTS rec (idx INTEGER PRIMARY KEY, matching TEXT, cleansed TEXT);"
                         "CREATE TABLE IF NOT EXISTS keys (key TEXT, idx INTEGER);"
                         "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);"
                         "CREATE TABLE IF NOT EXISTS changed (idx INTEGER PRIMARY KEY, record_id TEXT, entity_id TEXT, version INTEGER, old_hash TEXT, old_attrs TEXT);")
        if fresh:
            _meta_set(db, "meta", "ident", ident)
            _meta_set(db, "meta", "existing", existing_count)
            db.commit()
        elif _meta_get(db, "meta", "ident") != ident:
            raise BulkLoadError("this work directory belongs to a different load (another tenant, domain or configuration); use an empty one, or run without resume")
        elif int(_meta_get(db, "meta", "existing", 0)) != existing_count:
            raise BulkLoadError(f"the tenant's '{domain}' data changed since this load began (it held {int(_meta_get(db, 'meta', 'existing', 0)):,} records then and holds "
                                f"{existing_count:,} now), so the pairs already found may be stale. Start again without resume.")
        spent = lambda stage: float(_meta_get(db, "meta", f"t_{stage}", 0))
        add_time = lambda stage, t0: _meta_set(db, "meta", f"t_{stage}", spent(stage) + time.time() - t0)

        # ---- A: classify (a delta only), then standardize ---------------------------------------------------------
        if _meta_get(db, "meta", "A_complete") != "1":
            t0, done, read = time.time(), int(_meta_get(db, "meta", "A_done", 0)), int(_meta_get(db, "meta", "A_read", 0))
            source = (records(read) if callable(records) else islice(records, read, None))
            side = deque()  # what the classifier learned about each chunk, popped in step with the chunk's results (the pool keeps order)

            def classified(source, read, idx):
                """Yield each input chunk's PIPELINE rows [(idx, system, id, attrs)]. A record already held is a delta's business: unchanged ones are
                set aside, changed ones go on with a note of what they replace. Without a delta, a record already held is refused."""
                it = iter(source)
                while True:
                    block = list(islice(it, chunk))
                    if not block:
                        return
                    lookup = time.perf_counter()
                    held = store.find_source_records_by_keys(tenant_id, domain, [(sy, sid) for sy, sid, _ in block]) if onb else {}
                    lookup = time.perf_counter() - lookup
                    rows, info, skipped = [], {}, 0
                    for system, source_id, attrs in block:
                        rec = held.get((system, source_id))
                        if rec is not None:
                            if not update:
                                raise BulkLoadError(f"source record '{source_id}' of system '{system}' is already held ({len(held):,} such records in this chunk alone). Onboarding adds "
                                                    f"NEW records; to apply changes to records already held, load them as a delta (update=True, --delta).")
                            # (an unfinished record cannot get here: the tenant-level check above refuses any unfinished record before a single record is classified)
                            if content_hash(attrs) == (rec.content_hash or content_hash(rec.attributes)):
                                skipped += 1
                                continue
                            info[idx] = rec
                        rows.append((idx, system, source_id, attrs))
                        idx += 1
                    read += len(block)
                    side.append((info, read, skipped, lookup))
                    yield rows
            for rows in mapper(_standardize_chunk, classified(source, read, done)):
                info, read, skipped, lookup = side.popleft()
                _meta_set(db, "meta", "t_classify", float(_meta_get(db, "meta", "t_classify", 0)) + lookup)
                try:
                    db.executemany("INSERT INTO raw VALUES (?,?,?,?,?)", [r[:5] for r in rows])
                except sqlite3.IntegrityError:
                    seen = {}
                    for r in rows:
                        if (r[1], r[2]) in seen or db.execute("SELECT 1 FROM raw WHERE system = ? AND source_id = ?", (r[1], r[2])).fetchone():
                            raise BulkLoadError(f"the input holds the same record twice: source system '{r[1]}', source record id '{r[2]}'. A bulk load takes each record "
                                                f"once; de-duplicate the input, or use the regular ingest path, which versions a changed record.") from None
                        seen[(r[1], r[2])] = True
                    raise
                db.executemany("INSERT INTO rec VALUES (?,?,?)", [(r[0], r[5], r[6]) for r in rows])
                db.executemany("INSERT INTO keys VALUES (?,?)", [(k, r[0]) for r in rows for k in r[7]])
                db.executemany("INSERT INTO changed VALUES (?,?,?,?,?,?)", [(idx, rec.id, rec.entity_id, rec.version, rec.content_hash or content_hash(rec.attributes),
                                                                              json.dumps(rec.attributes)) for idx, rec in info.items()])
                done += len(rows)
                _meta_set(db, "meta", "A_done", done)
                _meta_set(db, "meta", "A_read", read)
                _meta_set(db, "meta", "n_unchanged", int(_meta_get(db, "meta", "n_unchanged", 0)) + skipped)
                db.commit()
                if deadline is not None and time.time() > deadline:
                    add_time("standardize", t0)
                    db.commit()
                    raise BulkPaused("standardize", read)
            _meta_set(db, "meta", "A_complete", 1)
            add_time("standardize", t0)
            db.commit()
            say(f"A standardized {done:,} records")
        n = int(_meta_get(db, "meta", "A_done", 0))
        n_changed = db.execute("SELECT COUNT(*) FROM changed").fetchone()[0]
        n_unchanged = int(_meta_get(db, "meta", "n_unchanged", 0))
        if update:
            say(f"classified: {n - n_changed:,} new, {n_changed:,} changed, {n_unchanged:,} unchanged (nothing is done for those)")

        # ---- B: candidate pairs ----------------------------------------------------------------------------------
        if _meta_get(db, "meta", "B_complete") != "1":
            t0 = time.time()
            db.execute("CREATE INDEX IF NOT EXISTS keys_key ON keys (key, idx)")  # covering: the group-by never touches the table
            db.execute("CREATE INDEX IF NOT EXISTS keys_idx ON keys (idx)")
            cap = config.max_bucket_size
            db.executescript("DROP TABLE IF EXISTS kcount; CREATE TABLE kcount (key TEXT PRIMARY KEY, existing INTEGER) WITHOUT ROWID;"
                             "DROP TABLE IF EXISTS ext; CREATE TABLE ext (rid INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT UNIQUE, entity_id TEXT);"
                             "DELETE FROM rec WHERE idx < 0;")
            if onb:  # how many EXISTING records hold each of the new records' keys: a bucket's size is existing + new, as in the reference matcher
                cur = db.execute("SELECT DISTINCT key FROM keys WHERE key <> ?", (NO_BLOCK_KEY,))
                while True:
                    names = [r[0] for r in cur.fetchmany(500)]
                    if not names:
                        break
                    db.executemany("INSERT INTO kcount VALUES (?,?)", list(store.blocking_bucket_sizes(tenant_id, domain, set(names)).items()))
            changed_ids = {r[1] for r in db.execute("SELECT idx, record_id FROM changed")}
            if changed_ids:  # a changed record is about to lose its old keys: they must not count towards a bucket, nor pair it with anyone
                db.executescript("DROP TABLE IF EXISTS oldkeys; CREATE TABLE oldkeys (key TEXT, record_id TEXT);")
                ids = sorted(changed_ids)
                for i in range(0, len(ids), 500):
                    db.executemany("INSERT INTO oldkeys VALUES (?,?)", [(k, record_id) for record_id, ks in store.blocking_keys_of(tenant_id, domain, ids[i:i + 500]).items() for k in ks])
                db.execute("CREATE INDEX oldkeys_key ON oldkeys (key)")
                db.execute("UPDATE kcount SET existing = existing - (SELECT COUNT(*) FROM oldkeys o WHERE o.key = kcount.key)")
            joined = "FROM keys k LEFT JOIN kcount c ON c.key = k.key WHERE k.key <> ? GROUP BY k.key"
            capped = db.execute(f"SELECT COUNT(*) FROM (SELECT k.key {joined} HAVING COUNT(*) + COALESCE(c.existing, 0) > ?)", (NO_BLOCK_KEY, cap + 1)).fetchone()[0]
            if os.path.exists(pairs_db):
                os.remove(pairs_db)
            pdb = sqlite3.connect(pairs_db)
            pdb.execute("PRAGMA journal_mode = OFF")
            pdb.execute("PRAGMA synchronous = OFF")
            pdb.execute("CREATE TABLE pairs (a INTEGER, b INTEGER, PRIMARY KEY (a, b)) WITHOUT ROWID")
            batch, pending = [], []

            def ext_ids(record_ids):  # existing records get the numbers -1, -2, ...
                record_ids = list(record_ids)
                db.executemany("INSERT OR IGNORE INTO ext (record_id) VALUES (?)", [(r,) for r in record_ids])
                out = {}
                for i in range(0, len(record_ids), 500):
                    part = record_ids[i:i + 500]
                    for record_id, number in db.execute(f"SELECT record_id, rid FROM ext WHERE record_id IN ({','.join('?' * len(part))})", part):
                        out[record_id] = -number
                return out

            def flush_pending():  # new records whose key an existing record also holds: pair each with those existing records
                if pending:
                    held = store.blocking_members(tenant_id, domain, [k for k, _ in pending])
                    numbers = ext_ids({r for members in held.values() for r in members if r not in changed_ids})
                    for key, news in pending:
                        for record_id in held.get(key, ()):
                            if record_id in changed_ids:
                                continue  # a changed record meets others through its NEW keys, which are among the new records'
                            batch.extend((numbers[record_id], new) for new in news)
                    pending.clear()
            for _key, members, existing in db.execute(f"SELECT k.key, GROUP_CONCAT(k.idx), COALESCE(c.existing, 0) {joined} HAVING COUNT(*) + COALESCE(c.existing, 0) BETWEEN 2 AND ?",
                                                      (NO_BLOCK_KEY, cap + 1)).fetchall() if onb else db.execute(
                    f"SELECT k.key, GROUP_CONCAT(k.idx), 0 {joined} HAVING COUNT(*) BETWEEN 2 AND ?", (NO_BLOCK_KEY, cap + 1)):
                new_ids = sorted(int(x) for x in members.split(","))
                batch.extend(combinations(new_ids, 2))
                if existing:
                    pending.append((_key, new_ids))
                if len(batch) >= 50_000 or len(pending) >= 500:
                    flush_pending()
                    pdb.executemany("INSERT OR IGNORE INTO pairs VALUES (?,?)", batch)
                    batch.clear()
            flush_pending()
            pdb.executemany("INSERT OR IGNORE INTO pairs VALUES (?,?)", batch)
            pdb.commit()
            last_rid = 0
            while True:  # the existing records that now take part in a pair: their matching form, which is all scoring reads of them
                part = db.execute("SELECT rid, record_id FROM ext WHERE rid > ? ORDER BY rid LIMIT 500", (last_rid,)).fetchall()
                if not part:
                    break
                last_rid = part[-1][0]
                matching = store.get_matching_many(tenant_id, domain, [r[1] for r in part])
                for number, record_id in part:
                    if record_id not in matching:
                        raise BulkLoadError(f"existing record {record_id} has no standardized form; run domains_cli.py reindex {domain} first")
                    db.execute("INSERT INTO rec VALUES (?,?,?)", (-number, json.dumps(matching[record_id]), "{}"))
            if changed_ids:
                # Which entity an existing record is in matters HERE only to drop pairs inside one entity, which only a CHANGED record is exempt from being scored
                # against: so it is looked up only for the existing records paired with one. Every other entity id is fetched in stage D, for the few records
                # on an auto-merge edge (looking up all of them was the largest single cost of this stage: a random read of a fat row for each).
                db.commit()
                pdb.execute("ATTACH DATABASE ? AS w", (work_db,))
                paired = [-r[0] for r in pdb.execute("SELECT DISTINCT p.a FROM pairs p JOIN w.changed c ON c.idx = p.b WHERE p.a < 0")]
                for i in range(0, len(paired), 500):
                    part = db.execute(f"SELECT rid, record_id FROM ext WHERE rid IN ({','.join('?' * len(paired[i:i + 500]))})", paired[i:i + 500]).fetchall()
                    entities_of = store.get_record_entities(tenant_id, domain, [r[1] for r in part])
                    db.executemany("UPDATE ext SET entity_id = ? WHERE rid = ?", [(entities_of.get(record_id), number) for number, record_id in part])
                db.executescript("DROP TABLE IF EXISTS ent; CREATE TABLE ent (idx INTEGER PRIMARY KEY, entity_id TEXT);"
                                 "INSERT INTO ent SELECT idx, entity_id FROM changed WHERE entity_id IS NOT NULL;"
                                 "INSERT INTO ent SELECT -rid, entity_id FROM ext WHERE entity_id IS NOT NULL;")
                db.commit()
                pdb.execute("DELETE FROM pairs WHERE EXISTS (SELECT 1 FROM w.ent x JOIN w.ent y ON y.entity_id = x.entity_id WHERE x.idx = pairs.a AND y.idx = pairs.b)")
                pdb.commit()
            _meta_set(db, "meta", "n_pairs", pdb.execute("SELECT COUNT(*) FROM pairs").fetchone()[0])
            _meta_set(db, "meta", "capped", capped)
            pdb.close()
            _meta_set(db, "meta", "B_complete", 1)
            add_time("pairs", t0)
            db.commit()
            say(f"B {_meta_get(db, 'meta', 'n_pairs')} candidate pairs ({capped} keys over the cap skipped)")
        n_pairs, capped = int(_meta_get(db, "meta", "n_pairs")), int(_meta_get(db, "meta", "capped"))

        # ---- C: score --------------------------------------------------------------------------------------------
        edb = sqlite3.connect(edges_db)
        for pr in pragma:
            edb.execute(pr)
        edb.executescript("CREATE TABLE IF NOT EXISTS edges (a INTEGER, b INTEGER, score REAL, decision TEXT, match_type TEXT, reasons TEXT, contributions TEXT, promoted INTEGER NOT NULL DEFAULT 0);"
                          "CREATE TABLE IF NOT EXISTS progress (k TEXT PRIMARY KEY, v TEXT);")
        edb.execute("ATTACH DATABASE ? AS w", (work_db,))
        if _meta_get(edb, "progress", "C_complete") != "1":
            t0 = time.time()
            start = tuple(int(x) for x in _meta_get(edb, "progress", "C_last", f"{MIN_KEY},{MIN_KEY}").split(","))
            bounds = deque()
            for edges in mapper(_score_chunk, _pair_chunks(pairs_db, work_db, chunk, start, bounds)):
                edb.executemany("INSERT INTO edges VALUES (?,?,?,?,?,?,?,?)", edges)
                last = bounds.popleft()
                _meta_set(edb, "progress", "C_last", f"{last[0]},{last[1]}")
                edb.commit()  # the edges and the position they reached are saved together
                if deadline is not None and time.time() > deadline:
                    _meta_set(edb, "progress", "t_score", float(_meta_get(edb, "progress", "t_score", 0)) + time.time() - t0)
                    edb.commit()
                    raise BulkPaused("score", int(last[0]))
            _meta_set(edb, "progress", "C_complete", 1)
            _meta_set(edb, "progress", "t_score", float(_meta_get(edb, "progress", "t_score", 0)) + time.time() - t0)
            edb.commit()
        auto = edb.execute("SELECT COUNT(*) FROM edges WHERE decision = 'auto_merge'").fetchone()[0]
        review = edb.execute("SELECT COUNT(*) FROM edges WHERE decision = 'review'").fetchone()[0]
        say(f"C scored {n_pairs:,} pairs: {auto:,} auto-merge, {review:,} review")

        # ---- D: cluster ------------------------------------------------------------------------------------------
        # The nodes are the new records and, for onboarding, the EXISTING ENTITIES an auto-merge edge reaches: an edge to an existing record
        # joins the new record to that record's entity, and a new record that reaches two existing entities merges them.
        if _meta_get(db, "meta", "D_complete") != "1":
            t0 = time.time()
            # the entity of each existing record on an auto-merge edge: the only existing records whose entity clustering needs
            lazily = edb.execute("SELECT DISTINCT x.rid, x.record_id FROM edges ed JOIN w.ext x ON x.rid = -ed.a WHERE ed.decision = 'auto_merge' AND ed.a < 0 AND x.entity_id IS NULL").fetchall()
            for i in range(0, len(lazily), 500):
                part = lazily[i:i + 500]
                entities_of = store.get_record_entities(tenant_id, domain, [r[1] for r in part])
                missing = [r[1] for r in part if not entities_of.get(r[1])]
                if missing:
                    raise BulkLoadError(f"existing record {missing[0]} belongs to no entity: finish the existing records first (domains_cli.py process-pending {domain})")
                db.executemany("UPDATE ext SET entity_id = ? WHERE rid = ?", [(entities_of[record_id], number) for number, record_id in part])
            db.commit()
            # SEPARATIONS. A person who unmerged a pair said those two records are different: no automatic merge may join two components that hold records of two
            # entities between which a person separated records. Each component carries the set of existing entities it contains; an edge that would join two
            # components with a separation between them is held for review instead (and a pair that IS a separated pair is dropped). Edges are taken strongest first,
            # ties by position, so a real conflict resolves the same way every run (a set load has no arrival order to decide it by).
            sep_adj, ents, held, dropped = {}, {}, 0, 0
            if store.has_separations(tenant_id, domain):
                sep_pairs = store.separation_pairs(tenant_id, domain)
                dropped = _drop_separated_pairs(db, edb, sep_pairs)
                of, involved = {}, sorted({r for pair in sep_pairs for r in pair})
                for lo in range(0, len(involved), 500):
                    of.update(store.get_record_entities(tenant_id, domain, involved[lo:lo + 500]))
                for ra_id, rb_id in sep_pairs:
                    ea, eb = of.get(ra_id), of.get(rb_id)
                    if ea and eb and ea != eb:  # (two separated records in ONE entity is an old violation: separation-check reports it)
                        sep_adj.setdefault(ea, set()).add(eb)
                        sep_adj.setdefault(eb, set()).add(ea)
            constrained = bool(sep_adj)
            parent, node_of, entity_of_node = array("l", range(n)), {}, []

            def entity_node(entity):
                if entity not in node_of:
                    node_of[entity] = len(parent)
                    parent.append(len(parent))
                    entity_of_node.append(entity)
                    if constrained:
                        ents[node_of[entity]] = {entity}
                return node_of[entity]

            def union(ra, rb):
                lo, hi = (ra, rb) if ra < rb else (rb, ra)  # the earliest record is the root, so an entity is named by its first arrival
                parent[hi] = lo
                if constrained and hi in ents:
                    ents.setdefault(lo, set()).update(ents.pop(hi))

            def separated(ra, rb):
                ea, eb = ents.get(ra), ents.get(rb)
                if not ea or not eb:
                    return False
                small, big = (ea, eb) if len(ea) <= len(eb) else (eb, ea)
                return any(sep_adj.get(e) and not sep_adj[e].isdisjoint(big) for e in small)
            for idx, entity in db.execute("SELECT idx, entity_id FROM changed WHERE entity_id IS NOT NULL"):  # a changed record already belongs to its entity
                ra, rb = _find(parent, idx), _find(parent, entity_node(entity))
                if ra != rb:
                    union(ra, rb)
            blocked = []
            for a, b, entity in edb.execute("SELECT ed.a, ed.b, x.entity_id FROM edges ed LEFT JOIN w.ext x ON x.rid = -ed.a WHERE ed.decision = 'auto_merge'"
                                            + (" ORDER BY ed.score DESC, ed.a, ed.b" if constrained else "")):
                node = entity_node(entity) if a < 0 else a
                ra, rb = _find(parent, node), _find(parent, b)
                if ra != rb:
                    if constrained and separated(ra, rb):
                        blocked.append((a, b))
                        continue
                    union(ra, rb)
            if blocked:
                edb.execute("CREATE INDEX IF NOT EXISTS edges_ab ON edges (a, b)")
                note = "not merged automatically: a person separated records in these two entities"
                for a, b in blocked:
                    reasons = json.loads(edb.execute("SELECT reasons FROM edges WHERE a = ? AND b = ?", (a, b)).fetchone()[0])
                    edb.execute("UPDATE edges SET decision = 'review', promoted = 0, reasons = ? WHERE a = ? AND b = ?", (json.dumps(reasons + [note]), a, b))
            held = len(blocked)
            edb.commit()
            _meta_set(db, "meta", "sep_held", held)
            _meta_set(db, "meta", "sep_dropped", dropped)
            db.execute("DROP TABLE IF EXISTS comp")
            db.execute("DROP TABLE IF EXISTS centity")
            db.execute("CREATE TABLE comp (idx INTEGER PRIMARY KEY, root INTEGER)")
            db.execute("CREATE TABLE centity (root INTEGER, entity_id TEXT)")
            for lo in range(0, n, 50_000):
                db.executemany("INSERT INTO comp VALUES (?,?)", [(i, _find(parent, i)) for i in range(lo, min(lo + 50_000, n))])
            db.executemany("INSERT INTO centity VALUES (?,?)", [(_find(parent, n + j), entity) for j, entity in enumerate(entity_of_node)])
            db.execute("CREATE INDEX comp_root ON comp (root)")
            del parent
            _meta_set(db, "meta", "D_complete", 1)
            add_time("cluster", t0)
            db.commit()
        held, dropped = int(_meta_get(db, "meta", "sep_held", 0)), int(_meta_get(db, "meta", "sep_dropped", 0))
        if held or dropped:  # separations changed what stage C found: the counts must say what will be written
            auto = edb.execute("SELECT COUNT(*) FROM edges WHERE decision = 'auto_merge'").fetchone()[0]
            review = edb.execute("SELECT COUNT(*) FROM edges WHERE decision = 'review'").fetchone()[0]
            say(f"D separations: {held:,} automatic merge(s) held for review, {dropped:,} pair(s) a person separated not proposed again")
        touched = db.execute("SELECT COUNT(*) FROM centity").fetchone()[0]
        merged_away = touched - db.execute("SELECT COUNT(DISTINCT root) FROM centity").fetchone()[0]
        n_entities = db.execute("SELECT COUNT(DISTINCT root) FROM comp").fetchone()[0] - db.execute("SELECT COUNT(DISTINCT root) FROM centity").fetchone()[0]  # entities this load CREATES
        seconds = {"classify": round(float(_meta_get(db, "meta", "t_classify", 0)), 2), "standardize": round(spent("standardize"), 2), "pairs": round(spent("pairs"), 2),
                   "score": round(float(_meta_get(edb, "progress", "t_score", 0)), 2), "cluster": round(spent("cluster"), 2)}
        say(f"D {n:,} records -> {n_entities:,} new entities" + (f", {touched:,} existing entities gain records and {merged_away:,} of them merge into an older one" if onb else ""))
        counts = dict(separations_present=store.has_separations(tenant_id, domain), separations_held=held, separations_dropped=dropped, promoted_pairs=edb.execute("SELECT COUNT(*) FROM edges WHERE promoted = 1").fetchone()[0], existing_records=existing_count, existing_entities_touched=touched, existing_entities_merged=merged_away,
                      new_records=n - n_changed, changed_records=n_changed, unchanged_records=n_unchanged)
        if not write:
            finished = True
            return BulkResult(n, n_entities, n_pairs, auto, review, capped, seconds, "", True, work, **counts)

        # ---- E: write (the first moment the store is touched) ----------------------------------------------------
        if store.count_tenant_records(tenant_id, domain) != existing_count:
            raise BulkLoadError(f"the tenant's '{domain}' data changed while this load was being planned (it held {existing_count:,} records and now holds "
                                f"{store.count_tenant_records(tenant_id, domain):,}), so the plan may be stale. Nothing was written; start again.")
        plan = {}  # component root -> (the existing entity that survives, [the existing entities merged into it])
        if touched:
            by_root, found = {}, {}
            for root, entity in db.execute("SELECT root, entity_id FROM centity"):
                by_root.setdefault(root, []).append(entity)
            wanted = sorted({e for ents in by_root.values() for e in ents})
            for i in range(0, len(wanted), 500):
                found.update(store.get_entities_batch(tenant_id, domain, wanted[i:i + 500]))
            if len(found) != len(wanted):
                raise BulkLoadError(f"{len(wanted) - len(found)} existing entities this load links no longer exist; nothing was written")
            for root, ents in by_root.items():
                survivor = min(ents, key=lambda e: (found[e].created_at or "", e))  # the OLDEST survives: downstream systems already hold its id
                plan[root] = (survivor, [e for e in ents if e != survivor])
        if run is None:
            run = Run.start(store, tenant_id, domain, "bulk", triggered_by)
        t0 = time.time()
        wrote = True
        owner = lambda root: plan[root][0] if root in plan else eid(root)
        changed = {r[0]: r[1:] for r in db.execute("SELECT idx, record_id, entity_id, version, old_hash, old_attrs FROM changed")}
        rec_id = lambda idx: changed[idx][0] if idx in changed else rid(idx)   # a changed record keeps its id; a new one gets the load's
        deferred = []                                                          # candidates that involve a changed record: written in the transaction below
        for lo in range(0, n, chunk):
            rows = db.execute("SELECT r.idx, r.system, r.source_id, r.attributes, r.hash, c.matching, c.cleansed, m.root FROM raw r "
                              "JOIN rec c ON c.idx = r.idx JOIN comp m ON m.idx = r.idx WHERE r.idx >= ? AND r.idx < ? AND r.idx NOT IN (SELECT idx FROM changed) ORDER BY r.idx", (lo, lo + chunk)).fetchall()
            keys_of = {}
            for idx, key in db.execute("SELECT idx, key FROM keys WHERE idx >= ? AND idx < ?", (lo, lo + chunk)):
                keys_of.setdefault(idx, []).append(key)
            store.save_source_records_batch([
                SourceRecord(source_system=r[1], source_record_id=r[2], attributes=json.loads(r[3]), id=rid(r[0]), tenant_id=tenant_id, domain=domain,
                             ingested_at=ts(r[0]), entity_id=owner(r[7]), content_hash=r[4], processing_status="matched") for r in rows])
            store.replace_derived_batch([(tenant_id, domain, rid(r[0]), json.loads(r[5]), json.loads(r[6]), fingerprint, keys_of.get(r[0], [])) for r in rows])
        last_root = -1
        while True:
            roots = [r[0] for r in db.execute("SELECT DISTINCT root FROM comp WHERE root > ? ORDER BY root LIMIT ?", (last_root, chunk))]
            if not roots:
                break
            last_root = roots[-1]
            roots = [r for r in roots if r not in plan]  # an entity that already exists is rebuilt below, once its new records and merges are known
            members = {}
            for row in db.execute(f"SELECT m.root, r.idx, r.system, r.source_id, r.attributes, c.cleansed FROM comp m JOIN raw r ON r.idx = m.idx "
                                  f"JOIN rec c ON c.idx = m.idx WHERE m.root IN ({','.join('?' * len(roots))}) ORDER BY m.root, m.idx", roots):
                members.setdefault(row[0], []).append(row)
            entities, events = [], []
            for root, rows in members.items():
                recs = [SourceRecord(source_system=r[2], source_record_id=r[3], attributes=json.loads(r[4]), id=rid(r[1]), tenant_id=tenant_id,
                                     domain=domain, ingested_at=ts(r[1])) for r in rows]
                decisions, issues = compute_golden(config, domain, recs, {rid(r[1]): json.loads(r[5]) for r in rows})
                values, evs = golden_values(decisions, {}, eid(root), tenant_id, domain, "bulk load")
                entities.append(Entity(id=eid(root), tenant_id=tenant_id, domain=domain, attributes=values, created_at=ts(rows[0][1]), quality_issues=issues))
                if lineage == "all" or (lineage == "multi_source" and len(rows) > 1):
                    events.extend(evs)
            store.save_entities_batch(entities)
            store.add_lineage_batch(events)
        last = (MIN_KEY, MIN_KEY)
        while True:
            rows = edb.execute("SELECT ed.a, ed.b, ed.score, ed.decision, ed.match_type, ed.reasons, ed.contributions, x.record_id, ed.promoted FROM edges ed "
                               "LEFT JOIN w.ext x ON x.rid = -ed.a WHERE (ed.a, ed.b) > (?, ?) ORDER BY ed.a, ed.b LIMIT ?", (*last, chunk)).fetchall()
            if not rows:
                break
            last = (rows[-1][0], rows[-1][1])
            decided = datetime.now(timezone.utc).isoformat()
            built = [(a, b, MatchCandidate(record_a_id=rec_id(b), record_b_id=(existing_id if a < 0 else rec_id(a)), tenant_id=tenant_id, domain=domain, score=score,
                                           match_type=mtype, reasons=json.loads(reasons), contributions=json.loads(contribs), created_at=ts(b),
                                           status="confirmed" if decision == "auto_merge" else "pending",
                                           decided_at=decided if decision == "auto_merge" else None,
                                           decided_by=(learned_scorer.PROMOTED_BY if promoted else "system:bulk_load") if decision == "auto_merge" else None))
                     for a, b, score, decision, mtype, reasons, contribs, existing_id, promoted in rows]
            touches = lambda a, b: b in changed or (a >= 0 and a in changed)
            immediate = [c for a, b, c in built if not touches(a, b)]
            store.save_candidates_batch(immediate)
            store.flag_merge_audits(tenant_id, domain, [c.id for c in immediate if c.status == "confirmed" and learned_scorer.audit_sampled(c.id, promoted=c.decided_by == learned_scorer.PROMOTED_BY)])   # 2% of automatic merges, flagged for a person
            deferred.extend(c for a, b, c in built if touches(a, b))
        if plan:  # everything that changes EXISTING data, in ONE transaction: the new rows above are undone by token if this fails, and this undoes itself
            reassign, upserts, events, now = [], [], [], datetime.now(timezone.utc).isoformat()
            reason = "delta load" if update else "onboarding load"
            payload = {}  # changed record id -> (new attributes, content hash, matching form, cleansed form, blocking keys)
            idxs = sorted(changed)
            for i in range(0, len(idxs), 500):
                part_idx = idxs[i:i + 500]
                marks = ",".join("?" * len(part_idx))
                for idx, attrs, h, matching, cleansed in db.execute(f"SELECT r.idx, r.attributes, r.hash, c.matching, c.cleansed FROM raw r JOIN rec c ON c.idx = r.idx WHERE r.idx IN ({marks})", part_idx):
                    payload[changed[idx][0]] = (json.loads(attrs), h, json.loads(matching), json.loads(cleansed), [], ts(idx))
                for idx, key in db.execute(f"SELECT idx, key FROM keys WHERE idx IN ({marks})", part_idx):
                    payload[changed[idx][0]][4].append(key)
            items = list(plan.items())
            for lo in range(0, len(items), 200):
                part = items[lo:lo + 200]
                involved = [e for _root, (survivor, losers) in part for e in (survivor, *losers)]
                by_entity = {}
                for record in store.list_records_of_entities(tenant_id, domain, involved):
                    by_entity.setdefault(record.entity_id, []).append(record)
                staged = store.get_staged_many(tenant_id, domain, [r.id for rs in by_entity.values() for r in rs if r.id not in payload])
                for _root, (survivor, losers) in part:
                    records_of = []
                    for e in (survivor, *losers):
                        for r in by_entity.get(e, []):
                            if r.id in payload:  # a changed record takes part with its NEW content, and is the most recent as of its place in the feed (survivorship goes by updated_at)
                                r = replace(r, attributes=payload[r.id][0], content_hash=payload[r.id][1], version=r.version + 1, updated_at=payload[r.id][5])
                            records_of.append(r)
                    decisions, issues = compute_golden(config, domain, records_of, {r.id: (payload[r.id][3] if r.id in payload else staged[r.id][1]) for r in records_of})
                    entity = found[survivor]
                    values, evs = golden_values(decisions, entity.attributes, survivor, tenant_id, domain, reason)
                    entity.attributes, entity.quality_issues, entity.updated_at, entity.status = values, issues, now, "active"
                    upserts.append(entity)
                    reassign.extend((loser, survivor) for loser in losers)
                    if lineage != "none":
                        events.extend(evs)
            history = [(tenant_id, domain, record_id, version, old_hash, json.loads(old_attrs), now) for record_id, _entity, version, old_hash, old_attrs in changed.values()]
            store.apply_onboarding(tenant_id, domain, reassign, upserts, events, history=history,
                                   record_updates=[(rid_, payload[rid_][0], payload[rid_][1], version + 1, payload[rid_][5]) for rid_, _e, version, _h, _a in changed.values()],
                                   supersede=[v[0] for v in changed.values()],
                                   derived=[(tenant_id, domain, rid_, payload[rid_][2], payload[rid_][3], fingerprint, payload[rid_][4]) for rid_ in payload],
                                   candidates=deferred)
            store.flag_merge_audits(tenant_id, domain, [c.id for c in deferred if c.status == "confirmed" and learned_scorer.audit_sampled(c.id, promoted=c.decided_by == learned_scorer.PROMOTED_BY)])
        seconds["write"] = round(time.time() - t0, 2)
        say(f"E wrote everything in {seconds['write']}s")
        run.counts.update(received=n + n_unchanged, new=n - n_changed, updated=n_changed, unchanged=n_unchanged, golden_rebuilt=n_entities + len(plan))
        summary = run.finish()
        finished = True
        return BulkResult(n, n_entities, n_pairs, auto, review, capped, seconds, summary["run_id"], False, work, **counts)
    except BulkPaused:
        raise  # not a failure: the progress is saved and the store was not touched
    except BaseException:
        if run is not None:
            run.aborted = True
        if wrote:
            store.rollback_bulk(tenant_id, domain, token)  # leave the tenant exactly as it was, so the load can simply be run again
        if run is not None:
            run.finish()
        raise
    finally:
        if pool:
            pool.terminate()
        for con in ("db", "edb"):
            if con in dir():
                try:
                    locals()[con].close()
                except Exception:
                    pass
        # temporary work files always go; a directory the caller gave is kept for a dry run or a pause, and emptied after a completed write
        if own or (finished and write):
            for path in ((work,) if own else (work_db, pairs_db, edges_db, work_db + "-wal", work_db + "-shm", edges_db + "-wal", edges_db + "-shm")):
                shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else (os.remove(path) if os.path.exists(path) else None)
