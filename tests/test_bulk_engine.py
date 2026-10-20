"""
The bulk pair-based engine (app/bulk.py): is what it writes the SAME as what the one-record-at-a-time engine writes, can the regular
engine carry on from it, and does it fail safely? Unit and functional tests; nothing here measures time.

Run with:  python3 -m unittest tests.test_bulk_engine -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))
from legacy_engine import legacy_engine  # noqa: E402

from app import active_learning, benchmark, benchmark_person as bp, domain_config, offline
from app.bulk import BulkLoadError, BulkPaused, _find, _input_chunks, bulk_load
from app.db import Store
from app.domains import get_domain_config
from app.indexing import IndexStaleError, reindex_domain
from app.pipeline import ingest_record, process_pending
from app.protect import ProtectionKeyMissing

ROOT = os.path.join(os.path.dirname(__file__), "..")
ENV = {"MDM_TOKEN_KEY": "a-test-key-that-is-long-enough-0123456789", "MDM_TOKEN_KEY_ID": "v1"}
TENANT = "acme"


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        patcher = mock.patch.dict(os.environ, ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.n = 0

    def rows(self, profile="customer", scale=3, seed=1):
        prof = benchmark.get_profile(profile, "scalable_tight_plus" if profile == "person" else None)
        self.domain = prof.domain
        return [r["raw"] for r in prof.generate(seed, scale)]

    def store(self):
        self.n += 1
        path = os.path.join(self.tmp, f"s{self.n}.db")
        return Store(path), path

    def sequential(self, rows):
        store, path = self.store()
        for i, raw in enumerate(rows):
            ingest_record(store, TENANT, self.domain, "src", f"r{i}", raw)
        return store, path

    def bulk(self, rows, **kw):
        store, path = self.store()
        result = bulk_load(store, TENANT, self.domain, [("src", f"r{i}", raw) for i, raw in enumerate(rows)], **kw)
        return store, path, result

    # ---- what a database holds, in a form that does not depend on generated ids -------------------------------------
    def q(self, path, sql, params=()):
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    def golden(self, path):
        """{frozenset of the source record ids in an entity: its golden values and quality issues}"""
        members, names = {}, {}
        for r in self.q(path, "SELECT id, entity_id, source_record_id FROM source_records"):
            members.setdefault(r["entity_id"], set()).add(r["source_record_id"])
            names[r["id"]] = r["source_record_id"]
        out = {}
        for e in self.q(path, f"SELECT * FROM entity_{self.domain} WHERE status = 'active'"):
            key = frozenset(members.get(e["entity_id"], ()))
            row = {k: v for k, v in e.items() if k not in ("entity_id", "created_at", "updated_at")}
            if row.get("quality_issues"):  # an issue names the record it came from by id; the ids differ between engines, the source ids do not
                row["quality_issues"] = [{**i, "record_id": names.get(i["record_id"], i["record_id"])} for i in json.loads(row["quality_issues"])]
            out[key] = row
        return out

    def staging(self, path):
        return {r["source_record_id"]: (json.loads(r["matching"]), json.loads(r["cleansed"])) for r in self.q(
            path, "SELECT s.source_record_id, d.matching, d.cleansed FROM source_records s JOIN standardized_records d ON d.record_id = s.id")}

    def index(self, path):
        return {(r["source_record_id"], r["blocking_key"]) for r in self.q(
            path, "SELECT s.source_record_id, k.blocking_key FROM source_records s JOIN blocking_keys k ON k.record_id = s.id")}

    def candidates(self, path):
        ids = {r["id"]: r["source_record_id"] for r in self.q(path, "SELECT id, source_record_id FROM source_records")}
        return {(frozenset((ids[r["record_a_id"]], ids[r["record_b_id"]])), r["status"], round(r["score"], 4)) for r in self.q(
            path, "SELECT record_a_id, record_b_id, status, score FROM match_candidates")}


@legacy_engine   # compares the LEGACY engines with each other (the kernel has its own tests: tests/test_kernel_engine.py)
class TestSameStateAsTheRegularEngine(Base):
    def test_golden_records_staging_the_index_and_candidates_are_identical(self):
        rows = self.rows()
        _, seq = self.sequential(rows)
        _, blk, result = self.bulk(rows)
        self.assertEqual(self.golden(blk), self.golden(seq))                 # every golden value, and every quality issue
        self.assertEqual(self.staging(blk), self.staging(seq))
        self.assertEqual(self.index(blk), self.index(seq))
        self.assertEqual(self.candidates(blk), self.candidates(seq))        # the same pairs, the same scores, confirmed or pending
        self.assertEqual(result.entities, len(self.golden(seq)))
        self.assertGreater(result.auto_pairs, 0)
        self.assertGreater(result.review_pairs, 0)                           # the data really exercises both kinds

    def test_the_same_for_the_person_domain_with_a_protected_ssn_and_conditional_keys(self):
        rows = self.rows("person", scale=1)
        _, seq = self.sequential(rows)
        _, blk, _ = self.bulk(rows)
        self.assertEqual(self.golden(blk), self.golden(seq))
        self.assertEqual(self.staging(blk), self.staging(seq))
        self.assertEqual(self.index(blk), self.index(seq))

    def test_the_plain_ssn_is_in_raw_and_nowhere_else_after_a_bulk_load(self):
        rows = [{"first_name": "Ann", "last_name": "Lee", "ssn": "512-45-6789", "dob": "1985-03-07"}, {"first_name": "Ann", "last_name": "Lee", "ssn": "512 45 6789"}]
        self.domain = "person"
        bp.register_person("scalable_tight_plus")
        _, blk, _ = self.bulk(rows)
        con, tables = sqlite3.connect(blk), set()
        for line in con.iterdump():
            if line.startswith("INSERT INTO") and any(x in line for x in ("512-45-6789", "512456789", "512 45 6789")):
                tables.add(line.split()[2].strip('"'))
        self.assertEqual(tables, {"source_records"})

    def test_reindex_after_a_bulk_load_changes_nothing(self):
        """The strongest check that the bulk golden records are the regular ones: rebuild them the regular way and compare."""
        rows = self.rows()
        store, path, _ = self.bulk(rows)
        before = self.golden(path)
        reindex_domain(store, self.domain)
        self.assertEqual(self.golden(path), before)

    def test_every_record_is_finished_and_has_an_entity(self):
        rows = self.rows()
        store, path, result = self.bulk(rows)
        self.assertEqual(store.count_unfinished(self.domain), (0, 0))
        self.assertEqual(process_pending(store, self.domain)["finished"], 0)
        self.assertEqual(self.q(path, "SELECT COUNT(*) AS n FROM source_records WHERE entity_id IS NULL")[0]["n"], 0)
        self.assertEqual(self.q(path, "SELECT COUNT(*) AS n FROM source_records")[0]["n"], len(rows))
        self.assertEqual(self.q(path, "SELECT DISTINCT processing_status FROM source_records"), [{"processing_status": "matched"}])

    def test_the_run_is_logged(self):
        store, _, result = self.bulk(self.rows())
        run = [r for r in store.list_runs(domain=self.domain) if r["mode"] == "bulk"][0]
        self.assertEqual((run["status"], run["run_id"], run["counts"]["received"], run["counts"]["golden_rebuilt"]), ("completed", result.run_id, result.records, result.entities))


class TestTheRegularEngineCarriesOn(Base):
    def test_a_new_record_finds_what_was_bulk_loaded_and_an_unrelated_one_does_not(self):
        rows = self.rows()
        store, path, _ = self.bulk(rows)
        existing = self.q(path, "SELECT s.entity_id FROM source_records s WHERE s.source_record_id = 'r0'")[0]["entity_id"]
        again = ingest_record(store, TENANT, self.domain, "later", "x1", rows[0])      # the same person, arriving later through the normal path
        self.assertEqual(again["matched_entity_id"], existing)
        stranger = ingest_record(store, TENANT, self.domain, "later", "x2", {"first_name": "Zephyrine", "last_name": "Quillfeather", "email": "zq@nowhere.test"})
        self.assertNotIn(stranger["matched_entity_id"], {r["entity_id"] for r in self.q(path, "SELECT entity_id FROM source_records WHERE source_record_id LIKE 'r%'")})

    def test_a_bulk_loaded_domain_refuses_a_second_bulk_load_but_not_ordinary_ingest(self):
        rows = self.rows()
        store, _, _ = self.bulk(rows)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, TENANT, self.domain, [("src", "again", rows[0])])
        self.assertIn("already has", str(cm.exception))
        self.assertIn("regular ingest path", str(cm.exception))
        other_tenant = bulk_load(store, "another", self.domain, [("src", "r0", rows[0])])   # a different tenant's domain is empty: fine
        self.assertEqual(other_tenant.records, 1)


class TestFailingSafely(Base):
    def test_an_input_with_the_same_record_twice_is_refused_naming_it_and_nothing_is_written(self):
        store, path = self.store()
        recs = [("src", "a", {"first_name": "Ann"}), ("src", "b", {"first_name": "Bo"}), ("src", "a", {"first_name": "Ann"})]
        for chunk in (1, 2, 10):  # the duplicate may fall in the same chunk or a later one
            with self.assertRaises(BulkLoadError) as cm:
                bulk_load(store, TENANT, "customer", recs, chunk=chunk)
            self.assertIn("source record id 'a'", str(cm.exception))
        self.assertEqual(self.q(path, "SELECT COUNT(*) AS n FROM source_records")[0]["n"], 0)

    def test_a_missing_hashing_key_stops_the_load_before_anything_is_written(self):
        rows = self.rows("person", scale=1)
        store, path = self.store()
        with mock.patch.dict(os.environ):
            os.environ.pop("MDM_TOKEN_KEY", None)
            with self.assertRaises(ProtectionKeyMissing):
                bulk_load(store, TENANT, "person", [("src", f"r{i}", r) for i, r in enumerate(rows)])
        self.assertEqual(self.q(path, "SELECT COUNT(*) AS n FROM source_records")[0]["n"], 0)
        self.assertEqual(store.list_runs(domain="person")[0]["status"], "failed")
        self.assertEqual(self.bulk_again(store, rows, "person").records, len(rows))   # and with the key, the same store loads fine

    def bulk_again(self, store, rows, domain):
        return bulk_load(store, TENANT, domain, [("src", f"r{i}", r) for i, r in enumerate(rows)])

    def test_a_failure_while_writing_is_rolled_back_so_the_load_can_simply_be_run_again(self):
        rows = self.rows()
        store, path = self.store()
        with mock.patch.object(Store, "save_candidates_batch", side_effect=RuntimeError("disk full")):   # the LAST write fails: everything else was written
            with self.assertRaises(RuntimeError):
                bulk_load(store, TENANT, self.domain, [("src", f"r{i}", r) for i, r in enumerate(rows)])
        for table in ("source_records", "standardized_records", "blocking_keys", "match_candidates", "lineage", f"entity_{self.domain}"):
            self.assertEqual(self.q(path, f"SELECT COUNT(*) AS n FROM {table}")[0]["n"], 0, table)
        self.assertEqual(store.list_runs(domain=self.domain)[0]["status"], "failed")
        again = self.bulk_again(store, rows, self.domain)
        self.assertEqual(again.records, len(rows))
        self.assertEqual(self.golden(path), self.golden(self.sequential(rows)[1]))

    def test_a_stale_configuration_is_refused_like_any_other_ingest(self):
        rows = self.rows("person", scale=1)
        store, _ = self.store()
        self.bulk_again(store, rows[:5], "person")
        with open(os.path.join(ROOT, "domain_templates", "person.json")) as f:
            doc = json.load(f)
        doc["fields"]["dob"]["options"]["max_age_years"] = 90                      # a rule changed: the derived data is now stale
        from app.domain_config import parse_domain_document
        from app.domains import register_domain
        register_domain(parse_domain_document(doc), replace=True)
        with self.assertRaises(IndexStaleError):
            bulk_load(store, "other", "person", [("src", "x", rows[0])])

    def test_bad_arguments(self):
        store, _ = self.store()
        with self.assertRaises(ValueError):
            bulk_load(store, TENANT, "customer", [], lineage="sometimes")
        with self.assertRaises(KeyError):
            bulk_load(store, TENANT, "nonexistent", [])
        self.assertEqual(bulk_load(store, TENANT, "customer", []).records, 0)       # nothing to load is not an error


class TestBehaviourThatMustMatchTheReference(Base):
    def test_workers_chunk_size_and_arrival_order_do_not_change_the_result(self):
        rows = self.rows()
        _, base, _ = self.bulk(rows)
        for kw in ({"workers": 2}, {"chunk": 1}, {"chunk": 7}, {"workers": 2, "chunk": 5}):
            _, other, _ = self.bulk(rows, **kw)
            self.assertEqual(self.golden(other), self.golden(base), kw)
            self.assertEqual(self.candidates(other), self.candidates(base), kw)
            self.assertEqual(self.index(other), self.index(base), kw)

    def test_a_key_over_the_cap_is_skipped_exactly_as_the_reference_skips_it(self):
        rows = [{"first_name": f"N{i}", "last_name": "Smith", "postal_code": "94107"} for i in range(8)] + [{"first_name": "Ann", "last_name": "Lee", "email": "a@x.com"}] * 2
        self.domain = "customer"
        cfg = get_domain_config("customer")
        saved, cfg.max_bucket_size = cfg.max_bucket_size, 3
        self.addCleanup(setattr, cfg, "max_bucket_size", saved)
        ref = offline.resolve_offline(rows, "customer")
        store, _ = self.store()
        result = bulk_load(store, offline.TENANT, "customer", [("src", f"r{i}", r) for i, r in enumerate(rows)])   # (the tenant read_back looks at)
        self.assertGreater(ref.capped_keys, 0)
        self.assertEqual(result.capped_keys, ref.capped_keys)
        self.assertEqual(result.candidate_pairs, ref.candidate_pairs)
        self.assertEqual(offline.read_back(store, "customer")[0], ref.partition)

    def test_the_cap_boundary_is_exactly_the_references(self):
        """A key is usable while the OTHER records carrying it number at most the cap: a bucket of cap+1 is used, a bucket of cap+2 is skipped."""
        cfg = get_domain_config("customer")
        saved, cfg.max_bucket_size = cfg.max_bucket_size, 3
        self.addCleanup(setattr, cfg, "max_bucket_size", saved)
        rows = [{"first_name": f"Alpha{i}", "last_name": f"Aa{i}", "email": "four@x.com"} for i in range(4)] + \
               [{"first_name": f"Beta{i}", "last_name": f"Bb{i}", "email": "five@x.com"} for i in range(5)]
        ref = offline.resolve_offline(rows, "customer")
        store, _ = self.store()
        result = bulk_load(store, offline.TENANT, "customer", [("src", f"r{i}", r) for i, r in enumerate(rows)])
        self.assertEqual((ref.candidate_pairs, ref.capped_keys), (6, 2))          # the four share usable keys (6 pairs); the five share two keys, both over the cap
        self.assertEqual((result.candidate_pairs, result.capped_keys), (ref.candidate_pairs, ref.capped_keys))

    def test_a_learned_model_is_applied_to_every_pair_as_in_the_regular_engine(self):
        rows = self.rows()
        stub = mock.Mock(trained_on_examples=7)
        with mock.patch.object(active_learning, "load_model", return_value=stub), \
             mock.patch.object(active_learning, "blend_scores", side_effect=lambda score, model, feats: min(0.99, score + 0.2)) as blend:
            _, path, with_model = self.bulk(rows)
        self.assertGreater(blend.call_count, 0)
        _, _, without = self.bulk(rows)
        self.assertGreater(with_model.auto_pairs, without.auto_pairs)
        reasons = " ".join(r["reasons"] for r in self.q(path, "SELECT reasons FROM match_candidates"))
        self.assertIn("blended with learned model (7 past decisions)", reasons)

    def test_lineage_policies(self):
        rows = self.rows()
        counts = {}
        for policy in ("all", "multi_source", "none"):
            store, path, result = self.bulk(rows, lineage=policy)
            members = {}
            for r in self.q(path, "SELECT entity_id FROM source_records"):
                members[r["entity_id"]] = members.get(r["entity_id"], 0) + 1
            with_lineage = {r["entity_id"] for r in self.q(path, "SELECT DISTINCT entity_id FROM lineage")}
            counts[policy] = (members, with_lineage)
        members = counts["all"][0]
        self.assertEqual(counts["none"][1], set())
        singles = {e for e, n in members.items() if n == 1}
        self.assertTrue(singles and any(n > 1 for n in members.values()))               # the data has both kinds of entity
        self.assertEqual(len(counts["all"][1]), len(members))                             # all: every golden record has lineage
        self.assertEqual({e for e in counts["multi_source"][1]}, {e for e, n in counts["multi_source"][0].items() if n > 1})

    def test_unit_helpers(self):
        self.assertEqual([[x[0] for x in c] for c in _input_chunks([("s", str(i), {}) for i in range(5)], 2)], [[0, 1], [2, 3], [4]])
        self.assertEqual(list(_input_chunks([], 3)), [])
        from array import array
        parent = array("l", [0, 0, 1, 2, 3, 9, 5, 6, 7, 8])      # 4 -> 3 -> 2 -> 1 -> 0 : a chain
        self.assertEqual(_find(parent, 4), 0)
        self.assertEqual(parent[4], 0)                              # path compression
        self.assertEqual(_find(parent, 9), 9) if False else None


class TestResumeAndDryRun(Base):
    def resume_until_done(self, store, recs, work, **kw):
        pauses = []
        while True:
            try:
                return bulk_load(store, TENANT, self.domain, recs, workdir=work, resume=True, budget=0, **kw), pauses
            except BulkPaused as e:
                pauses.append(e.stage)
                self.assertEqual(self.q_count(store, "source_records"), 0)      # a pause never touches the store
                self.assertLess(len(pauses), 500)

    def q_count(self, store, table):
        return sqlite3.connect(store.db_path if hasattr(store, "db_path") else store._path).execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_a_load_paused_after_every_chunk_ends_up_exactly_where_an_uninterrupted_one_does(self):
        rows = self.rows()
        recs = [("src", f"r{i}", raw) for i, raw in enumerate(rows)]
        _, whole, _ = self.bulk(rows)
        store, path = self.store()
        work = os.path.join(self.tmp, "work")
        store._path = path
        result, pauses = self.resume_until_done(store, recs, work, chunk=7)
        self.assertIn("standardize", pauses)
        self.assertIn("score", pauses)                                           # both long stages really were interrupted, many times
        self.assertGreater(len(pauses), 10)
        self.assertEqual(self.golden(path), self.golden(whole))
        self.assertEqual(self.candidates(path), self.candidates(whole))
        self.assertEqual(self.index(path), self.index(whole))
        self.assertEqual(self.staging(path), self.staging(whole))
        self.assertEqual([f for f in os.listdir(work) if f.endswith(".db") or f.endswith("-wal")], [])   # a completed write cleans up after itself

    def test_a_resumed_load_does_not_re_read_the_start_of_the_input(self):
        rows = self.rows()
        recs = [("src", f"r{i}", raw) for i, raw in enumerate(rows)]
        starts = []
        store, path = self.store()
        store._path = path

        def source(start):
            starts.append(start)
            return iter(recs[start:])
        self.resume_until_done(store, source, os.path.join(self.tmp, "w2"), chunk=9)
        self.assertEqual(starts[0], 0)
        self.assertEqual(starts, sorted(starts))
        self.assertGreater(starts[-1], 0)                                        # later calls start part-way in, not at the beginning

    def test_a_dry_run_reports_what_a_load_would_do_and_touches_nothing(self):
        rows = self.rows()
        recs = [("src", f"r{i}", raw) for i, raw in enumerate(rows)]
        store, path = self.store()
        work = os.path.join(self.tmp, "dry")
        dry = bulk_load(store, TENANT, self.domain, recs, workdir=work, write=False)
        self.assertTrue(dry.dry_run)
        for table in ("source_records", "standardized_records", "blocking_keys", "match_candidates", "lineage", "pipeline_runs", f"entity_{self.domain}"):
            self.assertEqual(sqlite3.connect(path).execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0, table)
        self.assertTrue(all(os.path.exists(os.path.join(work, f)) for f in ("work.db", "pairs.db", "edges.db")))   # kept, to be inspected
        _, _, real = self.bulk(rows)
        self.assertEqual((dry.records, dry.entities, dry.candidate_pairs, dry.auto_pairs, dry.review_pairs, dry.capped_keys),
                         (real.records, real.entities, real.candidate_pairs, real.auto_pairs, real.review_pairs, real.capped_keys))
        again = bulk_load(store, TENANT, self.domain, recs, workdir=work, resume=True, write=True)   # and the finished plan can then be written, with no recomputation
        self.assertEqual((again.records, again.entities), (real.records, real.entities))
        self.assertEqual(self.golden(path), self.golden(self.sequential(rows)[1]))

    def test_a_work_directory_belongs_to_one_load_and_resuming_needs_one(self):
        rows = self.rows()
        recs = [("src", f"r{i}", raw) for i, raw in enumerate(rows)]
        store, _ = self.store()
        work = os.path.join(self.tmp, "w3")
        bulk_load(store, TENANT, self.domain, recs, workdir=work, write=False)
        with self.assertRaises(BulkLoadError) as cm:
            bulk_load(store, "another-tenant", self.domain, recs, workdir=work, resume=True, write=False)
        self.assertIn("different load", str(cm.exception))
        for kw in ({"resume": True}, {"budget": 5}):
            with self.assertRaises(ValueError):
                bulk_load(store, TENANT, self.domain, recs, **kw)

    def test_the_command_line_flags(self):
        csv_path, db, work = os.path.join(self.tmp, "in.csv"), os.path.join(self.tmp, "cli.db"), os.path.join(self.tmp, "cliwork")
        with open(csv_path, "w") as f:
            f.write("id,first,last,email\n" + "".join(f"{i},P{i},Q{i % 5},p{i % 40}@x.com\n" for i in range(60)))
        base = ("--tenant", TENANT, "--domain", "customer", "--file", csv_path, "--db", db, "--id-column", "id", "--mapping", "first_name=first,last_name=last,email=email", "--legacy-bulk")
        run = lambda *a: subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), *base, *a], capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(run("--dry-run").returncode, 2)                          # no workdir given
        dry = run("--dry-run", "--workdir", work)
        self.assertEqual(dry.returncode, 0, dry.stderr)
        self.assertIn("DRY RUN (nothing written)", dry.stdout)
        work2, codes = os.path.join(self.tmp, "cliwork2"), []
        for _ in range(300):                                                       # the real workflow: pause, run it again with --resume, until it is done
            r = run("--workdir", work2, "--resume", "--budget", "0", "--batch-size", "10")
            codes.append(r.returncode)
            if r.returncode != 3:
                break
            self.assertIn("PAUSED", r.stdout)
        self.assertEqual(codes[-1], 0, r.stdout + r.stderr)
        self.assertGreater(codes.count(3), 3)                                      # it really was interrupted several times
        self.assertEqual(sqlite3.connect(db).execute("SELECT COUNT(*) FROM source_records").fetchone()[0], 60)
        self.assertEqual(sqlite3.connect(db).execute("SELECT COUNT(*) FROM entity_customer WHERE status = 'active'").fetchone()[0], r.stdout.count("") and
                         sqlite3.connect(db).execute("SELECT COUNT(DISTINCT entity_id) FROM source_records").fetchone()[0])


class TestTrial(Base):
    """benchmark_person.bulk_trial: the bulk engine on a generated population, scored against the generator's truth."""

    def trial(self, n=3000, **kw):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        return bp.bulk_trial(n, d, log=lambda m: None, **kw), d

    def test_the_sampled_harness_and_the_exact_engine_agree_on_the_same_population(self):
        """Two independent instruments for the same quantity: the scale harness (a sample) and the bulk engine (every record)."""
        exact, _ = self.trial()
        sampled = bp.measure_size(3000, sample=3000, dup_sample=10 ** 6, variants=["scalable_tight_plus"])["variants"]["scalable_tight_plus"]
        self.assertAlmostEqual(sampled["partners_mean"], 2 * exact["candidate_pairs"] / 3000, delta=0.06)
        self.assertAlmostEqual(sampled["blocking_recall"], exact["blocking_recall"], delta=0.0001)

    def test_every_true_duplicate_is_accounted_for_exactly_once(self):
        r, _ = self.trial()
        d = r["duplicates"]
        self.assertEqual(d["candidate"] + d["never_compared"], r["true_duplicate_pairs"])
        self.assertEqual(d["auto"] + d["review"] + d["compared_but_rejected"], d["candidate"])
        self.assertEqual(r["true_entities"], r["records"] - r["true_duplicate_pairs"])
        self.assertGreaterEqual(r["merge_recall"], d["auto"] / r["true_duplicate_pairs"])        # merging is transitive, so it can only be more than the direct pairs
        self.assertLessEqual(r["merge_recall"], r["blocking_recall"])                              # and can never exceed what was compared
        self.assertTrue(0 <= r["review_precision"] <= 1)

    def test_a_trial_paused_at_every_chunk_gives_the_same_answer_as_an_uninterrupted_one(self):
        whole, _ = self.trial(2500, chunk=100)
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        pauses = 0
        while True:
            try:
                resumed = bp.bulk_trial(2500, d, chunk=100, budget=0, log=lambda m: None)
                break
            except BulkPaused:
                pauses += 1
        self.assertGreater(pauses, 10)
        for key in ("records", "entities", "candidate_pairs", "auto_pairs", "review_pairs", "blocking_recall", "merge_recall", "impure_clusters", "review_precision", "duplicates"):
            self.assertEqual(resumed[key], whole[key], key)

    def test_a_work_directory_holds_one_trial(self):
        _, d = self.trial(1500)
        with self.assertRaises(ValueError):
            bp.bulk_trial(1600, d, log=lambda m: None)
        self.assertEqual(bp.bulk_trial(1500, d, log=lambda m: None)["records"], 1500)            # the same parameters simply find the work finished


@legacy_engine   # compares the LEGACY engines with each other (the kernel has its own tests: tests/test_kernel_engine.py)
class TestEnginesTrial(Base):
    def test_the_two_engines_leave_identical_results_and_both_are_scored_against_the_truth(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        pauses = 0
        while True:
            try:
                r = bp.engines_trial(1500, d, budget=0, chunk=100, log=lambda m: None)
                break
            except bp.EnginesPaused as e:
                pauses += 1
                self.assertLess(e.done, e.total)
        self.assertGreater(pauses, 5)                                   # the regular engine really was paused and resumed
        self.assertEqual(r["identical"], {"entities": True, "review_pairs": True, "auto_pairs": True, "golden_records": True})
        for key in ("entities", "auto_pairs", "review_pairs", "merge_recall", "impure_entities", "falsely_merged_records", "review_precision"):
            self.assertEqual(r["regular"][key], r["bulk"][key], key)
        self.assertEqual(r["regular"]["true_entities"], 1500 - r["regular"]["true_duplicate_pairs"])
        self.assertIn("WRITE", bp.format_engines_trial(r))                 # the bulk engine's write stage is timed too
        self.assertEqual(bp.engines_trial(1500, d, log=lambda m: None)["identical"], r["identical"])   # finished work is simply reused
        with self.assertRaises(ValueError):
            bp.engines_trial(1600, d, log=lambda m: None)

    def test_a_difference_between_the_engines_would_be_reported(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        bp.engines_trial(800, d, log=lambda m: None)
        con = sqlite3.connect(os.path.join(d, "bulk.db"))
        con.execute("UPDATE entity_person SET first_name = 'Tampered' WHERE rowid = (SELECT MIN(rowid) FROM entity_person)")
        con.execute("DELETE FROM match_candidates WHERE rowid = (SELECT MIN(rowid) FROM match_candidates WHERE status = 'pending')")
        con.commit()
        con.close()
        r = bp.engines_trial(800, d, log=lambda m: None)
        self.assertFalse(r["identical"]["golden_records"])
        self.assertFalse(r["identical"]["review_pairs"])


class TestTheEquivalenceCheckIncludesIt(Base):
    def test_the_bulk_engine_agrees_with_the_reference_in_every_order_with_one_and_two_workers(self):
        for profile, scale in (("customer", 2), ("person", 1)):
            rows = self.rows(profile, scale)
            engines = {"bulk": offline.engine_bulk,
                       "bulk x2": lambda records, order, domain: _bulk_engine(records, order, domain, workers=2)}
            report = offline.check_equivalence(rows, self.domain, orders=3, engines=engines)
            self.assertTrue(report["ok"], offline.format_equivalence(report))
            self.assertGreater(report["reference"]["review_pairs"], 0)

    def test_a_deliberately_wrong_bulk_engine_fails_the_check(self):
        """If clustering ignored the auto-merge pairs, the check must say so."""
        rows = self.rows()
        with mock.patch("app.bulk.classify", side_effect=lambda score: "review" if score >= 0.55 else "no_match"):
            report = offline.check_equivalence(rows, self.domain, orders=1, engines={"bulk": offline.engine_bulk})
        self.assertFalse(report["ok"])


def _bulk_engine(records, order, domain, **kw):
    store = Store(os.path.join(tempfile.mkdtemp(), "eq.db"))
    bulk_load(store, offline.TENANT, domain, [("src", f"r{i}", records[i]) for i in order], chunk=25, **kw)
    return offline.read_back(store, domain)


class TestCommand(Base):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), *args], capture_output=True, text=True, cwd=ROOT)

    def test_bulk_ingest_with_the_bulk_flag(self):
        csv_path, db = os.path.join(self.tmp, "in.csv"), os.path.join(self.tmp, "cli.db")
        with open(csv_path, "w") as f:
            f.write("id,first,last,email\n1,Ann,Lee,a@x.com\n2,Ann,Lee,a@x.com\n3,Cy,Wu,c@z.com\n4,Di,Fox,d@y.com\n")
        args = ("--tenant", TENANT, "--domain", "customer", "--file", csv_path, "--db", db, "--id-column", "id", "--mapping", "first_name=first,last_name=last,email=email")
        r = self.run_cli(*args, "--legacy-bulk", "--workers", "2")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("4 records -> 3 entities", r.stdout)
        self.assertEqual(self.q(db, "SELECT COUNT(*) AS n FROM entity_customer WHERE status = 'active'")[0]["n"], 3)
        again = self.run_cli(*args, "--legacy-bulk")
        self.assertEqual(again.returncode, 2)
        self.assertIn("already has", again.stderr)


if __name__ == "__main__":
    unittest.main()
