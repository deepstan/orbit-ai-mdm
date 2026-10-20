"""
LOADS ARE REPRODUCIBLE: the same records in the same order give the same groups, the same golden records and the same pairs (down to which member a pair points to), whatever the random ids
the run happened to draw and whatever the process's hash seed.

Found while writing the key filter tests: two identical loads could differ, because a TIE between equally good candidates (two groups with the same score, two members equally close) was broken
by an id, and ids are random. It was rare (7 pairs in 317, and in a few cases which group a record joined) and it only happens on genuine ties, but it made the engine impossible to compare with
itself, and Gate S (many workers) can only be judged against a sequential run that is reproducible.

Ties are now broken by what a record IS (its source system and source record id), never by an id the run made up.

  * the same load with DIFFERENT random id sequences: identical, in a batch, one record at a time, and with a limit on the candidates scored (which cuts ties across the cut-off)
  * the same load in SEPARATE PROCESSES with different hash seeds, through the real loader
  * the two small decisions inside a group: the closest member, and the lead member that supplies the group's comparison form
Run with:  python3 -m unittest tests.test_determinism -v
"""
import csv
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(__file__))

from app import domain_config, domains, kernel  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import SourceRecord  # noqa: E402
from app.pipeline import ingest_record, run_batch  # noqa: E402
from test_key_filter import FIELDS, TOKEN_KEY, TENANT, DOMAIN, people, snapshot  # noqa: E402


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._registry = dict(domains.REGISTRY)
        cls._env = mock.patch.dict(os.environ, {"MDM_TOKEN_KEY": TOKEN_KEY, "MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel",
                                                "MDM_DOMAINS_DIR": os.path.join(ROOT, "domain_templates")})
        cls._env.start()
        domain_config.reset()
        domain_config.load_configured_domains()

    @classmethod
    def tearDownClass(cls):
        domain_config.reset()
        domains.REGISTRY.clear()
        domains.REGISTRY.update(cls._registry)
        cls._env.stop()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.n = 0

    def store(self):
        self.n += 1
        return Store(os.path.join(self.tmp, f"s{self.n}.db"))


def with_random_ids(seed):
    """Ids as random as the real ones, but a different sequence for each seed."""
    rng = random.Random(seed)
    return mock.patch("app.models.new_id", lambda prefix="": f"{prefix}{rng.getrandbits(128):032x}")


class TestTheSameLoadWithDifferentRandomIds(Base):
    def load(self, records, seed, one_at_a_time=False):
        with with_random_ids(seed):
            store = self.store()
            if one_at_a_time:
                for ss, sid, attrs in records:
                    ingest_record(store, TENANT, DOMAIN, ss, sid, attrs)
            else:
                for i in range(0, len(records), 100):
                    run_batch(store, TENANT, DOMAIN, records[i:i + 100])
            return snapshot(store)

    def assertAllSame(self, snaps):
        first = snaps[0]
        for i, other in enumerate(snaps[1:], 1):
            self.assertEqual(first["groups"], other["groups"], f"groups differ between id sequence 0 and {i}")
            self.assertEqual(first["golden"], other["golden"], f"golden records differ between id sequence 0 and {i}")
            self.assertEqual(first["pairs"], other["pairs"], f"pairs (which two records, status, type, score) differ between id sequence 0 and {i}")

    def test_a_batch_load_gives_the_same_groups_golden_records_and_pairs(self):
        records = people(600, seed=5)                                                                # the data in which a tie was found: a record joined one of two equally good groups
        self.assertAllSame([self.load(records, seed) for seed in (1, 2, 3, 4)])

    def test_loading_one_record_at_a_time_does_too(self):
        records = people(250, seed=5)
        self.assertAllSame([self.load(records, seed, one_at_a_time=True) for seed in (1, 2, 3)])

    def test_with_a_limit_on_the_candidates_scored_so_ties_across_the_cut_off_are_broken_the_same_way(self):
        records = people(500, seed=5)
        with mock.patch.dict(os.environ, {"MDM_MAX_CANDIDATES": "2"}):
            self.assertAllSame([self.load(records, seed) for seed in (1, 2, 3)])


class TestSeparateProcessesWithDifferentHashSeeds(Base):
    def test_the_real_loader_gives_exactly_the_same_result_whatever_the_hash_seed(self):
        csv_path = os.path.join(self.tmp, "people.csv")
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["PersonID"] + FIELDS)
            for _ss, sid, attrs in people(400, seed=5):
                w.writerow([sid] + [attrs.get(k, "") for k in FIELDS])
        mapping = ",".join(f"{k}={k}" for k in FIELDS)
        results = []
        for hash_seed in ("1", "2", "3"):
            db = os.path.join(self.tmp, f"h{hash_seed}.db")
            env = {**os.environ, "PYTHONHASHSEED": hash_seed, "MDM_TOKEN_KEY": TOKEN_KEY, "MDM_AUDIT_RATE": "0", "MDM_DOMAINS_DIR": os.path.join(ROOT, "domain_templates"), "MDM_ENGINE": "kernel"}
            done = subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--tenant", "people", "--domain", "person", "--file", csv_path, "--mapping", mapping, "--id-column",
                                   "PersonID", "--source-system", "crm", "--bulk", "--db", db], capture_output=True, text=True, env=env, timeout=180)
            self.assertEqual(done.returncode, 0, done.stderr)
            c = sqlite3.connect(db)
            natural = dict(c.execute("SELECT id, source_record_id FROM source_records"))
            members = {}
            for ent, sid in c.execute("SELECT entity_id, source_record_id FROM source_records"):
                members.setdefault(ent, set()).add(sid)
            edges = sorted((tuple(sorted((natural[a], natural[b]))), st, mt, round(sc, 6)) for a, b, st, mt, sc in c.execute("SELECT record_a_id, record_b_id, status, match_type, score FROM match_candidates"))
            results.append((sorted(sorted(m) for m in members.values()), edges))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], results[2])


class TestTheSmallDecisionsInsideAGroup(Base):
    def record(self, rid, source_record_id, ingested_at="2026-01-01T00:00:00"):
        return SourceRecord(source_system="crm", source_record_id=source_record_id, attributes={"first_name": "Ann"}, id=rid, tenant_id=TENANT, domain=DOMAIN, ingested_at=ingested_at)

    def test_the_closest_member_of_two_equally_close_ones_is_chosen_by_what_it_is_not_by_its_id(self):
        from app import kernel_router
        config = domains.get_domain_config(DOMAIN)
        std = {"first_name": "ann", "last_name": "lee", "email": "ann@example.com"}
        group = kernel.GroupView("g", ["zzz_first_by_id", "aaa_last_by_id"], dict(std), {})
        matching = {"zzz_first_by_id": dict(std), "aaa_last_by_id": dict(std)}                      # both exactly as close as each other
        natural = {"zzz_first_by_id": ("crm", "A-1"), "aaa_last_by_id": ("crm", "B-2")}            # but the one with the LARGER id is the one that arrived first
        self.assertEqual(kernel_router._closest_member(config, std, group, matching, order=natural), "zzz_first_by_id")
        natural = {"zzz_first_by_id": ("crm", "B-2"), "aaa_last_by_id": ("crm", "A-1")}
        self.assertEqual(kernel_router._closest_member(config, std, group, matching, order=natural), "aaa_last_by_id")

    def test_the_lead_member_of_a_group_is_decided_by_what_it_is_not_by_its_id(self):
        config = domains.get_domain_config(DOMAIN)
        for first, second in (("rec_aaa", "rec_zzz"), ("rec_zzz", "rec_aaa")):                       # the same two records with their ids swapped
            a, b = self.record(first, "A-1"), self.record(second, "B-2")
            std = {first: {"first_name": "ann"}, second: {"first_name": "ann"}}
            view = kernel.build_group_view(config, DOMAIN, "g", [a, b], std, {first: {}, second: {}})
            self.assertEqual(view.std.get("first_name"), "ann")
            lead_id = max([a, b], key=lambda r: (0, "", (r.source_system, r.source_record_id))).id      # what the rule should pick: the larger natural key
            self.assertIn(lead_id, (first, second))
        a, b = self.record("rec_zzz", "A-1"), self.record("rec_aaa", "B-2")
        marked = {"rec_zzz": {"first_name": "from-A-1"}, "rec_aaa": {"first_name": "from-B-2"}}
        one = kernel.build_group_view(config, DOMAIN, "g", [a, b], marked, {"rec_zzz": {}, "rec_aaa": {}})
        a2, b2 = self.record("rec_aaa", "A-1"), self.record("rec_zzz", "B-2")                       # the same two records with the ids swapped
        marked2 = {"rec_aaa": {"first_name": "from-A-1"}, "rec_zzz": {"first_name": "from-B-2"}}
        two = kernel.build_group_view(config, DOMAIN, "g", [a2, b2], marked2, {"rec_aaa": {}, "rec_zzz": {}})
        self.assertEqual(one.std.get("first_name"), two.std.get("first_name"))                       # the same record leads, whichever ids they were given


class TestTheOtherTieBreaks(Base):
    """The paths the loads above only reach by luck: each is exercised directly."""

    def test_plans_with_equal_scores_are_ordered_by_what_their_records_are_not_by_the_group_id(self):
        from types import SimpleNamespace as P
        from app import kernel_router
        plans = [P(res=P(score=0.80), skey=("crm", "B"), gid="ent_z"), P(res=P(score=0.80), skey=("crm", "C"), gid="ent_a"), P(res=P(score=0.80 + 1e-12), skey=("crm", "A"), gid="ent_m"),
                 P(res=P(score=0.95), skey=("crm", "Z"), gid="ent_q")]
        self.assertEqual([p.gid for p in sorted(plans, key=kernel_router._plan_order)], ["ent_q", "ent_m", "ent_z", "ent_a"])   # 0.95 first; then the three that tie (to a billionth) by natural key A, B, C

    def test_both_the_join_choice_and_the_review_order_use_that_ordering(self):
        import inspect
        from app import kernel_router
        source = inspect.getsource(kernel_router.route_record)
        self.assertEqual(source.count("key=_plan_order"), 2, "the groups to join and the groups to review must both be ordered by _plan_order")

    def test_a_tie_across_the_candidate_cut_off_keeps_the_group_whose_records_come_first_by_name(self):
        from types import SimpleNamespace as R
        from app import kernel_router

        class Store:
            def __init__(self):
                self.asked = 0
                self.members = {"ent_z": ("crm", "A"), "ent_a": ("crm", "C"), "ent_m": ("crm", "B")}

            def list_records_of_entities(self, tenant, domain, gids):
                self.asked += 1
                return [R(entity_id=g, source_system=self.members[g][0], source_record_id=self.members[g][1]) for g in gids]

            def get_source_records_batch(self, tenant, domain, rids):
                self.asked += 1
                return {}
        weight = {"ent_z": 1.0, "ent_a": 1.0, "ent_m": 1.0}
        ordered = sorted(weight, key=lambda g: (-weight[g], g))                                        # by id: ent_a, ent_m, ent_z
        store = Store()
        self.assertEqual(kernel_router._best_n(store, "t", "d", ordered, weight, 1), ["ent_z"])        # not ent_a, the smallest id: ent_z holds the record named A
        self.assertEqual(kernel_router._best_n(store, "t", "d", ordered, weight, 2), ["ent_z", "ent_m"])
        before = store.asked
        clear = {"ent_z": 3.0, "ent_a": 2.0, "ent_m": 1.0}
        self.assertEqual(kernel_router._best_n(store, "t", "d", sorted(clear, key=lambda g: -clear[g]), clear, 2), ["ent_z", "ent_a"])
        self.assertEqual(store.asked, before, "no tie, so no need to read anything")

    def test_relinking_the_pieces_of_an_entity_anchors_on_the_record_that_comes_first_by_name_whatever_the_ids(self):
        from app import kernel_router
        config = domains.get_domain_config(DOMAIN)
        anchors = set()
        for seed in range(1, 7):
            with with_random_ids(seed):
                store = self.store()
                rids = {}
                for sid, first in (("P-1", "Ann"), ("P-2", "Bea"), ("P-3", "Cleo")):
                    rids[sid] = ingest_record(store, TENANT, DOMAIN, "crm", sid, {"first_name": first, "last_name": "Zulu", "email": f"{first.lower()}@x.example"})["record_id"]
                entity = store.get_record_entities(TENANT, DOMAIN, [rids["P-1"]])[rids["P-1"]]
                for sid in ("P-2", "P-3"):
                    store.set_record_entity(rids[sid], TENANT, DOMAIN, entity)                         # one entity of three records that no pair connects
                self.assertEqual(kernel_router._relink(store, TENANT, DOMAIN, config, entity), 2)
                natural = {v: k for k, v in rids.items()}
                links = [c for c in store.list_candidates(TENANT, DOMAIN, status=None) if c.match_type == "relink"]
                anchors.add(frozenset(natural[c.record_b_id] for c in links))
        self.assertEqual(anchors, {frozenset({"P-1"})}, "the anchor must be the record that comes first by name, in every run")


    def test_a_piece_of_several_records_is_represented_by_its_first_member_by_name_whatever_the_ids(self):
        from app import kernel_router
        from app.models import MatchCandidate, now_iso
        config = domains.get_domain_config(DOMAIN)
        seen = set()
        for seed in range(1, 9):
            with with_random_ids(seed):
                store = self.store()
                rids = {}
                for sid, first in (("P-1", "Ann"), ("P-2", "Bea"), ("P-3", "Cleo"), ("P-4", "Dina")):
                    rids[sid] = ingest_record(store, TENANT, DOMAIN, "crm", sid, {"first_name": first, "last_name": "Zulu", "email": f"{first.lower()}@x.example"})["record_id"]
                entity = store.get_record_entities(TENANT, DOMAIN, [rids["P-1"]])[rids["P-1"]]
                for sid in ("P-2", "P-3", "P-4"):
                    store.set_record_entity(rids[sid], TENANT, DOMAIN, entity)
                for a, b in (("P-1", "P-2"), ("P-3", "P-4")):                                           # two pieces of two records each
                    store.save_candidate(MatchCandidate(record_a_id=rids[a], record_b_id=rids[b], tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="deterministic", status="confirmed",
                                                        decided_at=now_iso(), decided_by="system:test"))
                self.assertEqual(kernel_router._relink(store, TENANT, DOMAIN, config, entity), 1)
                natural = {v: k for k, v in rids.items()}
                links = [c for c in store.list_candidates(TENANT, DOMAIN, status=None) if c.match_type == "relink"]
                seen.add(frozenset((natural[c.record_a_id], natural[c.record_b_id]) for c in links))
        self.assertEqual(seen, {frozenset({("P-3", "P-1")})}, "the other piece is joined to the first piece through the first member of each, by name, in every run")


if __name__ == "__main__":
    unittest.main()
