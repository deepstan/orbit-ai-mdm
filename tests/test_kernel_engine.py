"""
The kernel as the application's matching engine (app/kernel.py, app/kernel_router.py): what it decides, how it writes the decision in the product's tables, and what it must never do.

These test the ENGINE THROUGH THE REAL ENTRY POINTS (pipeline.ingest_record, run_batch, the loader), against a real SQLite store; nothing is mocked. The prototype and its benchmark
are in experiments/; the measurements behind each rule are in docs/kernel-design.md.
"""
import csv
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

from app import kernel, kernel_router, pipeline  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import MatchCandidate, now_iso  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate, run_batch  # noqa: E402

TENANT, DOMAIN = "acme", "customer"
ANN = {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "phone": "404-555-0188", "address_line1": "9 Elm Street", "city": "Atlanta", "state": "GA", "postal_code": "30301"}
BOB = {"first_name": "Bob", "last_name": "Cole", "email": "bob@y.com", "phone": "678-555-0142", "address_line1": "4 Oak Road", "city": "Macon", "state": "GA", "postal_code": "31201"}


class Kernel(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel"})
        env.start()
        self.addCleanup(env.stop)
        for name in ("MDM_MERGE_GROUPS", "MDM_MAX_CANDIDATES", "MDM_KERNEL_SCORER", "MDM_REVIEW_CAP", "MDM_RANK"):
            os.environ.pop(name, None)
        self.path = os.path.join(self.tmp, "t.db")
        self.store = Store(self.path)

    # ---- helpers
    def ingest(self, name, attrs, store=None):
        return ingest_record(store or self.store, TENANT, DOMAIN, "src", name, attrs)

    def rec(self, name):
        return next(r for r in self.store.list_source_records(TENANT, DOMAIN) if r.source_record_id == name)

    def entity(self, name):
        return self.rec(name).entity_id

    def together(self, *names):
        return len({self.entity(n) for n in names}) == 1

    def active_entities(self):
        return [e for e in self.store.list_entities(TENANT, DOMAIN, "active")]

    def pairs(self, status):
        return self.store.list_candidates(TENANT, DOMAIN, status=status)

    def connected(self, entity_id):
        """Are the entity's members ONE piece when only its confirmed pairs are followed? (what unmerge relies on)"""
        ids = [r.id for r in self.store.list_source_records(TENANT, DOMAIN, entity_id=entity_id)]
        parent = {i: i for i in ids}

        def find(x):
            while parent[x] != x:
                x = parent[x]
            return x
        for _c, a, b in self.store.confirmed_pairs_among(TENANT, DOMAIN, ids):
            parent[find(a)] = find(b)
        return len({find(i) for i in ids}) == 1


class TestJoiningAGroup(Kernel):
    def test_a_record_joins_the_group_it_matches_and_each_join_is_ONE_confirmed_pair(self):
        for name in ("A", "B", "C", "D"):
            self.ingest(name, ANN)
        self.assertTrue(self.together("A", "B", "C", "D"))
        confirmed = self.pairs("confirmed")
        self.assertEqual(len(confirmed), 3)                                       # four records, a TREE of three pairs: not six (every pair)
        self.assertTrue(all(c.decided_by == "system:auto_merge" for c in confirmed))
        self.assertTrue(self.connected(self.entity("A")))

    def test_a_record_that_matches_nothing_is_its_own_entity(self):
        self.ingest("A", ANN)
        self.ingest("B", BOB)
        self.assertFalse(self.together("A", "B"))
        self.assertEqual((len(self.active_entities()), len(self.pairs("confirmed")), len(self.pairs("pending"))), (2, 0, 0))

    def test_a_possible_match_is_a_pending_pair_for_a_person_and_the_record_is_still_an_entity_of_its_own(self):
        self.ingest("A", ANN)
        r = self.ingest("B", {**ANN, "email": "other@z.com", "phone": "", "last_name": "Leigh"})
        pending = self.pairs("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(r["queued_for_review"][0]["candidate_id"], pending[0].id)
        self.assertEqual(r["auto_merged_with"], [])
        self.assertFalse(self.together("A", "B"))
        resolve_candidate(self.store, TENANT, DOMAIN, pending[0].id, "confirm", "jane")      # a steward's confirm works exactly as before
        self.assertTrue(self.together("A", "B"))

    def test_the_result_has_the_shape_the_api_returns(self):
        self.ingest("A", ANN)
        r = self.ingest("B", ANN)
        self.assertEqual(set(r), {"record_id", "auto_merged_with", "queued_for_review", "matched_entity_id", "skipped_overpopulated_keys"})
        self.assertEqual(r["matched_entity_id"], self.entity("A"))
        self.assertEqual(list(r["auto_merged_with"][0]), ["record_id", "score"])

    def test_the_oldest_entity_keeps_its_id_when_a_record_joins_it(self):
        self.ingest("A", ANN)
        first = self.entity("A")
        self.ingest("B", ANN)
        self.assertEqual(self.entity("B"), first)


class TestWhatTheScorerKnows(Kernel):
    CHANGED = {**ANN, "last_name": "Cole"}                 # a different surname, the same two identifiers and first name: a marriage, not a stranger

    def test_a_changed_surname_does_not_make_a_different_person_when_two_identifiers_and_the_first_name_agree(self):
        self.ingest("A", ANN)
        r = self.ingest("B", self.CHANGED)
        self.assertTrue(self.together("A", "B"))
        self.assertTrue(any("changed name" in x for x in self.pairs("confirmed")[0].reasons))

    def test_the_plain_scorer_would_not_have_(self):
        with mock.patch.dict(os.environ, {"MDM_KERNEL_SCORER": "frequency"}):
            self.ingest("A", ANN)
            self.ingest("B", self.CHANGED)
        self.assertFalse(self.together("A", "B"))

    def test_two_different_people_sharing_an_email_and_a_phone_are_not_merged(self):
        self.ingest("A", ANN)
        self.ingest("B", {**ANN, "first_name": "Bob", "last_name": "Cole"})
        self.assertFalse(self.together("A", "B"))

    def test_an_identifier_many_groups_hold_does_not_identify_anyone(self):
        """A record that is ONLY a phone (no name to disagree with) is auto-merged into the one group that holds the phone, but not when the phone is held by THREE OTHER groups
        besides the one being scored (the measured setting: four holders in all): the phone does not say which."""
        shared, single = "404-555-0100", "678-555-0199"
        for i, n in enumerate(("Al", "Bea", "Cy", "Di")):
            self.ingest(f"H{i}", {"first_name": n, "last_name": f"Name{i}", "email": f"{n.lower()}@h.com", "phone": shared, "city": "Atlanta", "state": "GA", "postal_code": "30301"})
        self.ingest("S", {"first_name": "Sol", "last_name": "Solo", "email": "sol@h.com", "phone": single, "city": "Atlanta", "state": "GA", "postal_code": "30301"})
        held_by_one = self.ingest("P1", {"phone": single})
        self.assertEqual(len(held_by_one["auto_merged_with"]), 1)                   # one holder: it identifies
        trio = "770-555-0123"
        for i, n in enumerate(("Ed", "Flo", "Gus")):
            self.ingest(f"T{i}", {"first_name": n, "last_name": f"Trio{i}", "email": f"{n.lower()}@t.com", "phone": trio, "city": "Atlanta", "state": "GA", "postal_code": "30301"})
        self.assertEqual(len(self.ingest("P2", {"phone": trio})["auto_merged_with"]), 1)   # three holders (two besides the one scored): still identifies; the line is at four
        held_by_many = self.ingest("P3", {"phone": shared})
        self.assertEqual(held_by_many["auto_merged_with"], [])                      # four holders: it does not
        self.assertTrue(held_by_many["queued_for_review"])                         # ... and the record goes to a person instead of to one of them at random

    def test_the_default_scorer_is_the_corroborated_one_and_a_bad_setting_is_refused(self):
        self.assertEqual(kernel.settings().scorer, "corroborated")
        with mock.patch.dict(os.environ, {"MDM_KERNEL_SCORER": "psychic"}), self.assertRaises(ValueError):
            kernel.settings()
        with mock.patch.dict(os.environ, {"MDM_MERGE_GROUPS": "sometimes"}), self.assertRaises(ValueError):
            kernel.settings()
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "both"}), self.assertRaises(ValueError):
            kernel.engine()


class TestTwoGroupsThatTurnOutToBeOne(Kernel):
    """One person with two emails and two phones: the middle record matches two groups that do not match each other."""
    BASE = {"first_name": "Dee", "last_name": "Park", "city": "Atlanta", "state": "GA", "postal_code": "30301"}
    R1 = {**BASE, "email": "dee@a.com", "phone": "404-555-0101", "address_line1": "9 Elm Street"}
    R3 = {**BASE, "email": "dee@b.com", "phone": "678-555-0202"}
    R2 = {**BASE, "email": "dee@a.com", "phone": "678-555-0202", "address_line1": "9 Elm Street"}

    def person(self):
        for name, raw in (("R1", self.R1), ("R3", self.R3), ("R2", self.R2)):
            self.ingest(name, raw)

    def test_auto_merges_the_two_groups_as_senzing_and_reltio_do_and_the_older_entity_survives(self):
        self.person()
        self.assertTrue(self.together("R1", "R2", "R3"))
        self.assertEqual(len(self.active_entities()), 1)
        self.assertTrue(self.connected(self.entity("R1")))
        self.assertIn("group_merge", {c.match_type for c in self.pairs("confirmed")})

    def test_propose_merges_nothing_and_queues_a_review_that_says_why(self):
        with mock.patch.dict(os.environ, {"MDM_MERGE_GROUPS": "propose"}):
            self.person()
        self.assertEqual(len(self.active_entities()), 2)
        self.assertTrue(any("TWO groups" in " ".join(c.reasons) for c in self.pairs("pending")))

    def test_off_merges_nothing_and_proposes_nothing(self):
        with mock.patch.dict(os.environ, {"MDM_MERGE_GROUPS": "off"}):
            self.person()
        self.assertEqual(len(self.active_entities()), 2)
        self.assertFalse(any("TWO groups" in " ".join(c.reasons) for c in self.pairs("pending")))   # (the one pending item is R3 against R1: a plain possible match)

    def test_the_merge_can_be_unmerged_like_any_other(self):
        from app.unmerge import unmerge_pair
        self.person()
        edge = next(c for c in self.pairs("confirmed") if c.match_type == "group_merge")
        unmerge_pair(self.store, TENANT, DOMAIN, edge.id, "jane")
        self.assertEqual(len(self.active_entities()), 2)


class TestAChangedRecordIsMatchedAgain(Kernel):
    """The policy of Senzing and Reltio, chosen by the owner: a changed record is matched like a new one, so it can leave its group, unless a person tied it there."""

    def two(self):
        self.ingest("A", ANN)
        self.ingest("B", ANN)
        return self.entity("A")

    def test_a_change_that_still_matches_stays_and_is_not_reported_as_a_merge(self):
        home = self.two()
        r = self.ingest("B", {**ANN, "city": "Decatur"})
        self.assertEqual((self.entity("B"), r["matched_entity_id"]), (home, home))
        self.assertEqual(r["auto_merged_with"], [])
        self.assertTrue(r["stayed"])
        self.assertEqual(self.rec("B").version, 2)

    def test_a_change_into_someone_else_leaves_the_group_and_both_golden_records_are_right(self):
        home = self.two()
        self.ingest("B", BOB)
        self.assertNotEqual(self.entity("B"), home)
        self.assertEqual(self.entity("A"), home)
        self.assertEqual(self.store.get_entity(home, TENANT, DOMAIN).attributes["last_name"], "Lee")
        self.assertEqual(self.store.get_entity(self.entity("B"), TENANT, DOMAIN).attributes["last_name"], "Cole")
        self.assertEqual(sorted(len(self.store.list_source_records(TENANT, DOMAIN, entity_id=e.id)) for e in self.active_entities()), [1, 1])
        self.assertEqual(self.pairs("confirmed"), [])                                  # the pair that held them was superseded, not left standing

    def test_a_change_into_a_member_of_another_group_moves_it_there_without_merging_the_two_groups(self):
        self.two()
        self.ingest("C", BOB)
        self.ingest("D", BOB)
        before = {self.entity("A"), self.entity("C")}
        self.ingest("B", BOB)
        self.assertTrue(self.together("B", "C", "D"))
        self.assertFalse(self.together("A", "B"))
        self.assertEqual({self.entity("A"), self.entity("C")}, before)                 # nobody's entity id changed, nothing was merged

    def test_the_members_who_stay_are_kept_connected_when_the_one_that_joined_them_leaves(self):
        for name in ("A", "B", "C"):
            self.ingest(name, ANN)
        home = self.entity("A")
        degree = {}
        for _c, a, b in self.store.confirmed_pairs_among(TENANT, DOMAIN, [self.rec(n).id for n in "ABC"]):
            degree[a], degree[b] = degree.get(a, 0) + 1, degree.get(b, 0) + 1
        hub = next(n for n in "ABC" if degree[self.rec(n).id] == 2)                 # in a tree of three, one record joins the other two
        self.ingest(hub, BOB)
        rest = [n for n in "ABC" if n != hub]
        self.assertTrue(self.together(*rest))
        self.assertTrue(self.connected(self.entity(rest[0])))                          # still ONE piece, or unmerge would be wrong
        self.assertEqual(self.entity(rest[0]), home)

    def test_a_record_a_person_tied_to_its_group_never_leaves_it(self):
        self.ingest("A", ANN)
        self.ingest("B", {**ANN, "email": "other@z.com", "phone": "", "last_name": "Leigh"})        # a possible match: pending
        resolve_candidate(self.store, TENANT, DOMAIN, self.pairs("pending")[0].id, "confirm", "jane")
        home = self.entity("A")
        self.ingest("B", BOB)                                                              # its data now says it is someone else; a person said it is the same
        self.assertEqual(self.entity("B"), home)

    def test_rematching_a_record_is_idempotent(self):
        self.two()
        self.ingest("B", BOB)
        snapshot = ({n: self.entity(n) for n in "AB"}, len(self.pairs("confirmed")), len(self.pairs("pending")))
        self.ingest("B", BOB)                                                              # the same payload again: nothing happens
        self.assertEqual(({n: self.entity(n) for n in "AB"}, len(self.pairs("confirmed")), len(self.pairs("pending"))), snapshot)


class TestEveryWayInIsTheSameEngine(Kernel):
    @staticmethod
    def sample(n=120):
        with open(os.path.join(ROOT, "samples", "sample_customers_200.csv"), newline="") as f:
            rows = list(csv.DictReader(f))[:n]
        m = {"first_name": "First Name", "last_name": "Last Name", "email": "Email", "phone": "Phone", "address_line1": "Address", "city": "City", "state": "State", "postal_code": "Zip"}
        return [(r["CustomerID"], {k: r[v] for k, v in m.items()}) for r in rows]

    def groups(self, store):
        by = {}
        for r in store.list_source_records(TENANT, DOMAIN):
            by.setdefault(r.entity_id, set()).add(r.source_record_id)
        return {frozenset(v) for v in by.values()}

    def load_one_at_a_time(self, rows, store):
        for sid, a in rows:
            ingest_record(store, TENANT, DOMAIN, "src", sid, a)

    def test_realtime_and_batch_make_the_same_groups_from_the_same_records_in_the_same_order(self):
        rows = self.sample()
        self.load_one_at_a_time(rows, self.store)
        other = Store(os.path.join(self.tmp, "batch.db"))
        run_batch(other, TENANT, DOMAIN, [("src", sid, a) for sid, a in rows])
        self.assertEqual(self.groups(self.store), self.groups(other))

    def test_a_record_always_has_exactly_one_entity_and_no_entity_is_empty_in_any_order(self):
        rows = self.sample()
        for seed in (1, 2, 3):
            shuffled = rows[:]
            random.Random(seed).shuffle(shuffled)
            store = Store(os.path.join(self.tmp, f"o{seed}.db"))
            self.load_one_at_a_time(shuffled, store)
            records = store.list_source_records(TENANT, DOMAIN)
            self.assertEqual(len(records), len(rows))
            self.assertTrue(all(r.entity_id for r in records), seed)
            active = {e.id for e in store.list_entities(TENANT, DOMAIN, "active")}
            self.assertEqual({r.entity_id for r in records}, active, seed)             # every active entity has records, and every record is in an active entity

    def test_the_same_file_twice_changes_nothing(self):
        rows = self.sample(60)
        self.load_one_at_a_time(rows, self.store)
        before = (self.groups(self.store), len(self.pairs("confirmed")), len(self.pairs("pending")))
        self.load_one_at_a_time(rows, self.store)
        self.assertEqual((self.groups(self.store), len(self.pairs("confirmed")), len(self.pairs("pending"))), before)

    def test_the_sample_gives_the_result_the_legacy_engine_gave_for_the_whole_file(self):
        """The runbook's reference load. The kernel and the legacy engine agree on this file: 200 customers -> 165 entities, 35 automatic merges, 1 review item."""
        rows = self.sample(200)
        self.load_one_at_a_time(rows, self.store)
        self.assertEqual((len(self.active_entities()), len(self.pairs("confirmed")), len(self.pairs("pending"))), (165, 35, 1))


class TestTheEngineSwitch(Kernel):
    def test_the_kernel_is_the_default_and_legacy_is_one_setting_away(self):
        os.environ.pop("MDM_ENGINE", None)
        self.assertEqual(kernel.engine(), "kernel")
        with mock.patch.object(kernel_router, "route_record", wraps=kernel_router.route_record) as routed:
            self.ingest("A", ANN)
        self.assertEqual(routed.call_count, 1)
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"}), mock.patch.object(kernel_router, "route_record") as routed:
            self.ingest("B", BOB)
        routed.assert_not_called()

    def test_data_made_by_either_engine_can_be_continued_by_the_other(self):
        """Nothing about a group is stored apart from the product's own tables, so a rollback loses nothing and needs no rebuild."""
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"}):
            self.ingest("A", ANN)
        self.ingest("B", ANN)                                                              # the kernel continues what the legacy engine began
        self.assertEqual(self.rec("A").entity_id, self.rec("B").entity_id)
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"}):
            self.ingest("C", ANN)                                                          # and back
        self.assertEqual(len({r.entity_id for r in self.store.list_source_records(TENANT, DOMAIN)}), 1)


class TestTheLoader(Kernel):
    def run_cli(self, *args, db=None):
        env = {**os.environ, "MDM_AUDIT_RATE": "0"}
        env.pop("MDM_TOKEN_KEY", None)
        return subprocess.run([sys.executable, os.path.join(ROOT, "bulk_ingest.py"), "--db", db or self.path, "--domain", DOMAIN, "--id-column", "CustomerID", "--source-system", "crm",
                               "--mapping", "first_name=First Name,last_name=Last Name,email=Email,phone=Phone,address_line1=Address,city=City,state=State,postal_code=Zip", *args],
                              capture_output=True, text=True, cwd=ROOT, env=env)

    def sample_file(self, n, start=0):
        path = os.path.join(self.tmp, f"s{start}_{n}.csv")
        with open(os.path.join(ROOT, "samples", "sample_customers_200.csv"), newline="") as f:
            rows = list(csv.reader(f))
        with open(path, "w", newline="") as out:
            csv.writer(out).writerows([rows[0]] + rows[1 + start:1 + start + n])
        return path

    def test_bulk_now_runs_the_kernel_and_a_normal_load_and_bulk_load_are_the_same(self):
        file = self.sample_file(60)
        a = self.run_cli("--tenant", "t_bulk", "--file", file, "--bulk")
        b = self.run_cli("--tenant", "t_plain", "--file", file)
        self.assertEqual((a.returncode, b.returncode), (0, 0), a.stderr + b.stderr)
        import sqlite3
        c = sqlite3.connect(self.path)
        counts = {t: (c.execute("SELECT COUNT(*) FROM entity_customer WHERE tenant_id=? AND status='active'", (t,)).fetchone()[0],
                      c.execute("SELECT COUNT(*) FROM match_candidates WHERE tenant_id=? AND status='confirmed'", (t,)).fetchone()[0]) for t in ("t_bulk", "t_plain")}
        self.assertEqual(counts["t_bulk"], counts["t_plain"])
        self.assertIn("Auto-merged", a.stdout)                                           # the summary of a normal load: --bulk is no longer a different report

    def test_options_that_belong_to_the_set_based_engine_are_refused_not_ignored(self):
        file = self.sample_file(10)
        for flag in (("--workers", "2"), ("--workdir", self.tmp), ("--dry-run",), ("--resume",), ("--budget", "5"), ("--lineage", "all")):
            r = self.run_cli("--tenant", "t", "--file", file, "--bulk", *flag)
            self.assertEqual(r.returncode, 2, flag)
            self.assertIn("--legacy-bulk", r.stderr, flag)

    def test_the_legacy_bulk_engine_is_still_there_when_asked_for(self):
        r = self.run_cli("--tenant", "t_old", "--file", self.sample_file(40), "--legacy-bulk")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("Bulk load done", r.stdout)
        self.assertIn("PAIR engine", r.stderr)                                           # and it says that its decisions are not the kernel's

    def test_onboarding_a_new_source_and_a_delta_are_just_a_load_and_a_changed_record_is_matched_again(self):
        first, second = self.sample_file(100), self.sample_file(100, start=100)
        self.assertEqual(self.run_cli("--tenant", "t", "--file", first, "--bulk").returncode, 0)
        r = self.run_cli("--tenant", "t", "--file", second, "--bulk", "--onboard")          # accepted, and needed by no one
        self.assertEqual(r.returncode, 0, r.stderr)
        again = self.run_cli("--tenant", "t", "--file", first, "--bulk", "--delta")
        self.assertIn("unchanged: 100", again.stdout)                                      # nothing changed, nothing done
        import sqlite3
        c = sqlite3.connect(self.path)
        self.assertEqual(c.execute("SELECT COUNT(*) FROM source_records WHERE tenant_id='t'").fetchone()[0], 200)
        everyone = c.execute("SELECT COUNT(*) FROM source_records WHERE tenant_id='t' AND entity_id IS NOT NULL").fetchone()[0]
        self.assertEqual(everyone, 200)


if __name__ == "__main__":
    unittest.main()
