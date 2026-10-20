"""
Unmerge: a person says a confirmed merge is wrong, and the entity is split along it. The data is three customers where A-B share an email and B-C share a phone
(a chain: A and C are not paired), so there is a real split to make.

Run with:  python3 -m unittest tests.test_unmerge -v
"""
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import Store  # noqa: E402
from app.models import MatchCandidate, now_iso  # noqa: E402
from app.pipeline import ingest_record, merge_two_records, resolve_candidate  # noqa: E402
from app.unmerge import CannotUnmerge, plan_unmerge, separation_violations, unmerge_pair  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
TENANT, DOMAIN = "t", "customer"
BASE = {"city": "Atlanta"}
A = {**BASE, "first_name": "Robert", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0101"}
B = {**BASE, "first_name": "Bob", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0199"}
C = {**BASE, "first_name": "Robert", "last_name": "Jonas", "email": "bj@other.com", "phone": "404-555-0199"}
D = {**BASE, "first_name": "Robert", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0199"}   # matches A by email AND B, C by phone


class Chain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        patcher = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0"})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.path = os.path.join(self.tmp, "t.db")
        self.store = Store(self.path)
        for name, attrs in (("A", A), ("B", B), ("C", C)):
            self.ingest(name, attrs)
        self.original = self.entity("A")

    def ingest(self, name, attrs, store=None):
        return ingest_record(store or self.store, TENANT, DOMAIN, "src", name, attrs)

    def ingest_tied_every_way(self, name, attrs):
        """Ingest a record that matches A, B and C, and make sure it is tied to EACH of them: a second path between records that were already joined.
        The legacy engine saved a pair for every record the newcomer matched, so this happened by itself. The kernel joins a record to its group with ONE pair (an entity is a
        tree, so unmerge always has one pair to remove); a second path now arises when a steward confirms a pair that already holds, and that is what this does."""
        result = self.ingest(name, attrs)
        for other in "ABC":
            if not self.store.confirmed_pairs_among(TENANT, DOMAIN, [self.rid(name), self.rid(other)]):
                self.store.save_candidate(MatchCandidate(record_a_id=self.rid(name), record_b_id=self.rid(other), tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy",
                                                         status="confirmed", decided_by="steward:test", decided_at=now_iso()))
        return result

    def rec(self, name):
        return next(r for r in self.store.list_source_records(TENANT, DOMAIN) if r.source_record_id == name)

    def rid(self, name):
        return self.rec(name).id

    def entity(self, name):
        return self.rec(name).entity_id

    def pair(self, x, y):
        found = [c for c, a, b in self.store.confirmed_pairs_among(TENANT, DOMAIN, [self.rid(x), self.rid(y)])]
        self.assertEqual(len(found), 1, f"{x}-{y}")
        return found[0]

    def golden_of(self, names):
        """What a fresh database makes of just these records: the golden record the split parts should have."""
        store = Store(os.path.join(tempfile.mkdtemp(dir=self.tmp), "fresh.db"))
        for n in names:
            self.ingest(n, {"A": A, "B": B, "C": C, "D": D}[n], store=store)
        entities = {r.entity_id for r in store.list_source_records(TENANT, DOMAIN)}
        self.assertEqual(len(entities), 1)
        return store.get_entity(entities.pop(), TENANT, DOMAIN).attributes

    def every_candidate(self):
        """list_candidates returns only PENDING pairs unless told otherwise: look at all three statuses."""
        return [c for status in ("pending", "confirmed", "rejected") for c in self.store.list_candidates(TENANT, DOMAIN, status=status)]

    def snapshot(self):
        return ({n: self.entity(n) for n in "ABC"}, {c.id: (c.status, c.decided_by) for c in self.every_candidate()},
                {e: self.store.get_entity(e, TENANT, DOMAIN).attributes for e in {self.entity(n) for n in "ABC"}})


class TestSplitting(Chain):
    def test_the_fixture_is_one_entity_joined_in_a_chain(self):
        self.assertEqual(len({self.entity(n) for n in "ABC"}), 1)
        self.assertEqual(len(self.store.confirmed_pairs_among(TENANT, DOMAIN, [self.rid(n) for n in "ABC"])), 2)   # A-B and B-C, no A-C

    def test_unmerging_the_first_link_splits_the_chain_and_the_established_entity_keeps_its_id(self):
        ab = self.pair("A", "B")
        r = unmerge_pair(self.store, TENANT, DOMAIN, ab, "jane", "different people")
        self.assertTrue(r["split"])
        self.assertEqual(self.entity("A"), self.original)                                         # A is the oldest record: its side keeps the entity id downstream systems hold
        self.assertEqual(self.entity("B"), self.entity("C"))
        self.assertNotEqual(self.entity("B"), self.original)
        self.assertEqual(r["new_entity"], self.entity("B"))
        cand = self.store.get_candidate(ab, TENANT, DOMAIN)
        self.assertEqual((cand.status, cand.decided_by), ("rejected", "unmerge:jane"))
        self.assertEqual(self.store.get_candidate(self.pair("B", "C"), TENANT, DOMAIN).status, "confirmed")   # only the pair a person named was undone

    def test_both_golden_records_are_what_their_records_alone_would_make(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane")
        self.assertEqual(self.store.get_entity(self.entity("A"), TENANT, DOMAIN).attributes, self.golden_of(["A"]))
        self.assertEqual(self.store.get_entity(self.entity("B"), TENANT, DOMAIN).attributes, self.golden_of(["B", "C"]))

    def test_the_new_entitys_history_says_who_unmerged_it_and_why(self):
        r = unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane", "different people")
        conn = sqlite3.connect(self.path)
        reasons = " ".join(x[0] or "" for x in conn.execute("SELECT reason FROM lineage WHERE entity_id = ?", (r["new_entity"],)))
        self.assertIn("unmerged by jane: different people", reasons)

    def test_unmerging_the_second_link_moves_only_the_newest_record(self):
        r = unmerge_pair(self.store, TENANT, DOMAIN, self.pair("B", "C"), "jane")
        self.assertEqual((self.entity("A"), self.entity("B")), (self.original, self.original))
        self.assertNotEqual(self.entity("C"), self.original)
        self.assertEqual(r["moved"], [self.rid("C")])
        self.assertEqual(self.store.get_entity(self.entity("C"), TENANT, DOMAIN).attributes, self.golden_of(["C"]))

    def test_every_record_still_has_exactly_one_entity_and_no_entity_is_empty(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane")
        records = self.store.list_source_records(TENANT, DOMAIN)
        self.assertTrue(all(r.entity_id for r in records))
        for e in {r.entity_id for r in records}:
            self.assertTrue(self.store.list_source_records(TENANT, DOMAIN, entity_id=e))

    def test_a_dry_run_changes_nothing_and_says_what_would_move(self):
        before = self.snapshot()
        plan = unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane", dry_run=True)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(len(before[1]), 2)                                                        # the snapshot really sees both confirmed pairs, so "nothing changed" means something
        self.assertEqual(sorted(plan["move"]), sorted([self.rid("B"), self.rid("C")]))
        self.assertEqual(plan["keep"], [self.rid("A")])


class TestWhatItRefuses(Chain):
    def test_records_still_joined_by_another_path_are_not_split_and_the_path_is_named(self):
        self.ingest_tied_every_way("D", D)                                                        # D matches A, B and C: now A-D-B joins A and B another way
        self.assertEqual(len({self.entity(n) for n in "ABCD"}), 1)
        ab = self.pair("A", "B")
        before = self.snapshot()
        with self.assertRaises(CannotUnmerge) as raised:
            unmerge_pair(self.store, TENANT, DOMAIN, ab, "jane")
        self.assertIn("still joined through other confirmed pairs", str(raised.exception))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.store.get_candidate(ab, TENANT, DOMAIN).status, "confirmed")

    def test_only_a_confirmed_merge_can_be_unmerged(self):
        pending = MatchCandidate(record_a_id=self.rid("A"), record_b_id=self.rid("C"), tenant_id=TENANT, domain=DOMAIN, score=0.8, match_type="fuzzy")
        self.store.save_candidate(pending)
        for bad, text in ((pending.id, "is pending"), ("cand_nope", "No such candidate")):
            with self.assertRaises(CannotUnmerge) as raised:
                unmerge_pair(self.store, TENANT, DOMAIN, bad, "jane")
            self.assertIn(text, str(raised.exception))

    def test_a_pair_cannot_be_unmerged_twice(self):
        ab = self.pair("A", "B")
        unmerge_pair(self.store, TENANT, DOMAIN, ab, "jane")
        with self.assertRaises(CannotUnmerge) as raised:
            unmerge_pair(self.store, TENANT, DOMAIN, ab, "sam")
        self.assertIn("already unmerged by jane", str(raised.exception))

    def test_it_is_a_persons_decision_and_needs_a_name(self):
        for who in ("", "   ", None):
            with self.assertRaises(CannotUnmerge):
                unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), who)
        self.assertEqual(len({self.entity(n) for n in "ABC"}), 1)


class TestAnInterruptedUnmerge(Chain):
    def test_repeating_it_finishes_the_job(self):
        ab = self.pair("A", "B")
        real = Store.update_candidate_status
        calls = []

        def die_once(store, *a, **kw):
            if not calls:
                calls.append(1)
                raise RuntimeError("the process died here")
            return real(store, *a, **kw)
        with mock.patch.object(Store, "update_candidate_status", die_once), self.assertRaises(RuntimeError):
            unmerge_pair(self.store, TENANT, DOMAIN, ab, "jane")
        self.assertNotEqual(self.entity("A"), self.entity("B"))                                   # the records had moved ...
        self.assertEqual(self.store.get_candidate(ab, TENANT, DOMAIN).status, "confirmed")         # ... the separation was not recorded ...
        self.assertEqual(len(self.store.list_golden_dirty(DOMAIN)), 2)                             # ... and both golden records are durably marked stale
        r = unmerge_pair(self.store, TENANT, DOMAIN, ab, "jane")
        self.assertIn("already in different entities", r["note"])
        cand = self.store.get_candidate(ab, TENANT, DOMAIN)
        self.assertEqual((cand.status, cand.decided_by), ("rejected", "unmerge:jane"))
        self.assertEqual(self.store.list_golden_dirty(DOMAIN), [])
        self.assertEqual(self.store.get_entity(self.entity("B"), TENANT, DOMAIN).attributes, self.golden_of(["B", "C"]))


class TestKeepingItTrue(Chain):
    def unmerged(self):
        unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane")

    def test_the_regular_engine_never_rejoins_two_entities_a_person_separated(self):
        self.unmerged()
        result = self.ingest("D", D)             # D matches A (email) and B, C (phone): joining both would reunite them
        self.assertNotEqual(self.entity("A"), self.entity("B"))
        self.assertTrue(result["queued_for_review"])
        reasons = " ".join(" ".join(c.reasons) for c in self.store.list_candidates(TENANT, DOMAIN, status="pending"))
        self.assertIn("a person separated records in these two entities", reasons)

    def test_the_separated_pair_is_not_proposed_again_when_one_of_its_records_changes(self):
        self.unmerged()
        records_before = len(self.store.list_source_records(TENANT, DOMAIN))
        self.ingest("A", {**A, "phone": "404-555-0102"})         # A is re-sent with a new phone: it is matched again, against B among others (same email)
        self.assertEqual(len(self.store.list_source_records(TENANT, DOMAIN)), records_before)   # a CHANGE to A, not a new record
        both = {self.rid("A"), self.rid("B")}
        between = [c for c in self.every_candidate() if {c.record_a_id, c.record_b_id} == both]
        self.assertEqual([c.status for c in between], ["rejected"])      # still only the person's decision: no new candidate for the same pair
        self.assertNotEqual(self.entity("A"), self.entity("B"))

    def test_a_steward_cannot_confirm_a_pair_that_would_reunite_them_but_may_reject_it(self):
        self.unmerged()
        pending = MatchCandidate(record_a_id=self.rid("A"), record_b_id=self.rid("C"), tenant_id=TENANT, domain=DOMAIN, score=0.8, match_type="fuzzy")
        self.store.save_candidate(pending)
        with self.assertRaises(ValueError) as raised:
            resolve_candidate(self.store, TENANT, DOMAIN, pending.id, "confirm", "sam")
        self.assertIn("separated", str(raised.exception))
        self.assertEqual(self.store.get_candidate(pending.id, TENANT, DOMAIN).status, "pending")   # refused before anything changed
        resolve_candidate(self.store, TENANT, DOMAIN, pending.id, "reject", "sam")

    def test_a_separation_an_engine_undid_is_found(self):
        self.unmerged()
        self.assertEqual(separation_violations(self.store, TENANT, DOMAIN), [])
        merge_two_records(self.store, TENANT, DOMAIN, self.rid("A"), self.rid("B"), reason="what an engine that does not know about separations does")
        found = separation_violations(self.store, TENANT, DOMAIN)
        self.assertEqual([(c, a, b) for c, a, b, e in found], [(self.store.list_candidates(TENANT, DOMAIN, status="rejected")[0].id, found[0][1], found[0][2])])
        self.assertEqual(len(found), 1)

    def test_an_unmerge_is_a_human_label_of_different_even_over_an_earlier_audit_that_said_correct(self):
        ab = self.pair("A", "B")
        self.store.flag_merge_audits(TENANT, DOMAIN, [ab])
        self.store.record_merge_audit(ab, TENANT, DOMAIN, "correct", "pat")
        self.assertEqual([(c.id, label, source) for c, label, source in self.store.human_labeled_candidates(TENANT, DOMAIN)], [(ab, 1, "audit")])
        self.unmerged()
        labels = [(c.id, label, source) for c, label, source in self.store.human_labeled_candidates(TENANT, DOMAIN)]
        self.assertEqual(labels, [(ab, 0, "steward")])                                            # the later decision stands

    def test_with_no_unmerges_the_engine_pays_for_one_tiny_query_and_changes_nothing(self):
        self.assertFalse(self.store.has_separations(TENANT, DOMAIN))
        before = self.snapshot()
        self.ingest("D", D)
        self.assertEqual(len({self.entity(n) for n in "ABCD"}), 1)
        self.assertEqual({k: v for k, v in before[0].items()}, {k: self.original for k in "ABC"})


class TestTheBulkEnginesReportSeparations(Chain):
    def test_a_bulk_load_reports_that_separations_exist(self):
        from app.bulk import bulk_load
        other = {"first_name": "Zed", "last_name": "Quill", "email": "zed@quill.example", "phone": "404-555-0777", "city": "Macon"}
        before = bulk_load(self.store, TENANT, DOMAIN, [("src2", "X1", other)], onboard=True)
        self.assertFalse(before.separations_present)
        unmerge_pair(self.store, TENANT, DOMAIN, self.pair("A", "B"), "jane")
        after = bulk_load(self.store, TENANT, DOMAIN, [("src2", "X2", {**other, "email": "zed2@quill.example", "phone": "404-555-0778"})], onboard=True)
        self.assertTrue(after.separations_present)


class TestTheCommands(Chain):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), *args, "--db", self.path], capture_output=True, text=True, cwd=ROOT, env=dict(os.environ))

    def test_dry_run_unmerge_check_and_the_refusal_of_a_second_unmerge(self):
        ab = self.pair("A", "B")
        base = ("unmerge", ab, "--domain", DOMAIN, "--tenant", TENANT, "--by", "jane")
        before = self.snapshot()
        dry = self.run_cli(*base, "--dry-run")
        self.assertEqual(dry.returncode, 0, dry.stderr)
        self.assertIn("DRY RUN", dry.stdout)
        self.assertEqual(self.snapshot(), before)
        done = self.run_cli(*base, "--reason", "different people")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("moved to the new entity", done.stdout)
        self.assertNotEqual(self.entity("A"), self.entity("B"))
        again = self.run_cli(*base)
        self.assertEqual(again.returncode, 2)
        self.assertIn("already unmerged", again.stderr)
        self.assertEqual(self.run_cli("separation-check", DOMAIN, "--tenant", TENANT).returncode, 0)
        merge_two_records(self.store, TENANT, DOMAIN, self.rid("A"), self.rid("B"), reason="an engine that does not know")
        check = self.run_cli("separation-check", DOMAIN, "--tenant", TENANT)
        self.assertEqual(check.returncode, 3)
        self.assertIn("undone", check.stdout)


if __name__ == "__main__":
    unittest.main()
