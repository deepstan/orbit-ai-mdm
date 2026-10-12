"""
The bulk, onboarding and delta engines keep apart what a person separated.

The fixture is test_unmerge's chain of three customers (A-B share an email, B-C share a phone); a person has unmerged A-B, so A is one entity and B-C another.
Then data arrives through the SET-BASED engines that would, if nothing stopped it, reunite them: a record matching A by email and B, C by phone.

Run with:  python3 -m unittest tests.test_separations_bulk -v
"""
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from test_unmerge import A, B, C, D, DOMAIN, TENANT, Chain  # noqa: E402
from app.bulk import bulk_load  # noqa: E402
from app.db import Store  # noqa: E402
from app.pipeline import resolve_candidate  # noqa: E402
from app.unmerge import separation_violations, unmerge_pair  # noqa: E402

NOTE = "a person separated records in these two entities"
BASE_NEW = {"first_name": "Robert", "last_name": "Jones", "city": "Atlanta"}


class Separated(Chain):
    def setUp(self):
        super().setUp()
        self.ab = self.pair("A", "B")
        unmerge_pair(self.store, TENANT, DOMAIN, self.ab, "jane")
        self.e_a, self.e_bc = self.entity("A"), self.entity("B")
        self.assertNotEqual(self.e_a, self.e_bc)

    def load(self, records, store=None, **kw):
        return bulk_load(store or self.store, TENANT, DOMAIN, records, **kw)

    def groups(self, store=None):
        """The entities as sets of source record ids: comparable between two databases whose generated ids differ."""
        store = store or self.store
        out = {}
        for r in store.list_source_records(TENANT, DOMAIN):
            out.setdefault(r.entity_id, set()).add(r.source_record_id)
        return {frozenset(v) for v in out.values()}

    def pending_reasons(self):
        return [" ".join(c.reasons) for c in self.store.list_candidates(TENANT, DOMAIN, status="pending")]

    def assert_still_apart(self):
        self.assertEqual(separation_violations(self.store, TENANT, DOMAIN), [])
        self.assertNotEqual(self.entity("A"), self.entity("B"))


class TestOnboarding(Separated):
    def test_a_record_matching_both_sides_joins_only_one_and_the_other_pair_goes_to_review(self):
        r = self.load([("src2", "D1", D)], onboard=True)
        self.assert_still_apart()
        self.assertIn(self.entity("D1"), (self.e_a, self.e_bc))                                       # D joined exactly one side
        self.assertGreaterEqual(r.separations_held, 1)
        self.assertEqual(r.auto_pairs + r.review_pairs, 3)                                           # D matched A, B and C
        self.assertEqual((r.auto_pairs, r.review_pairs), (3 - r.separations_held, r.separations_held))   # the counts say what was written
        held = [x for x in self.pending_reasons() if NOTE in x]
        self.assertEqual(len(held), r.separations_held)
        self.assertEqual(len([c for c in self.store.list_candidates(TENANT, DOMAIN, status="confirmed") if c.decided_by == "system:bulk_load"]), r.auto_pairs)

    def test_two_new_records_that_each_reach_one_side_and_each_other_cannot_bridge_them(self):
        d1 = {**BASE_NEW, "email": "shared@x.example", "phone": A["phone"]}                          # D1 matches A (phone) ...
        d2 = {**BASE_NEW, "email": "shared@x.example", "phone": B["phone"]}                          # ... D2 matches B and C (phone) ... and D1-D2 match each other (email)
        r = self.load([("src2", "D1", d1), ("src2", "D2", d2)], onboard=True)
        self.assertGreaterEqual(r.auto_pairs + r.review_pairs, 3)                                    # all three links were found
        self.assertGreaterEqual(r.separations_held, 1)                                               # so at least one had to be held
        self.assert_still_apart()
        sides = {self.entity("D1"), self.entity("D2")}
        self.assertTrue(sides <= {self.e_a, self.e_bc, self.entity("D1")})
        self.assertFalse({self.e_a, self.e_bc} <= {self.entity(n) for n in ("D1", "D2")} and self.entity("D1") == self.entity("D2"))

    def test_a_held_pair_cannot_be_confirmed_by_hand_either(self):
        self.load([("src2", "D1", D)], onboard=True)
        pending = [c for c in self.store.list_candidates(TENANT, DOMAIN, status="pending") if NOTE in " ".join(c.reasons)]
        ea, eb = (self.store.get_source_record(r, TENANT, DOMAIN).entity_id for r in (pending[0].record_a_id, pending[0].record_b_id))
        self.assertNotEqual(ea, eb)
        with self.assertRaises(ValueError) as raised:
            resolve_candidate(self.store, TENANT, DOMAIN, pending[0].id, "confirm", "sam")
        self.assertIn("separated", str(raised.exception))

    def test_records_that_touch_only_one_side_are_merged_as_usual(self):
        r = self.load([("src2", "E1", {**C, "first_name": "Roberto"}), ("src2", "F1", {**A, "email": "other@x.example", "first_name": "Rob"})], onboard=True)
        self.assertEqual(r.separations_held, 0)                                                      # nothing is held that need not be
        self.assertEqual(self.entity("E1"), self.e_bc)                                               # E1 matches C (email/phone) and joins B-C
        self.assert_still_apart()

    def test_a_dry_run_says_what_would_be_held_and_writes_nothing(self):
        before = (self.groups(), len(self.every_candidate()))
        r = self.load([("src2", "D1", D)], onboard=True, write=False)
        self.assertTrue(r.dry_run)
        self.assertGreaterEqual(r.separations_held, 1)
        self.assertEqual((self.groups(), len(self.every_candidate())), before)

    def test_the_result_is_the_same_on_every_run_and_with_any_number_of_workers(self):
        results = []
        for workers in (1, 2, 1):
            copy = os.path.join(self.tmp, f"copy{len(results)}.db")
            shutil.copy(self.path, copy)
            store = Store(copy)
            r = self.load([("src2", "D1", D)], store=store, onboard=True, workers=workers)
            results.append((self.groups(store), r.separations_held, r.auto_pairs, r.review_pairs))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], results[2])


class TestDelta(Separated):
    def test_a_new_record_from_a_loaded_source_that_matches_both_sides(self):
        r = self.load([("src", "D1", D)], update=True)
        self.assert_still_apart()
        self.assertGreaterEqual(r.separations_held, 1)
        self.assertEqual(r.new_records, 1)

    def test_a_changed_record_is_not_paired_again_with_the_record_a_person_separated_from_it(self):
        records_before = len(self.store.list_source_records(TENANT, DOMAIN))
        r = self.load([("src", "A", {**A, "phone": "404-555-0102"})], update=True)                   # A is re-sent with a new phone: it meets B again (same email)
        self.assertEqual((r.changed_records, r.new_records), (1, 0))
        self.assertEqual(len(self.store.list_source_records(TENANT, DOMAIN)), records_before)
        self.assertEqual(r.separations_dropped, 1)
        both = {self.rid("A"), self.rid("B")}
        self.assertEqual([c.status for c in self.every_candidate() if {c.record_a_id, c.record_b_id} == both], ["rejected"])   # still only the person's decision
        self.assert_still_apart()

    def test_a_changed_record_that_now_matches_the_other_side_is_held_not_merged(self):
        r = self.load([("src", "A", {**A, "phone": "404-555-0199"})], update=True)                   # A now shares B's and C's phone: they match, but a person separated A and B
        self.assert_still_apart()
        self.assertEqual(self.entity("A"), self.e_a)
        self.assertGreaterEqual(r.separations_held + r.separations_dropped, 1)

    def test_a_delta_that_touches_nothing_separated_is_unaffected(self):
        r = self.load([("src", "G1", {"first_name": "Zed", "last_name": "Quill", "email": "zed@quill.example", "phone": "404-555-0777", "city": "Macon"})], update=True)
        self.assertEqual((r.separations_held, r.separations_dropped, r.new_records), (0, 0, 1))
        self.assertTrue(r.separations_present)


class TestWithNoSeparations(Chain):
    def test_nothing_changes_for_a_tenant_where_nobody_has_unmerged_anything(self):
        r = bulk_load(self.store, TENANT, DOMAIN, [("src2", "D1", D)], onboard=True)
        self.assertEqual((r.separations_present, r.separations_held, r.separations_dropped), (False, 0, 0))
        self.assertEqual(len({self.rec(n).entity_id for n in "ABC"}), 1)                              # A, B, C and D are one entity
        self.assertEqual(r.review_pairs, 0)                                                           # D joins the one entity: nothing held, as before


if __name__ == "__main__":
    unittest.main()
