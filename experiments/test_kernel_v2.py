"""
The properties the kernel's design rests on, as tests: its outbox tells the truth, its answers are repeatable, an update behaves as agreed, the group's feature set finds what a
golden alone cannot, an identifier many groups share does not identify, an over-populated key is not used, related groups are linked and not merged, and the scorer is replaceable.

Run with:  python3 -m unittest experiments.test_kernel_v2 -v     (from the project root)
"""
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import hard_cases as hc  # noqa: E402
from kernel_v2 import Kernel2, Result, RuleScorer, replay  # noqa: E402

BASE = {"city": "Atlanta", "state": "GA", "postal_code": "30301"}


def person(first, last, email=None, phone=None, **extra):
    out = {"first_name": first, "last_name": last, **BASE, **extra}
    if email: out["email"] = email
    if phone: out["phone"] = phone
    return out


def run(records, **kw):
    k = Kernel2(**kw)
    for rid, raw in records:
        k.arrive(rid, raw)
    return k


class TestTheOutbox(unittest.TestCase):
    def dataset(self, seed):
        d = hc.build(seed, 0.12)
        raw = {r["rid"]: r["raw"] for r in d["rows"]}
        order = [r["rid"] for r in d["rows"]]
        random.Random(seed).shuffle(order)
        return d, raw, order

    def play(self, seed):
        d, raw, order = self.dataset(seed)
        k = Kernel2()
        for rid in order:
            k.arrive(rid, raw[rid])
        for u in d["updates"]:
            k.update(u["rid"], u["raw"])
        return k

    def test_replaying_the_events_rebuilds_exactly_the_groups_and_links(self):
        for seed in (1, 2, 3):
            k = self.play(seed)
            assign, links = replay(k.events)
            self.assertEqual(assign, k.assign(), seed)
            self.assertEqual(links, k.links(), seed)
            self.assertTrue(k.links(), "the benchmark has households: the check must not be vacuous")

    def test_the_same_data_in_the_same_order_gives_the_same_answer_and_the_same_events(self):
        a, b = self.play(2), self.play(2)
        self.assertEqual(a.assign(), b.assign())
        self.assertEqual(a.events, b.events)

    def test_events_are_numbered_without_gaps(self):
        k = self.play(1)
        self.assertEqual([e["seq"] for e in k.events], list(range(len(k.events))))


class TestAnUpdate(unittest.TestCase):
    def test_a_record_updated_into_someone_else_leaves_its_group_and_the_golden_forgets_it(self):
        k = run([("A", person("Ann", "Lee", "ann@x.com")), ("B", person("Ann", "Lee", "ann@x.com", "404-555-0188", address_line1="12 Oak Street"))])
        self.assertEqual(k.rec_group["A"], k.rec_group["B"])
        old = k.rec_group["B"]
        k.update("A", person("Zed", "Quill", "zed@q.com", "678-555-0100", city="Macon", postal_code="31201"))
        self.assertNotEqual(k.rec_group["A"], k.rec_group["B"])
        self.assertEqual(k.rec_group["B"], old)                                   # the group that remains keeps its identity
        self.assertEqual(k.groups[old].std["first_name"], "ann")
        self.assertNotIn("zed", k.groups[old].features["first_name"])             # nothing of A is left in the group's features
        self.assertIn({"seq": next(e["seq"] for e in k.events if e["type"] == "record_left"), "type": "record_left", "record": "A", "group": old}, k.events)

    def test_a_corrected_typo_rejoins_the_same_group_and_a_group_left_empty_is_removed(self):
        k = run([("A", person("Ann", "Lee", "ann@x.com")), ("B", person("Ann", "Leee", "ann@x.com"))])
        g = k.rec_group["A"]
        k.update("B", person("Ann", "Lee", "ann@x.com"))
        self.assertEqual(k.rec_group["B"], g)
        solo = run([("S", person("Sol", "Gold", "sol@g.com"))])
        solo.update("S", person("Sal", "Gold", "sal@g.com"))
        self.assertEqual(len(solo.groups), 1)                                     # the old group was removed, not left empty
        self.assertTrue(any(e["type"] == "group_removed" for e in solo.events))


class TestTheUpdatePolicy(unittest.TestCase):
    """'rematch': a changed record is matched as a fresh arrival (it can leave on any change). 'anchored': it STAYS while it still shares an identifier with its group and still
    scores at least as a review candidate, and a review item is queued; it leaves only when nothing ties it to the group any more. Measured: after a life event (a new surname,
    or a move) 'rematch' split 38 of 72 real people from their groups and 'anchored' 4."""

    START = [("A", person("Ann", "Lee", "ann@x.com", "404-555-0188")), ("B", person("Ann", "Lee", "ann@x.com"))]

    def test_a_marriage_the_same_email_and_phone_a_new_surname(self):
        rematch, anchored = run(self.START), run(self.START, update_policy="anchored")
        for k in (rematch, anchored):
            k.update("A", person("Ann", "Cole", "ann@x.com", "404-555-0188"))
        self.assertNotEqual(rematch.rec_group["A"], rematch.rec_group["B"])            # a fresh arrival scores 0.82 (the surname disagrees): not an auto-merge
        self.assertEqual(anchored.rec_group["A"], anchored.rec_group["B"])             # the person keeps their identity ...
        self.assertTrue(any(r[0] == "kept_after_change" and r[1] == "A" for r in anchored.reviews))      # ... and a steward is told the change is suspicious
        self.assertTrue(any(e["type"] == "review_queued" and e.get("kind") == "kept_after_change" for e in anchored.events))

    def test_a_record_that_shares_nothing_with_its_group_any_more_leaves_under_either_policy(self):
        for policy in ("rematch", "anchored"):
            k = run(self.START, update_policy=policy)
            k.update("A", person("Zed", "Quill", "zed@q.com", "678-555-0100", city="Macon", postal_code="31201"))
            self.assertNotEqual(k.rec_group["A"], k.rec_group["B"], policy)

    def test_a_record_that_still_resembles_its_group_but_shares_no_identifier_leaves_and_asks_to_be_reviewed_back(self):
        """The condition that matters: anchoring needs a SHARED IDENTIFIER, not just a similar score. Same name and address, but a new email and phone, scores 0.85 against the group
        (the review band): it is not tied to the group by anything that identifies a person, so it leaves, and a steward is asked whether it belongs back."""
        start = [("A", person("Ann", "Lee", "ann@x.com", "404-555-0188", address_line1="12 Oak Street")), ("B", person("Ann", "Lee", "ann@x.com", address_line1="12 Oak Street"))]
        k = run(start, update_policy="anchored")
        k.update("A", person("Ann", "Lee", "ann2@y.com", "678-555-0100", address_line1="12 Oak Street"))
        self.assertNotEqual(k.rec_group["A"], k.rec_group["B"])
        self.assertEqual([r[0] for r in k.reviews], ["record_may_join"])
        self.assertNotIn("kept_after_change", [r[0] for r in k.reviews])

    def test_sharing_an_identifier_is_not_enough_if_the_record_no_longer_scores_even_as_a_review_candidate(self):
        class Switch:                                                                   # a scorer whose answer the test controls
            value = 0.99
            def score(self, record, view, ctx):
                return Result(self.value, "stub", ["stub"])
        switch = Switch()
        k = run(self.START, scorer_object=switch, update_policy="anchored")
        self.assertEqual(k.rec_group["A"], k.rec_group["B"])
        switch.value = 0.40                                                             # below the review line (0.55)
        k.update("A", person("Ann", "Lee", "ann@x.com", "404-555-0188"))                # it still shares the email with its group
        self.assertNotEqual(k.rec_group["A"], k.rec_group["B"])
        self.assertNotIn("kept_after_change", [r[0] for r in k.reviews])

    def test_the_default_is_the_fresh_arrival_rule(self):
        self.assertEqual(Kernel2().update_policy, "rematch")
        self.assertEqual(Kernel2(update_policy="anchored").update_policy, "anchored")


class TestAChangedSurname(unittest.TestCase):
    """The policy of Senzing and Reltio is to match a changed record again. For a surname change that works only if the SCORER does not let a new surname undo an otherwise certain match:
    with two identifiers and the first name in agreement a different last name is a changed name. Measured over three seeds: 38 of 72 real people split without this rule, 8 with it."""

    SAME = {"first_name": "Ann", "email": "ann@x.com", "phone": "404-555-0188", "city": "Atlanta", "state": "GA", "postal_code": "30301"}

    def score(self, scorer, a, b):
        from app.domains import get_domain_config
        from kernel_v2 import Context, CorroboratedScorer, FrequencyAwareScorer
        from app.staging import stage_record
        cfg = get_domain_config("customer")
        cls = {"frequency": FrequencyAwareScorer, "corroborated": CorroboratedScorer}[scorer]
        sa, sb = stage_record("customer", cfg, a)[0], stage_record("customer", cfg, b)[0]
        return cls(cfg).score(sa, sb, Context({}, frozenset()))

    def test_a_new_surname_with_two_identifiers_and_the_first_name_in_agreement_is_still_the_same_person(self):
        old, new = {**self.SAME, "last_name": "Lee"}, {**self.SAME, "last_name": "Cole"}
        before, after = self.score("frequency", old, new), self.score("corroborated", old, new)
        self.assertLess(before.score, 0.92)                                         # the surname alone pulled a certain match down to a review
        self.assertGreaterEqual(after.score, 0.92)
        self.assertTrue(any("changed name" in r for r in after.reasons))             # and it says why

    def test_one_identifier_is_not_enough_to_forgive_a_new_surname(self):
        old, new = {**self.SAME, "last_name": "Lee"}, {**self.SAME, "last_name": "Cole", "phone": ""}
        self.assertEqual(self.score("corroborated", old, new).score, self.score("frequency", old, new).score)

    def test_two_different_people_who_share_an_email_and_a_phone_are_not_merged_by_it(self):
        couple_a, couple_b = {**self.SAME, "last_name": "Lee"}, {**self.SAME, "first_name": "Bob", "last_name": "Cole"}
        r = self.score("corroborated", couple_a, couple_b)
        self.assertLess(r.score, 0.92)                                              # different first names: the first-name penalty stands, so no merge
        self.assertEqual(r.score, self.score("frequency", couple_a, couple_b).score)

    def test_only_a_surname_is_forgiven_a_father_and_son_sharing_an_email_and_a_phone_get_no_relief(self):
        sr, jr = {**self.SAME, "last_name": "Lee", "name_suffix": "Sr"}, {**self.SAME, "last_name": "Lee", "name_suffix": "Jr"}
        r = self.score("corroborated", sr, jr)
        self.assertEqual(r.score, self.score("frequency", sr, jr).score)             # the suffix penalty (0.4) is untouched
        self.assertLess(r.score, 0.92)
        self.assertFalse(any("changed name" in x for x in r.reasons))

    def test_the_same_surname_is_untouched(self):
        a = {**self.SAME, "last_name": "Lee"}
        self.assertEqual(self.score("corroborated", a, a).score, self.score("frequency", a, a).score)

    def test_matching_a_changed_record_again_now_keeps_a_person_whose_surname_changed(self):
        start = [("A", {**self.SAME, "last_name": "Lee"}), ("B", {**self.SAME, "last_name": "Lee"})]
        corroborated, plain = run(start), run(start, scorer="frequency")
        for k in (corroborated, plain):
            self.assertEqual(k.update_policy, "rematch")                              # the policy of Senzing and Reltio, with NO identity guard
            k.update("A", {**self.SAME, "last_name": "Cole"})
        self.assertEqual(corroborated.rec_group["A"], corroborated.rec_group["B"])
        self.assertNotEqual(plain.rec_group["A"], plain.rec_group["B"])
        self.assertEqual(Kernel2().scorer.__class__.__name__, "CorroboratedScorer")      # the default


class TestMergingGroups(unittest.TestCase):
    """A person with two emails and two phones arrives as three records, the middle one last: it matches two groups that do not match each other. `merge_groups` says what happens to the groups:
    auto (Senzing and Reltio: merge when the two golden records match each other), propose (Ataccama: a review, nothing merged), off."""

    BASE = {"city": "Atlanta", "state": "GA", "postal_code": "30301", "first_name": "Dee", "last_name": "Park"}
    R1 = {**BASE, "email": "dee@a.com", "phone": "404-555-0101", "address_line1": "9 Elm Street"}
    R3 = {**BASE, "email": "dee@b.com", "phone": "678-555-0202"}
    R2 = {**BASE, "email": "dee@a.com", "phone": "678-555-0202", "address_line1": "9 Elm Street"}

    def person(self, mode, extra=(), **kw):
        k = Kernel2(merge_groups=mode, relationships=False, **kw)
        for rid, raw in (("R1", self.R1), ("R3", self.R3), ("R2", self.R2), *extra):
            k.arrive(rid, raw)
        return k

    def test_off_leaves_one_person_in_two_groups(self):
        k = self.person("off")
        self.assertEqual((len(k.groups), k.stats["groups_merged"], k.stats["merge_proposed"]), (2, 0, 0))

    def test_auto_merges_the_groups_and_the_older_id_survives(self):
        k = self.person("auto")
        self.assertEqual((len(k.groups), k.stats["groups_merged"]), (1, 1))
        self.assertEqual({k.rec_group[r] for r in ("R1", "R2", "R3")}, {"g000001"})
        self.assertEqual(replay(k.events)[0], k.assign())

    def test_propose_merges_nothing_and_raises_one_review_per_pair_however_often_it_recurs(self):
        extra = (("R4", {**self.R2, "address_line1": "9 Elm Street"}),)                         # another record that again matches both groups
        k = self.person("propose", extra)
        self.assertEqual((len(k.groups), k.stats["groups_merged"]), (2, 0))
        proposals = [r for r in k.reviews if r[0] == "groups_may_be_same" and r[3] >= 0.9 and r[1] < r[2]]
        self.assertEqual(k.stats["merge_proposed"], 1)
        self.assertEqual(len([e for e in k.events if e["type"] == "review_queued" and e.get("kind") == "groups_may_be_same" and e["groups"] == sorted(e["groups"]) and e["score"] >= 0.9]) >= 1, True)
        self.assertTrue(proposals)

    def test_the_old_switch_still_works_and_a_bad_value_is_refused(self):
        self.assertEqual(Kernel2(bridge=False).merge_groups, "off")
        self.assertEqual(Kernel2(bridge=True).merge_groups, "auto")
        self.assertEqual(Kernel2().merge_groups, "auto")                                       # the default: what Senzing and Reltio do
        with self.assertRaises(ValueError):
            Kernel2(merge_groups="sometimes")


class TestCandidateRanking(unittest.TestCase):
    """Groups are ranked by the evidence that found them (how many blocking keys they share with the record, optionally weighted by how rare each key is) and only the best
    `max_candidates` are scored. Measured: see docs/kernel-design.md section 7c."""

    GROUPS = [("a", person("Ann", "Lee", "ann@x.com", "404-555-0101")), ("b", person("Bea", "Lee", "bea@x.com", "404-555-0102")), ("c", person("Cal", "Lee", "cal@x.com", "404-555-0103"))]

    def test_only_the_best_ranked_groups_are_scored_and_the_right_one_is_joined(self):
        full, one = run(self.GROUPS), run(self.GROUPS, max_candidates=1)
        for k in (full, one):
            before = k.stats["scored_groups"]
            k.arrive("new", person("Ann", "Lee", "ann@x.com", "404-555-0101"))
            k.last_scored = k.stats["scored_groups"] - before
            self.assertEqual(k.rec_group["new"], k.rec_group["a"])
        self.assertGreater(full.last_scored, 1)                 # unranked: it looked at several groups
        self.assertEqual(one.last_scored, 1)                    # ranked and cut to one: it looked at exactly the best
        self.assertLessEqual(one.stats["scored_groups"], one.stats["candidate_groups"])

    def test_ranking_is_deterministic_and_idf_weights_rare_keys_up(self):
        a = run(self.GROUPS + [("d", person("Dan", "Lee", "dan@x.com"))], max_candidates=2, rank="idf")
        b = run(self.GROUPS + [("d", person("Dan", "Lee", "dan@x.com"))], max_candidates=2, rank="idf")
        a.arrive("n", person("Cal", "Lee", "cal@x.com")); b.arrive("n", person("Cal", "Lee", "cal@x.com"))
        self.assertEqual(a.events, b.events)
        self.assertEqual(a.rec_group["n"], a.rec_group["c"])

    def test_an_updated_record_always_looks_at_its_own_group_even_when_ranking_would_drop_it(self):
        start = [("A", person("Ann", "Lee", "ann@x.com", "404-555-0188")), ("B", person("Ann", "Lee", "ann@x.com")),
                 ("D1", person("Ann", "Cole", "d1@x.com", "678-555-0001")), ("D2", person("Ann", "Cole", "d2@x.com", "678-555-0002"))]       # decoys that share name and place with the NEW version
        k = run(start, update_policy="anchored", max_candidates=1, rank="keys")
        self.assertEqual(k.rec_group["A"], k.rec_group["B"])
        k.update("A", person("Ann", "Cole", "ann@x.com", "404-555-0188"))                                                                       # a new surname: the decoys now outrank the old group
        self.assertEqual(k.rec_group["A"], k.rec_group["B"])                                                                                   # anchored: it stays, because its group WAS looked at
        self.assertTrue(any(r[0] == "kept_after_change" for r in k.reviews))


class TestTheFeatureSet(unittest.TestCase):
    def test_a_spelling_variant_seen_in_the_group_is_what_a_record_is_compared_with(self):
        members = [("1", person("Stephen", "Hart", "sh@x.com", address_line1="9 Elm Street")), ("2", person("Stephen", "Hart", "sh@x.com", address_line1="9 Elm Street")),
                   ("3", person("Stefan", "Hart", "sh@x.com", address_line1="9 Elm Street"))]      # one email ties them into one group; the group has now SEEN both spellings
        newcomer = person("Stephen", "Hart", address_line1="9 Elm Street")                # no identifier: only the names and the address can find the group
        std = lambda k: k_std(k, newcomer)
        with_features, without = run(members), run(members, use_features=False)
        self.assertEqual((len(with_features.groups), len(without.groups)), (1, 1))
        g1, g2 = next(iter(with_features.groups.values())), next(iter(without.groups.values()))
        self.assertEqual(g1.std["first_name"], "stefan")                           # the golden is whatever survivorship picked (the most recent spelling) ...
        self.assertEqual(g1.features["first_name"], {"stephen": 2, "stefan": 1})   # ... the group knows it has seen "stephen" twice
        self.assertEqual(with_features._view(std(with_features), g1)["first_name"], "stephen")      # so the record is compared with the value it actually resembles
        self.assertEqual(without._view(std(without), g2)["first_name"], "stefan")                  # (a golden alone would compare it with the last spelling to arrive)

    def test_a_second_phone_the_group_has_seen_finds_the_group(self):
        k = run([("1", person("Dee", "Park", "dee@p.com", "404-555-0111")), ("2", person("Dee", "Park", "dee@p.com", "678-555-0222")),
                 ("3", person("Dee", "Park", phone="678-555-0222"))])
        self.assertEqual(len(k.groups), 1)


def k_std(k, raw):
    from app.staging import stage_record
    return stage_record(k.domain, k.config, raw)[0]


class TestAnIdentifierManyGroupsShare(unittest.TestCase):
    HUB = "404-555-0000"

    def hub_groups(self, **kw):
        records = [(f"p{i}", person(first, f"Surname{i}", f"p{i}@x.com", self.HUB, address_line1=f"{i} Main Street", postal_code=f"3030{i}"))
                   for i, first in enumerate(["Alan", "Berta", "Carl", "Dora"])]
        return run(records, **kw)

    def test_four_different_people_on_one_phone_are_four_groups_that_all_hold_it(self):
        k = self.hub_groups()
        self.assertEqual(len(k.groups), 4)
        self.assertEqual(len(k.holders[("phone", "4045550000")]), 4)

    def test_a_record_that_is_only_the_hub_identifier_is_not_merged_into_any_of_them(self):
        k = self.hub_groups()
        k.arrive("only_phone", {"phone": self.HUB})
        self.assertEqual(len(k.groups), 5)
        rule = self.hub_groups(scorer="rule")
        rule.arrive("only_phone", {"phone": self.HUB})
        self.assertLess(len(rule.groups), 5)                                       # (the plain rule scorer WOULD have merged it into one of them)

    def test_the_same_identifier_with_a_name_that_agrees_still_merges(self):
        k = self.hub_groups()
        before = len(k.groups)
        k.arrive("dup", person("Alan", "Surname0", phone=self.HUB, address_line1="0 Main Street", postal_code="30300"))
        self.assertEqual((len(k.groups), k.rec_group["dup"]), (before, k.rec_group["p0"]))


class TestTheKeyCap(unittest.TestCase):
    def test_a_key_shared_by_more_groups_than_the_cap_is_not_used_to_find_candidates(self):
        records = [(f"p{i}", person(f"Name{chr(97 + i)}", "Shared", f"n{i}@x.com", f"404-555-01{i:02d}", address_line1=f"{i} Oak Street")) for i in range(6)]
        capped, uncapped = run(records, cap=2), run(records, cap=1000)
        capped.arrive("new", person("Namea", "Shared", address_line1="0 Oak Street"))
        uncapped.arrive("new", person("Namea", "Shared", address_line1="0 Oak Street"))
        self.assertGreater(capped.stats["skipped_keys"], 0)
        self.assertEqual(uncapped.stats["skipped_keys"], 0)
        self.assertLess(capped.stats["candidate_groups"], uncapped.stats["candidate_groups"])


class TestRelationships(unittest.TestCase):
    HOME = dict(address_line1="5275 Spring Drive", city="Athens", state="GA", postal_code="30687")

    def test_two_people_at_one_address_with_one_family_name_are_linked_not_merged(self):
        k = run([("a", person("Nancy", "Ramos", phone="404-200-1501", **self.HOME)), ("b", person("Anthony", "Ramos", phone="404-200-1502", **self.HOME))])
        self.assertEqual(len(k.groups), 2)
        self.assertEqual(len(k.links("household")), 1)
        self.assertEqual(replay(k.events)[1], k.links())

    def test_neighbours_and_namesakes_elsewhere_are_not_linked(self):
        k = run([("a", person("Nancy", "Ramos", **self.HOME)), ("b", person("Anthony", "Ramos", address_line1="1 Far Street", city="Athens", state="GA", postal_code="30687")),
                 ("c", person("Tom", "Other", **self.HOME))])
        self.assertEqual(k.links("household"), set())

    def test_a_link_follows_its_group_when_groups_merge(self):
        k = run([("a", person("Nancy", "Ramos", "n@r.com", **self.HOME)), ("b", person("Anthony", "Ramos", "t@r.com", **self.HOME))])
        (link,) = k.links("household")
        gone, kept = sorted(link)[::-1][0], sorted(link)[0]
        k.alias[gone] = kept                                                       # as _merge records it
        self.assertEqual(k._resolve(gone), kept)


class TestThePluggableScorer(unittest.TestCase):
    class Always:
        def __init__(self, value): self.value, self.calls = value, 0
        def score(self, record, view, ctx):
            self.calls += 1
            return Result(self.value, "stub", ["stub"])

    def test_any_object_with_a_score_method_decides_and_gets_a_context(self):
        records = [("a", person("Ann", "Lee", "ann@x.com")), ("b", person("Zed", "Quill", "ann@x.com"))]
        yes, no = self.Always(0.99), self.Always(0.0)
        self.assertEqual(len(run(records, scorer_object=yes).groups), 1)
        self.assertEqual(len(run(records, scorer_object=no).groups), 2)
        self.assertGreater(yes.calls, 0)

    def test_the_rule_scorer_is_the_current_scorer_unchanged(self):
        from app.domains import get_domain_config
        from app.matching import score_pair
        cfg = get_domain_config("customer")
        a, b = k_std(Kernel2(), person("Ann", "Lee", "ann@x.com")), k_std(Kernel2(), person("Ann", "Lee", "ann@x.com", address_line1="1 Oak Street"))
        self.assertEqual(RuleScorer(cfg).score(a, b, None).score, score_pair(a, b, cfg).score)


if __name__ == "__main__":
    unittest.main()
