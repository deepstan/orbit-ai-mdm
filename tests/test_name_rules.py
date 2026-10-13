"""
First names: unrelated names disagree. Jaro-Winkler scores richard / barbara at 0.62, william / wei at 0.69 and tony / thomas at 0.65, all in a 'partial' band that carried no
penalty, so two DIFFERENT people sharing a phone and an address were auto-merged. A given name now disagrees below GIVEN_NAME_DISAGREE_BELOW (and a domain may override it).

Nicknames are not this rule's business: the standardizer maps the unambiguous ones (Bob -> robert) before matching, and leaves the ambiguous ones alone on purpose. The
tests below pin that interaction too, because the household fix must not undo it.

Run with:  python3 -m unittest tests.test_name_rules -v
"""
import os
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import matching  # noqa: E402
from app.domains import get_domain_config  # noqa: E402
from app.matching import GIVEN_NAME_DISAGREE_BELOW, _below, _compare, classify, score_pair  # noqa: E402
from app.staging import stage_record  # noqa: E402

CONFIG = get_domain_config("customer")


def std(**fields):
    return stage_record("customer", CONFIG, fields)[0]


def state(a, b):
    return _compare(a, b, False, below=_below(CONFIG, "first_name"))[0]


class TestTheComparison(unittest.TestCase):
    def test_unrelated_names_disagree_where_they_used_to_be_partial(self):
        for a, b in (("richard", "barbara"), ("tony", "thomas"), ("william", "wei"), ("richard", "maria"), ("margaret", "charles"), ("linda", "nancy")):
            self.assertEqual(state(a, b), "disagree", (a, b))
            if (a, b) != ("linda", "nancy"):
                self.assertEqual(_compare(a, b, False)[0], "partial", (a, b))       # the old line, still the default for any other field

    def test_a_typo_is_still_the_same_name(self):
        for a, b in (("michael", "michaeel"), ("jennifer", "jenifer"), ("katherine", "katharine"), ("john", "jon"), ("maria", "mario")):
            self.assertEqual(state(a, b), "agree", (a, b))

    def test_spelling_variants_of_one_name_are_not_penalised_but_the_line_does_not_go_higher_than_they_sit(self):
        """Sofia / Sophie 0.76, Kris / Chris 0.78, Sean / Shawn 0.81, Stephen / Stefan 0.82, Allen / Alan 0.83, Rachel / Rochelle 0.84: real variants in the band between
        the line and the agreement threshold. They stay 'partial' (no penalty). A line above them (0.8 would catch Sofia and Kris; 0.85 all of them) turns every one into a
        mismatch, which is what the 0.60 to 0.80 sweep traded against the household errors."""
        for a, b in (("sofia", "sophie"), ("kris", "chris"), ("sean", "shawn"), ("stephen", "stefan"), ("allen", "alan"), ("rachel", "rochelle")):
            self.assertEqual(state(a, b), "partial", (a, b))

    def test_only_given_names_are_held_to_the_stricter_line_and_a_domain_can_override_it(self):
        self.assertEqual(_below(CONFIG, "first_name"), GIVEN_NAME_DISAGREE_BELOW)
        self.assertEqual(_below(CONFIG, "given_name"), GIVEN_NAME_DISAGREE_BELOW)
        for other in ("last_name", "city", "address_line1"):
            self.assertEqual(_below(CONFIG, other), matching.DISAGREE_BELOW, other)
        loose = replace(CONFIG, disagree_below={"first_name": 0.6})
        self.assertEqual(_below(loose, "first_name"), 0.6)
        self.assertEqual(_compare("richard", "maria", False, below=_below(loose, "first_name"))[0], "partial")        # 0.676: no longer a mismatch
        self.assertEqual(_below(replace(CONFIG, disagree_below={"city": 0.9}), "city"), 0.9)                          # any field may be given a line


class TestTheScore(unittest.TestCase):
    HOUSEHOLD = dict(last_name="Ramos", phone="404-200-1501", address_line1="5275 Spring Drive", city="Athens", state="GA", postal_code="30687")

    def test_two_different_people_sharing_a_phone_and_an_address_are_not_auto_merged(self):
        for a, b in (("Richard", "Barbara"), ("Tony", "Thomas"), ("William", "Wei"), ("Margaret", "Charles")):
            sc = score_pair(std(first_name=a, **self.HOUSEHOLD), std(first_name=b, **self.HOUSEHOLD), CONFIG)
            self.assertNotEqual(classify(sc.score), "auto_merge", (a, b, sc.score))
            self.assertTrue(any("first_name disagrees" in r for r in sc.reasons), (a, b, sc.reasons))     # and it says why

    def test_the_same_person_under_a_nickname_is_still_auto_merged_because_the_standardizer_maps_it(self):
        self.assertEqual(std(first_name="Bob")["first_name"], std(first_name="Robert")["first_name"])       # the existing mechanism the household fix must not undo
        sc = score_pair(std(first_name="Robert", **self.HOUSEHOLD), std(first_name="Bob", **self.HOUSEHOLD), CONFIG)
        self.assertEqual(classify(sc.score), "auto_merge", sc.reasons)

    def test_the_standardizers_policy_on_nicknames_is_what_this_rule_relies_on(self):
        """It maps a nickname to the full name only when it means ONE name (Bob, Liz, Ted), and leaves the ambiguous ones (Pat: Patrick or Patricia; Chris; Alex) alone,
        because guessing wrong merges or splits people. The scorer adds no nickname logic of its own: it would only undo that decision."""
        for nickname, full in (("Bob", "robert"), ("Liz", "elizabeth"), ("Ted", "edward"), ("Tony", "anthony")):
            self.assertEqual(std(first_name=nickname)["first_name"], full)
        for ambiguous in ("Pat", "Chris", "Alex", "Terry"):
            self.assertEqual(std(first_name=ambiguous)["first_name"], ambiguous.lower())

    def test_a_deterministic_match_with_agreeing_names_is_unchanged(self):
        sc = score_pair(std(first_name="Maria", **self.HOUSEHOLD), std(first_name="Maria", **self.HOUSEHOLD), CONFIG)
        self.assertEqual(sc.score, matching.DETERMINISTIC_SCORE)

    def test_a_fuzzy_match_with_a_name_typo_is_not_penalised(self):
        base = dict(last_name="Jones", address_line1="12 Oak Street", city="Macon", state="GA", postal_code="31201")
        sc = score_pair(std(first_name="Michael", **base), std(first_name="Michaeel", **base), CONFIG)
        self.assertFalse(any("first_name disagrees" in r for r in sc.reasons))
        self.assertGreater(sc.score, 0.9)


if __name__ == "__main__":
    unittest.main()
