"""
What an AGENT RECOMMENDS about a review item, and how those recommendations are judged (app/recommendations.py, the agent_recommendations table).

  * a recommendation CHANGES NOTHING: the item stays pending, no entity moves, the review queue looks exactly as before
  * WHO may propose (the agent role, administrators), under whose name (the key's, never the request's), about what (an item still waiting, in the caller's own tenant)
  * the rules a proposal must meet, enforced whatever the web framework checks
  * a PERSON's decision records the OUTCOME on every open recommendation for that item (agreed, disagreed, human_decided), once
  * the STATISTICS those outcomes make: agreement, and whether stated confidence means anything
  * right to be forgotten: a purge deletes what an agent wrote about the purged records

Calls the endpoint functions directly on a real database (tests/fake_web.py stands in for FastAPI where it is not installed).
Run with:  python3 -m unittest tests.test_recommendations -v
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import fake_web  # noqa: E402
from app import limits, recommendations  # noqa: E402
from app.auth import AuthError, Principal  # noqa: E402
from app.compliance import purge_entity  # noqa: E402
from app.db import Store  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate  # noqa: E402

api = None
_saved: dict = {}
_env: dict = {}


def setUpModule():
    global api
    _saved.update(fake_web.snapshot())
    for name in ("DATABASE_URL", "MDM_DEV_MODE", "MDM_AUDIT_RATE"):
        _env[name] = os.environ.get(name)
    fake_web.install()
    os.environ["DATABASE_URL"] = os.path.join(tempfile.mkdtemp(), "api_import.db")
    os.environ["MDM_AUDIT_RATE"] = "0"
    os.environ.pop("MDM_DEV_MODE", None)
    sys.modules.pop("app.api", None)
    import importlib
    api = importlib.import_module("app.api")


def tearDownModule():
    fake_web.restore(_saved)
    for name, value in _env.items():
        os.environ.pop(name, None) if value is None else os.environ.__setitem__(name, value)


PEOPLE = [("Ann", "Lee", "30301"), ("Bob", "Moss", "31201"), ("Cara", "Quinn", "30030"), ("Dev", "Patel", "30606"), ("Eve", "Stone", "31401"), ("Finn", "Rowe", "30501")]


def P(role="steward_agent", label="agent-1", tenant="acme", allowed=None):
    return Principal(tenant_id=tenant, role=role, key_id=f"key_{label}", label=label, allowed_domains=allowed)


def propose(**kw):
    return api.ProposeDecisionRequest(**{"recommendation": "confirm", "confidence": 0.9, "reasons": ["same person, new surname"], "evidence": ["group_b.members"],
                                         "model": "model-1", "prompt_version": "v1", **kw})


class Wired(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(os.path.join(self.tmp, "api.db"))
        for target, value in ((api, "store"), (api, "_limiter")):
            patcher = mock.patch.object(target, value, self.store if value == "store" else limits.RateLimiter(0))
            patcher.start()
            self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel", "MDM_MODEL_DIR": os.path.join(self.tmp, "models")})
        env.start()
        self.addCleanup(env.stop)
        self.agent, self.steward, self.admin = P(), P("data_steward", "Pat"), P("tenant_admin", "Root")

    def item(self, i=0, tenant="acme"):
        """A review item: a group of two records of one person, and a record that resembles them without being certain. Returns the item's candidate id."""
        first, last, zip_ = PEOPLE[i]
        base = {"first_name": first, "last_name": last, "email": f"{first.lower()}@x.com", "phone": f"404-555-01{i}0", "address_line1": f"{i + 1} Elm Street", "city": "Atlanta", "state": "GA", "postal_code": zip_}
        for n in (1, 2):
            ingest_record(self.store, tenant, "customer", "crm", f"{first}{n}", base)
        r = ingest_record(self.store, tenant, "customer", "crm", f"{first}X", {**base, "email": f"other.{first.lower()}@z.com", "phone": "", "last_name": last + "h"})
        self.assertEqual(len(r["queued_for_review"]), 1, f"the helper must make a review item: {r}")
        return r["queued_for_review"][0]["candidate_id"]

    def refused(self, status, fn, *a, **kw):
        with self.assertRaises(api.HTTPException) as raised:
            fn(*a, **kw)
        self.assertEqual(raised.exception.status_code, status, raised.exception.detail)
        return raised.exception

    def snapshot(self):
        return (len(self.store.list_source_records("acme", "customer")), sorted((c.id, c.status) for c in self.store.list_candidates("acme", "customer", status=None)),
                sorted(e.id for e in self.store.list_entities("acme", "customer", "active")))


class TestAProposalChangesNothing(Wired):
    def test_the_item_stays_pending_nothing_moves_and_the_queue_is_as_it_was(self):
        cid = self.item()
        before, queue = self.snapshot(), api.review_queue("customer", principal=self.steward)
        out = api.propose_decision("customer", cid, propose(), principal=self.agent)
        self.assertEqual((out["recommendation"], out["confidence"], out["outcome"]), ("confirm", 0.9, None))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.store.get_candidate(cid, "acme", "customer").status, "pending")
        self.assertEqual(api.review_queue("customer", principal=self.steward), queue)                          # the queue endpoint is untouched, down to its keys
        self.assertEqual(set(queue["items"][0]), {"candidate_id", "score", "match_type", "reasons", "contributions", "record_a", "record_b", "created_at"})

    def test_it_is_recorded_under_the_keys_name_and_the_audit_log_does_not_carry_the_reasons(self):
        cid = self.item()
        out = api.propose_decision("customer", cid, propose(reasons=["Ann Lee's email ann@x.com matches"]), principal=P(label="agent-7"))
        self.assertEqual(out["agent"], "agent:agent-7")
        log = api.get_audit_log(domain="customer", principal=self.admin)["entries"]
        entry = next(e for e in log if e["action"] == "propose_decision")
        self.assertEqual((entry["actor"], entry["details"]["recommendation"], entry["details"]["agent"]), ("agent-7", "confirm", "agent:agent-7"))
        self.assertNotIn("ann@x.com", json.dumps(entry))                                                       # what an agent writes may quote the data: it is not logged

    def test_proposing_again_replaces_it_and_a_new_prompt_version_or_agent_adds_an_opinion(self):
        cid = self.item()
        first = api.propose_decision("customer", cid, propose(recommendation="confirm", confidence=0.9), principal=self.agent)
        again = api.propose_decision("customer", cid, propose(recommendation="reject", confidence=0.4), principal=self.agent)
        self.assertEqual((again["recommendation_id"], again["recommendation"], again["confidence"]), (first["recommendation_id"], "reject", 0.4))
        api.propose_decision("customer", cid, propose(prompt_version="v2"), principal=self.agent)
        api.propose_decision("customer", cid, propose(), principal=P(label="agent-2"))
        listed = api.item_recommendations("customer", cid, principal=self.steward)["items"]
        self.assertEqual(len(listed), 3)
        self.assertEqual(listed, sorted(listed, key=lambda r: (r["created_at"], r["recommendation_id"]), reverse=True))        # newest first


class TestWhoMayProposeAndAbout(Wired):
    def test_the_agent_and_the_administrators_may_a_steward_or_a_reader_may_not(self):
        cid = self.item()
        for principal in (self.agent, P("domain_admin", "Dom"), self.admin):
            api.propose_decision("customer", cid, propose(model=principal.label), principal=principal)
        for role in ("data_steward", "api_consumer"):
            with self.assertRaises(AuthError):
                api.propose_decision("customer", cid, propose(), principal=P(role, "x"))

    def test_a_key_limited_to_another_domain_is_refused(self):
        cid = self.item()
        with self.assertRaises(AuthError):
            api.propose_decision("customer", cid, propose(), principal=P(allowed=["product"]))

    def test_an_item_that_does_not_exist_or_belongs_to_another_tenant_is_a_404(self):
        cid = self.item()
        self.refused(404, api.propose_decision, "customer", "cand_nope", propose(), principal=self.agent)
        self.refused(404, api.propose_decision, "customer", cid, propose(), principal=P(tenant="other"))
        self.refused(404, api.item_recommendations, "customer", cid, principal=P(tenant="other"))
        self.refused(404, api.propose_decision, "no_such_domain", cid, propose(), principal=self.agent)
        self.assertEqual(self.store.list_recommendations_for("other", "customer", cid), [])

    def test_an_item_a_person_already_decided_cannot_be_recommended_about(self):
        cid = self.item()
        resolve_candidate(self.store, "acme", "customer", cid, "confirm", "steward:Pat")
        self.refused(409, api.propose_decision, "customer", cid, propose(), principal=self.agent)

    def test_another_tenant_sees_none_of_it(self):
        cid = self.item()
        api.propose_decision("customer", cid, propose(), principal=self.agent)
        stranger = P("steward_agent", "agent-1", tenant="other")
        self.assertEqual(api.list_recommendations("customer", principal=stranger)["items"], [])
        self.assertIsNone(api.recommendation_stats("customer", principal=stranger)["agreement_rate"])


class TestTheRulesAProposalMustMeet(Wired):
    BAD = [dict(recommendation="merge_everything"), dict(recommendation=""), dict(confidence=1.01), dict(confidence=-0.1), dict(confidence="high"), dict(confidence=True),
           dict(reasons=["x"] * 11), dict(reasons=["x" * 301]), dict(reasons=[""]), dict(reasons=[5]), dict(evidence=["e"] * 21), dict(evidence=["e" * 121]),
           dict(model="m" * 81), dict(prompt_version="v" * 41)]

    def test_each_broken_rule_is_a_400_from_the_endpoint_whatever_the_web_framework_checks(self):
        """The framework may already refuse some of these while the request is built (a real server answers 422); the endpoint must not depend on that, so it gets a plain object."""
        import types
        cid = self.item()
        fine = dict(recommendation="confirm", confidence=0.9, reasons=["r"], evidence=["e"], model="m", prompt_version="v")
        for kw in self.BAD:
            self.refused(400, api.propose_decision, "customer", cid, types.SimpleNamespace(**{**fine, **kw}), principal=self.agent)
        self.assertEqual(self.store.list_recommendations_for("acme", "customer", cid), [])

    def test_the_framework_also_refuses_the_ones_it_can_see(self):
        with self.assertRaises(Exception):
            propose(recommendation="merge_everything")

    def test_the_rules_themselves(self):
        self.assertEqual(recommendations.check_proposal("confirm", 0.5, ["r"], ["e"], "m", "v"), [])
        for kw in self.BAD:
            fine = dict(recommendation="confirm", confidence=0.9, reasons=["r"], evidence=["e"], model="m", prompt_version="v")
            self.assertTrue(recommendations.check_proposal(**{**fine, **kw}), kw)

    def test_the_edges_that_are_allowed_are_allowed(self):
        cid = self.item()
        api.propose_decision("customer", cid, propose(confidence=0, reasons=[], evidence=[], model="", prompt_version=""), principal=P(label="a"))
        api.propose_decision("customer", cid, propose(confidence=1, reasons=["x" * 300] * 10, evidence=["e" * 120] * 20, model="m" * 80, prompt_version="v" * 40), principal=P(label="b"))
        api.propose_decision("customer", cid, propose(recommendation="needs_human"), principal=P(label="c"))
        self.assertEqual(len(api.item_recommendations("customer", cid, principal=self.steward)["items"]), 3)


class TestAPersonsDecisionGivesTheOutcome(Wired):
    def test_agreed_disagreed_and_human_decided_recorded_once_under_the_stewards_name(self):
        cid = self.item()
        api.propose_decision("customer", cid, propose(recommendation="confirm"), principal=P(label="agent-a"))
        api.propose_decision("customer", cid, propose(recommendation="reject"), principal=P(label="agent-b"))
        api.propose_decision("customer", cid, propose(recommendation="needs_human"), principal=P(label="agent-c"))
        api.resolve("customer", cid, api.ResolveCandidateRequest(decision="confirm"), principal=self.steward)
        got = {r["agent"]: (r["outcome"], r["outcome_decision"], r["outcome_by"]) for r in api.item_recommendations("customer", cid, principal=self.steward)["items"]}
        self.assertEqual(got, {"agent:agent-a": ("agreed", "confirm", "steward:Pat"), "agent:agent-b": ("disagreed", "confirm", "steward:Pat"),
                               "agent:agent-c": ("human_decided", "confirm", "steward:Pat")})

    def test_a_reject_is_judged_the_same_way_and_another_items_recommendations_are_left_alone(self):
        one, two = self.item(0), self.item(1)
        api.propose_decision("customer", one, propose(recommendation="reject"), principal=self.agent)
        api.propose_decision("customer", two, propose(recommendation="reject"), principal=self.agent)
        api.resolve("customer", one, api.ResolveCandidateRequest(decision="reject"), principal=self.steward)
        self.assertEqual(api.item_recommendations("customer", one, principal=self.steward)["items"][0]["outcome"], "agreed")
        self.assertIsNone(api.item_recommendations("customer", two, principal=self.steward)["items"][0]["outcome"])

    def test_a_decision_made_without_the_api_counts_too(self):
        cid = self.item()
        api.propose_decision("customer", cid, propose(recommendation="confirm"), principal=self.agent)
        resolve_candidate(self.store, "acme", "customer", cid, "reject", "steward:Lee")             # the command line, a script: the same function
        self.assertEqual(api.item_recommendations("customer", cid, principal=self.steward)["items"][0]["outcome"], "disagreed")

    def test_an_item_nobody_recommended_about_is_resolved_as_before(self):
        cid = self.item()
        self.assertEqual(api.resolve("customer", cid, api.ResolveCandidateRequest(decision="confirm"), principal=self.steward)["status"], "confirmed")


class TestWhetherThePersonSawTheSuggestion(Wired):
    def decide(self, cid, decision, **kw):
        return api.resolve("customer", cid, api.ResolveCandidateRequest(decision=decision, **kw), principal=self.steward)

    def test_it_is_recorded_as_opened_not_opened_or_not_known(self):
        a, b, c = self.item(0), self.item(1), self.item(2)
        for cid in (a, b, c):
            api.propose_decision("customer", cid, propose(), principal=self.agent)
        self.decide(a, "confirm", suggestion_viewed=True)
        self.decide(b, "confirm", suggestion_viewed=False)
        self.decide(c, "confirm")                                                                     # an older client, a script: not known
        seen = {cid: api.item_recommendations("customer", cid, principal=self.steward)["items"][0]["outcome_seen"] for cid in (a, b, c)}
        self.assertEqual(seen, {a: True, b: False, c: None})

    def test_the_audit_log_carries_it_only_when_it_was_sent(self):
        a, b = self.item(0), self.item(1)
        self.decide(a, "confirm", suggestion_viewed=True)
        self.decide(b, "confirm")
        entries = {e["details"]["candidate_id"]: e["details"] for e in api.get_audit_log(domain="customer", principal=self.admin)["entries"] if e["action"] == "resolve_review_queue"}
        self.assertIs(entries[a]["suggestion_viewed"], True)
        self.assertNotIn("suggestion_viewed", entries[b])

    def test_agreement_is_reported_apart_for_decisions_made_blind_and_after_reading_it(self):
        items = [self.item(i) for i in range(5)]
        for cid in items:
            api.propose_decision("customer", cid, propose(recommendation="confirm"), principal=self.agent)
        self.decide(items[0], "confirm", suggestion_viewed=False)                                    # blind, agreed
        self.decide(items[1], "reject", suggestion_viewed=False)                                     # blind, disagreed
        self.decide(items[2], "confirm", suggestion_viewed=True)                                     # after reading it, agreed
        self.decide(items[3], "confirm")                                                              # not known, agreed
        v = api.recommendation_stats("customer", principal=self.steward)["by_viewing"]
        self.assertEqual((v["decided_blind"]["decided"], v["decided_blind"]["agreed"], v["decided_blind"]["agreement_rate"]), (2, 1, 0.5))
        self.assertEqual((v["decided_after_viewing"]["decided"], v["decided_after_viewing"]["agreement_rate"]), (1, 1.0))
        self.assertEqual((v["viewing_not_known"]["decided"], v["viewing_not_known"]["agreement_rate"]), (1, 1.0))
        self.assertEqual(v["decided_blind"]["lower_bound_95"], recommendations.wilson_lower(1, 2))
        self.assertEqual(api.recommendation_stats("customer", principal=self.steward)["agreement_rate"], 0.75)      # the overall figure is still there, and still all of them

    def test_the_older_four_value_rows_still_summarise_as_not_known(self):
        s = recommendations.summarize([("confirm", "agreed", 0.9, 3), ("confirm", "disagreed", 0.9, 1)])
        self.assertEqual((s["decided"], s["by_viewing"]["viewing_not_known"]["decided"], s["by_viewing"]["decided_blind"]["decided"]), (4, 4, 0))
        self.assertIsNone(recommendations.summarize([])["by_viewing"]["decided_blind"]["agreement_rate"])             # nothing decided: no rate, not a made-up one

    def test_the_note_says_which_figure_is_the_unbiased_one(self):
        self.assertIn("only decided_blind", recommendations.summarize([])["note"])


class TestTheStatistics(Wired):
    def test_agreement_and_calibration_from_what_people_decided(self):
        items = [self.item(i) for i in range(4)]
        api.propose_decision("customer", items[0], propose(recommendation="confirm", confidence=0.95), principal=self.agent)       # the person agrees
        api.propose_decision("customer", items[1], propose(recommendation="confirm", confidence=0.8), principal=self.agent)        # the person disagrees
        api.propose_decision("customer", items[2], propose(recommendation="needs_human", confidence=0.2), principal=self.agent)    # the agent said it could not tell
        api.propose_decision("customer", items[3], propose(recommendation="reject", confidence=0.6), principal=self.agent)         # nobody has decided yet
        api.resolve("customer", items[0], api.ResolveCandidateRequest(decision="confirm"), principal=self.steward)
        api.resolve("customer", items[1], api.ResolveCandidateRequest(decision="reject"), principal=self.steward)
        api.resolve("customer", items[2], api.ResolveCandidateRequest(decision="confirm"), principal=self.steward)
        s = api.recommendation_stats("customer", principal=self.steward)
        self.assertEqual((s["total"], s["open"], s["decided"], s["agreed"], s["agreement_rate"]), (4, 1, 2, 1, 0.5))
        self.assertEqual(s["by_recommendation"]["needs_human"], {"agreed": 0, "disagreed": 0, "human_decided": 1, "open": 0})        # not counted as agreeing or disagreeing
        by_bucket = {b["confidence"]: (b["decided"], b["agreed"], b["agreement_rate"]) for b in s["calibration"]}
        self.assertEqual(by_bucket, {"0.0-0.5": (0, 0, None), "0.5-0.7": (0, 0, None), "0.7-0.9": (1, 0, 0.0), "0.9-1.0": (1, 1, 1.0)})

    def test_with_nothing_decided_there_is_no_rate_not_a_made_up_one(self):
        s = api.recommendation_stats("customer", principal=self.steward)
        self.assertEqual((s["total"], s["decided"], s["agreement_rate"]), (0, 0, None))
        cid = self.item()
        api.propose_decision("customer", cid, propose(), principal=self.agent)
        self.assertIsNone(api.recommendation_stats("customer", principal=self.steward)["agreement_rate"])

    def test_the_bucket_edges(self):
        rows = [("confirm", "agreed", 0.0, 1), ("confirm", "agreed", 0.5, 1), ("confirm", "disagreed", 0.7, 1), ("confirm", "agreed", 0.9, 1), ("confirm", "agreed", 1.0, 1)]
        by = {b["confidence"]: b["decided"] for b in recommendations.summarize(rows)["calibration"]}
        self.assertEqual(by, {"0.0-0.5": 1, "0.5-0.7": 1, "0.7-0.9": 1, "0.9-1.0": 2})                       # 0.5 starts the second bucket, 1.0 is in the last

    def test_who_may_read_the_statistics_and_the_lists(self):
        for role in ("data_steward", "api_consumer", "steward_agent"):
            api.recommendation_stats("customer", principal=P(role, "x"))
            api.list_recommendations("customer", principal=P(role, "x"))


class TestThePages(Wired):
    def test_a_page_at_a_time_newest_first_each_once_and_filtered_by_outcome(self):
        items = [self.item(i) for i in range(4)]
        for i, cid in enumerate(items):
            api.propose_decision("customer", cid, propose(model=f"m{i}"), principal=self.agent)
        api.resolve("customer", items[0], api.ResolveCandidateRequest(decision="confirm"), principal=self.steward)
        everything = api.list_recommendations("customer", limit=200, principal=self.steward)["items"]
        self.assertEqual(len(everything), 4)
        walked, cursor = [], None
        while True:
            page = api.list_recommendations("customer", limit=1, cursor=cursor, principal=self.steward)
            walked += [r["recommendation_id"] for r in page["items"]]
            cursor = page["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(walked, [r["recommendation_id"] for r in everything])
        self.assertEqual(len(api.list_recommendations("customer", outcome="open", principal=self.steward)["items"]), 3)
        self.assertEqual(len(api.list_recommendations("customer", outcome="agreed", principal=self.steward)["items"]), 1)

    def test_a_bad_outcome_or_cursor_is_a_400_and_the_limit_is_clamped(self):
        self.refused(400, api.list_recommendations, "customer", outcome="whatever", principal=self.steward)
        self.refused(400, api.list_recommendations, "customer", cursor="not-a-cursor", principal=self.steward)
        self.assertEqual(api.list_recommendations("customer", limit=10 ** 6, principal=self.steward)["limit"], 200)


class TestRightToBeForgotten(Wired):
    def test_a_purge_deletes_what_an_agent_wrote_about_the_purged_records_and_nothing_else(self):
        gone, kept = self.item(0), self.item(1)
        api.propose_decision("customer", gone, propose(reasons=["Ann Lee at 1 Elm Street"]), principal=self.agent)
        api.propose_decision("customer", kept, propose(), principal=self.agent)
        the_group = next(e for e in self.store.list_entities("acme", "customer", "active") if e.attributes.get("first_name") == "Ann" and len(
            self.store.list_source_records("acme", "customer", entity_id=e.id)) == 2)
        purge_entity(self.store, "acme", "customer", the_group.id, "dpo")
        self.assertEqual(self.store.list_recommendations_for("acme", "customer", gone), [])
        self.assertEqual(len(self.store.list_recommendations_for("acme", "customer", kept)), 1)
        entry = next(e for e in api.get_audit_log(domain="customer", principal=self.admin)["entries"] if e["action"] == "purge")
        self.assertEqual(entry["details"]["agent_recommendations_deleted"], 1)


if __name__ == "__main__":
    unittest.main()
