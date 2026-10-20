"""
REPLAY (app/replay.py, app/replay_agents.py, replay_decisions.py): how would an agent have done on the review items people already decided?

The thing these tests exist to protect is that the evidence handed to an agent does NOT give the answer away. Looked at today, a decided pair carries its verdict, its decider, and
(if confirmed) a merge of the two groups. The evidence is rebuilt as it stood at the decision, and when that cannot be done exactly the item is refused, not guessed.

  * WHO is replayed: pairs a person decided in review; not automatic merges, not verdicts on them, not unmerges, not what is still pending
  * the evidence is BLINDED and AS OF THE DECISION: groups separate, no verdict, versions as they were, separations only if made before; refused when inexact; masked
  * the AGENTS: the score rule, any command, the LLM agent (its request, its parsing, its retries, over a real HTTP round trip to a stub)
  * the RUN writes nothing, reuses finished work, and records skipped items and agent errors apart
  * the REPORT's numbers
  * the COMMAND LINE: read-only, resumable, refuses to send data to a model without a yes

Run with:  python3 -m unittest tests.test_replay -v
"""
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import unittest.mock as mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(__file__))

from app import evidence as live_evidence, replay  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import MatchCandidate  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate  # noqa: E402
from app.replay_agents import AgentError, CommandAgent, LlmAgent, RuleAgent, _Retryable  # noqa: E402
from app.stewardship import give_verdict, unmerge_pair  # noqa: E402

TENANT, DOMAIN = "acme", "customer"
LONG_AGO, FAR_FUTURE = "2000-01-01T00:00:00+00:00", "2999-01-01T00:00:00+00:00"
PEOPLE = [("Ann", "Lee", "30301"), ("Bob", "Moss", "31201"), ("Cara", "Quinn", "30030"), ("Dev", "Patel", "30606"), ("Eve", "Stone", "31401")]
KEEP = lambda attrs: attrs


def dump(path):
    conn = sqlite3.connect(path)
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.path = os.path.join(self.tmp, "r.db")
        self.store = Store(self.path)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel", "MDM_MODEL_DIR": os.path.join(self.tmp, "models")})
        env.start()
        self.addCleanup(env.stop)

    def base(self, i):
        first, last, zip_ = PEOPLE[i]
        return {"first_name": first, "last_name": last, "email": f"{first.lower()}@x.com", "phone": f"404-555-01{i}0", "address_line1": f"{i + 1} Elm Street", "city": "Atlanta", "state": "GA",
                "postal_code": zip_}

    def item(self, i=0):
        """A review item: a group of two records of one person and a record resembling them. Returns (candidate id, the queued record's source id, the base attributes)."""
        base, first = self.base(i), PEOPLE[i][0]
        for n in (1, 2):
            ingest_record(self.store, TENANT, DOMAIN, "crm", f"{first}{n}", base)
        r = ingest_record(self.store, TENANT, DOMAIN, "crm", f"{first}X", {**base, "email": f"other.{first.lower()}@z.com", "phone": "", "last_name": base["last_name"] + "h"})
        self.assertEqual(len(r["queued_for_review"]), 1, r)
        return r["queued_for_review"][0]["candidate_id"], f"{first}X", base

    def decide(self, cid, decision, by="steward:Pat"):
        resolve_candidate(self.store, TENANT, DOMAIN, cid, decision, by)
        return self.store.get_candidate(cid, TENANT, DOMAIN)

    def evidence(self, candidate, **kw):
        return replay.build_blinded_evidence(self.store, TENANT, DOMAIN, candidate, KEEP, **kw)

    def record(self, source_id):
        return next(r for r in self.store.list_source_records(TENANT, DOMAIN) if r.source_record_id == source_id)


def key_paths(value, prefix=""):
    """Every key path of a nested structure (list positions ignored): its SHAPE, which must not depend on what the person decided."""
    out = set()
    if isinstance(value, dict):
        for k, v in value.items():
            out.add(prefix + "/" + k)
            out |= key_paths(v, prefix + "/" + k)
    elif isinstance(value, list):
        for v in value:
            out |= key_paths(v, prefix + "[]")
    return out


# =================================================================================================== who is replayed
class TestWhoIsReplayed(Base):
    def test_only_what_a_person_decided_in_review_is_replayed(self):
        confirmed, _, _ = self.item(0)
        rejected, _, _ = self.item(1)
        self.item(2)                                                                   # still pending
        self.decide(confirmed, "confirm", "steward:Pat")
        self.decide(rejected, "reject", "steward:Lee")
        auto = self.base(3)                                                            # an AUTOMATIC merge, decided by the system
        ingest_record(self.store, TENANT, DOMAIN, "crm", "auto1", auto)
        ingest_record(self.store, TENANT, DOMAIN, "crm", "auto2", auto)
        system_decided = [c for c in self.store.list_candidates(TENANT, DOMAIN, "confirmed") if (c.decided_by or "").startswith("system:")]
        self.assertTrue(system_decided, "the fixture must make an automatic merge")
        audited = system_decided[0]                                                    # an automatic merge a person checked: an audit verdict, a different question
        self.store.flag_merge_audits(TENANT, DOMAIN, [audited.id])
        give_verdict(self.store, TENANT, DOMAIN, audited.id, "correct", "steward:Pat")
        other = self.base(4)                                                           # an automatic merge a person UNMERGED
        ingest_record(self.store, TENANT, DOMAIN, "crm", "un1", other)
        ingest_record(self.store, TENANT, DOMAIN, "crm", "un2", other)
        unmerged = next(c for c in self.store.list_candidates(TENANT, DOMAIN, "confirmed") if c.id != audited.id and (c.decided_by or "").startswith("system:"))
        unmerge_pair(self.store, TENANT, DOMAIN, unmerged.id, "jane", "different people")
        items = replay.labelled_items(self.store, TENANT, DOMAIN)
        self.assertEqual(sorted((i.candidate.id, i.label) for i in items), sorted([(confirmed, "confirm"), (rejected, "reject")]))

    def test_sampling_is_repeatable_by_seed_and_returns_everything_when_asked_for_more(self):
        for i in range(4):
            self.decide(self.item(i)[0], "confirm")
        items = replay.labelled_items(self.store, TENANT, DOMAIN)
        pick = lambda seed: [i.candidate.id for i in replay.sample_items(items, 2, seed)]
        self.assertEqual(pick(7), pick(7))
        self.assertEqual(len(pick(7)), 2)
        self.assertTrue(any(pick(s) != pick(7) for s in range(8, 30)))                   # a different seed can choose differently
        self.assertEqual(len(replay.sample_items(items, 99, 1)), 4)
        self.assertEqual(len(replay.sample_items(items, None, 1)), 4)


# =================================================================================================== the evidence is blinded, as of the decision
class TestTheEvidenceDoesNotGiveTheAnswerAway(Base):
    def test_a_confirmed_pair_shows_the_two_groups_as_they_were_not_the_merged_one(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "confirm")
        self.assertEqual(self.record("AnnX").entity_id, self.record("Ann1").entity_id)          # today they ARE one entity: that is the leak
        e = self.evidence(decided)
        self.assertEqual((e["group_a"]["member_count"], e["group_b"]["member_count"]), (1, 2))
        self.assertEqual((e["group_a"]["golden"]["last_name"], e["group_b"]["golden"]["last_name"]), ("Leeh", "Lee"))     # each side's golden record, as it was
        self.assertFalse(e["same_entity"])
        self.assertEqual((e["candidate"]["status"], e["candidate"]["decided_by"]), ("pending", None))
        self.assertIsNone(e["record_a"]["entity_id"])
        self.assertEqual(e["other_pairs_of_record_a"], [])
        self.assertEqual(e["reconstruction"], "exact")

    def test_nothing_that_names_the_decision_the_decider_or_the_time_is_in_it(self):
        cid, _, _ = self.item(0)
        e = self.evidence(self.decide(cid, "confirm", "steward:Pat"))
        text = json.dumps(e)
        self.assertNotIn("steward:Pat", text)
        self.assertNotIn("confirmed", text)
        self.assertNotIn(self.store.get_candidate(cid, TENANT, DOMAIN).decided_at, text.replace(e["as_of"], ""))      # the decision time appears only as `as_of`, the moment the picture is of

    def test_a_confirmed_and_a_rejected_pair_have_exactly_the_same_shape(self):
        confirmed, _, _ = self.item(0)
        rejected, _, _ = self.item(1)
        a = self.evidence(self.decide(confirmed, "confirm"))
        b = self.evidence(self.decide(rejected, "reject"))
        self.assertEqual(key_paths(a), key_paths(b))
        self.assertEqual((b["group_a"]["member_count"], b["group_b"]["member_count"]), (1, 2))

    def test_it_has_the_same_fields_as_the_live_evidence_so_one_agent_works_on_both(self):
        pending, _, _ = self.item(0)
        live = live_evidence.build_pair_evidence(self.store, TENANT, DOMAIN, pending, KEEP)
        replayed = self.evidence(self.decide(pending, "reject"))
        self.assertEqual(set(replayed) - set(live), {"as_of", "reconstruction", "notes"})
        self.assertEqual(set(live) - set(replayed), set())
        for part in ("candidate", "record_a", "group_a", "separations", "history"):
            self.assertEqual(set(replayed[part]), set(live[part]), part)

    def test_a_record_is_shown_as_it_was_when_the_person_decided(self):
        cid, _, base = self.item(0)
        r = ingest_record(self.store, TENANT, DOMAIN, "crm", "AnnX", {**base, "email": "other.ann@z.com", "phone": "", "last_name": "Leeh", "city": "Decatur"})      # it changes BEFORE the decision
        cid = r["queued_for_review"][0]["candidate_id"]
        e = self.evidence(self.decide(cid, "confirm"))
        self.assertEqual((e["record_a"]["version"], e["record_a"]["attributes"]["city"]), (2, "Decatur"))
        self.assertEqual([(h["version"], h["attributes"]["city"]) for h in e["history"]["record_a"]], [(1, "Atlanta")])
        self.assertEqual(e["reconstruction"], "exact")

    def test_a_record_that_changed_after_the_decision_makes_it_inexact_and_is_refused_unless_allowed(self):
        cid, _, base = self.item(0)
        decided = self.decide(cid, "confirm")
        ingest_record(self.store, TENANT, DOMAIN, "crm", "AnnX", {**base, "email": "other.ann@z.com", "phone": "", "last_name": "Leeh", "city": "Macon"})        # changes AFTER
        with self.assertRaises(replay.Unreconstructable) as caught:
            self.evidence(decided)
        self.assertIn("changed after the decision", str(caught.exception))
        e = self.evidence(decided, allow_approximate=True)
        self.assertEqual((e["reconstruction"], e["record_a"]["attributes"]["city"], e["record_a"]["version"]), ("approximate", "Atlanta", 1))       # still shown as it WAS
        self.assertEqual(e["group_a"]["golden"]["city"], "Atlanta")                           # and so is the golden record built from it, not from what the record says now
        self.assertTrue(e["notes"])

    def test_a_record_that_joined_a_group_after_the_decision_is_not_in_it(self):
        cid, _, base = self.item(0)
        decided = self.decide(cid, "confirm")
        ingest_record(self.store, TENANT, DOMAIN, "crm", "Ann3", base)                         # a third record of the same person arrives LATER and joins the group
        self.assertEqual(len({r.entity_id for r in self.store.list_source_records(TENANT, DOMAIN)}), 1)
        e = self.evidence(decided)
        self.assertEqual((e["group_a"]["member_count"], e["group_b"]["member_count"]), (1, 2))   # the picture is of the moment of the decision
        self.assertEqual(sorted(m["source_record_id"] for m in e["group_b"]["members"]), ["Ann1", "Ann2"])
        self.assertEqual(e["reconstruction"], "exact")

    def test_a_separation_counts_only_if_a_person_made_it_before_the_decision(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "reject")
        b, a1 = self.record("AnnX"), self.record("Ann1")

        def separate(when):
            self.store.save_candidate(MatchCandidate(record_a_id=b.id, record_b_id=a1.id, tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy", status="rejected",
                                                     decided_by="unmerge:jane", decided_at=when))
        separate(LONG_AGO)
        e = self.evidence(decided)
        self.assertTrue(e["separations"]["a_was_separated_from_a_record_in_group_b"])
        self.assertEqual(e["reconstruction"], "exact")

    def test_an_unmerge_after_the_decision_is_not_shown_and_makes_it_inexact(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "reject")
        b, a1 = self.record("AnnX"), self.record("Ann1")
        self.store.save_candidate(MatchCandidate(record_a_id=b.id, record_b_id=a1.id, tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy", status="rejected",
                                                 decided_by="unmerge:jane", decided_at=FAR_FUTURE))
        with self.assertRaises(replay.Unreconstructable) as caught:
            self.evidence(decided)
        self.assertIn("unmerged after the decision", str(caught.exception))
        e = self.evidence(decided, allow_approximate=True)
        self.assertFalse(e["separations"]["a_was_separated_from_a_record_in_group_b"])           # a later separation must not leak into the earlier picture
        self.assertEqual(e["reconstruction"], "approximate")

    def test_two_records_still_joined_by_other_confirmed_pairs_cannot_be_told_apart(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "confirm")
        b, a2 = self.record("AnnX"), self.record("Ann2")
        self.store.save_candidate(MatchCandidate(record_a_id=b.id, record_b_id=a2.id, tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy", status="confirmed",
                                                 decided_by="steward:Lee", decided_at=LONG_AGO))
        with self.assertRaises(replay.Unreconstructable) as caught:
            self.evidence(decided)
        self.assertIn("connected by other confirmed pairs", str(caught.exception))

    def test_a_purged_record_is_refused_with_the_reason(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "confirm")
        self.store.delete_source_record(self.record("Ann1").id, TENANT, DOMAIN)
        with self.assertRaises(replay.Unreconstructable) as caught:
            self.evidence(decided)
        self.assertIn("purged", str(caught.exception))

    def test_protected_values_are_masked_everywhere_in_it(self):
        cid, _, _ = self.item(0)
        hide = lambda attrs: {k: ("***" if k == "email" else v) for k, v in attrs.items()}
        e = replay.build_blinded_evidence(self.store, TENANT, DOMAIN, self.decide(cid, "confirm"), hide)
        text = json.dumps({k: v for k, v in e.items() if k != "candidate"})                  # the candidate's reasons are text the scorer wrote (see tests/test_evidence.py)
        self.assertNotIn("ann@x.com", text)
        self.assertNotIn("other.ann@z.com", text)
        self.assertIn("***", text)

    def test_a_large_group_is_cut_to_a_page_of_members_and_says_so(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "confirm")
        with mock.patch.object(replay, "MAX_MEMBERS", 1):
            e = self.evidence(decided)
        self.assertEqual((e["group_b"]["member_count"], len(e["group_b"]["members"]), e["group_b"]["truncated"]), (2, 1, True))


# =================================================================================================== the agents
class TestTheRuleAgent(unittest.TestCase):
    def ask(self, score, **kw):
        return RuleAgent(**kw).recommend({"candidate": {"score": score}})

    def test_confirm_reject_or_cannot_tell_by_the_score(self):
        self.assertEqual((self.ask(0.9)["recommendation"], self.ask(0.9)["confidence"]), ("confirm", 0.9))
        self.assertEqual((self.ask(0.3)["recommendation"], self.ask(0.3)["confidence"]), ("reject", 0.7))
        self.assertEqual((self.ask(0.7)["recommendation"], self.ask(0.7)["confidence"]), ("needs_human", 0.5))
        self.assertEqual(self.ask(0.85)["recommendation"], "confirm")                        # the cut-offs belong to the verdicts
        self.assertEqual(self.ask(0.55)["recommendation"], "reject")
        self.assertEqual(self.ask(0.7, hi=0.7)["recommendation"], "confirm")
        self.assertEqual(replay.recommendations.check_proposal(**{k: self.ask(0.9)[k] for k in ("recommendation", "confidence", "reasons", "evidence", "model", "prompt_version")}), [])


class TestTheCommandAgent(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def script(self, body):
        path = os.path.join(self.tmp, "agent.py")
        open(path, "w").write(body)
        return f'"{sys.executable}" "{path}"'

    def test_the_evidence_goes_in_on_standard_input_and_the_answer_comes_out(self):
        agent = CommandAgent(self.script("import sys, json\nev = json.load(sys.stdin)\nprint(json.dumps({'recommendation': 'confirm', 'confidence': ev['candidate']['score'], 'reasons': ['r']}))\n"))
        self.assertEqual(agent.recommend({"candidate": {"score": 0.77}}), {"recommendation": "confirm", "confidence": 0.77, "reasons": ["r"]})

    def test_a_failing_silent_slow_or_missing_command_is_an_agent_error_not_a_crash(self):
        for body, fragment in (("import sys\nsys.exit(3)\n", "exit 3"), ("print('not json')\n", "one JSON object"), ("print('[1, 2]')\n", "one JSON object"),
                               ("import sys\nsys.stderr.write('boom')\nsys.exit(1)\n", "boom")):
            with self.assertRaises(AgentError) as caught:
                CommandAgent(self.script(body)).recommend({})
            self.assertIn(fragment, str(caught.exception))
        with self.assertRaises(AgentError) as caught:
            CommandAgent(self.script("import time\ntime.sleep(5)\n"), timeout=0.5).recommend({})
        self.assertIn("did not answer", str(caught.exception))
        with self.assertRaises(AgentError):
            CommandAgent("/no/such/program").recommend({})
        with self.assertRaises(ValueError):
            CommandAgent("   ")


class StubModelApi:
    """A tiny HTTP server standing in for the Messages API: it records what it was sent and answers from a queue."""

    def __init__(self, answers):
        outer = self
        self.seen, self.answers = [], list(answers)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                outer.seen.append({"path": self.path, "headers": dict(self.headers), "body": json.loads(raw)})
                status, body = outer.answers.pop(0) if outer.answers else (500, {})
                out = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def says(text):
    return (200, {"content": [{"type": "text", "text": text}]})


GOOD = '{"recommendation": "confirm", "confidence": 0.9, "reasons": ["same person"], "evidence": ["group_b.members"]}'


class TestTheLlmAgent(unittest.TestCase):
    EVIDENCE = {"candidate": {"score": 0.7, "decided_by": None}, "record_a": {"attributes": {"first_name": "Ann"}}}

    def agent(self, post=None, **kw):
        return LlmAgent("model-x", api_key="sk-test-secret", post=post, sleep=lambda s: None, **kw)

    def test_the_request_names_the_model_warns_that_record_text_is_data_and_carries_the_evidence(self):
        sent = []
        self.agent(post=lambda body: sent.append(body) or {"content": [{"type": "text", "text": GOOD}]}).recommend(self.EVIDENCE)
        body = sent[0]
        self.assertEqual(body["model"], "model-x")
        self.assertIn("untrusted", body["system"])
        self.assertIn("needs_human", body["system"])
        user = body["messages"][0]["content"]
        self.assertEqual(json.loads(user[user.index("\n") + 1:]), self.EVIDENCE)
        self.assertNotIn("steward:", json.dumps(body))

    def test_what_it_understands_in_an_answer(self):
        agent = self.agent()
        for text in (GOOD, "```json\n" + GOOD + "\n```", "Here is my answer:\n" + GOOD + "\nHope that helps.", GOOD.replace("confirm", "CONFIRM")):
            got = agent.parse(text)
            self.assertEqual((got["recommendation"], got["confidence"], got["reasons"], got["model"]), ("confirm", 0.9, ["same person"], "model-x"))
        long = agent.parse('{"recommendation": "reject", "confidence": 0.2, "reasons": ["' + "x" * 500 + '"] }')
        self.assertEqual(len(long["reasons"][0]), 300)                                      # a wordy model is trimmed, not refused

    def test_what_it_refuses_to_guess_at(self):
        agent = self.agent()
        for text in ("I think they match.", "{not json}", "[1, 2]", '{"recommendation": "confirm"}', '{"confidence": 0.9}', ""):
            with self.assertRaises(AgentError):
                agent.parse(text)

    def test_it_retries_a_rate_limit_with_a_pause_and_gives_up_after_the_retries(self):
        calls, pauses = [], []
        def flaky(body):
            calls.append(1)
            if len(calls) < 3:
                raise _Retryable("HTTP 429")
            return {"content": [{"type": "text", "text": GOOD}]}
        agent = LlmAgent("m", api_key="k", post=flaky, sleep=pauses.append)
        self.assertEqual(agent.recommend(self.EVIDENCE)["recommendation"], "confirm")
        self.assertEqual((len(calls), pauses), (3, [1, 2]))
        always = LlmAgent("m", api_key="k", post=lambda body: (_ for _ in ()).throw(_Retryable("HTTP 529")), sleep=lambda s: None)
        with self.assertRaises(AgentError) as caught:
            always.recommend(self.EVIDENCE)
        self.assertIn("HTTP 529", str(caught.exception))

    def test_over_real_http_the_key_the_version_and_the_body_are_sent_and_a_refusal_is_not_retried(self):
        stub = StubModelApi([says(GOOD), (400, {"error": "bad"})])
        self.addCleanup(stub.stop)
        agent = LlmAgent("model-x", api_key="sk-test-secret", base_url=stub.url, sleep=lambda s: None)
        self.assertEqual(agent.recommend(self.EVIDENCE)["recommendation"], "confirm")
        sent = stub.seen[0]
        headers = {k.lower(): v for k, v in sent["headers"].items()}                         # header names are case-insensitive
        self.assertEqual(sent["path"], "/v1/messages")
        self.assertEqual((headers["x-api-key"], headers["anthropic-version"]), ("sk-test-secret", "2023-06-01"))
        self.assertEqual(sent["body"]["model"], "model-x")
        with self.assertRaises(AgentError) as caught:
            agent.recommend(self.EVIDENCE)
        self.assertIn("HTTP 400", str(caught.exception))
        self.assertNotIn("sk-test-secret", str(caught.exception))
        self.assertEqual(len(stub.seen), 2)                                                 # a 400 is a refusal, not worth another try

    def test_an_overloaded_api_is_retried_over_real_http(self):
        stub = StubModelApi([(529, {}), (429, {}), says(GOOD)])
        self.addCleanup(stub.stop)
        self.assertEqual(LlmAgent("m", api_key="k", base_url=stub.url, sleep=lambda s: None).recommend(self.EVIDENCE)["recommendation"], "confirm")
        self.assertEqual(len(stub.seen), 3)

    def test_without_a_key_or_a_model_it_says_so_and_a_dead_server_is_an_error_not_a_hang(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            with self.assertRaises(AgentError) as caught:
                LlmAgent("m", sleep=lambda s: None).recommend(self.EVIDENCE)
            self.assertIn("ANTHROPIC_API_KEY", str(caught.exception))
        with self.assertRaises(ValueError):
            LlmAgent("")
        with self.assertRaises(AgentError):
            LlmAgent("m", api_key="k", base_url="http://127.0.0.1:9", sleep=lambda s: None, timeout=0.5, retries=0).recommend(self.EVIDENCE)


# =================================================================================================== the run
class Counting:
    name = "counting"

    def __init__(self, answer=None):
        self.calls, self.answer = [], answer or {"recommendation": "confirm", "confidence": 0.9, "reasons": ["r"], "evidence": ["e"], "model": "m", "prompt_version": "v"}

    def recommend(self, evidence):
        self.calls.append(evidence["candidate"]["candidate_id"])
        return self.answer


class TestTheRun(Base):
    def test_it_writes_nothing_to_the_database(self):
        for i in range(3):
            self.decide(self.item(i)[0], "confirm" if i % 2 == 0 else "reject")
        before = dump(self.path)
        results = replay.run_replay(self.store, TENANT, DOMAIN, replay.labelled_items(self.store, TENANT, DOMAIN), RuleAgent(), KEEP)
        self.assertEqual(len(results), 3)
        self.assertEqual(dump(self.path), before)

    def test_skipped_items_and_agent_errors_are_recorded_apart_and_do_not_stop_the_run(self):
        good, bad, purged = self.item(0)[0], self.item(1)[0], self.item(2)[0]
        for cid in (good, bad, purged):
            self.decide(cid, "confirm")
        self.store.delete_source_record(self.record("CaraX").id, TENANT, DOMAIN)
        class Picky:
            name = "picky"
            def recommend(self, evidence):
                if evidence["record_a"]["source_record_id"] == "BobX":
                    raise AgentError("the model API did not answer (HTTP 529)")
                return Counting().answer
        by_id = {r["candidate_id"]: r for r in replay.run_replay(self.store, TENANT, DOMAIN, replay.labelled_items(self.store, TENANT, DOMAIN), Picky(), KEEP)}
        self.assertIn("recommendation", by_id[good])
        self.assertIn("HTTP 529", by_id[bad]["error"])
        self.assertIn("purged", by_id[purged]["skipped"])

    def test_an_answer_that_breaks_the_rules_is_an_error_not_a_recommendation(self):
        self.decide(self.item(0)[0], "confirm")
        for answer in ({"recommendation": "confirm", "confidence": 2}, {"recommendation": "maybe", "confidence": 0.5}, {"recommendation": "confirm", "confidence": True}):
            r = replay.run_replay(self.store, TENANT, DOMAIN, replay.labelled_items(self.store, TENANT, DOMAIN), Counting(answer), KEEP)[0]
            self.assertIn("broke the rules", r["error"])
            self.assertNotIn("recommendation", r)

    def test_work_already_done_is_reused_not_asked_again_and_only_new_work_is_reported(self):
        a, b = self.item(0)[0], self.item(1)[0]
        self.decide(a, "confirm")
        self.decide(b, "reject")
        items = replay.labelled_items(self.store, TENANT, DOMAIN)
        first = Counting()
        done = {r["candidate_id"]: r for r in replay.run_replay(self.store, TENANT, DOMAIN, items[:1], first, KEEP)}
        second, made = Counting(), []
        results = replay.run_replay(self.store, TENANT, DOMAIN, items, second, KEEP, done=done, on_result=made.append)
        self.assertEqual(len(results), 2)
        self.assertEqual(second.calls, [items[1].candidate.id])
        self.assertEqual([r["candidate_id"] for r in made], [items[1].candidate.id])

    def test_the_agent_is_given_exactly_what_the_blinded_builder_makes(self):
        cid = self.item(0)[0]
        decided = self.decide(cid, "confirm")
        seen = []
        class Spy:
            name = "spy"
            def recommend(self, evidence):
                seen.append(evidence)
                return Counting().answer
        replay.run_replay(self.store, TENANT, DOMAIN, replay.labelled_items(self.store, TENANT, DOMAIN), Spy(), KEEP)
        self.assertEqual(seen[0], self.evidence(decided))


# =================================================================================================== the report
def R(label, rec, conf=None, **kw):
    return {"candidate_id": f"c{R.n}", "label": label, "recommendation": rec, "confidence": conf, "score": kw.pop("score", 0.7), "match_type": "fuzzy", "quality": "exact", "group_sizes": [1, 2], **kw}


R.n = 0


def results(*rows):
    out = []
    for r in rows:
        R.n += 1
        out.append(R(*r[:3], **(r[3] if len(r) > 3 else {})))
    return out


class TestTheReport(unittest.TestCase):
    def test_wilson_bound_known_values(self):
        self.assertIsNone(replay.wilson_lower(0, 0))
        self.assertEqual(replay.wilson_lower(10, 10), 0.7225)
        self.assertEqual(replay.wilson_lower(50, 100), 0.4038)
        self.assertEqual(replay.wilson_lower(6, 8), 0.4093)          # by hand: (0.9901 - 0.3843) / 1.4802
        self.assertEqual(replay.wilson_lower(0, 5), 0.0)

    def test_the_numbers(self):
        rows = results(("confirm", "confirm", 0.95), ("confirm", "confirm", 0.95), ("confirm", "confirm", 0.9), ("reject", "confirm", 0.9), ("confirm", "confirm", 0.6),
                       ("reject", "reject", 0.8), ("reject", "reject", 0.8), ("confirm", "reject", 0.7), ("confirm", "needs_human", 0.5), ("reject", "needs_human", 0.5))
        rows += [{"candidate_id": "e1", "label": "confirm", "error": "x"}, {"candidate_id": "s1", "label": "confirm", "skipped": "purged"}]
        r = replay.summarize_replay(rows)
        self.assertEqual((r["items"], r["errors"], r["answered"], r["needs_human"], r["skipped"]), (12, 1, 10, 2, {"purged": 1}))
        self.assertEqual(r["agreement"], {"decided": 8, "agreed": 6, "rate": 0.75, "lower_bound_95": 0.4093})
        self.assertEqual(r["coverage"], 0.8)
        self.assertEqual(r["confusion"], {"confirm": {"confirm": 4, "reject": 1}, "reject": {"confirm": 1, "reject": 2}, "needs_human": {"confirm": 1, "reject": 1}})
        self.assertEqual(r["confirm"], {"precision": 0.8, "recall": 0.6667})                  # said confirm 5, 4 right; 6 truly confirm, 4 found
        at = {s["confidence_at_least"]: s for s in r["if_it_acted_only_above"]}
        self.assertEqual((at[0.9]["decided"], at[0.9]["agreed"], at[0.9]["agreement_rate"]), (4, 3, 0.75))
        self.assertEqual((at[0.99]["decided"], at[0.99]["agreement_rate"], at[0.99]["agreement_lower_bound_95"]), (0, None, None))
        self.assertEqual(at[0.5]["decided"], 8)
        by_bucket = {b["confidence"]: (b["decided"], b["agreed"]) for b in r["calibration"]}
        self.assertEqual(by_bucket["0.9-1.0"], (4, 3))                                       # confidence 0.9 or more: 2 x 0.95, 0.9 and 0.9: three agreed, one did not
        self.assertEqual(r["slices"]["match_type"]["fuzzy"]["decided"], 8)
        self.assertEqual(r["baselines"]["people_confirmed_share"], 0.6364)                     # 7 of the 11 not skipped

    def test_the_best_score_cut_is_the_bar_and_the_commoner_answer_is_the_floor(self):
        rows = results(("confirm", "confirm", 0.9, {"score": 0.9}), ("confirm", "confirm", 0.9, {"score": 0.8}), ("reject", "reject", 0.9, {"score": 0.6}), ("reject", "reject", 0.9, {"score": 0.5}),
                       ("confirm", "reject", 0.9, {"score": 0.55}))
        b = replay.summarize_replay(rows)["baselines"]
        # 'confirm at or above the cut': 0.55 and 0.7 both get 4 of 5 right; of tied cuts the LOWEST is reported
        self.assertEqual(b["best_score_cut_tuned_on_these_items"], {"threshold": 0.55, "accuracy": 0.8})
        self.assertEqual((b["people_confirmed_share"], b["always_the_commoner_answer"]), (0.6, 0.6))

    def test_slices_are_kept_apart_by_how_exact_the_evidence_was(self):
        rows = results(("confirm", "confirm", 0.9), ("confirm", "reject", 0.9, {"quality": "approximate"}), ("reject", "reject", 0.9, {"quality": "approximate"}))
        s = replay.summarize_replay(rows)["slices"]["reconstruction"]
        self.assertEqual((s["exact"]["decided"], s["exact"]["agreement_rate"]), (1, 1.0))
        self.assertEqual((s["approximate"]["decided"], s["approximate"]["agreement_rate"]), (2, 0.5))

    def test_nothing_replayed_gives_no_rates_not_made_up_ones_and_the_text_still_prints(self):
        r = replay.summarize_replay([])
        self.assertEqual((r["items"], r["agreement"]["rate"], r["coverage"], r["baselines"]["best_score_cut_tuned_on_these_items"]), (0, None, None, None))
        text = replay.format_report(r)
        self.assertIn("n/a", text)
        self.assertIn("Replayed 0 decided review items", text)

    def test_the_text_says_what_matters(self):
        rows = results(("confirm", "confirm", 0.95), ("reject", "reject", 0.95), ("confirm", "reject", 0.6), ("confirm", "needs_human", 0.5))
        text = replay.format_report(replay.summarize_replay(rows))
        for phrase in ("AGREEMENT with the person who decided: 2 of 3", "the best single score cut", "If it had acted only on its most confident answers", "Is its confidence honest?", "labels are noisy"):
            self.assertIn(phrase, text)


# =================================================================================================== the command line
class TestTheCommandLine(Base):
    def setUp(self):
        super().setUp()
        for i, d in enumerate(("confirm", "reject", "confirm")):
            self.decide(self.item(i)[0], d)
        self.env = {**os.environ, "MDM_AUDIT_RATE": "0"}
        self.env.pop("ANTHROPIC_API_KEY", None)
        self.env.pop("DATABASE_URL", None)

    def run_cli(self, *args, env=None):
        return subprocess.run([sys.executable, os.path.join(ROOT, "replay_decisions.py"), *args], capture_output=True, text=True, env=env or self.env, timeout=60)

    def test_it_reports_writes_nothing_and_says_so(self):
        before = dump(self.path)
        out = os.path.join(self.tmp, "results.jsonl")
        report = os.path.join(self.tmp, "report.json")
        done = self.run_cli("--db", self.path, "--tenant", TENANT, "--agent", "rule", "--out", out, "--json", report)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("READ-ONLY", done.stderr)
        self.assertIn("Replayed 3 decided review items", done.stdout)
        self.assertEqual(len(open(out).read().splitlines()), 3)
        self.assertEqual(json.load(open(report))["items"], 3)
        self.assertEqual(dump(self.path), before)

    def test_a_second_run_with_the_same_file_asks_for_nothing_again_and_report_only_needs_no_database(self):
        out = os.path.join(self.tmp, "results.jsonl")
        self.run_cli("--db", self.path, "--tenant", TENANT, "--agent", "rule", "--out", out)
        again = self.run_cli("--db", self.path, "--tenant", TENANT, "--agent", "rule", "--out", out)
        self.assertIn("3 of them are already in", again.stderr)
        self.assertEqual(len(open(out).read().splitlines()), 3)                              # nothing was added twice
        only = self.run_cli("--report-only", out)
        self.assertEqual(only.returncode, 0, only.stderr)
        self.assertIn("Replayed 3 decided review items", only.stdout)

    def test_any_program_can_be_the_agent(self):
        script = os.path.join(self.tmp, "always_confirm.py")
        open(script, "w").write("import json, sys\njson.load(sys.stdin)\nprint(json.dumps({'recommendation': 'confirm', 'confidence': 0.9, 'reasons': ['always']}))\n")
        done = self.run_cli("--db", self.path, "--tenant", TENANT, "--agent", "command", "--agent-command", f'"{sys.executable}" "{script}"')
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("AGREEMENT with the person who decided: 2 of 3", done.stdout)           # two of the three people confirmed

    def test_it_will_not_send_data_to_a_model_without_a_yes_a_model_and_a_key(self):
        base = ["--db", self.path, "--tenant", TENANT, "--agent", "llm"]
        no_model = self.run_cli(*base)
        self.assertEqual(no_model.returncode, 2)
        self.assertIn("--model", no_model.stderr)
        no_key = self.run_cli(*base, "--model", "m", "--yes")
        self.assertEqual(no_key.returncode, 2)
        self.assertIn("ANTHROPIC_API_KEY", no_key.stderr)
        with_key = {**self.env, "ANTHROPIC_API_KEY": "k"}
        no_yes = self.run_cli(*base, "--model", "m", env=with_key)
        self.assertEqual(no_yes.returncode, 2)
        self.assertIn("--yes", no_yes.stderr)
        self.assertIn("names, emails and addresses are not", no_yes.stderr)                  # it says what would leave
        self.assertNotIn("k\n", no_yes.stdout)

    def test_a_tenant_is_required_and_a_command_agent_needs_its_command(self):
        self.assertEqual(self.run_cli("--db", self.path).returncode, 2)
        self.assertEqual(self.run_cli("--db", self.path, "--tenant", TENANT, "--agent", "command").returncode, 2)


# =================================================================================================== audit labels, the costly error, stopping early
class TestAuditLabels(Base):
    def audited(self, i, verdict, unmerge=False):
        """An AUTOMATIC merge that a person then checked. Returns its candidate id."""
        attrs, first = self.base(i), PEOPLE[i][0]
        for n in (1, 2):
            ingest_record(self.store, TENANT, DOMAIN, "crm", f"{first}a{n}", attrs)
        ids = {self.record(f"{first}a1").id, self.record(f"{first}a2").id}
        cand = next(c for c in self.store.list_candidates(TENANT, DOMAIN, "confirmed") if {c.record_a_id, c.record_b_id} == ids)
        self.assertTrue(cand.decided_by.startswith("system:"))
        self.store.flag_merge_audits(TENANT, DOMAIN, [cand.id])
        give_verdict(self.store, TENANT, DOMAIN, cand.id, verdict, "steward:Pat", unmerge=unmerge)
        return cand.id

    def test_audits_are_replayed_only_when_asked_for_labelled_by_the_verdict_and_marked_as_audits(self):
        right, wrong, undone, review = self.audited(0, "correct"), self.audited(1, "wrong"), self.audited(2, "wrong", unmerge=True), self.item(3)[0]
        self.decide(review, "reject")
        self.assertEqual([(i.candidate.id, i.label, i.source) for i in replay.labelled_items(self.store, TENANT, DOMAIN)], [(review, "reject", "review")])
        got = {i.candidate.id: (i.label, i.source) for i in replay.labelled_items(self.store, TENANT, DOMAIN, include_audits=True)}
        self.assertEqual(got, {right: ("confirm", "audit"), wrong: ("reject", "audit"), review: ("reject", "review")})        # the unmerged one is out: its decision time was overwritten
        self.assertNotIn(undone, got)

    def test_an_audited_merge_is_shown_as_it_stood_when_the_system_made_it(self):
        cid = self.audited(0, "correct")
        candidate = self.store.get_candidate(cid, TENANT, DOMAIN)
        e = self.evidence(candidate)
        self.assertEqual((e["group_a"]["member_count"], e["group_b"]["member_count"]), (1, 1))                   # today they are one entity; then they were two records
        self.assertEqual((e["candidate"]["status"], e["candidate"]["decided_by"], e["reconstruction"]), ("pending", None, "exact"))
        text = json.dumps(e)
        self.assertNotIn("system:", text)
        self.assertNotIn("Pat", text.replace("Patel", ""))                                                       # nor who checked it, nor what they said
        self.assertNotIn("correct", text)

    def test_the_report_keeps_audits_and_review_decisions_apart(self):
        self.audited(0, "correct")
        self.decide(self.item(1)[0], "confirm")
        items = replay.labelled_items(self.store, TENANT, DOMAIN, include_audits=True)
        report = replay.summarize_replay(replay.run_replay(self.store, TENANT, DOMAIN, items, Counting(), KEEP))
        self.assertEqual({k: v["decided"] for k, v in report["slices"]["source"].items()}, {"audit": 1, "review": 1})


class TestTheCostlyError(unittest.TestCase):
    ROWS = [("reject", "confirm", 0.95), ("reject", "confirm", 0.6), ("confirm", "confirm", 0.95), ("confirm", "confirm", 0.9), ("confirm", "reject", 0.8),
            ("reject", "reject", 0.9), ("reject", "needs_human", 0.5), ("confirm", "needs_human", 0.5), ("reject", "reject", 0.7)]

    def test_upper_bounds_known_values(self):
        self.assertIsNone(replay.wilson_upper(0, 0))
        self.assertEqual(replay.wilson_upper(0, 10), 0.2775)
        self.assertEqual(replay.wilson_upper(0, 40), 0.0876)          # no false merge in 40 confirms does not mean none: it could be nearly 9%
        self.assertEqual(replay.wilson_upper(10, 10), 1.0)
        self.assertEqual(replay.wilson_upper(50, 100), 0.5962)

    def test_a_false_merge_is_a_confirm_where_the_person_said_different_counted_two_ways(self):
        fm = replay.summarize_replay(results(*self.ROWS))["false_merges"]
        self.assertEqual((fm["count"], fm["of_its_confirms"], fm["share_of_its_confirms"], fm["share_upper_bound_95"]), (2, 4, 0.5, 0.85))
        self.assertEqual((fm["pairs_that_were_different"], fm["rate_on_different_pairs"], fm["rate_upper_bound_95"]), (5, 0.4, 0.7693))      # needs_human on a different pair counts as answered
        self.assertEqual((fm["missed_merges"], fm["missed_merge_rate"]), (1, 0.25))                                                             # said reject, they matched; 4 pairs really matched

    def test_the_most_confident_answers_say_how_many_false_merges_they_would_have_made(self):
        at = {s["confidence_at_least"]: s for s in replay.summarize_replay(results(*self.ROWS))["if_it_acted_only_above"]}
        self.assertEqual((at[0.9]["confirms"], at[0.9]["false_merges"], at[0.9]["false_merge_upper_bound_95"]), (3, 1, 0.7923))          # 1 in 3: by the textbook formula
        self.assertEqual((at[0.99]["confirms"], at[0.99]["false_merges"], at[0.99]["false_merge_upper_bound_95"]), (0, 0, None))

    def test_a_failure_to_answer_never_improves_a_rate_and_nothing_confirmed_gives_no_rate(self):
        rows = results(("confirm", "confirm", 0.9)) + [{"candidate_id": "e", "label": "reject", "error": "x"}]
        fm = replay.summarize_replay(rows)["false_merges"]
        self.assertEqual((fm["pairs_that_were_different"], fm["rate_on_different_pairs"]), (0, None))              # the one different pair was not answered, so it is not a success
        none = replay.summarize_replay(results(("confirm", "reject", 0.9)))["false_merges"]
        self.assertEqual((none["of_its_confirms"], none["share_of_its_confirms"], none["share_upper_bound_95"]), (0, None, None))

    def test_the_text_names_the_costly_error_and_the_slices_by_score_band_exist(self):
        report = replay.summarize_replay(results(*self.ROWS, ("confirm", "confirm", 0.9, {"score": 0.93})))
        text = replay.format_report(report)
        self.assertIn("FALSE MERGES", text)
        self.assertIn("false merges in", text)
        self.assertIn("0.9-1.0", report["slices"]["score_band"])
        self.assertIn("0.7-0.8", report["slices"]["score_band"])


class TestAPairDecidedAtTheSameMoment(Base):
    def test_another_pair_decided_at_the_very_same_moment_cannot_be_ordered_so_it_is_inexact(self):
        cid, _, _ = self.item(0)
        decided = self.decide(cid, "confirm")
        ingest_record(self.store, TENANT, DOMAIN, "crm", "Zed", {**self.base(4), "first_name": "Zed"})
        self.store.save_candidate(MatchCandidate(record_a_id=self.record("Zed").id, record_b_id=self.record("Ann2").id, tenant_id=TENANT, domain=DOMAIN, score=0.9, match_type="fuzzy",
                                                 status="confirmed", decided_by="steward:Lee", decided_at=decided.decided_at))
        with self.assertRaises(replay.Unreconstructable) as caught:
            self.evidence(decided)
        self.assertIn("very same moment", str(caught.exception))
        self.assertEqual(self.evidence(decided, allow_approximate=True)["reconstruction"], "approximate")


class TestStoppingEarlyAndRetrying(Base):
    def decided(self, n):
        for i in range(n):
            self.decide(self.item(i)[0], "confirm")
        return replay.labelled_items(self.store, TENANT, DOMAIN)

    class Failing:
        name = "failing"

        def __init__(self):
            self.calls = 0

        def recommend(self, evidence):
            self.calls += 1
            raise AgentError("the model API did not answer (HTTP 529)")

    def test_it_stops_once_the_agent_has_failed_more_than_the_limit_and_keeps_what_it_has(self):
        items, agent = self.decided(4), self.Failing()
        with self.assertRaises(replay.TooManyErrors) as caught:
            replay.run_replay(self.store, TENANT, DOMAIN, items, agent, KEEP, max_errors=1)
        self.assertEqual((agent.calls, caught.exception.errors, caught.exception.limit, len(caught.exception.results)), (2, 2, 1, 2))      # failed twice: more than 1
        agent = self.Failing()
        with self.assertRaises(replay.TooManyErrors) as caught:
            replay.run_replay(self.store, TENANT, DOMAIN, items, agent, KEEP, max_errors=0)
        self.assertEqual(agent.calls, 1)                                                                                                       # 0 allowed: the first failure stops it

    def test_the_limit_is_more_than_not_at_least_and_no_limit_means_never(self):
        items = self.decided(3)
        r = replay.run_replay(self.store, TENANT, DOMAIN, items, self.Failing(), KEEP, max_errors=3)           # three failures are not MORE than three
        self.assertEqual([bool(x.get("error")) for x in r], [True, True, True])
        r = replay.run_replay(self.store, TENANT, DOMAIN, items, self.Failing(), KEEP)
        self.assertEqual(len(r), 3)

    def test_what_failed_is_tried_again_and_what_has_an_answer_is_not(self):
        items = self.decided(3)
        first = Counting()
        done = {r["candidate_id"]: r for r in replay.run_replay(self.store, TENANT, DOMAIN, items[:1], first, KEEP)}
        done[items[1].candidate.id] = {"candidate_id": items[1].candidate.id, "label": "confirm", "error": "the model API did not answer (HTTP 529)"}
        second = Counting()
        replay.run_replay(self.store, TENANT, DOMAIN, items, second, KEEP, done=done)
        self.assertEqual(second.calls, [items[1].candidate.id, items[2].candidate.id])                           # the answered one is kept; the failed one is asked again

    def test_a_stopped_report_says_so_at_the_top(self):
        text = replay.format_report(replay.summarize_replay(results(("confirm", "confirm", 0.9)), stopped_early=True))
        self.assertTrue(text.startswith("STOPPED EARLY"))
        self.assertNotIn("STOPPED EARLY", replay.format_report(replay.summarize_replay(results(("confirm", "confirm", 0.9)))))


class TestTheNewCommandLineOptions(Base):
    def setUp(self):
        super().setUp()
        for i, d in enumerate(("confirm", "reject", "confirm")):
            self.decide(self.item(i)[0], d)
        self.env = {**os.environ, "MDM_AUDIT_RATE": "0"}
        self.env.pop("ANTHROPIC_API_KEY", None)
        self.env.pop("DATABASE_URL", None)

    def run_cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "replay_decisions.py"), "--db", self.path, "--tenant", TENANT, *args], capture_output=True, text=True, env=self.env, timeout=60)

    def agent_script(self, ok):
        log = os.path.join(self.tmp, "calls.log")
        script = os.path.join(self.tmp, "agent.py")
        body = "import json, sys\njson.load(sys.stdin)\nopen(%r, 'a').write('x')\n" % log
        body += ("print(json.dumps({'recommendation': 'confirm', 'confidence': 0.9}))\n" if ok else "sys.exit(1)\n")
        open(script, "w").write(body)
        return f'"{sys.executable}" "{script}"', log

    def test_a_broken_agent_stops_the_run_with_a_message_a_non_zero_exit_and_a_resumable_file(self):
        out = os.path.join(self.tmp, "results.jsonl")
        bad, log = self.agent_script(ok=False)
        stopped = self.run_cli("--agent", "command", "--agent-command", bad, "--max-errors", "1", "--out", out)
        self.assertEqual(stopped.returncode, 1)
        self.assertIn("STOPPING", stopped.stderr)
        self.assertIn("STOPPED EARLY", stopped.stdout)
        self.assertEqual((len(open(log).read()), len(open(out).read().splitlines())), (2, 2))                  # it tried twice, saw it was broken, and did not try the third
        good, log2 = self.agent_script(ok=True)                                                                  # the same log file: it keeps counting, so compare before and after
        before = len(open(log2).read())
        fixed = self.run_cli("--agent", "command", "--agent-command", good, "--max-errors", "1", "--out", out)
        self.assertEqual(fixed.returncode, 0, fixed.stderr)
        self.assertEqual(len(open(log2).read()) - before, 3)                                                     # all three were asked: two had failed, one was never reached
        before = len(open(log2).read())
        again = self.run_cli("--agent", "command", "--agent-command", good, "--out", out)
        self.assertIn("3 of them are already in", again.stderr)
        self.assertEqual(len(open(log2).read()) - before, 0)                                                     # and now none is asked again

    def test_audits_are_included_only_when_asked_for(self):
        attrs = self.base(3)
        for n in (1, 2):
            ingest_record(self.store, TENANT, DOMAIN, "crm", f"Dev{n}", attrs)
        ids = {self.record("Dev1").id, self.record("Dev2").id}
        cand = next(c for c in self.store.list_candidates(TENANT, DOMAIN, "confirmed") if {c.record_a_id, c.record_b_id} == ids)
        self.store.flag_merge_audits(TENANT, DOMAIN, [cand.id])
        give_verdict(self.store, TENANT, DOMAIN, cand.id, "correct", "steward:Pat")
        plain = self.run_cli("--agent", "rule")
        with_audits = self.run_cli("--agent", "rule", "--include-audits")
        self.assertIn("Replayed 3 decided review items", plain.stdout)
        self.assertIn("Replayed 4 decided review items", with_audits.stdout)
        self.assertIn("or automatic merges checked by one", with_audits.stderr)


if __name__ == "__main__":
    unittest.main()
