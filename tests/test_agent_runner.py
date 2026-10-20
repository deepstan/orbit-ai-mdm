"""
THE AGENT RUNNER (app/agent_runner.py, agent_runner.py): our own stewardship agent, in shadow mode.

The runner may read the review queue and record recommendations, and nothing else. These tests are mostly about what it must NOT do and how it is stopped:

  * A PASS: records one recommendation per waiting item under the key's name; the items stay pending; a second pass asks the model about none; a person deciding in the meantime is a normal race
  * WHAT IT MAY ASK: four exact (method, path) pairs, pinned; anything else is refused inside the runner, whatever the platform would say
  * WHO MAY RUN IT: a key that cannot record recommendations (a steward's, a reader's) or a refused key stops it at once
  * THE CONTROLS: items per pass, model calls per day (kept across restarts), stop after too many errors, evidence too large to send, the kill switch, signals, a dry run
  * THE PROGRAM end to end over real HTTP: the key goes to the platform, the model's key to the model, nothing sensitive in the logs or the state file

The platform side runs the real endpoint functions on a real database (tests/fake_web.py stands in for FastAPI where it is not installed); the model is a fake or a stub HTTP server.
Run with:  python3 -m unittest tests.test_agent_runner -v
"""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock as mock
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(__file__))

import fake_web  # noqa: E402
from app import limits, replay_agents  # noqa: E402
from app.agent_runner import ALLOWED, GuardedClient, Runner, RunnerConfig, RunnerGuard  # noqa: E402
from app.auth import AuthError, Principal  # noqa: E402
from app.db import Store  # noqa: E402
from app.mcp_server.client import ApiError, BackendUnavailable  # noqa: E402
from app.pipeline import ingest_record, resolve_candidate  # noqa: E402
from app.replay_agents import PROMPT_VERSION, SYSTEM_PROMPT, LlmAgent  # noqa: E402

api = None
_saved: dict = {}
_env: dict = {}

PEOPLE = [("Ann", "Lee", "30301"), ("Bob", "Moss", "31201"), ("Cara", "Quinn", "30030"), ("Dev", "Patel", "30606"), ("Eve", "Stone", "31401")]
ANSWER = '{"recommendation": "confirm", "confidence": 0.8, "reasons": ["same person, new surname"], "evidence": ["group_b.members"]}'


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


def P(role="steward_agent", label="runner-1", tenant="acme"):
    return Principal(tenant_id=tenant, role=role, key_id=f"key_{label}", label=label, allowed_domains=None)


class LocalApi:
    """The platform behind the runner's client: the REAL endpoint functions on a real database, with the key deciding who is calling. It records every call."""

    def __init__(self, keys):
        self.keys, self.calls, self.hooks = keys, [], {}

    def _principal(self, key):
        p = self.keys.get(key)
        if p is None:
            raise ApiError(401, "Invalid or revoked API key")
        return p

    def _run(self, fn):
        try:
            return json.loads(json.dumps(fn(), default=str))
        except api.HTTPException as e:
            raise ApiError(e.status_code, str(e.detail))
        except AuthError as e:
            raise ApiError(403, str(e))

    def get(self, path, query, key):
        self.calls.append(("GET", path))
        if "before_get" in self.hooks:
            self.hooks["before_get"](path)
        p = self._principal(key)
        if re.match(r"^/v1/customer/review-queue$", path):
            return self._run(lambda: api.review_queue("customer", limit=int(query.get("limit", 50)), cursor=query.get("cursor"), principal=p))
        m = re.match(r"^/v1/customer/review-queue/([^/]+)/evidence$", path)
        if m:
            return self._run(lambda: api.review_evidence("customer", m.group(1), principal=p))
        if re.match(r"^/v1/customer/recommendations$", path):
            return self._run(lambda: api.list_recommendations("customer", outcome=query.get("outcome"), limit=int(query.get("limit", 50)), cursor=query.get("cursor"), principal=p))
        raise AssertionError(f"the runner asked for {path}")

    def post(self, path, body, key):
        self.calls.append(("POST", path))
        p = self._principal(key)
        m = re.match(r"^/v1/customer/review-queue/([^/]+)/recommendations$", path)
        assert m, f"the runner posted to {path}"
        return self._run(lambda: api.propose_decision("customer", m.group(1), api.ProposeDecisionRequest(**body), principal=p))


class Brain:
    """A fake model behind a real LlmAgent: it records what it was asked and answers from a function."""

    def __init__(self, answer=ANSWER, model="model-x"):
        self.requests, self.answer = [], answer
        self.agent = LlmAgent(model, api_key="not-used", post=self._post, sleep=lambda s: None)

    def _post(self, body):
        self.requests.append(body)
        text = self.answer(body) if callable(self.answer) else self.answer
        if isinstance(text, Exception):
            raise text
        return {"content": [{"type": "text", "text": text}]}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(os.path.join(self.tmp, "r.db"))
        for target, value in ((api, "store"), (api, "_limiter")):
            patcher = mock.patch.object(target, value, self.store if value == "store" else limits.RateLimiter(0))
            patcher.start()
            self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel", "MDM_MODEL_DIR": os.path.join(self.tmp, "models")})
        env.start()
        self.addCleanup(env.stop)
        self.platform = LocalApi({"k-agent": P(), "k-steward": P("data_steward", "Pat"), "k-reader": P("api_consumer", "Rae")})
        self.state_file = os.path.join(self.tmp, "state.json")
        self.stop_file = os.path.join(self.tmp, "STOP")
        self.now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)

    def items(self, n):
        """n review items waiting for a person. Returns their candidate ids."""
        ids = []
        for i in range(n):
            first, last, zip_ = PEOPLE[i]
            base = {"first_name": first, "last_name": last, "email": f"{first.lower()}@x.com", "phone": f"404-555-01{i}0", "address_line1": f"{i + 1} Elm Street", "city": "Atlanta", "state": "GA",
                    "postal_code": zip_}
            for k in (1, 2):
                ingest_record(self.store, "acme", "customer", "crm", f"{first}{k}", base)
            r = ingest_record(self.store, "acme", "customer", "crm", f"{first}X", {**base, "email": f"other.{first.lower()}@z.com", "phone": "", "last_name": last + "h"})
            ids.append(r["queued_for_review"][0]["candidate_id"])
        return ids

    def runner(self, brain=None, key="k-agent", **cfg):
        brain = brain or Brain()
        self.sleeps = []
        config = RunnerConfig(domain="customer", state_file=self.state_file, stop_file=self.stop_file, **cfg)
        return Runner(self.platform, key, brain.agent, config, clock=lambda: self.now, sleep=self.sleeps.append), brain

    def snapshot(self):
        return (len(self.store.list_source_records("acme", "customer")), sorted((c.id, c.status) for c in self.store.list_candidates("acme", "customer", status=None)),
                sorted(e.id for e in self.store.list_entities("acme", "customer", "active")))

    def recs(self):
        return [r for c in self.store.list_candidates("acme", "customer", status=None) for r in self.store.list_recommendations_for("acme", "customer", c.id)]


# =================================================================================================== a pass
class TestAPass(Base):
    def test_it_records_one_recommendation_per_waiting_item_under_the_keys_name_and_changes_nothing_else(self):
        self.items(3)
        before = self.snapshot()
        runner, brain = self.runner()
        s = runner.run_once()
        self.assertEqual((s["seen"], s["recommended"], s["model_calls"], s["errors"], s["stopped"]), (3, 3, 3, 0, None))
        recs = self.recs()
        self.assertEqual({(r.agent, r.recommendation, r.model, r.prompt_version, r.outcome) for r in recs}, {("agent:runner-1", "confirm", "model-x", PROMPT_VERSION, None)})
        self.assertEqual(self.snapshot(), before)                                                   # not one item decided, merged or changed
        self.assertEqual(len(brain.requests), 3)

    def test_a_second_pass_asks_the_model_about_nothing_and_a_new_model_or_prompt_gets_its_own_opinion(self):
        self.items(2)
        self.runner()[0].run_once()
        again, brain = self.runner()
        s = again.run_once()
        self.assertEqual((s["recommended"], s["already_recommended"], s["model_calls"], len(brain.requests)), (0, 2, 0, 0))
        other_model, brain2 = self.runner(Brain(model="model-y"))
        self.assertEqual(other_model.run_once()["recommended"], 2)
        newer_prompt, brain3 = self.runner()
        brain3.agent.prompt_version = "steward-v2"
        self.assertEqual(newer_prompt.run_once()["recommended"], 2)
        self.assertEqual(len(self.recs()), 6)                                                       # three opinions per item, none replaced

    def test_a_person_deciding_while_the_model_thinks_is_a_normal_race_not_an_error(self):
        ids = self.items(2)
        decided = []

        def answer(body):
            if not decided:                                                                           # during the FIRST model call a person decides that very item
                cid = re.search(r'"candidate_id":"([^"]+)"', body["messages"][0]["content"]).group(1)
                resolve_candidate(self.store, "acme", "customer", cid, "reject", "steward:Pat")
                decided.append(cid)
            return ANSWER
        s = self.runner(Brain(answer))[0].run_once()
        self.assertEqual((s["recommended"], s["decided_meanwhile"], s["errors"]), (1, 1, 0))
        self.assertEqual(len(self.recs()), 1)
        self.assertNotIn(decided[0], [r.candidate_id for r in self.recs()])

    def test_an_item_that_vanishes_between_the_queue_and_its_evidence_is_skipped(self):
        ids = self.items(2)
        def vanish(path):
            if path.endswith(f"/{ids[0]}/evidence"):
                raise ApiError(404, "candidate not found")
        self.platform.hooks["before_get"] = vanish
        s = self.runner()[0].run_once()
        self.assertEqual((s["recommended"], s["decided_meanwhile"], s["errors"]), (1, 1, 0))

    def test_items_per_pass_is_a_cap_and_the_next_pass_carries_on_where_it_left_off(self):
        self.items(4)
        s1 = self.runner(max_items=2)[0].run_once()
        s2 = self.runner(max_items=2)[0].run_once()
        s3 = self.runner(max_items=2)[0].run_once()
        self.assertEqual((s1["recommended"], s2["recommended"], s3["recommended"]), (2, 2, 0))
        self.assertEqual(len(self.recs()), 4)

    def test_once_its_quota_is_asked_it_stops_reading_the_queue(self):
        self.items(4)
        runner, _ = self.runner(page=1, max_items=2)
        runner.run_once()
        self.assertEqual(sum(1 for m, p in self.platform.calls if p == "/v1/customer/review-queue"), 2)   # two items, two pages: not all four

    def test_the_queue_is_read_a_page_at_a_time(self):
        self.items(3)
        runner, _ = self.runner(page=1)
        runner.run_once()
        self.assertEqual(sum(1 for m, p in self.platform.calls if p == "/v1/customer/review-queue"), 3)
        self.assertEqual(len(self.recs()), 3)

    def test_what_the_model_got_is_the_platforms_masked_evidence_and_only_that(self):
        self.items(1)
        runner, brain = self.runner()
        runner.run_once()
        sent = brain.requests[0]
        self.assertIs(sent["system"], SYSTEM_PROMPT)
        evidence = json.loads(sent["messages"][0]["content"].split("\n", 1)[1])
        self.assertEqual({"candidate", "record_a", "record_b", "group_a", "group_b", "separations", "history"} <= set(evidence), True)
        self.assertNotIn("steward:", json.dumps(sent))


class TestWhenTheModelMisbehaves(Base):
    def test_a_failing_model_counts_errors_and_stops_after_the_limit_and_the_failed_items_are_tried_again_later(self):
        self.items(4)
        flaky = Brain(lambda body: replay_agents.AgentError("no"))
        flaky.agent._post = lambda body: (_ for _ in ()).throw(replay_agents._Retryable("HTTP 529"))
        runner = Runner(self.platform, "k-agent", flaky.agent, RunnerConfig(domain="customer", state_file=self.state_file, max_errors=1), clock=lambda: self.now, sleep=lambda s: None)
        s = runner.run_once()
        self.assertEqual((s["errors"], s["recommended"], s["stop_kind"]), (2, 0, "problem"))        # failed twice: more than 1
        self.assertIn("--max-errors", s["stopped"])
        well, brain = self.runner()
        self.assertEqual(well.run_once()["recommended"], 4)                                         # the failures left nothing behind: all four are asked again

    def test_an_answer_that_breaks_the_rules_is_an_error_and_is_never_posted(self):
        self.items(1)
        runner, _ = self.runner(Brain('{"recommendation": "confirm", "confidence": 2}'), max_errors=5)
        s = runner.run_once()
        self.assertEqual((s["errors"], s["recommended"]), (1, 0))
        self.assertEqual(self.recs(), [])
        self.assertNotIn("POST", {m for m, p in self.platform.calls})                               # not even tried

    def test_prose_around_the_json_is_understood_and_an_answer_with_no_json_is_an_error(self):
        self.items(2)
        asked = []
        def answer(body):
            asked.append(1)
            return "Here you go: " + ANSWER + " Hope that helps." if len(asked) == 1 else "I really cannot say."
        s = self.runner(Brain(answer), max_errors=5)[0].run_once()
        self.assertEqual((s["recommended"], s["errors"], s["stopped"]), (1, 1, None))
        self.assertEqual(len(self.recs()), 1)

    def test_evidence_too_large_to_send_is_skipped_without_a_model_call(self):
        self.items(2)
        runner, brain = self.runner(max_evidence_chars=200)
        s = runner.run_once()
        self.assertEqual((s["too_large"], s["model_calls"], s["recommended"], len(brain.requests)), (2, 0, 0, 0))

    def test_a_dry_run_says_what_it_would_ask_and_how_much_and_asks_nobody(self):
        self.items(2)
        runner, brain = self.runner(dry_run=True)
        s = runner.run_once()
        self.assertEqual((s["planned"], s["model_calls"], s["recommended"], len(brain.requests)), (2, 0, 0, 0))
        self.assertGreater(s["planned_chars"], 200)
        self.assertEqual(self.recs(), [])
        self.assertNotIn("POST", {m for m, p in self.platform.calls})


class TestTheNetworkMisbehaves(Base):
    def test_rate_limiting_is_waited_out_and_endless_rate_limiting_is_an_error(self):
        self.items(1)
        limited = {"n": 0}
        def limit(path):
            if path.endswith("/review-queue") and limited["n"] < 2:
                limited["n"] += 1
                raise ApiError(429, "slow down")
        self.platform.hooks["before_get"] = limit
        runner, _ = self.runner()
        s = runner.run_once()
        self.assertEqual((s["recommended"], self.sleeps), (1, [5, 10]))
        self.platform.hooks["before_get"] = lambda path: (_ for _ in ()).throw(ApiError(429, "slow down"))
        s = self.runner(max_errors=0)[0].run_once()
        self.assertEqual(s["stop_kind"], "problem")

    def test_an_unreachable_platform_is_counted_and_stops_the_pass_after_the_limit(self):
        self.items(1)
        self.platform.hooks["before_get"] = lambda path: (_ for _ in ()).throw(BackendUnavailable("down"))
        s = self.runner(max_errors=1)[0].run_once()
        self.assertEqual((s["recommended"], s["stop_kind"]), (0, "problem"))


# =================================================================================================== what it may ask
class TestWhatTheRunnerMayAsk(Base):
    def test_exactly_these_four_requests_and_nothing_else_is_pinned(self):
        self.assertEqual([(m, rx.pattern) for m, rx in ALLOWED], [
            ("GET", "^/v1/[a-z][a-z0-9_]{0,63}/review-queue$"),
            ("GET", "^/v1/[a-z][a-z0-9_]{0,63}/review-queue/[A-Za-z0-9_.:\\-]{1,128}/evidence$"),
            ("GET", "^/v1/[a-z][a-z0-9_]{0,63}/recommendations$"),
            ("POST", "^/v1/[a-z][a-z0-9_]{0,63}/review-queue/[A-Za-z0-9_.:\\-]{1,128}/recommendations$")])

    def test_anything_else_is_refused_inside_the_runner_before_it_leaves(self):
        asked = []
        spy = type("Spy", (), {"get": lambda self, p, q, k: asked.append(("GET", p)), "post": lambda self, p, b, k: asked.append(("POST", p))})()
        guard = GuardedClient(spy)
        for method, path in (("POST", "/v1/customer/review-queue/c1/resolve"), ("POST", "/v1/customer/audits/c1/verdict"), ("POST", "/v1/customer/merges/c1/unmerge"),
                             ("POST", "/v1/customer/records"), ("POST", "/v1/customer/records/batch"), ("POST", "/v1/admin/api-keys"), ("GET", "/v1/admin/audit-log"),
                             ("GET", "/v1/customer/entities"), ("GET", "/v1/customer/audits"), ("GET", "/v1/customer/recommendations/stats"),
                             ("GET", "/v1/customer/review-queue/c1/recommendations"), ("POST", "/v1/customer/review-queue"), ("GET", "/v1/customer/review-queue/c1/resolve"),
                             ("POST", "/v1/customer/recommendations"), ("GET", "/v1/Customer/review-queue"), ("GET", "/v1/customer/review-queue/../admin/evidence"),
                             ("GET", "/v1/customer/review-queue/c1/evidence?x=1"), ("POST", "/v1/customer/review-queue/a b/recommendations"), ("GET", "/healthz")):
            with self.assertRaises(RunnerGuard, msg=f"{method} {path}"):
                (guard.get if method == "GET" else guard.post)(path, {}, "k")
        self.assertEqual(asked, [])                                                                  # none of them left the runner
        for path in ("/v1/customer/review-queue", "/v1/customer/review-queue/cand_1/evidence", "/v1/customer/recommendations"):
            guard.get(path, {}, "k")
        guard.post("/v1/customer/review-queue/cand_1/recommendations", {}, "k")
        self.assertEqual(len(asked), 4)

    def test_a_whole_pass_makes_only_the_allowed_kinds_of_request(self):
        self.items(2)
        self.runner()[0].run_once()
        shapes = {(m, re.sub(r"cand_[0-9a-f]+", "{id}", p)) for m, p in self.platform.calls}
        self.assertEqual(shapes, {("GET", "/v1/customer/recommendations"), ("GET", "/v1/customer/review-queue"), ("GET", "/v1/customer/review-queue/{id}/evidence"),
                                  ("POST", "/v1/customer/review-queue/{id}/recommendations")})

    def test_the_runner_cannot_even_name_the_decision_endpoints(self):
        import app.agent_runner as module
        code = open(module.__file__).read().split('"""', 2)[2]                                       # the code, not the explanation at the top
        for word in ("/resolve", "/verdict", "/unmerge", "/records", "/api-keys", "purge", "delete"):
            self.assertNotIn(word, code, word)


class TestWhoMayRunIt(Base):
    def test_a_stewards_key_can_read_but_may_not_record_so_the_runner_stops_at_the_first_refusal(self):
        self.items(3)
        runner, brain = self.runner(key="k-steward")
        s = runner.run_once()
        self.assertEqual((s["stop_kind"], s["recommended"], len(brain.requests)), ("problem", 0, 1))   # one model call, then it stopped: it did not keep spending
        self.assertIn("steward_agent", s["stopped"])

    def test_a_readers_key_is_refused_the_same_way(self):
        self.items(2)
        s = self.runner(key="k-reader")[0].run_once()
        self.assertEqual((s["stop_kind"], s["recommended"]), ("problem", 0))

    def test_a_wrong_or_revoked_key_stops_it_before_any_model_call(self):
        self.items(2)
        runner, brain = self.runner(key="not-a-key")
        s = runner.run_once()
        self.assertEqual((s["stop_kind"], len(brain.requests)), ("problem", 0))
        self.assertIn("401", s["stopped"])

    def test_the_right_key_works_and_could_not_resolve_anything_if_the_runner_tried(self):
        ids = self.items(1)
        with self.assertRaises(AuthError):
            api.resolve("customer", ids[0], api.ResolveCandidateRequest(decision="confirm"), principal=P())      # the platform would refuse it too: two locks, not one


# =================================================================================================== the controls
class TestTheControls(Base):
    def test_the_daily_cap_is_kept_across_restarts_and_resets_at_midnight_utc(self):
        self.items(5)
        first, _ = self.runner(max_per_day=3)
        s = first.run_once()
        self.assertEqual((s["model_calls"], s["recommended"], s["stop_kind"], s["calls_today"]), (3, 3, "cap", 3))
        restarted, brain = self.runner(max_per_day=3)                                              # a NEW runner, the same state file: a restart does not reset the day
        s = restarted.run_once()
        self.assertEqual((s["model_calls"], s["stop_kind"], len(brain.requests)), (0, "cap", 0))
        self.now += timedelta(days=1)
        tomorrow, _ = self.runner(max_per_day=3)
        s = tomorrow.run_once()
        self.assertEqual((s["model_calls"], s["recommended"], s["calls_today"]), (2, 2, 2))

    def test_a_call_is_counted_before_it_is_made_so_a_failed_call_is_not_free(self):
        self.items(1)
        runner, _ = self.runner(Brain(lambda body: "no json here"), max_errors=5)
        s = runner.run_once()
        self.assertEqual((s["errors"], s["calls_today"]), (1, 1))
        self.assertEqual(json.load(open(self.state_file))["calls_today"], 1)

    def test_the_kill_switch_before_a_pass_means_not_one_request_at_all(self):
        self.items(2)
        open(self.stop_file, "w").write("")
        runner, brain = self.runner()
        s = runner.run_once()
        self.assertEqual((s["stop_kind"], s["recommended"], len(brain.requests), self.platform.calls), ("switch", 0, 0, []))
        self.assertIn("kill switch", s["stopped"])

    def test_the_kill_switch_during_a_pass_stops_it_before_the_next_item(self):
        self.items(3)
        def answer(body):
            open(self.stop_file, "w").write("")                                                      # someone throws the switch while the FIRST item is being thought about
            return ANSWER
        s = self.runner(Brain(answer))[0].run_once()
        self.assertEqual((s["recommended"], s["model_calls"], s["stop_kind"]), (1, 1, "switch"))     # the item in hand is finished and recorded; the rest are not asked

    def test_a_stop_request_from_a_signal_finishes_the_item_in_hand_and_stops(self):
        self.items(3)
        holder = {}
        def answer(body):
            holder["runner"].request_stop()
            return ANSWER
        runner, _ = self.runner(Brain(answer))
        holder["runner"] = runner
        s = runner.run_once()
        self.assertEqual((s["recommended"], s["stop_kind"]), (1, "switch"))

    def test_the_state_file_holds_counts_and_times_and_nothing_sensitive(self):
        self.items(1)
        self.runner()[0].run_once()
        text = open(self.state_file).read()
        state = json.loads(text)
        self.assertEqual((state["date"], state["calls_today"], state["total_recommended"], state["last_summary"]["recommended"]), ("2026-10-17", 1, 1, 1))
        for secret in ("k-agent", "not-used", "ann@x.com", "Elm Street", "other.ann"):
            self.assertNotIn(secret, text)

    def test_a_write_that_fails_halfway_leaves_the_old_state_intact_and_no_litter(self):
        self.items(1)
        self.runner()[0].run_once()
        good = open(self.state_file).read()
        def half_written(obj, f, **kw):
            f.write('{"date": "2026-10-1')
            raise OSError(28, "No space left on device")
        with mock.patch("app.agent_runner.json.dump", half_written):
            runner, brain = self.runner(max_per_day=99)
            s = runner.run_once()
        self.assertEqual(open(self.state_file).read(), good)                                         # the old file is whole and valid
        json.loads(open(self.state_file).read())
        self.assertEqual([f for f in os.listdir(self.tmp) if f.startswith(".runner-state-")], [])    # and no half-written temporary file was left behind
        self.assertIsNotNone(s["stopped"])

    def test_if_the_state_cannot_be_written_the_runner_stops_before_spending_not_after(self):
        self.items(3)
        def half_written(obj, f, **kw):
            raise OSError(28, "No space left on device")
        with mock.patch("app.agent_runner.json.dump", half_written):
            runner, brain = self.runner()
            s = runner.run_once()
        self.assertEqual((s["stop_kind"], len(brain.requests), s["recommended"]), ("problem", 0, 0))   # the count is saved BEFORE each call, so with no way to keep it there is no call
        self.assertIn("state file could not be written", s["stopped"])

    def test_an_unreadable_state_file_is_started_over_not_a_crash(self):
        self.items(1)
        open(self.state_file, "w").write("{not json")
        self.assertEqual(self.runner()[0].run_once()["recommended"], 1)

    def test_a_bad_setting_is_refused_when_it_is_made(self):
        for bad in (dict(domain="Customer"), dict(domain="a/b"), dict(domain="customer", max_items=-1), dict(domain="customer", max_per_day=-5), dict(domain="customer", page=0),
                    dict(domain="customer", max_errors=1.5)):
            with self.assertRaises(ValueError, msg=str(bad)):
                RunnerConfig(**bad)


class TestSharedWithTheReplay(unittest.TestCase):
    def test_the_runner_uses_the_replays_instructions_and_version_so_the_replay_measures_what_it_does(self):
        agent = LlmAgent("m", api_key="k", post=lambda body: {"content": [{"type": "text", "text": ANSWER}]})
        self.assertIs(agent.request_body({"candidate": {}})["system"], replay_agents.SYSTEM_PROMPT)
        self.assertEqual(agent.parse(ANSWER)["prompt_version"], replay_agents.PROMPT_VERSION)
        self.assertEqual(replay_agents.PROMPT_VERSION, "steward-v1")


# =================================================================================================== the program, end to end over real HTTP
class StubServer:
    """A tiny HTTP server: answers from a table keyed by (method, path), records every request, and can run a side effect."""

    def __init__(self, table):
        outer = self
        self.table, self.seen, self.effects = table, [], {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _answer(self, method):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0)) if method == "POST" else b""
                path = self.path.split("?")[0]
                outer.seen.append({"method": method, "path": path, "query": self.path.partition("?")[2], "headers": {k.lower(): v for k, v in self.headers.items()}, "body": json.loads(raw) if raw else None})
                if (method, path) in outer.effects:
                    outer.effects[(method, path)]()
                status, body = outer.table.get((method, path), (404, {"detail": "no such thing"}))
                out = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def do_GET(self):
                self._answer("GET")

            def do_POST(self):
                self._answer("POST")
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


EVIDENCE = {"candidate": {"candidate_id": "cand_1", "score": 0.7, "decided_by": None}, "record_a": {"attributes": {"first_name": "Ann"}}, "group_a": {}, "group_b": {}}
QUEUE = {"items": [{"candidate_id": "cand_1"}], "next_cursor": None, "limit": 50, "total": 1}


class TestTheProgram(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.platform = StubServer({("GET", "/v1/customer/recommendations"): (200, {"items": [], "next_cursor": None, "limit": 200}), ("GET", "/v1/customer/review-queue"): (200, QUEUE),
                                    ("GET", "/v1/customer/review-queue/cand_1/evidence"): (200, EVIDENCE),
                                    ("POST", "/v1/customer/review-queue/cand_1/recommendations"): (200, {"recommendation_id": "reco_1", "agent": "agent:runner-1"})})
        self.model = StubServer({("POST", "/v1/messages"): (200, {"content": [{"type": "text", "text": ANSWER}]})})
        self.addCleanup(self.platform.stop)
        self.addCleanup(self.model.stop)
        self.state, self.stop_file = os.path.join(self.tmp, "state.json"), os.path.join(self.tmp, "STOP")
        self.env = {**os.environ, "MDM_API_KEY": "platform-key-123", "ANTHROPIC_API_KEY": "model-key-456", "ANTHROPIC_BASE_URL": self.model.url, "MDM_API_URL": self.platform.url}

    def run_cli(self, *args, env=None, timeout=60):
        return subprocess.run([sys.executable, os.path.join(ROOT, "agent_runner.py"), "--state", self.state, "--stop-file", self.stop_file, *args], capture_output=True, text=True,
                              env=env or self.env, timeout=timeout)

    def test_one_pass_over_real_http_each_key_goes_to_its_own_server_and_nothing_sensitive_is_logged(self):
        done = self.run_cli("--model", "model-x", "--yes")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("recommended 1", done.stdout)
        self.assertIn("SHADOW MODE", done.stderr)
        for request in self.platform.seen:
            self.assertEqual(request["headers"]["authorization"], "Bearer platform-key-123")        # the platform key, and only to the platform
        posted = [r for r in self.platform.seen if r["method"] == "POST"][0]
        self.assertEqual((posted["body"]["recommendation"], posted["body"]["model"], posted["body"]["prompt_version"]), ("confirm", "model-x", PROMPT_VERSION))
        self.assertEqual(len(self.model.seen), 1)
        self.assertEqual(self.model.seen[0]["headers"]["x-api-key"], "model-key-456")               # the model key, and only to the model
        self.assertNotIn("platform-key-123", json.dumps(self.model.seen))
        self.assertIn('"candidate_id":"cand_1"', self.model.seen[0]["body"]["messages"][0]["content"])
        for secret in ("platform-key-123", "model-key-456", "Ann"):
            self.assertNotIn(secret, done.stderr + done.stdout + open(self.state).read())

    def test_it_will_not_start_without_a_platform_key_a_model_a_model_key_or_a_yes(self):
        cases = [({"MDM_API_KEY": None}, ("--model", "m", "--yes"), "MDM_API_KEY"), ({}, ("--yes",), "--model"), ({"ANTHROPIC_API_KEY": None}, ("--model", "m", "--yes"), "ANTHROPIC_API_KEY"),
                 ({}, ("--model", "m"), "--yes")]
        for change, args, fragment in cases:
            env = {k: v for k, v in {**self.env, **change}.items() if v is not None}
            done = self.run_cli(*args, env=env)
            self.assertEqual(done.returncode, 2, (args, done.stderr))
            self.assertIn(fragment, done.stderr)
        self.assertEqual((self.model.seen, [r for r in self.platform.seen if r["method"] == "POST"]), ([], []))
        self.assertIn("names, emails and addresses are not", self.run_cli("--model", "m").stderr)     # it says what would leave

    def test_a_dry_run_needs_no_model_and_sends_nothing(self):
        env = {k: v for k, v in self.env.items() if k != "ANTHROPIC_API_KEY"}
        done = self.run_cli("--dry-run", env=env)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("DRY RUN: would ask about 1 items", done.stdout)
        self.assertEqual((self.model.seen, [r for r in self.platform.seen if r["method"] == "POST"]), ([], []))

    def test_with_the_kill_switch_present_it_exits_cleanly_having_asked_nobody(self):
        open(self.stop_file, "w").write("")
        done = self.run_cli("--model", "model-x", "--yes")
        self.assertEqual(done.returncode, 0)
        self.assertIn("STOPPED (switch)", done.stdout)
        self.assertEqual((self.model.seen, self.platform.seen), ([], []))                              # not even a request to the platform

    def test_a_refused_key_is_a_failure_with_the_reason(self):
        self.platform.table[("GET", "/v1/customer/recommendations")] = (401, {"detail": "Invalid or revoked API key"})
        done = self.run_cli("--model", "model-x", "--yes")
        self.assertEqual(done.returncode, 1)
        self.assertIn("STOPPED (problem)", done.stdout)
        self.assertIn("401", done.stdout)
        self.assertEqual(self.model.seen, [])

    def test_in_a_loop_the_kill_switch_ends_it_after_the_pass_in_progress(self):
        self.platform.effects[("POST", "/v1/customer/review-queue/cand_1/recommendations")] = lambda: open(self.stop_file, "w").write("")
        started = time.time()
        done = self.run_cli("--model", "model-x", "--yes", "--loop", "--interval", "30", timeout=40)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("recommended 1", done.stdout)
        self.assertIn("STOPPED (switch)", done.stdout)
        self.assertLess(time.time() - started, 15)                                                     # it did not wait out the 30 seconds

    def test_a_terminate_signal_finishes_the_pass_and_exits_cleanly(self):
        proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "agent_runner.py"), "--state", self.state, "--stop-file", self.stop_file, "--model", "model-x", "--yes", "--loop",
                                 "--interval", "60"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self.env)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        first = proc.stdout.readline()                                                                 # the first pass has finished
        self.assertIn("recommended 1", first)
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=20), 0)


if __name__ == "__main__":
    unittest.main()
