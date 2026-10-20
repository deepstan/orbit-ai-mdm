"""
THE AGENT RUNNER: our own stewardship agent, in SHADOW MODE (docs/agent-runner.md).

It works the review queue: for each item still waiting for a person it fetches the evidence, asks a model, and RECORDS the model's recommendation. That is all it can do. A recommendation
changes nothing about the item: a person still decides, and the platform later compares the two (agreement, calibration). The runner has no way to resolve, confirm, reject, give a verdict or
unmerge, and three things keep it that way:

  1. it talks to the platform over HTTP with ONE key, whose role is `steward_agent`: it can read what a steward reads and record a recommendation, and the platform refuses the rest.
     It never touches the database, so even a compromised or prompt-injected runner cannot decide anything
  2. a guard inside the runner refuses every request except four exact ones (read the queue, read an item's evidence, list recommendations, post one recommendation)
  3. what a model says is only ever a proposal: it is validated against the same rules as any proposal, and only then posted

It uses the SAME instructions and the SAME parsing as the replay (app/replay_agents.py), so what `replay_decisions.py` measures on past decisions is what this does on live ones.

OPERATING CONTROLS: a cap on items per pass; a persistent cap on model calls per day; a stop after too many errors; evidence too large to send is skipped; a KILL SWITCH (a file: while it
exists nothing is sent to the model, and a pass in progress stops before the next item); SIGINT/SIGTERM finish the current item and stop. Revoking the key stops it for certain.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from . import recommendations
from .mcp_server.client import ApiError, BackendUnavailable
from .replay_agents import AgentError

logger = logging.getLogger("mdm.runner")

_DOMAIN = "[a-z][a-z0-9_]{0,63}"
_ID = r"[A-Za-z0-9_.:\-]{1,128}"
ALLOWED = (
    ("GET", re.compile("^/v1/" + _DOMAIN + "/review-queue$")),
    ("GET", re.compile("^/v1/" + _DOMAIN + "/review-queue/" + _ID + "/evidence$")),
    ("GET", re.compile("^/v1/" + _DOMAIN + "/recommendations$")),
    ("POST", re.compile("^/v1/" + _DOMAIN + "/review-queue/" + _ID + "/recommendations$")),
)
LOOKUP_PAGES = 25                  # how many pages of existing recommendations are read to find what is already done (200 each)


class RunnerGuard(RuntimeError):
    """The runner tried to ask the platform for something it is not allowed to."""


class GuardedClient:
    """The only way the runner reaches the platform: four exact (method, path) pairs, nothing else, whatever the code above asks."""

    def __init__(self, client):
        self._client = client

    @staticmethod
    def _check(method: str, path: str) -> None:
        if not any(m == method and rx.match(path) for m, rx in ALLOWED):
            raise RunnerGuard(f"the runner is not allowed to {method} {path}")

    def get(self, path: str, query: dict, api_key: str):
        self._check("GET", path)
        return self._client.get(path, query, api_key)

    def post(self, path: str, body: dict, api_key: str):
        self._check("POST", path)
        return self._client.post(path, body, api_key)


class _Stop(Exception):
    """Why a pass ended early. kind: problem (something is wrong: a refused key, too many errors), cap (the day's model calls are used up), switch (a person asked: the kill switch or a signal)."""

    def __init__(self, reason: str, kind: str = "problem"):
        super().__init__(reason)
        self.reason, self.kind = reason, kind


@dataclass
class RunnerConfig:
    domain: str
    max_items: int = 50               # items asked about in one pass
    max_per_day: int = 500            # model calls per UTC day, kept across passes and restarts in the state file
    max_errors: int = 10              # stop a pass after MORE than this many errors
    max_evidence_chars: int = 60_000  # larger evidence is not sent to a model
    page: int = 50
    dry_run: bool = False             # list what would be asked and how big it is; no model call, nothing recorded
    stop_file: str | None = None      # the kill switch: while this file exists nothing is sent to a model
    state_file: str | None = None

    def __post_init__(self):
        if not re.fullmatch(_DOMAIN, self.domain or ""):
            raise ValueError("the domain is not valid")
        for name in ("max_items", "max_per_day", "max_errors", "max_evidence_chars", "page"):
            if not isinstance(getattr(self, name), int) or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a whole number, 0 or more")
        if self.page < 1:
            raise ValueError("page must be at least 1")


class Runner:
    def __init__(self, client, api_key: str, agent, config: RunnerConfig, clock=None, sleep=time.sleep):
        self.client, self.api_key, self.agent, self.cfg = GuardedClient(client), api_key, agent, config
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep
        self._stop = False

    # ---- stopping
    def request_stop(self) -> None:
        """Called from a signal handler: finish the item in hand, then stop."""
        self._stop = True

    def stopping(self):
        """None, or the reason a person has asked for it to stop."""
        if self._stop:
            return "asked to stop (a signal)"
        if self.cfg.stop_file and os.path.exists(self.cfg.stop_file):
            return f"the kill switch file {self.cfg.stop_file} exists"
        return None

    # ---- the state that outlives a pass: today's model calls, and when it last ran
    def _load_state(self) -> dict:
        try:
            return json.load(open(self.cfg.state_file)) if self.cfg.state_file and os.path.exists(self.cfg.state_file) else {}
        except (OSError, ValueError):
            return {}

    def _save_state(self, state: dict) -> None:
        """Written to a temporary file and then moved into place, so a crash or a full disk never leaves half a file. If it cannot be written at all the runner STOPS: the file holds the day's
        count of model calls, and a runner that cannot keep that count cannot keep its daily limit."""
        if not self.cfg.state_file:
            return
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(self.cfg.state_file)), prefix=".runner-state-")
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, self.cfg.state_file)
            tmp = None
        except OSError as e:
            raise _Stop(f"the state file could not be written ({e.strerror or type(e).__name__}); without it the daily limit cannot be kept, so the runner stops")
        finally:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)

    # ---- talking to the platform: what each refusal means for the run
    def _call(self, method: str, path: str, payload: dict, summary: dict, tolerate=()):
        """Returns the answer; or a ('refused', status, message) marker for a status in `tolerate`. Raises _Stop for what no retry can fix."""
        for attempt in range(3):
            try:
                return self.client.get(path, payload, self.api_key) if method == "GET" else self.client.post(path, payload, self.api_key)
            except ApiError as e:
                if e.status in tolerate:
                    return ("refused", e.status, e.message)
                if e.status == 401:
                    raise _Stop("the platform refused the API key (401): the key is wrong, revoked, or for another platform")
                if e.status == 403:
                    raise _Stop(f"this key is not permitted to do that (403: {e.message}); the runner needs a key with the role steward_agent")
                if e.status == 429 and attempt < 2:
                    self.sleep(5 * (attempt + 1))
                    continue
                self._error(summary, f"the platform answered {e.status}")
                return None
            except BackendUnavailable:
                self._error(summary, "the platform could not be reached")
                return None
        self._error(summary, "the platform kept rate-limiting this key (429)")
        return None

    def _error(self, summary: dict, why: str) -> None:
        summary["errors"] += 1
        logger.warning("error: %s", why)
        if summary["errors"] > self.cfg.max_errors:
            raise _Stop(f"{summary['errors']} errors, more than --max-errors {self.cfg.max_errors} (last: {why})")

    def _already_recommended(self, summary: dict) -> set:
        """The items this agent (same model, same instructions) has already been recorded for and nobody has decided yet: asking again would only cost money."""
        done, cursor = set(), None
        for _ in range(LOOKUP_PAGES):
            page = self._call("GET", f"/v1/{self.cfg.domain}/recommendations", {"outcome": "open", "limit": 200, **({"cursor": cursor} if cursor else {})}, summary)
            if not isinstance(page, dict):
                break
            for r in page.get("items", []):
                if r.get("model") == self.agent.model and r.get("prompt_version") == self.agent.prompt_version:
                    done.add(r["candidate_id"])
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return done

    # ---- one pass
    def run_once(self) -> dict:
        s = {"seen": 0, "recommended": 0, "already_recommended": 0, "decided_meanwhile": 0, "too_large": 0, "errors": 0, "model_calls": 0, "planned": 0, "planned_chars": 0,
             "stopped": None, "stop_kind": None}
        state, today = self._load_state(), self.clock().date().isoformat()
        if state.get("date") != today:
            state.update(date=today, calls_today=0)
        try:
            reason = self.stopping()
            if reason:
                raise _Stop(reason, "switch")
            already = self._already_recommended(s)
            asked, cursor = 0, None
            while asked < self.cfg.max_items:
                page = self._call("GET", f"/v1/{self.cfg.domain}/review-queue", {"limit": min(self.cfg.page, 50), **({"cursor": cursor} if cursor else {})}, s)
                if not isinstance(page, dict):
                    break
                for item in page.get("items", []):
                    if asked >= self.cfg.max_items:
                        break
                    s["seen"] += 1
                    cid = item["candidate_id"]
                    if cid in already:
                        s["already_recommended"] += 1
                        continue
                    reason = self.stopping()
                    if reason:
                        raise _Stop(reason, "switch")
                    asked += 1
                    self._one(cid, state, s)
                cursor = page.get("next_cursor")
                if not cursor:
                    break
        except _Stop as stop:
            s["stopped"], s["stop_kind"] = stop.reason, stop.kind
        state.update(last_run=self.clock().isoformat(), last_summary={k: v for k, v in s.items() if k != "planned_chars"})
        state["total_recommended"] = state.get("total_recommended", 0) + s["recommended"]
        try:
            self._save_state(state)
        except _Stop as stop:
            if not s["stopped"]:
                s["stopped"], s["stop_kind"] = stop.reason, stop.kind
        s["calls_today"] = state["calls_today"]
        return s

    def _one(self, cid: str, state: dict, s: dict) -> None:
        evidence = self._call("GET", f"/v1/{self.cfg.domain}/review-queue/{cid}/evidence", {}, s, tolerate=(404,))
        if not isinstance(evidence, dict):
            if isinstance(evidence, tuple):
                s["decided_meanwhile"] += 1                           # gone between the queue and now
            return
        size = len(json.dumps(evidence, separators=(",", ":")))
        if size > self.cfg.max_evidence_chars:
            s["too_large"] += 1
            return
        if self.cfg.dry_run:
            s["planned"] += 1
            s["planned_chars"] += size
            return
        if state["calls_today"] >= self.cfg.max_per_day:
            raise _Stop(f"the limit of {self.cfg.max_per_day} model calls a day is used up (it resets at midnight UTC)", "cap")
        state["calls_today"] += 1
        self._save_state(state)                                      # counted BEFORE the call: a crash must not make it free
        s["model_calls"] += 1
        try:
            proposal = self.agent.recommend(evidence)
        except AgentError as e:
            self._error(s, f"the model gave no usable answer ({e})")
            return
        problems = recommendations.check_proposal(proposal.get("recommendation"), proposal.get("confidence"), proposal.get("reasons", []), proposal.get("evidence", []),
                                                  proposal.get("model", ""), proposal.get("prompt_version", ""))
        if problems:
            self._error(s, "the model's answer broke the rules: " + "; ".join(problems))
            return
        body = {k: proposal[k] for k in ("recommendation", "confidence", "reasons", "evidence", "model", "prompt_version") if k in proposal}
        answer = self._call("POST", f"/v1/{self.cfg.domain}/review-queue/{cid}/recommendations", body, s, tolerate=(404, 409))
        if isinstance(answer, tuple):
            s["decided_meanwhile"] += 1                              # a person decided it while the model was thinking: the expected race, not an error
        elif isinstance(answer, dict):
            s["recommended"] += 1
