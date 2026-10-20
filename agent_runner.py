#!/usr/bin/env python3
"""
Our own stewardship agent, in SHADOW MODE (docs/agent-runner.md): works the review queue, asks a model about each item, and RECORDS a recommendation. It never decides anything:
a person still resolves every item, and the platform compares the two afterwards.

  MDM_API_URL=https://mdm.example.com MDM_API_KEY=<a steward_agent key> ANTHROPIC_API_KEY=... \\
      python3 agent_runner.py --domain customer --model <a model your account can use> --yes --max-items 20
  python3 agent_runner.py --domain customer --dry-run            (what it WOULD ask about, and how much would be sent; no model call, nothing recorded)
  python3 agent_runner.py --domain customer --model M --yes --loop --interval 300      (keep working; stop it with the kill switch file, or Ctrl-C)

KILL SWITCH: create the file named by --stop-file (default agent_runner.STOP). While it exists nothing is sent to the model. Revoking its API key stops it for certain.
"""
import argparse
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.agent_runner import Runner, RunnerConfig  # noqa: E402
from app.mcp_server.client import ProposingApiClient  # noqa: E402
from app.replay_agents import LlmAgent  # noqa: E402


def describe(s: dict, cap: int, dry_run: bool = False) -> str:
    lines = [f"seen {s['seen']} waiting items: recommended {s['recommended']}, already done {s['already_recommended']}, decided by a person meanwhile {s['decided_meanwhile']}, "
             f"too large to send {s['too_large']}, errors {s['errors']}; model calls today {s.get('calls_today', 0)} of {cap}"]
    if dry_run:
        lines.insert(0, f"DRY RUN: would ask about {s['planned']} items ({s['planned_chars']} characters of evidence in all). Nothing was sent to a model and nothing was recorded.")
    if s["stopped"]:
        lines.append(f"STOPPED ({s['stop_kind']}): {s['stopped']}")
    return "\n".join(lines)


def main(argv=None, sleep=time.sleep) -> int:
    p = argparse.ArgumentParser(description="Shadow-mode stewardship agent: records recommendations, decides nothing.")
    p.add_argument("--api-url", default=os.environ.get("MDM_API_URL", "http://127.0.0.1:8000"), help="the platform's address (default: $MDM_API_URL or http://127.0.0.1:8000)")
    p.add_argument("--domain", default="customer")
    p.add_argument("--model", help="the model to ask (a model your account can use); there is no default")
    p.add_argument("--yes", action="store_true", help="confirm that each item's evidence may be sent to the model's API")
    p.add_argument("--dry-run", action="store_true", help="list what would be asked and how much would be sent; no model call, nothing recorded")
    p.add_argument("--max-items", type=int, default=50, help="items to ask about in one pass (default 50)")
    p.add_argument("--max-per-day", type=int, default=500, help="model calls per UTC day, counted in --state across passes and restarts (default 500)")
    p.add_argument("--max-errors", type=int, default=10, help="stop a pass after MORE than this many errors (default 10)")
    p.add_argument("--max-evidence-chars", type=int, default=60000, help="do not send evidence larger than this to the model (default 60000)")
    p.add_argument("--state", default="agent_runner_state.json", help="where today's call count and the last summary are kept (no evidence, no keys)")
    p.add_argument("--stop-file", default="agent_runner.STOP", help="the kill switch: while this file exists nothing is sent to the model")
    p.add_argument("--loop", action="store_true", help="keep working: another pass every --interval seconds, until stopped")
    p.add_argument("--interval", type=int, default=300, help="seconds between passes with --loop (default 300)")
    p.add_argument("--timeout", type=float, default=20.0, help="seconds to wait for the platform")
    args = p.parse_args(argv)

    key = os.environ.get("MDM_API_KEY")
    if not key:
        p.error("MDM_API_KEY is not set: the runner needs an API key whose role is steward_agent (it reads what a steward reads and may only RECORD recommendations)")
    agent = None
    if not args.dry_run:
        if not args.model:
            p.error("--model is required (a model your account can use); use --dry-run to see what would be asked without one")
        if not os.environ.get("ANTHROPIC_API_KEY"):
            p.error("ANTHROPIC_API_KEY is not set in the environment")
        if not args.yes:
            p.error("the runner sends each item's evidence to the model's API (protected fields are masked by the platform, but names, emails and addresses are not); add --yes to confirm")
        agent = LlmAgent(args.model)
    else:
        class _NoModel:
            model, prompt_version = args.model or "", ""
            def recommend(self, evidence):
                raise AssertionError("a dry run never asks a model")
        agent = _NoModel()
        agent.prompt_version = LlmAgent("m").prompt_version

    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")
    try:
        config = RunnerConfig(domain=args.domain, max_items=args.max_items, max_per_day=args.max_per_day, max_errors=args.max_errors, max_evidence_chars=args.max_evidence_chars,
                              dry_run=args.dry_run, stop_file=args.stop_file, state_file=args.state)
    except ValueError as e:
        p.error(str(e))
    runner = Runner(ProposingApiClient(args.api_url, timeout=args.timeout), key, agent, config, sleep=sleep)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: runner.request_stop())
    print("SHADOW MODE: this only records recommendations; a person still decides every item. " + ("DRY RUN: nothing will be sent to a model.\n" if args.dry_run else
          "Each item's evidence is sent to the model: protected fields are masked by the platform; names, emails and addresses are not.\n")
          + f"Kill switch: create the file {args.stop_file}. Revoking the API key stops it for certain.", file=sys.stderr)
    while True:
        summary = runner.run_once()
        print(describe(summary, args.max_per_day, args.dry_run), flush=True)
        if summary["stop_kind"] in ("problem", "switch") or not args.loop or args.dry_run:
            return 1 if summary["stop_kind"] == "problem" else 0
        for _ in range(max(1, args.interval)):                       # sleep in one-second steps, so the kill switch and signals are noticed at once
            if runner.stopping():
                break
            sleep(1)


if __name__ == "__main__":
    sys.exit(main())
