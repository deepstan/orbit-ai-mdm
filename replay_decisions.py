#!/usr/bin/env python3
"""
Replay the review items people already decided through an agent, and report how often it would have agreed (docs/replay.md). READ-ONLY: it never writes to the database.

  python3 replay_decisions.py --tenant acme --domain customer --agent rule
  python3 replay_decisions.py --tenant acme --domain customer --agent command --agent-command "python3 my_agent.py" --limit 200 --out results.jsonl
  python3 replay_decisions.py --tenant acme --domain customer --agent llm --model <a model your account can use> --limit 50 --yes --out results.jsonl
  python3 replay_decisions.py --report-only results.jsonl            (no database: print the report again from saved results)

--out keeps every result as it is made, and a second run with the same file skips what is done, so a long run that stops costs nothing to restart.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import protect, replay  # noqa: E402
from app.replay_agents import CommandAgent, LlmAgent, RuleAgent  # noqa: E402


def load_results(path: str) -> dict:
    done = {}
    if path and os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if line.strip():
                r = json.loads(line)
                done[r["candidate_id"]] = r
    return done


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Replay decided review items through an agent (read-only).")
    p.add_argument("--tenant")
    p.add_argument("--domain", default="customer")
    p.add_argument("--db", default=None, help="SQLite path or postgresql:// URL (default: DATABASE_URL env or mdm.db)")
    p.add_argument("--agent", choices=("rule", "command", "llm"), default="rule")
    p.add_argument("--agent-command", help="for --agent command: the program to run; it reads the evidence JSON on standard input and prints the answer as one JSON object")
    p.add_argument("--model", help="for --agent llm: the model to use (a model your account can use)")
    p.add_argument("--rule-hi", type=float, default=0.85, help="for --agent rule: confirm at or above this score")
    p.add_argument("--rule-lo", type=float, default=0.55, help="for --agent rule: reject at or below this score")
    p.add_argument("--limit", type=int, default=100, help="how many decided items to replay, sampled at random (default 100)")
    p.add_argument("--seed", type=int, default=1, help="the sampling seed, so a run can be repeated")
    p.add_argument("--include-audits", action="store_true", help="also replay the automatic merges a person checked in the audit (judged on the merge as it stood when the system made it); reported apart from review decisions")
    p.add_argument("--max-errors", type=int, default=10, help="stop after the agent has failed MORE than this many times, so a broken setup does not keep running (default 10; -1 = never stop)")
    p.add_argument("--allow-approximate", action="store_true", help="also replay items whose past cannot be rebuilt exactly (a record changed or was unmerged after the decision); they are reported apart")
    p.add_argument("--out", help="a JSONL file that keeps each result as it is made, and lets a stopped run resume")
    p.add_argument("--json", dest="json_out", help="also write the report as JSON to this file")
    p.add_argument("--report-only", help="print the report from this results file and stop (no database)")
    p.add_argument("--yes", action="store_true", help="for --agent llm: confirm that the evidence (protected fields masked) may be sent to the model's API")
    args = p.parse_args(argv)

    if args.report_only:
        results = list(load_results(args.report_only).values())
        report = replay.summarize_replay(results)
        print(replay.format_report(report))
        if args.json_out:
            json.dump(report, open(args.json_out, "w"), indent=2)
        return 0
    if not args.tenant:
        p.error("--tenant is required (or use --report-only)")

    if args.agent == "llm":
        if not args.model:
            p.error("--agent llm needs --model")
        if not os.environ.get("ANTHROPIC_API_KEY"):
            p.error("--agent llm needs ANTHROPIC_API_KEY in the environment")
        if not args.yes:
            p.error("--agent llm sends the evidence of each replayed item to the model's API (protected fields are masked, but names, emails and addresses are not); add --yes to confirm")
        agent = LlmAgent(args.model)
    elif args.agent == "command":
        if not args.agent_command:
            p.error("--agent command needs --agent-command")
        agent = CommandAgent(args.agent_command)
    else:
        agent = RuleAgent(args.rule_hi, args.rule_lo)

    from app.store_factory import get_store
    store = get_store(args.db)
    items = replay.labelled_items(store, args.tenant, args.domain, include_audits=args.include_audits)
    chosen = replay.sample_items(items, args.limit, args.seed)
    what = "review items were decided by a person" + (" or automatic merges checked by one" if args.include_audits else "")
    print(f"READ-ONLY: nothing is written to the database. {len(items)} {what}; replaying {len(chosen)} of them with the {agent.name} agent.", file=sys.stderr)
    done = load_results(args.out)
    reused = sum(1 for i in chosen if "recommendation" in done.get(i.candidate.id, {}))
    if reused:
        print(f"{reused} of them are already in {args.out} and are not asked again (an item that failed is tried again).", file=sys.stderr)
    sink = open(args.out, "a", encoding="utf-8") if args.out else None
    count = {"n": 0}

    def keep(result):
        count["n"] += 1
        if sink:
            sink.write(json.dumps(result) + "\n")
            sink.flush()
        if count["n"] % 10 == 0:
            print(f"  {count['n']} replayed ...", file=sys.stderr)
    stopped = False
    try:
        results = replay.run_replay(store, args.tenant, args.domain, chosen, agent, mask=lambda attrs: protect.mask_attributes(args.domain, attrs),
                                    allow_approximate=args.allow_approximate, done=done, on_result=keep, max_errors=None if args.max_errors < 0 else args.max_errors)
    except replay.TooManyErrors as e:
        results, stopped = e.results, True
        print(f"STOPPING: the agent has failed {e.errors} times, more than --max-errors {e.limit}. What was done is kept"
              + (f" in {args.out}; fix the agent and run the same command again: finished items are not asked twice." if args.out else "; use --out next time so a run can be resumed."), file=sys.stderr)
    finally:
        if sink:
            sink.close()
    report = replay.summarize_replay(results, stopped_early=stopped)
    print(replay.format_report(report))
    if args.json_out:
        json.dump(report, open(args.json_out, "w"), indent=2)
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
