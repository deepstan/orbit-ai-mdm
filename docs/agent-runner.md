# The agent runner: our own stewardship agent, in shadow mode

Status (2026-10-17): **built and tested here with a fake model and stub servers; never run with a real model, and never against the real HTTP API.** `agent_runner.py`,
`app/agent_runner.py`, `tests/test_agent_runner.py` (41 tests). It is a **shadow** agent: it records what it thinks, a person still decides every item.

## What it does

A loop over the review queue: for each item still waiting for a person it fetches the evidence (the group-aware evidence endpoint), asks a model, validates the answer, and **records a
recommendation** (`POST .../recommendations`). The platform later compares each recommendation with what a person decided: agreement, calibration, false merges
(`GET /v1/{domain}/recommendations/stats`). That is how you learn whether it can be trusted, before anything lets it act.

It uses the **same instructions and the same parsing as the replay** (`app/replay_agents.py`, prompt version `steward-v1`), so what `replay_decisions.py` measures on past decisions is what the
runner does on live ones. Changing the prompt means changing its version string, or the statistics would mix two different agents; the version is stored with every recommendation.

## What it can never do

Three separate locks:

1. **It holds one API key, whose role is `steward_agent`**, and talks to the platform over HTTP. It never touches the database. The role can read what a steward reads and record a
   recommendation; the platform refuses everything else (resolve, verdicts, unmerge, ingest, purge, administration).
2. **A guard inside the runner** refuses every request except four exact ones: read the queue, read one item's evidence, list recommendations, post one recommendation. A test pins the four.
3. **A model's answer is only ever a proposal.** It is validated against the same rules as any proposal (a recommendation of confirm, reject or needs_human; a confidence from 0 to 1; short
   reasons) and only then posted. A record whose name field says "ignore your instructions" can at worst produce a poor recommendation, which a person ignores or overrules: no tool exists that
   could do more.

## Run it

```
python3 manage_api_keys.py create --tenant acme --role steward_agent --label "runner-1" --domains customer
MDM_API_URL=https://mdm.example.com MDM_API_KEY=<that key> python3 agent_runner.py --domain customer --dry-run
MDM_API_URL=... MDM_API_KEY=... ANTHROPIC_API_KEY=... python3 agent_runner.py --domain customer --model <a model your account can use> --yes --max-items 5
MDM_API_URL=... MDM_API_KEY=... ANTHROPIC_API_KEY=... python3 agent_runner.py --domain customer --model <model> --yes --loop --interval 300
```

Keys are read from the environment only (never from the command line, where `ps` would show them). There is no default model. One key is one tenant: to serve several tenants, run one runner each.

- `--dry-run` lists how many items it would ask about and how many characters of evidence would be sent. No model call, nothing recorded. Use it first.
- A pass asks about at most `--max-items` items (default 50); the next pass carries on with the next ones, because an item already recommended by the same model and prompt version is skipped.
  A **different model or prompt version gets its own, additional opinion** (nothing is replaced), which is how two models can be compared on the same live items.
- `--loop --interval N` keeps working. Without `--loop` it is one pass, which suits `cron`.

## Controls

| Control | What it does |
|---|---|
| `--max-items` | model calls in one pass |
| `--max-per-day` (default 500) | model calls per UTC day, **kept in the state file across passes and restarts**; counted *before* each call, so a failed call is not free. If the state file cannot be written, the runner stops: it could not keep its limit |
| `--max-errors` (default 10) | stops a pass after MORE than this many errors (a wrong model name, a failing API, rate limits) so a broken setup does not keep running or spending |
| `--max-evidence-chars` (default 60000) | evidence larger than this is not sent; counted as "too large" |
| **kill switch** `--stop-file` (default `agent_runner.STOP`) | while that file exists nothing is sent to a model, and a pass in progress stops before the next item. Create it from anywhere that can reach the machine |
| signals | SIGINT and SIGTERM finish the item in hand and exit cleanly |
| revoking the key | stops it for certain, whatever else is wrong |

Exit status: 0 for a normal end, a dry run, the kill switch or the daily limit; 1 if it stopped because something is wrong (a refused or unauthorised key, too many errors).
A key that cannot record recommendations (a steward's, a reader's) or is refused stops it at the first refusal, after at most one model call.

## What leaves the building

Each item's evidence goes to the model's API. The platform masks protected fields (as in the review queue); **names, emails, addresses and phone numbers are not protected fields and are sent.**
It will not start without `--yes` and says this when it does. Decide your policy before the first real run: which tenants, which domains, which model provider. The state file holds counts and
times only (no evidence, no keys), and the logs on standard error carry no evidence and no keys.

## How it is meant to be introduced

1. Run the **replay** on your real decisions with the same model (`replay_decisions.py --agent llm --model ...`): it tells you what to expect before anything is live.
2. `--dry-run`, then `--max-items 5`, with a low `--max-per-day`.
3. Let stewards work as usual. Watch `GET /v1/{domain}/recommendations/stats` and compare it with the replay.
4. Only then think about whether it should ever do more than recommend. That is a separate decision with its own design (`docs/steward-agent-design.md`, tier 3), and nothing here makes it.

## What stewards see: the suggestion panel in the console

On a review card that has a recommendation, the console shows one line: `Assistant's suggestion · available · advisory`, **closed**. Nothing of what the suggestion says is in the page until a steward opens it.
Opened, it shows each opinion (`Same entity: merge`, `Not a match`, or `Can't tell: needs a person`), how sure it is, its short reasons, and which agent, model and prompt version wrote it, with a note that
it is an AI model's opinion, that it changed nothing and can be wrong, and that the steward decides; and how often its suggestions have matched stewards' decisions so far, with how few cases that
is when there are few. It is placed after the two records, so they are read first.

It is deliberately plain, because a suggestion can steer the person reading it. "Merge" and "not a match" look the same (no green or red); there is **no accept button** and the two real buttons do not change
with the suggestion; and what a model wrote goes through the same escaping as every other piece of source data.

**Why it is closed, and what the decision reports.** The statistics ask how often the assistant agrees with stewards. If stewards read it first, part of that agreement is the suggestion influencing them,
and the measurement is quietly biased. So the console sends, with each decision, whether the steward had opened the suggestion: `suggestion_viewed` is `true` (opened, even if closed again), `false`
(one was there and they did not open it), or left out (there was none, or the decision did not come from the console). The platform keeps it with the outcome (`agent_recommendations.outcome_seen`,
migration 018), and `GET /v1/{domain}/recommendations/stats` reports agreement apart in `by_viewing`: **`decided_blind` is the unbiased measure of the assistant**; `decided_after_viewing` may partly
measure it swaying people (compare the two to see how much); `viewing_not_known` is everything else (scripts, older clients). Judge the assistant on the blind figure, with its lower bound.

The console works only in development mode (it has no sign-in yet), as the demo tenant `default`.

## It replaces the old LLM adjudication

`ENABLE_LLM_ADJUDICATION` (`app/llm_adjudication.py`) asks a model, inline, whether ONE record matches the closest ONE member of a group, and acts on a confident answer. It sees one member (not the group), has never run against the real API, is not
measured against what stewards decide, and had its model name hard-coded. Using it now logs a one-time DEPRECATED notice, and the model is set with `MDM_LLM_MODEL`. The runner differs in what matters: it sees whole groups,
**records a recommendation instead of deciding**, is measured (agreement, false merges, calibration), and has caps and a kill switch. The old path comes out once the runner has shadow-mode results to compare with
(`docs/legacy-engines.md`).

## Not built, and not proven

- **Never run with a real model or over the real HTTP API.** The tests use a fake model, a stub HTTP server for each side, and the real endpoint functions on a real database.
- **The console panel has never been opened in a real browser.** It is tested by running the console's real script in Node against a stub page (as the console's escaping test does), which cannot show how a real browser fires the open/close event of a `<details>` element or how the panel looks. The runbook (step 14) has a manual check.
- **It remembers that a steward opened a suggestion only for the life of the page.** Reload the page after opening one and the decision reports "did not open it": a small undercount of the stewards who looked, never an overcount.
- **Single-shot**: it gives the model the evidence once and takes one answer; it does not look anything else up. A fuller agent that decides what to look at is possible, but it is not needed for the first step.
- **It counts calls, not tokens or money.** The daily limit is a limit on calls; the evidence size limit bounds each call.
- **The prompt is a first draft**, not tuned on your data. That is what the replay is for.
- **No multi-tenant fan-out, no retry queue, no metrics endpoint.** A pass that stops early simply tries again next time.
