# Replay: how would an agent have done on decisions people already made?

Status (2026-10-17): **built and tested on small synthetic data; never run on real decisions, and the LLM agent never ran against the real API** (its request, retries and parsing are
tested over a real HTTP round trip to a stub server). `replay_decisions.py`, `app/replay.py`, `app/replay_agents.py`, `tests/test_replay.py` (56 tests).

## What it is for

Before an agent touches a live review item, ask the question that decides whether it can be trusted: **how often does it agree with the people who decide, and does its confidence mean
anything?** The platform already holds the answer key: every pair a steward confirmed or rejected. Replay takes those pairs, shows an agent the evidence as it stood when the person decided,
and compares its recommendation with what the person decided. It **reads the database and writes nothing**, so shadow results never mix with live recommendations.

## The trap, and how it is avoided

Looked at today, a decided pair gives away its own answer: the candidate carries its verdict and who decided it; a confirmed pair's two groups have been merged into one; the record may
have changed since. An agent shown that would score perfectly and prove nothing. So the evidence is **rebuilt as of the decision time** (the same shape the live evidence endpoint
returns, so one agent works on both):

- the two groups are the connected components of the confirmed pairs decided **before** the decision, **minus the pair itself** (removing the edge a confirm added splits the merge again);
  a record that joined a group *after* the decision is not in it
- every record is shown as it was at that moment (record history), and the golden records are recomputed from those versions
- the candidate is shown as `pending`, with no decider; a separation counts only if a person made it **before**; the record's other pairs are left out (they carry other decisions)
- protected fields are masked exactly as in the review queue

**When the past cannot be rebuilt exactly**, the item is refused with the reason and counted, not guessed: a record in the groups was changed *after* the decision, a pair touching them was
unmerged after it, the two records are still joined by other confirmed pairs, or a record was purged. `--allow-approximate` replays the first two kinds anyway (they are shown as they
were, flagged `approximate`) and the report keeps exact and approximate apart.

**Who is replayed:** pairs a *person* decided in review. Not pairs a person unmerged (their decision time was overwritten), not what is still pending. By default not automatic merges either; **`--include-audits` adds the automatic merges a person checked in the audit** (verdict correct means the groups should merge, wrong means they should not). An audited merge is shown **as it stood when the system made it**, with the same exact-or-refused rule, and the report keeps audits and review decisions apart: they are different populations (audits are high-score merges, review items are the ambiguous middle), so do not average them. Items whose reasons carry the old LLM adjudication's verdict show up under the `match_type` slice: an agent that merely repeats that verdict looks good there.

## Run it

```
python3 replay_decisions.py --tenant acme --domain customer --agent rule
python3 replay_decisions.py --tenant acme --domain customer --agent command --agent-command "python3 my_agent.py" --limit 200 --out results.jsonl
python3 replay_decisions.py --tenant acme --domain customer --agent llm --model <a model your account can use> --limit 50 --yes --out results.jsonl
python3 replay_decisions.py --report-only results.jsonl
```

- `--agent rule`: confirm at or above a score, reject at or below another, otherwise needs_human. A **baseline**. Review items sit in the ambiguous band by construction, so expect it to say needs_human a lot.
- `--agent command`: any program. The evidence JSON arrives on its standard input; it prints one JSON object (`recommendation`, `confidence`, optional `reasons`, `evidence`). This is how to test YOUR agent without writing code here.
- `--agent llm`: a Claude model through the Messages API. **It sends each replayed item's evidence to the model's API** (protected fields are masked; names, emails and addresses are not), so it refuses to run without `--yes`, a `--model` and `ANTHROPIC_API_KEY`. There is no default model.
- `--out FILE` keeps each result as it is made; a second run with the same file skips what has an answer, so a long run that stops costs nothing to restart. **An item that failed is tried again, not remembered as failed** (a failure is usually a hiccup).
- `--max-errors N` (default 10; -1 = never) stops the run once the agent has failed MORE than N times, so a broken setup (a wrong key, a model that does not exist, a program that crashes) does not keep running or spending. It keeps what it has, says so at the top of the report, and exits with status 1. `--report-only FILE` prints the report again with no database. `--json FILE` also saves the report.
- `--limit` (default 100) and `--seed` choose a repeatable random sample of the decided items.

## Reading the report

- **Agreement** with the person who decided, with a 95% lower bound (Wilson). A high rate on few cases means little: the bound says what can be claimed. `needs_human` is not counted as agreeing or disagreeing; **coverage** says how often it gave a verdict at all.
- **False merges** (it said confirm; the person said different) are the error that costs most: they join two real-world entities and undoing one is work. Reported two ways, each with how bad it could still be (95% upper bound): the share of **its confirms** that were wrong (how far to trust a confirm), and the rate among the **pairs that really were different** (how dangerous it is on a different pair). Zero false merges in 40 confirms does not mean zero risk: it could still be nearly 9%. **Missed merges** (it said reject; the person confirmed) are the cheap error and are counted separately. Pairs the agent never answered about are never counted as successes.
- **If it had acted only on its most confident answers**: for each confidence level, how many items it would have covered and what agreement it would have had. This is the evidence any later step toward acting on its own would need ("99%+ agreement on this kind of case over N decisions"); the lower bound is the number to read.
- **Calibration**: agreement by how sure it said it was. An agent that says 90% should be right about 90% of the time.
- **Baselines**: what always giving the commoner answer would score, and the **best single score cut tuned on these very items**. The second flatters the score (it is tuned on the answers), so an agent that cannot beat it adds nothing over the score.
- **Slices** by source (review or audit), by how exact the reconstruction was, by match type, by score band, and by whether a group has several records. The score band shows where the agent adds anything beyond the score.
- When two pairs were decided at the very same moment, which came first cannot be told, so the item is treated as inexact.

## Honest limits

- **People are noisy labels.** They disagree with each other and err; 100% is not the goal.
- **Selection.** Only items that reached a person are here: the hard, ambiguous ones. Agreement on them says nothing about the easy ones the system merged by itself.
- **Exactness depends on what the database recorded.** The tree of confirmed pairs plus the record history reconstruct most cases; a record that changed or was unmerged afterwards loses part of its past, and is flagged. A **record that left its group** may have lost a pair that is no longer visible.
- **Going forward, the exact fix is to keep the evidence when a person decides** (a snapshot stored with the decision). That is not built; until then replay is a reconstruction.
- It opens the store like every tool here, which applies any pending migrations; it then only reads. Read through the store interface, so it works on both databases; **not run on PostgreSQL**.

## History

The false-merge accounting, the early stop and the audit option were added after reviewing a separate implementation of the same idea that had appeared in the working directory (two devices on one project); its code was reimplemented here with its own tests, not copied.
