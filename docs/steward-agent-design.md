# A stewardship agent: design (nothing past tier 1 is built)

Status (2026-10-16): a DESIGN, except tiers 1 and 2, which exist: the read-only MCP server (`docs/mcp-server.md`), the group-aware evidence endpoint, a record-history endpoint, the
`steward_agent` role, and (tier 2, opt-in) the `agent_recommendations` table, the API to record and read recommendations, the outcome recorded when a person decides, and the agreement statistics.
Also built: the replay harness that evaluates an agent against decisions people already made (`docs/replay.md`), and **our own runner, in shadow mode** (`agent_runner.py`, `docs/agent-runner.md`): it works the queue, asks a model and records recommendations, under a `steward_agent` key, with caps and a kill switch. And the **suggestion panel in the console**: collapsed until a steward opens it, neutral in style, and the decision records whether it was opened, so agreement can be measured on the decisions it did not influence (`docs/agent-runner.md`). Not built: a tuned prompt, running it with a real model or in a real browser, and tier 3.

## The idea

An agent works the review queue next to a person: it investigates each item, recommends a decision with its reasoning, and learns from what stewards decide. A person stays in charge by
default. It would replace the existing single-shot LLM adjudication (`app/llm_adjudication.py`: one API call per ambiguous pair, no tools, off by default, **never run against the real API**,
model name hard-coded), so there is one AI path, not two. That adjudication also compares the new record with the group's closest single member, the same blind spot the evidence endpoint fixes:
the agent must be given the group.

## What it does

1. **Triage**: orders the queue by what matters (big entities, pairs that would merge two large groups, easy ones that can be cleared in bulk).
2. **Investigates a pair**: both records, both groups, lineage, the score breakdown, whether either was ever separated or changed (`get_pair_evidence`, `get_record_history`).
3. **Recommends**: confirm, reject or "needs a human", with a confidence and plain reasons.
4. **Groups similar cases**: one verdict proposed for forty pairs with the same pattern, a sample to check.
5. **Reviews the audits**: reads the sampled automatic merges and puts the suspicious ones first.
6. **Explains**: why a pair scored what it did; what merging it would do.

## Three tiers of autonomy

| Tier | The agent | Needs | State |
|---|---|---|---|
| 1 | reads only | the read-only MCP server and a `steward_agent` key | **built** |
| 2 | proposes decisions: a recommendation is stored, nothing changes | the `propose_decision` tool (behind `--allow-propose`), the `agent_recommendations` table (candidate, recommendation, confidence, evidence names, model and prompt version, later the person's outcome), the `propose_decision` permission, agreement and calibration statistics | **built** (API, table, MCP tool, statistics, the console panel and the viewed-before-deciding record) |
| 3 | resolves narrow, measured classes on its own | the existing `resolve_review_queue` permission on its key; decisions signed `agent:<name>`; sampled audits and rollback exactly like the learned scorer | design, only if tier 2's measurements justify it |
| never | unmerges, separates or deletes | a person | by design |

## Guardrails

- A person's decision is a constraint: the agent can never override a separation or a confirm a person made (the engine already enforces it).
- Record text is untrusted data. The agent's tools are read-only, its output is validated before anything is stored, and the strongest control is that tier 1 has no tool that can do harm.
- Only the fields needed leave for the model; protected fields stay masked; a tenant opts in; a PII policy decides what a model provider may see.
- Every recommendation is audited: inputs, tool calls, reasoning, model and prompt version. Cost caps per tenant and a kill switch.

## Proving it before trusting it

Shadow mode first (`replay_decisions.py`, `docs/replay.md`): run it on pairs stewards have already decided, then on the live queue without acting, and measure agreement with stewards, calibration (is 90% confidence right 90% of
the time), precision per kind of case, and cost per decision. A **disagreement report** (where the kernel and the legacy engine differ, labelled by stewards) doubles as the evaluation set.
Tier 3 needs explicit thresholds ("99%+ agreement on this class over N decisions"), chosen with the owner, not guessed.

## What competitors have (public material, 2026-10-16; the numbers are their marketing)

| Vendor | What they say they have | Notes |
|---|---|---|
| Reltio | AgentFlow (launched 2025-08-05); a licensed Match Resolver agent that can merge or reject duplicates; agents act through an MCP server; billed in credits per call | the one that acts on pairs; claims up to 70% less manual work |
| Tamr | Curator Hub: LLM agents explain why records match or not, triggered by change events, exceptions queued by priority; low-confidence cases go to a human inbox; "bring your own agent" | closest to this design |
| Informatica | Data Steward Agent and "agentic multidomain MDM" announced for Q4 2026; data-exploration agents and a Copilot for MDM exist | not confirmed shipped |
| Ataccama | "ONE AI Agent", a digital data steward: creates, tests and deploys data-quality rules; agentic observability | mostly data quality, not pair adjudication |
| Profisee | Aisey agents for data quality and enrichment, an MCP server, vector matching | not a match-review agent |
| Stibo Systems | AgentWorkx announced 2026-09-29, 24 available and upcoming agents | which ones touch duplicates not seen |
| Semarchy | human-in-the-loop agentic stewardship workflows | its named agent is for data engineering |
| Senzing | an MCP server for developers (documentation only; never touches live data) | no stewardship agent found |

Not checked: Splink, AWS Entity Resolution, Quantexa. Sources: each vendor's own pages and press releases (reltio.com, docs.reltio.com, tamr.com, salesforce.com/news (Informatica, 2026-05-20),
ataccama.com, profisee.com, unite.ai (Stibo), semarchy.com, senzing.com). The common pattern: the agent explains and recommends, a human decides the hard cases, MCP is the door.

What this suggests: a stewardship agent is now table stakes, not a differentiator. What could differentiate is the evidence around it (shadow-mode measurement against real steward decisions,
human decisions as hard constraints, signed and sampled automatic actions with rollback). Whether competitors have that is not visible in public material.

## Phases

0. Run the existing adjudication against the real API on a labelled sample (it never has); this sets the baseline and fixes the hard-coded model name.
1. Tier 1 (built) in a real client and over the real API (`validate_api.py` section 7b, runbook step 12).
2. Tier 2: **table, permission, API, MCP tool and statistics are built, and so is the shadow-mode evaluation** (`replay_decisions.py`: replay the pairs people decided, report only, nothing written). The console panel is built (it reports whether a steward opened the suggestion, because reading it first biases the agreement it is judged by). Still to do: running the replay on real decisions, and opening the panel in a real browser.
3. Grouping similar cases and the audit reviewer.
4. **The runner (built, shadow mode only):** `agent_runner.py`, tested with a fake model; run it with a real one only after the replay has shown what to expect.
5. Tier 3, only if the evidence supports it.

## Decisions for the owner

1. Which model, and whose API key: the platform's or each tenant's.
2. What data may go to the model (the PII policy).
3. The ceiling on autonomy: suggest-only forever, or a narrow auto-resolve.
4. Who owns the agent's mistakes: I would say the audit and rollback process, as for the learned scorer.
