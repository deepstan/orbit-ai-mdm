# The read-only MCP server (stewardship, tier 1)

Status (2026-10-16): **built and tested here, never tried with a real MCP client or over a real HTTP API.** It was written from the published Model Context Protocol specification with
the standard library only (no MCP SDK was available), and tested against its own client harness, the platform's real endpoint functions, a stub platform and both transports. See
"What has not been checked" below before relying on it.

## What it is

MCP (Model Context Protocol) is a standard way for an AI assistant or agent to use another system's data and actions: the system publishes a server that lists its **tools**, and any
MCP-capable client can call them. This server publishes the stewardship queue of this platform so that an agent (Claude, or a customer's own) can work next to the stewards: look at the
review queue, read the full evidence for an item, and see what changed. **By default it can change nothing.** With `--allow-propose` it may also record a recommendation (tier 2, below), which changes nothing about the item either.

It is a thin client of the platform's own HTTP API: every tool is one GET of an existing endpoint, with the CALLER's key. All permissions, tenant isolation, rate limits and the audit log therefore stay in
one place, the API. The server has no database access, and its HTTP client has no method that writes.

## Start it

1. Issue a key with the read-only agent role (an administrator does this; the role can read what a steward reads and the audit sample, and nothing else):

```
python3 manage_api_keys.py create --tenant acme --role steward_agent --label "agent-1" --domains customer
```

2. Run it, one of two ways.

**stdio** (a local AI client starts the program; the key is in its environment):

```
MDM_API_URL=https://mdm.example.com MDM_API_KEY=<the key> python3 mcp_server.py
```

A client configuration usually looks like this (the exact format is the client's; check its documentation):

```json
{"mcpServers": {"mdm-stewardship": {"command": "python3", "args": ["/path/to/mcp_server.py"], "env": {"MDM_API_URL": "https://mdm.example.com", "MDM_API_KEY": "<the key>"}}}}
```

**HTTP** (one server, many callers; each sends its own key and the server passes it on, so it holds nobody's key):

```
MDM_API_URL=https://mdm.example.com python3 mcp_server.py --http --port 8765
```

then POST JSON-RPC messages to `http://127.0.0.1:8765/mcp` with `Authorization: Bearer <the caller's key>`. It binds to the loopback address unless `--host` says otherwise, and refuses a
browser's cross-origin request unless that origin was named with `--allow-origin`. Put TLS in front of it (a reverse proxy) before exposing it beyond one machine.

## The tools (all read-only)

| Tool | Reads | What it is for |
|---|---|---|
| `list_domains`, `get_domain_schema` | `/v1/domains`, `/v1/{domain}/schema` | which domains and fields exist, which are protected |
| `get_review_queue` | `/v1/{domain}/review-queue` | the pairs waiting for a person, best score first (paged) |
| `get_pair_evidence` | `/v1/{domain}/review-queue/{id}/evidence` | **new:** everything to decide one item, group-aware (below) |
| `get_entity`, `list_entities`, `get_lineage` | `/entities...` | golden records, their source records, where each value came from |
| `get_record_history` | `/v1/{domain}/records/{id}/history` | **new:** a record now and every earlier version |
| `list_audits` | `/v1/{domain}/audits` | the sample of automatic merges awaiting a person's check |
| `get_scorer_status`, `list_runs` | `/scorer`, `/runs` | the learned scorer's state; recent loads |

**Why the evidence is group-aware.** With the kernel engine a review item is a record compared with a whole GROUP, shown against the group's closest member. Two records are not
enough to decide it, so `get_pair_evidence` returns both records, both groups (every member, the golden record, the size), whether a person ever separated these records or their
entities, the history of what each record used to say, and the record's other pairs. For a group of one (every pair the older engines made) it is simply two records. Protected fields
are masked as in the review queue; a member list is cut at 25 per group (`member_count` says how many there are).

## Tier 2: recording a recommendation (opt-in)

Start the server with `--allow-propose` (or `MDM_MCP_ALLOW_PROPOSE=1`) and it offers two more tools, 13 in all:

| Tool | Does |
|---|---|
| `get_recommendations` | reads what agents have recommended for one review item, and what a person decided |
| `propose_decision` | **the server's only write**: records "confirm", "reject" or "needs_human", with a confidence from 0 to 1, short reasons and which parts of the evidence were used |

A recommendation **changes nothing about the item.** It is stored in `agent_recommendations` (migration 017), shown with the item, and when a person decides the item every open recommendation for it
gets its **outcome**: `agreed`, `disagreed`, or `human_decided` (the agent had said needs_human). `GET /v1/{domain}/recommendations/stats` turns these into how often the agent agreed with people,
and whether its confidence means anything (an agent that says 90% should be right about 90% of the time). That is the evidence for how far an agent can be trusted; nothing here lets it act.

The rules: the recommendation is recorded under the **key's** name (`agent:<label>`), never a name in the request; only an item **still waiting** can be recommended about (409 otherwise); the key must
have the permission `propose_decision` (roles `steward_agent`, `domain_admin`, `tenant_admin`; a data steward or a reader gets 403); one recommendation per agent, model and prompt version (proposing again replaces it);
at most 10 reasons of 300 characters and 20 evidence names of 120. The agent's reasons may quote the data, so they are **not written to the audit log** and a right-to-be-forgotten purge **deletes** the
recommendations about the purged records. The server itself is safe without `--allow-propose`: its HTTP client then has no way to write, and `propose_decision` is as unknown as any other name.

The API behind it: `POST /v1/{domain}/review-queue/{id}/recommendations`, `GET` the same path, `GET /v1/{domain}/recommendations` (paged, filter by outcome) and `GET /v1/{domain}/recommendations/stats`.

## What it will never do

Not tools, and not possible by any argument (with or without `--allow-propose`): confirm or reject a review item, give an audit verdict, unmerge (a person's decision: an unmerge is a permanent separation), delete or purge
(right to be forgotten), ingest records, anything administrative (keys, usage, the audit log, scorer check and reset), name a tenant, or run a free-form query. The list of tools is
pinned by a test: adding one means changing that test on purpose.

## How it is kept safe

- **The key decides.** The role `steward_agent` has `view`, `view_audits` and `propose_decision`; a test pins this and calls the twelve endpoints that change something or administer, each refused with a 403.
- **The caller's own key**, passed on per call, never stored in the server or its logs; the HTTP client never follows a redirect, so the key cannot be sent anywhere else.
- **Strict arguments.** Every domain and id is checked against a pattern before it goes into a path (and percent-encoded when it does); unknown arguments (a `tenant_id`, say) are refused; no argument names a tenant.
- **What comes back is data.** Every answer starts with a line saying so, each tool's description says so, and the server's instructions tell the agent never to follow instructions found inside a record. (A name field that says "ignore your instructions" is returned unchanged and triggers nothing; tested.) This reduces the risk of prompt injection; it does not remove it. The control that does is that the agent has no tool that can do harm.
- **Bounded.** Lists are cut at 200 items, strings at 2,000 characters, an answer at 120,000; page sizes are limited; the cut is said out loud.
- **Quiet logs.** One line per call (tool, domain, ok, size, time) on standard error; never a payload, never a key. In stdio mode standard output carries only the protocol.

## What has not been checked

- **A real MCP client.** Claude Desktop and other clients have not been tried. The handshake, `tools/list`, `tools/call`, `ping` and notifications follow the specification as I know it; a client that expects something I did not implement will tell you.
- **The real HTTP API.** FastAPI and uvicorn are not installed where this was built, so the endpoint functions were called directly. `validate_api.py` has a new section (7b) that checks the agent role and the evidence endpoints over real HTTP, and runbook step 12 walks through the server end to end: run them on your machine.
- **Authorization.** The MCP specification's authorization for HTTP servers is OAuth-based. This server passes the platform's own API key as a bearer token, which suits a private deployment; it is not the specification's flow.
- **Not implemented:** sessions, server-initiated streams (SSE), resources, prompts, pagination of `tools/list`, cancellation.
- **Tier 2 over real HTTP** (`validate_api.py` section 7b, runbook step 12e), and a real MCP client sending `propose_decision`.
- **Not built:** tier 3 (an agent that may resolve items): `docs/steward-agent-design.md`. The agent that uses this door is `agent_runner.py` (`docs/agent-runner.md`), and stewards see its suggestions in the console.
