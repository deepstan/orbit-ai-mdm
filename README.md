# Multi-Domain MDM — Phase 1 + 2 + 3 MVP

A master data management prototype spanning **Customer, Product, and
Vendor** domains: ingestion → standardization → AI-assisted matching →
human-in-the-loop review → golden record with full lineage → active
learning, now with **RBAC/auth, an immutable audit log, right-to-be-
forgotten, usage metering, and a connector interface** on top.

## What's implemented, by phase

**Phase 1** — standardization, blocking, deterministic + fuzzy matching, survivorship, lineage, stewardship UI, REST API.

**Phase 2** — semantic embeddings, optional LLM adjudication, multi-tenancy, active learning, Product + Vendor domains (proving the engine is domain-agnostic via `app/domains.py`'s registry), Postgres backend.

**Phase 3 (this round)**
- **RBAC + API-key auth** (`app/auth.py`) — 4 roles (`tenant_admin`, `domain_admin`, `data_steward`, `api_consumer`), each API key optionally scoped to specific domains. Real, enforced permission checks — not a stub.
- **Immutable audit log** (`app/audit.py`) — hash-chained (blockchain-style) log of every ingest, review-queue decision, key lifecycle event, and purge. `verify_chain()` detects tampering, not just claims immutability — proven with a test that actually corrupts an entry and confirms detection.
- **Right-to-be-forgotten** (`app/compliance.py`) — permanently purges a golden record and every contributing source record across systems, logs the purge itself to the audit trail.
- **Usage metering / billing hooks** (`app/billing.py`) — real event tracking (ingests, API calls) with cost estimation; a pluggable `BillingProvider` interface with a working "log what would be charged" default and a documented Stripe extension point.
- **Connector interface** (`app/connectors.py`) — `BaseConnector` + a fully working `CSVConnector`; Salesforce/Shopify are documented scaffolds, not implementations (see caveat below).

## ⚠️ Honest status by feature

| Feature | Status |
|---|---|
| Standardization, matching, survivorship, lineage — all 3 domains | ✅ Fully tested |
| Cross-tenant AND cross-domain isolation | ✅ Fully tested |
| Semantic embeddings, active learning | ✅ Fully tested |
| **RBAC / permission checks** | ✅ Fully tested — role × action × domain matrix, all combinations |
| **Audit log hash chain, including tamper detection** | ✅ Fully tested — a test actually corrupts a stored entry and confirms `verify_chain()` catches it |
| **Right-to-be-forgotten purge** | ✅ Fully tested — entity + source records deleted, audit-logged, tenant/domain isolation preserved |
| **Usage metering** | ✅ Fully tested |
| **CSV / JSON connectors** | ✅ Fully tested, real file I/O, nested JSON paths |
| **Indexed blocking (candidate lookup)** | ✅ Fully tested + benchmarked live — fixed a real O(n²) problem, 2000 records in 15.9s with realistic data |
| **Bulk batching (`ingest_batch`)** | ✅ Fully tested — exact clustering parity with per-record ingestion verified (a real bug was caught and fixed here: an earlier version produced *different*, not just faster, results) |
| **JSONB storage (Postgres)** | ✅ Logic validated with a mock simulating real jsonb auto-adaptation; ⚠️ not run against a live server |
| **Postgres connection reuse** | ✅ Fixed a real, user-reported bug (378s for 200 records, down to expected ~seconds) — control flow validated with a mock, live behavior not verified here |
| LLM adjudication integration logic | ✅ Fully tested (API call itself mocked) |
| LLM adjudication **real API call** | ⚠️ Not tested — no network here |
| **FastAPI HTTP layer, including auth wiring** | ⚠️ **Not tested** — FastAPI isn't installable in this sandbox. The RBAC/audit/billing *logic* is tested directly (bypassing HTTP); the `api.py` wiring that calls it has not been run |
| **Schema migrations — SQLite** | ✅ Fully tested (fresh, legacy adoption, backfill, rollback, tamper detection, concurrency) |
| **Schema migrations — Postgres** | ⚠️ Control flow verified against a fake psycopg2; the SQL itself has not run on a live server |
| **Customer-defined domains** (config, validation, registry) | ✅ Fully tested; the built-ins are expressible in the same format with proven-identical output |
| **Stale-index guard + reindex job** (SQLite) | ✅ Fully tested (interruption, multi-tenant, upgrading old databases); the tests were checked by breaking the logic 8 ways |
| Stale-index guard / reindex SQL (Postgres) | ⚠️ Written, never run on a live server — covered by `test_postgres_backend.py` when `TEST_DATABASE_URL` is set |
| **Per-record processing state, resume, run log, realtime/batch modes** (`landed → staged → matched`, `process-pending`, `pipeline_runs`, deferred durable golden rebuilds, `?mode=`) | ✅ Tested (32 tests, checked by breaking it 16 ways; fixes a real stranded-record bug in the previous release) · ⚠️ API checked only under a stubbed FastAPI; Postgres SQL never run on a live server; no built-in scheduler |
| **One entity per record; oldest entity survives; offline reference matcher + `equivalence` check** (migration 013, `app/offline.py`) | ✅ Tested (16 tests, checked by breaking it 8 ways) · all three engines agree with the reference in every tried order on both benchmark profiles · ⚠️ not covered: learned model, LLM adjudication, a key crossing the cap mid-load |
| **SSN/DOB cleansing rules, quality issues without values, and `domains_cli.py profile`** (`ssn`/`dob` steps, `parse`/`options`) | ✅ Tested (60 unit and functional tests, checked by breaking it 15 ways; no performance tests) · wrong auto-merges on the defaulted-DOB scenario 56 → 11 of 75, junk-SSN review noise 75 → 0, but the review queue grew by about 160 pairs |
| **SSN protection: keyed hash for matching, mask for display, plain only in raw** (`"protect": "hash"`, `MDM_TOKEN_KEY`, key rotation by `reindex`, scrub of older rows) | ✅ Tested (30 tests, checked by breaking it 13 ways, including a scan of the whole database for the plain value; no performance tests) · ⚠️ DOB is not protected; plain values remain in raw and in superseded payloads; Postgres scrub never run on a live server |
| **Bulk pair-based engine** (`app/bulk.py`, `bulk_ingest.py --bulk`, resumable, dry run): standardize, pairs, score, cluster, write; into an empty tenant+domain, or **onboarding** a new source onto existing data (`--onboard`) | ✅ Tested (33 tests, checked by breaking it 13 ways): same entities and review pairs as the reference in every order; paused and resumed to identical results; rolled back on failure · **same 20,000 people through both engines: identical entities, review pairs, auto pairs and golden records; 11 s against 1,113 s (about 100x), writes included** · **1M dry run (exact): 1.39 pairs per record, 99.92% blocking recall, 85.4% of true duplicates merged, 10 impure entities, 1,210 records/s for stages A-D** · ⚠️ the write stage is timed at 20k only (1M is an estimate); **onboarding onto existing data (21 tests, 17 breakages caught): same result as the regular engine, existing entities merge with the oldest surviving, all changes to existing data in one transaction**; **delta loads of a source already loaded (new, unchanged, changed records) match the regular engine record by record (21 tests, 16 of 17 breakages caught, the 17th was dead code and was removed); measured: 500,000 records onto 500,000 in about 30 minutes on one core (282 records/s), no integrity problems**; Postgres batch writes never run live |
| **Scalable blocking: `transpose` keys, conditional groups, date-aware DOB, near-equal SSN** (`person.json` ships `scalable_tight_plus`) | ✅ Tested (30 tests, checked by breaking it 13 ways; engines still agree with the reference in any order) · **measured at 1M: 280 → 1.5 pairs per record, recall 99.9% against 99.7%** · ⚠️ 10M and 100M are extrapolated, not run (about 11 and about 86 pairs per record), and the 100M figure is above the target |
| **`person` benchmark: SSN/DOB scenarios and a scale harness** (`app/benchmark_person.py`) | ✅ Tested (23 tests; harness checked against brute force) · ⚠️ synthetic data, one core; **SSN/DOB placeholders are now cleansed (see the next row)** · SSN privacy handling is not built |
| **Idempotent raw ingest** (natural key, no-op re-send, in-place versions, history) | ✅ Tested (20 tests, checked by breaking it 9 ways); API behavior checked only under a stubbed FastAPI · ⚠️ Postgres SQL never run on a live server |
| **Typed golden columns** (`type`/`format` in the domain config, cast before survivorship, `quality_issues`, copy-and-swap retype) | ✅ Tested (30 tests, checked by breaking it 9 ways; found and fixed a real SQLite bug) · ⚠️ Postgres SQL never run on a live server; SQLite `numeric` is not exact |
| **Golden records as real tables** (`entity_<domain>`, a column per field; migration 009 from the JSON era) | ✅ Tested (18 tests, checked by breaking it 10 ways, incl. the migration) · ⚠️ Postgres SQL never run on a live server |
| **Staging layer** (raw -> standardized -> golden; golden from cleansed data) | ✅ Tested (20 tests, checked by breaking it 10 ways); matching results identical to before on the held-out benchmark · ⚠️ Postgres SQL never run on a live server |
| Console UI: output escaping, dynamic domain tabs | ✅ Escaping tested in Node against hostile input · ⚠️ not exercised in a real browser |
| **Matching evidence** (placeholders, penalties, suffix, nicknames, phonetic/window keys, bucket cap, score breakdown) | ✅ Tested (50 tests; checked by breaking the logic 11 ways) and measured on a benchmark, held-out seeds included · ⚠️ benchmark is synthetic and customer-only |
| Postgres SQL for index state, bucket stats, migrations 006/007, contributions | ⚠️ Written, never run on a live server |
| Postgres backend (incl. Phase 3 tables) | ⚠️ Written, not run. 26 Postgres tests skip without a server: 17 older ones, and `tests/test_store_contract.py`, one set of scenarios (unmerge, separations, audits, pages, migrations 014 to 016) that runs on SQLite here, which proves the tests, and on Postgres when `TEST_DATABASE_URL` is set. A static check (`tests/test_postgres_sql_lint.py`) finds no placeholder or percent-escaping mistake in its 148 statements, which says nothing about whether the SQL is valid Postgres. See the Postgres section and the runbook |
| **Row-Level Security (RLS)** | ⚠️⚠️ **The least-verified piece in this project** — SQL generation and the `SET LOCAL` mechanism are tested; actual cross-tenant denial by a live Postgres server has NOT been confirmed. See the RLS section — this needs two real database roles to validate meaningfully, which no mock can substitute for |
| **Salesforce / Shopify connectors** | ❌ **Not implemented** — scaffolds only. Real integrations need live API credentials this sandbox can't reach; unlike the LLM/Postgres code, there's no stable public contract to write against without a real account, so nothing was written to "validate later" here |

Run `python3 -m unittest discover -s tests -v` — 771 tests pass, 26 skip (Postgres-only).

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 seed_demo.py
uvicorn app.api:app --reload --port 8000
```

**Authentication is required by default.** Every request needs `Authorization: Bearer <api_key>` (create the first key with `manage_api_keys.py`, below). For a laptop and the demo, `MDM_DEV_MODE=1 uvicorn app.api:app --port 8000` turns authentication off: no key, tenant from the `X-Tenant-Id` header, full administrator rights, and the server says so loudly when it starts. **Never expose dev mode.** (It used to be the other way round, open unless `MDM_ENABLE_AUTH` was set, so forgetting one variable in production left the API open; that variable now does nothing. Only `/healthz`, which says `{"status": "ok"}` and nothing else, and the console's page itself need no key.)

The console (`/`) has no sign-in yet, so against a server with authentication it shows nothing; use it in dev mode, or call the API.

## API keys and roles

```bash
# 1. Create a tenant_admin key BEFORE starting the server (authentication is the default, so there is no other way in)
python3 manage_api_keys.py create --tenant acme --role tenant_admin --label "root key"
#    -> copy the printed key

# 2. Create scoped keys for other roles as needed. A key's LABEL is the name recorded on every decision made with it (it may not begin with "system:")
python3 manage_api_keys.py create --tenant acme --role data_steward --label "Jane" --domains customer,product
python3 manage_api_keys.py create --tenant acme --role api_consumer --label "BI dashboard read-only"

# 3. Start the server
uvicorn app.api:app --port 8000

# 4. Every request needs the header
curl -H "Authorization: Bearer mdm_..." http://localhost:8000/v1/customer/entities
```

**Who made a decision is always the key, never the request.** A steward's confirm or reject, an audit verdict and an unmerge are recorded under the name of the authenticated key (`steward:<label>`); a `decided_by` sent in a request is ignored (and kept in the audit log as "claimed", for the record).

**A key limited to some domains sees only those:** its domain list, schemas, review queue, audits, and its slice of the audit log.

**Limits** (environment variables; the defaults stop a runaway client, not a normal one): `MDM_MAX_BATCH_RECORDS` (10,000 records per batch request), `MDM_MAX_RECORD_BYTES` (65,536 per record as JSON), `MDM_MAX_RECORD_FIELDS` (500), `MDM_MAX_BODY_BYTES` (32 MiB, judged from the announced Content-Length: a proxy in front must cap what announces nothing), and `MDM_RATE_LIMIT_PER_MINUTE` (per key, default 0 = off; it limits one process, so put the real limit at the proxy). Over the limit: 413 or 422, or 429 with `Retry-After`. A key without the permission gets **403** (it used to get an unhandled 500).

**Roles:**

| Role | Can do |
|---|---|
| `tenant_admin` | Everything: ingest, resolve queue, retrain, manage API keys, purge (right-to-be-forgotten), view audit log/usage |
| `domain_admin` | Ingest, resolve queue, retrain — within their `allowed_domains` only |
| `data_steward` | Ingest, resolve queue — within their `allowed_domains` only |
| `api_consumer` | Read-only — within their `allowed_domains` only |

`allowed_domains: null` (the default, and what `create --domains` omitted means) grants all domains.

```bash
python3 manage_api_keys.py list --tenant acme
python3 manage_api_keys.py revoke --tenant acme --key-id key_abc123
```

## Audit log

Every ingest, review-queue resolution, API key created/revoked, and purge is logged with a hash that chains to the previous entry — tampering with any historical row breaks every subsequent hash.

```bash
curl -H "Authorization: Bearer <tenant_admin key>" http://localhost:8000/v1/admin/audit-log
# -> { "chain_valid": true, "broke_at_entry_id": null, "entries": [...] }
```

`chain_valid: false` plus a `broke_at_entry_id` means someone modified a stored entry after the fact — worth investigating immediately, not something that should ever happen in normal operation.

**What "immutable" means here, precisely:** tamper-*evident*, not tamper-*proof*. Someone with direct database write access could forge a whole new consistent-looking chain from scratch. What this actually guarantees: a partial edit (changing one entry without rebuilding everything after it) is always detectable. That's the realistic guarantee a hash chain on a regular SQL database can make — genuinely useful, not a false claim of cryptographic tamper-proofing.

## Right-to-be-forgotten

```bash
curl -X DELETE -H "Authorization: Bearer <tenant_admin key>" \
  http://localhost:8000/v1/customer/entities/<entity_id>
```

Permanently deletes the golden record and every source record that contributed to it. The purge itself is audit-logged (the fact that data was erased is retained, even though the data isn't). **Documented limitation:** doesn't clean up dangling `source_record_id` references in *other* entities' lineage history that might mention a since-purged record — full graph-wide referential cleanup is beyond this MVP's scope.

## Usage metering (billing hooks)

```bash
curl -H "Authorization: Bearer <tenant_admin key>" http://localhost:8000/v1/admin/usage
# -> { "counts_by_event_type": {"record_ingested": 47}, "estimated_cost_usd": 0.094 }
```

Real tracking, illustrative per-unit prices. `LoggingBillingProvider` (the only implementation that runs here) records what *would* be charged rather than charging anything — swapping in a real Stripe integration is a documented one-class extension point in `app/billing.py`, not a rewrite. **Not yet metered:** LLM adjudication calls specifically (only `record_ingested` is metered at the API boundary right now).

## Connectors

```python
from app.connectors import CSVConnector, ingest_from_connector
from app.store_factory import get_store

store = get_store()
connector = CSVConnector(
    csv_path="customers.csv",
    field_mapping={"Email Address": "email", "First": "first_name", "Last": "last_name"},
    source_record_id_column="CustomerID",
)
results = ingest_from_connector(store, "acme", "customer", connector)
```

Salesforce/Shopify connectors are scaffolds in the same file (`SalesforceConnectorStub`, `ShopifyConnectorStub`) that raise `NotImplementedError` with a clear explanation — building a real one means working against a live account's actual API, which isn't something to fake having validated.

## Bulk ingestion from files

```bash
# CSV
python3 bulk_ingest.py --tenant acme --domain customer --file customers.csv \
  --mapping "first_name=First Name,last_name=Last Name,email=Email" \
  --id-column CustomerID

# JSON (nested paths supported, e.g. "contact.email")
python3 bulk_ingest.py --tenant acme --domain vendor --file vendors.json \
  --mapping-file vendor_mapping.json --id-column vendor_id
```

`--mapping` is inline (`canonical=source_column,...`); `--mapping-file` points
to a JSON file for many fields or nested JSON paths. Prints progress every 50
records and a final summary (auto-merged / queued for review / standalone).

Built on `app/connectors.py`'s `CSVConnector` and `JSONConnector` — both real,
tested (`tests/test_phase3.py`), zero external dependencies.

### Performance: indexed candidate lookup (fixed a real O(n²) problem)

Earlier versions of this pipeline re-fetched and re-standardized **every
existing record** for a (tenant, domain) on every single ingest call —
fine for hundreds of records, but measured degrading badly at scale: 200
records dropped from ~136/sec to ~88/sec, and a real user report showed
200 records taking 378 seconds against Postgres (compounded by a since-
fixed connection-reuse bug — see the Postgres section below).

**Fixed**: `app/pipeline.py`'s `ingest_record()` now computes the new
record's blocking keys once, persists them to an indexed `blocking_keys`
table (`(tenant_id, domain, blocking_key)`), and queries that index
directly for candidates — instead of scanning the whole dataset. Measured
in the sandbox, 2000 records with realistic name/city diversity:

| Records | Rate |
|---|---|
| 1–200 | 133/sec |
| 401–600 | 178/sec |
| 1001–1200 | 133/sec |
| 1801–2000 | 86/sec |

Total: 2000 records in 15.9s — a real, measured fix, not a claim.

**Honest caveat, also measured:** this isn't magic — with *low-diversity*
data (a small, repeated pool of names/cities, so blocking keys collide
constantly and candidate sets themselves stay large), throughput still
degrades meaningfully (108/sec → 14/sec over 2000 records in that
adversarial test). That's inherent to blocking-based entity resolution,
not a remaining bug in the index: when a huge fraction of your dataset
genuinely shares the same blocking key, no index changes how many rows
match that key. Real-world data (more distinct names/addresses) looks
like the first table, not the second.

## The data layers: raw, staging, golden

```
source_records        RAW        the CURRENT payload of each source record, exactly as sent: one row per (tenant, domain, source system, record id)
source_record_history            every payload a change has since superseded (append-only)
      |  standardize (once, at ingest)
standardized_records  STAGING    per record, two forms derived from raw: MATCHING (robert, 5125550123) and CLEANSED (Robert, (512) 555-0123)
      |  block, score, merge                          |  survivorship over the CLEANSED forms
blocking_keys, match_candidates                       entity_<domain>   GOLDEN: one table per domain, one real column per field
```

**Golden records are derived from cleansed data, never from raw.** Raw is untrusted and unformatted: whitespace, shouting, placeholders. A golden record built from it is only as clean as whichever source happened to win, which is not mastered data. (An earlier version did exactly that; a golden record could hold `'  bob '` and `' Bob@X.COM '`.) Now `first_name: "  bob "` becomes `Bob`, `SMITH Jr.` becomes `Smith` plus `name_suffix: Jr`, phones become `(512) 555-0123`, ZIP+4 becomes `94107-1234`, and a placeholder (`none@none.com`, `000-000-0000`) is empty in both forms so it can never reach a golden record. Accents (`José`) and deliberate casing (`McDonald`) survive: only all-upper or all-lower input is re-cased.

**Golden records are real tables with real columns.** Each domain's golden records live in their own table, `entity_customer`, `entity_product`, `entity_vendor`, `entity_<yours>`, with the system columns (`entity_id`, `tenant_id`, `status`, `created_at`, `updated_at`) and **one column per field**. A report reads `SELECT email, phone FROM entity_customer WHERE status = 'active'` with no JSON functions; BI tools see ordinary columns. `python3 domains_cli.py schema customer` shows the table. It is one table per domain, not one shared table, because domains have different fields: a shared table would be wide, mostly NULL, and altered whenever any domain changed. `status` is `active` or `merged`; a merged row is a tombstone that keeps its values (rebuilds leave it alone).

**What that costs.** A domain used to need no database change; now it needs a table, and a new field needs a column. That DDL runs only where it is safe: migrations create the built-in tables; **`domains_cli.py reindex <domain>` creates or extends a custom domain's table** and rebuilds its golden records; and the app itself does it lazily only when auto-migration is on. With `MDM_AUTO_MIGRATE=false` (the least-privilege setup) the app only *verifies* and refuses with the command to run. Columns are **only ever added**: a field removed from a config keeps its column (dropping it would destroy data). Columns are typed by each field's `type` (default `text`; see *Typed golden columns*). Field names must be valid column names (63 characters, lowercase, and not `entity_id`/`tenant_id`/`status`/`created_at`/`updated_at`/`domain`/`id`); the validator rejects the rest. After adding a custom domain, re-run `setup_rls.py enable` so its table gets a row-level-security policy.

**Raw ingest is idempotent.** A source record is identified by `(tenant, domain, source_system, source_record_id)`. Sending the same record again is **a no-op if it finished** (the result says `unchanged`; if it never finished it is **resumed**, see *From raw to golden*; re-running a file is safe; the API does not meter it; `bulk_ingest.py` reports "Already held, unchanged: N"). Sending a **changed** payload updates the row **in place**: same record id, same entity, `version` + 1, and the payload it replaced goes to `source_record_history`, so raw history is never lost. Everything derived from it is refreshed: staging, blocking keys, the golden record (recency follows the update time, not first arrival), and any review item that was scored on the old data is marked `superseded`. A change can **add** merges (correcting a typo'd email can now find the match it missed) but it **never splits** an entity by itself: only a person's unmerge does (see "Unmerge and the audit page"). Records already in the same entity are not compared with the changed one. The purge deletes the history too. Databases from before this carry no hash: it is computed on demand, so no backfill is needed; and **old duplicates are reported, not deleted** (`domains_cli.py status` shows a DUPES column; the newest copy counts as current). The natural-key index is deliberately not unique, because a migration should not decide which of two existing copies to throw away.

**Typed golden columns.** A field declares its type in the same domain config document (the shipped `domain_templates/*.json` are exported from it, so `product.json` carries `"price": {"type": "numeric"}`):

```json
"price":     {"normalize": "text", "type": "numeric"},
"listed_on": {"normalize": "text", "type": "date", "format": ["%m/%d/%Y", "%Y-%m-%d"]},
"active":    {"normalize": "text", "type": "boolean"}
```

Types are `text` (default), `integer`, `numeric`, `boolean`, `date`, `timestamp`. The golden column then has that real type, so a report can `SELECT SUM(price)` or filter `WHERE price > 1000` with no casting. **Staging stays text**; the cast happens at the golden layer, **before survivorship**, so a newer `"N/A"` cannot beat an older valid number. A value that cannot be cast becomes `NULL` (never a wrong value), the record is kept, and the problem is listed in the golden row's `quality_issues`. The rules are strict: **dates are never guessed** (03/04/2026 is March 4th or April 3rd, so say which with `format`; with several patterns the first match wins); integers accept `1,200` and `12.0` but not `12.5`; numerics accept currency symbols and thousands commas but **not a decimal comma**; booleans accept true/false, t/f, yes/no, y/n, 1/0; timestamps are stored in UTC and a value with no timezone is taken to be UTC. `0` and `false` are values, not missing.

**Changing a field's type later** rebuilds that table by copy-and-swap in one transaction (SQLite cannot alter a type in place, and a plain Postgres `ALTER` fails the whole statement on one bad value). Existing values are re-cast; one that cannot be becomes `NULL`; then `reindex` re-derives every golden value from staging under the new type, which recovers anything the swap could not parse. With auto-migration off, the app only verifies and refuses with the command to run. **SQLite has no exact decimal type**: `numeric` is stored with SQLite's NUMERIC affinity (a number, not guaranteed exact); use Postgres where exactness matters, such as money.

**Two standardized forms, on purpose.** Matching wants `robert` and `5125550123`; a report wants `Robert` and `(512) 555-0123`. Both are stored. Each field's presentation is its `present` step in the domain config (`python3 domains_cli.py normalizers` lists both vocabularies); unspecified fields are trimmed and whitespace-collapsed. Company names are deliberately kept as typed (`LLC`, `IBM` can't be re-cased safely).

**Everything above raw is a function of (raw, config) and can be rebuilt.** `python3 domains_cli.py reindex <domain>` re-standardizes every raw record into staging, rebuilds the blocking keys from it, and re-derives every golden record (one lineage row per field that changed). The stale-index guard now covers all of it: a change to how **any** field is standardized or presented, to blocking, or to survivorship pauses ingestion until the rebuild runs. Scoring settings (weights, penalties, thresholds, deterministic fields) are not derived data and take effect on restart.

**Effects:** matching reads candidates from staging (one query, no re-standardizing: an ingest with 27 candidates used to make 28 `standardize()` calls and now makes none), staging is written in bulk by `ingest_batch`, and the right-to-be-forgotten purge deletes the staged copy too. On the held-out benchmark matching results are **identical** with and without staging, and throughput rose from 48.4 to 55.7 records/second (single run; directional).

**Not built:** typed *staging* (by choice, staging is text); a decimal-comma locale; a data-quality *score* on records beyond the per-value `quality_issues`.

**Upgrading (migrations 010 and 011):** 010 adds `content_hash`, `version` and `updated_at` to `source_records`, a non-unique natural-key index, and `source_record_history`; existing rows are untouched. 011 adds `quality_issues` to every entity table and retypes `entity_product.price` from text to numeric by copy-and-swap: a price that cannot be parsed becomes NULL, so run `python3 domains_cli.py reindex product` afterwards to recover any such values from staging. Neither migration pauses ingestion.

**Upgrading to real-column golden tables (migration 009):** existing golden records are moved into `entity_<domain>` tables with their ids, tenant, `status` (merged tombstones included), timestamps and values intact, so `source_records.entity_id` and lineage keep pointing at the same entities. Columns come from the loaded config, or from the keys actually in the data for a domain whose config isn't loaded (`migrate.py` loads none), so nothing is lost. Rows are counted afterwards and the migration rolls back if any are missing. The old table is **renamed `entities_json_legacy`, not dropped**: check your data, then drop it yourself. A `reindex` re-derives golden records from raw anyway.

**Upgrading (migration 008):** migration 008 adds the staging table and marks every domain that already holds records as needing a rebuild, so ingestion into **customer, product and vendor** pauses until you run `python3 domains_cli.py reindex <domain>`. That rewrites golden records from cleansed values, so expect a burst of lineage rows (whitespace, casing and formatting fixes).

## From raw to golden: what runs it, and realtime vs batch

There is no workflow engine, queue, or scheduler in this project. The path from raw to golden is a set of **idempotent steps** that each leave durable state, so any scheduler you already run (cron, Airflow, Dagster, Prefect) can drive them, and a crash at any point is recoverable.

**Every source record carries how far it got:** `landed` (raw saved) → `staged` (standardized and in the blocking index) → `matched` (the matching decision is made and recorded: merged, queued for review, or a new entity). `matched` is the **only** finished state. A record that is not `matched` is **resumed** when it is sent again, or by `process-pending`, and is never mistaken for a finished duplicate. (An earlier version had exactly this bug: after a crash, re-sending a record that never finished was answered "unchanged" and nothing completed it.) Resuming is safe to repeat: staging and keys are replaced, and a pair already decided is not decided twice, so it can't duplicate candidates or merges. If a step fails, the error and an attempt count are written on the record; after `max_attempts` (default 5) `process-pending` leaves it alone and `status` shows it as failing, because it needs a person, not another retry.

**Realtime and batch are treated differently, on purpose.** Pick with `?mode=` on the API, or by which call you use:

| | realtime | batch |
|---|---|---|
| Calls | `POST /v1/{domain}/records` (default), `ingest_record()` | `POST /v1/{domain}/records/batch`, `bulk_ingest.py`, `ingest_batch()`/`run_batch()`; or `?mode=batch` on one record |
| Matching | inline: done before the call returns | inline in one run, **or accepted now (HTTP 202) and matched later** by `process-pending` (`?mode=batch`, `"defer": true`, `--defer`) |
| Golden record | rebuilt right after each merge, so the caller sees it | rebuilt **once per affected entity at the end** of the run (6 records merging into one entity: 1 rebuild, not 6) |
| Errors | raised to the caller; the record stays resumable | collected per record and the run **continues**; `max_errors` aborts the run once exceeded and the rest wait for `process-pending` |
| Logging | one audit entry per request, metered per record | **one row in `pipeline_runs`** with counts (new / unchanged / updated / resumed / deferred / errors / merged / queued / standalone), **one audit entry** for the run, metered as a total |
| LLM adjudication | inline | **off** (ambiguous pairs go to review) unless `MDM_LLM_IN_BATCH=true`: thousands of slow calls inline is a bad default |

**What does not differ:** the matching itself, and its order. A batch is *not* matched in parallel or out of order: a record must never see the blocking keys of records that arrive after it. On the held-out benchmark matching results are identical to one-record-at-a-time, and a test checks that a deferred batch produces the same entities as realtime. A `mode=batch` single record is deliberately **not indexed** until it is processed, so a later realtime record cannot match it early; it matches in arrival order when `process-pending` reaches it.

**The run log is the arrival log.** Re-sending a file shows up as `unchanged=N, new=0` in `domains_cli.py runs`, which is how you answer "did the source send this again on Tuesday?" without storing every duplicate row. Realtime requests are not logged there (one row per API call would swamp it); the audit log has an entry per request, and now says whether it was `unchanged`, `updated` or `resumed`.

**Deferred golden rebuilds are crash-safe.** "This entity's golden record is stale" is written to `golden_dirty` before the run can lose it, and `process-pending` clears leftovers from a run that died. Progress (`matched` marks and dirty marks) is written in groups of 100 instead of one commit per record: a crash between groups leaves those records `staged`, they are matched again, and a test proves the repair gives the same candidates, entities and keys as a clean run. Grouping is what keeps the cost down: per-record commits cost 19% throughput on the benchmark (58.6 → 47.6 records/s); grouped, 56.4.

```bash
python3 domains_cli.py status                       # PENDING and FAILING columns; exit 1 if anything is failing
python3 domains_cli.py process-pending customer     # finish accepted / interrupted / failed records; safe to run on a schedule
python3 domains_cli.py runs customer                # what each batch run, process-pending pass and reindex received
python3 bulk_ingest.py ... --max-errors 50 --defer  # error budget; accept now, match later
# cron, every 5 minutes:   */5 * * * *  cd /app && python3 domains_cli.py process-pending customer
```

**Not built, said plainly:** a scheduler or a long-running worker (something has to call `process-pending`); per-record detail in the run log (counts and the first 20 errors only); parallel matching (order matters); prioritising realtime over a running batch on the same domain (they interleave, and each is correct, but a realtime request can wait on a long batch's writes); and `mode=batch` is for NEW records, one we already hold is handled as in realtime. The API handlers were exercised only under a stubbed FastAPI, and the Postgres SQL for all of this has never run on a live server.

**Upgrading (migration 012):** adds `processing_status`, `processing_attempts`, `processing_error` to `source_records`, the `pipeline_runs` and `golden_dirty` tables, indexes on the candidate record columns, and a small index over only the unfinished rows. Existing records count as finished, **except** records the previous release could strand (no golden record and no match candidate): those are marked unfinished. Run `python3 domains_cli.py process-pending <domain>` once after upgrading to complete them.

## One entity per record, and how every engine is checked

**Every record belongs to exactly one entity.** A record whose only matches are waiting for a steward is still its own entity (and has a golden record) until one is confirmed: confirming unions the two entities, rejecting changes nothing. Before, such a record had *no* entity at all: 28% of benchmark records were missing from the golden layer, rejecting never gave them one, and **which** record of a review pair was missing depended on arrival order.

**The oldest entity survives a merge**, whichever record is named first. Downstream systems hold entity ids, so the established id must keep working; letting the newcomer's brand-new entity win (which "the first record's entity survives" did once every record had its own) would retire the id everyone already uses on every steward confirmation. The loser stays as a `merged` tombstone, as before.

**Matching is a function of pairs, not of order.** Whether two records link depends only on those two records' standardized values, the config and the model. So the right answer for any set of records can be computed with no database and no ordering (`app/offline.py`): score every pair that shares a usable blocking key, union the auto-merge pairs into entities, keep the review pairs. `benchmark.py equivalence` runs the real engines (realtime, batch, accept-then-process, bulk) in several arrival orders and compares each with that reference:

```bash
python3 benchmark.py equivalence --profile customer --scale 4 --orders 3
python3 benchmark.py equivalence --profile person --variant full --scale 3
```

On both benchmark profiles, every engine in every order gives the same entities and the same review pairs as the reference, with no record left without an entity. **This is the test a faster engine (multi-process, DuckDB, Spark) must pass before it is trusted**; it can also fail (a test uses an engine that never merges). One known, reported difference: a blocking key crossing `max_bucket_size` *during* a load is judged by its size at that moment by the engines and by its final size by the reference, so the check says "not comparable" when that happens instead of giving a verdict. It does not cover the learned model or LLM adjudication.

**Cost, measured.** Matching quality on the held-out customer benchmark is identical. Creating the extra entities cost about 20% of throughput at first; profiling showed 64% of that was one store call, `add_lineage` (one commit per field per golden rebuild, already the most expensive call in the engine). Lineage is now written in one transaction per rebuild (`add_lineage_batch`, same events, same order), and back to back on the same machine the new engine runs **20% faster than the old one** (53.1 vs 44.3 records/s).

**Upgrading (migration 013):** it marks every entity-less record unfinished, and `python3 domains_cli.py process-pending <domain>` then gives each its own entity (their review items stay pending; a rejected review gets an entity too). `process-pending` handles `--limit` records per pass and says how many remain, so on a large database run it until it reports nothing left.

## SSN, date of birth, and scale: the `person` benchmark

`domain_templates/person.json` is a `person` domain with SSN and date of birth. Its SSN and DOB fields are cleansed (next section); `--variant full:baseline` (any variant with `:baseline`) turns the cleansing off, so the benchmark can show what placeholders cost. The scenario results below are that **baseline**. It lists twelve candidate blocking keys, and `--variant` picks which are live:

| variant | keys |
|---|---|
| `customer_like` | what the customer domain uses today: no SSN, no DOB, phone windows |
| `ssn_no_phone` | exact SSN + name/address keys, **no phone keys** |
| `dob_phone_no_ssn` | name + **DOB** + address + exact phone + email, **no SSN key** |
| `dob_phone_windows_no_ssn` | the same plus 4-digit phone windows |
| `scalable` | the first step-4 set: selective keys for everyone and the loose name keys only for sparse records, with 3-character-prefix keys that grow almost linearly |
| `scalable_tight` | `scalable` with `whole surname + zip5` and `soundex(last) + first initial + full DOB` in place of the prefix keys: flat growth, lower recall |
| `scalable_tight_plus` | `scalable_tight` plus `whole first name + zip5` and `whole surname + whole first name`, which win back the lost recall; **what `person.json` ships** |
| `full` | every candidate key |

```bash
python3 benchmark.py run --profile person --variant ssn_no_phone --seeds 1 2 3 --scale 25   # scenarios through the real pipeline
python3 benchmark.py scale --sizes 10000 100000 1000000 --out benchmarks/scale_person.json    # blocking at scale: no database, ~4 min for 1M on one core
python3 benchmark.py scale --sizes 10000000 --extend benchmarks/scale_person.json --workers 16 # add a size later without redoing the others
python3 benchmark.py scale --sizes 1000000 --extend R.json --checkpoint ck.pkl --budget 200      # pause after ~200 s, keep progress; run again to continue (exit code 3 = paused)
```

**Two different instruments, and they answer different questions.** The *scenario* run measures **scoring**: labelled cases through the real pipeline. Every blocking variant gave identical decisions there (79.2% auto precision, 42.9% recall over 2,850 records), because every positive case shares at least one key in every variant; so it cannot tell blocking designs apart. The *scale* run measures **blocking**: for a sample of records, how many distinct other records share a usable key (a record's "partners"; pairs to score per record is half), and what share of sampled duplicates still share a key (blocking recall). It is checked against a brute-force computation of the same quantity.

*Scale, 2,000 records sampled per size, 8% duplicates, SSN present 70% / DOB 90% / phone 80% / email 60% (all configurable):*

| pairs to score per record | 10k | 100k | 1M | blocking recall at 1M | growth exponent |
|---|---|---|---|---|---|
| `customer_like` (today's keys) | 15.8 | 161 | 280 | 99.7% | 1.01, then 0.24 *(keys dying, see below)* |
| `ssn_no_phone` | 0.4 | 3.3 | 23 | 99.5% | 0.92, 0.85 |
| `dob_phone_no_ssn` | 0.1 | 0.5 | 4.6 | 98.4% | 0.56, 0.92 |

* **Phone windows are the culprit.** Alone they contribute 514 of ~560 partners at 1M. With only 10,000 possible 4-digit values, their buckets outgrow the cap near 700k records, and at 1M **28-38% of keys are over the cap** and silently stop working: the exponent "improves" to 0.24 because the key is being thrown away, not because blocking got better. The cap is a cliff, not a dial, so *keys over cap* is reported as a health metric.
* **Exact SSN and DOB combinations are nearly free**: exact SSN 0.1 partners at 1M, `dob + soundex(last name)` 0.1, `soundex(last) + first + birth year` 0.2. The cost is in **name-only keys** (`last+first` 28, `soundex+first` 11, `first+zip` 7 partners at 1M).
* **Both of your ideas beat today's keys by a wide margin**; the DOB variant generates the fewest pairs and loses about a point of recall at 1M (98.4% vs 99.5%).
* **Neither is enough at 100M.** The best variants still grow with an exponent of 0.85-0.92 (close to linear). If that held to 100M, partners per record would be in the hundreds to thousands, far above the ~25 pairs per record that fits a 12-hour run. That extrapolation is crude. The design this points to: exact SSN, DOB combinations, exact phone and email, with name-only keys used **only for records that lack those fields**. That needs conditional key groups, which do not exist yet.

*What the scenarios showed about scoring (identical in every variant):*

| | result |
|---|---|
| **DOB placeholder** (two different people, same common name and town, both with a defaulted DOB) | **75% wrongly merged**: the defaulted DOB "agrees" exactly. This one scenario is 56 of the run's 59 false-merged pairs (3 more are twins). |
| twins (same DOB and address, different first names) | 4% wrongly merged, 96% to review |
| junk SSN shared by strangers | 0% merged: the mismatch penalties already neutralise it. But same-name strangers sharing a junk SSN all go to **review** (noise for stewards) |
| same SSN, different people (data-entry error) | 0% merged |
| exact SSN, everything else different (new address, phone, email, nickname) | 100% auto-linked |
| **SSN with one wrong digit**, everything else identical | 0% auto, 100% review |
| DOB day/month swapped; year-only DOB; same name+DOB after a move | 0% auto, 100% review |

The last two rows are scoring gaps, not blocking ones: a DOB difference is penalised as an exact mismatch even when it is a swap or a partial date, and one mistyped SSN digit outweighs a perfect name, address, phone and email match. Both are candidates for a date-aware comparator and for treating SSN as strong-but-fallible evidence.

**What this does not tell you.** The data is synthetic: names, zips and streets follow long-tailed distributions with a real head, but real surnames cluster phonetically, phone numbers and zips are not uniform, and real duplicates are messier. Use it to compare designs and to see growth, not as a forecast. It needs a sample of your own data (even 1M anonymized rows) before any absolute number is trusted. A single core and no real Postgres. (SSN protection has its own section; the benchmark runs with a throwaway key because its data is synthetic.)

## Cleansing SSN and date of birth, and profiling a new source

An SSN or a date of birth is strong evidence, so a **placeholder** is worse than a missing value: it makes strangers "agree". The `ssn` and `dob` normalizer steps (with `present_ssn` / `present_date`) turn implausible values into *missing*, in both the matching form and the golden record, and record why.

* **SSN** is nine digits or missing. Rejected: not 9 digits (a short number is never padded: a spreadsheet that dropped a leading zero is not guessed at), area 000 / 666 / 9xx, group 00, serial 0000, one digit repeated, and the widely published examples (123-45-6789, 078-05-1120, ...). ITINs (area 9xx) are rejected unless the field sets `"options": {"itin": "allow"}`.
* **DOB** is read as ISO plus **only the formats the config declares** in `"parse"` (`03/07/1985` is March 7th or July 3rd because the config says so, never because the code guessed). Rejected: not a real calendar date, the usual system defaults (1900-01-01, 1970-01-01, 0001-01-01, 9999-12-31, ...), in the future, older than `max_age_years` (default 120). A bare year (`1985`) is kept for **matching** but is not a golden date. Extra defaults go in `"options": {"defaults": ["1960-06-15"]}`.
* **Recorded without the value.** Each discard becomes a `quality_issues` entry on the golden record (`{"field": "ssn", "type": "cleansing", "problem": "area 000", "value": "[withheld]"}`): the reason is enough to act on, and an SSN must not end up in an entity's quality issues. Raw is never rewritten, so changing a rule and running `reindex` re-applies it to everything already loaded (a rule change is part of the stale-index fingerprint, so ingestion pauses until you do).

```json
"ssn": {"normalize": ["ssn"], "present": ["present_ssn"], "options": {"itin": "reject"}},
"dob": {"normalize": ["dob"], "present": ["present_date"], "type": "date", "format": "%Y-%m-%d",
        "parse": ["%m/%d/%Y", "%m-%d-%Y"], "options": {"max_age_years": 120}}
```

**What it did, measured** (functional comparison: the same 2,850 labelled records and the same engine, `--variant full:baseline` vs `full`; `benchmarks/person_full_baseline.json` and `person_full_cleansed.json`):

| | before | after |
|---|---|---|
| strangers, same common name and town, **defaulted DOB**: wrongly auto-merged | 56 of 75 | **11 of 75** (all `2000-01-01`, see below) |
| strangers, same name, sharing a **junk SSN**: sent to review | 75 of 75 | **0 of 75** |
| one-digit SSN typo: auto-linked | 0 of 75 | 3 of 75 (a typo that lands on an impossible SSN is now discarded instead of counting as a contradiction) |
| exact-SSN, nickname and name+DOB+address matches | all linked | all still linked |
| false-merged pairs / auto-link precision / recall | 59 / 79.2% / 42.9% | **14 / 94.2% / 43.4%** |
| review queue | 602 pairs, 50% true duplicates | 761 pairs, 39% true duplicates |

**The cost, plainly:** wrong auto-merges did not vanish, they moved into review, so stewards see about 160 more pairs and a smaller share of them are true duplicates. That is the right place for them, but it is work.

**What remains is the point of profiling.** All 11 leftover false merges use `2000-01-01`, which is **not** a built-in default: a real person can be born that day, and a false positive silently discards real data. The other three false merges are twins. A source that stamps 2000-01-01 on every record nobody filled in looks valid to every rule. It is recognisable by its behaviour: one value shared by many records whose **names disagree**. That is what `domains_cli.py profile` finds:

```bash
python3 domains_cli.py profile person --file new_source.csv --mapping "first_name=First,last_name=Last,ssn=SSN,dob=DOB"   # BEFORE loading a new source
python3 domains_cli.py profile person --tenant acme --source-system crm                                                   # or on loaded data
python3 domains_cli.py profile person --file x.csv --mapping "..." --reveal --fail-on-suspects                            # print a config snippet; exit 1 if anything is found
```

It reports what the rules rejected (by reason), the share of dates on January 1st (real data: about 0.3%), year-only dates, and **suspect values**: shared by at least `max(min_count, 10 x the average count per distinct value)` records (so a date every large file shares many times is not flagged), with at least `min_names` different names making up at least half of a sample of those records (so one person repeated is not flagged). Nothing is changed: a person reviews the suspects and pastes the suggested `"anonymous"` list into the field's config, after which it is an ordinary, deterministic, versioned rule (a test does exactly this loop: profile, apply the snippet, and the 11 false merges go to 0). SSNs are masked in the report unless `--reveal`; only values shared by many different people are ever listed. Memory is bounded (`approximate` is reported when once-seen values had to be dropped).

**Privacy of the SSN is handled in the next section** (keyed hash, mask, plain only in raw). DOB is not hashed. Not covered here: one DOB parse convention per domain (a source that sends day-first dates needs its own domain or a re-formatted file), and partial dates beyond a bare year.

*Tested with functional and unit tests only (rules, config, golden records, before/after matching, the profile-then-fix loop, the commands); no performance or scale tests were run for this step.*

## Protecting the SSN: a keyed hash for matching, a mask for display, the plain value only in raw

An SSN has about a billion possible values, so an unkeyed hash can be reversed in minutes. The SSN field is protected with `"protect": "hash"`: the system can still tell whether two records carry the same SSN, but the number itself is kept out of every layer except **raw**.

| where | what it holds |
|---|---|
| **raw** (`source_records`, and superseded payloads in `source_record_history`) | exactly what the source sent, **plain**. This is a deliberate choice: raw is what lets `reindex` re-apply a changed rule and re-hash under a new key |
| staging, matching form | a keyed hash: `v1:3f9a...` (equal SSNs give equal hashes, so exact matching works however the SSN was written) |
| staging, cleansed form; golden record; lineage | the mask: `***-**-6789` |
| blocking index | the hash |
| match reasons; quality issues | "exact ssn match" with no value; "[withheld]" |
| API: the raw attributes of a record (entity detail, both sides of a review item) | masked on the way out |

**A test scans the whole database** after ingesting records and a merge: the plain SSN, in any of the formats it was sent in, appears in `source_records` and **nowhere else**; the hash appears only in staging and the blocking index.

**The key.** `MDM_TOKEN_KEY` (at least 32 characters) is read from the environment, which is where your secrets store should put it. It must **not** live in the database, or whoever steals the database steals the key. Generate one with `python3 -c "import secrets; print(secrets.token_urlsafe(48))"`. `MDM_TOKEN_KEY_ID` (default `v1`) is stored in front of every hash. The benchmark sets a throwaway key itself because its data is synthetic; nothing else has a default.

**It fails closed.** A protected field with no valid key stops ingestion with a message that says what to set. It never stores the value unprotected instead: the raw record is kept, and `process-pending` finishes it once the key is there. The API refuses to start, and `bulk_ingest.py` refuses to read a file, without the key. The profiler does not need it (it is read-only and stores nothing).

**Rotating the key.** Set a new `MDM_TOKEN_KEY` **and** a new `MDM_TOKEN_KEY_ID` (`v2`). The key id is part of the derived-data fingerprint, so ingestion stops (the stale-index guard) until `python3 domains_cli.py reindex person` re-hashes everything from raw; afterwards a new record still finds the old ones. Changing the key without changing the id leaves old hashes unmatchable, so always change both. This is possible *because* raw keeps the plain value.

**Turning protection on for data that already exists.** Add `"protect": "hash"` to the field, set the key, and `reindex`. Besides re-deriving staging, the index and golden records, reindex **masks what older rows still hold in plain text**: lineage values, the golden column of entities that were merged away (reindex does not rebuild those), and the text of old match reasons. It is idempotent.

**What you give up.** An SSN can only be compared for equality (config validation refuses a hashed field in `fuzzy_weights`, the semantic signal, or a blocking key that is not `exact`), so a one-digit typo can never be recognised as "nearly equal". The golden record holds the mask, so a consumer that needs the full SSN cannot get it from golden; it is in raw. One key serves the deployment: tenants are separated by `tenant_id` everywhere, but the same SSN gives the same hash in two tenants.

**What it does not cover.** The plain SSN is in raw and in superseded payloads, so those need database encryption and tight access. Old backups and database log files from before protection was switched on still contain plain values. **DOB is not hashed**: the matcher needs the real date for fuzzy comparison and the birth-year blocking key. The Postgres version of the scrub is written to the same shape but has never run on a live server.

*Tested with functional and unit tests only (30 tests, checked by breaking it 13 ways); no performance tests were run.*

## Scalable blocking (step 4): four mechanisms, and what was measured

Blocking decides which pairs get scored, so it decides what 100M records cost. Today's keys cannot scale: 4-digit phone windows alone gave 514 of ~560 partners per record at 1M and stop working past ~700k records, and the name keys grow with the data. Four mechanisms, all configured in the domain file:

* **`transpose` key kind:** `{"field": "phone", "as": "transpose"}` keys a value and every variant with two adjacent characters swapped (at most one extra key per character). It catches the swapped digits that windows were added for, but each key is nearly unique, so its bucket stays tiny however much data there is (a test: among 6,000 random numbers no bucket exceeds 3).
* **Conditional groups:** `{"fields": ["last_name", "first_name"], "unless_any": ["ssn", "dob", "email", "phone"]}` generates the group's keys **only for records that have none of those fields** (a junk SSN counts as none, because cleansing already made it missing). The loose name-only keys are kept for the records that have nothing better and removed from everyone else's buckets. **It is symmetric on purpose:** a sparse and a rich record simply never share a conditional key, whichever arrives first, so pair discovery cannot depend on arrival order (a test runs every engine in several orders against the reference matcher with these keys). The condition is part of the derived-data fingerprint.
* **Date-aware DOB comparison:** `"dob": {"penalty": 0.3, "compare": "date"}` (the field must be typed `date`). Equal agrees; a **swapped day and month**, or a **year one out** with the same day and month, *nearly* agrees (10% of the penalty); a **bare year** agrees with any date in that year and disagrees with another; anything else disagrees. The same comparison feeds the fuzzy score. Before, a swapped or year-only DOB scored as a hard mismatch and every such pair went to review.
* **Near-equal SSN, scoring only:** `"options": {"near": true}` on a protected SSN stores, in the matching form only, short keyed hashes (10 hex characters, up to 18 per record) of the SSN, of each one-digit-blanked variant and of each adjacent-swapped variant. Two SSNs one typo apart share a token, so the pair is a *near* match (10% of the penalty) instead of a contradiction. Nothing readable is stored (a whole-database test checks the tokens appear in staging only and the plain SSN in raw only), the tokens are **never** a blocking key, and they only reduce a penalty: a near SSN alone never creates a match. Cost: about 200 characters of staging per record that has an SSN. Requires `"protect": "hash"`.

**`domain_templates/person.json` ships the `scalable_tight_plus` key set:** SSN, email, phone with adjacent swaps, `dob + soundex(last)`, `soundex(last) + first + birth year`, `soundex(last) + first initial + full DOB`, `whole surname + zip5`, `whole first name + zip5`, `whole surname + whole first name`, plus the three loose name keys as conditional groups. **An existing `person` domain needs a `reindex`** (the keys changed).

**Measured at 1,000,000 records** (synthetic data, 2,000 records sampled per size, 775 sampled duplicates; `benchmarks/scale_person_step4.json` and `scale_person_step4b.json`):

| key set | pairs to score per record | growth exponent (100k to 1M) | blocking recall |
|---|---|---|---|
| today's keys (phone windows, name keys) | 280 | 0.24, because 38% of its keys hit the cap and silently die | 99.7% |
| `scalable`: first step-4 set, with 3-character-prefix keys | 4.2 | 0.92 | 99.4% |
| `scalable_tight`: whole surname + zip5 and soundex + first initial + full DOB instead | 0.2 | 0.3 | 99.0% |
| **`scalable_tight_plus` (shipped)** | **1.5** | 0.88 | **99.9%** |

**How the shipped set was found.** The prefix keys in `scalable` grew almost linearly (`last+zip`, `soundex+zip3`, `dob+first initial`); replacing them (`scalable_tight`) made growth flat but cost recall. A larger sample (4,000 duplicates from the 1M population) showed recall of 99.75% for `scalable` and 99.48% for `scalable_tight`, and that every one of the 21 duplicates the tight set misses has the same shape: **no identifier survives** (email, phone and SSN all missing or different) and either the surname is mistyped or the person moved, with the DOB also off. The two whole-name keys (`whole first name + zip5`, `whole surname + whole first name`) recover 20 of those 21: **99.98%** on the same sample (the 775-duplicate sample in the table reads about 0.3 points lower, which is sampling noise). On the labelled scenarios every pair the full key set finds is also found by the shipped set; a swapped DOB, a year-only DOB and a one-digit SSN typo now auto-link (they all went to review before); Jr/Sr, shared real SSN, junk SSN and same-name/different-DOB still produce no merges.

**The gate** was about 25 pairs per record at 10M records with recall no lower than today. **Met at 1M** (recall 99.9% against 99.7%; 1.5 pairs per record). **At 10M it is extrapolated, not measured:** the 0.88 exponent gives about 11 pairs per record. **At 100M the same exponent gives about 86 pairs per record, above the 25 that fits a 12-hour run**, so 100M is not yet shown to fit. The part still growing is `whole surname + whole first name` (2.3 partners at 1M, roughly linear; its commonest names exceed the per-key cap and are then found only through other keys). 10M was not run: at this machine's speed it is about an hour on one core (`--workers` on a larger machine).

**A long measurement can now be paused and resumed.** `benchmark.py scale ... --checkpoint FILE --budget SECONDS` stops cleanly after the budget (exit code 3, "PAUSED"), keeps its progress in the checkpoint, and running the same command again continues; a test proves a measurement paused after every chunk gives numbers identical to an uninterrupted one. (The 1M runs above took two calls of about 200 seconds each this way.)

**Risks to know about.** A sparse record (no SSN, DOB, email or phone) and a rich record of the same person find each other only through the unconditional keys; what that costs is included in the recall figures. `transpose` writes up to 8 to 10 key rows per phone number (nearly unique ones). The date comparison reads one DOB convention per domain.

*Tested with functional and unit tests (28 for the mechanisms and key sets, checked by breaking the four mechanisms 13 ways; 2 for the resumable harness). The scale harness is a count of pairs, not a timing; no timing measurements were made.*

## The bulk pair-based engine: load a whole source in stages

The one-record-at-a-time engine spends 83% of its time on the database (about 15 commits per record) and under 4% on matching. Matching is a function of **pairs**, so a whole source can be resolved set-wise instead. `app/bulk.py` does that for a **whole domain into an empty tenant**:

```bash
python3 bulk_ingest.py --tenant acme --domain person --file people.csv --mapping "..." --bulk --workers 8 [--workdir /big/disk] [--lineage multi_source|all|none]
python3 bulk_ingest.py ... --bulk --onboard                        # ADD a new source system to data that already exists (see Onboarding below)
python3 bulk_ingest.py ... --bulk --delta                          # more records from a source already loaded: new, unchanged (skipped) or changed (see Delta below)
python3 bulk_ingest.py ... --bulk --dry-run --workdir W        # stages A-D only: report what the load WOULD do, write nothing, keep the work files
python3 bulk_ingest.py ... --bulk --workdir W --resume --budget 200   # stop cleanly after ~200 s (exit code 3); run it again to continue
python3 benchmark.py bulk --size 1000000 --workdir W --budget 200 --before benchmarks/scale_person_step4b.json   # generated people, scored against the truth
python3 benchmark.py delta --base 500000 --feed 500000 --workdir W --budget 230   # a delta onto existing data, in pieces that each fit one call (resumable)
```

| stage | what happens | parallel |
|---|---|---|
| A standardize | every record: matching form, cleansed form, blocking keys | yes (`--workers`) |
| B pairs | records grouped by blocking key on disk; a key shared by more than `max_bucket_size` OTHER records is skipped (the same rule as the regular engine); each candidate pair emitted once | no |
| C score | every pair scored with the same `score_pair` and the same learned-model blend | yes |
| D cluster | the auto-merge pairs are unioned: every record is in exactly one entity, named by its first arrival | no |
| E write | raw records, staging, blocking index, candidates (auto pairs confirmed, review pairs pending), golden records and lineage, all in batches | no |

**Nothing is written to the store before stage E**, so a failure in A to D (including a missing hashing key) leaves it untouched, and a failure during E is **rolled back** (every id of the load carries a token), so the load can simply be run again. Work files are on disk (a temporary SQLite database), so memory does not grow with the number of pairs; what stays in memory is the union-find array (4 to 8 bytes per record) and one chunk of rows.

**What is tested** (33 tests, checked by breaking the engine 13 ways): it is a fourth engine in `benchmark.py equivalence`, and in every arrival order, with one and with two workers, it gives the same entities and review pairs as the reference matcher. Against the regular engine on the same input it leaves **identical golden records (every value and every quality issue), staging forms, blocking index and candidates** (same pairs, same scores, confirmed or pending). `reindex` straight after a bulk load changes nothing, which is the strongest check that its golden records are the regular ones (both engines build them with the same `compute_golden`). The regular engine then carries on from it: a later record finds what was bulk-loaded. It refuses a tenant that already has records of the domain and an input that holds the same `(source_system, source_record_id)` twice, and the SSN is still plain in raw only.

**Where it differs from the regular engine, on purpose.** Lineage defaults to `multi_source` (only golden records of two or more source records get one row per field; the regular engine also writes a row at every intermediate rebuild, and at 100M records `all` is over a billion rows). `ingested_at` is the input order, a microsecond apart, instead of wall-clock arrival (so "most recent" means "latest in the input"). Ids carry a load token (`rec_<token>_<n>`). LLM adjudication is not used (ambiguous pairs go to review).

**What it is not.** It is single-machine. Nothing else may ingest into the same tenant and domain while a load or an onboarding runs (the guards that detect a change are checks, not locks). The Postgres batch writes are written to the same shape as the SQLite ones and have never run on a live server.

### Onboarding a new source onto existing data

```bash
python3 bulk_ingest.py --tenant acme --domain person --file new_crm.csv --mapping "..." --source-system new_crm --bulk --onboard [--dry-run --workdir W]
```

A bulk load refuses a tenant that already holds the domain; `--onboard` (`bulk_load(..., onboard=True)`) is the other case: a **new source system** arrives and must be resolved against what is already there. The result is exactly what the regular engine would have produced by ingesting the new records one at a time onto the existing data (it is an engine in `benchmark.py equivalence`, onto a bulk-loaded base and onto a record-by-record base).

| stage | what changes for an onboarding |
|---|---|
| A | each new record's natural key is checked against the existing records; **a record already held is refused** (a changed record is the regular path's job: it versions it) |
| B | new x new pairs as before, plus **new x existing pairs found through the stored blocking index**. A key's bucket is counted over existing AND new records (the reference matcher's rule). Existing records get negative numbers, so "the later record plays the new one" still holds for every pair. Pairs between two existing records are **never rescored**, so steward decisions stand |
| C | the same scorer, over the existing records' stored matching forms |
| D | the nodes are the new records **and the existing entities an auto-merge edge reaches**: a new record that reaches two existing entities merges them |
| E | in two parts. First the **new rows** (records, staging, index, candidates, new entities): a failure undoes them by the load's token. Then **every change to existing data in one transaction**: the losers' records move to the survivor (the **oldest entity survives**, so downstream systems keep its id), losers become `merged` tombstones, survivors' golden records are rebuilt with lineage ("onboarding load"). All of it applies or none of it does |

**Refused, with a message:** existing data without `--onboard`; a natural key already held (`--delta` is for applying changes to records already held); existing records that are unfinished (`process-pending` first); existing data that changed between a dry run or a pause and the write. `--dry-run` reports how many existing entities would gain records and how many would merge, and writes nothing.

**What is tested** (21 tests, `tests/test_onboarding.py`, checked by breaking the engine 17 ways, every one caught): equal to the regular engine record by record (golden values, candidates and their statuses, staging, the blocking index, the entity id of every existing record, existing tombstones), for customers, for people with a protected SSN, onto a bulk base and a record-by-record base, with several workers; **steward confirmations and rejections made before the onboarding stand**; a generated scenario of 25 people with two- and three-way bridges (35 existing entities merged away, including losers that held several records); the survivor keeps its age; new records are the most recent, so their values win conflicts; the cap counts existing holders; a failure in either part of the write leaves every table exactly as it was; the guards; the store operations on their own; the command line.

**Measured, once, as a sanity check and not a benchmark:** 30,000 generated people loaded at once and 20,000 loaded then 10,000 **onboarded** ended **identical** (entities, review pairs, auto pairs, golden records). The onboarding took 6.5 s on one core (567 existing entities gained records; 0 merged). The merge path is therefore exercised by the tests above, not by that run.

**Limits.** A bucket that grows past the cap *after* its records merged does not unmerge them (only a person unmerges, one pair at a time: see "Unmerge and the audit page"). Changes to existing entities are collected in memory and applied in one transaction, so an onboarding that touches millions of existing entities needs memory for them. Each new record's natural key is checked with one query, which a very large onboarding will want batched. The guards against concurrent writers are detection, not locking.

**Measured at 1,000,000 people** (a dry run: stages A to D, exact numbers for every record, one core; `benchmarks/bulk_trial_1m.json`; the population is the one the earlier sampled 1M test used, scored against the generator's truth):

| | exact, all 1M records | the earlier sampled estimate |
|---|---|---|
| candidate pairs | **1,389,661** (1.39 per record; 0 keys over the cap) | 1.5 per record |
| true duplicate pairs that were compared (blocking recall) | **99.916%** | 99.90% |
| stage seconds | standardize 370 · pairs 32 · score 424 · cluster 1.4 = **827 s**, 1,210 records/s | not measured |

What only an exact, end-to-end run can say: of the **79,837** true duplicate pairs, 67,945 were auto-merged, 11,556 went to review, 269 were compared but scored too low, and 67 were never compared. Counting transitively, **85.4%** of true duplicates ended in one entity (931,793 entities against 920,163 true ones). **Only 10 entities contain records of different people** (10 records), so what it merges, it merges correctly. The sampled estimate held up: 1.5 against 1.39 pairs per record and 99.90% against 99.92% recall are within sampling noise (a test cross-checks the two instruments on the same population).

**A finding to act on: the review queue.** 89,990 pairs went to review, but only **18.9% of them are the same person**; about 73,000 are different people the scorer was unsure about. That is 9 review pairs per 100 records. If it scaled linearly, 100M records would mean about 9 million review items, four fifths of them non-duplicates: not something stewards can work through. The auto-merge threshold (0.92) and review threshold (0.55) were set on a few hundred labelled cases and have not been tuned against a population like this; that is the next quality lever.

**The same 20,000 people through both engines** (`python3 benchmark.py engines --size 20000 --workdir W --budget 240`, resumable; `benchmarks/engines_trial_20k.json`). The regular engine is the path `bulk_ingest.py` takes without `--bulk`; the bulk engine was run as a full load, writes included. 1M was not run on the regular engine: at the rates below it would take most of a day, which does not fit this environment.

| 20,000 people, one core | regular engine | bulk engine |
|---|---|---|
| time | **1,113 s** (18 records/s) | **11 s** (1,828 records/s): standardize 5.1, pairs 0.3, score 0.1, cluster 0.02, **write 5.3** |
| entities (truth: 18,461) | 18,692 | 18,692 |
| auto-merged pairs / review pairs | 1,371 / 280 | 1,371 / 280 |
| true duplicates merged | 84.99% | 84.99% |
| entities holding different people | 0 | 0 |
| review precision | 90.7% | 90.7% |

**The two engines left identical results**: the same entities, the same review pairs, the same auto-merged pairs, and the same golden records (every value and quality issue), checked by comparing the two databases, not by trusting two counters. The bulk engine was **about 100 times faster end to end** at this size.

**The regular engine slowed down as the data grew**: its rate was 48 records per second over the first 5,400 records, 22 over the next 5,200, and about 15 later, averaging 18. I have not diagnosed why (a per-record cost that rises with the size of the store is the obvious suspect, but that is a guess). It means the gap would widen at 1M, which cannot be measured here.

**What this adds to the 1M dry run.** The write stage is now timed: 5.3 s for 20,000 people (about 3,750 records per second). **As an estimate only**, if it slows with size the way standardizing did (3,900 records/s at 20k, 2,700 at 1M), writing 1M would take about 6 minutes, and a whole 1M load about 20 minutes (the measured 827 s for stages A to D plus the estimate). Not measured at 1M.

**The review queue gets worse with scale, not just bigger.** Review precision is 90.7% at 20,000 people but 18.9% at 1,000,000, and review pairs are 1.4% of records at 20,000 but 9% at 1M: with more people there are far more different people who look alike enough to be uncertain. So the 100M figure is not "9 million, scaled linearly"; it is likely worse.

### Delta: more records from a source that is already loaded

```bash
python3 bulk_ingest.py --tenant acme --domain person --file todays_changes.csv --mapping "..." --source-system crm --bulk --delta [--dry-run --workdir W]
```

A delta feed carries **new and changed records** (deletes are not handled yet: they will be a soft-delete flag). It is the onboarding pipeline with a classification step in front, so a new source system is simply the case where every record is new. Matching never looks at the source system: it compares standardized field values. A record's identity, (source system, source record id), is what tells **new** from **changed**.

| a record of the feed is | what happens |
|---|---|
| **new** (natural key not held) | onboarded, as above |
| **unchanged** (same content hash) | set aside after one lookup: nothing is read, scored or written for it |
| **changed** (same key, new hash) | updated **in place** (same record id, same entity); the old payload goes to `source_record_history`; its pending review items are superseded; it is re-matched on its **new** keys; its entity's golden record is rebuilt |

It mirrors the regular engine's rules for a changed record, set-wise: a change can **add** merges and never splits an entity; a changed record is **not rescored against its own entity**; it becomes the **most recent** as of its place in the feed (survivorship sorts by `updated_at`, falling back to `ingested_at`; an earlier version of this document said recency did not move, which was wrong). Two traps are handled that onboarding did not have: the **keys a changed record gives up** are ignored (they would otherwise inflate the bucket count against the cap and pair it with records it no longer resembles), and **everything that touches existing rows**, the record updates, history, superseded items, restaged keys, new pairs, merges and golden rebuilds, goes through the one all-or-nothing store call onboarding uses.

**What is tested** (21 tests, `tests/test_delta.py`): equal to the regular engine record by record on mixed feeds of new, changed (four kinds of change) and unchanged records, onto a bulk base and a record-by-record base, for customers and for people with a protected SSN, with several workers, and paused and resumed; versions, hashes, payloads, history rows, the arrival time of every existing record, candidates with their statuses (superseded included), entity ids and tombstones all compared. Scenarios for a change that links two entities, the old payload and the record's age, stale keys, the bucket cap, the own-entity rule, superseding, unchanged records writing nothing, and a failure in either part of the write leaving every table as it was. Checked by breaking the code 17 ways: 16 were caught, and the 17th showed a check that could never be reached, which was deleted.

**Measured, once, as a sanity check:** 20,000 people loaded, then a feed of 15,000 records (10,000 new, 2,000 cosmetically changed, 3,000 unchanged) applied as a delta in about **10 s** on one core, against 24 s to load all 30,000 final records from scratch. Against that from-scratch load (changed records placed after the others, so recency matches): the **entities and auto-merged pairs are identical**; the review pairs differ by exactly 2, both inside one entity with a changed record (the own-entity rule), none unexplained; and 28,001 of 28,002 golden records match, the last differing only in which record is named for a junk SSN, an ordering artifact of that comparison: the regular engine's golden rebuild over all 1,983 entities holding a changed record changes nothing.

**Measured at 500,000 records** (`python3 benchmark.py delta --base 500000 --feed 500000 --workdir W --budget 230`, resumable, `benchmarks/delta_trial_500k.json`): a feed of 500,000 records (400,000 new people and 100,000 existing people whose email, phone, last name or address changed) applied to a base of 500,000 people, on **one core of a sandbox that ran about 1.7x slower than earlier in the same session** (judged by the test suite), 4 GB of memory, SQLite. Each write must finish inside one call there, so the feed went in as **17 deltas** (3 of 50,000 and 14 of 25,000); each is a complete delta and their stage times add up.

| | seconds | share of the 1,772 s |
|---|---|---|
| **pairs**: look up each key in the stored index | 760 | 43% |
| **write** | 493 | 28% |
| standardize (the key lookups that classify records are 9 s of this) | 195 | 11% |
| score | 167 | 9% |
| cluster | 1.4 | 0.1% |
| not timed per stage (planning entity merges, setup, cleanup) | about 156 | 9% |
| **the whole 500,000-record delta** | **1,772 (about 30 minutes, 282 records/s)** | |

The base took 894 s (559 records/s, in ten pieces). 925,948 pairs were compared (1.85 per feed record): 28,918 auto-merged and 45,320 for review; 126,001 existing entities were rebuilt, 66 were merged into an older one, and 372,929 new entities were created. Afterwards: 900,000 records, 838,791 active entities (115 merged tombstones, some from building the base), 100,000 history rows and 100,000 records at version 2, a 4.10 GB database, and **no integrity problems** (no record without an entity, none unfinished, none in a merged entity, no active entity without records).

**What it shows.** (1) Classifying a record costs about 20 microseconds: the per-record key lookup that looked like the risk is not one. (2) **The cost of a record grows with the data already held**: 2.6 ms for the first slices, 3.2 to 3.7 ms for the later warm ones. The growth is in the pairs stage (1.6 s per 50,000 records onto an empty base, 57 s onto 500,000) and the write stage (28 s to 57 s); standardizing stays flat. (3) The pairs stage **is sensitive to a cold cache** (and was, it turned out, also scanning too much, see below): after the sandbox restarted, one 25,000-record slice with a cold disk cache took 214 s against about 85 s for its neighbours, and only that stage slowed (86 s against about 41 s) while the CPU-bound stages were faster. Pairs and write are 71% of the time; profiling them (below) found the pairing stage's growth came mostly from two lookups that scanned the whole tenant, now fixed.

### Making the pairing stage cheaper

The 500,000-record delta above spent 43% of its time pairing and 28% writing. Profiling a delta against the 900,000-record store it left behind (`benchmarks/profiling/`, cold page cache, same feed, same store, results compared) found the pairing cost was **not** what I first thought, and corrected two of my own claims:

* **Two lookups were full scans in disguise.** `get_record_entities` and `get_entities_batch` (both mine) were planned by SQLite as a scan of *every* record or entity of the tenant for each batch of 500 ids, instead of a primary-key lookup. That is why they cost 0.45 s per batch and **grew with the size of the base**: it, more than a "cache miss on the index", was the growth I measured. The fix is a unary `+` on the tenant columns, so the planner uses the primary key, with the tenant still enforced; a test pins both plans.
* **Entity ids were fetched for every existing record that was paired, and almost none need one.** Only existing records on an auto-merge edge do (looked up in stage D) and, for a delta, those paired with a changed record (for the same-entity rule). Scoring also needs only an existing record's matching form, not its cleansed form.
* `unfinished_in` read every record's row to test one column; it now goes through the partial index that holds only unfinished records. Superseding review items reads the pending set once instead of once per 400 changed records.

| the same 25,000-record delta (5,000 changed, 20,000 new) on a 900,000-record store, cold cache | before | after |
|---|---|---|
| dry run, whole | 84.7 s | **38.4 s** (-55%) |
| pairing stage | 64.5 s | **19.4 s** (-70%) |
| existing-record entity lookups | 45.8 s | 1.1 s |
| **a real delta, whole (new code ran second, on a store 25,000 records larger)** | **110.6 s** | **65.0 s (-41%)** |
| planning (loading the entities to rebuild) | 4.3 s | 0.3 s |

The results are **identical** (60,987 pairs, 763 auto-merged, 3,244 for review, 5,624 entities touched, 6 merged) between the old and new code on the same store. The full 500,000-record run was **not repeated** with the new code, so its 30-minute figure is for the old code; the 25,000-record comparison above is what was measured.

**Tried and rejected, by measurement:** counting a key's bucket only up to the cap (buckets are tiny at this scale, median 1, and the capped count was slower), the journal mode (committing 24,000 rows costs 0.1 s in any mode, WAL included), skipping the per-record deletes (0.00 s), sorting the key inserts (no gain), and a cheaper cleansed-form fetch (the cost is per row, not per byte). **The write stage is unchanged**: about half of it is inserting each record's roughly 14 blocking keys into the one large key index, at about 76 microseconds a key with a cold cache, because real keys land all over the index. Larger write batches would cut that by about a quarter (58 against 76 microseconds a key, measured in isolation, not in the engine); fewer keys per record, which I have not tried and which changes what the index holds, is the structural lever.

**What it does not show.** It is not a check of correctness at this scale (that is the 30,000-person comparison above and the tests; the integrity counts are consistency checks, not accuracy). The regular engine was not run for comparison (at its 18 records/s measured earlier on 20,000 people it would take over 7 hours, and it slows as data grows). Because the cost grows with the data held, **it cannot be extrapolated to a base of tens of millions**, nor to Postgres or other hardware.

**Limits.** Deletes are not handled. There is no drift report (a list of existing merges that new data no longer supports): a change cannot undo a merge. A pair a steward rejected is re-proposed if one of its records changes, as in the regular engine. Each feed record's key is looked up with one query: at 500,000 records that cost 9 s in total, so it is not the concern it looked like. Postgres versions of the new store operations have never run on a live server.


## How matching weighs evidence (and how that was measured)

A match score that can only go **up** treats "the emails differ" the same as "no email recorded", and lets one exact identifier override clearly different names. Measured on the original matcher, that wrongly merged households, Jr/Sr pairs and strangers sharing a placeholder 100% of the time. What changed:

- **Placeholders are missing, not evidence.** `none@none.com`, `000-000-0000`, `unknown` standardize to empty, so they never match, never block, never count as agreement. Built-in rules are deliberately conservative (a real-looking tax ID like `123456789` is kept). Add your own per field: `"anonymous": ["x@y.com"]`, plus `"anonymous_defaults": true` for the built-in list.
- **Disagreement subtracts.** `mismatch_penalties: {field: penalty}`: when both values are present and disagree, the penalty is subtracted. Identifiers compare exactly; names by similarity (below 0.6 = disagree); `{"penalty": 0.4, "compare": "exact"}` forces exact. A second identifier differing (another email) is **not** penalized once something else matched exactly, because people have several; `exclusive_fields` (tax ID, DUNS, UPC) are the exception: two different tax IDs are two companies.
- **Missing is not different.** An empty field contributes nothing either way.
- **Generational suffix is kept** (`name_suffix`, read from `last_name` if not given), so a Jr and a Sr at one address go to review instead of merging.
- **Nicknames are canonicalized** (`Bob` becomes `robert`) before comparing. String similarity can't do this: Bob/Robert scores 0.50 while unrelated first names score under 0.6 about 90% of the time. The table (`NICKNAMES` in `app/standardize.py`) is small, US-English, and leaves ambiguous ones (Pat, Chris, Sam, Jamie) alone.
- **Blocking keys got smarter** (`"blocking": [[{"field": "last_name", "as": "soundex"}, ...]]`): `soundex` (phonetic; Smith/Smyth), `windows` (overlapping runs; a transposed phone digit still shares a window), `prefix`, `exact`. The built-in customer domain uses the combination that measured best.
- **Bucket cap** (`max_bucket_size`, default 500): a key shared by more records than that is skipped for lookup, and the ingest result lists it in `skipped_overpopulated_keys`. `python3 domains_cli.py blocking-stats` shows bucket sizes and flags a value shared by many records as a probable placeholder.
- **Every review candidate carries a per-field breakdown** (`contributions`: field, evidence, similarity, points that sum to the score), shown in the console and returned by the API.

### Measuring it: `benchmark.py`

```bash
python3 benchmark.py run --seeds 1 2 3 --scale 20 --label mine --out benchmarks/mine.json
python3 benchmark.py compare benchmarks/baseline_heldout.json benchmarks/mine.json
```

It generates labeled synthetic data (positives with controlled errors; hard negatives that must not merge; a Zipf-distributed background), runs the **real pipeline**, and scores it against the hidden truth, including **blocking recall**. `--domains-dir` benchmarks a config variant without touching code, which is how the blocking was chosen: four variants were compared on recall *and* cost before one was adopted. `benchmarks/` holds the original code's results and the current ones.

Held-out seeds 101-103 (never examined while tuning), original code vs now:

| | Before | After |
|---|---|---|
| Auto-link precision / recall | 28.6% / 58.0% | **94.8% / 77.8%** |
| Falsely merged pairs | 783 | **23** |
| Blocking recall on positives | 77.8% | **100%** |
| Wrong merges: placeholder / Jr-Sr / household | 100% / 100% / 100% | **0% / 0% / 8%** |
| Review-queue pairs (4% are true duplicates) | 6,062 | 2,637 |
| Candidates per record / records per second | 6.5 / 52.1 | 10.9 / 48.4 |

**Read this honestly.** The data is synthetic and the scenarios were written around defects found by inspection, so gains on them are expected by construction; the informative parts are the held-out run and the costs (about 68% more candidates per record, about 7% slower). Soundex helps only where a typo is phonetically neutral (the benchmark's mix is 60% vowel swaps, 40% random letters, and that mix is stated, not hidden). Real steward decisions are the true ground truth. Only the **customer** domain was benchmarked: product and vendor got placeholder handling and penalties by the same reasoning but are unmeasured.

**Still wrong or unbuilt:** same-name strangers in one ZIP all go to review (no frequency-based weighting, so "John Smith" counts the same as a rare name); moved + surname typo + transposed phone reaches review (88%) but never auto-links; households still wrong-merge 8% when the first names happen to look alike; Soundex only (no NYSIIS/Metaphone); placeholders are not detected automatically at ingest (the first records would already have merged: the cap and `blocking-stats` are the safety net); no re-match pass over existing records.

**Upgrading:** key functions and placeholder rules change what the blocking index stores, so the stale-index guard will stop ingestion into **customer, product and vendor** until you run `python3 domains_cli.py reindex <domain>`. `Jen`/`Jennifer` now auto-merge (eight test fixtures that used that pair as their "ambiguous" example now use Jenna). The customer schema gained `name_suffix`. The built-in domains now run on the declarative standardizer; the original hand-written functions are kept, and a test compares the two.

## Customer-defined domains

A domain is described by a config file, not code. You can define a new one (Location, Policy, Claim, ...) or change a built-in, with **no code change**. A new domain gets its own golden-record table (`entity_<name>`, a real column per field), created by `reindex`; everything else is shared tables keyed by a `domain` string.

```yaml
# domains/location.yaml   (JSON works too; YAML needs `pip install pyyaml`)
name: location
fields:                                   # every field, and how it's standardized
  location_code: identifier               # shorthand for one step
  name: name
  postal_code: postal_code
  state: [text, upper]                    # steps run in order
  country: {normalize: [text, upper], default: US}
deterministic_fields: [location_code]     # exact match on any of these = same record
fuzzy_weights: {name: 0.4, postal_code: 0.3}
semantic: {fields: [name], weight: 0.3}   # meaning-based similarity
blocking: [[location_code], [postal_code, name]]   # how candidate pairs are found
survivorship: {default: most_recent, fields: {name: most_complete}}
```

`domain_templates/` has a fully commented example (`location.yaml`) and the three built-ins as editable JSON. Files go in `./domains` or `$MDM_DOMAINS_DIR`, are read **at startup** (restart after editing), and an invalid file stops startup with every problem listed — never a half-applied set.

```bash
python3 domains_cli.py validate           # check config; reports every problem at once, suggests fixes for typos
python3 domains_cli.py list               # every domain served, and where each came from
python3 domains_cli.py export customer > domains/customer.json   # dump a built-in to edit (override already set)
python3 domains_cli.py normalizers        # the standardization steps a config may use
python3 domains_cli.py status             # is each domain's blocking index current?
python3 domains_cli.py reindex location   # rebuild one after a config change
```

**Standardization is a fixed vocabulary of 12 steps**, not code (`app/normalizers.py`) — a config file that could run code would be a security hole. They are the same functions the built-in standardizers are made of, and a test proves the three built-in domains are expressible in this format with byte-identical output, so a custom domain behaves exactly like a built-in one for the same step.

**Changing a built-in** needs `"override": true` (the `export` output has it); without it the file is rejected, so a typo can't silently replace `customer`. Copying a template *unchanged* changes nothing and needs no reindex.

### Why a reindex exists, and why you can't forget it

Candidate lookup trusts the blocking keys stored when each record was ingested. If an edit changes what those keys *would be*, the stored ones are stale and lookups quietly miss real duplicates — nothing errors. So instead of relying on someone remembering, each domain's index records a **fingerprint** of the configuration it was built under, and every ingest checks it. On a mismatch, ingestion into that domain stops (`IndexStaleError`; HTTP **409** from the API; a clear message from `bulk_ingest.py`) until `domains_cli.py reindex <domain>` runs.

| You change | Reindex needed? |
|---|---|
| `blocking` groups or key functions | **Yes** |
| How **any** field is standardized (`normalize`, `anonymous`, `sources`, `default`) or presented (`present`) | **Yes** — the staging layer stores every field |
| Survivorship (`survivorship`) | **Yes** — golden records are derived from it |
| The blocking key layout in `matching.py` (prefix length / format) | **Yes** — detected automatically |
| Fuzzy weights, semantic settings, deterministic fields, mismatch penalties, `max_bucket_size` | No — take effect on restart |

A reindex (staging, blocking keys, golden records) pauses ingestion into that domain while it runs, and **stays paused if interrupted** so a half-rebuilt index can never be used; re-run it to finish. Existing databases upgrade cleanly: migration 006 stamps the built-in fingerprint on domains that already hold data, so upgrading doesn't force a pointless reindex — but an override already in place at upgrade time is correctly flagged stale.

### What this does NOT do (read before relying on it)

- **A reindex rebuilds derived data (staging, candidate index, golden records) from raw. It does not re-run matching over existing records.** Matching is incremental — each record is compared once, when it arrives — so duplicates your new rules would have caught earlier stay unmerged until a new record matches them. Re-resolving history is a separate job and isn't built.
- **Golden records ARE rebuilt** by a reindex (they are derived from staging), but a changed *source* record is not re-matched, so a record that now matches something else stays where it is.
- **Config is deployment-wide, not per-tenant.** Every tenant shares one definition of each domain.
- **The validator catches structural mistakes, not bad matching design.** It can't tell that your blocking fields are too common (huge candidate sets) or too rare (misses everything) — that takes real data. Try changes on a copy first.
- Under Postgres row-level security with `FORCE`, run `reindex` as a role that bypasses it: with no tenant set it would see no rows and report a successful, empty reindex.

**What's verified:** parsing/validation, the registry, built-in equivalence, the guard, and the reindex job are fully tested on SQLite (57 tests, plus the end-to-end `edit config → status → reindex → ingest` flow through the real CLIs), and I confirmed the tests can fail by breaking the logic eight ways. **Not verified:** the new Postgres statements (index-state upsert, key swap) have never run on a live server — `tests/test_postgres_backend.py` covers them when `TEST_DATABASE_URL` is set. The `api.py` changes were exercised only with a stubbed FastAPI. The console's JavaScript was syntax-checked and tested in Node against hostile input, not in a real browser.

**Security fix in the same change:** the stewardship console previously inserted ingested values, field names, source names and match reasons into the page unescaped — a stored XSS (a record containing `<img src=x onerror=…>` would run in a steward's browser). It now escapes everything that comes from data; `tests/test_ui_escaping.py` runs the real script against hostile input, and fails on the previous version.

## Schema migrations

The database schema is owned by `app/migrations/` — a numbered, **append-only** history of changes. The app brings its database up to date on startup, **preserving existing data**. This replaces the old "`CREATE TABLE IF NOT EXISTS` on every start" approach, which could never change a table that already existed (a database from an older release silently kept its old structure — that is the `no such column: domain` error — and the only fix was deleting it).

```bash
python3 migrate.py status                  # what's applied, what's pending (exit 1 if not current)
python3 migrate.py upgrade --dry-run       # print the SQL that would run, change nothing
python3 migrate.py upgrade                 # apply pending migrations
```

**Guarantees** (each one is covered by a test in `tests/test_migrations.py`):

- **Atomic.** Each migration — its SQL, its optional Python data step, and the row recording it — commits as one transaction. A failure partway leaves the database exactly as it was.
- **One at a time.** A Postgres advisory lock (SQLite: `BEGIN IMMEDIATE`) means two app instances starting together can't both apply the same migration.
- **Tamper-evident history.** Each applied migration's checksum is stored; editing a shipped migration makes startup refuse with a clear message. The shipped checksums are also pinned in the test suite, so an accidental edit fails CI before it ships.
- **Fail-fast on "database newer than code"** (e.g. rolling back the app without the database).
- **No DDL when already current.** An up-to-date database starts with no schema changes at all, so an app role without CREATE/ALTER privileges can still boot.
- **Forward-only, on purpose.** There are no "down" migrations; rolling back means restoring a backup.

**Adopting databases created before this system existed.** The migrations are idempotent, so a database built by the multi-tenant/multi-domain-era code is adopted as-is and its data kept. Migration 005 backfills blocking keys for records that predate the index — without it, new records could never match those old ones. A database from *before* multi-tenant/multi-domain support (no `tenant_id`/`domain` columns) can't be upgraded by guessing values; it's refused with an explanation, and nothing is changed. Recreate it.

**Production pattern (least privilege).** Run `migrate.py upgrade` as the schema owner, then start the app with `MDM_AUTO_MIGRATE=false` under a restricted role. In that mode the app only *verifies* the schema is current and refuses to start otherwise — it never alters the schema itself.

**Adding a migration:** append a `Migration(...)` to `MIGRATIONS` in `app/migrations/versions.py` (instructions are in that file's docstring), then pin its checksums in `test_shipped_platform_migrations_are_frozen`. Never edit or renumber one that has shipped. The tracking table is keyed by `(namespace, version)`, so a second, independently versioned list (e.g. customer extensions) can share the runner later; only the `platform` namespace exists today.

**What's verified, and what isn't.** SQLite is fully tested: fresh install, adopting a legacy database with data intact, the backfill, rollback, tamper detection, check-only mode, and 6 threads racing to start on a fresh database (repeated across 6 databases per run). The tests were checked by deliberately breaking the runner four ways to confirm each break is caught. **Postgres SQL has never been executed:** the control flow (lock → one transaction per migration → unlock, placeholder translation, no lock or DDL when current) is verified against a fake `psycopg2`, and the Postgres statements match the old schema text exactly — but the `DO $$` blocks, the advisory lock, and the `information_schema` queries have not run against a live server. To validate: `python3 migrate.py upgrade` against a scratch database, then `psql -c "\d source_records"` (`attributes` should be `jsonb`); `tests/test_postgres_backend.py` also checks this when `TEST_DATABASE_URL` is set.

**Upgrading an existing Postgres database** created before JSONB support: migration 004 converts six columns (a table rewrite) on first start. It is atomic — if any value isn't valid JSON it rolls back and changes nothing — but **take a `pg_dump` first**. Afterwards, `GET /v1/admin/audit-log` should still report `chain_valid: true`; the chain hashes the parsed, key-sorted JSON, so the conversion shouldn't affect it (audit details hold strings, integers and booleans), but that is reasoned, not verified on a live server. If Postgres row-level security is enabled with `FORCE`, run migrations as a role that bypasses it: under RLS with no tenant set, the backfill would see zero rows and silently do nothing.

## Postgres (optional — SQLite is the default)

```bash
docker run --name mdm-postgres -e POSTGRES_PASSWORD=devpass -p 5432:5432 -d postgres:16
createdb -h localhost -U postgres mdm_dev
pip install psycopg2-binary
export DATABASE_URL=postgresql://postgres:devpass@localhost:5432/mdm_dev
python3 seed_demo.py
```

`app/db_postgres.py` now includes the Phase 3 tables (`api_keys`, `audit_log`, `usage_events`) with the same interface as SQLite's `Store`.

**⚠️ Update: a real run against real Postgres surfaced a genuine bug**,
since fixed. The original version opened a brand-new network connection
(full TCP handshake + auth) on **every single database call** instead of
reusing one — invisible against SQLite (opening a local file is nearly
free) but devastating against a real networked database: 200 records
took **378 seconds** (expected ~2-5s). Fixed by holding one persistent
connection per `PostgresStore`, guarded by a reentrant lock (`RLock` —
some methods call other methods that reacquire it from the same thread,
e.g. `get_entity()` calling `list_source_records()` from inside its own
connection block; a plain `Lock` would deadlock there). The fix's
control flow was validated with a mocked `psycopg2` in the build sandbox
(confirms exactly 1 connection opened across nested and
concurrent-thread access, zero deadlocks) — real query execution against
a live server is only as validated as the test suite below. Validate
with:

```bash
export TEST_DATABASE_URL=postgresql://postgres:devpass@localhost:5432/mdm_dev
python3 -m unittest tests.test_postgres_backend -v
```

### JSONB, not TEXT, for JSON-holding columns

`attributes`, `reasons`, `details`, `metadata`, and `allowed_domains` are native `JSONB` columns, not `TEXT` holding JSON strings. This unlocks querying *into* attribute fields directly in SQL — `Store.find_entities_by_attribute(tenant_id, domain, field, value)` uses Postgres's `->>` operator instead of loading every entity into Python to filter. SQLite has no equivalent (that method only exists on `PostgresStore`).

Writes use `psycopg2.extras.Json(...)` (via a small `_j()` helper); reads rely on psycopg2's built-in jsonb auto-adaptation — a `SELECT` on a jsonb column returns an already-parsed Python dict, not a string, so the old `json.loads()` calls were removed (and would now raise `TypeError` if left in, since you can't `json.loads()` a dict).

**Validated:** the full write→read logic was tested against a mock psycopg2 that simulates real jsonb auto-adaptation (confirms no crash trying to re-parse an already-parsed value, correct round-trip of the original dict). **Not validated:** actual behavior against a live Postgres server — `tests/test_postgres_backend.py` covers this, runs for real once you have `TEST_DATABASE_URL` set.

### Bulk batching (`ingest_batch`)

`app/pipeline.py`'s `ingest_batch()` (used by `bulk_ingest.py` via `--batch-size`, default 200) batches the *write* side of ingestion — all raw records in one bulk `INSERT`/commit instead of one commit per record. Matching stays strictly per-record and in-order, by necessity: an earlier version also batched blocking-key writes upfront, which let record #3's candidate search see record #500's data before #500 had "happened" in ingestion order — a real correctness bug (327 vs 79 entities for identical 1000-record input), caught by comparing cluster outputs, not just timing. Fixed by keeping blocking-key writes per-record; only the safe part (raw record storage, which doesn't affect matching) is batched.

**Verified:** exact clustering parity with the per-record loop (not just matching counts — the actual record-to-entity groupings compared directly). Measured speedup on SQLite: **~1.15x** — modest, since SQLite's per-commit cost is already low. Expected to matter more on Postgres, where each avoided commit is a network round-trip — not measured live.

### Row-Level Security (RLS) — the least-verified piece in this project

`app/rls_policies.py` + `setup_rls.py` add RLS as defense-in-depth *beneath* the application-level `WHERE tenant_id = ...` filtering every query already does — enforced by Postgres itself, so even a bug in application code can't leak one tenant's rows to another.

```bash
# 1. Create a restricted role for your app to actually run as
python3 setup_rls.py create-role --role-name mdm_app --password CHANGE_ME

# 2. Apply policies (as the role that owns the tables -- e.g. your default postgres connection)
python3 setup_rls.py enable

# 3. Point the app at the RESTRICTED role, then verify it's actually subject to RLS
export DATABASE_URL=postgresql://mdm_app:CHANGE_ME@localhost:5432/mdm_dev
python3 setup_rls.py verify
```

**⚠️ Read this before trusting it:** RLS policies are silently **invisible** to superuser roles, table owners (unless `FORCE ROW LEVEL SECURITY` is set — this module sets it), and any role with `BYPASSRLS`. If your app connects as the default `postgres` superuser — which is exactly what the Docker quickstart above has you do — **RLS does nothing**, even though it's technically "enabled." `setup_rls.py verify` exists specifically to catch this: it checks the *current* connection's role against all three exemptions and fails loudly if any apply.

`db_postgres.py`'s `_conn()` now accepts a `tenant_id` and issues `SET LOCAL app.current_tenant_id = %s` before every tenant-scoped query — this is what the RLS policies check. **Deliberately excluded:** the `api_keys` table, and specifically `get_api_key_by_hash()` — authentication has to find a key *before* knowing its tenant (the key is what reveals the tenant), so there's no tenant_id to scope that one lookup by.

**What's actually validated:**
- ✅ The `SET LOCAL` mechanism fires with the correct `tenant_id` before scoped queries, and correctly skips it for the auth lookup — confirmed with a mock capturing every executed statement.
- ✅ The generated SQL itself — every tenant-scoped table gets `FORCE ROW LEVEL SECURITY` and a policy using `current_setting(..., true)` (the fail-closed variant — a forgotten `tenant_id` means zero rows returned, never every tenant's rows).
- ⚠️ **Not validated: that Postgres actually denies cross-tenant rows.** This needs two real database roles and a live server to test meaningfully — there's no way to mock "does the database refuse this row." `test_rls_policies_apply_without_breaking_normal_queries` in `test_postgres_backend.py` only confirms enabling RLS doesn't break normal queries for whatever role runs the test suite (commonly a superuser, i.e. exempt from RLS anyway) — that is a smoke test, explicitly **not** a security validation, and its docstring says so.

**To actually validate the security guarantee**, after `setup_rls.py verify` passes: ingest a record under `tenant_a`, connect as the restricted role, and confirm a query for `tenant_b` genuinely returns nothing for `tenant_a`'s data — by hand, or scripted, against your real instance.

## Active learning per domain

```bash
python3 retrain.py --tenant acme --domain product
```

Models isolated per (tenant, domain) — see `models/<tenant>__<domain>_match_model.json`.

## Learning the thresholds: a bounding experiment

**The policy this was measured against:** at most **1 false merge in 2,000 automatic merges** (counted per merged pair; per record or per entity would need re-measuring). The auto-merge and review cutoffs (0.92 and 0.55) are constants in `matching.py`; nothing learns or calibrates them.

**Two problems in the existing active-learning loop.** (1) `build_training_set` takes every `confirmed` and `rejected` candidate, and auto-merged pairs are stored as `confirmed`: the model is trained largely on the system's own decisions, and its only negatives are steward rejections from the review band, so nobody ever labels the auto-merges and their precision cannot be measured. (2) The model only nudges the score (a fixed 35% blend); the cutoffs do not move with it, and there is no holdout, no calibration, and a floor of 10 training examples.

**The experiment** (`benchmarks/profiling/extract_pairs.py`, `threshold_experiment.py`, `transfer_experiment.py`; they need scikit-learn, which the project itself does not use). Data: the 900,000-record store left by the delta trial, 130,401 scored pairs of which 81,065 are real duplicates by the generator's truth, with the stored per-field evidence as features. A gradient-boosted model is trained and scored out of fold, split by person so no identity crosses folds. "Realistic" trains only on what stewards would label (the review band) plus a random 2% audit of auto-merges.

| At 1 false auto-merge in 2,000 | auto-merged pairs | review queue (precision) |
|---|---|---|
| today, fixed 0.92 cutoff | 67,370 (28 false: 1 in 2,406) | 63,031 (21.8%) |
| today's rule score, best possible cutoff | 70,336 (35 false) | 44,299 (23.4%) after dropping 15,766 |
| model trained on all labels (upper bound) | 78,473 (39 false) | 2,296 (97.0%) after dropping 49,632 |
| model trained on review band + 2% audit | 78,405 (39 false) | 2,375 (96.6%) after dropping 49,621 |

By the stricter test (the 95% upper confidence bound on the false rate must itself stay under 1 in 2,000) the rule score is safe only up to 53,011 auto-merges and the model up to about 74,000. A "drop" removes the lowest-ranked pairs while losing at most 0.5% of the real duplicates.

**Does it transfer?** World B: 120,000 people, SSN present in 20% of records instead of 70%, email 30% instead of 60%, 15% duplicates instead of 8%.

| World B | auto-merged | false | real duplicates lost to the drop | review queue (precision) |
|---|---|---|---|---|
| fixed 0.92 cutoff | 13,288 | 0 | none dropped | 15,676 (47.4%) |
| model and thresholds from world A, unchanged | 19,307 | 0 | **788 (3.8%)**, against a 0.5% budget in A | 628 (99.7%) |
| model trained on B's own labels | 20,113 | 11 (1 in 1,828) | 102 (0.49%) | 5,833 (8.9%) |

**What it shows.** (1) The potential is large: the review queue shrinks from 63,031 to about 2,300 while auto-merges rise by about 11,000 real duplicates. (2) The **auto-merge cutoff transferred** (no false merge in 19,307) but the **drop cutoff did not**: a threshold learned elsewhere silently lost 3.8% of the real duplicates. Never auto-drop on thresholds learned for another tenant; calibrate per tenant. (3) **Choosing the cutoff by point estimate overshoots out of sample**: the realized rates were 1 in 1,961 and 1 in 1,828 against a target of 1 in 2,000. Select with the upper confidence bound (costing about 6% of the auto-merges) or a safety margin. (4) **Verifying 1 in 2,000 takes audit volume**: about 6,000 audited auto-merges with no error to bound the rate under 1 in 2,000 at 95% confidence, so a 2% audit needs about 300,000 auto-merges to accumulate that.

**What it does not show.** Everything is synthetic, from a generator whose hard cases each have a distinctive evidence pattern that a tree model finds easily; real data overlaps more, so treat the size of the gain as an upper bound, and the review-queue reduction in particular. Two worlds from one generator are not proof of transfer. It counts false merged *pairs*; auto-merges chain into entities, so entity-level error needs its own measurement. It does not cover drift over time, or any feature beyond the stored evidence. No real labelled data has been used.

**Order I would build it in** (steps 1 and 2 are now built, see "The learned scorer"): (1) fix the label hygiene (exclude system decisions from training, record where each label came from, add the random audit); (2) a calibrated scorer with policy thresholds chosen by upper confidence bound per tenant, run in shadow mode against today's decisions using the dry-run engine; (3) never auto-drop until a tenant has its own audit labels; (4) unmerge, before any automation of merges.

## The learned scorer: policy B, with a 2% audit and an automatic rollback

**The decision (yours): policy B.** The rule score's automatic merges stay exactly as they are. A learned model may **promote** a pair the rule score sent to review, at the lowest probability that keeps the **pooled** estimated false-merge rate of ALL automatic merges within the target (1 in 2,000), and the promoted pairs must also be good on their own (an estimated false rate no more than 10 times the target, because the pooled average alone would let a handful of very bad promotions hide among thousands of good merges). That threshold is a **point estimate, not a certificate**. What guards it is that **2% of the rule score's automatic merges and 10% of the model's promotions are flagged for a person to check** (`MDM_AUDIT_RATE`, `MDM_AUDIT_RATE_PROMOTED`; `MDM_AUDIT_RATE=0` switches all auditing off), and an **automatic rollback** that switches the model off if the audits show it is hurting.

**The pieces.** `train_scorer.py` trains offline (scikit-learn and numpy; nothing else needs them) and exports plain JSON; `app/learned_scorer.py` evaluates those trees in **pure Python** and is called by all three engines through one function, so promotion cannot differ between them (a test runs six engines in two arrival orders against the reference matcher with a live scorer); `app/monitor.py` re-estimates from the audits and rolls back; `domains_cli.py shadow-report` shows what a model would do, read-only; `domains_cli.py audit` and `scorer` are the commands.

```bash
python3 train_scorer.py --tenant acme --domain customer              # PROPOSES a threshold (needs scikit-learn); nothing changes
python3 domains_cli.py shadow-report customer --tenant acme          # what it would promote, read-only, beside what people said
python3 train_scorer.py --tenant acme --domain customer --activate   # makes the threshold LIVE: the engines promote from the next record
python3 domains_cli.py scorer check customer --tenant acme           # run on a schedule: rolls back on evidence of harm; exit code 3 = rolled back
python3 domains_cli.py scorer status customer --tenant acme          # state, the two kinds of automatic merge and how each was audited
python3 domains_cli.py scorer reset customer --tenant acme --by jane # switch promotion back on after a rollback
python3 domains_cli.py audit list customer --tenant acme --pending   # the merges waiting for a person
python3 domains_cli.py audit verdict customer --tenant acme --candidate cand_... --verdict correct --by jane
```

**Training only on what people decided.** `Store.human_labeled_candidates` is the one source of labels: a steward's confirm or reject, and a person's verdict on an audited merge. A system decision (an automatic merge, a promotion, an LLM verdict) is not a label, and the system cannot audit itself. (The active-learning loop used to train on every `confirmed` pair, automatic merges included.)

**The rollback acts on evidence of harm, not on the lack of proof of safety.** It rolls back only when the lower 95% bound of the pooled false rate is over the target (so even the optimistic reading is over), at least 100 promoted merges have been audited, and the rule score's own merges are not what is over the target. Zero errors never trigger it, however few audits there are: a smoothed point estimate would have rolled back a clean model for want of about 1,000 audits. **A rollback stops future promotions; pairs already promoted stay merged** (a person can unmerge them one at a time: see below). The check lists the promoted pairs audited wrong, and the audit page can unmerge each on the spot.

**Measured** on the synthetic 130,401-pair world, at a 2% audit (1,343 audited rule merges, none wrong) and the real target of 1 in 2,000: the model proposed promoting at p >= 0.951, which would turn **2,176 of the pending review pairs into automatic merges** (+3.2%). Estimated false rate of those 0.254%, of the rule's merges 0.037%, pooled 0.0440% against the limit 0.0500%. Against the generator's truth: 69,546 automatic merges, 29 wrong (**1 in 2,398**), against the rule score alone's 67,370 and 28 wrong (1 in 2,406); of the 2,176 promoted, 1 was wrong. The gain is modest because the rule score's own merges already use about 83% of the error budget: the room for promotion is about 6 false merges plus 1 in 2,000 of the promoted pairs. The earlier "+11,000" figure used the truth of every pair to pick the threshold, which a real tenant never has. Training took 15 s on 64,374 labels (30 trees); the exported trees match scikit-learn to 1.8e-15; scoring costs 11 to 37 microseconds a pair in pure Python (the engine's own rule scoring is about 300). scikit-learn's classic boosting, the version whose trees export through a stable interface, is slow on very many labels (65,000 labels with 150 trees took over 285 s), which a real tenant's human labels will not be.

**Where it is weak, said plainly.**
* **Detection needs enough audited promotions.** The rollback acts only on 100 audited promotions. At the 2% that the rule's merges get, that would take about 5,000 promoted merges and a model promoting 2,176 pairs would never be watched; so **the promotions are audited at 10%** (`MDM_AUDIT_RATE_PROMOTED`, as chosen), which needs about 1,000 promoted merges and would give about 218 audits for the 2,176 above. A bad model can still make about a thousand wrong merges before the rollback can act on them, and a rollback does not unmerge them (a person does that, below).
* A point estimate from a few thousand human labels can be wrong; the audits and the rollback are the net, and a rollback comes after the damage it limits.
* The trainer only **proposes** by default; `--activate` makes it live, so the first one can be read in the shadow report.
* All of this is measured on synthetic data; the engines' use of the scorer is tested with a hand-built one-tree model; the Postgres versions of the new store methods have never run on a live server.
* Retraining as labels arrive is manual.

### Unmerge and the audit page

**Unmerge undoes ONE confirmed merge that a person says is wrong** (`python3 domains_cli.py unmerge CANDIDATE --domain customer --tenant acme --by jane [--reason ...] [--dry-run]`, or the audit page). The pair becomes a permanent "these are different" decision recorded as `unmerge:<who>`; it is also a training label of *different*, and it overrides an earlier audit verdict of "correct" on the same pair.
* **How it splits.** It follows the entity's OTHER confirmed pairs from each of the two records. If they still reach each other (A joined to B through C), it **refuses and names the pairs on that path**: a person has to say which merge is the wrong one. Otherwise the side holding the **oldest record keeps the entity id** (systems downstream hold entity ids) and the other side moves to a new entity; both golden records are rebuilt, and the new entity's history says who unmerged it and why. Records that no confirmed pair reaches from either side stay where they are. Entities merged into this one earlier stay tombstones: nothing is resurrected.
* **Interrupted halfway, repeat it.** The steps run so that a crash leaves a state the same call finishes (records moved, both golden records durably marked stale, then the separation recorded); `process-pending` also rebuilds the stale ones.
* **Kept true by the regular engine.** It never merges two entities when a person separated records between them (the pair goes to review with the reason "a person separated records in these two entities"), never re-proposes a separated pair when one of its records changes, and refuses a steward's *confirm* of a pair that would reunite them. The check costs one tiny query per record, and only a tenant that has unmerged something pays for more.
* **Kept true by the bulk, onboarding and delta engines too**, inside their clustering. Each component of the union-find carries the existing entities it holds; an automatic merge that would join two components with a separation between them is **held for review** (reason: "a person separated records in these two entities") instead, and a pair that IS a separated pair, met again by a changed record, is **dropped**, not proposed. Edges are taken strongest first, ties by position, so a conflict resolves the same way on every run and for any number of workers (a set load has no arrival order to decide it by). The result reports `separations_held` and `separations_dropped`, and its auto/review counts say what was written. Nothing changes, and nothing extra is paid, for a tenant where nobody has unmerged anything. **In a real conflict the regular and the bulk engines may hold back DIFFERENT edges** (the regular engine decides by arrival order); both uphold every separation, which is what is tested, and without a conflict they agree as before. `python3 domains_cli.py separation-check customer --tenant acme` stays as the check that every separation still holds (exit code 3 if not).
* A flagged (audited) merge that is unmerged is recorded as a **wrong** verdict by the person who unmerged it, which is accurate and keeps the false-rate estimate honest; a merge nobody flagged can be unmerged but does not enter the sample.

**The audit page** (the "Audit" panel of the console, under the review queue) shows a random sample of automatic merges, **the model's promotions first**, with how each was merged and both records (masked fields stay masked), and the buttons *Correct*, *Wrong (only record it)* and *Wrong: unmerge*, which says how many records would move out, or why it cannot be unmerged on its own. Above them: how much has been checked for each kind of merge, the plausible range of its error rate, and whether the model is live or rolled back. The person recorded is the authenticated key's label, never a name sent by the browser. Endpoints (role `data_steward` and up): `GET /v1/{domain}/audits`, `POST /v1/{domain}/audits/{candidate_id}/verdict`, `POST /v1/{domain}/merges/{candidate_id}/unmerge` (`dry_run` available); `GET /v1/{domain}/scorer` to view; `POST /v1/{domain}/scorer/check` and `/scorer/reset` need `domain_admin` or `tenant_admin`.

**Tested** (`tests/test_unmerge.py`, 20 tests, every one of 12 deliberate breakages caught; `tests/test_separations_bulk.py`, 11 tests for the onboarding and delta engines, 10 of 10 breakages caught, plus a two-new-records bridge that only the set bookkeeping can stop; the same lifecycle in the product and vendor domains in `tests/test_separations_domains.py`, and by hand in the config-defined `person` domain; `tests/test_stewardship.py`, 13 tests; the page's rendering in `tests/test_ui_escaping.py`, against hostile data, 5 breakages caught). **NOT tested here:** the web endpoints themselves and the page in a browser (FastAPI is not installed in the environment this was built in, as for every endpoint of this project: the logic behind each is a plain module that is tested, the endpoint is a few lines around it), and the Postgres versions of the new store methods.

**The audit page's cost does not grow with the audits.** The counts, the wrong pairs and the queue are computed in SQL, and the queue reads only a page of merges (40,000 flagged audits: the summary takes about 125 ms and the queue about 90 ms; counting every automatic merge is the one cost that still grows, so the page uses a total up to 30 seconds old, `MDM_SUMMARY_CACHE_SECONDS`, while the audit counts a verdict changes are never cached and the rollback check never uses the cache). **Upgrading (migration 015):** adds `merge_audits.made_by`, who MADE each audited merge (the rule score or the learned scorer), recorded when the merge is flagged and filled in for existing audits from what their candidates say now. It is needed because unmerging rewrites a candidate's `decided_by`: attributing audits by it counted a promotion a person unmerged against the RULE score instead of the model, which is the evidence the rollback exists to see.

**Tested too:** `tests/test_audit_sql.py` (10 tests: the SQL agrees with a plain tally on random data, the maker survives an unmerge, the migration backfills and can run twice, the page never reads the whole table), `tests/test_hardening.py` (20: authentication fails closed, names come from keys, domain scope, limits, the rate limiter) and `tests/test_api_wiring.py` (27: the endpoint functions called with real keys and a real database through `tests/fake_web.py`, a stand-in for FastAPI; every one of 15 deliberate breakages of the wiring is caught).

## Multi-domain API (generic across Customer/Product/Vendor)

```
POST   /v1/{domain}/records
GET    /v1/{domain}/entities
GET    /v1/{domain}/entities/{entity_id}
GET    /v1/{domain}/entities/{entity_id}/lineage
DELETE /v1/{domain}/entities/{entity_id}          # purge (tenant_admin only)
GET    /v1/{domain}/review-queue
POST   /v1/{domain}/review-queue/{candidate_id}/resolve
GET    /v1/{domain}/schema
GET    /v1/domains
POST   /v1/admin/api-keys                          # tenant_admin only
GET    /v1/admin/api-keys
DELETE /v1/admin/api-keys/{key_id}
GET    /v1/admin/audit-log
GET    /v1/admin/usage
GET    /healthz                                   # the only endpoint that needs no key; says {"status": "ok"} and nothing else
```

### Pages: the review queue and the entity list

Both lists are paged, and **their response is an envelope, not a bare list** (a change from earlier versions; a client that silently received only the first page would be worse off than one that is told there is more):

```
GET /v1/{domain}/review-queue?limit=50&cursor=...   ->  {"items": [...], "next_cursor": "...", "limit": 50, "total": 1234}
GET /v1/{domain}/entities?status=active&limit=100&cursor=...  ->  {"items": [...], "next_cursor": "...", "limit": 100}
```

* `limit` defaults to 50 for the queue (at most 200) and 100 for entities (at most 1000); anything else is clamped. `next_cursor` is `null` on the last page; pass it back as `cursor` for the next. `total` (queue only) is how many pairs are waiting in all; the entity list has no total, because counting every entity would itself scan the table.
* The queue is best score first (ties by id, descending); entities are in id order. A cursor names the last item seen by those same fields (keyset paging), so **a page never repeats or skips an item when others are decided or added between two requests**, and a page costs the same however deep it is. A cursor carries only a position, never a tenant: every query is filtered by the key's tenant whatever the cursor says. A bad or foreign-list cursor is a `400`.
* **Measured** (SQLite, 200,000 pending pairs): the old whole-queue response cost about 625 to 845 microseconds per pair (two single-record reads), so roughly 125 to 170 seconds per request (measured on 3,000 pairs, extrapolated). A page is about 1 ms for the query plus about 3 ms for its records, **the same at the first page and at depth 199,950**. The entity list went from a connection per entity to three queries per page (about 3 ms for 100, flat across 5,000 entities; deeper is pinned by the query plan, not measured). `total` is an index-only count (11 ms at 200,000) that grows with the queue.
* **Migration 016** adds the indexes that make this so: a *partial* index over the pending pairs only (`tenant, domain, score DESC, id DESC WHERE status = 'pending'`, so it costs a bulk load's millions of automatic merges nothing, and a decided pair leaves it), and `(tenant, entity id)` on every golden-record table. The cursor condition is one row-value comparison, `(score, id) < (?, ?)`, because an `OR` of two conditions cannot be a single index seek and made deep pages cost more the deeper they were (29 ms at the end against 0.9 ms; a test pins the plan).
* The console shows a page with "Show more". Its 8-second refresh leaves an expanded list alone; a decision or a change of domain starts again from the first page.
* **Not paged, and bounded already:** the audit page (at most 200 per request), the audit log (200 entries), pipeline runs (200). **Tested:** `tests/test_paging.py` (5), `tests/test_pagination.py` (17: exact with many ties, stable under decisions made between pages, tenant isolation, the real queries' plans, a constant number of connections per page, the migration) and the endpoint tests in `tests/test_api_wiring.py`. 14 of 15 deliberate breakages caught; the 15th changes nothing on SQLite 3.45. **Not run:** the Postgres versions of these queries and of migration 016.

## Running the tests

```bash
python3 -m unittest discover -s tests -v
```

## Validating everything the build sandbox couldn't

```bash
python3 validate_setup.py
```

Runs through every ⚠️/❌ item in the status table above, on your actual
machine, and prints a clear PASS/FAIL/SKIP report:

1. Environment (Python version, dependencies)
2. Unit tests (re-confirms the 55 tests already proven in the build sandbox)
3. **FastAPI HTTP layer** — starts a real `uvicorn` server, hits real endpoints, tears it down
4. **RBAC over HTTP** — confirms 401 with no key, 200 with a valid key, 403 for a role that shouldn't be allowed to write
5. **Postgres backend** — only if `TEST_DATABASE_URL` is set (SKIPs otherwise, doesn't fail)
6. **Real LLM adjudication** — only if `ANTHROPIC_API_KEY` is set
7. **Real semantic embeddings** — only if `sentence-transformers` is installed

Sections 5–7 SKIP rather than FAIL by default, since they need optional
setup you may not have done yet — pass `--all` once everything's
provisioned (e.g. in CI) to make "not set up" a hard failure instead.

```bash
python3 validate_api.py                  # starts its own isolated servers (secure mode) and tests them over HTTP
python3 validate_api.py --base http://localhost:8000 --admin-key mdm_... --confirm-writes   # or a server you run (it WRITES: use a scratch tenant)
```

`validate_api.py` checks the security and limits work **over real HTTP**, which could not be done where this was built (the endpoint logic is unit-tested by calling the functions; this checks the web layer applies it): nothing but `/healthz` answers without a key (401 with no key, with a tenant header, with a wrong key); a read-only key gets 403 (not 500) for what it may not do; a key limited to one domain sees only that domain, its schema, its entities and its slice of the audit log; a key labelled `system:...` is refused; a steward's decision is recorded under the key's name even when the request claims another; an audit verdict and an unmerge work and are signed by the key; the two long lists come back as paged envelopes, walking them a page at a time yields every item exactly once in the same order as one big page, a huge `limit` is clamped and a bad cursor is a 400; oversize batches, records and bodies are refused (422/413); and, in the servers it starts itself, the rate limit gives 429 with `Retry-After`, `MDM_DEV_MODE=1` opens the API and the retired `MDM_ENABLE_AUTH=false` does not. `tests/test_validate_api.py` proves the script's checks can fail (a wide-open stand-in server makes it report failures; a locked one lets the no-key checks pass). **Run it once before exposing anything: it is the only test of this layer against the real framework.**

Each server-starting section spins up an isolated SQLite file
(`validate_test.db`, `validate_rbac_test.db`) and cleans it up
afterward — your real `mdm.db` is never touched.

## Known gaps / honest limitations (cumulative)

- **The active-learning loop's labels are now human-only**, and a learned scorer can promote pairs out of review under policy B (see "The learned scorer"), but the rule score's own cutoffs are still fixed constants, nothing retrains the scorer as labels arrive, and the LLM adjudication path has never run against the real API.

- **The bulk engine's write stage is timed at 20,000 people only** (5.3 s); at 1M it is an estimate (about 6 minutes, about 20 minutes for the whole load). Its write stage is all-or-nothing, and it is single-machine. Onboarding (new source onto existing data) exists, but its merge path has only been exercised by tests and generated bridge scenarios, not at scale.
- **Delta loads have no deletes and no drift report**, and the cost of a record **grows with the data held** (measured at 500,000 onto 500,000: about 30 minutes on one slow core; pairs and write are 71% of it). Larger bases are unmeasured.
- **The review queue is too big and mostly wrong at scale, and it gets worse with size:** review precision 90.7% at 20,000 people but 18.9% at 1M, and review pairs 1.4% of records at 20k but 9% at 1M (89,990 review pairs, so about 9 million at 100M if only linear, probably more). The thresholds need tuning on a population like this.
- **SSN is protected; DOB is not.** The SSN is a keyed hash for matching and a mask everywhere but raw, but raw and superseded payloads still hold it in plain text (by design: option A), and old backups from before protection was switched on do too. DOB stays in plain text because matching needs the real date. One hashing key serves the deployment.
- **Scoring gaps (closed by step 4, with limits):** a swapped, year-off-by-one or year-only DOB is now a near match or an agreement instead of a contradiction, and a one-digit SSN typo or adjacent swap is near-equal (only for a protected SSN with `near`). Not recognised: two typos in an SSN, a DOB wrong in a way that is not a swap or a one-year slip, and any near-equality for an SSN stored unprotected.
- **Blocking at 100M is not demonstrated.** The shipped keys meet the step 4 gate at 1M (recall 99.9%, 1.5 pairs per record) and extrapolate to about 11 pairs per record at 10M, but 10M was not run, and at 100M the same exponent gives about 86 pairs per record, above the 25 that fits a 12-hour run. `whole surname + whole first name` is the key still growing roughly linearly.
- **Scale numbers are from synthetic data on one core**; a real sample is needed before trusting any absolute figure.
- **The reference matcher** does not cover the learned model or LLM adjudication, and reports "not comparable" when a key crosses the cap mid-load.
- **No scheduler or worker.** Records accepted with `mode=batch`/`--defer`, and failed ones, wait until something runs `domains_cli.py process-pending`. See *From raw to golden*.
- **`most_complete` survivorship is the same as `most_recent`**: both take the newest non-empty value (it says so in `survivorship.py`'s own docstring). The name suggests "the longest / most informative value"; that is not built. Choose `most_recent` where you mean recency.
- **Raw ingest is idempotent, but old duplicates and splits remain open**: duplicates stored before idempotency are reported, not removed; and a changed record can add merges but never undo one.
- **Typing:** SQLite `numeric` is not exact (use Postgres for money); no decimal-comma locale; naive timestamps are assumed UTC; typed columns exist only in the golden layer.
- **Matching:** see *Still wrong or unbuilt* under *How matching weighs evidence*. In short: no frequency-based weighting (Fellegi-Sunter-style weights learned from your own data), Soundex only, no automatic runtime placeholder detection, no re-match pass, product/vendor unmeasured.
- **Resolved this round:** customer-editable domain config with a stale-index guard and reindex job; and an XSS hole in the console. **Still open on this track:** a re-match pass to re-resolve existing records under new rules, and rebuilding existing golden records after a survivorship change (see *What this does NOT do* above).
- **Resolved this round:** bulk write batching (`ingest_batch`, with a real correctness bug caught and fixed along the way), JSONB storage for Postgres, and RLS policies as a second isolation layer.
- **Resolved earlier:** the O(n²) full-table-rescan-per-ingest problem (indexed blocking table) and the Postgres per-query-connection bug (persistent connection + RLock) — both real, measured problems, not theoretical.
- **RLS is applied but not security-validated** — see the RLS section above. This is genuinely different from "written but not run" (true of several other pieces here): RLS's actual guarantee can only be confirmed against a live server with two real database roles, which no amount of mocking substitutes for. Treat it as "the mechanism is in place," not "cross-tenant isolation is confirmed at the database level," until you've run the manual two-role check yourself.
- Indexed blocking still degrades on low-diversity data where blocking keys collide across a large fraction of the dataset — inherent to blocking-based ER, not a remaining index bug (see the Performance section above).
- `ingest_batch()` only batches the write side; matching itself is still per-record by necessity (see the Bulk batching section) — a genuinely different, offline clustering algorithm would be needed to batch matching itself, and isn't built here.
- Rejecting a match candidate doesn't immediately spin the still-unclustered record into its own entity in the same call.
- Active learning has no train/test split or drift monitoring, and doesn't get retroactively updated when its training data is later purged (right-to-be-forgotten deletes the source records, but a model already trained on them keeps their influence in its weights until the next retrain happens to exclude them).
- `EMBEDDING_PROVIDER=sentence_transformers`, `ENABLE_LLM_ADJUDICATION=true`, Postgres, and the full `api.py` HTTP layer are real code paths never run against live network/services in this sandbox.
- No connection pooling for Postgres — the single persistent connection (see the connection-reuse fix) serializes all DB access within one process via a lock; fine for an MVP's request volume, not for high concurrent throughput.
- Schema migrations are forward-only (no down-migrations), and their Postgres SQL has not been executed against a live server — see the Schema migrations section. The backfill (005) runs in one transaction, which is long on a very large table.
- N+1 query patterns remain in a few places (e.g. `list_entities()` calling `get_entity()` once per row) — not addressed this round.
- LLM adjudication calls aren't separately usage-metered yet.
- Purge doesn't clean up dangling references in other entities' lineage history.
- Salesforce/Shopify connectors are documented scaffolds, not working integrations.
- No SSO (Auth0/Ory/Keycloak) — API keys are the real, working auth mechanism for now; SSO would be a second way to resolve a `Principal` alongside API keys, not a replacement.

## Suggested next steps

1. **Validate RLS for real** — the two-role, live-server security check described in the RLS section. This is now the single highest-value validation gap in the whole project, more so than the other ⚠️ items, since it's the one where a mock genuinely cannot substitute for a real check.
2. Validate the rest of what's flagged ⚠️/❌ above (real LLM calls, Postgres end-to-end, the FastAPI layer itself).
3. Connection pooling for Postgres, if concurrent request volume becomes real.
4. ~~Schema migrations~~, ~~customer-editable domain config + reindex~~, ~~matching evidence + benchmark~~, ~~staging layer~~, ~~golden records as real columns~~, ~~idempotent raw ingest~~, ~~typed golden columns~~, ~~processing state + realtime/batch~~, ~~one entity per record + reference matcher~~, ~~person benchmark + scale harness~~ — done. Next, in order: ~~(3) cleansing, profiling and SSN protection~~ done (DOB is not hashed; to stop keeping the plain SSN in raw, option B would store it encrypted); ~~(4) scalable blocking~~ done at 1M (recall 99.9%, 1.5 pairs per record); **10M is extrapolated and 100M is not yet shown to fit**, so the next measurement is the 10M run on a bigger machine and, if it holds, key tuning for the roughly-linear `surname + first name` key; ~~(6) onboarding a new source onto existing data~~ built and tested against the regular engine (see Onboarding); ~~delta loads of a source already loaded~~ built the same way (see Delta), without deletes; ~~(5) a bulk pair-based engine~~ built, passing `benchmark.py equivalence`, about 100 times faster than the regular engine on the same 20,000 people with identical results, and dry-run at 1M (1,210 records/s on one core); **still to measure: its write stage at 1M**, and the review volume (which worsens with scale) needs a threshold policy; a Spark executor only if the numbers need it; **(6) onboarding mode** (new source against existing data). Then: `most_complete` survivorship, unmerge/split, Postgres tests on a real server, a Dockerfile, a least-privilege role split.
5. Real Stripe billing integration.
6. A real Salesforce or Shopify connector, built against an actual account.
7. Extend usage metering to LLM adjudication calls.

[multi-domain MDM architecture doc]: ./mdm-saas-architecture.md
