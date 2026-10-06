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
| Console UI: output escaping, dynamic domain tabs | ✅ Escaping tested in Node against hostile input · ⚠️ not exercised in a real browser |
| Postgres backend (incl. Phase 3 tables) | ⚠️ Written, not run — see the Postgres section |
| **Row-Level Security (RLS)** | ⚠️⚠️ **The least-verified piece in this project** — SQL generation and the `SET LOCAL` mechanism are tested; actual cross-tenant denial by a live Postgres server has NOT been confirmed. See the RLS section — this needs two real database roles to validate meaningfully, which no mock can substitute for |
| **Salesforce / Shopify connectors** | ❌ **Not implemented** — scaffolds only. Real integrations need live API credentials this sandbox can't reach; unlike the LLM/Postgres code, there's no stable public contract to write against without a real account, so nothing was written to "validate later" here |

Run `python3 -m unittest discover -s tests -v` — 147 tests pass, 13 skip (Postgres-only).

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 seed_demo.py
uvicorn app.api:app --reload --port 8000
```

By default, **auth is OFF** (`MDM_ENABLE_AUTH` unset) — the API and stewardship UI work exactly as before, full access, tenant from the `X-Tenant-Id` header. This is "dev mode," clearly labeled as such everywhere it appears (mirrors how LLM adjudication defaults off) — real functionality is opt-in, not silently broken by default.

## Turning on RBAC for real

```bash
# 1. Bootstrap a tenant_admin key (do this BEFORE enabling auth, or you'll lock yourself out)
python3 manage_api_keys.py create --tenant acme --role tenant_admin --label "root key"
#    -> copy the printed key

# 2. Create scoped keys for other roles as needed
python3 manage_api_keys.py create --tenant acme --role data_steward --label "Jane" --domains customer,product
python3 manage_api_keys.py create --tenant acme --role api_consumer --label "BI dashboard read-only"

# 3. Turn on enforcement
export MDM_ENABLE_AUTH=true
uvicorn app.api:app --reload --port 8000

# 4. Every request now needs the header
curl -H "Authorization: Bearer mdm_..." http://localhost:8000/v1/customer/entities
```

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

## Customer-defined domains

A domain is described by a config file, not code. You can define a new one (Location, Policy, Claim, ...) or change a built-in, with **no code change and no database change** — records are stored as JSON and every table is keyed by a `domain` string.

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
| `blocking` groups | **Yes** |
| How a field *used in blocking* is standardized | **Yes** |
| The blocking key layout in `matching.py` (prefix length / format) | **Yes** — detected automatically |
| Fuzzy weights, semantic settings, deterministic fields, survivorship | No — take effect on restart |
| Standardization of a field *not* used in blocking | No |

A reindex pauses ingestion into that domain while it runs, and **stays paused if interrupted** so a half-rebuilt index can never be used; re-run it to finish. Existing databases upgrade cleanly: migration 006 stamps the built-in fingerprint on domains that already hold data, so upgrading doesn't force a pointless reindex — but an override already in place at upgrade time is correctly flagged stale.

### What this does NOT do (read before relying on it)

- **A reindex rebuilds the candidate index only. It does not re-run matching over existing records.** Matching is incremental — each record is compared once, when it arrives — so duplicates your new rules would have caught earlier stay unmerged until a new record matches them. Re-resolving history is a separate job and isn't built.
- **Existing golden records aren't rebuilt** when you change survivorship or add fields; each updates the next time that entity changes.
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
```

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

Each server-starting section spins up an isolated SQLite file
(`validate_test.db`, `validate_rbac_test.db`) and cleans it up
afterward — your real `mdm.db` is never touched.

## Known gaps / honest limitations (cumulative)

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
4. ~~Schema migrations~~ and ~~customer-editable domain config + reindex~~ — done. Next: run `migrate.py upgrade` and the Postgres tests against a real Postgres; a re-match pass that re-resolves existing records after a config change; a Dockerfile / one-command deployment; a least-privilege role split with append-only grants on `audit_log`.
5. Real Stripe billing integration.
6. A real Salesforce or Shopify connector, built against an actual account.
7. Extend usage metering to LLM adjudication calls.

[multi-domain MDM architecture doc]: ./mdm-saas-architecture.md
