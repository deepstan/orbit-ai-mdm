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
| **CSV connector** | ✅ Fully tested, real file I/O |
| LLM adjudication integration logic | ✅ Fully tested (API call itself mocked) |
| LLM adjudication **real API call** | ⚠️ Not tested — no network here |
| **FastAPI HTTP layer, including auth wiring** | ⚠️ **Not tested** — FastAPI isn't installable in this sandbox. The RBAC/audit/billing *logic* is tested directly (bypassing HTTP); the `api.py` wiring that calls it has not been run |
| Postgres backend (incl. new Phase 3 tables) | ⚠️ Written, not run — see the Postgres section |
| **Salesforce / Shopify connectors** | ❌ **Not implemented** — scaffolds only. Real integrations need live API credentials this sandbox can't reach; unlike the LLM/Postgres code, there's no stable public contract to write against without a real account, so nothing was written to "validate later" here |

Run `python3 -m unittest discover -s tests -v` — 48 tests pass, 7 skip (Postgres-only).

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

## Postgres (optional — SQLite is the default)

```bash
docker run --name mdm-postgres -e POSTGRES_PASSWORD=devpass -p 5432:5432 -d postgres:16
createdb -h localhost -U postgres mdm_dev
pip install psycopg2-binary
export DATABASE_URL=postgresql://postgres:devpass@localhost:5432/mdm_dev
python3 seed_demo.py
```

`app/db_postgres.py` now includes the Phase 3 tables (`api_keys`, `audit_log`, `usage_events`) with the same interface as SQLite's `Store` — written but **not run** here (no reachable Postgres server). Validate with:

```bash
export TEST_DATABASE_URL=postgresql://postgres:devpass@localhost:5432/mdm_dev
python3 -m unittest tests.test_postgres_backend -v
```

**Not yet built:** Postgres row-level security policies as a second isolation layer beneath the application-level `tenant_id` + `domain` filtering.

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

## Known gaps / honest limitations (cumulative)

- Rejecting a match candidate doesn't immediately spin the still-unclustered record into its own entity in the same call.
- Active learning has no train/test split or drift monitoring.
- `EMBEDDING_PROVIDER=sentence_transformers`, `ENABLE_LLM_ADJUDICATION=true`, Postgres, and the full `api.py` HTTP layer are real code paths never run against live network/services in this sandbox.
- Postgres RLS (defense-in-depth beneath application-level tenant/domain filtering) not built.
- LLM adjudication calls aren't separately usage-metered yet.
- Purge doesn't clean up dangling references in other entities' lineage history.
- Salesforce/Shopify connectors are documented scaffolds, not working integrations.
- No SSO (Auth0/Ory/Keycloak) — API keys are the real, working auth mechanism for now; SSO would be a second way to resolve a `Principal` alongside API keys, not a replacement.

## Suggested next steps

1. Validate everything flagged ⚠️/❌ above on your own machine — this is genuinely the highest-value next step, since it's the largest block of "written but unverified" code accumulated so far.
2. Postgres RLS policies.
3. Real Stripe billing integration.
4. A real Salesforce or Shopify connector, built against an actual account.
5. Extend usage metering to LLM adjudication calls.

[multi-domain MDM architecture doc]: ./mdm-saas-architecture.md
