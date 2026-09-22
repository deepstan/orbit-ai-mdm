# Multi-Domain MDM — Phase 1 + Phase 2 MVP

A master data management prototype spanning **three domains — Customer,
Product, and Vendor** — sharing one engine: ingestion → standardization →
AI-assisted matching (string + semantic + optional LLM adjudication) →
human-in-the-loop review → golden record with full lineage → active
learning from steward decisions, all with per-tenant AND per-domain
isolation.

This implements Phase 1, Phase 2, and the Phase 3 roadmap item "prove the
entity envelope is genuinely domain-agnostic" from the [multi-domain MDM
architecture doc] — the same engine now runs three domains without any
domain-specific code in the matching, survivorship, or pipeline layers.

## The domain registry — how multi-domain actually works

Every domain-specific decision lives in **one place**: `app/domains.py`.
A `DomainConfig` ties together a schema, a standardizer, which fields are
deterministic identity keys, fuzzy-match weights, which fields feed
semantic similarity, blocking strategy, and survivorship rules:

```python
PRODUCT_CONFIG = DomainConfig(
    name="product",
    schema_fields=PRODUCT_SCHEMA_FIELDS,
    standardizer=standardize_product,
    deterministic_fields=["sku", "upc"],
    fuzzy_weights={"brand": 0.20, "category": 0.15, "description": 0.10},
    semantic_fields=["brand", "product_name"],
    semantic_weight=0.35,
    blocking_field_groups=[["sku"], ["upc"], ["brand", "category"]],
    field_strategies={...},
)
```

`matching.py`, `survivorship.py`, `active_learning.py`, and `pipeline.py`
contain **zero references** to "first_name" or "sku" by name — they're
driven entirely by whichever `DomainConfig` is passed in. That's what let
Product and Vendor plug in as config additions rather than engine rewrites.

**Adding a 4th domain** means: add a schema field list to `models.py`, a
`standardize_X()` function to `standardize.py`, and a `DomainConfig` entry
to `domains.py`. Nothing else changes.

## Isolation: two independent axes

Every table and query is scoped by **both** `tenant_id` and `domain`.
Matching never crosses either boundary — proven with dedicated tests, not
just assumed:

- `test_identical_records_in_different_tenants_never_match_each_other` — byte-identical customer data across two tenants never merges.
- `test_identical_looking_data_never_matches_across_domains` — a customer record and a vendor record with colliding field values (same "company name", same email) never merge, because there's no shared candidate pool between domains.
- `test_entity_lookup_respects_domain_boundary` / `test_entity_lookup_respects_tenant_boundary` — fetching an entity ID under the wrong domain or tenant scope returns nothing, even though the ID technically exists in the database.

## What's implemented

**Phase 1 (core pipeline)** — standardization, blocking, deterministic +
fuzzy matching, confidence-band routing, configurable survivorship, full
lineage, stewardship UI, REST API.

**Phase 2**
- **Semantic embeddings** (`app/embeddings.py`) — pure-Python n-gram hash fallback (zero deps), pluggable to real `sentence-transformers`.
- **LLM adjudication** (`app/llm_adjudication.py`) — optional Anthropic API call for the ambiguous score band, domain-aware prompt, fails safe to the review queue.
- **Multi-tenancy** — every table/query scoped by `tenant_id`.
- **Active learning** (`app/active_learning.py`, `retrain.py`) — per-(tenant, domain) logistic regression trained from steward decisions, with domain-appropriate feature vectors (a product model's features aren't shaped like a customer model's).
- **Product and Vendor domains** — full parity with Customer: matching, survivorship, lineage, review queue, active learning, all working through the same domain-agnostic engine.

## ⚠️ Tested live vs. needs your machine

Built in a sandbox with **no network access** — this shaped what could be verified directly:

| Feature | Status |
|---|---|
| Standardization, matching, blocking, survivorship, lineage — all 3 domains | ✅ Fully tested (26 unit tests) |
| Cross-tenant AND cross-domain isolation | ✅ Fully tested, dedicated tests for each |
| Semantic embeddings (n-gram hash fallback) | ✅ Fully tested, all 3 domains |
| Active learning (training, persistence, blending, per-domain isolation) | ✅ Fully tested |
| LLM adjudication integration logic (routing, fallback, domain-aware prompts) | ✅ Fully tested, actual API call mocked |
| LLM adjudication **real API call** | ⚠️ Not tested — no network here |
| FastAPI HTTP layer (`api.py`) | ⚠️ Not tested — FastAPI isn't installable in this sandbox |
| `sentence-transformers` real embeddings | ⚠️ Not tested — optional upgrade path |
| Real PostgreSQL | ⚠️ Not tested — still SQLite; the tenant+domain isolation *pattern* is proven, the engine swap isn't |

Run `python3 -m unittest discover -s tests -v` — 26 tests pass with zero setup.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 seed_demo.py            # seeds all 3 domains, exercises every matching path + isolation
uvicorn app.api:app --reload --port 8000
```

Open **http://localhost:8000/** — the stewardship console now has **Customer / Product / Vendor tabs**. Or **http://localhost:8000/docs** for the API.

## API surface (generic across domains)

```
POST /v1/{domain}/records
GET  /v1/{domain}/entities
GET  /v1/{domain}/entities/{entity_id}
GET  /v1/{domain}/entities/{entity_id}/lineage
GET  /v1/{domain}/review-queue
POST /v1/{domain}/review-queue/{candidate_id}/resolve
GET  /v1/{domain}/schema
GET  /v1/domains                  # lists all registered domains + their schemas
```

`{domain}` is `customer`, `product`, or `vendor` — an unknown domain returns 404, not a silent fallback.

```bash
# Product domain example
curl -X POST http://localhost:8000/v1/product/records \
  -H "Content-Type: application/json" \
  -d '{"source_system":"erp","source_record_id":"SKU-1","attributes":{"sku":"ABC-123","product_name":"Widget Pro","brand":"Acme"}}'

curl http://localhost:8000/v1/product/review-queue

# Vendor domain example
curl -X POST http://localhost:8000/v1/vendor/records \
  -H "Content-Type: application/json" -H "X-Tenant-Id: acme_corp" \
  -d '{"source_system":"ap_system","source_record_id":"V1","attributes":{"company_name":"Acme Supply","tax_id":"12-3456789"}}'
```

## Active learning per domain

```bash
python3 retrain.py --tenant acme_corp --domain product
python3 retrain.py --tenant acme_corp --domain vendor
```

Models are saved to `models/<tenant>__<domain>_match_model.json` — isolated per (tenant, domain) pair, since a product-domain model's feature vector isn't even the same shape as a customer-domain model's (see `feature_names()` in `app/active_learning.py`).

## Known gaps / honest limitations

- Rejecting a match candidate doesn't immediately spin the still-unclustered record into its own entity in the same call.
- Active learning models have no train/test split or drift monitoring — good enough to prove the loop closes, not production ML ops.
- `EMBEDDING_PROVIDER=sentence_transformers` and `ENABLE_LLM_ADJUDICATION=true` are real, complete code paths, neither run against a live model/API from this sandbox.
- Blocking key prefix length (3 chars) is a reasonable default but untuned against real duplicate-pair data for any domain — tune per your actual data before trusting recall at scale.
- Postgres migration is still a documented intention in `app/db.py`'s docstring, not built.

## Suggested next steps

1. Validate the untested paths above on your machine.
2. Swap SQLite → PostgreSQL (tenant+domain scoping pattern already transfers).
3. Add train/test split and basic precision/recall metrics to active learning before trusting it in production.
4. Load-test blocking against realistic volume per domain.
5. A 4th domain (Location? Employee?) as a config-only addition, to further stress-test the registry pattern.

[multi-domain MDM architecture doc]: ./mdm-saas-architecture.md
