"""
FastAPI HTTP layer. Thin wrapper around app/pipeline.py -- all matching/
survivorship logic lives in the dependency-free modules so it's testable
without a server running.

Routes are generic across domains: /v1/{domain}/records, /v1/{domain}/entities,
etc. -- matching the architecture doc's indicative API surface
(Section 8: `POST /v1/{domain}/records`). `domain` is validated against
the registry in app/domains.py; an unknown domain returns 404, not a
silent fallback.

Multi-tenancy: every request is scoped by an `X-Tenant-Id` header,
defaulting to "default" if omitted. See the production note below.

Run with:
    pip install -r requirements.txt
    uvicorn app.api:app --reload --port 8000
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import Store
from .domains import REGISTRY, get_domain_config
from .models import DEFAULT_TENANT
from .pipeline import ingest_record, resolve_candidate

app = FastAPI(
    title="Multi-Domain MDM — Phase 1+2 MVP",
    description="Customer / Product / Vendor MDM API with AI-assisted matching, "
                 "semantic similarity, optional LLM adjudication, tenant isolation, "
                 "and active learning from steward decisions.",
    version="0.3.0",
)

store = Store("mdm.db")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "MDM API is running. See /docs."}


def _validate_domain(domain: str) -> None:
    if domain not in REGISTRY:
        raise HTTPException(404, f"Unknown domain '{domain}'. Known domains: {list(REGISTRY)}")


# NOTE on `tenant_id: str = Header(..., alias="X-Tenant-Id")` throughout:
# in production, never trust a client-supplied tenant header directly --
# resolve tenant_id server-side from the authenticated session/API key
# instead. This header-based approach is intentionally simplified for
# local MVP testing.

# ---------------------------------------------------------------------------
# Request/response schemas
# ---------------------------------------------------------------------------
class IngestRequest(BaseModel):
    source_system: str = Field(..., examples=["salesforce", "shopify", "erp", "csv_import"])
    source_record_id: str
    attributes: dict[str, Any] = Field(..., description="Domain-specific attributes; unknown keys allowed.")


class ResolveCandidateRequest(BaseModel):
    decision: str = Field(..., pattern="^(confirm|reject)$")
    decided_by: str = Field(..., examples=["steward:jane"])


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
@app.get("/v1/domains", tags=["meta"])
def list_domains():
    return [{"domain": name, "fields": cfg.schema_fields} for name, cfg in REGISTRY.items()]


@app.get("/v1/{domain}/schema", tags=["meta"])
def schema(domain: str):
    _validate_domain(domain)
    return {"domain": domain, "fields": get_domain_config(domain).schema_fields}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
@app.post("/v1/{domain}/records", tags=["ingestion"])
def ingest(domain: str, req: IngestRequest, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    return ingest_record(store, tenant_id, domain, req.source_system, req.source_record_id, req.attributes)


# ---------------------------------------------------------------------------
# Golden records
# ---------------------------------------------------------------------------
@app.get("/v1/{domain}/entities", tags=["entities"])
def list_entities(domain: str, status: Optional[str] = None,
                   tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    return [
        {"entity_id": e.id, "status": e.status, "attributes": e.attributes,
         "source_record_ids": e.source_record_ids, "updated_at": e.updated_at}
        for e in store.list_entities(tenant_id, domain, status=status)
    ]


@app.get("/v1/{domain}/entities/{entity_id}", tags=["entities"])
def get_entity(domain: str, entity_id: str, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    e = store.get_entity(entity_id, tenant_id, domain)
    if not e:
        raise HTTPException(404, "entity not found")
    sources = [
        {"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id,
         "attributes": r.attributes, "ingested_at": r.ingested_at}
        for r in store.list_source_records(tenant_id, domain, entity_id=entity_id)
    ]
    return {"entity_id": e.id, "status": e.status, "attributes": e.attributes,
            "updated_at": e.updated_at, "source_records": sources}


@app.get("/v1/{domain}/entities/{entity_id}/lineage", tags=["entities"])
def get_lineage(domain: str, entity_id: str, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    if not store.get_entity(entity_id, tenant_id, domain):
        raise HTTPException(404, "entity not found")
    return [
        {"field": ev.field, "old_value": ev.old_value, "new_value": ev.new_value,
         "source_record_id": ev.source_record_id, "reason": ev.reason, "changed_at": ev.changed_at}
        for ev in store.list_lineage(entity_id, tenant_id, domain)
    ]


# ---------------------------------------------------------------------------
# Stewardship review queue
# ---------------------------------------------------------------------------
@app.get("/v1/{domain}/review-queue", tags=["stewardship"])
def review_queue(domain: str, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    candidates = store.list_candidates(tenant_id, domain, status="pending")
    out = []
    for c in candidates:
        rec_a = store.get_source_record(c.record_a_id, tenant_id, domain)
        rec_b = store.get_source_record(c.record_b_id, tenant_id, domain)
        out.append({
            "candidate_id": c.id, "score": c.score, "match_type": c.match_type, "reasons": c.reasons,
            "record_a": {"id": rec_a.id, "source_system": rec_a.source_system, "attributes": rec_a.attributes} if rec_a else None,
            "record_b": {"id": rec_b.id, "source_system": rec_b.source_system, "attributes": rec_b.attributes} if rec_b else None,
            "created_at": c.created_at,
        })
    return out


@app.post("/v1/{domain}/review-queue/{candidate_id}/resolve", tags=["stewardship"])
def resolve(domain: str, candidate_id: str, req: ResolveCandidateRequest,
            tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    _validate_domain(domain)
    try:
        return resolve_candidate(store, tenant_id, domain, candidate_id, req.decision, req.decided_by)
    except ValueError as e:
        raise HTTPException(400, str(e))
