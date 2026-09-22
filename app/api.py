"""
FastAPI HTTP layer. Thin wrapper around app/pipeline.py -- all matching/
survivorship logic lives in the dependency-free modules so it's testable
without a server running.

Multi-tenancy: every request is scoped by an `X-Tenant-Id` header. If
omitted, requests fall back to the "default" tenant, so existing demo
flows/tests keep working without every curl command needing the header --
but in a real deployment this would come from authenticated session
context (e.g. resolved from an API key or JWT), never trusted as a raw
client-supplied header. That's flagged again below at the dependency.

Run with:
    pip install -r requirements.txt
    uvicorn app.api:app --reload --port 8000
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import Store
from .models import CUSTOMER_SCHEMA_FIELDS, DEFAULT_TENANT
from .pipeline import ingest_record, resolve_candidate

app = FastAPI(
    title="Customer MDM — Phase 1+2 MVP",
    description="Single-domain (Customer) MDM API with AI-assisted matching, "
                 "semantic similarity, optional LLM adjudication, and tenant isolation.",
    version="0.2.0",
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
    return {"message": "Customer MDM API is running. See /docs."}


# NOTE on the `tenant_id: str = Header(..., alias="X-Tenant-Id")` params
# used throughout below: in production, never trust a client-supplied
# tenant header directly -- resolve tenant_id server-side from the
# authenticated session/API key instead, so one tenant can't simply set a
# different header value and read another tenant's data. This header-based
# approach is intentionally simplified for local MVP testing.

# ---------------------------------------------------------------------------
# Request/response schemas
# ---------------------------------------------------------------------------
class CustomerAttributes(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address_line1: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

    class Config:
        extra = "allow"


class IngestRequest(BaseModel):
    source_system: str = Field(..., examples=["salesforce", "shopify", "csv_import"])
    source_record_id: str
    attributes: CustomerAttributes


class ResolveCandidateRequest(BaseModel):
    decision: str = Field(..., pattern="^(confirm|reject)$")
    decided_by: str = Field(..., examples=["steward:jane"])


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
@app.post("/v1/customer/records", tags=["ingestion"])
def ingest(req: IngestRequest, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    result = ingest_record(
        store, tenant_id, req.source_system, req.source_record_id, req.attributes.model_dump(exclude_none=True)
    )
    return result


# ---------------------------------------------------------------------------
# Golden records
# ---------------------------------------------------------------------------
@app.get("/v1/customer/entities", tags=["entities"])
def list_entities(status: Optional[str] = None, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    return [
        {
            "entity_id": e.id, "status": e.status, "attributes": e.attributes,
            "source_record_ids": e.source_record_ids, "updated_at": e.updated_at,
        }
        for e in store.list_entities(tenant_id, status=status)
    ]


@app.get("/v1/customer/entities/{entity_id}", tags=["entities"])
def get_entity(entity_id: str, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    e = store.get_entity(entity_id, tenant_id)
    if not e:
        raise HTTPException(404, "entity not found")
    sources = [
        {"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id,
         "attributes": r.attributes, "ingested_at": r.ingested_at}
        for r in store.list_source_records(tenant_id, entity_id=entity_id)
    ]
    return {"entity_id": e.id, "status": e.status, "attributes": e.attributes,
            "updated_at": e.updated_at, "source_records": sources}


@app.get("/v1/customer/entities/{entity_id}/lineage", tags=["entities"])
def get_lineage(entity_id: str, tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    if not store.get_entity(entity_id, tenant_id):
        raise HTTPException(404, "entity not found")
    return [
        {"field": ev.field, "old_value": ev.old_value, "new_value": ev.new_value,
         "source_record_id": ev.source_record_id, "reason": ev.reason, "changed_at": ev.changed_at}
        for ev in store.list_lineage(entity_id, tenant_id)
    ]


# ---------------------------------------------------------------------------
# Stewardship review queue
# ---------------------------------------------------------------------------
@app.get("/v1/customer/review-queue", tags=["stewardship"])
def review_queue(tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    candidates = store.list_candidates(tenant_id, status="pending")
    out = []
    for c in candidates:
        rec_a = store.get_source_record(c.record_a_id, tenant_id)
        rec_b = store.get_source_record(c.record_b_id, tenant_id)
        out.append({
            "candidate_id": c.id, "score": c.score, "match_type": c.match_type, "reasons": c.reasons,
            "record_a": {"id": rec_a.id, "source_system": rec_a.source_system, "attributes": rec_a.attributes} if rec_a else None,
            "record_b": {"id": rec_b.id, "source_system": rec_b.source_system, "attributes": rec_b.attributes} if rec_b else None,
            "created_at": c.created_at,
        })
    return out


@app.post("/v1/customer/review-queue/{candidate_id}/resolve", tags=["stewardship"])
def resolve(candidate_id: str, req: ResolveCandidateRequest,
            tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id")):
    try:
        return resolve_candidate(store, tenant_id, candidate_id, req.decision, req.decided_by)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get("/v1/customer/schema", tags=["meta"])
def schema():
    return {"domain": "customer", "fields": CUSTOMER_SCHEMA_FIELDS}
