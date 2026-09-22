"""
FastAPI HTTP layer. Thin wrapper around app/pipeline.py -- all matching/
survivorship logic lives in the dependency-free modules so it's testable
without a server running.

Routes are generic across domains: /v1/{domain}/records, /v1/{domain}/entities,
etc.

AUTH: two modes, controlled by MDM_ENABLE_AUTH.
  - unset / "false" (DEFAULT): "dev mode" -- no API key required,
    tenant resolved from the X-Tenant-Id header (default "default"),
    full tenant_admin access. This is what keeps the existing
    stewardship UI, seed_demo.py's assumptions, and a quick `curl`
    working with zero setup -- exactly like LLM adjudication and the
    Postgres backend, real functionality stays opt-in rather than
    silently breaking existing usage.
  - "true": every request needs `Authorization: Bearer <api_key>`.
    tenant_id and role come from the key, NOT from any client-supplied
    header. Use manage_api_keys.py to create the first key before
    turning this on, or you'll lock yourself out.

Run with:
    pip install -r requirements.txt
    uvicorn app.api:app --reload --port 8000
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import audit, billing
from .auth import AuthError, Principal, check_permission, hash_key
from .compliance import purge_entity
from .domains import REGISTRY, get_domain_config
from .models import DEFAULT_TENANT
from .pipeline import ingest_record, resolve_candidate
from .store_factory import get_store

app = FastAPI(
    title="Multi-Domain MDM — Phase 1+2+3 MVP",
    description="Customer / Product / Vendor MDM API with AI-assisted matching, "
                 "tenant isolation, RBAC, audit logging, usage metering, and "
                 "right-to-be-forgotten.",
    version="0.4.0",
)

store = get_store()

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


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------
def auth_enabled() -> bool:
    return os.environ.get("MDM_ENABLE_AUTH", "").lower() in ("1", "true", "yes")


def get_principal(
    authorization: Optional[str] = Header(default=None),
    x_tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id"),
) -> Principal:
    if not auth_enabled():
        # Dev mode: full access, tenant from the (client-supplied, so not
        # security-meaningful in this mode -- see module docstring) header.
        return Principal(tenant_id=x_tenant_id, role="tenant_admin", key_id="dev-mode",
                          label="dev-mode (MDM_ENABLE_AUTH not set)", allowed_domains=None)

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header. Expected: Bearer <api_key>")
    plaintext_key = authorization[len("Bearer "):].strip()
    key_record = store.get_api_key_by_hash(hash_key(plaintext_key))
    if not key_record or key_record.revoked_at:
        raise HTTPException(401, "Invalid or revoked API key")
    return Principal(tenant_id=key_record.tenant_id, role=key_record.role, key_id=key_record.id,
                      label=key_record.label, allowed_domains=key_record.allowed_domains)


def require(action: str, domain: Optional[str] = None):
    """FastAPI dependency factory: require(action) checks the resolved
    Principal can perform `action`, raising 403 if not. Use as:
        principal: Principal = Depends(require("ingest"))
    """
    def _check(principal: Principal = Depends(get_principal)) -> Principal:
        try:
            check_permission(principal, action, domain=domain)
        except AuthError as e:
            raise HTTPException(403, str(e))
        return principal
    return _check


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


class CreateApiKeyRequest(BaseModel):
    role: str = Field(..., examples=["data_steward"])
    label: str = Field(..., examples=["Jane's steward key"])
    allowed_domains: Optional[list[str]] = Field(default=None, description="null = all domains")


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
def ingest(domain: str, req: IngestRequest, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "ingest", domain=domain)  # raises AuthError -> handled below via except

    try:
        result = ingest_record(store, principal.tenant_id, domain, req.source_system,
                                req.source_record_id, req.attributes)
    except Exception:
        raise

    billing.record_usage(store, principal.tenant_id, "record_ingested", domain=domain)
    audit.log_action(store, principal.tenant_id, action="ingest", actor=principal.label, domain=domain,
                      details={"source_system": req.source_system, "source_record_id": req.source_record_id,
                               "result": {"matched_entity_id": result["matched_entity_id"],
                                          "auto_merged": len(result["auto_merged_with"]) > 0,
                                          "queued_for_review": len(result["queued_for_review"]) > 0}})
    return result


# ---------------------------------------------------------------------------
# Golden records
# ---------------------------------------------------------------------------
@app.get("/v1/{domain}/entities", tags=["entities"])
def list_entities(domain: str, status: Optional[str] = None, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    return [
        {"entity_id": e.id, "status": e.status, "attributes": e.attributes,
         "source_record_ids": e.source_record_ids, "updated_at": e.updated_at}
        for e in store.list_entities(principal.tenant_id, domain, status=status)
    ]


@app.get("/v1/{domain}/entities/{entity_id}", tags=["entities"])
def get_entity(domain: str, entity_id: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    e = store.get_entity(entity_id, principal.tenant_id, domain)
    if not e:
        raise HTTPException(404, "entity not found")
    sources = [
        {"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id,
         "attributes": r.attributes, "ingested_at": r.ingested_at}
        for r in store.list_source_records(principal.tenant_id, domain, entity_id=entity_id)
    ]
    return {"entity_id": e.id, "status": e.status, "attributes": e.attributes,
            "updated_at": e.updated_at, "source_records": sources}


@app.get("/v1/{domain}/entities/{entity_id}/lineage", tags=["entities"])
def get_lineage(domain: str, entity_id: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    if not store.get_entity(entity_id, principal.tenant_id, domain):
        raise HTTPException(404, "entity not found")
    return [
        {"field": ev.field, "old_value": ev.old_value, "new_value": ev.new_value,
         "source_record_id": ev.source_record_id, "reason": ev.reason, "changed_at": ev.changed_at}
        for ev in store.list_lineage(entity_id, principal.tenant_id, domain)
    ]


@app.delete("/v1/{domain}/entities/{entity_id}", tags=["compliance"])
def delete_entity(domain: str, entity_id: str, principal: Principal = Depends(get_principal)):
    """Right-to-be-forgotten: permanently purges a golden record and all
    its source records. tenant_admin only. See app/compliance.py."""
    _validate_domain(domain)
    check_permission(principal, "purge", domain=domain)
    try:
        result = purge_entity(store, principal.tenant_id, domain, entity_id, requested_by=principal.label)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {
        "entity_id": result.entity_id, "source_records_deleted": result.source_records_deleted,
        "match_candidates_affected": result.match_candidates_affected,
    }


# ---------------------------------------------------------------------------
# Stewardship review queue
# ---------------------------------------------------------------------------
@app.get("/v1/{domain}/review-queue", tags=["stewardship"])
def review_queue(domain: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    candidates = store.list_candidates(principal.tenant_id, domain, status="pending")
    out = []
    for c in candidates:
        rec_a = store.get_source_record(c.record_a_id, principal.tenant_id, domain)
        rec_b = store.get_source_record(c.record_b_id, principal.tenant_id, domain)
        out.append({
            "candidate_id": c.id, "score": c.score, "match_type": c.match_type, "reasons": c.reasons,
            "record_a": {"id": rec_a.id, "source_system": rec_a.source_system, "attributes": rec_a.attributes} if rec_a else None,
            "record_b": {"id": rec_b.id, "source_system": rec_b.source_system, "attributes": rec_b.attributes} if rec_b else None,
            "created_at": c.created_at,
        })
    return out


@app.post("/v1/{domain}/review-queue/{candidate_id}/resolve", tags=["stewardship"])
def resolve(domain: str, candidate_id: str, req: ResolveCandidateRequest,
            principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "resolve_review_queue", domain=domain)
    try:
        result = resolve_candidate(store, principal.tenant_id, domain, candidate_id, req.decision, req.decided_by)
    except ValueError as e:
        raise HTTPException(400, str(e))
    audit.log_action(store, principal.tenant_id, action="resolve_review_queue", actor=principal.label,
                      domain=domain, details={"candidate_id": candidate_id, "decision": req.decision,
                                               "decided_by": req.decided_by})
    return result


# ---------------------------------------------------------------------------
# Admin: API key management (tenant_admin only)
# ---------------------------------------------------------------------------
@app.post("/v1/admin/api-keys", tags=["admin"])
def create_api_key(req: CreateApiKeyRequest, principal: Principal = Depends(get_principal)):
    check_permission(principal, "manage_api_keys")
    if req.role not in ("tenant_admin", "domain_admin", "data_steward", "api_consumer"):
        raise HTTPException(400, f"Unknown role '{req.role}'")

    from .auth import generate_api_key
    from .models import ApiKeyRecord
    plaintext, key_hash = generate_api_key()
    record = ApiKeyRecord(tenant_id=principal.tenant_id, role=req.role, key_hash=key_hash,
                           label=req.label, allowed_domains=req.allowed_domains)
    store.save_api_key(record)
    audit.log_action(store, principal.tenant_id, action="api_key_created", actor=principal.label,
                      details={"new_key_id": record.id, "role": req.role, "label": req.label})

    return {"api_key": plaintext, "key_id": record.id, "role": record.role, "label": record.label,
            "warning": "This key is shown once. Store it securely -- it cannot be retrieved again."}


@app.get("/v1/admin/api-keys", tags=["admin"])
def list_api_keys(principal: Principal = Depends(get_principal)):
    check_permission(principal, "manage_api_keys")
    return [
        {"key_id": k.id, "role": k.role, "label": k.label, "allowed_domains": k.allowed_domains,
         "created_at": k.created_at, "revoked_at": k.revoked_at}
        for k in store.list_api_keys(principal.tenant_id)
    ]


@app.delete("/v1/admin/api-keys/{key_id}", tags=["admin"])
def revoke_api_key(key_id: str, principal: Principal = Depends(get_principal)):
    check_permission(principal, "manage_api_keys")
    from .models import now_iso
    store.revoke_api_key(key_id, principal.tenant_id, now_iso())
    audit.log_action(store, principal.tenant_id, action="api_key_revoked", actor=principal.label,
                      details={"revoked_key_id": key_id})
    return {"key_id": key_id, "revoked": True}


# ---------------------------------------------------------------------------
# Admin: audit log + usage
# ---------------------------------------------------------------------------
@app.get("/v1/admin/audit-log", tags=["admin"])
def get_audit_log(domain: Optional[str] = None, principal: Principal = Depends(get_principal)):
    check_permission(principal, "view_audit_log")
    entries = store.list_audit_log(principal.tenant_id, domain=domain)
    chain_ok, broke_at = audit.verify_chain(store, principal.tenant_id)
    return {
        "chain_valid": chain_ok, "broke_at_entry_id": broke_at,
        "entries": [
            {"id": e.id, "action": e.action, "actor": e.actor, "domain": e.domain,
             "details": e.details, "created_at": e.created_at}
            for e in entries
        ],
    }


@app.get("/v1/admin/usage", tags=["admin"])
def get_usage(since: Optional[str] = None, principal: Principal = Depends(get_principal)):
    check_permission(principal, "view_usage")
    summary = billing.get_usage_summary(store, principal.tenant_id, since=since)
    return {"tenant_id": summary.tenant_id, "since": summary.since,
            "counts_by_event_type": summary.counts_by_event_type,
            "estimated_cost_usd": summary.estimated_cost_usd}
