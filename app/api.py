"""
FastAPI HTTP layer. Thin wrapper around app/pipeline.py -- all matching/
survivorship logic lives in the dependency-free modules so it's testable
without a server running.

Routes are generic across domains: /v1/{domain}/records, /v1/{domain}/entities,
etc.

AUTH: required, unless you say otherwise.
  - DEFAULT: every request needs `Authorization: Bearer <api_key>`. The tenant and the role come from the key, NOT from any
    header the client sends. Create the first key with manage_api_keys.py BEFORE starting the server.
  - MDM_DEV_MODE=1 (and only that): "dev mode" -- no key, tenant from the X-Tenant-Id header, full tenant_admin access, for a laptop
    and the demo. The server says so loudly at startup. It used to be the other way round (open unless MDM_ENABLE_AUTH was set), so
    forgetting one variable in production left the whole API open; the old variable now does nothing.

The decisions a person makes are recorded under the NAME OF THE AUTHENTICATED KEY, never a name sent in a request.

Limits (see app/limits.py): request body size, records per batch, size of a record, and an optional per-key rate limit.

Run with:
    pip install -r requirements.txt
    uvicorn app.api:app --reload --port 8000
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from . import audit, billing, learned_scorer, limits, monitor, paging, stewardship, unmerge
from .auth import (AuthError, Principal, check_permission, dev_mode, hash_key, human_actor, may_see_log_entry, valid_key_label, visible_domains)
from .compliance import purge_entity
from .domains import REGISTRY, get_domain_config
from .models import DEFAULT_TENANT
from .pipeline import ingest_record, resolve_candidate, run_batch, MODES
from .domain_config import load_configured_domains
from .indexing import IndexStaleError
from .store_factory import get_store

app = FastAPI(
    title="Multi-Domain MDM — Phase 1+2+3 MVP",
    description="Customer / Product / Vendor MDM API with AI-assisted matching, "
                 "tenant isolation, RBAC, audit logging, usage metering, and "
                 "right-to-be-forgotten.",
    version="0.4.0",
)

# Customer-defined domains (see app/domain_config.py). An invalid config file stops startup with every
# problem listed, rather than serving a half-applied set of domains.
load_configured_domains()
from . import protect as protection  # noqa: E402
from .domains import REGISTRY as _REGISTRY  # noqa: E402
protection.require_key_if_needed(_REGISTRY)  # fail closed: a protected field without its key means the service does not start
store = get_store()

log = logging.getLogger("mdm.api")
if dev_mode():
    log.warning("MDM_DEV_MODE is ON: there is NO authentication. Anyone who can reach this server is a full administrator of any tenant they name. Never expose it.")


@app.exception_handler(AuthError)
async def _forbidden(request, exc: AuthError):
    """A key that may not do something gets 403 and the reason. (Most endpoints call check_permission directly, and without this handler the AuthError it
    raises reached the framework as an unhandled exception: HTTP 500 and a traceback in the log, for what is an ordinary refusal.)"""
    return JSONResponse(status_code=403, content={"detail": str(exc)})


@app.middleware("http")
async def _limit_body_size(request, call_next):
    if limits.body_too_large(request.headers.get("content-length")):
        return JSONResponse(status_code=413, content={"detail": f"The request body is over the {limits.max_body_bytes():,}-byte limit. Send smaller batches."})
    return await call_next(request)


_limiter = limits.RateLimiter(limits.rate_limit_per_minute())


def _throttle(principal: Principal) -> None:
    allowed, wait = _limiter.allow(f"{principal.tenant_id}:{principal.key_id}")
    if not allowed:
        raise HTTPException(429, "Too many requests for this key. Slow down.", headers={"Retry-After": str(int(wait) + 1)})


@app.get("/healthz", tags=["meta"])
def healthz():
    """Is the server up? Needs no key and says nothing else: for a load balancer or the setup validator."""
    return {"status": "ok"}


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
    return not dev_mode()


def get_principal(
    authorization: Optional[str] = Header(default=None),
    x_tenant_id: str = Header(default=DEFAULT_TENANT, alias="X-Tenant-Id"),
) -> Principal:
    if not auth_enabled():
        # Dev mode, and only because MDM_DEV_MODE was set: full access, tenant from the (client-supplied, so not
        # security-meaningful in this mode) header.
        principal = Principal(tenant_id=x_tenant_id, role="tenant_admin", key_id="dev-mode", label="dev-mode", allowed_domains=None)
        _throttle(principal)
        return principal

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header. Expected: Bearer <api_key>")
    plaintext_key = authorization[len("Bearer "):].strip()
    key_record = store.get_api_key_by_hash(hash_key(plaintext_key))
    if not key_record or key_record.revoked_at:
        raise HTTPException(401, "Invalid or revoked API key")
    principal = Principal(tenant_id=key_record.tenant_id, role=key_record.role, key_id=key_record.id,
                           label=key_record.label, allowed_domains=key_record.allowed_domains)
    _throttle(principal)
    return principal


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
    attributes: dict[str, Any] = Field(..., description="Domain-specific attributes; unknown keys allowed. Bounded: see app/limits.py.")

    @field_validator("attributes")
    @classmethod
    def _bounded(cls, value):
        return limits.check_attributes(value)   # ValueError -> 422 with the reason


class BatchIngestRequest(BaseModel):
    records: list[IngestRequest] = Field(..., max_length=limits.max_batch_records())
    max_errors: Optional[int] = None   # abort the run once more than this many records fail (the rest stay unfinished)
    defer: bool = False                # land + stage only; matching happens later, in `process-pending`


class ResolveCandidateRequest(BaseModel):
    decision: str = Field(..., pattern="^(confirm|reject)$")
    decided_by: Optional[str] = Field(default=None, description="IGNORED. The decision is recorded under the authenticated key's name; this is kept only so older clients still work.")


class AuditVerdictRequest(BaseModel):
    verdict: str = Field(..., pattern="^(correct|wrong)$")
    unmerge: bool = False      # a 'wrong' verdict may also split the entity on the spot
    reason: Optional[str] = None


class UnmergeRequest(BaseModel):
    reason: Optional[str] = None
    dry_run: bool = False


class CreateApiKeyRequest(BaseModel):
    role: str = Field(..., examples=["data_steward"])
    label: str = Field(..., examples=["Jane's steward key"])
    allowed_domains: Optional[list[str]] = Field(default=None, description="null = all domains")


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
@app.get("/v1/domains", tags=["meta"])
def list_domains(principal: Principal = Depends(get_principal)):
    """The domains this key may see (a domain's fields are configuration a customer may consider theirs, so this needs a key like everything else)."""
    check_permission(principal, "view")
    return [{"domain": name, "fields": REGISTRY[name].schema_fields, "source": REGISTRY[name].source} for name in visible_domains(principal, REGISTRY)]


@app.get("/v1/{domain}/schema", tags=["meta"])
def schema(domain: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    return {"domain": domain, "fields": get_domain_config(domain).schema_fields}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
@app.post("/v1/{domain}/records", tags=["ingestion"])
def ingest(domain: str, req: IngestRequest, mode: str = "realtime", principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "ingest", domain=domain)  # raises AuthError -> handled below via except
    if mode not in MODES:
        raise HTTPException(422, f"mode must be one of {list(MODES)}: 'realtime' matches and publishes before answering; "
                                 f"'batch' accepts the record now (HTTP 202) and matches it later in a batch run")

    try:
        result = ingest_record(store, principal.tenant_id, domain, req.source_system,
                                req.source_record_id, req.attributes, mode=mode)
    except IndexStaleError as e:
        raise HTTPException(409, str(e))

    if not result.get("unchanged"):  # re-sending a record we already hold is a no-op: not metered
        billing.record_usage(store, principal.tenant_id, "record_ingested", domain=domain)
    audit.log_action(store, principal.tenant_id, action="ingest", actor=principal.label, domain=domain,
                      details={"source_system": req.source_system, "source_record_id": req.source_record_id,
                               "result": {"matched_entity_id": result["matched_entity_id"],
                                          "auto_merged": len(result["auto_merged_with"]) > 0,
                                          "queued_for_review": len(result["queued_for_review"]) > 0,
                                          "unchanged": bool(result.get("unchanged")), "updated": bool(result.get("updated")),
                                          "resumed": bool(result.get("resumed")), "mode": mode}})
    if result.get("deferred"):
        return JSONResponse(status_code=202, content=result)  # accepted, not yet matched
    return result


# ---------------------------------------------------------------------------
# Golden records
# ---------------------------------------------------------------------------
@app.post("/v1/{domain}/records/batch", tags=["ingestion"])
def ingest_batch_endpoint(domain: str, req: BatchIngestRequest, principal: Principal = Depends(get_principal)):
    """BATCH ingest: many records as one logged run. Errors are collected per record and the run continues (see `max_errors`);
    golden records are rebuilt once per affected entity at the end; the audit log gets ONE entry for the run, not one per record."""
    _validate_domain(domain)
    check_permission(principal, "ingest", domain=domain)
    if not req.records:
        raise HTTPException(422, "records must not be empty")
    try:
        batch = run_batch(store, principal.tenant_id, domain, [(r.source_system, r.source_record_id, r.attributes) for r in req.records],
                          max_errors=req.max_errors, defer=req.defer, triggered_by="api")
    except IndexStaleError as e:
        raise HTTPException(409, str(e))
    counts = batch.run["counts"]
    metered = counts["new"] + counts["updated"] + counts["deferred"]  # re-sent unchanged records are not metered
    if metered:
        billing.record_usage(store, principal.tenant_id, "record_ingested", domain=domain, quantity=metered)
    audit.log_action(store, principal.tenant_id, action="ingest_batch", actor=principal.label, domain=domain,
                     details={"run_id": batch.run["run_id"], "status": batch.run["status"], "counts": counts})
    return JSONResponse(status_code=202 if req.defer else 200, content=batch.run)


@app.get("/v1/{domain}/runs", tags=["operations"])
def list_runs(domain: str, limit: int = 20, principal: Principal = Depends(get_principal)):
    """Recent batch runs, process-pending passes and reindexes for this domain: what each received and what became of it."""
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    return store.list_runs(domain=domain, tenant_id=principal.tenant_id, limit=min(max(limit, 1), 200))


@app.get("/v1/{domain}/entities", tags=["entities"])
def list_entities(domain: str, status: Optional[str] = None, limit: int = 100, cursor: Optional[str] = None, principal: Principal = Depends(get_principal)):
    """A page of golden records, in id order: {"items", "next_cursor", "limit"}. Pass next_cursor back as `cursor` for the next page; it is null on the last. `limit`
    defaults to 100, at most 1000. (A list of every entity in one response does not scale, and a client that silently got only the first 100 would be worse off than
    one that is told there is more.)"""
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    limit = paging.clamp_limit(limit, default=100, maximum=1000)
    try:
        after = paging.decode(cursor, {"i": str})["i"] if cursor else None
    except ValueError as e:
        raise HTTPException(400, str(e))
    page = store.entities_page(principal.tenant_id, domain, limit + 1, after_id=after, status=status)   # one more than asked: that is how it knows there is a next page
    more, page = len(page) > limit, page[:limit]
    return {"items": [{"entity_id": e.id, "status": e.status, "attributes": e.attributes, "source_record_ids": e.source_record_ids, "updated_at": e.updated_at} for e in page],
            "next_cursor": paging.encode({"i": page[-1].id}) if more else None, "limit": limit}


@app.get("/v1/{domain}/entities/{entity_id}", tags=["entities"])
def get_entity(domain: str, entity_id: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    e = store.get_entity(entity_id, principal.tenant_id, domain)
    if not e:
        raise HTTPException(404, "entity not found")
    sources = [
        {"id": r.id, "source_system": r.source_system, "source_record_id": r.source_record_id,
         "attributes": protection.mask_attributes(domain, r.attributes), "ingested_at": r.ingested_at}
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
def review_queue(domain: str, limit: int = 50, cursor: Optional[str] = None, principal: Principal = Depends(get_principal)):
    """A page of the pairs waiting for a person, best score first: {"items", "next_cursor", "limit", "total"}. Pass next_cursor back as `cursor` for the next page; it is
    null on the last. `limit` defaults to 50, at most 200. `total` is how many pairs are waiting in all. The records of a page are fetched together, not one by one."""
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    limit = paging.clamp_limit(limit, default=50, maximum=200)
    try:
        position = paging.decode(cursor, {"s": (int, float), "i": str}) if cursor else None
    except ValueError as e:
        raise HTTPException(400, str(e))
    page = store.pending_page(principal.tenant_id, domain, limit + 1, after=(position["s"], position["i"]) if position else None)
    more, page = len(page) > limit, page[:limit]
    records = store.get_source_records_batch(principal.tenant_id, domain, [r for c in page for r in (c.record_a_id, c.record_b_id)])

    def side(record_id):
        rec = records.get(record_id)
        return {"id": rec.id, "source_system": rec.source_system, "attributes": protection.mask_attributes(domain, rec.attributes)} if rec else None
    items = [{"candidate_id": c.id, "score": c.score, "match_type": c.match_type, "reasons": c.reasons, "contributions": c.contributions,
              "record_a": side(c.record_a_id), "record_b": side(c.record_b_id), "created_at": c.created_at} for c in page]
    return {"items": items, "next_cursor": paging.encode({"s": page[-1].score, "i": page[-1].id}) if more else None, "limit": limit,
            "total": store.count_pending(principal.tenant_id, domain)}


@app.post("/v1/{domain}/review-queue/{candidate_id}/resolve", tags=["stewardship"])
def resolve(domain: str, candidate_id: str, req: ResolveCandidateRequest,
            principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "resolve_review_queue", domain=domain)
    try:
        result = resolve_candidate(store, principal.tenant_id, domain, candidate_id, req.decision, human_actor(principal))   # the key's name, never the request's
    except ValueError as e:
        raise HTTPException(400, str(e))
    details = {"candidate_id": candidate_id, "decision": req.decision, "decided_by": human_actor(principal)}
    if req.decided_by and req.decided_by != details["decided_by"]:
        details["claimed_decided_by"] = req.decided_by   # kept for the record: what the client SAID, which was not used
    audit.log_action(store, principal.tenant_id, action="resolve_review_queue", actor=principal.label, domain=domain, details=details)
    return result


# ---------------------------------------------------------------------------
# Audits and unmerge. Who is acting is the authenticated key's label, never a name sent by the browser: a verdict and a
# separation are a person's decisions and are recorded as theirs.
# ---------------------------------------------------------------------------
@app.get("/v1/{domain}/audits", tags=["stewardship"])
def audit_page(domain: str, limit: int = 50, principal: Principal = Depends(get_principal)):
    """A random sample of the system's automatic merges for a person to check (the model's promotions first), with how the checks have come out so far."""
    _validate_domain(domain)
    check_permission(principal, "resolve_review_queue", domain=domain)
    queue = stewardship.audit_queue(store, principal.tenant_id, domain, mask=lambda attrs: protection.mask_attributes(domain, attrs), limit=max(1, min(limit, 200)))
    return {"summary": stewardship.audit_summary(store, principal.tenant_id, domain), **queue}


@app.post("/v1/{domain}/audits/{candidate_id}/verdict", tags=["stewardship"])
def audit_verdict(domain: str, candidate_id: str, req: AuditVerdictRequest, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "resolve_review_queue", domain=domain)
    try:
        result = stewardship.give_verdict(store, principal.tenant_id, domain, candidate_id, req.verdict, principal.label, unmerge=req.unmerge, reason=req.reason or "")
    except ValueError as e:
        raise HTTPException(400, str(e))
    audit.log_action(store, principal.tenant_id, action="audit_verdict", actor=principal.label, domain=domain,
                      details={"candidate_id": candidate_id, "verdict": req.verdict, "unmerge": req.unmerge, "unmerge_result": result["unmerge"]})
    return result


@app.post("/v1/{domain}/merges/{candidate_id}/unmerge", tags=["stewardship"])
def unmerge_merge(domain: str, candidate_id: str, req: UnmergeRequest, principal: Principal = Depends(get_principal)):
    """Undo ONE confirmed merge a person says is wrong. dry_run shows what would move and changes nothing."""
    _validate_domain(domain)
    check_permission(principal, "resolve_review_queue", domain=domain)
    try:
        result = unmerge.unmerge_pair(store, principal.tenant_id, domain, candidate_id, principal.label, req.reason or "", dry_run=req.dry_run)
    except unmerge.CannotUnmerge as e:
        raise HTTPException(409, str(e))
    if not req.dry_run:
        audit.log_action(store, principal.tenant_id, action="unmerge", actor=principal.label, domain=domain,
                          details={"candidate_id": candidate_id, "reason": req.reason, "kept_entity": result["kept_entity"], "new_entity": result["new_entity"], "moved": len(result["moved"])})
    return result


@app.get("/v1/{domain}/scorer", tags=["stewardship"])
def scorer_status(domain: str, principal: Principal = Depends(get_principal)):
    _validate_domain(domain)
    check_permission(principal, "view", domain=domain)
    return stewardship.audit_summary(store, principal.tenant_id, domain)


@app.post("/v1/{domain}/scorer/check", tags=["stewardship"])
def scorer_check(domain: str, principal: Principal = Depends(get_principal)):
    """Re-estimate from the audits and roll the model back if they show it is hurting. (The same check `domains_cli.py scorer check` runs on a schedule.)"""
    _validate_domain(domain)
    check_permission(principal, "retrain", domain=domain)
    result = monitor.check_scorer(store, principal.tenant_id, domain)
    if result.get("state") == "rolled_back":
        audit.log_action(store, principal.tenant_id, action="scorer_rollback", actor=principal.label, domain=domain, details={"why": result["why"]})
    return {k: v for k, v in result.items() if k != "strata"} | {"strata": {n: {f: x for f, x in v.items() if f != "wrong_pairs"} for n, v in result.get("strata", {}).items()}}


@app.post("/v1/{domain}/scorer/reset", tags=["stewardship"])
def scorer_reset(domain: str, principal: Principal = Depends(get_principal)):
    """Switch promotion back on after a rollback, once a person has looked at why it was rolled back."""
    _validate_domain(domain)
    check_permission(principal, "retrain", domain=domain)
    learned_scorer.write_state(principal.tenant_id, domain, "active", f"reset by {principal.label}")
    audit.log_action(store, principal.tenant_id, action="scorer_reset", actor=principal.label, domain=domain, details={})
    return {"state": "active"}


# ---------------------------------------------------------------------------
# Admin: API key management (tenant_admin only)
# ---------------------------------------------------------------------------
@app.post("/v1/admin/api-keys", tags=["admin"])
def create_api_key(req: CreateApiKeyRequest, principal: Principal = Depends(get_principal)):
    check_permission(principal, "manage_api_keys")
    try:
        label = valid_key_label(req.label)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if req.role not in ("tenant_admin", "domain_admin", "data_steward", "api_consumer"):
        raise HTTPException(400, f"Unknown role '{req.role}'")

    from .auth import generate_api_key
    from .models import ApiKeyRecord
    plaintext, key_hash = generate_api_key()
    record = ApiKeyRecord(tenant_id=principal.tenant_id, role=req.role, key_hash=key_hash,
                           label=label, allowed_domains=req.allowed_domains)
    store.save_api_key(record)
    audit.log_action(store, principal.tenant_id, action="api_key_created", actor=principal.label,
                      details={"new_key_id": record.id, "role": req.role, "label": label})

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
    check_permission(principal, "view_audit_log", domain=domain)   # a key limited to some domains may not name another
    entries = [e for e in store.list_audit_log(principal.tenant_id, domain=domain) if may_see_log_entry(principal, e.domain)]
    chain_ok, broke_at = audit.verify_chain(store, principal.tenant_id)
    if broke_at is not None and not any(e.id == broke_at for e in entries):
        broke_at = "(an entry outside the domains of this key)"   # the chain is tenant-wide: say it is broken without pointing into another domain
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
