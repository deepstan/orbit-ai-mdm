"""
Core data models for the Customer MDM MVP.

Deliberately dependency-free (stdlib `dataclasses` only) so the domain
model and matching engine can be unit-tested without installing anything.
The FastAPI layer (api.py) wraps these in Pydantic models for the HTTP
boundary.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Canonical attribute schemas, one per domain. Multi-domain support lives in
# app/domains.py (the registry that ties a schema to its matching config,
# standardizer, and survivorship rules) -- these lists are just the field
# names, kept here since models.py is the shared, dependency-free base.
# ---------------------------------------------------------------------------
CUSTOMER_SCHEMA_FIELDS = [
    "first_name",
    "last_name",
    "name_suffix",
    "email",
    "phone",
    "address_line1",
    "city",
    "state",
    "postal_code",
    "country",
]

PRODUCT_SCHEMA_FIELDS = [
    "sku",
    "upc",
    "product_name",
    "brand",
    "category",
    "description",
    "price",
]

VENDOR_SCHEMA_FIELDS = [
    "company_name",
    "tax_id",
    "duns_number",
    "contact_email",
    "contact_phone",
    "address_line1",
    "city",
    "state",
    "postal_code",
    "country",
]


DEFAULT_TENANT = "default"
DEFAULT_DOMAIN = "customer"


@dataclass
class SourceRecord:
    """One raw record as it arrived from a source system, before matching."""
    source_system: str
    source_record_id: str
    attributes: dict[str, Any]
    id: str = field(default_factory=lambda: new_id("rec_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    ingested_at: str = field(default_factory=now_iso)
    entity_id: Optional[str] = None  # set once clustered into a golden record
    content_hash: Optional[str] = None  # sha256 of the canonical payload: what makes re-ingesting the same record a no-op
    version: int = 1                     # 1 + the number of times the source changed this record
    updated_at: Optional[str] = None     # when the current version arrived (None = still the first)
    # Where the record is on its way from raw to golden. landed -> staged -> matched (the only finished state). A record that
    # stops short is RESUMED when it is sent again or by `process-pending`, never mistaken for a finished duplicate.
    processing_status: str = "landed"
    processing_attempts: int = 0
    processing_error: Optional[str] = None


@dataclass
class MatchCandidate:
    """A proposed match between two records/entities awaiting a decision."""
    record_a_id: str
    record_b_id: str
    score: float
    match_type: str  # "deterministic" | "fuzzy" | "llm_adjudicated"
    reasons: list[str] = field(default_factory=list)
    # Per-field breakdown of the score (field, state, similarity, points) -- what a steward needs to see WHY.
    contributions: list = field(default_factory=list)
    id: str = field(default_factory=lambda: new_id("cand_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    status: str = "pending"  # pending | confirmed | rejected
    created_at: str = field(default_factory=now_iso)
    decided_at: Optional[str] = None
    decided_by: Optional[str] = None


@dataclass
class Recommendation:
    """What a stewardship AGENT thinks should happen to a review item. It changes NOTHING: it is stored, shown to a steward, and later compared with what the steward decided
    (`outcome`), which is the evidence for deciding how far the agent can be trusted. One per (candidate, agent, model, prompt version): proposing again replaces it."""
    candidate_id: str
    recommendation: str            # confirm | reject | needs_human
    confidence: float              # 0 to 1
    agent: str                     # who proposed it: the KEY's name ("agent:<label>"), never something the request says
    reasons: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)      # what it looked at (names of the evidence parts), not copies of the data
    model: str = ""
    prompt_version: str = ""
    id: str = field(default_factory=lambda: new_id("reco_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    created_at: str = field(default_factory=now_iso)
    outcome: Optional[str] = None            # None while the item waits; then agreed | disagreed | human_decided (it said needs_human)
    outcome_decision: Optional[str] = None   # what the person decided: confirm | reject
    outcome_by: Optional[str] = None
    outcome_at: Optional[str] = None
    outcome_seen: Optional[bool] = None      # True: the person opened the suggestion before deciding. False: it was there and they did not. None: not known


@dataclass
class LineageEvent:
    entity_id: str
    field: str
    old_value: Optional[str]
    new_value: Optional[str]
    source_record_id: Optional[str]
    reason: str
    id: str = field(default_factory=lambda: new_id("lin_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    changed_at: str = field(default_factory=now_iso)


@dataclass
class ApiKeyRecord:
    """Stored record for an API key -- only the hash is kept, never the
    plaintext key. See app/auth.py for generation/verification."""
    tenant_id: str
    role: str
    key_hash: str
    label: str
    id: str = field(default_factory=lambda: new_id("key_"))
    allowed_domains: Optional[list[str]] = None  # None = all domains
    created_at: str = field(default_factory=now_iso)
    revoked_at: Optional[str] = None


@dataclass
class AuditLogEntry:
    """One immutable, hash-chained audit log entry. See app/audit.py."""
    tenant_id: str
    action: str
    actor: str  # API key label/id, or "system"
    details: dict[str, Any]
    prev_hash: str
    entry_hash: str
    id: str = field(default_factory=lambda: new_id("audit_"))
    domain: Optional[str] = None
    created_at: str = field(default_factory=now_iso)


@dataclass
class UsageEvent:
    """One billing-relevant usage event. See app/billing.py."""
    tenant_id: str
    event_type: str  # e.g. "record_ingested", "api_call", "llm_adjudication_call"
    id: str = field(default_factory=lambda: new_id("usage_"))
    domain: Optional[str] = None
    quantity: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=now_iso)


@dataclass
class Entity:
    """A golden record -- for whichever domain it belongs to."""
    id: str = field(default_factory=lambda: new_id("ent_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    status: str = "active"  # pending | active | merged | deprecated | disputed
    attributes: dict[str, Any] = field(default_factory=dict)
    source_record_ids: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    # Values that could not be cast to their field's declared type (app/fieldtypes.py). The record is kept; the value is NULL.
    quality_issues: list = field(default_factory=list)
