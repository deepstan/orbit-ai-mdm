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


@dataclass
class MatchCandidate:
    """A proposed match between two records/entities awaiting a decision."""
    record_a_id: str
    record_b_id: str
    score: float
    match_type: str  # "deterministic" | "fuzzy" | "llm_adjudicated"
    reasons: list[str] = field(default_factory=list)
    id: str = field(default_factory=lambda: new_id("cand_"))
    tenant_id: str = DEFAULT_TENANT
    domain: str = DEFAULT_DOMAIN
    status: str = "pending"  # pending | confirmed | rejected
    created_at: str = field(default_factory=now_iso)
    decided_at: Optional[str] = None
    decided_by: Optional[str] = None


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
