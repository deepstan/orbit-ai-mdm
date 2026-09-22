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
# Canonical customer attribute schema (v1 — keep it flat and simple for MVP)
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


DEFAULT_TENANT = "default"


@dataclass
class SourceRecord:
    """One raw record as it arrived from a source system, before matching."""
    source_system: str
    source_record_id: str
    attributes: dict[str, Any]
    id: str = field(default_factory=lambda: new_id("rec_"))
    tenant_id: str = DEFAULT_TENANT
    ingested_at: str = field(default_factory=now_iso)
    entity_id: Optional[str] = None  # set once clustered into a golden record


@dataclass
class MatchCandidate:
    """A proposed match between two records/entities awaiting a decision."""
    record_a_id: str
    record_b_id: str
    score: float
    match_type: str  # "deterministic" | "fuzzy"
    reasons: list[str] = field(default_factory=list)
    id: str = field(default_factory=lambda: new_id("cand_"))
    tenant_id: str = DEFAULT_TENANT
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
    changed_at: str = field(default_factory=now_iso)


@dataclass
class Entity:
    """A golden customer record."""
    id: str = field(default_factory=lambda: new_id("ent_"))
    tenant_id: str = DEFAULT_TENANT
    status: str = "active"  # pending | active | merged | deprecated | disputed
    attributes: dict[str, Any] = field(default_factory=dict)
    source_record_ids: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
