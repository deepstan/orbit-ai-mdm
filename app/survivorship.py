"""
Survivorship engine (Section 5.2 of the architecture doc): given a cluster
of source records believed to represent the same customer, decide which
value "survives" into the golden record for each field — and record *why*,
for lineage/audit purposes.

Strategies:
  - most_recent      : value from the most recently ingested source record
                        that has a non-empty value for this field.
  - most_complete     : same as most_recent, but simply the first non-empty
                        value found across all sources (useful when recency
                        isn't meaningful, e.g. address).
  - source_priority   : prefer sources in a configured priority order,
                        e.g. ["salesforce", "shopify", "csv_import"].

Every field's decision is returned with the winning source_record_id and a
human-readable reason, so nothing is a silent overwrite.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .models import SourceRecord

DEFAULT_STRATEGY = "most_recent"

# Per-field strategy overrides — tenant-configurable in a real deployment.
FIELD_STRATEGIES: dict[str, str] = {
    "email": "most_recent",
    "phone": "most_recent",
    "address_line1": "most_recent",
    "city": "most_recent",
    "state": "most_recent",
    "postal_code": "most_recent",
    "country": "most_complete",
    "first_name": "most_complete",
    "last_name": "most_complete",
}

SOURCE_PRIORITY: list[str] = []  # e.g. ["salesforce", "shopify"], empty = no preference


@dataclass
class FieldDecision:
    field: str
    value: Any
    winning_source_record_id: str | None
    strategy: str
    reason: str


def _non_empty_candidates(records: list[SourceRecord], field: str):
    for rec in sorted(records, key=lambda r: r.ingested_at, reverse=True):
        val = rec.attributes.get(field)
        if val not in (None, ""):
            yield rec, val


def _resolve_field(field: str, records: list[SourceRecord]) -> FieldDecision:
    strategy = FIELD_STRATEGIES.get(field, DEFAULT_STRATEGY)
    candidates = list(_non_empty_candidates(records, field))  # already newest-first

    if not candidates:
        return FieldDecision(field, None, None, strategy, "no source had a value")

    if strategy == "most_recent":
        rec, val = candidates[0]
        return FieldDecision(
            field, val, rec.id, strategy,
            f"most recently ingested value, from {rec.source_system} at {rec.ingested_at}",
        )

    if strategy == "most_complete":
        rec, val = candidates[0]
        return FieldDecision(
            field, val, rec.id, strategy,
            f"first non-empty value found, from {rec.source_system}",
        )

    if strategy == "source_priority" and SOURCE_PRIORITY:
        for preferred in SOURCE_PRIORITY:
            for rec, val in candidates:
                if rec.source_system == preferred:
                    return FieldDecision(
                        field, val, rec.id, strategy,
                        f"source '{preferred}' is highest-priority for this field",
                    )
        rec, val = candidates[0]
        return FieldDecision(
            field, val, rec.id, strategy,
            "no priority source had a value; fell back to most recent",
        )

    # Fallback
    rec, val = candidates[0]
    return FieldDecision(field, val, rec.id, "most_recent (fallback)", "default rule")


def build_golden_attributes(records: list[SourceRecord]) -> dict[str, FieldDecision]:
    """Runs survivorship across every canonical field for a cluster of
    source records and returns the per-field decisions (value + why)."""
    from .models import CUSTOMER_SCHEMA_FIELDS

    return {f: _resolve_field(f, records) for f in CUSTOMER_SCHEMA_FIELDS}
