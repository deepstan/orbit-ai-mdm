"""
Immutable, hash-chained audit log (Phase 3 item #2: audit/compliance).

Every audit entry's hash is computed from its own content PLUS the
previous entry's hash -- a blockchain-style hash chain, scoped per
tenant. This means: if anyone (an attacker with DB access, a rogue
admin, a bug) modifies or deletes a historical entry, every subsequent
entry's hash becomes unverifiable, and `verify_chain()` below detects
exactly where the chain breaks. This is what "immutable audit log" means
in practice for a system built on a regular SQL database with no
specialized immutable-storage backend -- tamper-evident, not
tamper-proof (someone with direct DB write access could still forge a
consistent-looking chain from scratch), but that's a genuine, meaningful
step up from a plain mutable log table with no integrity guarantee at all.

This is a SEPARATE log from `lineage` (survivorship.py / pipeline.py).
Lineage records *why a golden record's field changed*, for data-quality
explainability. Audit log records *who did what, when*, for security and
compliance -- every ingest, every steward decision, every API key
created/revoked, every purge. Different audiences, different retention
needs, so they're kept as different tables rather than overloading one.
"""
from __future__ import annotations

import hashlib
import json

from .db import Store
from .models import AuditLogEntry, now_iso

GENESIS_HASH = "0" * 64  # the "previous hash" for a tenant's very first audit entry


def _compute_hash(prev_hash: str, tenant_id: str, domain: str | None, action: str,
                   actor: str, details: dict, created_at: str) -> str:
    payload = json.dumps(
        {"prev_hash": prev_hash, "tenant_id": tenant_id, "domain": domain, "action": action,
         "actor": actor, "details": details, "created_at": created_at},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def log_action(store: Store, tenant_id: str, action: str, actor: str,
                details: dict | None = None, domain: str | None = None) -> AuditLogEntry:
    """Appends one entry to the tenant's audit chain. Call this for every
    security/compliance-relevant action: ingest, review-queue resolution,
    API key lifecycle, retrain, purge. Not for routine reads -- this is
    an audit trail of changes and access-control-relevant events, not a
    full request log."""
    details = details or {}
    prev = store.get_last_audit_entry(tenant_id)
    prev_hash = prev.entry_hash if prev else GENESIS_HASH
    created_at = now_iso()
    entry_hash = _compute_hash(prev_hash, tenant_id, domain, action, actor, details, created_at)

    entry = AuditLogEntry(
        tenant_id=tenant_id, domain=domain, action=action, actor=actor, details=details,
        prev_hash=prev_hash, entry_hash=entry_hash, created_at=created_at,
    )
    store.append_audit_entry(entry)
    return entry


def verify_chain(store: Store, tenant_id: str) -> tuple[bool, str | None]:
    """Recomputes every entry's hash from its content and checks it both
    (a) matches the stored hash and (b) correctly chains to the previous
    entry. Returns (True, None) if the whole chain is intact, or
    (False, entry_id) naming the first entry where it breaks."""
    entries = store.list_audit_log(tenant_id, limit=10**9)  # full history for verification
    prev_hash = GENESIS_HASH
    for entry in entries:
        expected = _compute_hash(prev_hash, entry.tenant_id, entry.domain, entry.action,
                                  entry.actor, entry.details, entry.created_at)
        if entry.prev_hash != prev_hash or entry.entry_hash != expected:
            return False, entry.id
        prev_hash = entry.entry_hash
    return True, None
