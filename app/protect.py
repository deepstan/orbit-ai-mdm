"""
Protecting sensitive identifiers (an SSN): keyed hash for matching, mask for display, plain value only in RAW.

THE MODEL (option A): `raw` keeps exactly what the source sent, because raw is what lets `reindex` re-apply a changed rule and
re-hash under a new key. Everywhere ELSE the plain value never appears:

    staging (matching form)   a keyed hash:  v1:3f9a...   equal values give equal hashes, so exact matching and exact blocking work
    staging (cleansed form)   a mask:        ***-**-6789
    golden record, lineage    the mask (golden is built from the cleansed form)
    blocking index            the hash
    match reasons             "exact ssn match" (no value); quality issues: "[withheld]"
    API                       the raw attributes of a record are masked on the way out

THE KEY. `MDM_TOKEN_KEY` (at least 32 characters) is read from the environment, which is where a secrets store should put it; it
must NOT live in the database, or whoever steals the database steals the key and the hashes can be reversed (an SSN has only about
a billion possible values). `MDM_TOKEN_KEY_ID` (default v1) is stored in front of every hash. It is part of the derived-data
fingerprint, so changing the key id stops ingestion until `reindex` re-hashes everything from raw: that is how a key is rotated.
Changing the KEY without changing the id would leave old hashes unmatchable; always change both.

FAIL CLOSED. If a field is protected and no valid key is set, standardizing raises ProtectionKeyMissing. Nothing is ever stored
unprotected instead; the raw record is kept and `process-pending` finishes it once the key is there.

WHAT IT DOES NOT DO: one key for the deployment (tenants are isolated by tenant_id everywhere, but the same SSN gives the same hash
in two tenants); a hash is only as strong as the key's secrecy; and plain values still exist in raw and in superseded payloads
(source_record_history). Protect those with database encryption and tight access.
"""
from __future__ import annotations

import contextlib
import hashlib
import hmac
import os
import re

MIN_KEY_CHARS = 32
MISSING = ("a field is protected (\"protect\": \"hash\") but MDM_TOKEN_KEY is not set. Set it in the environment from your secrets store "
           "(at least 32 characters; generate one with: python3 -c \"import secrets; print(secrets.token_urlsafe(48))\") and keep it OUT of the "
           "database. Nothing is stored unprotected instead.")


class ProtectionKeyMissing(RuntimeError):
    pass


_UNKEYED = {"on": False}


def key_id() -> str:
    kid = os.environ.get("MDM_TOKEN_KEY_ID", "v1").strip() or "v1"
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,16}", kid):
        raise ProtectionKeyMissing("MDM_TOKEN_KEY_ID must be 1-16 letters, digits, '-' or '_' (it is stored in front of every hash)")
    return kid


def _secret() -> bytes:
    key = os.environ.get("MDM_TOKEN_KEY", "")
    if not key and _UNKEYED["on"]:
        return b"unkeyed-read-only"
    if not key:
        raise ProtectionKeyMissing(MISSING)
    if len(key) < MIN_KEY_CHARS:
        raise ProtectionKeyMissing(f"MDM_TOKEN_KEY is too short ({len(key)} characters; at least {MIN_KEY_CHARS} are required)")
    return key.encode("utf-8")


@contextlib.contextmanager
def unkeyed():
    """For READ-ONLY tools (the profiler) that need to standardize records but never store the result: no key needed, and the hashes
    produced inside are meaningless."""
    before, _UNKEYED["on"] = _UNKEYED["on"], True
    try:
        yield
    finally:
        _UNKEYED["on"] = before


def token(value: str, label: str) -> str:
    """The keyed hash of a normalized value; `label` (the field's last normalizer, e.g. ssn) keeps one field's hashes apart from another's."""
    mac = hmac.new(_secret(), f"{label}\x00{value}".encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{key_id()}:{mac}"


def near_token(variant: str, label: str) -> str:
    """A SHORT keyed hash of one typo-variant of a value (10 hex characters), for near-equality in SCORING only: a record carries
    a few dozen of these, so they are kept short, and they are never a blocking key. Under the same key as `token`."""
    return hmac.new(_secret(), f"{label}~near\x00{variant}".encode("utf-8"), hashlib.sha256).hexdigest()[:10]


def mask_prefix(label: str) -> str:
    return "***-**-" if label == "ssn" else "***"


def mask_value(value: str, label: str) -> str:
    """***-**-6789 for an SSN, ***6789 for anything else: the last four characters only."""
    text = str(value or "")
    tail = (re.sub(r"\D", "", text) if label == "ssn" else re.sub(r"[^A-Za-z0-9]", "", text))[-4:]
    return mask_prefix(label) + tail


def protected_fields(config) -> dict:
    return {f: s for f, s in (getattr(config, "field_specs", None) or {}).items() if getattr(s, "protect", "")}


def mask_attributes(domain: str, attrs: dict) -> dict:
    """RAW attributes as they may leave the system: the protected fields masked (each source name the field reads from)."""
    from .domains import REGISTRY
    config = REGISTRY.get(domain)
    prot = protected_fields(config) if config else {}
    if not prot or not attrs:
        return attrs
    out = dict(attrs)
    for f, spec in prot.items():
        for name in (spec.sources or (f,)):
            if out.get(name) not in (None, ""):
                out[name] = mask_value(str(out[name]), spec.normalize[-1])
    return out


def scrub_reason(field: str, reason: str) -> str:
    """A match reason from before the field was protected, without the value it printed."""
    f = re.escape(field)
    reason = re.sub(rf"^exact {f} match \(.*\)$", f"exact {field} match", reason)
    return re.sub(rf"^{f} disagrees \(.*\): (-[\d.]+)$", rf"{field} disagrees: \1", reason)


def require_key_if_needed(registry) -> None:
    """Called when the API starts: if any domain protects a field, a valid key must be there, or the service does not start."""
    needy = sorted(f"{name}.{f}" for name, cfg in registry.items() for f in protected_fields(cfg))
    if needy:
        try:
            _secret()
            key_id()
        except ProtectionKeyMissing as e:
            raise ProtectionKeyMissing(f"{e} (protected: {', '.join(needy)})") from None
