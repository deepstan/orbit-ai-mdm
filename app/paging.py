"""
Cursors and limits for the paginated list endpoints (the review queue and the entity list).

A page is "the next N after the last one you saw" (keyset pagination), not "skip K": a cursor names the last item by the same fields the list is ordered by, so a page never
repeats or skips an item when other items are added or resolved between two requests, and costs the same however far down the list it is.

A cursor is opaque to clients. It carries only a position, never a tenant or a permission: every query is filtered by the authenticated key's tenant whatever the cursor
says, so a cursor from another tenant, or a forged one, can only move the start of the page within the caller's own data.
"""
from __future__ import annotations

import base64
import json

VERSION = 1
MAX_CURSOR_LENGTH = 512


def encode(payload: dict) -> str:
    raw = json.dumps({"v": VERSION, **payload}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode(token, shape: dict) -> dict:
    """The position a cursor names, checked against `shape` ({field: type or tuple of types}). Raises ValueError('invalid cursor') for anything that is not exactly
    a cursor this module made for this list: wrong version, missing or extra fields, wrong types, too long, not decodable."""
    bad = ValueError("invalid cursor")
    if not isinstance(token, str) or not token or len(token) > MAX_CURSOR_LENGTH:
        raise bad
    try:
        data = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise bad from None
    if not isinstance(data, dict) or data.get("v") != VERSION or set(data) != set(shape) | {"v"}:
        raise bad
    for field, kind in shape.items():
        if isinstance(data[field], bool) or not isinstance(data[field], kind):
            raise bad
    return {field: data[field] for field in shape}


def clamp_limit(limit, default: int, maximum: int) -> int:
    """The page size to use: `default` when it is missing or not a number, and never below 1 or above `maximum`."""
    try:
        value = int(limit)
    except (TypeError, ValueError):
        return default
    return max(1, min(value, maximum))
