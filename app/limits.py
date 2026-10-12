"""
Limits on what one request or one key may ask of the service. Pure Python, so each rule is testable without a web server; app/api.py applies them.

All are set by environment variables, and the defaults are generous: they stop a runaway or hostile client, not a normal one.
  MDM_MAX_BATCH_RECORDS      records in one batch request                      10000
  MDM_MAX_RECORD_BYTES       one record's attributes, as JSON                  65536
  MDM_MAX_RECORD_FIELDS      attributes in one record                          500
  MDM_MAX_BODY_BYTES         one request body (checked from Content-Length)    33554432 (32 MiB)
  MDM_RATE_LIMIT_PER_MINUTE  requests per key per minute                       0 = off (a limit in one process; put a real one at the proxy too)
"""
from __future__ import annotations

import json
import os
import threading
import time


def _int(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, default)))
    except ValueError:
        return default


def max_batch_records() -> int:
    return _int("MDM_MAX_BATCH_RECORDS", 10_000) or 10_000


def max_record_bytes() -> int:
    return _int("MDM_MAX_RECORD_BYTES", 65_536) or 65_536


def max_record_fields() -> int:
    return _int("MDM_MAX_RECORD_FIELDS", 500) or 500


def max_body_bytes() -> int:
    return _int("MDM_MAX_BODY_BYTES", 32 * 1024 * 1024) or 32 * 1024 * 1024


def rate_limit_per_minute() -> int:
    return _int("MDM_RATE_LIMIT_PER_MINUTE", 0)


def check_attributes(attributes) -> dict:
    """Refuse a record that is absurdly large or wide before anything is done with it. Raises ValueError with the reason; returns the attributes unchanged."""
    if not isinstance(attributes, dict):
        raise ValueError("attributes must be an object")
    if len(attributes) > max_record_fields():
        raise ValueError(f"a record may have at most {max_record_fields()} attributes (this one has {len(attributes)})")
    size = len(json.dumps(attributes, default=str).encode("utf-8"))
    if size > max_record_bytes():
        raise ValueError(f"a record's attributes may be at most {max_record_bytes():,} bytes as JSON (this one is {size:,})")
    return attributes


def body_too_large(content_length) -> bool:
    """Whether a request that announces this Content-Length is over the limit. A missing or unreadable header is not judged here: the proxy in front must cap those."""
    try:
        return int(content_length) > max_body_bytes()
    except (TypeError, ValueError):
        return False


class RateLimiter:
    """A token bucket per key: `per_minute` requests may be made in a burst, refilled evenly over the minute. Thread-safe; memory is one small entry per key seen
    recently. per_minute = 0 allows everything. This limits one PROCESS: behind several workers each has its own bucket, so a proxy needs the real limit."""

    def __init__(self, per_minute: int, clock=time.monotonic):
        self.per_minute, self.clock = per_minute, clock
        self._buckets: dict = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> tuple:
        """(allowed, seconds to wait before trying again)."""
        if self.per_minute <= 0:
            return True, 0.0
        rate, now = self.per_minute / 60.0, self.clock()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self.per_minute), now))
            tokens = min(float(self.per_minute), tokens + (now - last) * rate)
            if tokens >= 1.0:
                self._buckets[key] = (tokens - 1.0, now)
                allowed, wait = True, 0.0
            else:
                self._buckets[key] = (tokens, now)
                allowed, wait = False, (1.0 - tokens) / rate
            if len(self._buckets) > 10_000:   # forget the keys that have been idle long enough to be full again
                full = [k for k, (t, last_seen) in self._buckets.items() if t + (now - last_seen) * rate >= self.per_minute]
                for k in full:
                    self._buckets.pop(k, None)
        return allowed, wait
