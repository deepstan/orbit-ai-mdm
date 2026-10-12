"""
Field types for golden records.

A field in a domain config can declare a type; its golden-record column then has that real type, so a report can
`SELECT SUM(price)` or `WHERE listed_on > '2026-01-01'` without casting anything.

    "price":     {"normalize": "text", "type": "numeric"}
    "listed_on": {"normalize": "text", "type": "date", "format": ["%m/%d/%Y", "%Y-%m-%d"]}
    "active":    {"normalize": "text", "type": "boolean"}

Typing happens at the GOLDEN layer, not in staging (staging stays text). A value that cannot be cast becomes NULL, never
a wrong value, and the record is kept: the problem is listed on the golden row's `quality_issues`.

Rules, chosen to be strict rather than clever:
  integer    "1,200" and "12.0" are fine; "12.5" is not
  numeric    currency symbols and thousands commas are fine; a decimal COMMA ("1,5") is not understood
  boolean    true/false, t/f, yes/no, y/n, 1/0
  date       ISO "YYYY-MM-DD" unless `format` lists patterns. It is never guessed: 03/04/2026 is March 4th or April 3rd
             depending on who wrote it, so you say which. With several patterns, the first that matches wins.
  timestamp  ISO 8601 unless `format` is given. Stored in UTC; a value with no timezone is taken to be UTC.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

FIELD_TYPES = ("text", "integer", "numeric", "boolean", "date", "timestamp")

SQL_TYPES = {
    "sqlite": {"text": "TEXT", "integer": "INTEGER", "numeric": "NUMERIC", "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP"},
    "postgres": {"text": "TEXT", "integer": "BIGINT", "numeric": "NUMERIC", "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMPTZ"},
}
_SQLITE_BACK = {"TEXT": "text", "INTEGER": "integer", "NUMERIC": "numeric", "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamp"}
_PG_BACK = {"text": "text", "bigint": "integer", "integer": "integer", "numeric": "numeric", "boolean": "boolean", "date": "date",
            "timestamp with time zone": "timestamp"}

_TRUE, _FALSE = {"true", "t", "yes", "y", "1"}, {"false", "f", "no", "n", "0"}


def canonical_type(backend: str, declared: str) -> str:
    """The field type a column's declared database type corresponds to (unknown types come back as themselves)."""
    declared = (declared or "").strip()
    return (_SQLITE_BACK if backend == "sqlite" else _PG_BACK).get(declared.upper() if backend == "sqlite" else declared.lower(), declared.lower())


def cast_value(value, type_: str, formats=()):
    """-> (typed value or None, problem or None). Empty is missing, not a problem."""
    if value is None:
        return None, None
    if type_ == "text":
        return str(value), None
    s = str(value).strip()
    if s == "":
        return None, None
    try:
        if type_ == "integer":
            if re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+", s):
                s = s.replace(",", "")
            m = re.fullmatch(r"([+-]?\d+)(\.0+)?", s)
            if not m:
                return None, f"{s!r} is not a whole number"
            return int(m.group(1)), None
        if type_ == "numeric":
            t = re.sub(r"^[\s$€£¥]+|[\s$€£¥]+$", "", s)
            if re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+(\.\d+)?", t):
                t = t.replace(",", "")
            d = Decimal(t)
            if not d.is_finite():
                return None, f"{s!r} is not a finite number"
            return d, None
        if type_ == "boolean":
            low = s.lower()
            if low in _TRUE:
                return True, None
            if low in _FALSE:
                return False, None
            return None, f"{s!r} is not a yes/no value (true/false, t/f, yes/no, y/n, 1/0)"
        if type_ == "date":
            patterns = list(formats) or ["%Y-%m-%d"]
            for p in patterns:
                try:
                    return datetime.strptime(s, p).date(), None
                except ValueError:
                    continue
            return None, f"{s!r} does not match {' or '.join(patterns)}"
        if type_ == "timestamp":
            parsed = None
            if formats:
                for p in formats:
                    try:
                        parsed = datetime.strptime(s, p)
                        break
                    except ValueError:
                        continue
            else:
                try:
                    parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
                except ValueError:
                    parsed = None
            if parsed is None:
                return None, f"{s!r} does not match {' or '.join(formats) if formats else 'ISO 8601'}"
            return (parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)), None
    except (InvalidOperation, ValueError):
        return None, f"{s!r} is not a valid {type_}"
    raise ValueError(f"unknown field type '{type_}'")


def to_db(backend: str, type_: str, value):
    """A typed Python value as the database wants it. Postgres takes native objects; SQLite has no such types."""
    if value is None or backend != "sqlite":
        return value
    if type_ == "boolean":
        return int(value)
    if type_ == "numeric":
        return str(value)  # NUMERIC affinity turns it into a number; Decimal can't be bound directly
    if type_ in ("date", "timestamp"):
        return value.isoformat()
    return value


def from_db(backend: str, type_: str, value):
    """The typed Python value for what the database returned. (Postgres already returns native objects.)
    If a stored value cannot be converted, it is returned as it is rather than failing the read."""
    if value is None or backend != "sqlite" or type_ == "text":
        return value
    try:
        if type_ == "integer":
            return int(value)
        if type_ == "numeric":
            return Decimal(str(value))
        if type_ == "boolean":
            return bool(int(value))
        if type_ == "date":
            return date.fromisoformat(str(value))
        if type_ == "timestamp":
            return datetime.fromisoformat(str(value))
    except (ValueError, InvalidOperation):
        return value
    return value
