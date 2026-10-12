"""
The staging layer.

    source_records        RAW landing. Exactly what the source sent; never rewritten.
    standardized_records  STAGING. Derived from raw, rebuildable, one row per source record, holding two forms:
                            matching  - normalized for comparison ("robert", "5125550123")
                            cleansed  - presentable ("Robert", "(512) 555-0123"); what golden records come from
    blocking_keys, match_candidates, entities   - matching and the golden layer, derived from staging

Golden records are built from the CLEANSED form, so they never carry raw whitespace, casing, placeholders, or
unformatted values. Everything above raw is a function of (raw, domain config) and is rebuilt by
`domains_cli.py reindex <domain>` when the config changes; the stale-index guard (app/indexing.py) makes sure
nobody forgets.
"""
from __future__ import annotations

from .fieldtypes import cast_value
from .standardize import standardize


def stage_record(domain: str, config, raw: dict) -> tuple:
    """(matching form, cleansed form) of one raw record."""
    both = getattr(config.standardizer, "standardize_both", None)
    if both is not None:
        return both(raw)
    matching = standardize(domain, raw)  # a custom callable standardizer has no separate cleansed form
    return matching, dict(matching)


def cast_record(config, cleansed: dict, record_id: str) -> tuple:
    """Casts one record's CLEANSED values to their fields' declared types, for the golden layer.
    -> (typed values, quality issues). A value that cannot be cast becomes None (so survivorship skips it and a valid value
    from another record can win) and is reported; it is never stored as a wrong value."""
    typed, issues = {}, []
    for f in config.schema_fields:
        spec = config.field_specs.get(f)
        value, kind = cleansed.get(f), (spec.type if spec else "text")
        if kind == "text" or value in (None, ""):
            typed[f] = value
            continue
        typed[f], problem = cast_value(value, kind, spec.formats)
        if problem:
            issues.append({"record_id": record_id, "field": f, "value": str(value), "type": kind, "problem": problem})
    return typed, issues


def cleansing_issues(config, raw: dict, record_id: str) -> list:
    """What the cleansing rules threw away (or kept only partly) in ONE raw record, for the fields that report it (ssn, dob).
    Quality issues for the golden record. The VALUE is deliberately never included: an SSN must not end up in an entity's
    quality_issues, and the reason is enough to act on."""
    specs = getattr(config.standardizer, "specs", None)
    if not specs:
        return []
    issues = []
    for f in config.schema_fields:
        spec = specs.get(f)
        if spec is None or not spec.reports_rejections:
            continue
        value = next((raw.get(n) for n in (spec.sources or (f,)) if raw.get(n) not in (None, "")), None)
        problem = spec.diagnose(value)
        if problem:
            issues.append({"record_id": record_id, "field": f, "value": "[withheld]", "type": "cleansing", "problem": problem})
    return issues
