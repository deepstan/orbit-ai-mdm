"""
Keeping the blocking index honest.

Candidate lookup (pipeline.py) trusts the blocking_keys table: it finds
matches by querying keys that were computed when each record was ingested.
Those keys are a function of the domain's configuration. If a customer edits
the config in a way that changes the keys (different blocking groups, or a
different standardization of a field those groups use), every stored key is
now stale: lookups would quietly miss real duplicates and nothing would
fail. "Remember to reindex" is not a safety mechanism.

So the index records a FINGERPRINT of the configuration it was built under,
per domain. Every ingest compares it with the current configuration:

    READY        same fingerprint        -> ingest
    EMPTY        nothing indexed yet     -> adopt the current config, ingest
    STALE        config changed          -> refuse (IndexStaleError) until reindexed
    REINDEXING   a reindex is running,
                 or was interrupted      -> refuse until it completes
    UNKNOWN      keys exist but no state -> refuse; reindex establishes the state

The fingerprint covers only what determines blocking keys: the blocking
groups, the standardization of the fields they use, and the key layout in
matching.py. Editing fuzzy weights, thresholds, semantic settings,
deterministic fields or survivorship does NOT change it and needs no reindex.

What a reindex does and does not do: it rebuilds the candidate index so FUTURE
ingests use the new rules. It does not re-run matching over existing records.
Matching is incremental (each record is compared once, when it arrives), so
duplicates the new rules would have caught earlier stay unmerged until a
record arrives that matches them.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass

from .protect import key_id, mask_prefix, protected_fields
from . import matching
from .domains import DomainConfig, get_domain_config
from .staging import stage_record


class IndexStaleError(Exception):
    """Ingestion into a domain is paused because its blocking index doesn't match its configuration."""


def derived_fingerprint(config: DomainConfig) -> str:
    """Fingerprint of everything the DERIVED data depends on: how every field is standardized and presented (the
    staging layer), the blocking layout (the candidate index), and survivorship (golden records). Raw data and
    scoring settings (weights, penalties, thresholds, semantic, deterministic fields) are NOT part of it: changing
    those needs no rebuild."""
    groups = [[spec.field for spec in group] for group in config.keyspecs()]
    used = sorted({f for g in groups for f in g})
    missing = [f for f in used if f not in config.field_specs]
    if missing:
        raise ValueError(f"Domain '{config.name}' blocks on field(s) {missing} that have no field spec")
    payload = {
        "prefix_len": matching.BLOCKING_PREFIX_LEN,
        "key_format": matching.BLOCKING_KEY_FORMAT,
        "groups": groups,
        "fields": {f: config.field_specs[f].to_json() for f in config.schema_fields if f in config.field_specs},
        "survivorship": {"default": config.default_strategy, "fields": dict(sorted(config.field_strategies.items()))},
    }
    if any(spec.kind != "auto" or spec.size is not None for group in config.keyspecs() for spec in group):
        payload["keys"] = [[spec.to_json() for spec in group] for group in config.keyspecs()]  # soundex/windows/... change the keys
    if any(config.blocking_unless):
        payload["unless"] = [list(u) for u in config.blocking_unless]  # a conditional group changes WHICH records get keys
    if protected_fields(config):
        payload["token_key_id"] = key_id()  # rotating the key (a new id) makes every derived hash stale: reindex re-hashes from raw
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


blocking_fingerprint = derived_fingerprint  # the original name, kept for callers and tests


def index_state(store, domain: str, config: DomainConfig) -> str:
    """One of READY, EMPTY, STALE, REINDEXING, UNKNOWN."""
    state = store.get_index_state(domain)
    if state is None:
        return "UNKNOWN" if store.domain_has_index_entries(domain) else "EMPTY"
    fingerprint, status = state
    if status != "ready":
        return "REINDEXING"
    return "READY" if fingerprint == blocking_fingerprint(config) else "STALE"


def ensure_index_ready(store, domain: str, config: DomainConfig) -> str:
    """Called before every ingest; returns the current derived-data fingerprint. Cheap: one primary-key read when READY."""
    state = index_state(store, domain, config)
    fingerprint = derived_fingerprint(config)
    if state == "READY":
        return fingerprint
    if state == "EMPTY":
        store.set_index_state(domain, fingerprint, "ready")
        return fingerprint
    fix = f"python3 domains_cli.py reindex {domain}"
    if state == "STALE":
        raise IndexStaleError(
            f"The configuration for domain '{domain}' changed since its derived data (staging layer, blocking "
            f"index, golden records) was built (config: {config.source}). Ingesting now would miss real duplicates, or "
            f"mix old and new rules, without any error. Rebuild it first:  {fix}   (restart the app first if you edited the config file).")
    if state == "REINDEXING":
        raise IndexStaleError(
            f"A reindex of domain '{domain}' is running, or was interrupted. Ingestion into it is paused "
            f"until it completes. If nothing is running, re-run:  {fix}")
    raise IndexStaleError(
        f"Domain '{domain}' has indexed records but no recorded index state, so there is no way to tell "
        f"whether the index matches the current configuration. Establish it with:  {fix}")


@dataclass
class ReindexResult:
    domain: str
    records: int
    batches: int
    seconds: float
    fingerprint: str
    entities: int = 0


def reindex_domain(store, domain: str, batch_size: int = 500, progress=None) -> ReindexResult:
    """Rebuilds every DERIVED layer of a domain from raw, under the current configuration, for ALL tenants
    (configuration is deployment-wide):

        1. the staging layer  (re-standardize every raw record: matching + cleansed forms)
        2. the blocking index (keys recomputed from the fresh matching forms)
        3. golden records     (re-derived from the fresh cleansed forms, one lineage row per changed field)

    Raw is never touched. Safe to interrupt and re-run: ingestion into the domain is paused for the duration (state
    'reindexing') and stays paused if this fails, so a half-rebuilt layer can never be used. Each batch swaps a record
    set's staged forms and keys atomically. NOTE: under Postgres row-level security with FORCE, run this as a role
    that bypasses it: with no tenant set it would see no rows and report a successful, empty rebuild."""
    from .pipeline import Run, rebuild_golden_record  # local import: pipeline imports this module

    config = get_domain_config(domain)  # KeyError for an unknown domain
    fingerprint = derived_fingerprint(config)
    started = time.time()
    run = Run.start(store, "*", domain, "reindex", "cli")  # configuration is deployment-wide, so a reindex belongs to no one tenant
    store.sync_entity_table(domain)  # the golden-record table and its columns, as the config now defines them
    store.set_index_state(domain, fingerprint, "reindexing")

    total = store.count_source_records(domain)
    done = batches = 0
    last_id = ""
    while True:
        page = store.list_source_records_page(domain, last_id, batch_size)
        if not page:
            break
        entries = []
        for rec in page:
            matched, cleansed = stage_record(domain, config, rec.attributes)
            entries.append((rec.tenant_id, domain, rec.id, matched, cleansed, fingerprint, matching.blocking_keys(matched, config)))
        store.replace_derived_batch(entries)
        last_id = page[-1].id
        done += len(page)
        batches += 1
        if progress:
            progress(done, total)

    entities, last_entity = 0, ""
    while True:
        page = store.list_entities_page(domain, last_entity, batch_size)
        if not page:
            break
        for tenant_id, entity_id in page:
            rebuild_golden_record(store, tenant_id, domain, entity_id, reason="rebuilt from the staging layer")
            entities += 1
        last_entity = page[-1][1]

    for f, spec in protected_fields(config).items():  # what OLDER rows still hold in plain text: lineage, merged-away golden rows, old match reasons
        store.scrub_protected(domain, f, mask_prefix(spec.normalize[-1]))
    store.set_index_state(domain, fingerprint, "ready")
    run.counts.update(received=done, golden_rebuilt=entities)
    run.finish()
    return ReindexResult(domain, done, batches, time.time() - started, fingerprint, entities)
