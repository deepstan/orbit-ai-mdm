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

from . import matching
from .domains import DomainConfig, get_domain_config
from .standardize import standardize


class IndexStaleError(Exception):
    """Ingestion into a domain is paused because its blocking index doesn't match its configuration."""


def blocking_fingerprint(config: DomainConfig) -> str:
    groups = [list(g) for g in config.blocking_field_groups]
    used = sorted({f for g in groups for f in g})
    missing = [f for f in used if f not in config.field_specs]
    if missing:
        raise ValueError(f"Domain '{config.name}' blocks on field(s) {missing} that have no field spec")
    payload = {
        "prefix_len": matching.BLOCKING_PREFIX_LEN,
        "key_format": matching.BLOCKING_KEY_FORMAT,
        "groups": groups,
        "specs": {f: config.field_specs[f].to_json() for f in used},
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def index_state(store, domain: str, config: DomainConfig) -> str:
    """One of READY, EMPTY, STALE, REINDEXING, UNKNOWN."""
    state = store.get_index_state(domain)
    if state is None:
        return "UNKNOWN" if store.domain_has_index_entries(domain) else "EMPTY"
    fingerprint, status = state
    if status != "ready":
        return "REINDEXING"
    return "READY" if fingerprint == blocking_fingerprint(config) else "STALE"


def ensure_index_ready(store, domain: str, config: DomainConfig) -> None:
    """Called before every ingest. Cheap: one primary-key read when READY."""
    state = index_state(store, domain, config)
    if state == "READY":
        return
    if state == "EMPTY":
        store.set_index_state(domain, blocking_fingerprint(config), "ready")
        return
    fix = f"python3 domains_cli.py reindex {domain}"
    if state == "STALE":
        raise IndexStaleError(
            f"The matching configuration for domain '{domain}' changed since its blocking index was "
            f"built (config: {config.source}). Ingesting now would miss real duplicates without any "
            f"error. Rebuild the index first:  {fix}   (restart the app first if you edited the config file).")
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


def reindex_domain(store, domain: str, batch_size: int = 500, progress=None) -> ReindexResult:
    """Rebuilds the blocking keys of EVERY record in a domain (all tenants:
    configuration is deployment-wide) under the current configuration.

    Safe to interrupt and re-run: ingestion into the domain is paused for the
    duration (state 'reindexing'), and it stays paused if this fails, so a
    half-rebuilt index can never be used. Each batch swaps one record set's
    keys atomically. NOTE: under Postgres row-level security with FORCE, run
    this as a role that bypasses it -- with no tenant set it would see no
    rows and report a successful, empty reindex."""
    config = get_domain_config(domain)  # KeyError for an unknown domain
    fingerprint = blocking_fingerprint(config)
    started = time.time()
    store.set_index_state(domain, fingerprint, "reindexing")

    total = store.count_source_records(domain)
    done = batches = 0
    last_id = ""
    while True:
        page = store.list_source_records_page(domain, last_id, batch_size)
        if not page:
            break
        store.replace_blocking_keys_batch([
            (rec.tenant_id, domain, rec.id, matching.blocking_keys(standardize(domain, rec.attributes), config))
            for rec in page
        ])
        last_id = page[-1].id
        done += len(page)
        batches += 1
        if progress:
            progress(done, total)

    store.set_index_state(domain, fingerprint, "ready")
    return ReindexResult(domain, done, batches, time.time() - started, fingerprint)
