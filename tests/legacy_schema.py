"""Schema exactly as created by the pre-migration code (CREATE TABLE IF NOT EXISTS on startup).
Used to prove the migration system builds an identical schema, and to simulate adopting
an existing, un-versioned database. Do not edit."""

LEGACY_SQLITE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS source_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    attributes TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    entity_id TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    attributes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_candidates (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_a_id TEXT NOT NULL,
    record_b_id TEXT NOT NULL,
    score REAL NOT NULL,
    match_type TEXT NOT NULL,
    reasons TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    decided_by TEXT
);

CREATE TABLE IF NOT EXISTS lineage (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    source_record_id TEXT,
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocking_keys (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    record_id TEXT NOT NULL,
    blocking_key TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    role TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    allowed_domains TEXT,
    created_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain TEXT,
    event_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_records_entity ON source_records(tenant_id, domain, entity_id);
CREATE INDEX IF NOT EXISTS idx_source_records_scope ON source_records(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_entities_scope ON entities(tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_match_candidates_scope_status ON match_candidates(tenant_id, domain, status);
CREATE INDEX IF NOT EXISTS idx_lineage_scope_entity ON lineage(tenant_id, domain, entity_id);
CREATE INDEX IF NOT EXISTS idx_blocking_keys_lookup ON blocking_keys(tenant_id, domain, blocking_key);
CREATE INDEX IF NOT EXISTS idx_blocking_keys_record ON blocking_keys(tenant_id, domain, record_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_events_tenant ON usage_events(tenant_id, created_at);
'''
