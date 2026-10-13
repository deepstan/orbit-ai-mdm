"""
Golden records as real, TYPED tables.

Each domain's golden records live in their own table, `entity_<domain>`, and every field of the domain is a real column
with the type the config declares (app/fieldtypes.py):

    entity_product(entity_id, tenant_id, status, created_at, updated_at, quality_issues, sku, upc, product_name, ..., price NUMERIC)

so a BI tool or an analyst reads `SELECT SUM(price) FROM entity_product WHERE status = 'active'` with no JSON functions and
no casting. One table per domain (not one shared table) because domains have different fields.

`quality_issues` lists values that could not be cast to their field's type. The record is kept, the value is NULL.

THE TRADEOFF: a domain needs a table, and a new field needs a column. That DDL is done only where it is safe to:
    * migrations (migrate.py)            create/extend the built-in tables
    * `domains_cli.py reindex <domain>`  creates/extends a custom domain's table, then rebuilds its golden records
    * the app, lazily, ONLY if auto-migration is on; with MDM_AUTO_MIGRATE=false (the least-privilege setup, where the
      app role has no DDL rights) it only VERIFIES, and refuses with the command to run
Columns are only ever ADDED. A field removed from a config keeps its column (dropping it would destroy data); it is
simply no longer written or read.

CHANGING A FIELD'S TYPE rebuilds the table by copy-and-swap in one transaction (neither SQLite nor a plain Postgres ALTER can
do it safely: one bad value fails the whole statement). Each existing value is re-cast; one that cannot be becomes NULL.
Nothing is lost for good: `reindex` then re-derives every golden value from staging under the new type.
"""
from __future__ import annotations

import json
import re

from .fieldtypes import cast_value, to_db
from .migrations.runner import MigrationContext, MigrationError
from .sql_dialect import get_dialect

SYSTEM_COLUMNS = ("entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues")
RESERVED_FIELD_NAMES = set(SYSTEM_COLUMNS) | {"domain", "id"}  # a field with one of these names would collide with a system column
VALID_DOMAIN = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
VALID_FIELD = re.compile(r"^[a-z][a-z0-9_]{0,62}$")  # 63: Postgres' identifier limit


def table_name(domain: str) -> str:
    if not VALID_DOMAIN.match(domain):
        raise ValueError(f"'{domain}' is not a valid domain name")
    return f"entity_{domain}"


def q(identifier: str) -> str:
    """Quote an identifier (valid in SQLite and Postgres). Names are validated against the patterns above first."""
    return '"' + identifier.replace('"', '""') + '"'


def cell(value):
    return None if value is None else str(value)


def field_columns(config) -> list:
    """[(field, type)] for a domain config."""
    return [(f, config.field_specs[f].type if f in config.field_specs else "text") for f in config.schema_fields]


def normalize_columns(fields) -> list:
    """Accepts field names (text columns) or (name, type) pairs."""
    return [(f, "text") if isinstance(f, str) else (f[0], f[1]) for f in fields]


def create_table(ctx: MigrationContext, domain: str, columns: list, table: str | None = None, index: bool = True) -> None:
    name = table or table_name(domain)
    defs = ["entity_id TEXT PRIMARY KEY", "tenant_id TEXT NOT NULL", "status TEXT NOT NULL", "created_at TEXT NOT NULL",
            "updated_at TEXT NOT NULL", f"quality_issues {ctx.dialect.json_type}"]
    defs += [f"{q(f)} {ctx.dialect.column_type(t)}" for f, t in columns]
    ctx.execute(f"CREATE TABLE {q(name)} (\n    " + ",\n    ".join(defs) + "\n)")
    if index:
        ctx.execute(ctx.dialect.create_index_if_not_exists(q('idx_' + table_name(domain) + '_tenant'), q(name), "tenant_id, status"))
        ctx.execute(ctx.dialect.create_index_if_not_exists(q('idx_' + table_name(domain) + '_tenant_entity'), q(name), "tenant_id, entity_id"))   # a page of entities is a range of this


def retype_table(ctx: MigrationContext, domain: str, columns: list, have: dict) -> None:
    """Rebuild by copy-and-swap so existing values are re-cast safely. Columns the config no longer lists are carried over."""
    name = table_name(domain)
    tmp = name + "__retype"
    wanted = {f for f, _ in columns}
    extras = [(c, ctx.dialect.canonical_type(raw)) for c, raw in have.items() if c not in SYSTEM_COLUMNS and c not in wanted]
    every = list(columns) + extras
    if ctx.table_exists(tmp):
        ctx.execute(ctx.dialect.drop_table(q(tmp)))
    rows = ctx.query(f"SELECT * FROM {q(name)}")
    create_table(ctx, domain, every, table=tmp, index=False)
    out = []
    for r in rows:
        values = [r.get(c) for c in SYSTEM_COLUMNS[:5]] + [ctx.dialect.json_param(r.get("quality_issues"))]
        for c, t in every:
            v = r.get(c)
            typed = None if v is None else cast_value(v, t)[0]  # cannot be cast -> NULL; reindex re-derives it from staging
            values.append(to_db(ctx.backend, t, typed))
        out.append(tuple(values))
    names = ", ".join(q(c) for c in list(SYSTEM_COLUMNS) + [c for c, _ in every])
    ctx.executemany(f"INSERT INTO {q(tmp)} ({names}) VALUES ({', '.join('?' for _ in range(len(SYSTEM_COLUMNS) + len(every)))})", out)
    if ctx.query(f"SELECT COUNT(*) AS n FROM {q(tmp)}")[0]["n"] != len(rows):
        raise MigrationError(f"Retyping {name} lost rows; nothing was changed")
    ctx.execute(ctx.dialect.drop_table(q(name)))
    ctx.execute(ctx.dialect.rename_table(q(tmp), q(name)))
    ctx.execute(ctx.dialect.create_index_if_not_exists(q('idx_' + name + '_tenant'), q(name), "tenant_id, status"))
    ctx.execute(ctx.dialect.create_index_if_not_exists(q('idx_' + name + '_tenant_entity'), q(name), "tenant_id, entity_id"))


def ensure_table(ctx: MigrationContext, domain: str, fields) -> list:
    """Creates the table, adds missing columns, and re-types columns whose declared type changed. Returns the field columns it
    added. Never drops a column."""
    columns = normalize_columns(fields)
    for f, _ in columns:
        if not VALID_FIELD.match(f) or f in RESERVED_FIELD_NAMES:
            raise MigrationError(f"Field '{f}' of domain '{domain}' cannot be a column name "
                                 f"(lowercase letters, digits, underscores; not one of {sorted(RESERVED_FIELD_NAMES)})")
    name = table_name(domain)
    if not ctx.table_exists(name):
        create_table(ctx, domain, columns)
        return [f for f, _ in columns]
    have = ctx.column_types(name)
    added = [f for f, _ in columns if f not in have]
    if any(f in have and ctx.dialect.canonical_type(have[f]) != t for f, t in columns):
        retype_table(ctx, domain, columns, have)
        return added
    if "quality_issues" not in have:
        ctx.execute(ctx.dialect.add_column(q(name), "quality_issues", ctx.dialect.json_type))
    for f, t in columns:
        if f not in have:
            ctx.execute(ctx.dialect.add_column(q(name), q(f), ctx.dialect.column_type(t)))
    return added


def sync_table(backend, domain: str, fields) -> list:
    """ensure_table() in its own transaction, on its own connection."""
    with backend.connection() as conn:
        backend.begin(conn)
        try:
            added = ensure_table(MigrationContext(backend.name, conn), domain, fields)
            backend.commit(conn)
            return added
        except Exception:
            backend.rollback(conn)
            raise


def schema_gaps(backend, domain: str, fields):
    """None if the table does not exist, else what is out of date. Read-only."""
    columns = normalize_columns(fields)
    with backend.connection() as conn:
        try:
            ctx = MigrationContext(backend.name, conn)
            name = table_name(domain)
            if not ctx.table_exists(name):
                return None
            have = ctx.column_types(name)
            return {"missing": [f for f, _ in columns if f not in have],
                    "retype": [(f, get_dialect(backend.name).canonical_type(have[f]), t) for f, t in columns
                               if f in have and get_dialect(backend.name).canonical_type(have[f]) != t],
                    "system": "quality_issues" not in have}
        finally:
            backend.finish_read(conn)


def ensure_or_verify(backend, domain: str, fields, auto: bool) -> None:
    if auto:
        sync_table(backend, domain, fields)
        return
    gaps = schema_gaps(backend, domain, fields)
    problems = []
    if gaps is None:
        problems.append(f"table {table_name(domain)} does not exist")
    else:
        if gaps["missing"]:
            problems.append(f"it lacks column(s) {gaps['missing']}")
        if gaps["retype"]:
            problems.append("column type(s) differ: " + ", ".join(f"{f} is {have}, the config says {want}" for f, have, want in gaps["retype"]))
        if gaps["system"]:
            problems.append("it has no quality_issues column")
    if problems:
        raise MigrationError(f"Golden-record schema for domain '{domain}' is not up to date ({'; '.join(problems)}) and "
                             f"auto-migration is disabled (MDM_AUTO_MIGRATE). Run:  python3 domains_cli.py reindex {domain}")
