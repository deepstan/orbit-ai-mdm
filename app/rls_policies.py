"""
Row-Level Security policies -- defense-in-depth BENEATH the application-
level `WHERE tenant_id = ...` filtering every query in db_postgres.py
already does. Enforced by Postgres itself, so even a bug in application
code (a forgotten WHERE clause, a copy-paste error) can't leak one
tenant's rows to another -- the database refuses to return them.

⚠️ THIS IS THE LEAST-VERIFIED PIECE IN THE ENTIRE CODEBASE. Everything
else here was at minimum logic-tested against a mock. RLS is fundamentally
different: it's not application code, it's a database access-control
POLICY plus a PRIVILEGE SETUP that only means something when both are
correct together, on a real server, with a real non-superuser role. There
is no meaningful way to mock "does Postgres actually deny this row" --
it either works against a real Postgres instance or it doesn't, and this
has never been run against one. Validate it yourself (see the bottom of
this file) before trusting it for anything real.

WHY THIS IS OPT-IN, NOT AUTOMATIC:
Enabling RLS changes query behavior -- a role that ISN'T exempted (see
below) will silently see zero rows for any query that doesn't have
app.current_tenant_id set correctly first. That's a serious behavior
change to apply automatically inside `_init_schema()` on every app
startup. This module is applied deliberately, once, by someone with
the privileges to run CREATE POLICY -- via setup_rls.py.

CRITICAL PREREQUISITE -- read this before running setup_rls.py:
RLS policies are IGNORED by:
  1. Superuser roles (e.g. the default `postgres` role)
  2. The table's OWNER, unless FORCE ROW LEVEL SECURITY is also set
     (this module sets FORCE on every table, closing that gap)
  3. Any role with the BYPASSRLS attribute

This means: if your app connects to Postgres AS `postgres` (the default
superuser, which is exactly what the Docker/local-install quickstart in
the README's Postgres section has you do), RLS policies are COMPLETELY
INVISIBLE to your app -- every query still sees every row, silently.
The policies would be sitting there, technically "enabled," doing
nothing. This is not a bug in this module -- it's how Postgres RLS
works -- but it's the single most common way people accidentally
believe RLS is protecting them when it isn't.

To actually be protected, your app's runtime DB role must be:
  - NOT a superuser
  - NOT the owner of these tables (have a separate owner/migration role
    create the schema, and a separate, more restricted role run the app)
  - NOT granted BYPASSRLS

See setup_rls.py for a role-creation example that gets this right.
"""

# One tenant-isolation policy per tenant-scoped table, all following the
# identical pattern: a row is visible only if its tenant_id matches the
# session variable app.current_tenant_id that db_postgres.py's _conn()
# sets via SET LOCAL before every scoped query.
#
# current_setting(..., true) -- the `true` (missing_ok) argument means
# "return NULL instead of raising an error if the session variable was
# never set," which matters a lot here: if some code path forgets to
# pass tenant_id (a bug), the policy FAILS CLOSED -- NULL never equals
# any real tenant_id, so that query sees ZERO rows rather than every
# tenant's rows. Failing closed is the safe direction for a security
# policy: a bug here causes "nothing shows up" (a support ticket you'll
# hear about immediately), never "wrong tenant's data leaked" (the
# failure mode this whole feature exists to prevent).
TENANT_SCOPED_TABLES = [
    "source_records",
    "match_candidates",
    "lineage",
    "blocking_keys",
    "source_record_history",
    "merge_audits",
    "agent_recommendations",
    "pipeline_runs",
    "golden_dirty",
    "standardized_records",
    "audit_log",
    "usage_events",
    # NOTE: api_keys deliberately excluded -- see the module docstring
    # and db_postgres.py's get_api_key_by_hash() for why. Authentication
    # has to be able to find a key WITHOUT already knowing its tenant.
]


def tenant_scoped_tables(domains=None) -> list:
    """The fixed tables plus one golden-record table per domain (default: every registered domain). Re-run
    `setup_rls.py enable` after adding a custom domain so its new table gets a policy."""
    from .domains import REGISTRY
    return TENANT_SCOPED_TABLES + [f"entity_{d}" for d in (domains if domains is not None else REGISTRY)]


def generate_rls_sql(tables=None) -> str:
    statements = []
    for table in (tables if tables is not None else tenant_scoped_tables()):
        statements.append(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        statements.append(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
        statements.append(f"""
DROP POLICY IF EXISTS tenant_isolation ON {table};
CREATE POLICY tenant_isolation ON {table}
    USING (tenant_id = current_setting('app.current_tenant_id', true));
""".strip())
    return "\n\n".join(statements)


def generate_disable_rls_sql(tables=None) -> str:
    """The reverse operation -- drops the policies and disables RLS.
    For rolling back if something goes wrong, or for local dev where
    you've decided RLS is more friction than it's worth right now."""
    statements = []
    for table in (tables if tables is not None else tenant_scoped_tables()):
        statements.append(f"DROP POLICY IF EXISTS tenant_isolation ON {table};")
        statements.append(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;")
    return "\n".join(statements)
