"""
Apply, verify, or remove Row-Level Security policies on the Postgres
backend. See app/rls_policies.py's module docstring FIRST -- this is
the least-tested piece of this whole project (never run against a real
Postgres server), and RLS has a specific, easy-to-hit failure mode
(connecting as a superuser silently makes the policies do nothing) that
the `verify` command below exists specifically to catch.

    # 1. Create a restricted role for your app to actually connect as
    #    (skip if you already have one that isn't a superuser/owner)
    python3 setup_rls.py create-role --role-name mdm_app --password CHANGE_ME

    # 2. Apply the policies (run as a role that OWNS the tables, e.g.
    #    whatever role ran seed_demo.py / created the schema -- often
    #    just your default `postgres` superuser connection)
    python3 setup_rls.py enable

    # 3. Point DATABASE_URL at the RESTRICTED role from step 1, then verify
    export DATABASE_URL=postgresql://mdm_app:CHANGE_ME@localhost:5432/mdm_dev
    python3 setup_rls.py verify

    # To roll back:
    python3 setup_rls.py disable
"""
import argparse
import sys

from app.domain_config import load_configured_domains
from app.rls_policies import generate_disable_rls_sql, generate_rls_sql, tenant_scoped_tables


def _get_connection(db_url: str | None):
    import os
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 is not installed. Run: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)
    url = db_url or os.environ.get("DATABASE_URL")
    if not url or not url.startswith(("postgresql://", "postgres://")):
        print("This only applies to the Postgres backend. Set DATABASE_URL to a "
              "postgresql:// connection string (via --db or the env var).", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(url)


def cmd_enable(args):
    conn = _get_connection(args.db)
    try:
        with conn.cursor() as cur:
            cur.execute(generate_rls_sql())
        conn.commit()
        print(f"RLS policies applied to: {', '.join(tenant_scoped_tables())}")
        print("\n⚠️  This does NOTHING if your app connects as a superuser or as these "
              "tables' owner. Run `python3 setup_rls.py verify` AS YOUR APP'S ACTUAL "
              "RUNTIME ROLE to confirm RLS is genuinely in effect, not just 'enabled'.")
    finally:
        conn.close()


def cmd_disable(args):
    conn = _get_connection(args.db)
    try:
        with conn.cursor() as cur:
            cur.execute(generate_disable_rls_sql())
        conn.commit()
        print("RLS policies removed.")
    finally:
        conn.close()


def cmd_verify(args):
    conn = _get_connection(args.db)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, usesuper FROM pg_user WHERE usename = current_user;")
            user, is_super = cur.fetchone()
            cur.execute("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user;")
            (bypasses_rls,) = cur.fetchone()

            print(f"Connected as: {user}")
            problems = []
            if is_super:
                problems.append(f"'{user}' IS A SUPERUSER -- RLS policies are completely "
                                 f"invisible to this role. Every query sees every tenant's rows.")
            if bypasses_rls:
                problems.append(f"'{user}' has the BYPASSRLS attribute -- same effect as superuser.")

            for table in tenant_scoped_tables():
                cur.execute(
                    "SELECT tableowner FROM pg_tables WHERE tablename = %s;", (table,)
                )
                row = cur.fetchone()
                if row and row[0] == user:
                    problems.append(f"'{user}' OWNS table '{table}' -- FORCE ROW LEVEL SECURITY "
                                     f"should cover this, but double check pg_tables.rowsecurity "
                                     f"and pg_tables.forcerowsecurity for '{table}' directly if "
                                     f"anything looks off.")

            if problems:
                print("\n❌ RLS is NOT effectively protecting this connection:")
                for p in problems:
                    print(f"   - {p}")
                print(f"\nFix: create a restricted role (see `python3 setup_rls.py create-role "
                      f"--help`) and point DATABASE_URL at it instead of '{user}'.")
                sys.exit(1)
            else:
                print("✅ This role is not a superuser, doesn't bypass RLS, and doesn't own the "
                      "tenant-scoped tables -- RLS policies should be genuinely enforced for it.")
                print("\nThis does NOT confirm the policies themselves behave correctly under "
                      "real queries -- only that this role isn't automatically exempt from them. "
                      "Test with two different tenant_ids and confirm cross-tenant rows are "
                      "actually invisible before trusting this in production.")
    finally:
        conn.close()


def cmd_create_role(args):
    conn = _get_connection(args.db)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DO $$ BEGIN "
                f"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{args.role_name}') THEN "
                f"CREATE ROLE {args.role_name} LOGIN PASSWORD %s NOSUPERUSER NOBYPASSRLS; "
                f"END IF; END $$;",
                (args.password,),
            )
            for table in tenant_scoped_tables() + ["api_keys"]:
                cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {args.role_name};")
        conn.commit()
        print(f"Role '{args.role_name}' created (or already existed) with NOSUPERUSER NOBYPASSRLS, "
              f"and granted table access. It does NOT own these tables, so FORCE ROW LEVEL "
              f"SECURITY applies to it once you run `enable`.")
        print(f"\nNow point your app at it:")
        print(f'  export DATABASE_URL=postgresql://{args.role_name}:***@localhost:5432/<your_db>')
    finally:
        conn.close()


def main():
    load_configured_domains()  # custom domains have their own golden-record tables
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=None, help="postgresql:// URL (default: DATABASE_URL env var)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("enable", help="Apply RLS policies to all tenant-scoped tables")
    sub.add_parser("disable", help="Remove RLS policies (rollback)")
    sub.add_parser("verify", help="Check whether the CURRENT connection's role is actually subject to RLS")

    p_role = sub.add_parser("create-role", help="Create a restricted role for the app to run as")
    p_role.add_argument("--role-name", required=True)
    p_role.add_argument("--password", required=True)

    args = parser.parse_args()
    {"enable": cmd_enable, "disable": cmd_disable, "verify": cmd_verify,
     "create-role": cmd_create_role}[args.command](args)


if __name__ == "__main__":
    main()
