"""
Create, list, and revoke API keys directly against the database --
bypassing the HTTP API entirely, which is how you bootstrap the very
first key (you can't call POST /v1/admin/api-keys without already
having one once MDM_ENABLE_AUTH=true).

    python3 manage_api_keys.py create --tenant acme --role tenant_admin --label "root key"
    python3 manage_api_keys.py create --tenant acme --role data_steward --label "Jane" --domains customer,product
    python3 manage_api_keys.py list --tenant acme
    python3 manage_api_keys.py revoke --tenant acme --key-id key_abc123

Typical bootstrap flow:
    python3 manage_api_keys.py create --tenant acme --role tenant_admin --label "root key"
    # copy the printed key, then:
    export MDM_ENABLE_AUTH=true
    uvicorn app.api:app --reload --port 8000
    curl -H "Authorization: Bearer <the key>" http://localhost:8000/v1/customer/entities
"""
import argparse

from app.auth import ROLES, generate_api_key
from app.models import ApiKeyRecord, now_iso
from app.store_factory import get_store


def cmd_create(args, store):
    if args.role not in ROLES:
        print(f"Unknown role '{args.role}'. Valid roles: {ROLES}")
        return
    allowed_domains = args.domains.split(",") if args.domains else None
    plaintext, key_hash = generate_api_key()
    record = ApiKeyRecord(tenant_id=args.tenant, role=args.role, key_hash=key_hash,
                           label=args.label, allowed_domains=allowed_domains)
    store.save_api_key(record)
    print(f"Created key_id={record.id} for tenant='{args.tenant}' role='{args.role}'")
    print(f"Allowed domains: {allowed_domains or 'ALL'}")
    print()
    print(f"API KEY (shown once, store it securely): {plaintext}")


def cmd_list(args, store):
    keys = store.list_api_keys(args.tenant)
    if not keys:
        print(f"No API keys found for tenant '{args.tenant}'.")
        return
    for k in keys:
        status = f"REVOKED at {k.revoked_at}" if k.revoked_at else "active"
        print(f"{k.id}  role={k.role:14s}  label={k.label!r:30s}  "
              f"domains={k.allowed_domains or 'ALL'}  [{status}]")


def cmd_revoke(args, store):
    store.revoke_api_key(args.key_id, args.tenant, now_iso())
    print(f"Revoked key_id={args.key_id} for tenant '{args.tenant}'.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=None, help="SQLite path or postgresql:// URL (default: DATABASE_URL env or mdm.db)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="Create a new API key")
    p_create.add_argument("--tenant", required=True)
    p_create.add_argument("--role", required=True, choices=list(ROLES))
    p_create.add_argument("--label", required=True)
    p_create.add_argument("--domains", default=None, help="Comma-separated domain list, e.g. customer,product. Omit for all domains.")

    p_list = sub.add_parser("list", help="List API keys for a tenant")
    p_list.add_argument("--tenant", required=True)

    p_revoke = sub.add_parser("revoke", help="Revoke an API key")
    p_revoke.add_argument("--tenant", required=True)
    p_revoke.add_argument("--key-id", required=True)

    args = parser.parse_args()
    store = get_store(args.db)

    {"create": cmd_create, "list": cmd_list, "revoke": cmd_revoke}[args.command](args, store)


if __name__ == "__main__":
    main()
