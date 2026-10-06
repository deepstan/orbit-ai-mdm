"""
Manage customer-defined domains and their blocking indexes.

    python3 domains_cli.py validate [FILE ...]   # check config files (default: the whole config directory)
    python3 domains_cli.py list                  # every domain the app will serve, and where it came from
    python3 domains_cli.py export customer       # dump a domain as an editable config document
    python3 domains_cli.py normalizers           # the standardization steps a config may use
    python3 domains_cli.py status                # is each domain's blocking index current?
    python3 domains_cli.py reindex location      # rebuild a domain's index after a config change

Config files live in $MDM_DOMAINS_DIR, or ./domains (override per command with --domains-dir).
Files are read at startup: restart the app after editing one.

Typical workflow for changing a built-in domain:
    python3 domains_cli.py export customer > domains/customer.json   # has "override": true already
    (edit domains/customer.json)
    python3 domains_cli.py validate
    python3 domains_cli.py status        # STALE if you changed blocking / a blocking field's standardization
    python3 domains_cli.py reindex customer
    (restart the app)

Exit codes: 0 fine; 1 problems found (invalid config, or an index that needs rebuilding); 2 error.
"""
import argparse
import json
import sys

from app.domain_config import (
    DomainConfigError,
    check_applicable,
    config_to_document,
    load_configured_domains,
    load_domain_file,
)
from app.domains import REGISTRY, get_domain_config
from app.indexing import IndexStaleError, index_state, reindex_domain
from app.normalizers import NORMALIZER_DOCS
from app.store_factory import get_store


def cmd_validate(args) -> int:
    problems = 0
    if args.paths:
        configs = []
        for path in args.paths:
            try:
                cfg = load_domain_file(path)
            except DomainConfigError as e:
                print(f"{e}\n")
                problems += 1
                continue
            configs.append(cfg)
            print(f"OK  {path}: domain '{cfg.name}', {len(cfg.schema_fields)} fields, "
                  f"{len(cfg.blocking_field_groups)} blocking group(s)")
        for message in check_applicable(configs):
            print(f"{message}\n")
            problems += 1
    else:
        try:
            names = load_configured_domains(args.domains_dir)
        except DomainConfigError as e:
            print(e)
            return 1
        print(f"Loaded and valid: {', '.join(names)}" if names else
              "No domain config found (looked in --domains-dir, $MDM_DOMAINS_DIR, then ./domains).")
    return 1 if problems else 0


def cmd_list(args) -> int:
    load_configured_domains(args.domains_dir)
    print(f"{'DOMAIN':16s} {'SOURCE':44s} {'FIELDS':>6s}  BLOCKING GROUPS")
    for name, cfg in REGISTRY.items():
        print(f"{name:16s} {cfg.source:44s} {len(cfg.schema_fields):6d}  {len(cfg.blocking_field_groups)}")
    return 0


def cmd_export(args) -> int:
    load_configured_domains(args.domains_dir)
    print(json.dumps(config_to_document(get_domain_config(args.domain)), indent=2))
    return 0


def cmd_normalizers(args) -> int:
    width = max(len(n) for n in NORMALIZER_DOCS)
    for name, doc in NORMALIZER_DOCS.items():
        print(f"  {name:{width}s}  {doc}")
    return 0


def cmd_status(args) -> int:
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    needs_action = 0
    print(f"{'DOMAIN':16s} {'INDEX':12s} {'RECORDS':>9s}  SOURCE")
    for name, cfg in REGISTRY.items():
        state = index_state(store, name, cfg)
        hint = ""
        if state not in ("READY", "EMPTY"):
            needs_action += 1
            hint = f"   <- python3 domains_cli.py reindex {name}"
        print(f"{name:16s} {state:12s} {store.count_source_records(name):9d}  {cfg.source}{hint}")
    return 1 if needs_action else 0


def cmd_reindex(args) -> int:
    load_configured_domains(args.domains_dir)
    if args.domain not in REGISTRY:
        print(f"Unknown domain '{args.domain}'. Known: {', '.join(REGISTRY)}", file=sys.stderr)
        return 2
    store = get_store(args.db)
    state = index_state(store, args.domain, REGISTRY[args.domain])
    if state in ("READY", "EMPTY") and not args.force:
        print(f"Domain '{args.domain}' is already {state}: nothing to do (use --force to rebuild anyway).")
        return 0

    def progress(done, total):
        print(f"  {done}/{total} records")

    print(f"Reindexing '{args.domain}' (was {state}). Ingestion into it is paused until this finishes.")
    result = reindex_domain(store, args.domain, batch_size=args.batch_size, progress=progress)
    print(f"\nDone: {result.records} record(s) in {result.batches} batch(es), {result.seconds:.1f}s. "
          f"Ingestion into '{args.domain}' is enabled again.")
    print("Note: this rebuilt the candidate index only. It does NOT re-run matching over existing records, "
          "so duplicates the new rules would have caught stay unmerged until a new record matches them.")
    if args.restart_hint:
        print("Restart any running app instances so they load the same configuration.")
    return 0


def main() -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help="SQLite path or postgresql:// URL (default: DATABASE_URL, else mdm.db)")
    common.add_argument("--domains-dir", default=argparse.SUPPRESS, help="Config directory (default: $MDM_DOMAINS_DIR, else ./domains)")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--domains-dir", default=None, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate", parents=[common], help="Check config files")
    p.add_argument("paths", nargs="*")
    sub.add_parser("list", parents=[common], help="List all domains")
    p = sub.add_parser("export", parents=[common], help="Print a domain as an editable config document")
    p.add_argument("domain")
    sub.add_parser("normalizers", parents=[common], help="List the available standardization steps")
    sub.add_parser("status", parents=[common], help="Is each domain's blocking index current?")
    p = sub.add_parser("reindex", parents=[common], help="Rebuild a domain's blocking index")
    p.add_argument("domain")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--force", action="store_true", help="Rebuild even if the index looks current")
    p.add_argument("--restart-hint", action="store_true", help=argparse.SUPPRESS)

    args = parser.parse_args()
    handlers = {"validate": cmd_validate, "list": cmd_list, "export": cmd_export,
                "normalizers": cmd_normalizers, "status": cmd_status, "reindex": cmd_reindex}
    try:
        return handlers[args.command](args)
    except (DomainConfigError, IndexStaleError, KeyError) as e:
        print(f"\nError: {e.args[0] if isinstance(e, KeyError) else e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
