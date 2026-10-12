"""
Manage customer-defined domains and their blocking indexes.

    python3 domains_cli.py validate [FILE ...]   # check config files (default: the whole config directory)
    python3 domains_cli.py list                  # every domain the app will serve, and where it came from
    python3 domains_cli.py export customer       # dump a domain as an editable config document
    python3 domains_cli.py normalizers           # the standardization steps a config may use
    python3 domains_cli.py status                # is each domain's blocking index current? any unfinished records?
    python3 domains_cli.py process-pending customer   # finish accepted/interrupted/failed records (safe to run on a schedule)
    python3 domains_cli.py profile person --file new_source.csv --mapping "ssn=SSN,dob=DOB,first_name=First,last_name=Last"   # BEFORE loading a new source
    python3 domains_cli.py runs [DOMAIN]          # what recent batch runs received, and what became of it
    python3 domains_cli.py schema customer       # the golden-record table and columns a report can query
    python3 domains_cli.py blocking-stats [DOMAIN]  # how big are the buckets? which values are suspiciously common?
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

from app.protect import ProtectionKeyMissing
from app.domain_config import (
    DomainConfigError,
    check_applicable,
    config_to_document,
    load_configured_domains,
    load_domain_file,
)
from app.domains import REGISTRY, get_domain_config
from app.indexing import IndexStaleError, index_state, reindex_domain
from app.normalizers import NORMALIZER_DOCS, PRESENTER_DOCS
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
            for warning in cfg.warnings:
                print(f"    warning: {warning}")
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
    print("Matching steps (normalize): how a value is compared")
    for name, doc in NORMALIZER_DOCS.items():
        print(f"  {name:{width}s}  {doc}")
    width = max(len(n) for n in PRESENTER_DOCS)
    print("\nField types (type): the golden column's type; staging stays text. date/timestamp take a strptime `format`.")
    print("  " + ", ".join(__import__("app.fieldtypes", fromlist=["FIELD_TYPES"]).FIELD_TYPES))
    print("\nPresentation steps (present): how a value looks in the golden record")
    for name, doc in PRESENTER_DOCS.items():
        print(f"  {name:{width}s}  {doc}")
    return 0


def cmd_status(args) -> int:
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    needs_action = 0
    print(f"{'DOMAIN':16s} {'INDEX':12s} {'RECORDS':>9s} {'STAGED':>9s} {'DUPES':>6s} {'PENDING':>8s} {'FAILING':>8s}  SOURCE")
    for name, cfg in REGISTRY.items():
        state = index_state(store, name, cfg)
        pending, failing = store.count_unfinished(name)
        hint = ""
        if failing:
            needs_action += 1  # records that errored and are waiting for a retry: someone should look
            hint = f"   <- {failing} record(s) failed: see `domains_cli.py runs {name}`, then `process-pending {name}`"
        elif pending:
            hint = f"   <- {pending} record(s) accepted/unfinished: `python3 domains_cli.py process-pending {name}`"
        if state not in ("READY", "EMPTY"):
            needs_action += 1
            hint = f"   <- python3 domains_cli.py reindex {name}"
        print(f"{name:16s} {state:12s} {store.count_source_records(name):9d} {store.count_staged(name):9d} {store.count_duplicate_raw_keys(name):6d} {pending:8d} {failing:8d}  {cfg.source}{hint}")
    return 1 if needs_action else 0


def cmd_schema(args) -> int:
    """The golden-record table of a domain: what a BI tool or an analyst queries."""
    import os
    from app.golden import SYSTEM_COLUMNS, field_columns, q, schema_gaps, table_name
    from app.migrations import backend_for_url
    load_configured_domains(args.domains_dir)
    cfg = get_domain_config(args.domain)
    table, columns = table_name(args.domain), field_columns(cfg)
    gaps = schema_gaps(backend_for_url(args.db or os.environ.get("DATABASE_URL", "mdm.db")), args.domain, columns)
    print(f"{table}   (one row per golden record)\n")
    print("  system columns:  " + ", ".join(SYSTEM_COLUMNS))
    print("                   (quality_issues lists values that could not be cast to their field's type; the value is NULL)")
    print("  field columns:")
    for f, t in columns:
        fmt = cfg.field_specs[f].formats if f in cfg.field_specs else ()
        print(f"    {f:26s} {t}" + (f"   format: {' | '.join(fmt)}" if fmt else ""))
    if gaps is None:
        print(f"\n  NOT CREATED YET: run  python3 domains_cli.py reindex {args.domain}")
    elif gaps["missing"] or gaps["retype"] or gaps["system"]:
        print(f"\n  OUT OF DATE (missing {gaps['missing']}, retype {[g[0] for g in gaps['retype']]}): run  python3 domains_cli.py reindex {args.domain}")
    first = [f for f, _ in columns[:2]]
    print(f"\n  example:\n    SELECT {', '.join(q(f) for f in first)} FROM {q(table)} WHERE status = 'active';")
    print("\n  Columns are only ever added: a field you remove from the config keeps its column (dropping it would destroy data).")
    return 0


def cmd_process_pending(args) -> int:
    """Finishes whatever is unfinished, oldest first. Safe to run any time and as often as you like (cron, Airflow, ...)."""
    from app.pipeline import process_pending
    load_configured_domains(args.domains_dir)
    if args.domain not in REGISTRY:
        print(f"Unknown domain '{args.domain}'. Known: {', '.join(REGISTRY)}", file=sys.stderr)
        return 2
    store = get_store(args.db)
    out = process_pending(store, args.domain, limit=args.limit, max_attempts=args.max_attempts)
    if not out["runs"]:
        print(f"Nothing to finish for '{args.domain}'.")
        return 0
    print(f"Finished {out['finished']} record(s), {out['errors']} failed, {out['golden_rebuilt']} golden record(s) rebuilt.")
    for run in out["runs"]:
        print(f"  {run['run_id']}  {run['status']}")
    if out["remaining"]:
        print(f"  {out['remaining']} more record(s) are still unfinished (--limit {args.limit} per pass): run it again, or raise --limit.")
    for run in out["runs"]:
        for e in run["errors"][:5]:
            print(f"    {e['source_system']}/{e['source_record_id']}: {e['error']}")
    return 1 if out["errors"] else 0


def cmd_runs(args) -> int:
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    runs = store.list_runs(domain=args.domain, limit=args.limit)
    if not runs:
        print("No runs recorded yet.")
        return 0
    print(f"{'STARTED':19s} {'RUN':28s} {'DOMAIN':10s} {'MODE':8s} {'STATUS':22s} COUNTS")
    for r in runs:
        c = {k: v for k, v in r["counts"].items() if v}
        print(f"{r['started_at'][:19]:19s} {r['run_id']:28s} {r['domain']:10s} {r['mode']:8s} {r['status']:22s} "
              + (", ".join(f"{k}={v}" for k, v in c.items()) or "-"))
    return 0


def cmd_profile(args) -> int:
    """Finds placeholder values the rules cannot know about. Run it on a new source's file BEFORE loading it."""
    import json
    from app.profile import format_profile, profile_records
    load_configured_domains(args.domains_dir)
    if args.domain not in REGISTRY:
        print(f"Unknown domain '{args.domain}'. Known: {', '.join(REGISTRY)}", file=sys.stderr)
        return 2
    if args.file:
        from app.connectors import CSVConnector, JSONConnector
        from bulk_ingest import parse_inline_mapping
        if args.mapping_file:
            with open(args.mapping_file) as f:
                mapping = json.load(f)
        elif args.mapping:
            mapping = parse_inline_mapping(args.mapping)
        else:
            print("Error: --file needs --mapping or --mapping-file", file=sys.stderr)
            return 2
        if args.file.lower().endswith(".json"):
            connector = JSONConnector(args.file, mapping, source_record_id_field=args.id_column, source_system=args.source_system or "json_import")
        else:
            connector = CSVConnector(args.file, mapping, source_record_id_column=args.id_column, source_system=args.source_system or "csv_import")
        records = (attrs for _, attrs in connector.fetch_records())
    else:
        if not args.tenant:
            print("Error: give --tenant to profile loaded data, or --file to profile a file before loading it", file=sys.stderr)
            return 2
        records = get_store(args.db).iter_raw_attributes(args.domain, args.tenant, args.source_system)
    try:
        report = profile_records(args.domain, records, fields=args.field, min_count=args.min_count, min_names=args.min_names, reveal=args.reveal)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2) if args.json else format_profile(report))
    return 1 if args.fail_on_suspects and any(f["suspects"] for f in report["fields"].values()) else 0


def cmd_blocking_stats(args) -> int:
    """Bucket analysis. The validator can't tell whether your blocking is any good (that depends on your data);
    this shows it: how big the buckets are, what the cost of an ingest is, and which values are suspiciously common."""
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    names = [args.domain] if args.domain else list(REGISTRY)
    for name in names:
        if name not in REGISTRY:
            print(f"Unknown domain '{name}'. Known: {', '.join(REGISTRY)}", file=sys.stderr)
            return 2
        cfg, total = REGISTRY[name], store.count_source_records(name)
        print(f"\n{name}   {total:,} records   (a key held by more than {cfg.max_bucket_size} other records is skipped at ingest)")
        if not total:
            print("  no records yet")
            continue
        st = store.blocking_key_stats(name, args.top)
        expected = st["sum_squares"] / st["key_rows"] if st["key_rows"] else 0
        print(f"  buckets: {st['buckets']:,}    key rows: {st['key_rows']:,}    largest bucket: {st['biggest']:,}")
        print(f"  size of the bucket a typical key lands in: {expected:.1f}   <- the cost driver: roughly how many records each lookup returns")
        unkeyed = st["records_without_keys"]
        print(f"  records with NO usable blocking key: {unkeyed:,} ({unkeyed / total:.1%})" +
              ("   <- these can never be matched; their blocking fields are empty" if unkeyed else ""))
        print("  buckets by size:   " + "   ".join(f"{band}: {st['bands'].get(band, 0):,}" for band in ("1", "2-10", "11-100", "101-1000", "1000+")))
        keyspecs = cfg.keyspecs()
        print(f"  largest buckets:")
        for tenant, key, n in st["top"]:
            idx = int(key[1:].split(":", 1)[0]) if key.startswith("g") and key[1:2].isdigit() else None
            group = keyspecs[idx] if idx is not None and idx < len(keyspecs) else None
            fields = "+".join(spec.field for spec in group) if group else "?"
            notes = []
            if n - 1 > cfg.max_bucket_size:
                notes.append("OVER CAP: skipped at ingest")
            if group and len(group) == 1 and group[0].kind in ("auto", "exact") and n >= 10:
                notes.append(f"one {fields} value shared by {n} records: if it's a placeholder, add it to that field's `anonymous` list, then reindex")
            print(f"    {n:6,}  {key[:56]:56s}  [{fields}]" + (f"   <- {'; '.join(notes)}" if notes else ""))
    return 0


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
    print(f"\nDone in {result.seconds:.1f}s: {result.records} record(s) re-standardized into the staging layer "
          f"(blocking keys rebuilt with them), {result.entities} golden record(s) re-derived. "
          f"Raw data was not touched. Ingestion into '{args.domain}' is enabled again.")
    print("Note: this rebuilt the candidate index only. It does NOT re-run matching over existing records, "
          "so duplicates the new rules would have caught stay unmerged until a new record matches them.")
    if args.restart_hint:
        print("Restart any running app instances so they load the same configuration.")
    return 0


def cmd_audit(args) -> int:
    """Checks of automatic merges by a person: the labels that certify a threshold. See app/learned_scorer.py."""
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    if args.action == "sample":
        n = store.sample_merge_audit(args.tenant, args.domain, args.rate, args.seed)
        print(f"Flagged {n:,} automatic merges for a person to check. Proving 1 false merge in 2,000 takes about 6,000 checked merges with no error; "
              f"at a {args.rate:.0%} sample that is about {int(6000 / args.rate):,} automatic merges in all.")
        return 0
    if args.action == "sample-review":
        import random
        from app import learned_scorer as ls
        scorer = ls.TreeScorer.from_json(open(args.model).read()) if args.model else ls.load_scorer(args.tenant, args.domain)
        if scorer is None:
            print("No exported scorer for this tenant and domain: train one (train_scorer.py) or pass --model.", file=sys.stderr)
            return 2
        floor = args.min_p if args.min_p is not None else (scorer.policy.get("promote_threshold") or scorer.policy.get("promote_threshold_uncertified"))
        if floor is None:
            print("The model has no threshold to aim at; pass --min-p.", file=sys.stderr)
            return 2
        have = {a["candidate_id"] for a in store.list_merge_audits(args.tenant, args.domain)}
        high = [c.id for c in store.list_candidates(args.tenant, args.domain, status="pending")
                if c.id not in have and scorer.score_candidate(c.score, c.match_type, c.contributions) >= floor]
        rng = random.Random(args.seed)
        n = store.flag_merge_audits(args.tenant, args.domain, [i for i in high if rng.random() < args.rate])
        print(f"{len(high):,} review pairs score at or above p = {floor:.4f}; flagged {n:,} of them (a random {args.rate:.0%}) for a person to check. "
              f"These audits are what can widen the certificate to promote pairs out of review.")
        return 0
    if args.action == "list":
        rows = store.list_merge_audits(args.tenant, args.domain, pending_only=args.pending)
        for r in rows:
            print(f"{r['candidate_id']}  score {r['score']:.4f}  {r['verdict'] or 'WAITING':8s} {r['audited_by'] or ''}")
        print(f"{len(rows):,} audits" + (" waiting" if args.pending else ""))
        return 0
    if not (args.candidate and args.verdict and args.by):
        print("verdict needs --candidate, --verdict and --by", file=sys.stderr)
        return 2
    try:
        done = store.record_merge_audit(args.candidate, args.tenant, args.domain, args.verdict, args.by)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    print("Recorded." if done else "That pair was never flagged for audit.")
    return 0 if done else 1


def cmd_unmerge(args) -> int:
    """Undo ONE confirmed merge a person says is wrong; split the entity along it. --dry-run shows what would move."""
    from app.unmerge import CannotUnmerge, unmerge_pair
    load_configured_domains(args.domains_dir)
    try:
        r = unmerge_pair(get_store(args.db), args.tenant, args.domain, args.candidate, args.by, args.reason or "", dry_run=args.dry_run)
    except CannotUnmerge as e:
        print(f"Not unmerged: {e}", file=sys.stderr)
        return 2
    if r.get("dry_run"):
        print(f"DRY RUN. Unmerging {r['candidate_id']} would " + ("only record the separation (the records are already in different entities)." if r["already_apart"] else
              f"move {len(r['move'])} record(s) out of entity {r['entity_id']} into a NEW entity; {len(r['keep'])} stay" + (f"; {len(r['left_alone'])} that no confirmed pair reaches stay too" if r["left_alone"] else "") + "."))
        return 0
    print(f"Unmerged {candidate_id_of(r)}: " + (f"entity {r['kept_entity']} keeps {len(r['keep'])} record(s); {len(r['moved'])} moved to the new entity {r['new_entity']}. Both golden records are rebuilt." if r["split"] else r["note"]))
    print("These two records will not be merged again by any engine: an automatic merge that would reunite them is held for review (separation-check verifies it).")
    return 0


def candidate_id_of(r: dict) -> str:
    return r["candidate_id"]


def cmd_separation_check(args) -> int:
    """Separations a person made that an engine has undone: two records a person said are different, sitting in one entity again."""
    from app.unmerge import separation_violations
    load_configured_domains(args.domains_dir)
    bad = separation_violations(get_store(args.db), args.tenant, args.domain)
    if not bad:
        print("Every separation a person made still holds.")
        return 0
    print(f"{len(bad)} separation(s) have been undone (a person said these records are different; they are in one entity again):")
    for cid, a, b, entity in bad[:50]:
        print(f"  {cid}: {a} and {b} are both in {entity}")
    print("Unmerge the pair that joins them (`unmerge`), or tell me if the person was wrong.")
    return 3


def cmd_scorer(args) -> int:
    """status | check | reset: the learned scorer's live state and its automatic rollback (app/monitor.py)."""
    from app import learned_scorer as ls
    from app.monitor import assess, check_scorer, format_check
    load_configured_domains(args.domains_dir)
    store = get_store(args.db)
    scorer = ls.load_scorer(args.tenant, args.domain)
    if args.action == "reset":
        ls.write_state(args.tenant, args.domain, "active", f"reset by {args.by or 'an operator'}")
        print("Promotion is switched back ON (if the model has a live threshold). Run `scorer check` on a schedule.")
        return 0
    if scorer is None:
        print("No exported scorer for this tenant and domain.", file=sys.stderr)
        return 2
    if args.action == "status":
        pol, state = scorer.policy, ls.read_state(args.tenant, args.domain)
        live = pol.get("promote_threshold")
        print(f"Model {scorer.model_id}; state {state.get('status', 'active').upper()}" + (f" ({state.get('reason')})" if state.get("status") == "rolled_back" else "") + ".")
        print(f"  Promotion: " + (f"LIVE at p >= {live:.4f}" if live is not None else "NOT live") + (f"; a proposed threshold p >= {pol['promote_threshold_uncertified']:.4f} exists" if live is None and pol.get("promote_threshold_uncertified") is not None else "")
              + f". Audit rate {ls.audit_rate():.0%}. Target {pol.get('target_false_rate', 0):.4%}.")
        print(format_check({**assess(store, args.tenant, args.domain, pol.get("target_false_rate", 0.0005), pol.get("confidence", 0.95)), "state": state.get("status", "active")}))
        return 0
    result = check_scorer(store, args.tenant, args.domain)
    print(format_check(result))
    return 3 if result.get("state") == "rolled_back" else 0


def cmd_shadow_report(args) -> int:
    from app import learned_scorer as ls
    from app.shadow import format_shadow_report, shadow_report
    load_configured_domains(args.domains_dir)
    scorer = ls.TreeScorer.from_json(open(args.model).read()) if args.model else ls.load_scorer(args.tenant, args.domain)
    if scorer is None:
        print("No exported scorer for this tenant and domain: train one with train_scorer.py, or pass --model.", file=sys.stderr)
        return 2
    print(format_shadow_report(shadow_report(get_store(args.db), args.tenant, args.domain, scorer)))
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
    p = sub.add_parser("profile", parents=[common], help="Find placeholder values (SSN/DOB) before they merge strangers; run on a new source's file before loading it")
    p.add_argument("domain")
    p.add_argument("--file", help="a .csv or .json file to profile BEFORE loading it (needs --mapping or --mapping-file)")
    p.add_argument("--mapping", help='"canonical=source_column,..." as for bulk_ingest.py')
    p.add_argument("--mapping-file")
    p.add_argument("--id-column")
    p.add_argument("--tenant", help="profile data already loaded for this tenant instead of a file")
    p.add_argument("--source-system", help="profile one source system only")
    p.add_argument("--field", action="append", help="field to profile (repeatable); default: every ssn/dob field of the domain")
    p.add_argument("--min-count", type=int, default=20, help="a value shared by fewer records than this is never suspect")
    p.add_argument("--min-names", type=int, default=5, help="...nor one with fewer different names than this")
    p.add_argument("--reveal", action="store_true", help="show SSNs unmasked and print a config snippet to paste")
    p.add_argument("--json", action="store_true")
    p.add_argument("--fail-on-suspects", action="store_true", help="exit 1 when suspects are found (to gate an onboarding script)")
    p = sub.add_parser("process-pending", parents=[common], help="Finish unfinished records (accepted, interrupted, failed)")
    p.add_argument("domain")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--max-attempts", type=int, default=5)
    p = sub.add_parser("runs", parents=[common], help="Recent batch runs, process-pending passes and reindexes")
    p.add_argument("domain", nargs="?")
    p.add_argument("--limit", type=int, default=20)
    p = sub.add_parser("schema", parents=[common], help="The golden-record table for a domain")
    p.add_argument("domain")
    p = sub.add_parser("blocking-stats", parents=[common], help="Bucket sizes and suspiciously common values")
    p.add_argument("domain", nargs="?")
    p.add_argument("--top", type=int, default=10)
    p = sub.add_parser("audit", parents=[common], help="Have a person check automatic merges: the labels that certify a learned threshold")
    p.add_argument("action", choices=("sample", "sample-review", "list", "verdict"))
    p.add_argument("domain")
    p.add_argument("--tenant", default="default")
    p.add_argument("--rate", type=float, default=0.02, help="the random share to flag (default 2%%)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--model", help="sample-review: a scorer file (default: the tenant's exported scorer)")
    p.add_argument("--min-p", type=float, default=None, help="sample-review: only review pairs the model scores at or above this")
    p.add_argument("--pending", action="store_true", help="list: only audits still waiting for a person")
    p.add_argument("--candidate")
    p.add_argument("--verdict", choices=("correct", "wrong"))
    p.add_argument("--by", help="who checked it (a person, not 'system:...')")
    p = sub.add_parser("unmerge", parents=[common], help="Undo ONE confirmed merge a person says is wrong: the entity is split along it and the pair is never merged again")
    p.add_argument("candidate", help="the confirmed pair (candidate id) to undo")
    p.add_argument("--domain", required=True)
    p.add_argument("--tenant", default="default")
    p.add_argument("--by", required=True, help="who is making this decision: recorded as theirs")
    p.add_argument("--reason")
    p.add_argument("--dry-run", action="store_true", help="show what would move, change nothing")
    p = sub.add_parser("separation-check", parents=[common], help="Find separations a person made that an engine has undone")
    p.add_argument("domain")
    p.add_argument("--tenant", default="default")
    p = sub.add_parser("scorer", parents=[common], help="The learned scorer: its state, the audit-based check that rolls it back automatically, and the reset")
    p.add_argument("action", choices=("status", "check", "reset"))
    p.add_argument("domain")
    p.add_argument("--tenant", default="default")
    p.add_argument("--by", help="reset: who is switching it back on")
    p = sub.add_parser("shadow-report", parents=[common], help="What the learned scorer WOULD have decided on the stored pairs, beside what happened and what people said (read-only)")
    p.add_argument("domain")
    p.add_argument("--tenant", default="default")
    p.add_argument("--model", help="a scorer file (default: the tenant's exported scorer)")
    p = sub.add_parser("reindex", parents=[common], help="Rebuild a domain's blocking index")
    p.add_argument("domain")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--force", action="store_true", help="Rebuild even if the index looks current")
    p.add_argument("--restart-hint", action="store_true", help=argparse.SUPPRESS)

    args = parser.parse_args()
    handlers = {"validate": cmd_validate, "list": cmd_list, "export": cmd_export,
                "normalizers": cmd_normalizers, "status": cmd_status, "reindex": cmd_reindex,
                "blocking-stats": cmd_blocking_stats, "schema": cmd_schema,
                "process-pending": cmd_process_pending, "runs": cmd_runs, "profile": cmd_profile, "audit": cmd_audit, "shadow-report": cmd_shadow_report, "scorer": cmd_scorer, "unmerge": cmd_unmerge, "separation-check": cmd_separation_check}
    try:
        try:
            return handlers[args.command](args)
        except ProtectionKeyMissing as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2
    except (DomainConfigError, IndexStaleError, KeyError) as e:
        print(f"\nError: {e.args[0] if isinstance(e, KeyError) else e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
