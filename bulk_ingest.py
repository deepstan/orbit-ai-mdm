"""
Bulk-ingest records from a CSV or JSON file, with progress output and a
summary of outcomes at the end.

    # CSV, simple field mapping inline
    python3 bulk_ingest.py --tenant acme --domain customer --file customers.csv \
        --mapping "first_name=First Name,last_name=Last Name,email=Email" \
        --id-column CustomerID

    # JSON, mapping loaded from a file (handy for many fields or nested paths)
    python3 bulk_ingest.py --tenant acme --domain vendor --file vendors.json \
        --mapping-file vendor_mapping.json --id-column vendor_id

A mapping file (--mapping-file) is a JSON object:
    {"Company Name": "company_name", "Tax ID": "tax_id", "contact.email": "contact_email"}
(dotted paths like "contact.email" only apply to --file *.json; CSV has no nesting.)

Uses app/pipeline.py's ingest_batch() under the hood: records are
processed in chunks of --batch-size (default 200), which reduces
database commits for the raw record writes. Candidate matching itself
is indexed (a blocking_keys table, not a full-table scan) and stays
strictly per-record and in-order within each batch -- see
ingest_batch()'s docstring for why batching the matching step itself
isn't safe (it would change WHICH records match, not just how fast).

⚠️ Performance note: this replaced an older, much slower design (full
per-record table rescans) -- measured in the sandbox at 2000 records in
15.9s with the indexed lookup, vs. what would have taken many minutes
under the old approach. Batching only adds a modest additional win on
top of that (~15% faster on SQLite in one measured run; likely more on
Postgres, where each avoided commit is a network round-trip, not just
tested live here).
"""
import argparse
import os
import sys
import time

from app.connectors import CSVConnector, JSONConnector
from app.domain_config import DomainConfigError, load_configured_domains
from app.domains import REGISTRY
from app.indexing import IndexStaleError
from app.pipeline import Run, run_batch
from app.store_factory import get_store


def parse_inline_mapping(spec: str) -> dict[str, str]:
    """Parses "canonical=source,canonical2=source2" into
    {source: canonical, source2: canonical2} (field_mapping's expected
    direction: {source_field: canonical_field})."""
    mapping = {}
    for pair in spec.split(","):
        if "=" not in pair:
            raise ValueError(f"Bad --mapping entry (expected canonical=source): {pair!r}")
        canonical, source = pair.split("=", 1)
        mapping[source.strip()] = canonical.strip()
    return mapping


def _count_records(path: str) -> int:
    """How many records a file holds, cheaply, to size the key filter: the lines of a CSV (less the header), the items of a JSON array."""
    if path.lower().endswith(".json"):
        import json
        with open(path) as f:
            data = json.load(f)
        return len(data) if isinstance(data, list) else 1
    with open(path, "rb") as f:
        return max(0, sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b"")) - 1)


def main():
    load_configured_domains()  # custom domains must exist before the --domain choices are built
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--domain", required=True, choices=list(REGISTRY))
    parser.add_argument("--file", required=True, help="Path to a .csv or .json file")
    parser.add_argument("--mapping", default=None,
                         help='Inline field mapping: "canonical=source_column,canonical2=source_column2"')
    parser.add_argument("--mapping-file", default=None,
                         help="Path to a JSON file mapping {source_field: canonical_field}")
    parser.add_argument("--id-column", default=None,
                         help="Source column/field to use as the stable record ID (recommended for re-runs)")
    parser.add_argument("--source-system", default=None,
                         help="Label for where this data came from (default: csv_import / json_import)")
    parser.add_argument("--db", default=None, help="SQLite path or postgresql:// URL (default: DATABASE_URL env or mdm.db)")
    parser.add_argument("--batch-size", type=int, default=200,
                         help="Records per batch (default 200). Larger batches write faster "
                              "(see app/pipeline.py's ingest_batch) but hold more in memory at "
                              "once and delay progress output until each batch completes.")
    parser.add_argument("--max-errors", type=int, default=None,
                        help="abort the run once more than this many records fail (the rest stay unfinished; `process-pending` completes them)")
    parser.add_argument("--bulk", action="store_true",
                        help="a large load. With the default engine (the kernel) this is the SAME engine and the same code path as a normal load: every record is new, unchanged or changed, and each is "
                             "matched against the groups already built, in order, so a first load, an onboarding of a new source and a delta are all this one command. "
                             "With MDM_ENGINE=legacy, or --legacy-bulk, it is the set-based pair engine below")
    parser.add_argument("--key-filter", action="store_true",
                        help="--bulk (kernel): skip the blocking lookups that would find nothing, using a Bloom filter of the keys already indexed. Measured: 92%% of records share no key with any other record. "
                             "NOTHING ELSE may write this tenant and domain while the load runs (no API traffic, no second loader): a key added behind the filter's back would be missed. Off by default")
    parser.add_argument("--key-filter-records", type=int, default=None, help="--key-filter: how many records this load will add, to size the filter (default: counted from the file)")
    parser.add_argument("--key-filter-max-keys", type=int, default=200_000_000, help="--key-filter: do not use the filter if more keys than this are already in the index (it is loaded from them first)")
    parser.add_argument("--legacy-bulk", action="store_true",
                        help="use the set-based PAIR engine (parallel, resumable, faster on a huge first load, but different decisions from the kernel and no group merging). "
                             "Kept for very large first loads and for rollback; it is what --workers, --workdir, --dry-run, --resume, --budget and --lineage belong to")
    parser.add_argument("--workers", type=int, default=1, help="--legacy-bulk: processes for the standardize and score stages")
    parser.add_argument("--workdir", default=None, help="--legacy-bulk: where its temporary files go (default: a temporary directory; needs disk, not memory)")
    parser.add_argument("--onboard", action="store_true", help="ADD these records, a new source system, to data that already exists. The kernel needs no flag for this (a normal load does it); "
                        "with --legacy-bulk they are compared with each other and with the existing records, and entities they link merge (the oldest survives); they must be new records, and nothing else may write meanwhile")
    parser.add_argument("--delta", action="store_true", help="more records from a source ALREADY loaded. The kernel needs no flag for this (a normal load classifies each record as new, unchanged or changed, "
                        "and a changed record is matched again, so it can leave its group). With --legacy-bulk: changed records are updated in place and re-matched on their new keys; a change can add merges, never split. Implies --onboard")
    parser.add_argument("--dry-run", action="store_true", help="--legacy-bulk: stages A to D only; nothing is written to the store and the work files are kept (needs --workdir)")
    parser.add_argument("--resume", action="store_true", help="--legacy-bulk: continue a load that paused or was interrupted (needs the same --workdir). A normal load is resumed by running it again, or by process-pending")
    parser.add_argument("--budget", type=float, default=None, help="--legacy-bulk: stop cleanly after about this many seconds, keeping progress in --workdir (exit code 3); run again with --resume")
    parser.add_argument("--lineage", default=None, choices=["multi_source", "all", "none"],
                        help="--legacy-bulk: which golden records get one lineage row per field (default multi_source: only entities of 2+ records)")
    parser.add_argument("--defer", action="store_true", help="only land and stage the records; match them later with `domains_cli.py process-pending`")
    args = parser.parse_args()

    if not args.mapping and not args.mapping_file:
        print("Error: provide either --mapping or --mapping-file", file=sys.stderr)
        sys.exit(1)

    from app import kernel
    set_based = args.legacy_bulk or (args.bulk and kernel.engine() == "legacy")   # the set-based pair engine, only when asked for (or when the whole app is on the legacy engine)
    if args.key_filter and set_based:
        print("Error: --key-filter belongs to the kernel: the set-based engine (--legacy-bulk, or --bulk with MDM_ENGINE=legacy) does not use it. Drop --key-filter, or drop --legacy-bulk.", file=sys.stderr)
        sys.exit(2)
    if args.legacy_bulk and kernel.engine() == "kernel":
        print("Note: --legacy-bulk is DEPRECATED. It uses the set-based PAIR engine: its decisions differ from the kernel's, and a later normal load or a steward's action uses the kernel. See docs/legacy-engines.md.", file=sys.stderr)
    if args.bulk and not set_based:
        only_set_based = [flag for flag, given in (("--workers", args.workers != 1), ("--workdir", args.workdir), ("--dry-run", args.dry_run), ("--resume", args.resume),
                                                   ("--budget", args.budget is not None), ("--lineage", args.lineage)) if given]
        if only_set_based:
            print(f"Error: {', '.join(only_set_based)} belong to the set-based engine, and --bulk now runs the kernel (one engine, one record at a time, in order). "
                  f"Add --legacy-bulk to use them. A kernel load is resumed by running the same command again, or `domains_cli.py process-pending`.", file=sys.stderr)
            sys.exit(2)

    if args.mapping_file:
        import json
        with open(args.mapping_file) as f:
            field_mapping = json.load(f)
    else:
        field_mapping = parse_inline_mapping(args.mapping)

    is_json = args.file.lower().endswith(".json")
    if is_json:
        connector = JSONConnector(args.file, field_mapping, source_record_id_field=args.id_column,
                                   source_system=args.source_system or "json_import")
    else:
        connector = CSVConnector(args.file, field_mapping, source_record_id_column=args.id_column,
                                  source_system=args.source_system or "csv_import")

    from app.protect import ProtectionKeyMissing, require_key_if_needed
    try:
        require_key_if_needed({args.domain: REGISTRY[args.domain]})  # fail before anything is read, not once per record
    except ProtectionKeyMissing as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)
    store = get_store(args.db)

    if set_based:
        from app.bulk import BulkLoadError, BulkPaused, bulk_load
        if (args.dry_run or args.resume or args.budget is not None) and not args.workdir:
            print("Error: --dry-run, --resume and --budget keep their progress in --workdir, so give one", file=sys.stderr)
            sys.exit(2)
        try:
            res = bulk_load(store, args.tenant, args.domain, ((connector.source_system, rid, attrs) for rid, attrs in connector.fetch_records()),
                            workers=args.workers, chunk=args.batch_size, workdir=args.workdir, lineage=args.lineage or "multi_source", triggered_by="bulk_ingest", log=print,
                            write=not args.dry_run, resume=args.resume, budget=args.budget, onboard=args.onboard or args.delta, update=args.delta)
        except BulkPaused as e:
            print(f"\nPAUSED: {e}. Run the same command again with --resume to continue.")
            sys.exit(3)
        except BulkLoadError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"\n{'DRY RUN (nothing written): ' if res.dry_run else ''}Bulk load done: {res.records:,} records -> {res.entities:,} entities; {res.candidate_pairs:,} candidate pairs "
              f"({res.auto_pairs:,} auto-merged, {res.review_pairs:,} for review); {res.capped_keys} blocking keys over the cap skipped; run {res.run_id}")
        if res.changed_records or res.unchanged_records:
            print(f"Delta: {res.new_records:,} new, {res.changed_records:,} changed, {res.unchanged_records:,} unchanged (nothing done for those)")
        if res.separations_held or res.separations_dropped:
            print(f"Separations a person made were kept: {res.separations_held:,} automatic merge(s) that would have reunited separated records were held for review"
                  f" (reason: \"a person separated records in these two entities\"), and {res.separations_dropped:,} separated pair(s) were not proposed again.")
        if res.promoted_pairs:
            print(f"The learned scorer promoted {res.promoted_pairs:,} pairs out of review to automatic merges (they are flagged for audit like any other automatic merge).")
        if res.existing_records:
            print(f"Onboarded onto {res.existing_records:,} existing records: {res.entities:,} new entities; {res.existing_entities_touched:,} existing entities gained records, "
                  f"{res.existing_entities_merged:,} of them merged into an older one")
        sys.exit(0)
    counts = {"auto_merged": 0, "queued_for_review": 0, "standalone": 0, "unchanged": 0, "updated": 0, "errors": 0, "deferred": 0}
    start = time.time()
    n = 0
    run = Run.start(store, args.tenant, args.domain, "batch", "bulk_ingest")  # ONE run for the whole file, however many batches it is read in

    print(f"Ingesting '{args.file}' into tenant='{args.tenant}' domain='{args.domain}' "
          f"(batch size {args.batch_size})...")

    key_filter = None
    if args.key_filter:
        if os.environ.get("MDM_ENGINE", "kernel") == "legacy":
            print("Note: --key-filter belongs to the kernel and MDM_ENGINE=legacy is set: it is ignored.", file=sys.stderr)
        else:
            from app import keyfilter
            expected = args.key_filter_records if args.key_filter_records is not None else _count_records(args.file)
            key_filter, message = keyfilter.build_for_load(store, args.tenant, args.domain, expected, args.key_filter_max_keys)
            print(message + (f"\n  YOU ARE PROMISING that nothing else writes tenant '{args.tenant}' domain '{args.domain}' until this load is finished." if key_filter else ""))

    batch: list[tuple[str, str, dict]] = []

    def flush_batch():
        nonlocal n
        if not batch:
            return
        results = run_batch(store, args.tenant, args.domain, batch, max_errors=args.max_errors, defer=args.defer, run=run, key_filter=key_filter).results
        for result in results:
            if "error" in result:
                counts["errors"] += 1
            elif result.get("unchanged"):
                counts["unchanged"] += 1
            elif result.get("deferred"):
                counts["deferred"] += 1
            elif result.get("updated"):
                counts["updated"] += 1
            elif result["auto_merged_with"]:
                counts["auto_merged"] += 1
            elif result["queued_for_review"]:
                counts["queued_for_review"] += 1
            else:
                counts["standalone"] += 1
        n += len(batch)
        batch.clear()
        elapsed = time.time() - start
        print(f"  ...{n} records ({n / elapsed:.1f}/sec)")

    try:
        for source_record_id, attributes in connector.fetch_records():
            batch.append((connector.source_system, source_record_id, attributes))
            if len(batch) >= args.batch_size:
                flush_batch()
                if run.aborted:
                    break
        if not run.aborted:
            flush_batch()  # remaining records that didn't fill a full batch
    except BaseException:
        run.aborted = True
        raise
    finally:
        summary = run.finish()

    elapsed = time.time() - start
    print()
    print(f"Done: {n} records in {elapsed:.1f}s ({n / elapsed:.1f}/sec)" if elapsed > 0 else f"Done: {n} records")
    print(f"  Auto-merged:        {counts['auto_merged']}")
    print(f"  Queued for review:  {counts['queued_for_review']}")
    print(f"  Standalone (new):   {counts['standalone']}")
    if counts["unchanged"] or counts["updated"]:
        print(f"  Already held, unchanged: {counts['unchanged']}   |   changed, new version: {counts['updated']}")
    if counts["deferred"]:
        print(f"  Accepted, not yet matched: {counts['deferred']}   ->  python3 domains_cli.py process-pending {args.domain}")
    print(f"  Run {summary['run_id']}: {summary['status']}")
    if key_filter is not None:
        print(f"  {key_filter.summary()}")
    if counts["errors"]:
        print(f"\n{counts['errors']} record(s) FAILED. The rest of the run carried on. First errors:")
        for e in summary["errors"][:5]:
            print(f"    {e['source_system']}/{e['source_record_id']}: {e['error']}")
        print(f"  They stay unfinished: re-run this command, or `python3 domains_cli.py process-pending {args.domain}`.")
    if run.aborted and summary["status"] == "failed":
        print(f"  ABORTED after more than {args.max_errors} errors; the remaining records stay unfinished.")
    if counts["queued_for_review"]:
        print(f"\n{counts['queued_for_review']} record(s) need steward review — "
              f"check the UI or GET /v1/{args.domain}/review-queue")


if __name__ == "__main__":
    try:
        main()
    except (DomainConfigError, IndexStaleError) as e:
        print(f"\nError: {e}", file=__import__("sys").stderr)
        raise SystemExit(2)
