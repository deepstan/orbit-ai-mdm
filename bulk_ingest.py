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
import sys
import time

from app.connectors import CSVConnector, JSONConnector
from app.domains import REGISTRY
from app.pipeline import ingest_batch
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


def main():
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
    args = parser.parse_args()

    if not args.mapping and not args.mapping_file:
        print("Error: provide either --mapping or --mapping-file", file=sys.stderr)
        sys.exit(1)

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

    store = get_store(args.db)

    counts = {"auto_merged": 0, "queued_for_review": 0, "standalone": 0}
    start = time.time()
    n = 0

    print(f"Ingesting '{args.file}' into tenant='{args.tenant}' domain='{args.domain}' "
          f"(batch size {args.batch_size})...")

    batch: list[tuple[str, str, dict]] = []

    def flush_batch():
        nonlocal n
        if not batch:
            return
        results = ingest_batch(store, args.tenant, args.domain, batch)
        for result in results:
            if result["auto_merged_with"]:
                counts["auto_merged"] += 1
            elif result["queued_for_review"]:
                counts["queued_for_review"] += 1
            else:
                counts["standalone"] += 1
        n += len(batch)
        batch.clear()
        elapsed = time.time() - start
        print(f"  ...{n} records ({n / elapsed:.1f}/sec)")

    for source_record_id, attributes in connector.fetch_records():
        batch.append((connector.source_system, source_record_id, attributes))
        if len(batch) >= args.batch_size:
            flush_batch()
    flush_batch()  # remaining records that didn't fill a full batch

    elapsed = time.time() - start
    print()
    print(f"Done: {n} records in {elapsed:.1f}s ({n / elapsed:.1f}/sec)" if elapsed > 0 else f"Done: {n} records")
    print(f"  Auto-merged:        {counts['auto_merged']}")
    print(f"  Queued for review:  {counts['queued_for_review']}")
    print(f"  Standalone (new):   {counts['standalone']}")
    if counts["queued_for_review"]:
        print(f"\n{counts['queued_for_review']} record(s) need steward review — "
              f"check the UI or GET /v1/{args.domain}/review-queue")


if __name__ == "__main__":
    main()
