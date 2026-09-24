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

⚠️ Performance note: each record ingested re-scans every existing record
for that (tenant, domain) to find matches (see app/pipeline.py's
ingest_record -- store.list_source_records() is called fresh per call).
This is correct and fine for hundreds to low thousands of records. For
much larger bulk loads (tens of thousands+), expect this to get slow
since the cost grows with how much data already exists -- that's a
known scaling limitation of the current MVP pipeline, not something
this script works around. If you hit that wall, the fix belongs in
pipeline.py (batch the blocking step across the whole file at once
instead of one DB round-trip per record), not here.
"""
import argparse
import sys
import time

from app.connectors import CSVConnector, JSONConnector
from app.domains import REGISTRY
from app.pipeline import ingest_record
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

    print(f"Ingesting '{args.file}' into tenant='{args.tenant}' domain='{args.domain}'...")
    for source_record_id, attributes in connector.fetch_records():
        result = ingest_record(store, args.tenant, args.domain, connector.source_system,
                                source_record_id, attributes)
        if result["auto_merged_with"]:
            counts["auto_merged"] += 1
        elif result["queued_for_review"]:
            counts["queued_for_review"] += 1
        else:
            counts["standalone"] += 1

        n += 1
        if n % 50 == 0:
            elapsed = time.time() - start
            print(f"  ...{n} records ({n / elapsed:.1f}/sec)")

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
