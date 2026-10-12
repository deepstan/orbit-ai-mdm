"""
Seeds the MDM database across all three domains (Customer, Product,
Vendor) for the "default" tenant, exercising auto-merge, review-queue,
and standalone paths in each -- plus a cross-tenant isolation check.

Run with:  python3 seed_demo.py
Then:      uvicorn app.api:app --reload --port 8000
           open http://localhost:8000/ (defaults to Customer domain in the UI)
"""
from app.pipeline import ingest_record
from app.store_factory import get_store

TENANT = "default"


def seed_customer(store):
    print("--- Customer domain ---")
    print(ingest_record(store, TENANT, "customer", "salesforce", "SF-1001", {
        "first_name": "Robert", "last_name": "Smith", "email": "bob.smith@acme.com",
        "phone": "415-555-0100", "address_line1": "123 Main Street", "city": "San Francisco",
        "state": "CA", "postal_code": "94107",
    }))
    print(ingest_record(store, TENANT, "customer", "shopify", "SHOP-9981", {
        "first_name": "Bob", "last_name": "Smith", "email": "bob.smith@acme.com",
        "phone": "(415) 555-0100", "address_line1": "123 Main St, Apt 4", "city": "San Francisco",
        "state": "CA", "postal_code": "94107-1234",
    }))
    print(ingest_record(store, TENANT, "customer", "salesforce", "SF-2002", {
        "first_name": "Jennifer", "last_name": "Nguyen", "email": "jnguyen@work.com",
        "address_line1": "45 Birch Ln", "city": "Austin", "state": "TX", "postal_code": "73301",
    }))
    print(ingest_record(store, TENANT, "customer", "csv_import", "ROW-77", {
        "first_name": "Jen", "last_name": "Nguyen",
        "address_line1": "450 Birchwood Lane", "city": "Austin", "state": "TX", "postal_code": "73344",
    }))
    print(ingest_record(store, TENANT, "customer", "salesforce", "SF-3003", {
        "first_name": "Maria", "last_name": "Garcia", "email": "maria.garcia@example.com",
        "address_line1": "90 Lake Shore Dr", "city": "Chicago", "state": "IL", "postal_code": "60601",
    }))


def seed_product(store):
    print("\n--- Product domain ---")
    print(ingest_record(store, TENANT, "product", "erp", "SKU-1001", {
        "sku": "NK-AM90-BLK-10", "product_name": "Nike Air Max 90", "brand": "Nike",
        "category": "Shoes", "price": "120.00",
    }))
    print(ingest_record(store, TENANT, "product", "shopify", "SHOP-P-77", {
        "sku": "nk-am90-blk-10", "product_name": "Air Max 90 by Nike", "brand": "Nike",
        "category": "Footwear", "price": "119.99",
    }))
    print(ingest_record(store, TENANT, "product", "erp", "SKU-2002", {
        "sku": "GEN-WDGT-PRO", "product_name": "Widget Pro 3000", "brand": "Acme Tools",
        "category": "Hardware", "price": "45.00",
    }))
    print(ingest_record(store, TENANT, "product", "distributor_feed", "DIST-9", {
        "sku": "GEN-WDGT-PRO-V2", "product_name": "Widget Pro 3000 (New Edition)", "brand": "Acme Tools",
        "category": "Hardware", "price": "47.50",
    }))
    print(ingest_record(store, TENANT, "product", "erp", "SKU-3003", {
        "sku": "AD-UB22-WHT-9", "product_name": "Adidas Ultraboost 22", "brand": "Adidas",
        "category": "Shoes", "price": "180.00",
    }))


def seed_vendor(store):
    print("\n--- Vendor domain ---")
    print(ingest_record(store, TENANT, "vendor", "erp", "VEND-1", {
        "company_name": "Acme Supply Co", "tax_id": "12-3456789",
        "contact_email": "ap@acmesupply.com", "city": "Dallas", "state": "TX", "postal_code": "75201",
    }))
    print(ingest_record(store, TENANT, "vendor", "ap_system", "AP-VEND-1", {
        "company_name": "Acme Supply Company Inc", "tax_id": "123456789",
        "contact_email": "ap@acmesupply.com", "city": "Dallas", "state": "TX", "postal_code": "75201",
    }))
    print(ingest_record(store, TENANT, "vendor", "erp", "VEND-2", {
        "company_name": "Northwind Distributors", "contact_email": "info@northwind-dist.com",
        "contact_phone": "312-555-4400", "city": "Chicago", "state": "IL", "postal_code": "60601",
    }))
    print(ingest_record(store, TENANT, "vendor", "csv_import", "ROW-V-9", {
        "company_name": "Northwind Distribution LLC", "contact_phone": "(312) 555-4400",
        "city": "Chicago", "state": "IL", "postal_code": "60602",
    }))
    print(ingest_record(store, TENANT, "vendor", "erp", "VEND-3", {
        "company_name": "Globex Industrial", "tax_id": "98-7654321",
        "contact_email": "billing@globex.com", "city": "Reno", "state": "NV", "postal_code": "89501",
    }))


def main():
    # Uses DATABASE_URL if set (e.g. postgresql://...), else SQLite's mdm.db
    # in the current directory -- see app/store_factory.py.
    store = get_store()
    seed_customer(store)
    seed_product(store)
    seed_vendor(store)

    print(f"\n--- (Isolation demo) Ingesting a vendor-shaped payload with a customer-shaped "
          f"'first_name' field into the 'product' domain, same tenant ---")
    r = ingest_record(store, TENANT, "product", "erp", "SKU-9999", {
        "sku": "MISC-9999", "product_name": "Assorted Widget", "brand": "Acme Tools",
    })
    print(r)
    print("This only ever matches against other PRODUCT records for this tenant -- "
          "never against the Customer or Vendor records above, no matter how similar any field looks.")

    print("\n--- Phase 3: audit log + usage (the demo writes to the database directly: no API key involved) ---")
    from app.audit import log_action, verify_chain
    from app.billing import get_usage_summary, record_usage

    log_action(store, TENANT, "seed_demo_run", actor="system:seed_demo",
               details={"note": "seed_demo.py populated demo data"})
    for _ in range(15):
        record_usage(store, TENANT, "record_ingested", domain="customer")
    ok, _ = verify_chain(store, TENANT)
    print(f"Audit chain valid: {ok}")
    summary = get_usage_summary(store, TENANT)
    print(f"Usage so far: {summary.counts_by_event_type} (~${summary.estimated_cost_usd})")

    print("\nDone. Start the API with:")
    print("  uvicorn app.api:app --reload --port 8000")
    print(f"Then, to look around on a laptop (NO authentication: never expose it), run  MDM_DEV_MODE=1 uvicorn app.api:app --port 8000  and open http://localhost:8000/ (tenant '{TENANT}', Customer domain).")
    print("Switch domains in the UI's tabs, or query directly (in dev mode; with authentication add -H 'Authorization: Bearer <key>'):")
    print('  curl http://localhost:8000/v1/product/review-queue')
    print('  curl http://localhost:8000/v1/vendor/review-queue')
    print("\nTo try RBAC + audit log + usage metering for real:")
    print(f'  python3 manage_api_keys.py create --tenant {TENANT} --role tenant_admin --label "root key"')
    print("  uvicorn app.api:app --port 8000        # authentication is the default: no MDM_DEV_MODE")
    print('  curl -H "Authorization: Bearer <key>" http://localhost:8000/v1/admin/audit-log')


if __name__ == "__main__":
    main()
