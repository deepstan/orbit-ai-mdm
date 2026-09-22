"""
Seeds the MDM database with sample records that deliberately exercise:
  - auto-merge on exact email match
  - queued for steward review (moderate fuzzy similarity)
  - stands alone (no match at all)
  - two tenants with IDENTICAL data, proving they never cross-match

Run with:  python3 seed_demo.py
Then start the API (uvicorn app.api:app --reload) and open http://localhost:8000/
(the UI defaults to the "default" tenant -- see note at the bottom).
"""
from app.db import Store
from app.pipeline import ingest_record

DB_PATH = "mdm.db"
TENANT = "default"  # matches api.py's default when no X-Tenant-Id header is sent


def main():
    store = Store(DB_PATH)

    print("Ingesting seed records for tenant:", TENANT, "\n")

    print(">> Robert Smith from CRM")
    print(ingest_record(store, TENANT, "salesforce", "SF-1001", {
        "first_name": "Robert", "last_name": "Smith", "email": "bob.smith@acme.com",
        "phone": "415-555-0100", "address_line1": "123 Main Street", "city": "San Francisco",
        "state": "CA", "postal_code": "94107",
    }))

    print("\n>> Bob Smith from Shopify (same email -> should auto-merge)")
    print(ingest_record(store, TENANT, "shopify", "SHOP-9981", {
        "first_name": "Bob", "last_name": "Smith", "email": "bob.smith@acme.com",
        "phone": "(415) 555-0100", "address_line1": "123 Main St, Apt 4", "city": "San Francisco",
        "state": "CA", "postal_code": "94107-1234",
    }))

    print("\n>> Jennifer Nguyen from CRM")
    print(ingest_record(store, TENANT, "salesforce", "SF-2002", {
        "first_name": "Jennifer", "last_name": "Nguyen", "email": "jnguyen@work.com",
        "phone": "512-555-2200", "address_line1": "45 Birch Ln", "city": "Austin",
        "state": "TX", "postal_code": "73301",
    }))

    print("\n>> Jen Nguyen from a CSV import (similar but not certain -> should queue for review)")
    print(ingest_record(store, TENANT, "csv_import", "ROW-77", {
        "first_name": "Jen", "last_name": "Nguyen", "email": "",
        "phone": "", "address_line1": "450 Birchwood Lane", "city": "Austin",
        "state": "TX", "postal_code": "73344",
    }))

    print("\n>> Maria Garcia from CRM (a genuinely distinct customer)")
    print(ingest_record(store, TENANT, "salesforce", "SF-3003", {
        "first_name": "Maria", "last_name": "Garcia", "email": "maria.garcia@example.com",
        "phone": "312-555-9999", "address_line1": "90 Lake Shore Dr", "city": "Chicago",
        "state": "IL", "postal_code": "60601",
    }))

    # --- Bonus: prove tenant isolation with a second tenant ---
    other_tenant = "tenant_globex"
    print(f"\n>> (Isolation demo) Ingesting a record IDENTICAL to Robert Smith's, "
          f"but under a different tenant ('{other_tenant}')")
    print(ingest_record(store, other_tenant, "salesforce", "SF-1001", {
        "first_name": "Robert", "last_name": "Smith", "email": "bob.smith@acme.com",
        "phone": "415-555-0100", "address_line1": "123 Main Street", "city": "San Francisco",
        "state": "CA", "postal_code": "94107",
    }))
    print(f"   -> This creates its own separate golden record under '{other_tenant}' -- "
          f"it will NEVER show up when querying tenant '{TENANT}', even with identical data.")

    print("\nDone. Start the API with:")
    print("  uvicorn app.api:app --reload --port 8000")
    print(f"Then open http://localhost:8000/ (defaults to tenant '{TENANT}').")
    print(f"To see the isolated '{other_tenant}' tenant's data instead, query the API directly with:")
    print(f'  curl -H "X-Tenant-Id: {other_tenant}" http://localhost:8000/v1/customer/entities')


if __name__ == "__main__":
    main()
