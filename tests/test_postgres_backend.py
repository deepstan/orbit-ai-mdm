"""
Validates PostgresStore against a REAL Postgres instance -- the one piece
of this codebase that could not be tested in the sandbox this was built
in (no network access there). This file SKIPS automatically unless both:

  1. psycopg2 is installed (`pip install psycopg2-binary`)
  2. TEST_DATABASE_URL is set to a real, reachable Postgres database

Run it once you have Postgres running locally:

    docker run --name mdm-test-pg -e POSTGRES_PASSWORD=test -p 5432:5432 -d postgres:16
    createdb -h localhost -U postgres mdm_test        # or: psql -h localhost -U postgres -c "CREATE DATABASE mdm_test"
    pip install psycopg2-binary
    export TEST_DATABASE_URL=postgresql://postgres:test@localhost:5432/mdm_test
    python3 -m unittest tests.test_postgres_backend -v

This intentionally re-runs a subset of the same scenarios test_core.py
already proves against SQLite -- the point is confirming PostgresStore
behaves identically, not testing new logic (the matching/pipeline logic
itself is already fully covered against SQLite).
"""
import os
import unittest

TEST_DB_URL = os.environ.get("TEST_DATABASE_URL")

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

SKIP_REASON = (
    "Set TEST_DATABASE_URL to a real Postgres connection string and "
    "`pip install psycopg2-binary` to run this test against Postgres."
)


@unittest.skipUnless(PSYCOPG2_AVAILABLE and TEST_DB_URL, SKIP_REASON)
class TestPostgresBackend(unittest.TestCase):
    def setUp(self):
        from app.db_postgres import PostgresStore
        self.store = PostgresStore(TEST_DB_URL)
        # Clean slate: this tenant_id is unique to this test run so it
        # never collides with real data if pointed at a shared database.
        self.tenant = f"pgtest_{os.getpid()}"

    def test_ingest_and_retrieve_round_trips_correctly(self):
        from app.pipeline import ingest_record
        result = ingest_record(self.store, self.tenant, "customer", "crm", "PG-1", {
            "first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
        })
        entity = self.store.get_entity(result["matched_entity_id"], self.tenant, "customer")
        self.assertIsNotNone(entity)
        self.assertEqual(entity.attributes["email"], "ada@example.com")

    def test_auto_merge_works_identically_to_sqlite(self):
        from app.pipeline import ingest_record
        r1 = ingest_record(self.store, self.tenant, "customer", "crm", "PG-2", {
            "first_name": "Grace", "last_name": "Hopper", "email": "grace@navy.mil",
        })
        r2 = ingest_record(self.store, self.tenant, "customer", "shop", "PG-3", {
            "first_name": "Grace", "last_name": "Hopper", "email": "grace@navy.mil",
        })
        self.assertEqual(r1["matched_entity_id"], r2["matched_entity_id"])

    def test_tenant_and_domain_isolation_holds_on_postgres_too(self):
        from app.pipeline import ingest_record
        payload = {"first_name": "Alan", "last_name": "Turing", "email": "alan@example.com"}
        r_a = ingest_record(self.store, self.tenant, "customer", "crm", "PG-4", payload)
        r_b = ingest_record(self.store, self.tenant + "_other", "customer", "crm", "PG-4", payload)
        self.assertNotEqual(r_a["matched_entity_id"], r_b["matched_entity_id"])

    def test_lineage_is_recorded(self):
        from app.pipeline import ingest_record
        result = ingest_record(self.store, self.tenant, "product", "erp", "PG-5", {
            "sku": "PGTEST-1", "product_name": "Test Widget",
        })
        lineage = self.store.list_lineage(result["matched_entity_id"], self.tenant, "product")
        self.assertGreater(len(lineage), 0)

    def test_api_keys_round_trip(self):
        from app.auth import generate_api_key
        from app.models import ApiKeyRecord
        plaintext, key_hash = generate_api_key()
        record = ApiKeyRecord(tenant_id=self.tenant, role="tenant_admin", key_hash=key_hash, label="pg test key")
        self.store.save_api_key(record)
        fetched = self.store.get_api_key_by_hash(key_hash)
        self.assertEqual(fetched.label, "pg test key")

    def test_audit_chain_verifies_on_postgres(self):
        from app.audit import log_action, verify_chain
        log_action(self.store, self.tenant, "ingest", "key:test", {"x": 1})
        log_action(self.store, self.tenant, "ingest", "key:test", {"x": 2})
        ok, _ = verify_chain(self.store, self.tenant)
        self.assertTrue(ok)

    def test_purge_works_on_postgres(self):
        from app.compliance import purge_entity
        from app.pipeline import ingest_record
        result = ingest_record(self.store, self.tenant, "customer", "crm", "PG-6", {
            "first_name": "Test", "last_name": "Purge",
        })
        purge_entity(self.store, self.tenant, "customer", result["matched_entity_id"], requested_by="test")
        self.assertIsNone(self.store.get_entity(result["matched_entity_id"], self.tenant, "customer"))


if __name__ == "__main__":
    unittest.main()
