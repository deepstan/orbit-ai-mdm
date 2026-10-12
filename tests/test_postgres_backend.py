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

    def test_merge_audits_and_human_labels_work_on_postgres(self):
        """Migration 014's table, the audit sampling and verdicts, and the rule that only a person's decision is a label. (Written without a server to run it on.)"""
        from app.pipeline import ingest_record
        payload = {"first_name": "Edsger", "last_name": "Dijkstra", "email": "edsger@example.com"}
        ingest_record(self.store, self.tenant, "customer", "crm", "PG-A1", payload)
        ingest_record(self.store, self.tenant, "customer", "shop", "PG-A2", payload)          # the same person: merged automatically, by the system
        self.assertEqual(self.store.human_labeled_candidates(self.tenant, "customer"), [])     # the system's own merge is not a label
        self.assertEqual(self.store.sample_merge_audit(self.tenant, "customer", 1.0, seed=1), 1)
        self.assertEqual(self.store.sample_merge_audit(self.tenant, "customer", 1.0, seed=1), 0)        # already flagged
        audits = self.store.list_merge_audits(self.tenant, "customer")
        self.assertEqual(len(audits), 1)
        cid = audits[0]["candidate_id"]
        with self.assertRaises(ValueError):
            self.store.record_merge_audit(cid, self.tenant, "customer", "correct", "system:auto_merge")
        self.assertFalse(self.store.record_merge_audit(cid, self.tenant + "_other", "customer", "correct", "jane"))   # another tenant cannot judge it
        self.assertTrue(self.store.record_merge_audit(cid, self.tenant, "customer", "correct", "jane"))
        labeled = self.store.human_labeled_candidates(self.tenant, "customer")
        self.assertEqual([(c.id, label, source) for c, label, source in labeled], [(cid, 1, "audit")])
        self.assertEqual(self.store.human_labeled_candidates(self.tenant + "_other", "customer"), [])

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

    def test_blocking_key_index_works_on_postgres(self):
        self.store.save_blocking_keys(self.tenant, "customer", "rec_a", {"g0:smith:94107"})
        self.store.save_blocking_keys(self.tenant, "customer", "rec_b", {"g0:smith:94107"})
        found = self.store.find_candidate_record_ids(self.tenant, "customer", {"g0:smith:94107"}, exclude_record_id="rec_a")
        self.assertEqual(found, {"rec_b"})

    def test_jsonb_attributes_round_trip_as_dict_not_string(self):
        """The core JSONB migration guarantee: attributes come back as
        a real dict (via psycopg2's jsonb auto-adaptation), not a JSON
        string needing json.loads()."""
        from app.pipeline import ingest_record
        result = ingest_record(self.store, self.tenant, "customer", "crm", "PG-JSONB", {
            "first_name": "Grace", "last_name": "Hopper", "email": "grace@navy.mil",
        })
        entity = self.store.get_entity(result["matched_entity_id"], self.tenant, "customer")
        self.assertIsInstance(entity.attributes, dict)
        self.assertEqual(entity.attributes["email"], "grace@navy.mil")

    def test_find_entities_by_attribute_queries_into_jsonb(self):
        """The payoff the JSONB migration unlocks: querying by attribute
        value directly in SQL via the ->> operator, Postgres-only."""
        from app.pipeline import ingest_record
        ingest_record(self.store, self.tenant, "customer", "crm", "PG-JSONB2", {
            "first_name": "Katherine", "last_name": "Johnson", "email": "katherine@nasa.gov",
        })
        found = self.store.find_entities_by_attribute(self.tenant, "customer", "email", "katherine@nasa.gov")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].attributes["last_name"], "Johnson")

    def test_rls_policies_apply_without_breaking_normal_queries(self):
        """Applies RLS policies (idempotent -- safe to re-run), then
        confirms normal ingest/query operations still work through the
        SAME connection this test suite already uses.

        ⚠️ THIS TEST DOES NOT PROVE RLS ACTUALLY ISOLATES TENANTS. The
        connection used throughout this test file is whatever role
        TEST_DATABASE_URL specifies -- if that's a superuser (the
        common case for a local dev Postgres), RLS policies are
        invisible to it by design (see app/rls_policies.py), so this
        only proves "enabling RLS doesn't break queries for a role
        that's exempt from it anyway." That's still worth knowing (a
        broken policy could break ALL queries, exempt or not), but it
        is NOT a security validation.

        To actually validate isolation: run `python3 setup_rls.py
        create-role --role-name mdm_test_restricted --password test123`,
        then `python3 setup_rls.py verify` while connected AS that role,
        then manually confirm two different tenant_ids' rows are
        genuinely invisible to each other through that restricted
        connection. That's a real security test; this one is a smoke
        test that the policies don't break anything.
        """
        from app.rls_policies import generate_rls_sql
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute(generate_rls_sql())

        # Normal operations should still work identically after RLS is applied
        # (whether or not it's actually restricting THIS role).
        from app.pipeline import ingest_record
        result = ingest_record(self.store, self.tenant, "customer", "crm", "RLS-1", {
            "first_name": "Test", "last_name": "AfterRLS", "email": "rls@test.local",
        })
        entity = self.store.get_entity(result["matched_entity_id"], self.tenant, "customer")
        self.assertIsNotNone(entity)
        self.assertEqual(entity.attributes["last_name"], "AfterRLS")

    def test_migrations_are_applied_and_json_columns_are_jsonb(self):
        """PostgresStore() has already run the migrations in setUp. Confirms
        the history is recorded, nothing is pending, and migration 004's
        TEXT -> JSONB conversion actually took effect on a real server --
        the one thing the mocked tests in test_migrations.py can't show."""
        from app.migrations import PostgresMigrationBackend, get_status
        self.assertTrue(get_status(PostgresMigrationBackend(TEST_DB_URL)).ok)
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND (table_name, column_name) IN "
                "(('source_records','attributes'), ('match_candidates','reasons'), "
                "('audit_log','details'), ('usage_events','metadata'), ('api_keys','allowed_domains'))")
            types = {(r["table_name"], r["column_name"]): r["data_type"] for r in cur.fetchall()}
        self.assertEqual(len(types), 5)
        self.assertEqual(set(types.values()), {"jsonb"})

    def test_index_state_and_reindex_primitives_work_on_postgres(self):
        """The Postgres side of customer-defined domains: the index-state upsert, and swapping a
        record's blocking keys. Uses a throwaway domain name with raw store calls, so no registry
        setup is needed."""
        domain = f"pgdomain_{os.getpid()}"

        def cleanup():
            with self.store._conn() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM blocking_index_state WHERE domain = %s", (domain,))
                cur.execute("DELETE FROM blocking_keys WHERE domain = %s", (domain,))
        self.addCleanup(cleanup)

        self.assertIsNone(self.store.get_index_state(domain))
        self.store.set_index_state(domain, "fp1", "ready")
        self.assertEqual(self.store.get_index_state(domain), ("fp1", "ready"))
        self.store.set_index_state(domain, "fp2", "reindexing")  # must upsert, not raise a duplicate-key error
        self.assertEqual(self.store.get_index_state(domain), ("fp2", "reindexing"))

        self.store.replace_blocking_keys_batch([(self.tenant, domain, "rec1", {"k1", "k2"})])
        self.assertTrue(self.store.domain_has_index_entries(domain))
        self.store.replace_blocking_keys_batch([(self.tenant, domain, "rec1", {"k3"})])
        self.assertEqual(self.store.find_candidate_record_ids(self.tenant, domain, {"k1"}, exclude_record_id="x"), set())
        self.assertEqual(self.store.find_candidate_record_ids(self.tenant, domain, {"k3"}, exclude_record_id="x"), {"rec1"})
        self.assertEqual(self.store.count_source_records(domain), 0)
        self.assertEqual(self.store.list_source_records_page(domain, "", 10), [])

    def test_golden_records_are_real_columns_on_postgres(self):
        """Migration 009 and the per-domain entity tables, on a real server: one column per field, no JSON blob, and
        the equality lookup that used to need a JSONB expression is now an ordinary column query."""
        from app.pipeline import ingest_record
        r = ingest_record(self.store, self.tenant, "customer", "crm", "GOLD-1",
                          {"first_name": "  gina ", "last_name": "LOPEZ", "email": "gina.pg@test.local"})
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = current_schema() AND table_name = 'entity_customer'")
            cols = {x["column_name"]: x["data_type"] for x in cur.fetchall()}
        self.assertIn("email", cols)
        self.assertNotIn("attributes", cols)
        entity = self.store.get_entity(r["matched_entity_id"], self.tenant, "customer")
        self.assertEqual((entity.attributes["first_name"], entity.attributes["last_name"]), ("Gina", "Lopez"))
        found = self.store.find_entities_by_attribute(self.tenant, "customer", "email", "gina.pg@test.local")
        self.assertEqual([e.id for e in found], [entity.id])

    def test_golden_columns_are_typed_on_postgres(self):
        """Typed golden columns on a real server: product.price is NUMERIC, quality_issues is JSONB, and a value that cannot be cast
        is NULL with the problem recorded, not a wrong value."""
        from decimal import Decimal
        from app.pipeline import ingest_record
        ok = ingest_record(self.store, self.tenant, "product", "erp", "TYPED-1", {"sku": "TYPED-1", "product_name": "Widget", "price": "$1,299.00"})
        bad = ingest_record(self.store, self.tenant, "product", "erp", "TYPED-2", {"sku": "TYPED-2", "product_name": "Gadget", "price": "N/A"})
        with self.store._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() "
                        "AND table_name = 'entity_product' AND column_name IN ('price', 'quality_issues')")
            types = {r["column_name"]: r["data_type"] for r in cur.fetchall()}
        self.assertEqual(types, {"price": "numeric", "quality_issues": "jsonb"})
        self.assertEqual(self.store.get_entity(ok["matched_entity_id"], self.tenant, "product").attributes["price"], Decimal("1299.00"))
        gadget = self.store.get_entity(bad["matched_entity_id"], self.tenant, "product")
        self.assertIsNone(gadget.attributes["price"])
        self.assertEqual(gadget.quality_issues[0]["field"], "price")

    def test_processing_state_run_log_and_golden_dirty_work_on_postgres(self):
        """Resumable processing on a real server: a record is unfinished until matched, runs are logged with JSON counts, and the durable
        golden-dirty marker round-trips."""
        from app.pipeline import Run, batch_ctx, flush_golden, ingest_record, process_pending
        a = ingest_record(self.store, self.tenant, "customer", "crm", "PROC-1", {"first_name": "Pia", "last_name": "Roe", "email": "pia.proc@test.local"}, mode="batch")
        self.assertTrue(a["deferred"])
        self.assertIn(a["record_id"], [r.id for r in self.store.list_unfinished_records("customer", 100000, 5)])
        out = process_pending(self.store, "customer")
        self.assertGreaterEqual(out["finished"], 1)
        self.assertEqual(self.store.get_source_record(a["record_id"], self.tenant, "customer").processing_status, "matched")
        run = Run.start(self.store, self.tenant, "customer", "batch", "test")
        run.counts["new"] = 2
        run.finish()
        self.assertEqual(self.store.list_runs(domain="customer", tenant_id=self.tenant, limit=1)[0]["counts"]["new"], 2)
        self.store.mark_golden_dirty([(self.tenant, "customer", "ent_does_not_exist")])
        self.assertIn((self.tenant, "ent_does_not_exist"), self.store.list_golden_dirty("customer"))
        flush_golden(self.store, "customer", {(self.tenant, "ent_does_not_exist")})  # a missing entity is skipped, not an error
        self.assertNotIn((self.tenant, "ent_does_not_exist"), self.store.list_golden_dirty("customer"))

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
