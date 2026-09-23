"""
Phase 3: RBAC/auth, audit log, right-to-be-forgotten, usage metering,
and the CSV connector. All fully testable without network access.

Run with:  python3 -m unittest tests.test_phase3 -v
"""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.audit import log_action, verify_chain
from app.auth import AuthError, Principal, check_permission, generate_api_key, hash_key
from app.billing import LoggingBillingProvider, get_usage_summary, record_usage
from app.compliance import purge_entity
from app.connectors import CSVConnector, ingest_from_connector
from app.db import Store
from app.models import ApiKeyRecord
from app.pipeline import ingest_record


class TestAuth(unittest.TestCase):
    def test_key_generation_and_hashing_round_trips(self):
        """A generated key starts with ``mdm_`` and hashing it gives the returned hash."""
        plaintext, key_hash = generate_api_key()
        self.assertTrue(plaintext.startswith("mdm_"))
        self.assertEqual(hash_key(plaintext), key_hash)

    def test_different_keys_hash_differently(self):
        """Two generated keys, and their hashes, are different."""
        p1, h1 = generate_api_key()
        p2, h2 = generate_api_key()
        self.assertNotEqual(p1, p2)
        self.assertNotEqual(h1, h2)

    def test_tenant_admin_can_do_everything(self):
        """``tenant_admin`` is allowed every main action (ingest, resolve, view, retrain, manage keys, purge)."""
        principal = Principal(tenant_id="acme", role="tenant_admin", key_id="k1", label="root")
        for action in ("ingest", "resolve_review_queue", "view", "retrain", "manage_api_keys", "purge"):
            check_permission(principal, action)  # should not raise

    def test_api_consumer_is_read_only(self):
        """``api_consumer`` may only ``view``; every write or admin action raises ``AuthError``."""
        principal = Principal(tenant_id="acme", role="api_consumer", key_id="k1", label="readonly")
        check_permission(principal, "view")  # allowed
        for action in ("ingest", "resolve_review_queue", "retrain", "manage_api_keys", "purge"):
            with self.assertRaises(AuthError):
                check_permission(principal, action)

    def test_data_steward_cannot_manage_keys_or_purge(self):
        """``data_steward`` can ingest and resolve reviews, but not manage keys or purge."""
        principal = Principal(tenant_id="acme", role="data_steward", key_id="k1", label="steward")
        check_permission(principal, "ingest")
        check_permission(principal, "resolve_review_queue")
        with self.assertRaises(AuthError):
            check_permission(principal, "manage_api_keys")
        with self.assertRaises(AuthError):
            check_permission(principal, "purge")

    def test_domain_scoping_restricts_access(self):
        """A key limited to ``["customer"]`` can act in customer but not in vendor."""
        principal = Principal(tenant_id="acme", role="data_steward", key_id="k1", label="steward",
                               allowed_domains=["customer"])
        check_permission(principal, "ingest", domain="customer")  # fine
        with self.assertRaises(AuthError):
            check_permission(principal, "ingest", domain="vendor")

    def test_no_domain_restriction_means_all_domains(self):
        """A key with ``allowed_domains=None`` can act in every domain."""
        principal = Principal(tenant_id="acme", role="data_steward", key_id="k1", label="steward",
                               allowed_domains=None)
        for domain in ("customer", "product", "vendor"):
            check_permission(principal, "ingest", domain=domain)  # none should raise

    def test_unknown_role_has_no_permissions(self):
        """A role that isn't defined is denied even the ``view`` action."""
        principal = Principal(tenant_id="acme", role="nonexistent_role", key_id="k1", label="bogus")
        with self.assertRaises(AuthError):
            check_permission(principal, "view")


class TestAuditLog(unittest.TestCase):
    def setUp(self):
        """Create a fresh SQLite store in ``test_phase3.db`` for each test.

        Any leftover file from a previous run is deleted first.
        """
        self.db_path = "test_phase3.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

    def tearDown(self):
        """Delete the test database file after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_chain_starts_valid_and_stays_valid_across_entries(self):
        """Three entries written in a row form a valid chain."""
        log_action(self.store, "acme", "ingest", "key:a", {"x": 1})
        log_action(self.store, "acme", "resolve_review_queue", "key:a", {"x": 2})
        log_action(self.store, "acme", "purge", "key:a", {"x": 3})
        ok, broke_at = verify_chain(self.store, "acme")
        self.assertTrue(ok)
        self.assertIsNone(broke_at)

    def test_empty_chain_is_valid(self):
        """A tenant with no audit entries counts as a valid chain."""
        ok, broke_at = verify_chain(self.store, "brand_new_tenant")
        self.assertTrue(ok)

    def test_tampering_with_stored_entry_breaks_verification(self):
        """Editing a stored entry is detected.

        Writes two entries, changes the first one's ``details`` directly in the
        database, and checks ``verify_chain`` reports a break and names an entry.
        """
        log_action(self.store, "acme", "ingest", "key:a", {"record": "real"})
        log_action(self.store, "acme", "ingest", "key:a", {"record": "real2"})

        with self.store._conn() as conn:
            conn.execute("UPDATE audit_log SET details = '{\"record\": \"FORGED\"}' WHERE rowid = 1")

        ok, broke_at = verify_chain(self.store, "acme")
        self.assertFalse(ok)
        self.assertIsNotNone(broke_at)

    def test_chains_are_independent_per_tenant(self):
        """Each tenant has its own chain.

        One entry per tenant; each tenant sees only its own entry, and tenant
        A's entry doesn't chain to tenant B's.
        """
        log_action(self.store, "tenant_a", "ingest", "key:a", {})
        log_action(self.store, "tenant_b", "ingest", "key:b", {})
        entries_a = self.store.list_audit_log("tenant_a")
        entries_b = self.store.list_audit_log("tenant_b")
        self.assertEqual(len(entries_a), 1)
        self.assertEqual(len(entries_b), 1)
        self.assertNotEqual(entries_a[0].prev_hash, entries_b[0].entry_hash)  # different chains entirely


class TestRightToBeForgotten(unittest.TestCase):
    def setUp(self):
        """Create a fresh SQLite store in ``test_phase3.db`` for each test.

        Any leftover file from a previous run is deleted first.
        """
        self.db_path = "test_phase3.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

    def tearDown(self):
        """Delete the test database file after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_purge_removes_entity_and_all_source_records(self):
        """Purging deletes the golden record and both of its source records.

        Two records sharing an email are merged, then purged. Checks the
        result reports 2 deleted records, and that neither the entity nor its
        source records can be found afterwards.
        """
        r1 = ingest_record(self.store, "acme", "customer", "crm", "C1", {
            "first_name": "Ada", "last_name": "Lovelace", "email": "ada@x.com",
        })
        r2 = ingest_record(self.store, "acme", "customer", "shop", "C2", {
            "first_name": "Ada", "last_name": "Lovelace", "email": "ada@x.com",
        })
        entity_id = r1["matched_entity_id"]
        self.assertEqual(len(self.store.list_source_records("acme", "customer", entity_id=entity_id)), 2)

        result = purge_entity(self.store, "acme", "customer", entity_id, requested_by="steward:dpo")

        self.assertEqual(result.source_records_deleted, 2)
        self.assertIsNone(self.store.get_entity(entity_id, "acme", "customer"))
        self.assertEqual(len(self.store.list_source_records("acme", "customer", entity_id=entity_id)), 0)

    def test_purge_is_audit_logged(self):
        """A purge writes a ``purge`` entry to the audit log, and the chain stays valid."""
        r1 = ingest_record(self.store, "acme", "customer", "crm", "C1", {"first_name": "X", "last_name": "Y"})
        purge_entity(self.store, "acme", "customer", r1["matched_entity_id"], requested_by="steward:dpo")
        log = self.store.list_audit_log("acme")
        self.assertTrue(any(e.action == "purge" for e in log))
        ok, _ = verify_chain(self.store, "acme")
        self.assertTrue(ok)

    def test_purging_nonexistent_entity_raises(self):
        """Purging an entity that doesn't exist raises ``ValueError``."""
        with self.assertRaises(ValueError):
            purge_entity(self.store, "acme", "customer", "ent_does_not_exist", requested_by="steward:dpo")

    def test_purge_does_not_affect_other_tenants_or_domains(self):
        """A purge only touches its own tenant and domain.

        After purging a customer in tenant ``acme``, acme's product entity and
        another tenant's customer entity must still exist.
        """
        r1 = ingest_record(self.store, "acme", "customer", "crm", "C1", {"first_name": "X", "last_name": "Y"})
        ingest_record(self.store, "acme", "product", "erp", "P1", {"product_name": "Widget"})
        ingest_record(self.store, "other_tenant", "customer", "crm", "C1", {"first_name": "X", "last_name": "Y"})

        purge_entity(self.store, "acme", "customer", r1["matched_entity_id"], requested_by="steward:dpo")

        self.assertEqual(len(self.store.list_entities("acme", "product")), 1)  # untouched
        self.assertEqual(len(self.store.list_entities("other_tenant", "customer")), 1)  # untouched


class TestBilling(unittest.TestCase):
    def setUp(self):
        """Create a fresh SQLite store in ``test_phase3.db`` for each test.

        Any leftover file from a previous run is deleted first.
        """
        self.db_path = "test_phase3.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

    def tearDown(self):
        """Delete the test database file after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_usage_is_recorded_and_summarized(self):
        """Usage events are totalled per type and priced.

        Two ``record_ingested`` events and one ``api_call`` event with
        quantity 10 must summarize as 2 and 10, with a cost above zero.
        """
        record_usage(self.store, "acme", "record_ingested", domain="customer")
        record_usage(self.store, "acme", "record_ingested", domain="customer")
        record_usage(self.store, "acme", "api_call", quantity=10)

        summary = get_usage_summary(self.store, "acme")
        self.assertEqual(summary.counts_by_event_type["record_ingested"], 2)
        self.assertEqual(summary.counts_by_event_type["api_call"], 10)
        self.assertGreater(summary.estimated_cost_usd, 0)

    def test_usage_is_isolated_per_tenant(self):
        """Usage counts are kept separately per tenant (1 for tenant A, 2 for tenant B)."""
        record_usage(self.store, "tenant_a", "record_ingested")
        record_usage(self.store, "tenant_b", "record_ingested")
        record_usage(self.store, "tenant_b", "record_ingested")
        self.assertEqual(get_usage_summary(self.store, "tenant_a").counts_by_event_type["record_ingested"], 1)
        self.assertEqual(get_usage_summary(self.store, "tenant_b").counts_by_event_type["record_ingested"], 2)

    def test_logging_billing_provider_never_charges_for_real(self):
        """``LoggingBillingProvider`` only logs the intended charge.

        The returned record's note says no real charge was made, and the charge
        log holds exactly one entry.
        """
        record_usage(self.store, "acme", "record_ingested")
        summary = get_usage_summary(self.store, "acme")
        provider = LoggingBillingProvider()
        result = provider.charge("acme", summary)
        self.assertIn("no real charge", result["note"])
        self.assertEqual(len(provider.charge_log), 1)


class TestCSVConnector(unittest.TestCase):
    def setUp(self):
        """Create a fresh test database and a temporary 3-row CSV file.

        Columns: ``ID, First, Last, Email``. Rows: Robert Smith and Bob Smith
        (same email), and Maria Garcia.
        """
        self.db_path = "test_phase3.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

        self.csv_path = tempfile.mktemp(suffix=".csv")
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "First", "Last", "Email"])
            writer.writerow(["1", "Robert", "Smith", "bob@x.com"])
            writer.writerow(["2", "Bob", "Smith", "bob@x.com"])
            writer.writerow(["3", "Maria", "Garcia", "maria@x.com"])

    def tearDown(self):
        """Delete the test database and the temporary CSV file."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)

    def test_csv_connector_reads_and_maps_fields(self):
        """The CSV connector reads all rows and renames columns to canonical fields.

        Checks 3 records are returned and the first is
        ``("1", {"first_name": "Robert", "last_name": "Smith", "email": "bob@x.com"})``.
        """
        connector = CSVConnector(
            csv_path=self.csv_path,
            field_mapping={"First": "first_name", "Last": "last_name", "Email": "email"},
            source_record_id_column="ID",
        )
        records = list(connector.fetch_records())
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0], ("1", {"first_name": "Robert", "last_name": "Smith", "email": "bob@x.com"}))

    def test_csv_connector_missing_file_raises_clearly(self):
        """Reading from a file that doesn't exist raises ``FileNotFoundError``."""
        connector = CSVConnector(csv_path="/nonexistent/path.csv", field_mapping={})
        with self.assertRaises(FileNotFoundError):
            list(connector.fetch_records())

    def test_ingest_from_connector_runs_full_pipeline(self):
        """Importing the CSV runs every row through matching.

        3 rows are ingested; the two Smiths share an email and merge, so the
        tenant ends up with 2 golden records.
        """
        connector = CSVConnector(
            csv_path=self.csv_path,
            field_mapping={"First": "first_name", "Last": "last_name", "Email": "email"},
            source_record_id_column="ID",
        )
        results = ingest_from_connector(self.store, "acme", "customer", connector)
        self.assertEqual(len(results), 3)
        entities = self.store.list_entities("acme", "customer")
        # Bob/Robert Smith share an email -> should merge into 1; Maria stands alone -> 2 total
        self.assertEqual(len(entities), 2)


if __name__ == "__main__":
    unittest.main()
