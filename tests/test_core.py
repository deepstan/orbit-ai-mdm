"""
Run with:  python3 -m unittest discover -s tests -v
No external dependencies required.
"""
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.active_learning import LearnedModel, save_model
from app.db import Store
from app.domains import get_domain_config
from app.llm_adjudication import AdjudicationResult
from app.matching import classify, score_pair
from app.pipeline import ingest_record, resolve_candidate
from app.standardize import normalize_phone, normalize_postal_code, standardize

TENANT_A = "tenant_acme"
TENANT_B = "tenant_globex"


class TestStandardize(unittest.TestCase):
    def test_phone_normalization_strips_formatting_and_country_code(self):
        """Phone numbers in different formats normalize to the same 10 digits.

        Checks that brackets, dashes and spaces are removed, and that a leading
        US country code ``+1`` is dropped.
        """
        self.assertEqual(normalize_phone("(415) 555-0100"), "4155550100")
        self.assertEqual(normalize_phone("+1 415-555-0100"), "4155550100")

    def test_postal_code_collapses_zip_plus_4(self):
        """A ZIP+4 code (``"94107-1234"``) is reduced to its 5-digit ZIP."""
        self.assertEqual(normalize_postal_code("94107-1234"), "94107")

    def test_missing_fields_never_raise_for_any_domain(self):
        # country defaults to "US" when absent (see standardize_customer /
        # standardize_vendor) -- that's intentional, not a bug, so it's
        # excluded from the "everything else is empty" check below.
        """Standardizing an empty payload works for every domain.

        For customer, product and vendor, ``standardize(domain, {})`` must not
        raise, and every field must be ``""``, except ``country``, which
        defaults to ``"US"`` by design.
        """
        for domain in ("customer", "product", "vendor"):
            result = standardize(domain, {})
            non_default_values = {k: v for k, v in result.items() if k != "country"}
            self.assertTrue(all(v == "" for v in non_default_values.values()), non_default_values)

    def test_unknown_domain_raises_clear_error(self):
        """Standardizing for an unregistered domain raises ``KeyError``."""
        with self.assertRaises(KeyError):
            standardize("nonexistent_domain", {})

    def test_sku_normalization_strips_separators_and_case(self):
        """SKUs lose dashes and are uppercased: ``"nk-am90-blk-10"`` -> ``"NKAM90BLK10"``."""
        result = standardize("product", {"sku": "nk-am90-blk-10"})
        self.assertEqual(result["sku"], "NKAM90BLK10")

    def test_tax_id_normalization_strips_formatting(self):
        """Tax IDs lose formatting: ``"12-3456789"`` -> ``"123456789"``."""
        result = standardize("vendor", {"tax_id": "12-3456789"})
        self.assertEqual(result["tax_id"], "123456789")


class TestMatchingAcrossDomains(unittest.TestCase):
    def test_customer_exact_email_is_deterministic_auto_merge(self):
        """Two customers with the same email are a deterministic match and auto-merge."""
        config = get_domain_config("customer")
        a = standardize("customer", {"email": "x@y.com", "first_name": "A", "last_name": "B"})
        b = standardize("customer", {"email": "x@y.com", "first_name": "A", "last_name": "B"})
        r = score_pair(a, b, config)
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "auto_merge")

    def test_product_exact_sku_is_deterministic_auto_merge(self):
        """Two products with the same SKU (after normalization) auto-merge.

        ``"ABC-123"`` and ``"abc123"`` match even though the product names
        differ.
        """
        config = get_domain_config("product")
        a = standardize("product", {"sku": "ABC-123", "product_name": "Widget"})
        b = standardize("product", {"sku": "abc123", "product_name": "Widget Pro"})
        r = score_pair(a, b, config)
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "auto_merge")

    def test_vendor_exact_tax_id_is_deterministic_auto_merge(self):
        """Two vendors with the same tax ID (after normalization) auto-merge.

        ``"12-3456789"`` and ``"123456789"`` match even though the company names
        differ.
        """
        config = get_domain_config("vendor")
        a = standardize("vendor", {"tax_id": "12-3456789", "company_name": "Acme"})
        b = standardize("vendor", {"tax_id": "123456789", "company_name": "Acme Inc"})
        r = score_pair(a, b, config)
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "auto_merge")

    def test_product_semantic_signal_catches_reworded_names(self):
        """Reworded product names still score as a possible match.

        "Nike Air Max 90" and "Air Max 90 by Nike" (different categories, no
        SKU) must not be classified as ``no_match``; the semantic signal should
        lift them to at least review.
        """
        config = get_domain_config("product")
        a = standardize("product", {"product_name": "Nike Air Max 90", "brand": "Nike", "category": "Shoes"})
        b = standardize("product", {"product_name": "Air Max 90 by Nike", "brand": "Nike", "category": "Footwear"})
        r = score_pair(a, b, config)
        self.assertNotEqual(classify(r.score), "no_match")

    def test_vendor_clearly_different_companies_do_not_auto_merge(self):
        """Clearly different vendors in different cities are not auto-merged."""
        config = get_domain_config("vendor")
        a = standardize("vendor", {"company_name": "Acme Supply", "city": "Dallas", "postal_code": "75201"})
        b = standardize("vendor", {"company_name": "Zenith Robotics", "city": "Reno", "postal_code": "89501"})
        r = score_pair(a, b, config)
        self.assertNotEqual(classify(r.score), "auto_merge")


class TestPipelineEndToEnd(unittest.TestCase):
    def setUp(self):
        """Create a fresh SQLite store in ``test_pipeline.db`` for each test.

        Any leftover file from a previous run is deleted first.
        """
        self.db_path = "test_pipeline.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

    def tearDown(self):
        """Delete the test database file after each test."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_exact_email_match_auto_merges_into_one_entity(self):
        """Full pipeline: two customers sharing an email end up in one golden record."""
        r1 = ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Pat", "last_name": "Nolan", "email": "pat@x.com",
            "address_line1": "1 Elm St", "city": "Boston", "postal_code": "02108",
        })
        r2 = ingest_record(self.store, TENANT_A, "customer", "shop", "S1", {
            "first_name": "Patricia", "last_name": "Nolan", "email": "pat@x.com",
            "address_line1": "1 Elm Street", "city": "Boston", "postal_code": "02108",
        })
        self.assertEqual(r1["matched_entity_id"], r2["matched_entity_id"])

    def test_product_ingestion_auto_merges_on_sku(self):
        """Full pipeline: two products with the same SKU (different case) end up in one golden record."""
        r1 = ingest_record(self.store, TENANT_A, "product", "erp", "P1", {
            "sku": "ABC-123", "product_name": "Widget", "brand": "Acme",
        })
        r2 = ingest_record(self.store, TENANT_A, "product", "shop", "P2", {
            "sku": "abc-123", "product_name": "Widget Pro", "brand": "Acme",
        })
        self.assertEqual(r1["matched_entity_id"], r2["matched_entity_id"])

    def test_vendor_ingestion_auto_merges_on_tax_id(self):
        """Full pipeline: two vendors with the same tax ID (different formatting) end up in one golden record."""
        r1 = ingest_record(self.store, TENANT_A, "vendor", "erp", "V1", {
            "company_name": "Acme Supply", "tax_id": "12-3456789",
        })
        r2 = ingest_record(self.store, TENANT_A, "vendor", "ap", "V2", {
            "company_name": "Acme Supply Co", "tax_id": "123456789",
        })
        self.assertEqual(r1["matched_entity_id"], r2["matched_entity_id"])

    def test_review_queue_confirm_merges_records(self):
        """A steward can confirm a queued match.

        Ingests the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band, checks exactly one candidate is pending, then confirms it
        and checks the result status is ``"confirmed"``.
        """
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        pending = self.store.list_candidates(TENANT_A, "customer", "pending")
        self.assertEqual(len(pending), 1)
        result = resolve_candidate(self.store, TENANT_A, "customer", pending[0].id, "confirm", "steward:test")
        self.assertEqual(result["status"], "confirmed")

    # --- Cross-DOMAIN isolation (the new guarantee this round proves) -----
    def test_identical_looking_data_never_matches_across_domains(self):
        """The core new isolation guarantee: two different domains, same
        tenant, with attribute dicts that happen to share key names/values
        must never be compared -- there is no shared candidate pool."""
        # A vendor record and a customer record that both have a
        # "company/last name" that happens to collide, and both mention
        # "acme.com" -- these must never be compared to each other.
        r_customer = ingest_record(self.store, TENANT_A, "customer", "crm", "X1", {
            "first_name": "Acme", "last_name": "Corp", "email": "contact@acme.com",
        })
        r_vendor = ingest_record(self.store, TENANT_A, "vendor", "erp", "X2", {
            "company_name": "Acme Corp", "contact_email": "contact@acme.com",
        })
        self.assertNotEqual(r_customer["matched_entity_id"], r_vendor["matched_entity_id"])
        customer_entities = self.store.list_entities(TENANT_A, "customer")
        vendor_entities = self.store.list_entities(TENANT_A, "vendor")
        self.assertEqual(len(customer_entities), 1)
        self.assertEqual(len(vendor_entities), 1)
        # No pending candidates in either domain -- proves matching never
        # even attempted a cross-domain comparison.
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "customer", "pending")), 0)
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "vendor", "pending")), 0)

    def test_entity_lookup_respects_domain_boundary(self):
        """Domain isolation: an entity can only be read in its own domain.

        A product entity is found under ``"product"`` but must return ``None``
        when looked up with the same ID under ``"customer"`` for the same
        tenant.
        """
        r = ingest_record(self.store, TENANT_A, "product", "erp", "P1", {
            "sku": "XYZ-1", "product_name": "Thing",
        })
        entity_id = r["matched_entity_id"]
        self.assertIsNotNone(self.store.get_entity(entity_id, TENANT_A, "product"))
        # Same entity_id, same tenant, but asked under the "customer" domain
        # scope -- must not resolve, even though the ID technically exists
        # in the product table. This is what stops a domain mix-up bug
        # from leaking one domain's data into another's API response.
        self.assertIsNone(self.store.get_entity(entity_id, TENANT_A, "customer"))

    # --- Cross-TENANT isolation (still holds with domains added) ----------
    def test_identical_records_in_different_tenants_never_match_each_other(self):
        """Tenant isolation: identical records in two tenants are never matched.

        The same payload (same email) ingested into tenant A and tenant B must
        produce two separate golden records, one per tenant.
        """
        payload = {"first_name": "Robert", "last_name": "Smith", "email": "bob@shared.com"}
        r_a = ingest_record(self.store, TENANT_A, "customer", "crm", "SAME-ID", payload)
        r_b = ingest_record(self.store, TENANT_B, "customer", "crm", "SAME-ID", payload)
        self.assertNotEqual(r_a["matched_entity_id"], r_b["matched_entity_id"])
        self.assertEqual(len(self.store.list_entities(TENANT_A, "customer")), 1)
        self.assertEqual(len(self.store.list_entities(TENANT_B, "customer")), 1)

    def test_resolving_candidate_from_wrong_tenant_fails(self):
        """Tenant isolation: another tenant can't resolve your review items.

        Creates a pending candidate in tenant A from the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band, then checks that
        resolving it as tenant B raises ``ValueError`` (the candidate isn't
        visible to tenant B).
        """
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        pending = self.store.list_candidates(TENANT_A, "customer", "pending")
        self.assertEqual(len(pending), 1)
        with self.assertRaises(ValueError):
            resolve_candidate(self.store, TENANT_B, "customer", pending[0].id, "confirm", "steward:intruder")

    # --- LLM adjudication (mocked -- no network in this sandbox) ---------
    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_confident_match_auto_merges_without_steward(self, mock_adjudicate, *_mocks):
        """A confident LLM "match" verdict auto-merges without a steward.

        LLM adjudication is mocked to be enabled and to return a match with
        0.93 confidence. Ingesting the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band must leave nothing pending and give the
        second record a golden record.

        Args:
            mock_adjudicate: Mock for ``llm_adjudication.adjudicate``.
            *_mocks: Mocks for ``in_adjudication_band`` and ``is_enabled`` (unused).
        """
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=True, confidence=0.93, explanation="Same person, moved address."
        )
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        r2 = ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "customer", "pending")), 0)
        self.assertIsNotNone(r2["matched_entity_id"])

    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_confident_non_match_rejects_without_steward(self, mock_adjudicate, *_mocks):
        """A confident LLM "not a match" verdict rejects without a steward.

        The mocked LLM returns no-match with 0.90 confidence. After ingesting
        the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band, nothing is pending and the second record gets its own golden
        record, separate from the first.

        Args:
            mock_adjudicate: Mock for ``llm_adjudication.adjudicate``.
            *_mocks: Mocks for ``in_adjudication_band`` and ``is_enabled`` (unused).
        """
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=False, confidence=0.90, explanation="Common name, different households."
        )
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        r1 = self.store.list_source_records(TENANT_A, "customer")[0]
        r2 = ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "customer", "pending")), 0)
        self.assertNotEqual(r2["matched_entity_id"], r1.entity_id)

    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_uncertain_falls_back_to_steward_review(self, mock_adjudicate, *_mocks):
        """An unsure LLM verdict leaves the pair for a steward.

        The mocked LLM says "match" with only 0.55 confidence (below the 0.85
        needed to act), so exactly one candidate must stay pending.

        Args:
            mock_adjudicate: Mock for ``llm_adjudication.adjudicate``.
            *_mocks: Mocks for ``in_adjudication_band`` and ``is_enabled`` (unused).
        """
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=True, confidence=0.55, explanation="Plausible but not confident."
        )
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "customer", "pending")), 1)

    def test_adjudication_disabled_by_default_never_calls_llm(self):
        """With no LLM settings, the LLM is never called.

        Ingests the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band without enabling adjudication and checks
        ``adjudicate`` was never called, so no API cost can occur by default.
        """
        with patch("app.pipeline.llm_adjudication.adjudicate") as mock_adjudicate:
            ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
                "first_name": "Jennifer", "last_name": "Nguyen",
                "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
            })
            ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
                "first_name": "Jen", "last_name": "Nguyen",
                "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
            })
            mock_adjudicate.assert_not_called()

    # --- Active learning ----------------------------------------------------
    def test_no_model_means_unchanged_behavior(self):
        """Without a trained model, matching behaves as before.

        Ingests the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band and checks it lands in the review queue (one pending
        candidate), as the rule-based scorer alone decides.
        """
        ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "customer", "pending")), 1)

    def test_trained_model_measurably_changes_the_score(self):
        """A saved learned model is blended into the match score.

        Saves a customer model that always predicts "match" (bias 10, all
        weights 0), ingests the Jennifer / Jen Nguyen pair (similar names and addresses, no shared email or phone), which scores in the review band, and checks the candidate's score is above
        0.87 and its reasons mention the learned model. The model file is
        deleted afterwards.

        Note: writes to the real ``models/`` folder in the current directory.
        """
        config = get_domain_config("customer")
        from app.active_learning import feature_names
        n_features = len(feature_names(config))
        confident_model = LearnedModel(
            weights=[0.0] * n_features, bias=10.0, trained_on_examples=25,
            tenant_id=TENANT_A, domain="customer",
        )
        save_model(confident_model)
        try:
            ingest_record(self.store, TENANT_A, "customer", "crm", "C1", {
                "first_name": "Jennifer", "last_name": "Nguyen",
                "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
            })
            ingest_record(self.store, TENANT_A, "customer", "csv", "C2", {
                "first_name": "Jen", "last_name": "Nguyen",
                "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
            })
            candidates = self.store.list_candidates(TENANT_A, "customer", status=None)
            self.assertEqual(len(candidates), 1)
            self.assertGreater(candidates[0].score, 0.87)
            self.assertTrue(any("blended with learned model" in r for r in candidates[0].reasons))
        finally:
            import os as _os
            model_file = f"models/{TENANT_A}__customer_match_model.json"
            if _os.path.exists(model_file):
                _os.remove(model_file)

    def test_active_learning_models_are_isolated_per_domain(self):
        """A model trained for the 'customer' domain must never apply to
        'product' matching for the same tenant -- feature vectors aren't
        even the same shape between domains.

        Saves an always-match customer model for tenant A, ingests two similar
        products for the same tenant, and checks no product candidate mentions
        the learned model. The model file is deleted afterwards.
        """
        config = get_domain_config("customer")
        from app.active_learning import feature_names
        n_features = len(feature_names(config))
        save_model(LearnedModel(weights=[0.0] * n_features, bias=10.0, trained_on_examples=25,
                                 tenant_id=TENANT_A, domain="customer"))
        try:
            # Ingest into PRODUCT for the same tenant -- the customer model
            # on disk must not be loaded or applied here.
            r1 = ingest_record(self.store, TENANT_A, "product", "erp", "P1", {
                "product_name": "Blue Widget", "brand": "Acme", "category": "Tools",
            })
            r2 = ingest_record(self.store, TENANT_A, "product", "shop", "P2", {
                "product_name": "Blue Widget V2", "brand": "Acme", "category": "Hardware",
            })
            # No crash, and no "blended with learned model" reasons should
            # appear anywhere in the product domain's candidates.
            candidates = self.store.list_candidates(TENANT_A, "product", status=None)
            for c in candidates:
                self.assertFalse(any("blended with learned model" in r for r in c.reasons))
        finally:
            import os as _os
            model_file = f"models/{TENANT_A}__customer_match_model.json"
            if _os.path.exists(model_file):
                _os.remove(model_file)


if __name__ == "__main__":
    unittest.main()
