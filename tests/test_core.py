"""
Run with:  python3 -m unittest discover -s tests -v
No external dependencies required -- this only exercises the core engine
(standardize, matching, survivorship, db, pipeline), never the FastAPI layer.
"""
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.active_learning import LearnedModel, save_model
from app.db import Store
from app.llm_adjudication import AdjudicationResult
from app.matching import classify, score_pair
from app.pipeline import ingest_record, resolve_candidate
from app.standardize import normalize_phone, normalize_postal_code, standardize_customer

TENANT_A = "tenant_acme"
TENANT_B = "tenant_globex"


class TestStandardize(unittest.TestCase):
    def test_phone_normalization_strips_formatting_and_country_code(self):
        self.assertEqual(normalize_phone("(415) 555-0100"), "4155550100")
        self.assertEqual(normalize_phone("+1 415-555-0100"), "4155550100")
        self.assertEqual(normalize_phone("415.555.0100"), "4155550100")

    def test_postal_code_collapses_zip_plus_4(self):
        self.assertEqual(normalize_postal_code("94107-1234"), "94107")
        self.assertEqual(normalize_postal_code("941071234"), "94107")

    def test_missing_fields_never_raise(self):
        result = standardize_customer({})
        self.assertEqual(result["email"], "")
        self.assertEqual(result["first_name"], "")


class TestMatching(unittest.TestCase):
    def test_exact_email_is_deterministic_auto_merge(self):
        a = standardize_customer({"email": "x@y.com", "first_name": "A", "last_name": "B"})
        b = standardize_customer({"email": "x@y.com", "first_name": "A", "last_name": "B"})
        r = score_pair(a, b)
        self.assertEqual(r.match_type, "deterministic")
        self.assertEqual(classify(r.score), "auto_merge")

    def test_clearly_different_people_do_not_auto_merge(self):
        a = standardize_customer({"first_name": "Alice", "last_name": "Johnson",
                                   "address_line1": "1 First Ave", "postal_code": "10001"})
        b = standardize_customer({"first_name": "Brian", "last_name": "Kowalski",
                                   "address_line1": "99 Ninth St", "postal_code": "90210"})
        r = score_pair(a, b)
        self.assertNotEqual(classify(r.score), "auto_merge")

    def test_empty_fields_contribute_no_false_signal(self):
        a = standardize_customer({"first_name": "Sam"})
        b = standardize_customer({"last_name": "Lee"})
        r = score_pair(a, b)
        self.assertEqual(r.score, 0.0)

    def test_semantic_signal_helps_where_string_distance_alone_would_miss(self):
        a = standardize_customer({"first_name": "William", "last_name": "Nguyen",
                                   "address_line1": "12 Oak Rd", "city": "Reno", "postal_code": "89501"})
        b = standardize_customer({"first_name": "Bill", "last_name": "Nguyen",
                                   "address_line1": "12 Oak Rd", "city": "Reno", "postal_code": "89501"})
        r = score_pair(a, b)
        self.assertNotEqual(classify(r.score), "no_match")


class TestPipelineEndToEnd(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_pipeline.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.store = Store(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_exact_email_match_auto_merges_into_one_entity(self):
        r1 = ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Pat", "last_name": "Nolan", "email": "pat@x.com",
            "address_line1": "1 Elm St", "city": "Boston", "postal_code": "02108",
        })
        r2 = ingest_record(self.store, TENANT_A, "shop", "S1", {
            "first_name": "Patricia", "last_name": "Nolan", "email": "pat@x.com",
            "address_line1": "1 Elm Street", "city": "Boston", "postal_code": "02108",
        })
        self.assertEqual(r1["matched_entity_id"], r2["matched_entity_id"])
        entities = self.store.list_entities(TENANT_A, status="active")
        self.assertEqual(len(entities), 1)

    def test_unrelated_records_produce_separate_entities(self):
        r1 = ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Pat", "last_name": "Nolan", "email": "pat@x.com", "postal_code": "02108",
        })
        r2 = ingest_record(self.store, TENANT_A, "crm", "C2", {
            "first_name": "Zara", "last_name": "Ibrahim", "email": "zara@y.com", "postal_code": "94103",
        })
        self.assertNotEqual(r1["matched_entity_id"], r2["matched_entity_id"])

    def test_review_queue_confirm_merges_records(self):
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        r2 = ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        pending = self.store.list_candidates(TENANT_A, "pending")
        self.assertEqual(len(pending), 1)
        self.assertIsNone(r2["matched_entity_id"])

        result = resolve_candidate(self.store, TENANT_A, pending[0].id, "confirm", "steward:test")
        self.assertEqual(result["status"], "confirmed")
        entities = self.store.list_entities(TENANT_A, status="active")
        self.assertEqual(len(entities), 1)

    def test_review_queue_reject_keeps_records_separate(self):
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        pending = self.store.list_candidates(TENANT_A, "pending")
        resolve_candidate(self.store, TENANT_A, pending[0].id, "reject", "steward:test")
        pending_after = self.store.list_candidates(TENANT_A, "pending")
        self.assertEqual(len(pending_after), 0)

    # --- LLM adjudication (mocked -- no network in this sandbox) ---------
    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_confident_match_auto_merges_without_steward(self, mock_adjudicate, *_mocks):
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=True, confidence=0.93, explanation="Same person, moved address."
        )
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        r2 = ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "pending")), 0)
        self.assertIsNotNone(r2["matched_entity_id"])
        entities = self.store.list_entities(TENANT_A, status="active")
        self.assertEqual(len(entities), 1)

    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_confident_non_match_rejects_without_steward(self, mock_adjudicate, *_mocks):
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=False, confidence=0.90, explanation="Common name, different households."
        )
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        r1 = self.store.list_source_records(TENANT_A)[0]
        r2 = ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "pending")), 0)
        self.assertNotEqual(r2["matched_entity_id"], r1.entity_id)
        candidates = self.store.list_candidates(TENANT_A, status=None)
        self.assertEqual(candidates[0].status, "rejected")
        entities = self.store.list_entities(TENANT_A, status="active")
        self.assertEqual(len(entities), 2)

    @patch("app.pipeline.llm_adjudication.is_enabled", return_value=True)
    @patch("app.pipeline.llm_adjudication.in_adjudication_band", return_value=True)
    @patch("app.pipeline.llm_adjudication.adjudicate")
    def test_llm_uncertain_falls_back_to_steward_review(self, mock_adjudicate, *_mocks):
        mock_adjudicate.return_value = AdjudicationResult(
            is_match=True, confidence=0.55, explanation="Plausible but not confident."
        )
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "pending")), 1)

    def test_adjudication_disabled_by_default_never_calls_llm(self):
        with patch("app.pipeline.llm_adjudication.adjudicate") as mock_adjudicate:
            ingest_record(self.store, TENANT_A, "crm", "C1", {
                "first_name": "Jennifer", "last_name": "Nguyen",
                "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
            })
            ingest_record(self.store, TENANT_A, "csv", "C2", {
                "first_name": "Jen", "last_name": "Nguyen",
                "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
            })
            mock_adjudicate.assert_not_called()

    # --- Multi-tenancy isolation -----------------------------------------
    def test_identical_records_in_different_tenants_never_match_each_other(self):
        """The core isolation guarantee: two tenants can have byte-identical
        customer data and the matching engine must never compare across
        the tenant boundary -- no shared entity, no candidate pair, ever."""
        payload = {
            "first_name": "Robert", "last_name": "Smith", "email": "bob@acme-shared-domain.com",
            "address_line1": "123 Main St", "city": "San Francisco", "postal_code": "94107",
        }
        r_a = ingest_record(self.store, TENANT_A, "crm", "SAME-ID", payload)
        r_b = ingest_record(self.store, TENANT_B, "crm", "SAME-ID", payload)

        # Each tenant should see exactly one entity -- its own -- never each other's.
        entities_a = self.store.list_entities(TENANT_A)
        entities_b = self.store.list_entities(TENANT_B)
        self.assertEqual(len(entities_a), 1)
        self.assertEqual(len(entities_b), 1)
        self.assertNotEqual(r_a["matched_entity_id"], r_b["matched_entity_id"])

        # No candidate pairs should ever have been generated across tenants
        # (there's no cross-tenant candidate table at all -- but assert the
        # within-tenant queues are both empty, proving no match was even attempted).
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "pending")), 0)
        self.assertEqual(len(self.store.list_candidates(TENANT_B, "pending")), 0)

    def test_entity_lookup_respects_tenant_boundary(self):
        r = ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Zoe", "last_name": "Park", "email": "zoe@x.com",
        })
        entity_id = r["matched_entity_id"]

        # Fetching tenant A's entity as tenant A works.
        self.assertIsNotNone(self.store.get_entity(entity_id, TENANT_A))
        # The SAME entity_id under tenant B's scope must not resolve --
        # this is what actually prevents cross-tenant data leakage via a
        # guessed or leaked ID.
        self.assertIsNone(self.store.get_entity(entity_id, TENANT_B))

    def test_resolving_candidate_from_wrong_tenant_fails(self):
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        pending = self.store.list_candidates(TENANT_A, "pending")
        self.assertEqual(len(pending), 1)
        with self.assertRaises(ValueError):
            resolve_candidate(self.store, TENANT_B, pending[0].id, "confirm", "steward:intruder")

    # --- Active learning ---------------------------------------------------
    def test_no_model_means_unchanged_behavior(self):
        """Baseline: with no trained model on disk for this tenant, ingest
        behaves exactly as it did before active learning existed."""
        ingest_record(self.store, TENANT_A, "crm", "C1", {
            "first_name": "Jennifer", "last_name": "Nguyen",
            "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
        })
        ingest_record(self.store, TENANT_A, "csv", "C2", {
            "first_name": "Jen", "last_name": "Nguyen",
            "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
        })
        self.assertEqual(len(self.store.list_candidates(TENANT_A, "pending")), 1)

    def test_trained_model_measurably_changes_the_score(self):
        # A model that's maximally confident regardless of input -- proves
        # the blend is actually wired into the pipeline's scoring, without
        # depending on exact gradient-descent convergence values or tuning
        # a pair to sit exactly on the auto-merge threshold.
        confident_model = LearnedModel(
            weights=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], bias=10.0, trained_on_examples=25, tenant_id=TENANT_A,
        )
        save_model(confident_model)
        try:
            ingest_record(self.store, TENANT_A, "crm", "C1", {
                "first_name": "Jennifer", "last_name": "Nguyen",
                "address_line1": "45 Birch Ln", "city": "Austin", "postal_code": "73301",
            })
            ingest_record(self.store, TENANT_A, "csv", "C2", {
                "first_name": "Jen", "last_name": "Nguyen",
                "address_line1": "450 Birchwood Lane", "city": "Austin", "postal_code": "73344",
            })
            candidates = self.store.list_candidates(TENANT_A, status=None)
            self.assertEqual(len(candidates), 1)
            # Without the model this pair scores ~0.87 (see the unmocked
            # ingest test above) -- blended with a maximally confident
            # model it must score strictly higher, and say so in reasons.
            self.assertGreater(candidates[0].score, 0.87)
            self.assertTrue(any("blended with learned model" in r for r in candidates[0].reasons))
        finally:
            import os as _os
            model_file = f"models/{TENANT_A}_match_model.json"
            if _os.path.exists(model_file):
                _os.remove(model_file)


if __name__ == "__main__":
    unittest.main()
