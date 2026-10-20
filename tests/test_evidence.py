"""
What a steward (or an agent working next to one) sees for a review item, and what the read-only agent role may and may not do.

  * the EVIDENCE for a review item is group-aware: both records, both GROUPS (members, golden record, size), whether a person separated them, the history of what changed
  * a record's HISTORY: now, and every earlier version
  * every value that leaves is MASKED; another tenant's candidate is a 404 (the same answer as 'does not exist')
  * the `steward_agent` role can look at everything a steward looks at and the audit sample, and can change NOTHING

Like the other wiring tests this calls the endpoint functions directly, on a real database (tests/fake_web.py stands in for FastAPI where it is not installed).
Run with:  python3 -m unittest tests.test_evidence -v
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import fake_web  # noqa: E402
from app import limits  # noqa: E402
from app.auth import PERMISSIONS, AuthError, Principal, ROLES  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import MatchCandidate  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402

api = None
_saved: dict = {}
_env: dict = {}


def setUpModule():
    global api
    _saved.update(fake_web.snapshot())
    for name in ("DATABASE_URL", "MDM_DEV_MODE", "MDM_AUDIT_RATE"):
        _env[name] = os.environ.get(name)
    fake_web.install()
    os.environ["DATABASE_URL"] = os.path.join(tempfile.mkdtemp(), "api_import.db")
    os.environ["MDM_AUDIT_RATE"] = "0"
    os.environ.pop("MDM_DEV_MODE", None)
    sys.modules.pop("app.api", None)
    import importlib
    api = importlib.import_module("app.api")


def tearDownModule():
    fake_web.restore(_saved)
    for name, value in _env.items():
        os.environ.pop(name, None) if value is None else os.environ.__setitem__(name, value)


ANN = {"first_name": "Ann", "last_name": "Lee", "email": "ann@x.com", "phone": "404-555-0188", "address_line1": "9 Elm Street", "city": "Atlanta", "state": "GA", "postal_code": "30301"}
MAYBE = {**ANN, "email": "other@z.com", "phone": "", "last_name": "Leigh"}        # resembles the group of Anns without being certain: a review item


def P(role="data_steward", allowed=None, label="Jane", tenant="acme"):
    return Principal(tenant_id=tenant, role=role, key_id=f"key_{role}", label=label, allowed_domains=allowed)


class Wired(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(os.path.join(self.tmp, "api.db"))
        for target, value in ((api, "store"), (api, "_limiter")):
            patcher = mock.patch.object(target, value, self.store if value == "store" else limits.RateLimiter(0))
            patcher.start()
            self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_ENGINE": "kernel", "MDM_MODEL_DIR": os.path.join(self.tmp, "models")})
        env.start()
        self.addCleanup(env.stop)

    def ingest(self, sid, attrs, tenant="acme"):
        return ingest_record(self.store, tenant, "customer", "crm", sid, attrs)

    def review_item(self, group_size=2):
        for i in range(group_size):
            self.ingest(f"A{i + 1}", ANN)                       # one group of `group_size` records of the same person
        result = self.ingest("B", MAYBE)
        self.assertEqual(len(result["queued_for_review"]), 1, result)
        return result["queued_for_review"][0]["candidate_id"], result


class TestEvidenceIsGroupAware(Wired):
    def test_both_groups_are_shown_with_their_members_and_golden_records(self):
        cid, _ = self.review_item(group_size=2)
        e = api.review_evidence("customer", cid, principal=P())
        self.assertEqual((e["candidate"]["status"], e["candidate"]["candidate_id"]), ("pending", cid))
        self.assertEqual(e["record_a"]["source_record_id"], "B")                          # the record that was being matched
        self.assertIn(e["record_b"]["source_record_id"], ("A1", "A2"))                   # the member of the other group it resembles most
        self.assertEqual((e["group_a"]["member_count"], e["group_b"]["member_count"]), (1, 2))
        self.assertEqual(sorted(m["source_record_id"] for m in e["group_b"]["members"]), ["A1", "A2"])
        self.assertEqual(e["group_b"]["golden"]["last_name"], "Lee")
        self.assertEqual(e["group_a"]["golden"]["last_name"], "Leigh")
        self.assertFalse(e["same_entity"])
        self.assertTrue(e["candidate"]["reasons"] and e["candidate"]["contributions"])   # why it scored what it did

    def test_for_a_group_of_one_it_is_simply_two_records(self):
        cid, _ = self.review_item(group_size=1)
        e = api.review_evidence("customer", cid, principal=P())
        self.assertEqual((e["group_a"]["member_count"], e["group_b"]["member_count"]), (1, 1))
        self.assertEqual(e["record_b"]["source_record_id"], "A1")

    def test_a_big_group_is_cut_to_a_page_of_members_and_says_so(self):
        cid, _ = self.review_item(group_size=3)
        with mock.patch("app.evidence.MAX_MEMBERS", 2):
            e = api.review_evidence("customer", cid, principal=P())
        self.assertEqual((e["group_b"]["member_count"], len(e["group_b"]["members"]), e["group_b"]["truncated"]), (3, 2, True))

    def test_it_says_when_a_person_separated_the_record_from_someone_in_the_other_group(self):
        cid, _ = self.review_item(group_size=2)
        before = api.review_evidence("customer", cid, principal=P())["separations"]
        self.assertEqual(before, {"records_separated_by_a_person": False, "a_was_separated_from_a_record_in_group_b": False, "entities_separated_by_a_person": False})
        records = {r.source_record_id: r for r in self.store.list_source_records("acme", "customer")}
        member = records["A1"] if api.review_evidence("customer", cid, principal=P())["record_b"]["source_record_id"] != "A1" else records["A2"]     # a member that is NOT the closest one
        self.store.save_candidate(MatchCandidate(record_a_id=records["B"].id, record_b_id=member.id, tenant_id="acme", domain="customer", score=0.9, match_type="fuzzy",
                                                 status="rejected", decided_by="unmerge:jane"))
        after = api.review_evidence("customer", cid, principal=P())["separations"]
        self.assertTrue(after["a_was_separated_from_a_record_in_group_b"])
        self.assertTrue(after["entities_separated_by_a_person"])
        self.assertFalse(after["records_separated_by_a_person"])                        # the closest member is not the one separated

    def test_the_history_of_what_the_record_used_to_say_is_included(self):
        cid, _ = self.review_item(group_size=1)
        self.ingest("B", {**MAYBE, "city": "Decatur"})                                 # B changes: version 2, and version 1 goes to history
        pending = [c for c in self.store.list_candidates("acme", "customer", status="pending")]
        e = api.review_evidence("customer", pending[0].id if pending else cid, principal=P()) if pending else api.review_evidence("customer", cid, principal=P())
        self.assertEqual(e["record_a"]["version"], 2)
        self.assertEqual([(h["version"], h["attributes"]["city"]) for h in e["history"]["record_a"]], [(1, "Atlanta")])

    def test_the_other_pairs_of_the_record_are_listed(self):
        cid, _ = self.review_item(group_size=2)
        e = api.review_evidence("customer", cid, principal=P())
        self.assertTrue(all(set(o) == {"candidate_id", "other_record_id", "status", "score"} for o in e["other_pairs_of_record_a"]))
        self.assertNotIn(cid, [o["candidate_id"] for o in e["other_pairs_of_record_a"]])      # the item itself is not its own context


class TestRecordHistory(Wired):
    def test_now_and_every_earlier_version_oldest_first(self):
        first = self.ingest("A1", ANN)
        self.ingest("A1", {**ANN, "city": "Decatur"})
        self.ingest("A1", {**ANN, "city": "Macon"})
        h = api.record_history("customer", first["record_id"], principal=P())
        self.assertEqual((h["record"]["version"], h["record"]["attributes"]["city"]), (3, "Macon"))
        self.assertEqual([(x["version"], x["attributes"]["city"]) for x in h["history"]], [(1, "Atlanta"), (2, "Decatur")])


class TestMaskingAndIsolation(Wired):
    def test_protected_values_never_leave_in_any_attribute_of_either_answer(self):
        """Every place a record's or a golden record's ATTRIBUTES appear is masked: both records, every member of both groups, both golden records, every historical version.
        (The REASONS of a score are not attributes: for a really protected field the scorer writes 'exact ssn match' without the value, which tests/test_protect.py covers.)"""
        cid, result = self.review_item(group_size=2)
        hide = lambda domain, attrs: {k: ("***" if k == "email" else v) for k, v in attrs.items()}
        with mock.patch.object(api.protection, "mask_attributes", hide):
            evidence = api.review_evidence("customer", cid, principal=P())
            history = api.record_history("customer", result["record_id"], principal=P())
        evidence = {k: v for k, v in evidence.items() if k != "candidate"}          # the candidate's reasons are text the scorer wrote, not attributes
        for name, payload in (("evidence", evidence), ("history", history)):
            text = json.dumps(payload)
            self.assertNotIn("ann@x.com", text, name)
            self.assertNotIn("other@z.com", text, name)
            self.assertIn("***", text, name)

    def test_another_tenants_candidate_or_record_is_a_404_exactly_like_one_that_does_not_exist(self):
        cid, result = self.review_item(group_size=2)
        stranger = P(tenant="other")
        for call in (lambda: api.review_evidence("customer", cid, principal=stranger), lambda: api.review_evidence("customer", "cand_nope", principal=P()),
                     lambda: api.record_history("customer", result["record_id"], principal=stranger), lambda: api.record_history("customer", "rec_nope", principal=P())):
            with self.assertRaises(api.HTTPException) as raised:
                call()
            self.assertEqual(raised.exception.status_code, 404)
        with self.assertRaises(api.HTTPException) as raised:
            api.review_evidence("nonexistent_domain", cid, principal=P())
        self.assertEqual(raised.exception.status_code, 404)

    def test_a_key_limited_to_other_domains_is_refused(self):
        cid, _ = self.review_item(group_size=1)
        with self.assertRaises(AuthError):
            api.review_evidence("customer", cid, principal=P(allowed=["product"]))


class TestTheAgentRoleChangesNothing(Wired):
    AGENT = staticmethod(lambda: P("steward_agent", label="agent-1"))

    def test_it_can_look_at_everything_a_steward_looks_at_and_the_audit_sample(self):
        cid, result = self.review_item(group_size=2)
        agent = self.AGENT()
        self.assertTrue(api.review_queue("customer", principal=agent)["items"])
        self.assertTrue(api.review_evidence("customer", cid, principal=agent)["group_b"])
        self.assertTrue(api.record_history("customer", result["record_id"], principal=agent)["record"])
        entity_id = self.store.list_entities("acme", "customer", "active")[0].id
        api.get_entity("customer", entity_id, principal=agent)
        api.get_lineage("customer", entity_id, principal=agent)
        api.list_entities("customer", principal=agent)
        api.list_runs("customer", principal=agent)
        api.schema("customer", principal=agent)
        api.list_domains(principal=agent)
        api.scorer_status("customer", principal=agent)
        self.assertIn("summary", api.audit_page("customer", principal=agent))

    def test_it_can_change_nothing(self):
        cid, result = self.review_item(group_size=2)
        agent = self.AGENT()
        entity_id = self.store.list_entities("acme", "customer", "active")[0].id
        refused = [
            lambda: api.resolve("customer", cid, api.ResolveCandidateRequest(decision="confirm"), principal=agent),
            lambda: api.audit_verdict("customer", cid, api.AuditVerdictRequest(verdict="wrong"), principal=agent),
            lambda: api.unmerge_merge("customer", cid, api.UnmergeRequest(), principal=agent),
            lambda: api.ingest("customer", api.IngestRequest(source_system="crm", source_record_id="Z", attributes=ANN), principal=agent),
            lambda: api.ingest_batch_endpoint("customer", api.BatchIngestRequest(records=[api.IngestRequest(source_system="crm", source_record_id="Z", attributes=ANN)]), principal=agent),
            lambda: api.delete_entity("customer", entity_id, principal=agent),
            lambda: api.scorer_check("customer", principal=agent),
            lambda: api.scorer_reset("customer", principal=agent),
            lambda: api.create_api_key(api.CreateApiKeyRequest(role="tenant_admin", label="sneaky"), principal=agent),
            lambda: api.list_api_keys(principal=agent),
            lambda: api.get_audit_log(principal=agent),
            lambda: api.get_usage(principal=agent),
        ]
        before = (len(self.store.list_source_records("acme", "customer")), len(self.store.list_candidates("acme", "customer", status="pending")), len(self.store.list_entities("acme", "customer", "active")))
        for i, call in enumerate(refused):
            with self.assertRaises(AuthError, msg=f"call #{i} was not refused"):
                call()
        after = (len(self.store.list_source_records("acme", "customer")), len(self.store.list_candidates("acme", "customer", status="pending")), len(self.store.list_entities("acme", "customer", "active")))
        self.assertEqual(before, after)

    def test_the_permission_table_gives_it_two_read_permissions_and_one_write_that_changes_nothing(self):
        granted = sorted(action for action, roles in PERMISSIONS.items() if "steward_agent" in roles)
        self.assertEqual(granted, ["propose_decision", "view", "view_audits"])      # a new permission for this role is a decision, and this test makes it a visible one
        self.assertIn("steward_agent", ROLES)
        self.assertFalse(self.AGENT().has_permission("resolve_review_queue"))

    def test_an_administrator_can_issue_a_key_with_the_role_and_an_unknown_role_is_still_refused(self):
        admin = P("tenant_admin")
        made = api.create_api_key(api.CreateApiKeyRequest(role="steward_agent", label="agent-1", allowed_domains=["customer"]), principal=admin)
        self.assertEqual(made["role"], "steward_agent")
        with self.assertRaises(api.HTTPException) as raised:
            api.create_api_key(api.CreateApiKeyRequest(role="superuser", label="x"), principal=admin)
        self.assertEqual(raised.exception.status_code, 400)

    def test_reading_the_audit_sample_did_not_loosen_who_may_judge_it(self):
        with self.assertRaises(AuthError):
            api.audit_page("customer", principal=P("api_consumer"))     # unchanged: an api_consumer still cannot read it
        self.assertEqual([r for r in PERMISSIONS["view_audits"]], ["tenant_admin", "domain_admin", "data_steward", "steward_agent"])
        self.assertEqual([r for r in PERMISSIONS["resolve_review_queue"]], ["tenant_admin", "domain_admin", "data_steward"])


if __name__ == "__main__":
    unittest.main()
