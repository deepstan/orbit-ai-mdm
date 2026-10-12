"""
The API layer's WIRING, tested by calling its endpoint functions directly with real keys and a real database: authentication fails closed, a decision is signed by the
key and not by the request, a key limited to some domains sees only those, a refusal is a 403, and the limits are applied. (Where FastAPI is not installed,
tests/fake_web.py stands in for it; the real thing, over HTTP, is checked by validate_api.py.)

Run with:  python3 -m unittest tests.test_api_wiring -v
"""
import asyncio
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import fake_web  # noqa: E402
from app import audit, limits  # noqa: E402
from app.auth import AuthError, Principal, generate_api_key  # noqa: E402
from app.db import Store  # noqa: E402
from app.models import ApiKeyRecord, MatchCandidate  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402
from app.unmerge import unmerge_pair  # noqa: E402

api = None   # app.api, imported in setUpModule on whatever FastAPI is installed (the real one, or tests/fake_web.py), and forgotten again afterwards
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


BASE = {"city": "Atlanta"}
A = {**BASE, "first_name": "Robert", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0101"}
B = {**BASE, "first_name": "Bob", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0199"}
C = {**BASE, "first_name": "Robert", "last_name": "Jonas", "email": "bj@other.com", "phone": "404-555-0199"}
ZED = {"first_name": "Zed", "last_name": "Quill", "email": "zed@quill.example", "phone": "404-555-0777", "city": "Macon"}


def P(role="tenant_admin", allowed=None, label="Jane", tenant="acme"):
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
        env = mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "0", "MDM_AUDIT_RATE_PROMOTED": "0", "MDM_MODEL_DIR": os.path.join(self.tmp, "models")})   # nothing is written into the project
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("MDM_DEV_MODE", None)

    def key(self, role="data_steward", allowed=None, label="Jane", tenant="acme", revoked=False):
        plaintext, key_hash = generate_api_key()
        record = ApiKeyRecord(tenant_id=tenant, role=role, key_hash=key_hash, label=label, allowed_domains=allowed)
        if revoked:
            record.revoked_at = "2026-01-01T00:00:00Z"
        self.store.save_api_key(record)
        return plaintext

    def signin(self, authorization=None, tenant="x"):
        return api.get_principal(authorization=authorization, x_tenant_id=tenant)

    def refused(self, status, fn, *a, **kw):
        with self.assertRaises(api.HTTPException) as raised:
            fn(*a, **kw)
        self.assertEqual(raised.exception.status_code, status, raised.exception.detail)
        return raised.exception


class TestAuthenticationFailsClosed(Wired):
    def test_with_nothing_set_a_request_without_a_key_is_refused(self):
        self.refused(401, self.signin)
        self.refused(401, self.signin, None, "acme")                      # naming a tenant in a header opens nothing
        with mock.patch.dict(os.environ, {"MDM_ENABLE_AUTH": "false"}):   # the retired switch does nothing
            self.refused(401, self.signin)

    def test_dev_mode_works_only_when_it_is_asked_for(self):
        with mock.patch.dict(os.environ, {"MDM_DEV_MODE": "1"}):
            p = self.signin(None, "acme")
        self.assertEqual((p.tenant_id, p.role, p.label, p.allowed_domains), ("acme", "tenant_admin", "dev-mode", None))

    def test_a_valid_key_gives_its_tenant_role_and_domains_whatever_the_header_says(self):
        key = self.key("data_steward", ["customer"], "Pat", "acme")
        p = self.signin(f"Bearer {key}", "someone-elses-tenant")
        self.assertEqual((p.tenant_id, p.role, p.label, p.allowed_domains), ("acme", "data_steward", "Pat", ["customer"]))

    def test_wrong_revoked_or_malformed_keys_are_refused(self):
        good = self.key()
        for header in ("Bearer not-a-key", f"Bearer {self.key(revoked=True)}", f"Basic {good}", good, "Bearer", ""):
            self.refused(401, self.signin, header)

    def test_the_rate_limit_refuses_with_a_wait_and_applies_per_key(self):
        a, b = self.key(label="A"), self.key(label="B")
        with mock.patch.object(api, "_limiter", limits.RateLimiter(2)):
            self.signin(f"Bearer {a}")
            self.signin(f"Bearer {a}")
            exc = self.refused(429, self.signin, f"Bearer {a}")
            self.assertIn("Retry-After", exc.headers)
            self.signin(f"Bearer {b}")                                    # another key is not affected


class TestARefusalIsA403(Wired):
    def test_an_authorization_error_is_turned_into_403_with_the_reason(self):
        response = asyncio.run(api._forbidden(None, AuthError("Role 'api_consumer' is not permitted to perform 'resolve_review_queue'")))
        self.assertEqual(response.status_code, 403)
        self.assertIn("not permitted", response.content["detail"])
        self.assertTrue(any(k.__name__ == "AuthError" for k in api.app.exception_handlers))   # and it is registered, so it covers every endpoint

    def test_the_endpoints_raise_it_for_a_key_that_may_not(self):
        with self.assertRaises(AuthError):
            api.audit_page("customer", principal=P("api_consumer"))
        with self.assertRaises(AuthError):
            api.scorer_check("customer", principal=P("data_steward"))
        with self.assertRaises(AuthError):
            api.resolve("customer", "cand_x", api.ResolveCandidateRequest(decision="confirm"), principal=P("api_consumer"))


class TestNothingIsPublicButTheHealthCheck(Wired):
    def test_health_says_ok_and_nothing_else(self):
        self.assertEqual(api.healthz(), {"status": "ok"})

    def test_the_domain_list_and_schema_need_a_key_and_respect_its_domains(self):
        every = {d["domain"] for d in api.list_domains(principal=P("api_consumer"))}
        self.assertTrue({"customer", "product", "vendor"} <= every)
        self.assertEqual([d["domain"] for d in api.list_domains(principal=P("api_consumer", ["product"]))], ["product"])
        self.assertEqual(api.schema("product", principal=P("api_consumer", ["product"]))["domain"], "product")
        with self.assertRaises(AuthError):
            api.schema("customer", principal=P("api_consumer", ["product"]))
        self.refused(404, api.schema, "nope", principal=P())
        with self.assertRaises(AuthError):
            api.list_domains(principal=P("nobody"))                       # an unknown role may do nothing


class TestADecisionIsSignedByTheKey(Wired):
    def pending(self):
        ra = ingest_record(self.store, "acme", "customer", "src", "A", A)["record_id"]
        rb = ingest_record(self.store, "acme", "customer", "src", "Z", ZED)["record_id"]
        cand = MatchCandidate(record_a_id=ra, record_b_id=rb, tenant_id="acme", domain="customer", score=0.7, match_type="fuzzy")
        self.store.save_candidate(cand)
        return cand.id

    def test_what_the_request_claims_is_ignored_and_kept_only_as_a_record_of_the_claim(self):
        cid = self.pending()
        api.resolve("customer", cid, api.ResolveCandidateRequest(decision="reject", decided_by="steward:the_ceo"), principal=P("data_steward", label="Pat"))
        self.assertEqual(self.store.get_candidate(cid, "acme", "customer").decided_by, "steward:Pat")
        entry = next(e for e in self.store.list_audit_log("acme") if e.action == "resolve_review_queue")
        self.assertEqual((entry.actor, entry.details["decided_by"], entry.details["claimed_decided_by"]), ("Pat", "steward:Pat", "steward:the_ceo"))

    def test_deciding_the_same_pair_twice_is_a_400_not_a_500(self):
        cid = self.pending()
        api.resolve("customer", cid, api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward", label="Pat"))
        self.refused(400, api.resolve, "customer", cid, api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward", label="Pat"))

    def test_a_claim_to_be_the_system_does_not_work_and_no_claim_is_needed(self):
        cid = self.pending()
        api.resolve("customer", cid, api.ResolveCandidateRequest(decision="reject", decided_by="system:auto_merge"), principal=P("data_steward", label="Pat"))
        self.assertEqual(self.store.get_candidate(cid, "acme", "customer").decided_by, "steward:Pat")      # still a human label, signed by the key
        cid2 = self.pending()
        api.resolve("customer", cid2, api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward", label="Pat"))
        self.assertEqual(self.store.get_candidate(cid2, "acme", "customer").decided_by, "steward:Pat")

    def test_even_a_key_named_like_the_system_cannot_pass_as_it(self):
        cid = self.pending()
        api.resolve("customer", cid, api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward", label="system:auto_merge"))
        self.assertFalse(self.store.get_candidate(cid, "acme", "customer").decided_by.startswith("system:"))

    def test_a_key_without_the_permission_cannot_decide(self):
        with self.assertRaises(AuthError):
            api.resolve("customer", self.pending(), api.ResolveCandidateRequest(decision="confirm"), principal=P("api_consumer"))
        with self.assertRaises(AuthError):
            api.resolve("product", self.pending(), api.ResolveCandidateRequest(decision="confirm"), principal=P("data_steward", ["customer"]))


class TestTheAuditLogRespectsDomains(Wired):
    def setUp(self):
        super().setUp()
        for domain, action in (("customer", "c1"), ("customer", "c2"), ("product", "p1"), (None, "key_created")):
            audit.log_action(self.store, "acme", action=action, actor="x", domain=domain, details={})

    def actions(self, principal, domain=None):
        return sorted(e["action"] for e in api.get_audit_log(domain=domain, principal=principal)["entries"])

    def test_an_unrestricted_admin_sees_everything(self):
        self.assertEqual(self.actions(P("tenant_admin")), ["c1", "c2", "key_created", "p1"])
        self.assertEqual(self.actions(P("tenant_admin"), "product"), ["p1"])

    def test_a_key_limited_to_a_domain_sees_only_that_domain_and_cannot_ask_for_another(self):
        scoped = P("domain_admin", ["customer"])
        self.assertEqual(self.actions(scoped), ["c1", "c2"])               # not product, and not the tenant-wide entry
        self.assertEqual(self.actions(scoped, "customer"), ["c1", "c2"])
        with self.assertRaises(AuthError):
            api.get_audit_log(domain="product", principal=scoped)
        self.assertTrue(api.get_audit_log(principal=scoped)["chain_valid"])

    def test_a_broken_chain_is_reported_without_pointing_into_another_domain(self):
        import sqlite3
        conn = sqlite3.connect(self.store.path)
        conn.execute("UPDATE audit_log SET details = '{\"tampered\": true}' WHERE action = 'p1'")
        conn.commit()
        conn.close()
        result = api.get_audit_log(principal=P("domain_admin", ["customer"]))
        self.assertFalse(result["chain_valid"])                            # it says the log was tampered with ...
        self.assertNotIn("ent_", str(result["broke_at_entry_id"]))         # ... without naming the entry in a domain it may not see
        self.assertIn("outside", str(result["broke_at_entry_id"]))


class TestKeyLabels(Wired):
    def test_a_label_is_checked_and_stored_trimmed(self):
        made = api.create_api_key(api.CreateApiKeyRequest(role="data_steward", label="  Jane  "), principal=P("tenant_admin"))
        self.assertEqual(made["label"], "  Jane  ".strip())
        self.assertEqual(next(k for k in self.store.list_api_keys("acme") if k.id == made["key_id"]).label, "Jane")

    def test_a_label_that_passes_as_the_system_or_is_empty_is_refused(self):
        for bad in ("system:auto_merge", "SYSTEM:x", "", "   ", "x" * 101):
            self.refused(400, api.create_api_key, api.CreateApiKeyRequest(role="data_steward", label=bad), principal=P("tenant_admin"))
        self.refused(400, api.create_api_key, api.CreateApiKeyRequest(role="emperor", label="Jane"), principal=P("tenant_admin"))
        self.assertEqual(self.store.list_api_keys("acme"), [])

    def test_only_a_tenant_admin_makes_keys(self):
        with self.assertRaises(AuthError):
            api.create_api_key(api.CreateApiKeyRequest(role="data_steward", label="Jane"), principal=P("domain_admin"))


class TestTheLimits(Wired):
    def test_a_huge_or_overwide_record_is_refused_with_the_reason(self):
        api.IngestRequest(source_system="s", source_record_id="r", attributes={"a": 1})
        with self.assertRaises(ValueError) as raised:
            api.IngestRequest(source_system="s", source_record_id="r", attributes={"note": "x" * 70_000})
        self.assertIn("bytes", str(raised.exception))
        with self.assertRaises(ValueError):
            api.IngestRequest(source_system="s", source_record_id="r", attributes={f"f{i}": 1 for i in range(501)})

    def test_a_batch_over_the_limit_is_refused(self):
        record = api.IngestRequest(source_system="s", source_record_id="r", attributes={"a": 1})
        api.BatchIngestRequest(records=[record] * limits.max_batch_records())
        with self.assertRaises(ValueError):
            api.BatchIngestRequest(records=[record] * (limits.max_batch_records() + 1))

    def test_a_body_that_announces_too_much_is_refused_before_it_is_read(self):
        class Request:
            def __init__(self, length):
                self.headers = {"content-length": length} if length is not None else {}

        async def passed(request):
            return "passed"
        with mock.patch.dict(os.environ, {"MDM_MAX_BODY_BYTES": "1000"}):
            too_big = asyncio.run(api._limit_body_size(Request("1001"), passed))
            fine = asyncio.run(api._limit_body_size(Request("1000"), passed))
            unannounced = asyncio.run(api._limit_body_size(Request(None), passed))
        self.assertEqual(too_big.status_code, 413)
        self.assertEqual((fine, unannounced), ("passed", "passed"))


class TestTheAuditAndUnmergeEndpoints(Wired):
    """Written last session without tests of their wiring."""

    def setUp(self):
        super().setUp()
        for name, attrs in (("A", A), ("B", B), ("C", C)):
            ingest_record(self.store, "acme", "customer", "src", name, attrs)
        recs = {r.source_record_id: r for r in self.store.list_source_records("acme", "customer")}
        pairs = self.store.confirmed_pairs_among("acme", "customer", [r.id for r in recs.values()])
        self.ab = next(c for c, a, b in pairs if {a, b} == {recs["A"].id, recs["B"].id})
        self.store.flag_merge_audits("acme", "customer", [self.ab])

    def test_the_page_lists_what_waits_and_the_verdict_is_signed_by_the_key(self):
        page = api.audit_page("customer", principal=P("data_steward", label="Pat"))
        self.assertEqual((page["waiting"], [i["candidate_id"] for i in page["items"]]), (1, [self.ab]))
        self.assertIn("summary", page)
        api.audit_verdict("customer", self.ab, api.AuditVerdictRequest(verdict="correct"), principal=P("data_steward", label="Pat"))
        self.assertEqual(self.store.list_merge_audits("acme", "customer")[0]["audited_by"], "Pat")
        self.refused(400, api.audit_verdict, "customer", self.ab, api.AuditVerdictRequest(verdict="correct", unmerge=True), principal=P("data_steward"))
        self.refused(400, api.audit_verdict, "customer", "cand_nope", api.AuditVerdictRequest(verdict="wrong"), principal=P("data_steward"))

    def test_unmerge_previews_does_it_and_refuses_a_second_time(self):
        preview = api.unmerge_merge("customer", self.ab, api.UnmergeRequest(dry_run=True), principal=P("data_steward", label="Pat"))
        self.assertTrue(preview["dry_run"])
        self.assertEqual(self.store.get_candidate(self.ab, "acme", "customer").status, "confirmed")
        done = api.unmerge_merge("customer", self.ab, api.UnmergeRequest(reason="two people"), principal=P("data_steward", label="Pat"))
        self.assertTrue(done["split"])
        self.assertEqual(self.store.get_candidate(self.ab, "acme", "customer").decided_by, "unmerge:Pat")
        self.refused(409, api.unmerge_merge, "customer", self.ab, api.UnmergeRequest(), principal=P("data_steward"))
        self.assertEqual([e.action for e in self.store.list_audit_log("acme") if e.action == "unmerge"], ["unmerge"])   # only the real one was logged, not the preview

    def test_a_key_limited_to_another_domain_cannot_use_them(self):
        for call in (lambda: api.audit_page("customer", principal=P("data_steward", ["product"])),
                     lambda: api.unmerge_merge("customer", self.ab, api.UnmergeRequest(), principal=P("data_steward", ["product"])),
                     lambda: api.scorer_status("customer", principal=P("api_consumer", ["product"]))):
            with self.assertRaises(AuthError):
                call()

    def test_the_scorer_can_be_viewed_by_anyone_who_may_view_but_checked_and_reset_only_by_an_admin(self):
        self.assertFalse(api.scorer_status("customer", principal=P("api_consumer"))["scorer"]["present"])
        self.assertEqual(api.scorer_check("customer", principal=P("domain_admin"))["verdict"], "no_scorer")
        self.assertEqual(api.scorer_reset("customer", principal=P("tenant_admin"))["state"], "active")
        self.assertEqual([e.action for e in self.store.list_audit_log("acme") if e.action == "scorer_reset"], ["scorer_reset"])



class TestPaginatedLists(Wired):
    """The review queue and the entity list, a page at a time."""

    def setUp(self):
        super().setUp()
        self.ids = [ingest_record(self.store, "acme", "customer", "src", f"c{i}", {"first_name": f"Name{i}", "last_name": f"Surname{i}", "email": f"n{i}@example.com",
                                                                                   "city": f"City{i}"})["record_id"] for i in range(30)]
        rng = __import__("random").Random(3)
        self.cands = []
        for i in range(25):                                                       # few distinct scores, so many pairs tie
            c = MatchCandidate(record_a_id=self.ids[i], record_b_id=self.ids[(i + 7) % 30], tenant_id="acme", domain="customer", score=rng.choice([0.55, 0.65, 0.75]), match_type="fuzzy")
            self.store.save_candidate(c)
        # the engine may have queued pairs of its own while these customers were ingested: what is PENDING is whatever the store says it is
        self.cands = self.store.list_candidates("acme", "customer", status="pending")
        self.assertGreaterEqual(len(self.cands), 25)
        self.total = len(self.cands)
        self.expected = [c.id for c in sorted(self.cands, key=lambda c: (c.score, c.id), reverse=True)]
        self.entities = sorted(e.id for e in self.store.list_entities("acme", "customer"))

    def walk(self, call, **kw):
        pages, cursor = [], None
        while True:
            page = call(cursor=cursor, **kw)
            pages.append(page)
            cursor = page["next_cursor"]
            if cursor is None:
                return pages
            self.assertLess(len(pages), 200, "the cursor never ends")

    def test_the_queue_is_one_page_with_a_total_and_a_cursor_to_the_next(self):
        page = api.review_queue("customer", limit=10, cursor=None, principal=P("data_steward"))
        self.assertEqual((len(page["items"]), page["total"], page["limit"]), (10, self.total, 10))
        self.assertIsNotNone(page["next_cursor"])
        self.assertEqual([i["candidate_id"] for i in page["items"]], self.expected[:10])
        first = page["items"][0]
        for key in ("score", "match_type", "reasons", "contributions", "record_a", "record_b", "created_at"):
            self.assertIn(key, first)
        self.assertEqual(first["record_a"]["attributes"]["first_name"][:4], "Name")

    def test_walking_the_cursors_gives_the_whole_queue_once_in_order_whatever_the_page_size(self):
        for size in (1, 4, 10, self.total, 200):
            pages = self.walk(lambda cursor, size=size: api.review_queue("customer", limit=size, cursor=cursor, principal=P("data_steward")))
            self.assertEqual([i["candidate_id"] for p in pages for i in p["items"]], self.expected, f"page size {size}")
            self.assertIsNone(pages[-1]["next_cursor"])
            self.assertEqual({p["total"] for p in pages}, {self.total})
        exact = api.review_queue("customer", limit=self.total, cursor=None, principal=P("data_steward"))
        self.assertIsNone(exact["next_cursor"])                                      # a page that holds exactly the rest does not promise another

    def test_a_decision_between_two_pages_loses_and_repeats_nothing(self):
        first = api.review_queue("customer", limit=10, cursor=None, principal=P("data_steward"))
        api.resolve("customer", first["items"][2]["candidate_id"], api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward"))
        api.resolve("customer", self.expected[15], api.ResolveCandidateRequest(decision="reject"), principal=P("data_steward"))
        rest = self.walk(lambda cursor: api.review_queue("customer", limit=10, cursor=cursor, principal=P("data_steward")))
        # (the walk restarts at page one: compare the continuation from the first page's cursor instead)
        cont = []
        cursor = first["next_cursor"]
        while cursor:
            page = api.review_queue("customer", limit=10, cursor=cursor, principal=P("data_steward"))
            cont += [i["candidate_id"] for i in page["items"]]
            cursor = page["next_cursor"]
        self.assertEqual(cont, [c for c in self.expected[10:] if c != self.expected[15]])
        self.assertEqual(rest[0]["total"], self.total - 2)

    def test_the_page_size_is_clamped_and_a_bad_cursor_is_a_400(self):
        self.assertEqual(api.review_queue("customer", limit=10 ** 6, cursor=None, principal=P("data_steward"))["limit"], 200)
        self.assertEqual(api.review_queue("customer", limit=0, cursor=None, principal=P("data_steward"))["limit"], 1)
        for bad in ("garbage", "AAAA", "x" * 600, paging_cursor_for_entities()):
            self.refused(400, api.review_queue, "customer", limit=10, cursor=bad, principal=P("data_steward"))
            self.refused(400, api.list_entities, "customer", limit=10, cursor=bad if bad != paging_cursor_for_entities() else "garbage", principal=P("data_steward"))

    def test_a_cursor_from_another_tenant_shows_the_caller_only_the_callers_own_pairs(self):
        other = ingest_record(self.store, "other", "customer", "src", "o1", {"first_name": "Zed", "last_name": "Quill", "city": "Z"})["record_id"]
        other2 = ingest_record(self.store, "other", "customer", "src", "o2", {"first_name": "Zee", "last_name": "Quill", "city": "Y"})["record_id"]
        for i in range(12):
            self.store.save_candidate(MatchCandidate(record_a_id=other, record_b_id=other2, tenant_id="other", domain="customer", score=0.9, match_type="fuzzy"))
        theirs = api.review_queue("customer", limit=3, cursor=None, principal=P("data_steward", tenant="other"))
        self.assertEqual(theirs["total"], 12)
        mine = api.review_queue("customer", limit=50, cursor=theirs["next_cursor"], principal=P("data_steward"))
        self.assertTrue(set(i["candidate_id"] for i in mine["items"]) <= set(self.expected))
        self.assertEqual(mine["total"], self.total)

    def test_the_queue_page_does_not_cost_a_query_per_record(self):
        import sqlite3
        connects, real = [], sqlite3.connect
        with mock.patch.object(sqlite3, "connect", lambda *a, **k: (connects.append(1), real(*a, **k))[1]):
            page = api.review_queue("customer", limit=50, cursor=None, principal=P("data_steward"))
        self.assertEqual(len(page["items"]), min(50, self.total))
        self.assertLessEqual(len(connects), 6, f"{len(connects)} connections for one page of {len(page['items'])} pairs")

    def test_the_entity_list_is_a_page_with_a_cursor_and_no_total(self):
        page = api.list_entities("customer", status="active", limit=8, cursor=None, principal=P("api_consumer"))
        self.assertEqual((len(page["items"]), page["limit"]), (8, 8))
        self.assertNotIn("total", page)
        self.assertEqual(set(page["items"][0]), {"entity_id", "status", "attributes", "source_record_ids", "updated_at"})
        pages = self.walk(lambda cursor: api.list_entities("customer", status="active", limit=8, cursor=cursor, principal=P("api_consumer")))
        ids = [i["entity_id"] for p in pages for i in p["items"]]
        self.assertEqual(ids, self.entities)                                         # every entity once, in id order
        self.assertEqual(api.list_entities("customer", limit=10 ** 6, cursor=None, principal=P("api_consumer"))["limit"], 1000)
        self.assertEqual(api.list_entities("customer", limit=100, cursor=None, principal=P("api_consumer"))["next_cursor"], None)   # fewer than 100: one page

    def test_the_lists_respect_permissions_and_domains(self):
        for call in (lambda: api.review_queue("customer", limit=10, cursor=None, principal=P("api_consumer", ["product"])),
                     lambda: api.list_entities("customer", limit=10, cursor=None, principal=P("api_consumer", ["product"])),
                     lambda: api.list_entities("customer", limit=10, cursor=None, principal=P("nobody"))):
            with self.assertRaises(AuthError):
                call()
        self.refused(404, api.review_queue, "nope", limit=10, cursor=None, principal=P())


def paging_cursor_for_entities():
    from app import paging
    return paging.encode({"i": "ent_1"})


if __name__ == "__main__":
    unittest.main()
