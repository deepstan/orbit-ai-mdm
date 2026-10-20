#!/usr/bin/env python3
"""
Checks the API OVER HTTP, against a real running server: authentication is required, a key may do only what its role and domains allow, a decision is signed by the key
and not by the request, the audit log respects a key's domains, requests are bounded, and refusals are 4xx (not 500). The logic behind each is unit-tested; this checks
that the web layer really applies it, which the build environment could not do (FastAPI cannot be installed there).

    python3 validate_api.py                     starts its OWN isolated servers (SQLite file, secure mode) and tests them. Needs: pip install -r requirements.txt
    python3 validate_api.py --base http://localhost:8000 --admin-key mdm_... --confirm-writes
                                                tests a server you already run. It WRITES: it creates three API keys and a few records in the admin key's tenant,
                                                so point it at a scratch tenant. Checks that need a special server setting are skipped.

Exit code 0 if nothing failed (skips are fine), 1 otherwise.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RESULTS: list = []
BASE_FIELDS = {"city": "Atlanta"}
CHAIN = [("A", {**BASE_FIELDS, "first_name": "Robert", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0101"}),
         ("B", {**BASE_FIELDS, "first_name": "Bob", "last_name": "Jones", "email": "rob.jones@example.com", "phone": "404-555-0199"}),
         ("C", {**BASE_FIELDS, "first_name": "Robert", "last_name": "Jonas", "email": "bj@other.com", "phone": "404-555-0199"})]
REVIEW_PAIRS = [({"first_name": "Maria", "last_name": "Alvarez", "city": "Macon", "email": "maria.a@one.example"}, {"first_name": "Maria", "last_name": "Alvarez", "city": "Savannah", "email": "malvarez@two.example"}),
                ({"first_name": "Dana", "last_name": "Whitfield", "city": "Athens", "phone": "706-555-0111"}, {"first_name": "Dana", "last_name": "Whitfeld", "city": "Athens", "phone": "706-555-0122"})]


def note(status: str, name: str, detail: str = "") -> None:
    RESULTS.append((status, name, detail))
    print(f"  {status:4s} {name}" + (f"   ({detail})" if detail and status != "PASS" else ""), flush=True)


def check(name: str, ok: bool, detail: str = "") -> bool:
    note("PASS" if ok else "FAIL", name, detail)
    return ok


def call(method: str, url: str, body=None, key: str | None = None, headers: dict | None = None):
    """(status, parsed JSON or text, response headers). Never raises on an HTTP error status."""
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    if key:
        hdrs["Authorization"] = f"Bearer {key}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw, status, rheaders = response.read().decode("utf-8", "replace"), response.status, dict(response.headers)
    except urllib.error.HTTPError as e:
        try:
            raw, status, rheaders = e.read().decode("utf-8", "replace"), e.code, dict(e.headers)
        finally:
            e.close()
    except (urllib.error.URLError, http.client.HTTPException, OSError) as e:   # unreachable, refused, or dropped mid-request (what a crashing handler does)
        return 0, f"no connection: {getattr(e, 'reason', e)}", {}
    try:
        return status, json.loads(raw), rheaders
    except json.JSONDecodeError:
        return status, raw, rheaders


def status_of(*args, **kw) -> int:
    return call(*args, **kw)[0]


def make_key(base: str, admin: str, role: str, label: str, domains=None):
    status, body, _ = call("POST", f"{base}/v1/admin/api-keys", {"role": role, "label": label, "allowed_domains": domains}, admin)
    return body["api_key"] if status == 200 and isinstance(body, dict) and "api_key" in body else None


# ---------------------------------------------------------------------------------------------------------------- the checks
def section_no_key(base: str) -> None:
    print("\n1. No key: nothing but the health check")
    status, body, _ = call("GET", f"{base}/healthz")
    check("/healthz needs no key and says only ok", status == 200 and body == {"status": "ok"}, f"{status} {body}")
    for path in ("/v1/domains", "/v1/customer/schema", "/v1/customer/entities", "/v1/customer/review-queue", "/v1/customer/audits", "/v1/customer/scorer", "/v1/admin/audit-log", "/v1/admin/usage"):
        got = (status_of("GET", base + path), status_of("GET", base + path, headers={"X-Tenant-Id": "acme"}), status_of("GET", base + path, key="not-a-real-key"))
        check(f"GET {path}: 401 with no key, with a tenant header, with a wrong key", got == (401, 401, 401), str(got))


def section_roles(base: str, keys: dict) -> None:
    print("\n2. Roles and domains")
    reader, scoped = keys["reader"], keys["scoped"]
    s, body, _ = call("GET", f"{base}/v1/domains", key=reader)
    check("a read-only key lists the domains", s == 200 and isinstance(body, list) and {"customer", "product", "vendor"} <= {d["domain"] for d in body}, f"{s}")
    record = {"source_system": "validate", "source_record_id": "R1", "attributes": {"first_name": "X"}}
    for label, method, path, payload in (("ingest", "POST", "/v1/customer/records", record), ("see the audits", "GET", "/v1/customer/audits", None),
                                         ("run the scorer check", "POST", "/v1/customer/scorer/check", {}), ("resolve a review pair", "POST", "/v1/customer/review-queue/cand_x/resolve", {"decision": "reject"})):
        got = status_of(method, base + path, payload, key=reader)
        check(f"a read-only key may not {label}: 403, not 500", got == 403, str(got))
    s, body, _ = call("GET", f"{base}/v1/domains", key=scoped)
    check("a key limited to customer sees only customer", s == 200 and [d["domain"] for d in body] == ["customer"], f"{s} {body if s != 200 else [d['domain'] for d in body]}")
    for path in ("/v1/product/schema", "/v1/product/entities", "/v1/admin/audit-log?domain=product"):
        got = status_of("GET", base + path, key=scoped)
        check(f"that key may not read {path}: 403", got == 403, str(got))
    s, body, _ = call("GET", f"{base}/v1/admin/audit-log", key=scoped)
    entries = body.get("entries", []) if isinstance(body, dict) else []
    check("its slice of the audit log holds only its own domain (nothing tenant-wide, nothing of another domain)", s == 200 and all(e.get("domain") == "customer" for e in entries), f"{s} {[e.get('domain') for e in entries][:6]}")


def section_labels(base: str, admin: str) -> None:
    print("\n3. A key's label is a name")
    for label in ("system:auto_merge", "SYSTEM:x", "", "   "):
        got = status_of("POST", f"{base}/v1/admin/api-keys", {"role": "data_steward", "label": label}, admin)
        check(f"a key labelled {label!r} is refused: 400", got == 400, str(got))


def section_signing(base: str, admin: str, steward: str) -> None:
    print("\n4. A decision is signed by the key, not by the request")
    pending = []
    for i, (a, b) in enumerate(REVIEW_PAIRS):
        for j, attrs in enumerate((a, b)):
            call("POST", f"{base}/v1/customer/records", {"source_system": "validate", "source_record_id": f"pair{i}{j}", "attributes": attrs}, steward)
    s, queue, _ = call("GET", f"{base}/v1/customer/review-queue", key=steward)
    pending = queue.get("items", []) if isinstance(queue, dict) else []
    if not pending:
        note("SKIP", "signing a review decision", "no pair reached the review queue on this server")
        return
    candidate = pending[0].get("candidate_id") or pending[0].get("id")
    s, body, _ = call("POST", f"{base}/v1/customer/review-queue/{candidate}/resolve", {"decision": "reject", "decided_by": "steward:the_ceo"}, steward)
    check("resolving with a claimed name works", s == 200, f"{s} {body}")
    s, log, _ = call("GET", f"{base}/v1/admin/audit-log", key=admin)
    entry = next((e for e in reversed(log.get("entries", [])) if e.get("action") == "resolve_review_queue"), None) if isinstance(log, dict) else None
    details = (entry or {}).get("details", {})
    check("the audit log records the KEY's name, and keeps the claim only as a claim", details.get("decided_by") == "steward:Pat" and details.get("claimed_decided_by") == "steward:the_ceo", str(details))
    s, body, _ = call("POST", f"{base}/v1/customer/review-queue/{candidate}/resolve", {"decision": "reject"}, steward)
    check("a second decision on the same pair is refused with 400 (not 500)", s == 400, f"{s}")


def section_audits_and_unmerge(base: str, admin: str, steward: str) -> None:
    print("\n5. Audits and unmerge")
    for sid, attrs in CHAIN:
        call("POST", f"{base}/v1/customer/records", {"source_system": "validate", "source_record_id": f"chain{sid}", "attributes": attrs}, steward)
    s, page, _ = call("GET", f"{base}/v1/customer/audits", key=steward)
    items = page.get("items", []) if isinstance(page, dict) else []
    if not items:
        note("SKIP", "audit verdict and unmerge", "no merge was flagged for audit: start the server with MDM_AUDIT_RATE=1 to test this")
        return
    check("the audit page lists merges waiting, with a summary", s == 200 and "summary" in page and page["waiting"] >= 1, f"{s}")
    first = items[0]["candidate_id"]
    check("a verdict is recorded", status_of("POST", f"{base}/v1/customer/audits/{first}/verdict", {"verdict": "correct"}, steward) == 200)
    s, log, _ = call("GET", f"{base}/v1/admin/audit-log", key=admin)
    check("... under the key's name", any(e.get("action") == "audit_verdict" and e.get("actor") == "Pat" for e in (log.get("entries", []) if isinstance(log, dict) else [])))
    check("a verdict on something never flagged is refused: 400", status_of("POST", f"{base}/v1/customer/audits/cand_nope/verdict", {"verdict": "wrong"}, steward) == 400)
    target = next((i["candidate_id"] for i in items[1:] if i.get("unmerge", {}).get("possible")), None)
    if not target:
        note("SKIP", "unmerge", "no waiting merge could be unmerged on its own")
        return
    count = lambda: len(call("GET", f"{base}/v1/customer/entities?limit=1000", key=steward)[1].get("items", []))
    before = count()
    s, preview, _ = call("POST", f"{base}/v1/customer/merges/{target}/unmerge", {"dry_run": True}, steward)
    check("an unmerge preview says what would move and changes nothing", s == 200 and preview.get("dry_run") is True, f"{s}")
    again = count()
    check("... the entity count is unchanged by the preview", again == before, f"{before} -> {again}")
    s, done, _ = call("POST", f"{base}/v1/customer/merges/{target}/unmerge", {"reason": "validate_api"}, steward)
    check("unmerge splits the entity", s == 200 and done.get("split") is True, f"{s} {done}")
    after = count()
    check("... there is one more entity", after == before + 1, f"{before} -> {after}")
    check("unmerging the same pair twice is refused: 409", status_of("POST", f"{base}/v1/customer/merges/{target}/unmerge", {}, steward) == 409)


def walk(base: str, path: str, key: str, limit: int, max_pages: int = 500):
    """Follow next_cursor from the first page to the last: (every item, the number of pages, the first page's body), or (None, 0, body) if something is off."""
    items, cursor, pages, first = [], None, 0, None
    while pages < max_pages:
        url = f"{base}{path}{'&' if '?' in path else '?'}limit={limit}" + (f"&cursor={cursor}" if cursor else "")
        status, body, _ = call("GET", url, key=key)
        if status != 200 or not isinstance(body, dict) or "items" not in body:
            return None, pages, body
        first = first or body
        items += body["items"]
        pages += 1
        cursor = body.get("next_cursor")
        if not cursor:
            return items, pages, first
    return None, pages, first


def section_pages(base: str, steward: str) -> None:
    print("\n6. Pages of the review queue and of the entities")
    for path, ident in (("/v1/customer/entities", "entity_id"), ("/v1/customer/review-queue", "candidate_id")):
        everything, pages_all, first = walk(base, path, steward, 1000)
        if everything is None:
            check(f"GET {path} returns {{items, next_cursor, limit}}", False, str(first)[:120])
            continue
        check(f"GET {path} returns an envelope, and the last page has no next_cursor", {"items", "next_cursor", "limit"} <= set(first) and pages_all >= 1)
        if len(everything) < 2:
            note("SKIP", f"walking {path} a page at a time", "fewer than two items on this server")
            continue
        one_by_one, pages_one, _ = walk(base, path, steward, 1)
        ids = [i[ident] for i in (one_by_one or [])]
        check(f"{path}: one item per page walks the same items, each once, in the same order",
              one_by_one is not None and ids == [i[ident] for i in everything] and len(set(ids)) == len(ids), f"{len(ids)} vs {len(everything)}")
        check(f"{path}: one item per page took one page per item", pages_one == len(everything), f"{pages_one} pages for {len(everything)} items")
    s, body, _ = call("GET", f"{base}/v1/customer/review-queue?limit=1", key=steward)
    check("the queue says how many are waiting in all (total)", s == 200 and isinstance(body.get("total"), int) and body["total"] >= len(body["items"]), str(body)[:100])
    for path, cap in (("/v1/customer/review-queue", 200), ("/v1/customer/entities", 1000)):
        s, body, _ = call("GET", f"{base}{path}?limit=1000000", key=steward)
        check(f"{path}: a huge limit is clamped to {cap}", s == 200 and body.get("limit") == cap, f"{s} {body.get('limit') if isinstance(body, dict) else body}")
        for bad in ("garbage", "AAAA"):
            got = status_of("GET", f"{base}{path}?cursor={bad}", key=steward)
            check(f"{path}: a bad cursor ({bad!r}) is a 400, not a 500", got == 400, str(got))
    s, queue, _ = call("GET", f"{base}/v1/customer/review-queue?limit=1", key=steward)
    if queue.get("next_cursor"):
        got = status_of("GET", f"{base}/v1/customer/entities?cursor={queue['next_cursor']}", key=steward)
        check("a cursor for the queue is refused by the entity list: 400", got == 400, str(got))


def section_steward_agent(base: str, admin: str, steward: str) -> None:
    """An AI agent's key: it reads what a steward reads (the review queue, the group-aware evidence, a record's history, the audit sample), may RECORD a recommendation about a review
    item (which changes nothing about the item), and can change nothing else."""
    print("\n7b. A steward agent: reads like a steward, changes nothing; the evidence endpoints")
    ann = {"first_name": "Agent", "last_name": "Checker", "email": "agent.checker@example.com", "phone": "404-555-0142", "address_line1": "1 Probe Way", "city": "Atlanta", "state": "GA", "postal_code": "30301"}
    maybe = {**ann, "email": "other.checker@example.com", "phone": "", "last_name": "Checkerr"}
    for sid, attrs in (("agent-a1", ann), ("agent-a2", ann), ("agent-b", maybe)):
        status_of("POST", f"{base}/v1/customer/records", {"source_system": "validate", "source_record_id": sid, "attributes": attrs}, steward)
    agent = make_key(base, admin, "steward_agent", "Agent-1")
    check("an administrator can issue a key with the role steward_agent", bool(agent))
    if not agent:
        return
    s, queue, _ = call("GET", f"{base}/v1/customer/review-queue?limit=200", key=agent)
    check("the agent reads the review queue", s == 200 and isinstance(queue.get("items"), list), f"{s}")
    mine = next((i for i in (queue.get("items") or []) if (i.get("record_a") or {}).get("attributes", {}).get("last_name") == "Checkerr"
                 or (i.get("record_b") or {}).get("attributes", {}).get("last_name") == "Checkerr"), None)
    check("... and finds the review item this check created", mine is not None, "no item for the probe record: the matcher did not queue it")
    if mine:
        s, ev, _ = call("GET", f"{base}/v1/customer/review-queue/{mine['candidate_id']}/evidence", key=agent)
        check("the evidence endpoint answers the agent", s == 200 and {"candidate", "record_a", "record_b", "group_a", "group_b", "separations", "history"} <= set(ev), f"{s} {list(ev)[:8] if isinstance(ev, dict) else ev}")
        sizes = [(ev.get(k) or {}).get("member_count") for k in ("group_a", "group_b")] if isinstance(ev, dict) else []
        check("... it is GROUP-aware: one of the groups holds the two probe records", s == 200 and 2 in sizes, f"group sizes {sizes}")
        rec_id = (ev.get("record_a") or {}).get("id")
        s, hist, _ = call("GET", f"{base}/v1/customer/records/{rec_id}/history", key=agent)
        check("a record's history answers the agent", s == 200 and "record" in hist and "history" in hist, f"{s}")
        check("the evidence of a candidate that does not exist is 404", status_of("GET", f"{base}/v1/customer/review-queue/cand_nope/evidence", key=agent) == 404)
        check("the history of a record that does not exist is 404", status_of("GET", f"{base}/v1/customer/records/rec_nope/history", key=agent) == 404)
        check("the evidence needs a key: 401", status_of("GET", f"{base}/v1/customer/review-queue/{mine['candidate_id']}/evidence") == 401)
    check("the agent reads the audit sample", status_of("GET", f"{base}/v1/customer/audits", key=agent) == 200)
    reader = make_key(base, admin, "api_consumer", "Reader-2")
    check("a plain read-only key still may NOT read the audit sample: 403", status_of("GET", f"{base}/v1/customer/audits", key=reader) == 403)
    waiting = call("GET", f"{base}/v1/customer/review-queue?limit=1", key=agent)[1].get("total")
    if mine:
        url = f"{base}/v1/customer/review-queue/{mine['candidate_id']}/recommendations"
        proposal = {"recommendation": "confirm", "confidence": 0.9, "reasons": ["validate_api probe"], "evidence": ["group_b.members"], "model": "validate", "prompt_version": "v1"}
        s, rec, _ = call("POST", url, proposal, agent)
        check("the agent records a recommendation, under its key's name", s == 200 and rec.get("agent") == "agent:Agent-1" and rec.get("outcome") is None, f"{s} {rec}")
        check("... which changes nothing: the same number of items are waiting and this one is still among them",
              call("GET", f"{base}/v1/customer/review-queue?limit=200", key=agent)[1].get("total") == waiting, f"{waiting}")
        s, listed, _ = call("GET", url, key=steward)
        check("... and a steward reads it next to the item", s == 200 and [r.get("recommendation") for r in listed.get("items", [])] == ["confirm"], f"{s}")
        check("a steward may NOT record one: 403", status_of("POST", url, proposal, steward) == 403)
        check("a plain read-only key may NOT record one: 403", status_of("POST", url, proposal, reader) == 403)
        check("a confidence of 7 is refused (400 or 422), nothing stored", status_of("POST", url, {**proposal, "confidence": 7, "model": "other"}, agent) in (400, 422))
        check("a recommendation about an item that does not exist is 404", status_of("POST", f"{base}/v1/customer/review-queue/cand_nope/recommendations", proposal, agent) == 404)
        s, stats, _ = call("GET", f"{base}/v1/customer/recommendations/stats", key=agent)
        check("the agreement statistics are readable", s == 200 and "agreement_rate" in stats and stats.get("total", 0) >= 1, f"{s}")
        s, page, _ = call("GET", f"{base}/v1/customer/recommendations?outcome=open&limit=5", key=agent)
        check("the list of recommendations is a page: items, next_cursor, limit", s == 200 and {"items", "next_cursor", "limit"} <= set(page), f"{s}")
    refused = {
        "resolve a review item": ("POST", "/v1/customer/review-queue/cand_x/resolve", {"decision": "confirm"}),
        "give an audit verdict": ("POST", "/v1/customer/audits/cand_x/verdict", {"verdict": "wrong"}),
        "unmerge": ("POST", "/v1/customer/merges/cand_x/unmerge", {}),
        "ingest a record": ("POST", "/v1/customer/records", {"source_system": "validate", "source_record_id": "agent-z", "attributes": ann}),
        "ingest a batch": ("POST", "/v1/customer/records/batch", {"records": [{"source_system": "validate", "source_record_id": "agent-y", "attributes": ann}]}),
        "delete an entity": ("DELETE", "/v1/customer/entities/ent_x", None),
        "re-check the scorer": ("POST", "/v1/customer/scorer/check", {}),
        "reset the scorer": ("POST", "/v1/customer/scorer/reset", {}),
        "create an API key": ("POST", "/v1/admin/api-keys", {"role": "tenant_admin", "label": "sneaky"}),
        "list API keys": ("GET", "/v1/admin/api-keys", None),
        "read the audit log": ("GET", "/v1/admin/audit-log", None),
        "read usage": ("GET", "/v1/admin/usage", None),
    }
    for label, (method, path, body) in refused.items():
        check(f"the agent may not {label}: 403, not 500", status_of(method, f"{base}{path}", body, agent) == 403)
    still = call("GET", f"{base}/v1/customer/review-queue?limit=1", key=agent)[1].get("total")
    check("after all those attempts the same number of review items are waiting", waiting == still, f"{waiting} -> {still}")


def section_limits(base: str, steward: str, body_limit: bool) -> None:
    print("\n7. Limits")
    batch = {"records": [{"source_system": "validate", "source_record_id": f"b{i}", "attributes": {"a": 1}} for i in range(10_001)]}
    check("a batch of 10,001 records is refused: 422", status_of("POST", f"{base}/v1/customer/records/batch", batch, steward) == 422)
    wide = {"source_system": "validate", "source_record_id": "wide", "attributes": {f"f{i}": 1 for i in range(501)}}
    check("a record with 501 attributes is refused: 422", status_of("POST", f"{base}/v1/customer/records", wide, steward) == 422)
    big = {"source_system": "validate", "source_record_id": "big", "attributes": {"note": "x" * 70_000}}
    check("a record of 70,000 characters is refused: 422", status_of("POST", f"{base}/v1/customer/records", big, steward) == 422)
    if body_limit:
        huge = {"source_system": "validate", "source_record_id": "huge", "attributes": {"note": "x" * 300_000}}
        check("a request body over MDM_MAX_BODY_BYTES is refused: 413", status_of("POST", f"{base}/v1/customer/records", huge, steward) == 413)
    else:
        note("SKIP", "request body limit", "start the server with MDM_MAX_BODY_BYTES=200000 to test this")


# ---------------------------------------------------------------------------------------------------------------- running them
def bootstrap_admin(db_path: str) -> str:
    from app.auth import generate_api_key
    from app.db import Store
    from app.models import ApiKeyRecord
    plain, key_hash = generate_api_key()
    Store(db_path).save_api_key(ApiKeyRecord(tenant_id="validate_api", role="tenant_admin", key_hash=key_hash, label="validate admin"))
    return plain


def full_run(base: str, admin: str, body_limit: bool) -> None:
    keys = {"reader": make_key(base, admin, "api_consumer", "Reader"), "scoped": make_key(base, admin, "domain_admin", "Scoped admin", ["customer"]),
            "steward": make_key(base, admin, "data_steward", "Pat")}
    if not all(keys.values()):
        check("the admin key can create the keys these checks use", False, "POST /v1/admin/api-keys did not return a key: nothing further can be checked")
        return
    section_no_key(base)
    section_roles(base, keys)
    section_labels(base, admin)
    section_signing(base, admin, keys["steward"])
    section_audits_and_unmerge(base, admin, keys["steward"])
    section_pages(base, keys["steward"])
    section_steward_agent(base, admin, keys["steward"])
    section_limits(base, keys["steward"], body_limit)


def spawn_all() -> None:
    from validate_setup import running_server
    db = os.path.abspath("validate_api.db")
    for path in (db,):
        if os.path.exists(path):
            os.remove(path)
    print("Starting an isolated server in SECURE mode (MDM_AUDIT_RATE=1, MDM_MAX_BODY_BYTES=200000) ...")
    admin = bootstrap_admin(db)
    with running_server(env_overrides={"MDM_AUDIT_RATE": "1", "MDM_MAX_BODY_BYTES": "200000"}, db_path=db) as base:
        full_run(base, admin, body_limit=True)

    print("\n8. Server settings")
    admin = bootstrap_admin(db)
    with running_server(env_overrides={"MDM_RATE_LIMIT_PER_MINUTE": "5"}, db_path=db) as base:
        codes = [call("GET", f"{base}/v1/domains", key=admin) for _ in range(7)]
        check("MDM_RATE_LIMIT_PER_MINUTE=5: five requests pass, the sixth is 429 with Retry-After", [c[0] for c in codes[:5]] == [200] * 5 and codes[5][0] == 429 and "Retry-After" in codes[5][2], str([c[0] for c in codes]))
    with running_server(env_overrides={"MDM_DEV_MODE": "1"}, db_path=db) as base:
        check("MDM_DEV_MODE=1 (and only that) opens the API: no key needed", status_of("GET", f"{base}/v1/domains") == 200)
    admin = bootstrap_admin(db)
    with running_server(env_overrides={"MDM_ENABLE_AUTH": "false"}, db_path=db) as base:
        check("the retired MDM_ENABLE_AUTH=false does NOT open the API", status_of("GET", f"{base}/v1/domains") == 401)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", help="test a server that is already running (default: start isolated ones)")
    parser.add_argument("--admin-key", help="a tenant_admin key of a SCRATCH tenant (with --base)")
    parser.add_argument("--confirm-writes", action="store_true", help="with --base: yes, it may create keys and records")
    args = parser.parse_args(argv)
    RESULTS.clear()
    if args.base:
        if not args.admin_key or not args.confirm_writes:
            print("--base needs --admin-key and --confirm-writes (this writes keys and records into that key's tenant).", file=sys.stderr)
            return 2
        full_run(args.base.rstrip("/"), args.admin_key, body_limit=False)
    else:
        spawn_all()
    failed = [r for r in RESULTS if r[0] == "FAIL"]
    print(f"\n{sum(r[0] == 'PASS' for r in RESULTS)} passed, {len(failed)} FAILED, {sum(r[0] == 'SKIP' for r in RESULTS)} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
