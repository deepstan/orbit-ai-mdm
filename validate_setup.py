"""
Runs through every path flagged "written but not tested in the sandbox"
in the README, on YOUR machine, and prints a clear PASS/FAIL/SKIP report.

    python3 validate_setup.py              # runs whatever's available
    python3 validate_setup.py --all        # fails (not skips) anything unavailable

Sections, each independent -- one failing doesn't stop the others:
  1. Environment          (Python version, dependencies installed)
  2. Unit tests            (the 48 tests already proven in the sandbox --
                             re-confirms your machine agrees)
  3. FastAPI HTTP layer    (starts a real uvicorn server, hits real endpoints)
  4. RBAC over HTTP        (401 with no key, 200 with a valid key, 403 for
                             a role that shouldn't be allowed)
  5. Postgres backend      (only if DATABASE_URL/TEST_DATABASE_URL is set)
  6. Real LLM adjudication (only if ANTHROPIC_API_KEY is set)
  7. Real semantic embeddings (only if sentence-transformers is installed)

Sections 5-7 SKIP (not fail) by default, since they need optional setup
(Postgres running, an Anthropic API key, an extra pip install) --
exactly like the pytest-style "skip vs fail" distinction the codebase
already uses for tests/test_postgres_backend.py. Pass --all to treat
"not set up" as a failure instead, e.g. in CI once you've provisioned
everything.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager

RESULTS: list[tuple[str, str, str]] = []  # (section, status, detail)
PORT = 8931  # unlikely to collide with anything else running locally


def report(section: str, status: str, detail: str = "") -> None:
    RESULTS.append((section, status, detail))
    icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️ "}[status]
    print(f"  {icon} {section}{': ' + detail if detail else ''}")


def http_get(url: str, headers: dict | None = None) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body


def http_post(url: str, payload: dict, headers: dict | None = None) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body


@contextmanager
def running_server(env_overrides: dict | None = None, db_path: str = "validate_test.db"):
    """Starts `uvicorn app.api:app` as a subprocess with the given env
    overrides, waits until it responds, yields, then kills it. Isolated
    to its own SQLite file so it never touches your real mdm.db."""
    env = os.environ.copy()
    env["DATABASE_URL"] = db_path  # plain path -> SQLite backend, isolated from real data
    env.update(env_overrides or {})

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.api:app", "--port", str(PORT)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        base = f"http://127.0.0.1:{PORT}"
        for _ in range(40):  # ~10s max wait
            try:
                with socket.create_connection(("127.0.0.1", PORT), timeout=0.5):
                    status, _ = http_get(f"{base}/v1/domains")
                    if status == 200:
                        break
            except (ConnectionRefusedError, OSError):
                pass
            time.sleep(0.25)
        else:
            output = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
            raise RuntimeError(f"Server never came up. Output:\n{output[-2000:]}")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        if os.path.exists(db_path):
            os.remove(db_path)


# ---------------------------------------------------------------------------
def section_environment():
    print("\n1. Environment")
    if sys.version_info >= (3, 10):
        report("Python version", "PASS", f"{sys.version.split()[0]}")
    else:
        report("Python version", "FAIL", f"{sys.version.split()[0]} -- need 3.10+")

    for pkg in ("fastapi", "uvicorn", "pydantic"):
        try:
            __import__(pkg)
            report(f"{pkg} installed", "PASS")
        except ImportError:
            report(f"{pkg} installed", "FAIL", "pip install -r requirements.txt")


def section_unit_tests():
    print("\n2. Unit tests (core logic, already proven in the build sandbox)")
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        # unittest prints summary to stderr
        summary_line = [l for l in result.stderr.splitlines() if l.startswith("Ran ")]
        report("Full test suite", "PASS", summary_line[0] if summary_line else "")
    else:
        report("Full test suite", "FAIL", "see output below")
        print(result.stdout[-2000:])
        print(result.stderr[-2000:])


def section_fastapi_http(require: bool):
    print("\n3. FastAPI HTTP layer (never run in the build sandbox -- this is the real test)")
    try:
        import fastapi, uvicorn  # noqa
    except ImportError:
        report("FastAPI HTTP layer", "FAIL" if require else "SKIP", "fastapi/uvicorn not installed")
        return

    try:
        with running_server() as base:
            status, body = http_get(f"{base}/v1/domains")
            assert status == 200 and len(body) == 3, f"expected 3 domains, got {body}"
            report("GET /v1/domains", "PASS", f"{len(body)} domains registered")

            status, body = http_post(f"{base}/v1/customer/records", {
                "source_system": "validate_script", "source_record_id": "V1",
                "attributes": {"first_name": "Test", "last_name": "User", "email": "test@validate.local"},
            })
            assert status == 200 and body.get("matched_entity_id"), f"ingest failed: {body}"
            report("POST /v1/customer/records", "PASS", f"entity_id={body['matched_entity_id'][:16]}...")

            status, body = http_get(f"{base}/v1/customer/entities")
            assert status == 200 and len(body) == 1, f"expected 1 entity, got {body}"
            report("GET /v1/customer/entities", "PASS")

            status, body = http_get(f"{base}/v1/product/schema")
            assert status == 200 and "sku" in body["fields"], f"unexpected product schema: {body}"
            report("GET /v1/product/schema", "PASS", "Product domain reachable over HTTP")

            status, body = http_get(f"{base}/v1/nonexistent_domain/schema")
            assert status == 404, f"expected 404 for unknown domain, got {status}"
            report("Unknown domain returns 404", "PASS")
    except Exception as e:
        report("FastAPI HTTP layer", "FAIL", str(e))


def section_rbac_http(require: bool):
    print("\n4. RBAC over HTTP (auth enforcement, never run in the build sandbox)")
    try:
        import fastapi, uvicorn  # noqa
    except ImportError:
        report("RBAC over HTTP", "FAIL" if require else "SKIP", "fastapi/uvicorn not installed")
        return

    db_path = "validate_rbac_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        # Bootstrap keys directly against the isolated test DB, bypassing HTTP
        from app.auth import generate_api_key
        from app.models import ApiKeyRecord
        from app.store_factory import get_store

        store = get_store(db_path)
        admin_plain, admin_hash = generate_api_key()
        store.save_api_key(ApiKeyRecord(tenant_id="validate_tenant", role="tenant_admin",
                                         key_hash=admin_hash, label="validate admin"))
        reader_plain, reader_hash = generate_api_key()
        store.save_api_key(ApiKeyRecord(tenant_id="validate_tenant", role="api_consumer",
                                         key_hash=reader_hash, label="validate reader"))

        with running_server(env_overrides={"MDM_ENABLE_AUTH": "true"}, db_path=db_path) as base:
            status, _ = http_get(f"{base}/v1/customer/entities")
            assert status == 401, f"expected 401 with no key, got {status}"
            report("No API key -> 401", "PASS")

            status, _ = http_get(f"{base}/v1/customer/entities",
                                  headers={"Authorization": f"Bearer {admin_plain}"})
            assert status == 200, f"expected 200 with valid admin key, got {status}"
            report("Valid tenant_admin key -> 200", "PASS")

            status, body = http_post(f"{base}/v1/customer/records", {
                "source_system": "validate", "source_record_id": "R1", "attributes": {"first_name": "X"},
            }, headers={"Authorization": f"Bearer {reader_plain}"})
            assert status == 403, f"expected 403 for api_consumer trying to ingest, got {status}: {body}"
            report("api_consumer denied write (403)", "PASS")

            status, body = http_get(f"{base}/v1/admin/audit-log",
                                     headers={"Authorization": f"Bearer {admin_plain}"})
            assert status == 200 and body.get("chain_valid") is True, f"audit log check failed: {body}"
            report("Audit log reachable + chain valid over HTTP", "PASS")
    except Exception as e:
        report("RBAC over HTTP", "FAIL", str(e))
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def section_postgres(require: bool):
    print("\n5. Postgres backend (optional -- needs a real Postgres instance)")
    test_db_url = os.environ.get("TEST_DATABASE_URL")
    if not test_db_url:
        report("Postgres backend", "FAIL" if require else "SKIP",
               "set TEST_DATABASE_URL to run this -- see README's Postgres section")
        return
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "tests.test_postgres_backend"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        report("Postgres backend tests", "PASS")
    else:
        report("Postgres backend tests", "FAIL", "see output below")
        print(result.stderr[-2000:])


def section_llm_adjudication(require: bool):
    print("\n6. Real LLM adjudication call (optional -- needs ANTHROPIC_API_KEY)")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        report("LLM adjudication", "FAIL" if require else "SKIP", "set ANTHROPIC_API_KEY to run this")
        return
    try:
        from app.llm_adjudication import adjudicate
        result = adjudicate(
            record_a={"first_name": "Jennifer", "last_name": "Nguyen", "address_line1": "45 Birch Ln"},
            record_b={"first_name": "Jen", "last_name": "Nguyen", "address_line1": "450 Birchwood Lane"},
            fuzzy_reasons=["last_name exact match", "address moderately similar"],
            domain="customer",
        )
        if result is None:
            report("LLM adjudication", "FAIL", "adjudicate() returned None -- check API key validity/network")
        else:
            report("LLM adjudication", "PASS",
                   f"is_match={result.is_match}, confidence={result.confidence:.2f}")
    except Exception as e:
        report("LLM adjudication", "FAIL", str(e))


def section_real_embeddings(require: bool):
    print("\n7. Real semantic embeddings (optional -- needs sentence-transformers)")
    try:
        import sentence_transformers  # noqa
    except ImportError:
        report("sentence-transformers embeddings", "FAIL" if require else "SKIP",
               "pip install sentence-transformers to run this")
        return
    try:
        os.environ["EMBEDDING_PROVIDER"] = "sentence_transformers"
        from app.embeddings import get_provider
        sim = get_provider().similarity("Nike Air Max 90", "Air Max 90 by Nike")
        report("sentence-transformers embeddings", "PASS", f"similarity={sim:.3f} (downloads model on first run)")
    except Exception as e:
        report("sentence-transformers embeddings", "FAIL", str(e))
    finally:
        os.environ.pop("EMBEDDING_PROVIDER", None)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--all", action="store_true",
                         help="Treat unavailable optional sections (Postgres/LLM/embeddings) as FAIL, not SKIP")
    args = parser.parse_args()

    if not shutil.which(sys.executable):
        print("Could not locate the Python interpreter -- unexpected, aborting.")
        sys.exit(1)

    print("=" * 70)
    print("MDM validation pass -- confirming what the build sandbox couldn't")
    print("=" * 70)

    section_environment()
    section_unit_tests()
    section_fastapi_http(require=args.all)
    section_rbac_http(require=args.all)
    section_postgres(require=args.all)
    section_llm_adjudication(require=args.all)
    section_real_embeddings(require=args.all)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    counts = {"PASS": 0, "FAIL": 0, "SKIP": 0}
    for section, status, detail in RESULTS:
        counts[status] += 1
    for status in ("PASS", "FAIL", "SKIP"):
        print(f"  {status}: {counts[status]}")

    if counts["FAIL"] > 0:
        print("\nSome checks failed -- see details above. Paste the output back if you want help debugging.")
        sys.exit(1)
    else:
        print("\nEverything that ran, passed. SKIPped sections just need optional setup (see their notes above).")


if __name__ == "__main__":
    main()
