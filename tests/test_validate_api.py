"""
validate_api.py is only worth running if its checks CAN FAIL. Here it is pointed at stand-in servers: a wide-open one (it must report failures, and not crash), one that
cannot even make keys (it must say so and stop), and a locked one for the no-key checks (they must be able to pass). The real thing, against the real server,
is for whoever runs `python3 validate_api.py`.

Run with:  python3 -m unittest tests.test_validate_api -v
"""
import io
import json
import os
import sys
import threading
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import validate_api  # noqa: E402


def serve(behaviour):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def handle_any(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            status, payload = behaviour(self.command, self.path, self.headers.get("Authorization"), body)
            raw = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        do_GET = do_POST = do_DELETE = handle_any

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


class TestTheChecksCanFail(unittest.TestCase):
    def run_main(self, base):
        out = io.StringIO()
        with redirect_stdout(out):
            code = validate_api.main(["--base", base, "--admin-key", "adminkey", "--confirm-writes"])
        return code, out.getvalue()

    def test_a_wide_open_server_fails_loudly_and_does_not_crash_the_script(self):
        counter = [0]

        def wide_open(method, path, auth, body):
            if method == "POST" and path == "/v1/admin/api-keys":
                counter[0] += 1
                return 200, {"api_key": f"key{counter[0]}"}
            return 200, {}                                    # everything allowed, for anyone, with no key
        server, base = serve(wide_open)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        code, output = self.run_main(base)
        self.assertEqual(code, 1)
        self.assertGreater(output.count("FAIL"), 15, output)
        self.assertIn("/v1/admin/audit-log: 401 with no key", output)
        self.assertNotIn("Traceback", output)

    def test_a_server_that_cannot_make_keys_is_reported_and_the_run_stops(self):
        server, base = serve(lambda m, p, a, b: (404, {"detail": "no such route"}))
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        code, output = self.run_main(base)
        self.assertEqual(code, 1)
        self.assertIn("the admin key can create the keys these checks use", output)
        self.assertEqual(len([line for line in output.splitlines() if line.lstrip().startswith("FAIL")]), 1)   # one clear failure, not a hundred follow-on ones

    def test_a_locked_server_passes_the_no_key_checks(self):
        issued = {"adminkey"}

        def locked(method, path, auth, body):
            if path == "/healthz":
                return 200, {"status": "ok"}
            if auth is None or auth.replace("Bearer ", "") not in issued:
                return 401, {"detail": "Missing or invalid key"}
            return 403, {"detail": "not permitted"}
        server, base = serve(locked)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        validate_api.RESULTS.clear()
        out = io.StringIO()
        with redirect_stdout(out):
            validate_api.section_no_key(base)
        self.assertTrue(validate_api.RESULTS)
        self.assertEqual([r for r in validate_api.RESULTS if r[0] != "PASS"], [], out.getvalue())

    def test_the_paging_checks_can_pass_against_a_server_that_pages_correctly(self):
        from urllib.parse import parse_qs, urlparse
        data = {"/v1/customer/entities": ("e", [{"entity_id": f"e{i:03d}"} for i in range(7)], 1000), "/v1/customer/review-queue": ("q", [{"candidate_id": f"c{i:03d}"} for i in range(5)], 200)}

        def pager(method, path, auth, body):
            parsed = urlparse(path)
            if parsed.path not in data:
                return 404, {}
            kind, items, cap = data[parsed.path]
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            limit = max(1, min(int(query.get("limit", 50)), cap))
            start = 0
            if "cursor" in query:
                if not query["cursor"].startswith(kind + ":") or not query["cursor"][2:].isdigit():
                    return 400, {"detail": "invalid cursor"}
                start = int(query["cursor"][2:])
            chunk = items[start:start + limit]
            more = start + limit < len(items)
            return 200, {"items": chunk, "next_cursor": f"{kind}:{start + limit}" if more else None, "limit": limit, **({"total": len(items)} if kind == "q" else {})}
        server, base = serve(pager)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        validate_api.RESULTS.clear()
        out = io.StringIO()
        with redirect_stdout(out):
            validate_api.section_pages(base, "k")
        self.assertTrue(len(validate_api.RESULTS) >= 10, out.getvalue())
        self.assertEqual([r for r in validate_api.RESULTS if r[0] == "FAIL"], [], out.getvalue())

    def test_the_paging_checks_fail_on_a_server_that_drops_or_repeats_items(self):
        from urllib.parse import parse_qs, urlparse
        items = [{"entity_id": f"e{i:03d}"} for i in range(7)]

        def buggy(method, path, auth, body):                  # skips an item at every page boundary
            parsed = urlparse(path)
            if parsed.path != "/v1/customer/entities":
                return 404, {}
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            limit = max(1, min(int(query.get("limit", 50)), 1000))
            start = int(query["cursor"]) + 1 if "cursor" in query else 0
            more = start + limit < len(items)
            return 200, {"items": items[start:start + limit], "next_cursor": str(start + limit) if more else None, "limit": limit}
        server, base = serve(buggy)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        validate_api.RESULTS.clear()
        with redirect_stdout(io.StringIO()):
            validate_api.section_pages(base, "k")
        self.assertTrue([r for r in validate_api.RESULTS if r[0] == "FAIL" and "same items, each once" in r[1]])

    def test_pointing_it_at_a_server_needs_the_explicit_go_ahead(self):
        err = io.StringIO()
        from contextlib import redirect_stderr
        with redirect_stderr(err):
            self.assertEqual(validate_api.main(["--base", "http://127.0.0.1:9", "--admin-key", "k"]), 2)
        self.assertIn("--confirm-writes", err.getvalue())

    def test_a_server_that_drops_the_connection_is_a_failure_not_a_crash(self):
        def crashes(method, path, auth, body):
            raise RuntimeError("handler blew up")                  # the stand-in's handler thread dies and the connection closes with no response
        server, base = serve(crashes)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        code, output = self.run_main(base)
        self.assertEqual(code, 1)
        self.assertNotIn("Traceback (most recent call last)", output)

    def test_an_unreachable_server_is_a_failure_not_a_crash(self):
        code, output = self.run_main("http://127.0.0.1:9")
        self.assertEqual(code, 1)
        self.assertNotIn("Traceback", output)


if __name__ == "__main__":
    unittest.main()
