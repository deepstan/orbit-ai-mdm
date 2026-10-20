"""
Two ways to reach the server.

  * stdio: one JSON message per line on standard input, one per line on standard output (logs go to standard error, never to standard output). The API key is the process's
    own: the environment variable MDM_API_KEY. This is how a desktop AI client starts a local server.
  * HTTP (the simple form of the Streamable HTTP transport): POST /mcp with a JSON-RPC message; a request is answered with application/json, a notification with 202. Each caller sends
    ITS OWN key as `Authorization: Bearer ...` and it is passed on to the platform, so one running server serves many tenants without holding anyone's key. It binds to the loopback
    address unless told otherwise, and refuses a browser's cross-origin request (a DNS-rebinding defence) unless the Origin was named with --allow-origin.
"""
from __future__ import annotations

import json
import logging
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger("mdm.mcp")
MAX_BODY = 1_000_000
MAX_DRAIN = 8_000_000           # a refused request's body is read and thrown away up to this much, so the client can finish sending and SEE the refusal; beyond it the connection is closed


def serve_stdio(server, api_key: str | None, stdin=None, stdout=None) -> None:
    stdin, stdout = stdin or sys.stdin, stdout or sys.stdout
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        answer = server.handle_text(line, api_key)
        if answer is not None:
            stdout.write(answer + "\n")
            stdout.flush()


def make_http_server(server, host: str = "127.0.0.1", port: int = 8765, allowed_origins=()) -> ThreadingHTTPServer:
    allowed = set(allowed_origins)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "mdm-mcp"

        def log_message(self, fmt, *args):              # one short line; never headers, never the body
            logger.info("http %s %s", self.command, self.path.split("?")[0])

        def _send(self, status: int, body: bytes = b"", extra: dict | None = None):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _refuse(self, status: int, body: bytes, length: int):
            """Answer a request we will not process. Its body is read and discarded first (up to MAX_DRAIN): replying to a client that is still sending makes it see a broken pipe
            instead of the refusal. A body larger than that closes the connection instead."""
            if length > MAX_DRAIN:
                self.close_connection = True
                return self._send(status, body, {"Connection": "close"})
            remaining = length
            while remaining > 0:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
            self._send(status, body)

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            if self.path.split("?")[0] != "/mcp":
                return self._refuse(404, b'{"error":"not found"}', length)
            origin = self.headers.get("Origin")
            if origin and origin not in allowed:       # a browser page on another site: refused
                return self._refuse(403, b'{"error":"origin not allowed"}', length)
            if length <= 0:
                return self._send(400, b'{"error":"empty body"}')
            if length > MAX_BODY:
                return self._refuse(413, b'{"error":"body too large"}', length)
            body = self.rfile.read(length).decode("utf-8", "replace")
            auth = self.headers.get("Authorization") or ""
            key = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else None
            answer = server.handle_text(body, key)
            if answer is None:
                return self._send(202)
            self._send(200, answer.encode("utf-8"))

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/healthz":
                return self._send(200, json.dumps({"ok": True}).encode())
            if path == "/mcp":                          # no server-initiated stream: allowed by the specification
                return self._send(405, b'{"error":"POST only"}', {"Allow": "POST"})
            self._send(404, b'{"error":"not found"}')

        def _no(self):
            self._send(405, b'{"error":"POST only"}', {"Allow": "POST"})
        do_PUT = do_DELETE = do_PATCH = _no

    return ThreadingHTTPServer((host, port), Handler)
