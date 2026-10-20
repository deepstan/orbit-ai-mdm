#!/usr/bin/env python3
"""
Start the read-only MCP server for the stewardship queue (docs/mcp-server.md).

  stdio (a local AI client starts it; the key is in the environment):
      MDM_API_URL=https://mdm.example.com MDM_API_KEY=... python3 mcp_server.py
  HTTP (each caller sends its own key as `Authorization: Bearer ...`):
      MDM_API_URL=https://mdm.example.com python3 mcp_server.py --http --port 8765

Use a key whose role is `steward_agent`: it can read what a steward reads and change nothing.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.mcp_server import build_server  # noqa: E402
from app.mcp_server.transports import make_http_server, serve_stdio  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only MCP server for the MDM stewardship queue.")
    parser.add_argument("--http", action="store_true", help="serve over HTTP (POST /mcp) instead of standard input and output")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP: the address to bind (default: loopback only)")
    parser.add_argument("--port", type=int, default=8765, help="HTTP: the port (default 8765)")
    parser.add_argument("--allow-origin", action="append", default=[], help="HTTP: a browser Origin that may call it (repeatable); none by default")
    parser.add_argument("--api-url", default=os.environ.get("MDM_API_URL", "http://127.0.0.1:8000"), help="the platform's address (default: $MDM_API_URL or http://127.0.0.1:8000)")
    parser.add_argument("--timeout", type=float, default=20.0, help="seconds to wait for the platform")
    parser.add_argument("--allow-propose", action="store_true", default=os.environ.get("MDM_MCP_ALLOW_PROPOSE") == "1",
                        help="also offer propose_decision (the only write: it records a recommendation and changes nothing about the item; the key still needs the propose_decision permission). Off by default")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")     # standard error only: standard output is the protocol in stdio mode
    server = build_server(args.api_url, timeout=args.timeout, allow_propose=args.allow_propose)
    if args.http:
        httpd = make_http_server(server, args.host, args.port, args.allow_origin)
        logging.getLogger("mdm.mcp").info("listening on http://%s:%d/mcp -> %s", args.host, httpd.server_address[1], args.api_url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    key = os.environ.get("MDM_API_KEY")
    if not key:
        print("MDM_API_KEY is not set: the server would start but could not ask the platform anything.", file=sys.stderr)
    serve_stdio(server, key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
