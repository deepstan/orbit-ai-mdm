"""
The MCP server's way to the platform: HTTP GET of the platform's own API, with the CALLER's key. Nothing else.

  * It has no method that writes: there is only `get`. A tool cannot write, because there is nothing for it to call.
  * The key is passed in for each call and never stored on this object or in a log.
  * A redirect is never followed (so the Authorization header can only ever go to the platform's own address).
  * The answer is size-limited and must be JSON; a failure becomes a short, safe message (never a traceback, never the key).
"""
from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request


class ApiError(Exception):
    """The platform answered, and refused or failed."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


class BackendUnavailable(Exception):
    """The platform could not be reached in time."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class HttpApiClient:
    """GET only. The default server uses this, so it has nothing to write with."""

    def __init__(self, base_url: str, timeout: float = 20.0, max_bytes: int = 5_000_000):
        self.base_url = base_url.rstrip("/")
        self.timeout, self.max_bytes = timeout, max_bytes
        self._opener = urllib.request.build_opener(_NoRedirect)

    def get(self, path: str, query: dict, api_key: str):
        url = self.base_url + path + (("?" + urllib.parse.urlencode(query)) if query else "")
        return self._send(urllib.request.Request(url, method="GET", headers=self._headers(api_key)))

    @staticmethod
    def _headers(api_key: str) -> dict:
        return {"Authorization": f"Bearer {api_key}", "Accept": "application/json", "User-Agent": "mdm-mcp-server/1"}

    def _send(self, request):
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                body = response.read(self.max_bytes + 1)
        except urllib.error.HTTPError as e:
            raise ApiError(e.code, self._detail(e)) from None
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError):
            raise BackendUnavailable("the platform could not be reached") from None
        if len(body) > self.max_bytes:
            raise ApiError(502, f"the platform's answer is larger than {self.max_bytes} bytes")
        try:
            return json.loads(body)
        except ValueError:
            raise ApiError(502, "the platform did not answer with JSON") from None

    @staticmethod
    def _detail(error: urllib.error.HTTPError) -> str:
        try:
            detail = json.loads(error.read(20_000)).get("detail")
        except Exception:
            detail = None
        if isinstance(detail, (dict, list)):
            detail = json.dumps(detail)
        return str(detail)[:300] if detail else (error.reason or "error")


class ProposingApiClient(HttpApiClient):
    """Used ONLY when the server is started with --allow-propose. It adds one thing: a POST of a JSON body. Which paths it is ever asked to post to is decided by the tool table
    (one tool, propose_decision); the platform decides whether the key may (the permission propose_decision)."""

    def post(self, path: str, body: dict, api_key: str):
        data = json.dumps(body).encode("utf-8")
        return self._send(urllib.request.Request(self.base_url + path, data=data, method="POST", headers={**self._headers(api_key), "Content-Type": "application/json"}))
