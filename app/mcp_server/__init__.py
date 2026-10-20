"""A read-only MCP (Model Context Protocol) server for the stewardship queue: see docs/mcp-server.md. `python3 mcp_server.py --help` starts it."""
from .client import ApiError, BackendUnavailable, HttpApiClient, ProposingApiClient
from .protocol import McpServer
from .tools import BY_NAME, TIER2_TOOLS, TOOLS


def build_server(api_url: str, timeout: float = 20.0, allow_propose: bool = False) -> McpServer:
    """The default server can only read: its client has no way to write. allow_propose adds the one write, recording a recommendation."""
    return McpServer(ProposingApiClient(api_url, timeout=timeout) if allow_propose else HttpApiClient(api_url, timeout=timeout), allow_propose=allow_propose)


__all__ = ["McpServer", "HttpApiClient", "ProposingApiClient", "ApiError", "BackendUnavailable", "TOOLS", "TIER2_TOOLS", "BY_NAME", "build_server"]
