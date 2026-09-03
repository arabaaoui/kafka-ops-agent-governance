#!/usr/bin/env python3
"""
LIVE test — requires the mcp-confluent service running (`make app`, part of
`make demo-diag`). Confirms the app-layer restriction is real: calls the
actual MCP server's `tools/list` and asserts it advertises exactly the
read-only pair this repo relies on, nothing else — no produce-message,
create-topics, delete-topics, or alter-topic-config.

This does not re-derive the restriction mechanism (that's --allow-tools +
connections.local.read_only in docker-compose.app.yml / mcp-confluent/config.yaml,
verified against the official @confluentinc/mcp-confluent v1.5.0 package
source — see the comments there); it independently checks the OUTCOME, so a
future config or package change that silently widens the catalogue fails this
test rather than going unnoticed.

Skips cleanly (exit 0) if the server isn't reachable.

Usage: python tests/test_tool_catalogue.py
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "agents"))

os.environ.setdefault("MCP_CONFLUENT_URL", "http://localhost:3002")

from common.mcp_client import list_tools  # noqa: E402

EXPECTED_TOOLS = {"get-consumer-group-lag", "consume-messages"}

# A representative sample of mutating tool names from the real ToolName enum
# (dist/confluent/tools/tool-name.js in the installed 1.5.0 package) that must
# never appear in this server's advertised catalogue for this connection.
KNOWN_MUTATING_TOOLS = {
    "produce-message",
    "create-topics",
    "delete-topics",
    "alter-topic-config",
    "create-connector",
    "delete-connector",
    "create-schema",
    "delete-schema",
}


def main() -> None:
    print("=" * 70)
    print("LIVE MCP TOOL CATALOGUE TEST — requires mcp-confluent running")
    print(f"MCP_CONFLUENT_URL={os.environ['MCP_CONFLUENT_URL']}")
    print("=" * 70)

    advertised = set(list_tools())
    if not advertised:
        print("\nSKIP: mcp-confluent not reachable — start it first: make app")
        sys.exit(0)

    print(f"\nAdvertised tools: {sorted(advertised)}")

    passed = 0
    total = 0

    total += 1
    ok = advertised == EXPECTED_TOOLS
    print(f"  {'PASS' if ok else 'FAIL'} advertised tool set is exactly {sorted(EXPECTED_TOOLS)}")
    passed += ok

    total += 1
    overlap = advertised & KNOWN_MUTATING_TOOLS
    ok = len(overlap) == 0
    print(f"  {'PASS' if ok else 'FAIL'} no known mutating tool is advertised (overlap: {sorted(overlap)})")
    passed += ok

    print(f"\nResult: {passed}/{total} tests passed")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
