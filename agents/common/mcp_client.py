"""
MCP Confluent client helpers used by the diagnostic agent. Both tools used by
this repo (get-consumer-group-lag, consume-messages) work against a plain
bootstrap_servers connection, and are the only two tools the mcp-confluent
process is started with via --allow-tools (see docker-compose.app.yml) — no
other tool name will resolve against this server, by construction.
"""

import json
import logging
import time

import httpx

from .config import MCP_CONFLUENT_URL

logger = logging.getLogger(__name__)


_mcp_session_id = None


def _get_mcp_session(client: httpx.Client) -> str:
    """Initialize or return the cached MCP Confluent session ID."""
    global _mcp_session_id
    if _mcp_session_id is not None:
        return _mcp_session_id

    logger.info("Initializing stateful MCP Confluent session...")
    init_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "diagnostic-agent", "version": "1.0.0"},
        },
    }
    response = client.post(
        f"{MCP_CONFLUENT_URL}/mcp",
        json=init_payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    response.raise_for_status()
    session_id = response.headers.get("mcp-session-id")
    if not session_id:
        raise ValueError("mcp-session-id header missing from initialize response")

    _mcp_session_id = session_id
    logger.info(f"MCP Confluent session successfully initialized: {_mcp_session_id}")
    return _mcp_session_id


def _parse_sse_json(response: httpx.Response) -> dict:
    """Parse the JSON payload out of an SSE-formatted (text/event-stream) response."""
    for line in response.text.splitlines():
        if line.startswith("data:"):
            try:
                return json.loads(line[5:].strip())
            except json.JSONDecodeError:
                pass
    return {}


def _call_mcp_raw(tool_name: str, arguments: dict) -> dict:
    """Call an MCP Confluent tool via HTTP JSON-RPC using the stateful Streamable HTTP transport,
    returning the raw JSON-RPC 'result' object (isError / content / structuredContent)."""
    global _mcp_session_id
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": int(time.time() * 1000),
        }
        with httpx.Client(timeout=30.0) as client:
            session_id = _get_mcp_session(client)
            response = client.post(
                f"{MCP_CONFLUENT_URL}/mcp",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    "mcp-session-id": session_id,
                },
            )
            response.raise_for_status()
            result = _parse_sse_json(response)

            mcp_result = result.get("result", {})
            if mcp_result.get("isError"):
                content = mcp_result.get("content") or []
                text = content[0].get("text", "") if content else ""
                logger.warning(f"MCP Confluent tool '{tool_name}' returned an error: {text}")
                return {}
            return mcp_result
    except Exception as e:
        logger.warning(f"MCP Confluent call failed ({tool_name}): {e}")
        _mcp_session_id = None
        return {}


def list_tools() -> list[str]:
    """Real MCP call: tools/list. Returns the tool names this server instance
    actually advertises — used at agent startup as a self-check log line, and
    by tests/test_tool_catalogue.py to assert the catalogue stays read-only."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "params": {},
            "id": int(time.time() * 1000),
        }
        with httpx.Client(timeout=30.0) as client:
            session_id = _get_mcp_session(client)
            response = client.post(
                f"{MCP_CONFLUENT_URL}/mcp",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    "mcp-session-id": session_id,
                },
            )
            response.raise_for_status()
            result = _parse_sse_json(response)
            tools = result.get("result", {}).get("tools", [])
            return sorted(t.get("name", "") for t in tools if t.get("name"))
    except Exception as e:
        logger.warning(f"MCP Confluent tools/list failed: {e}")
        return []


def get_consumer_group_lag(group: str, topic: str) -> dict:
    """Real MCP call: get-consumer-group-lag. Returns the structuredContent
    payload {groupId, topics: [{topic, partitions: [...]}], totalLag}."""
    mcp_result = _call_mcp_raw("get-consumer-group-lag", {"groupId": group, "topics": [topic]})
    return mcp_result.get("structuredContent") or {}


def extract_partition_lag(lag_payload: dict, topic: str, partition: int) -> dict:
    """Pull the {partition, committedOffset, highWatermark, lag, ...} row for
    one (topic, partition) out of a get_consumer_group_lag() payload."""
    for t in lag_payload.get("topics", []):
        if t.get("topic") == topic:
            for p in t.get("partitions", []):
                if p.get("partition") == partition:
                    res = dict(p)
                    for key in ("committedOffset", "highWatermark", "lag"):
                        if res.get(key) is not None:
                            try:
                                res[key] = int(res[key])
                            except (ValueError, TypeError):
                                pass
                    return res
    return {}


def consume_messages(topic: str, partition: int, offset: int, count: int) -> list:
    """Real MCP call: consume-messages, seeking to an absolute partition
    offset. The tool's response embeds the JSON message list inside a text
    block rather than structuredContent — parsed out here."""
    mcp_result = _call_mcp_raw(
        "consume-messages",
        {
            "topics": [{"name": topic, "partition": partition, "start": {"offset": str(offset)}}],
            "maxMessages": count,
            "timeoutMs": 5000,
        },
    )
    content = mcp_result.get("content") or []
    text = content[0].get("text", "") if content else ""
    marker = "Consumed messages: "
    idx = text.find(marker)
    if idx == -1:
        return []
    try:
        return json.loads(text[idx + len(marker):])
    except json.JSONDecodeError:
        return []
