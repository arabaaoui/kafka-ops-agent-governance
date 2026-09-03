"""
Diagnostic Agent — the only agent in this repo. Diagnoses a poison message
blocking the 'facturation' consumer group via MCP Confluent (read-only tool
catalogue), proposes the exact CLI command an operator would run to fix it,
publishes that diagnosis to 'incidents', then exits.

Unlike the Article 2 PoC this adapts, there is no apply_fix_simulated() and no
verify_fix(): no tool exists to alter the broker, and the diagnostic-agent-ro
Kafka identity this process authenticates as has no ACL that would let such a
call succeed even if one existed (see kafka/init-acls.sh and
tests/test_denied_mutations.py). Remediation is a human operator's action,
taken outside this process entirely.

Run once, in order:
  1. get_consumer_lag    — MCP get-consumer-group-lag (real) — find the
                            offset the group is stuck on, sampled twice to
                            confirm the lag is stagnant rather than merely high.
  2. read_from_offset x2 — MCP consume-messages (real) — read the stuck
                            message plus a few past it, to identify the
                            poison message and rule out a burst.
  3. diagnose             — deterministic tool: builds the root-cause
                            diagnosis and the exact 'kafka-consumer-groups.sh
                            --reset-offsets' command an operator would run,
                            and publishes it to 'incidents' via this
                            process's own direct producer — the one place
                            this identity is allowed to write.

get_consumer_lag and read_from_offset are real MCP Confluent calls against the
local Kafka broker, both authenticated as diagnostic-agent-ro. The diagnostic
LLM is OPTIONAL: if DIAGNOSTIC_LLM_API_KEY is empty, no ADK agent is ever
instantiated and the Python loop calls every tool directly, in order,
producing the exact same deterministic result — the loop's correctness never
depends on trusting the LLM to behave.
"""

import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from common.config import (
    TOPIC_INCIDENTS,
    TOPIC_FACTURES,
    FACTURATION_CONSUMER_GROUP,
    FACTURATION_PARTITION,
    POISON_OFFSET,
    DIAGNOSTIC_SCAN_COUNT,
    DIAGNOSTIC_LLM_PROVIDER,
    DIAGNOSTIC_LLM_MODEL,
    DIAGNOSTIC_LLM_API_KEY,
    kafka_client_config,
)
from common.adk_factory import AdkAgentRunner
from common.mcp_client import get_consumer_group_lag, extract_partition_lag, consume_messages, list_tools
from diagnostic.prompts import SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DIAGNOSTIC] %(levelname)s %(message)s",
)
logger = logging.getLogger("diagnostic")

# Delay between the two get-consumer-group-lag samples used to confirm the
# lag isn't draining (a real consumer would shrink it) rather than merely high.
STAGNANT_CHECK_DELAY_S = 5

# Tool names this process expects the MCP server to have advertised, checked
# at startup as a self-audit — see list_tools() below. Any name outside this
# set showing up would mean the catalogue restriction in
# mcp-confluent/config.yaml / docker-compose.app.yml regressed.
EXPECTED_TOOLS = {"get-consumer-group-lag", "consume-messages"}


def _is_broken(message: dict) -> bool:
    """A message is broken if its value isn't valid JSON, or is JSON missing 'siret'."""
    value = message.get("value")
    if value is None:
        return True
    try:
        data = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return True
    return "siret" not in data


def build_diagnostic(consumer_group: str, lag_data: dict, stuck_messages: list, scan_messages: list) -> dict:
    """
    Deterministic root-cause analysis: the message the consumer group is
    stuck on is missing 'siret', which crashes the consumer before it can
    commit past it. Scanning a few messages past that offset distinguishes
    one isolated poison message from a burst of bad data. This is the actual
    diagnosis — no LLM required.
    """
    committed_offset = lag_data.get("committedOffset")
    poison_offset = committed_offset if committed_offset is not None else POISON_OFFSET

    broken_in_stuck = [m for m in stuck_messages if _is_broken(m)]
    broken_in_scan = [m for m in scan_messages if _is_broken(m)]
    messages_affected = len(broken_in_stuck) + len(broken_in_scan)
    burst = len(broken_in_scan) > 0

    cause = (
        f"Poison message at offset {poison_offset}, partition {FACTURATION_PARTITION}. "
        f"Reason: missing 'siret' field, causing the consumer to crash during parsing. "
        f"{messages_affected} affected message(s), "
        + ("burst detected — multiple consecutive failing messages." if burst else "no burst.")
    )

    precondition = (
        "Stop the 'facturation' consumer process first — kafka-consumer-groups.sh --reset-offsets "
        "will fail if the group has active members."
    )
    reset_command = (
        f"kafka-consumer-groups.sh --bootstrap-server kafka:9093 --group {consumer_group} "
        f"--topic {TOPIC_FACTURES}:{FACTURATION_PARTITION} --reset-offsets --to-offset {poison_offset + 1} --execute"
    )
    operator_note = (
        "This agent has no tool and no Kafka permission to run the command above. It is diagnostic-agent-ro: "
        "Describe+Read on 'factures', Describe only on 'facturation' (Read is explicitly denied on this specific "
        "group, so this identity can never alter its offsets), Write on 'incidents' only — nothing that could "
        "alter an offset, produce to 'factures', or touch any other resource. A human operator must run this command."
    )

    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "event": "diagnostic",
        "incident_id": f"incident-{consumer_group}-{int(time.time() * 1000)}",
        "consumer_group": consumer_group,
        "topic": TOPIC_FACTURES,
        "partition": FACTURATION_PARTITION,
        "poison_offset": poison_offset,
        "cause": cause,
        "messages_affected": messages_affected,
        "burst": burst,
        "precondition": precondition,
        "reset_command": reset_command,
        "operator_note": operator_note,
        "lag_state": lag_data,
        "timestamp": timestamp,
    }


def produce_event(producer: Producer, event: dict) -> None:
    """Publish a diagnostic event to the 'incidents' topic — the only topic
    diagnostic-agent-ro can write to."""
    event_json = json.dumps(event, ensure_ascii=False)
    producer.produce(
        TOPIC_INCIDENTS,
        key=event.get("consumer_group"),
        value=event_json.encode("utf-8"),
        on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
    )
    producer.flush(timeout=5)


class DiagnosticTools:
    """Tools exposed to the diagnostic ADK agent. Bound to a live Kafka
    producer (write access to 'incidents' only). No AdminClient: this class
    has no method capable of altering broker state, by construction — not by
    convention. There is no apply_fix / verify_fix here to remove; they were
    never written into this repo (see README's Article 2 -> Article 3
    migration/copy map)."""

    def __init__(self, producer: Producer):
        self._producer = producer
        self.incident_produced = False
        self._last_incident: dict = {}

    def get_consumer_lag(self, group: str) -> dict:
        """Fetch consumer group lag via MCP Confluent (get-consumer-group-lag),
        sampled twice a few seconds apart to confirm the lag is stagnant
        (not draining) rather than merely high."""
        first = extract_partition_lag(
            get_consumer_group_lag(group, TOPIC_FACTURES), TOPIC_FACTURES, FACTURATION_PARTITION
        )
        time.sleep(STAGNANT_CHECK_DELAY_S)
        second = extract_partition_lag(
            get_consumer_group_lag(group, TOPIC_FACTURES), TOPIC_FACTURES, FACTURATION_PARTITION
        )
        stagnant = bool(first) and bool(second) and first.get("committedOffset") == second.get("committedOffset")
        latest = second or first
        return {**latest, "stagnant": stagnant}

    def read_from_offset(self, topic: str, partition: int, offset: int, count: int = DIAGNOSTIC_SCAN_COUNT) -> list:
        """Read messages starting at an absolute offset via MCP Confluent
        (consume-messages) — call once at the committed offset to find the
        message the consumer is stuck on, and once past it to scan for a burst."""
        return consume_messages(topic, partition, offset, count)

    def diagnose(self, lag_data_json: str, stuck_messages_json: str, scan_messages_json: str) -> str:
        """
        Deterministic tool: analyzes the lag reading, the message the group
        is stuck on, and the scan of messages past it — all already fetched
        by the other tools (no external call here) — and publishes the
        diagnostic, including the proposed CLI fix command, to the
        'incidents' topic. Call exactly once. This is the last step: there is
        no follow-on tool to apply or verify the fix.
        """
        def _load(value):
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return None
            return value

        lag_data = _load(lag_data_json) or {}
        stuck_messages = _load(stuck_messages_json) or []
        scan_messages = _load(scan_messages_json) or []

        incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, stuck_messages, scan_messages)
        produce_event(self._producer, incident)
        self._last_incident = incident
        self.incident_produced = True
        logger.info(f"Incident produced: {incident['incident_id']} — {incident['cause']}")
        return f"OK: diagnostic published — {incident['cause']} Proposed command: {incident['reset_command']}"


def deterministic_diagnose(tools: DiagnosticTools) -> None:
    """No-LLM path: run the full diagnose flow by calling the same tools directly, in order."""
    logger.info("No DIAGNOSTIC_LLM_API_KEY configured — running deterministic diagnosis")
    lag_data = tools.get_consumer_lag(FACTURATION_CONSUMER_GROUP)
    committed_offset = lag_data.get("committedOffset")
    poison_offset = committed_offset if committed_offset is not None else POISON_OFFSET
    stuck_messages = tools.read_from_offset(TOPIC_FACTURES, FACTURATION_PARTITION, poison_offset, 1)
    scan_messages = tools.read_from_offset(
        TOPIC_FACTURES, FACTURATION_PARTITION, poison_offset + 1, DIAGNOSTIC_SCAN_COUNT
    )
    incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, stuck_messages, scan_messages)
    produce_event(tools._producer, incident)
    tools._last_incident = incident
    tools.incident_produced = True
    logger.info(f"Incident produced: {incident['incident_id']} — {incident['cause']}")


def run_diagnosis(agent_runner: AdkAgentRunner | None, tools: DiagnosticTools) -> None:
    """Run one full pass — diagnose and publish — guaranteeing it always completes."""
    tools.incident_produced = False

    if agent_runner is not None:
        user_prompt = DIAGNOSTIC_USER_PROMPT.format(
            consumer_group=FACTURATION_CONSUMER_GROUP,
            topic=TOPIC_FACTURES,
            partition=FACTURATION_PARTITION,
        )
        try:
            response = agent_runner.run(user_prompt)
            logger.info(f"Agent response: {response}")
        except Exception as e:
            logger.error(f"Diagnostic agent LLM call failed: {e}")

    if not tools.incident_produced:
        if agent_runner is not None:
            logger.warning("No incident produced by the agent — falling back to deterministic diagnosis")
        deterministic_diagnose(tools)


def _audit_tool_catalogue() -> None:
    """Self-check at startup: log what the MCP server actually advertises and
    warn loudly if it's not exactly the expected read-only pair. This is not
    the enforcement mechanism (the server's --allow-tools flag and
    connections.local.read_only are); it's a visible tripwire if that
    restriction ever regresses."""
    advertised = set(list_tools())
    if not advertised:
        logger.warning("Could not retrieve the MCP tool catalogue at startup (server not reachable yet?)")
        return
    unexpected = advertised - EXPECTED_TOOLS
    if unexpected:
        logger.error(f"MCP server advertises unexpected tools beyond the read-only pair: {sorted(unexpected)}")
    else:
        logger.info(f"MCP tool catalogue confirmed read-only: {sorted(advertised)}")


def main():
    """One-shot: diagnose the stuck consumer group, publish the finding, exit.
    No listener loop, no self-trigger, no cooldown — those existed in Article 2
    to close a remediation loop that doesn't exist here."""
    _audit_tool_catalogue()

    producer = Producer(kafka_client_config())
    tools = DiagnosticTools(producer)

    agent_runner = None
    if DIAGNOSTIC_LLM_API_KEY:
        agent_runner = AdkAgentRunner(
            name="diagnostic_agent",
            description="Diagnoses a poison message blocking a Kafka consumer group and proposes the operator fix. Read-only: cannot apply any fix.",
            instruction=SYSTEM_PROMPT,
            tools=[
                tools.get_consumer_lag,
                tools.read_from_offset,
                tools.diagnose,
            ],
            provider=DIAGNOSTIC_LLM_PROVIDER,
            model=DIAGNOSTIC_LLM_MODEL,
            api_key=DIAGNOSTIC_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={DIAGNOSTIC_LLM_PROVIDER} model={DIAGNOSTIC_LLM_MODEL}")
    else:
        logger.info("No DIAGNOSTIC_LLM_API_KEY configured — running deterministic diagnosis")

    logger.info("Diagnostic agent started — single pass, no listener loop")
    run_diagnosis(agent_runner, tools)

    producer.flush(timeout=10)
    logger.info("Diagnostic agent finished.")


if __name__ == "__main__":
    main()
