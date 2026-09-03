#!/usr/bin/env python3
"""
Standalone business logic unit test — no Docker, no real Kafka, no LLM calls required.

Usage: python tests/test_deterministic_flow.py

This script does not depend on any .env file or external services (Kafka, MCP Confluent,
LLM provider): all required environment variables are set below, and the tested functions
are either pure (deterministic) or called with a mock Kafka producer (FakeProducer) that
does not open any network connections.

This is the POSITIVE path test: it proves the diagnosis logic works. The NEGATIVE path
(forbidden mutations denied by the broker) is a separate, live-broker test — see
tests/test_denied_mutations.py — because a real authorization denial cannot be proven
without a real authorizer to deny it.
"""

import json
import os
import sys
from pathlib import Path

# --- Setup required environment variables (independent of local .env) ---
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["MCP_CONFLUENT_URL"] = "http://localhost:3000"
os.environ["DIAGNOSTIC_LLM_PROVIDER"] = "openai"
os.environ["DIAGNOSTIC_LLM_MODEL"] = "gpt-4o"
os.environ["DIAGNOSTIC_LLM_API_KEY"] = ""

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / "agents"

# The tested modules live under agents/ (common, problem_injector, diagnostic)
# and are imported just like in production (PYTHONPATH=/app in the Dockerfile).
sys.path.insert(0, str(AGENTS_DIR))

RESULTS: list[tuple[str, bool]] = []


def check(label: str, condition: bool) -> bool:
    """Print pass/fail for an individual test assertion."""
    print(f"  {'PASS' if condition else 'FAIL'} {label}")
    return condition


def run_test(name: str, func) -> None:
    print(f"\n=== {name} ===")
    try:
        ok = bool(func())
    except Exception as e:
        print(f"  FAIL Unhandled exception: {e!r}")
        ok = False
    RESULTS.append((name, ok))


# ---------------------------------------------------------------------------
# Test 1 — Imports
# ---------------------------------------------------------------------------

def test_imports() -> bool:
    modules = [
        "common.config",
        "common.adk_factory",
        "common.mcp_client",
        "problem_injector.app",
        "diagnostic.agent",
        "diagnostic.prompts",
    ]
    ok = True
    for mod_name in modules:
        try:
            __import__(mod_name)
            print(f"  PASS import {mod_name}")
        except Exception as e:
            print(f"  FAIL import {mod_name} — {e}")
            ok = False
    return ok


# ---------------------------------------------------------------------------
# Test 2 — Config
# ---------------------------------------------------------------------------

def test_config() -> bool:
    try:
        from common import config
    except Exception as e:
        print(f"  FAIL Failed to import common.config — {e}")
        return False

    ok = True
    for attr in (
        "DIAGNOSTIC_LLM_PROVIDER", "DIAGNOSTIC_LLM_MODEL", "DIAGNOSTIC_LLM_API_KEY",
        "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD",
        "MCP_CONFLUENT_URL", "TOPIC_FACTURES", "TOPIC_INCIDENTS",
        "FACTURATION_CONSUMER_GROUP", "FACTURATION_PARTITION", "POISON_OFFSET",
        "MESSAGES_AFTER_POISON", "DIAGNOSTIC_SCAN_COUNT",
    ):
        ok &= check(f"config.{attr} defined", hasattr(config, attr))

    ok &= check("config has no 'alerts' topic constant (one-shot agent, no trigger topic)",
                not hasattr(config, "TOPIC_ALERTS"))
    ok &= check("config has no fix/verify timing constants (no apply/verify loop exists)",
                not hasattr(config, "VERIFY_FIX_DELAY_S") and not hasattr(config, "CATCHUP_TIMEOUT_S"))

    conf = config.kafka_client_config()
    ok &= check("kafka_client_config() sets bootstrap.servers", conf.get("bootstrap.servers") == "localhost:9092")

    print("\n  Config detected:")
    print(f"    DIAGNOSTIC: provider={config.DIAGNOSTIC_LLM_PROVIDER} model={config.DIAGNOSTIC_LLM_MODEL} "
          f"api_key={'(empty)' if not config.DIAGNOSTIC_LLM_API_KEY else '***'}")
    print(f"    KAFKA_BOOTSTRAP_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    MCP_CONFLUENT_URL={config.MCP_CONFLUENT_URL}")
    print(f"    Topics: {config.TOPIC_FACTURES}, {config.TOPIC_INCIDENTS}")
    print(f"    FACTURATION_CONSUMER_GROUP={config.FACTURATION_CONSUMER_GROUP} "
          f"partition={config.FACTURATION_PARTITION} POISON_OFFSET={config.POISON_OFFSET}")

    return ok


# ---------------------------------------------------------------------------
# Test 3 — Prompts
# ---------------------------------------------------------------------------

def test_prompts() -> bool:
    try:
        from diagnostic.prompts import SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT
    except Exception as e:
        print(f"  FAIL Failed to import prompts — {e}")
        return False

    ok = True
    ok &= check("diagnostic.SYSTEM_PROMPT is not empty", bool(SYSTEM_PROMPT.strip()))
    ok &= check("SYSTEM_PROMPT states the agent cannot apply a fix",
                "no tool to apply" in SYSTEM_PROMPT or "cannot" in SYSTEM_PROMPT.lower())

    try:
        DIAGNOSTIC_USER_PROMPT.format(consumer_group="facturation", topic="factures", partition=0)
        ok &= check("DIAGNOSTIC_USER_PROMPT.format(...) formats successfully", True)
    except Exception as e:
        ok &= check(f"DIAGNOSTIC_USER_PROMPT.format() raised an exception — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 4 — Deterministic Flow (Injection -> Diagnosis -> Publication)
# ---------------------------------------------------------------------------

class FakeProducer:
    """Mock Kafka producer: does not open any network connections, just records calls."""

    def __init__(self):
        self.produced: list[dict] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, None)

    def flush(self, timeout=None):
        return 0


def test_deterministic_flow() -> bool:
    try:
        from problem_injector.app import make_message, make_poison_message
        from diagnostic.agent import build_diagnostic, DiagnosticTools, _is_broken
        from common.mcp_client import extract_partition_lag
    except Exception as e:
        print(f"  FAIL Failed to import flow functions — {e}")
        return False

    ok = True

    # 1. Problem Injector — valid messages vs. poison message
    msg = make_message(1)
    ok &= check("make_message() has siret", bool(msg.get("siret")))
    ok &= check("make_message() has montant", isinstance(msg.get("montant"), float))
    ok &= check("make_message() has client_id", bool(msg.get("client_id")))

    poison = make_poison_message(42)
    ok &= check("make_poison_message() does not have siret", "siret" not in poison)

    # 2. _is_broken() — parsing error detection
    valid_wire = {"value": json.dumps(make_message(2))}
    poison_wire = {"value": json.dumps(make_poison_message(2))}
    malformed_wire = {"value": "not valid json"}
    ok &= check("_is_broken() returns False for valid message", _is_broken(valid_wire) is False)
    ok &= check("_is_broken() returns True for poison message (siret missing)", _is_broken(poison_wire) is True)
    ok &= check("_is_broken() returns True for malformed JSON", _is_broken(malformed_wire) is True)

    # 3. mcp_client.extract_partition_lag() — pure partition layout extraction
    lag_payload = {"topics": [{"topic": "factures", "partitions": [
        {"partition": 0, "committedOffset": 1452, "highWatermark": 1483, "lag": 31}
    ]}]}
    partition_lag = extract_partition_lag(lag_payload, "factures", 0)
    ok &= check("extract_partition_lag() finds correct partition committedOffset", partition_lag.get("committedOffset") == 1452)

    # 4. build_diagnostic() — root-cause analysis + proposed CLI command
    lag_data = {**partition_lag, "stagnant": True}
    incident = build_diagnostic("facturation", lag_data, [poison_wire], [valid_wire])
    ok &= check("build_diagnostic() identifies missing siret root cause", "siret" in incident.get("cause", ""))
    ok &= check("build_diagnostic() detects no burst (clean scan)", incident.get("burst") is False)
    ok &= check(
        "build_diagnostic() proposes the --reset-offsets command",
        "--reset-offsets" in incident.get("reset_command", "") and "--to-offset 1453" in incident["reset_command"],
    )
    ok &= check("build_diagnostic() has precondition (stopping consumer)", "Stop" in incident.get("precondition", ""))
    ok &= check("build_diagnostic() has an incident_id", bool(incident.get("incident_id")))
    ok &= check("build_diagnostic() states the agent cannot run the command itself",
                "no tool" in incident.get("operator_note", "").lower() and "diagnostic-agent-ro" in incident.get("operator_note", ""))

    incident_burst = build_diagnostic("facturation", lag_data, [poison_wire], [poison_wire])
    ok &= check("build_diagnostic() detects a burst when scan contains broken messages", incident_burst.get("burst") is True)

    # 5. DiagnosticTools — code-level tool barriers, not just prompt suggestions.
    # Structural proof the mutation capability is ABSENT from the code (not merely
    # unused): Article 2's apply_fix_simulated/verify_fix/_catch_up_simulated and its
    # AdminClient no longer exist anywhere on this class.
    ok &= check("DiagnosticTools has no apply_fix_simulated method", not hasattr(DiagnosticTools, "apply_fix_simulated"))
    ok &= check("DiagnosticTools has no verify_fix method", not hasattr(DiagnosticTools, "verify_fix"))
    ok &= check("DiagnosticTools has no _catch_up_simulated method", not hasattr(DiagnosticTools, "_catch_up_simulated"))
    ok &= check("DiagnosticTools constructor takes no AdminClient parameter",
                "admin" not in DiagnosticTools.__init__.__code__.co_varnames[:DiagnosticTools.__init__.__code__.co_argcount])

    producer = FakeProducer()
    tools = DiagnosticTools(producer)

    result = tools.diagnose(json.dumps(lag_data), json.dumps([poison_wire]), json.dumps([valid_wire]))
    ok &= check("diagnose() confirms publication", result.startswith("OK"))
    ok &= check("diagnose() marks incident_produced", tools.incident_produced is True)
    ok &= check(
        "diagnose() publishes the incident on 'incidents' topic",
        any(json.loads(p["value"]).get("event") == "diagnostic" for p in producer.produced),
    )
    ok &= check(
        "diagnose() never writes to any topic other than 'incidents'",
        all(p["topic"] == "incidents" for p in producer.produced),
    )

    return ok


# ---------------------------------------------------------------------------
# Test 5 — ADK Factory (without LLM network calls)
# ---------------------------------------------------------------------------

def test_adk_factory() -> bool:
    try:
        from common.adk_factory import create_llm, AdkAgentRunner
    except Exception as e:
        print(f"  FAIL Failed to import common.adk_factory — {e}")
        return False

    ok = True

    # create_llm() must raise ValueError for unknown providers, without LLM network calls
    try:
        create_llm("unknown-provider", "unknown-model", "fake-key")
        ok &= check("create_llm(unknown provider) raises ValueError", False)
    except ValueError as e:
        ok &= check("create_llm(unknown provider) raised ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"create_llm(unknown provider) raised {type(e).__name__} instead of ValueError — {e}", False)

    # AdkAgentRunner calls create_llm() during initialization (before any network calls),
    # so the error must surface at instantiation, without contacting any LLM APIs.
    try:
        AdkAgentRunner(
            name="test_agent",
            description="test agent description",
            instruction="test instruction",
            tools=[],
            provider="unknown-provider",
            model="unknown-model",
            api_key="fake-key",
        )
        ok &= check("AdkAgentRunner(unknown provider) raises an error", False)
    except ValueError as e:
        ok &= check("AdkAgentRunner(unknown provider) raised ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"AdkAgentRunner(unknown provider) raised {type(e).__name__} instead of ValueError — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Final Execution Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("DETERMINISTIC TEST — offline positive-path flow, no Docker/Kafka/LLM")
    print("(negative path — denied mutations — lives in test_denied_mutations.py,")
    print(" and requires the live broker stack; see README)")
    print("=" * 70)

    run_test("Test 1 — Imports", test_imports)
    run_test("Test 2 — Configuration", test_config)
    run_test("Test 3 — Prompts", test_prompts)
    run_test("Test 4 — Deterministic Flow (Injection -> Diagnosis -> Publication)", test_deterministic_flow)
    run_test("Test 5 — ADK Factory (without LLM network calls)", test_adk_factory)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    passed = 0
    for name, ok in RESULTS:
        print(f"  {'PASS' if ok else 'FAIL'} {name}")
        passed += ok

    total = len(RESULTS)
    print(f"\nResult: {passed}/{total} tests passed")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
