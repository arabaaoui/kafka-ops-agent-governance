"""
Configuration loader for all agents.
Reads from environment variables (set via Docker Compose or .env).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present (local dev)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path)

# Kafka bootstrap + SASL identity. Each service (problem-injector vs
# diagnostic-agent) is launched with a DIFFERENT KAFKA_SASL_USERNAME /
# KAFKA_SASL_PASSWORD pair in docker-compose.app.yml — this module has no
# notion of "the" identity, only whatever identity the process was handed.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")


def kafka_client_config(**overrides) -> dict:
    """Base confluent_kafka client config for whichever identity this process
    was launched as. SASL is applied whenever KAFKA_SASL_USERNAME is set;
    local-dev-only PLAIN over SASL_PLAINTEXT, matching docker-compose.test.yml.
    """
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_SASL_USERNAME:
        conf.update(
            {
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": KAFKA_SASL_USERNAME,
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )
    conf.update(overrides)
    return conf


# MCP Confluent
MCP_CONFLUENT_URL = os.getenv("MCP_CONFLUENT_URL", "http://mcp-confluent:3000")

# --- LLM config ---
# Supported providers: openai, anthropic, gemini (see agents/common/adk_factory.py
# — all providers route through LiteLLM).
DIAGNOSTIC_LLM_PROVIDER = os.getenv("DIAGNOSTIC_LLM_PROVIDER", "openai")
DIAGNOSTIC_LLM_MODEL = os.getenv("DIAGNOSTIC_LLM_MODEL", "gpt-4o")
DIAGNOSTIC_LLM_API_KEY = os.getenv("DIAGNOSTIC_LLM_API_KEY", "")

# Topic names — only two exist in this scenario. There is no 'alerts' topic:
# the diagnostic agent is a one-shot run (see agents/diagnostic/agent.py), not
# a long-poll listener, so there is no separate trigger topic to read from.
TOPIC_FACTURES = "factures"
TOPIC_INCIDENTS = "incidents"

# Consumer group monitored by the diagnostic agent (stuck on a poison message — see problem_injector)
FACTURATION_CONSUMER_GROUP = "facturation"

# 'factures' is a single partition so the offset arithmetic in the article stays exact.
FACTURATION_PARTITION = 0

# Offset of the poison message manufactured by the problem injector — the exact figure
# from the article's incident. The message at this offset is missing its 'siret' field,
# which crashes the (simulated) 'facturation' consumer before it commits past it.
POISON_OFFSET = 1452

# Valid messages produced after the poison offset, so the diagnostic agent can scan a
# window past it and confirm a single isolated bad message rather than a burst.
MESSAGES_AFTER_POISON = 30

# How many messages the diagnostic agent scans past the poison offset to rule out a burst.
DIAGNOSTIC_SCAN_COUNT = 5


def validate() -> list[str]:
    """Validate required config. Returns list of missing items."""
    missing = []
    if not DIAGNOSTIC_LLM_API_KEY:
        missing.append("DIAGNOSTIC_LLM_API_KEY (set in .env or environment)")
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing
