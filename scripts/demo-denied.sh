#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Ops Agent Governance — Demo: Denied Mutations ==="
echo "Proves, against the real broker, that the diagnostic-agent-ro identity —"
echo "the SAME credentials the diagnostic agent and mcp-confluent use — cannot"
echo "mutate anything it isn't explicitly granted, while its observation path works."
echo ""

# 1. Start the test stack (broker + SASL users + ACLs + topics).
make test-stack
sleep 15

# 2. Seed data so the positive controls (read facturation lag) have something to read.
echo "-> Seeding scenario data (admin identity)..."
docker compose -f docker-compose.app.yml run --rm problem-injector

# 3. Run the live negative-path test as the diagnostic-agent-ro identity, from the host,
#    against the broker's HOST listener (port 9095, advertised as localhost:9095 —
#    see docker-compose.test.yml) — bypassing the application and MCP entirely, so
#    this proves the broker itself enforces the boundary.
echo ""
echo "-> Running tests/test_denied_mutations.py against the live broker..."
KAFKA_BOOTSTRAP_SERVERS=localhost:9095 \
KAFKA_AGENT_SASL_USERNAME=diagnostic-agent-ro \
KAFKA_AGENT_SASL_PASSWORD="${KAFKA_AGENT_SASL_PASSWORD:-agent-secret}" \
  python3 tests/test_denied_mutations.py

echo ""
echo "-> ACL bindings actually in force for diagnostic-agent-ro:"
docker compose -f docker-compose.test.yml exec -T kafka \
  /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9093 \
  --command-config /etc/kafka/admin.properties \
  --list --principal User:diagnostic-agent-ro
