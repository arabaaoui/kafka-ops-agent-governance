#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Ops Agent Governance — Demo: Diagnostic (poison message) ==="
echo ""

# 1. Start the test stack (broker + SASL users + ACLs + topics)
make test-stack
sleep 15

# 2. Inject the scenario: poison message on 'factures', consumer group 'facturation' stuck.
#    Runs as `admin` — see docker-compose.app.yml.
echo "-> Injecting the scenario (poison message on 'factures', consumer group 'facturation' stuck)..."
docker compose -f docker-compose.app.yml run --rm problem-injector

# 3. Start MCP Confluent (restricted, read-only tool catalogue) then the diagnostic agent.
#    Runs as diagnostic-agent-ro — see docker-compose.app.yml.
echo "-> Starting MCP Confluent (restricted to get-consumer-group-lag, consume-messages)..."
docker compose -f docker-compose.app.yml up -d mcp-confluent
echo "-> Starting the diagnostic agent (one-shot: diagnose, publish, exit)..."
docker compose -f docker-compose.app.yml run --rm diagnostic-agent

echo ""
echo "=== Tool catalogue self-audit (from the agent's own startup log) ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep -i "tool catalogue" || echo "  (not found in logs)"
echo ""
echo "=== Diagnostic ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep -A2 "Incident produced" || echo "  (no diagnostic logged — increase the sleep above if the stack is slow to start)"
echo ""
echo "=== Published to 'incidents' ==="
docker compose -f docker-compose.test.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9093 \
  --consumer.config /etc/kafka/admin.properties \
  --topic incidents --from-beginning --timeout-ms 5000 2>/dev/null || echo "  (topic incidents empty so far)"
echo ""
echo "Note: this run ends at the proposed CLI command. Nothing in this stack applies it —"
echo "see 'make demo-denied' for proof the diagnostic identity could not apply it even if it tried."
