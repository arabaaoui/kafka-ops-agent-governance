#!/usr/bin/env bash
# Grants the diagnostic-agent-ro principal the minimal broker-level ACLs its
# observation-only diagnostic path needs — and nothing else. Run once against a
# healthy broker by the kafka-acl-init service, authenticated as the `admin`
# super user (which bypasses ACL checks entirely; it needs no grants of its own).
#
# Every grant below is sized against what @confluentinc/mcp-confluent's own tool
# handlers actually call (verified by reading dist/confluent/tools/handlers/kafka/
# in the installed 1.5.0 package), not guessed:
#   - get-consumer-group-lag  -> admin.fetchOffsets({groupId}) + a topic
#                                 metadata/watermark fetch.
#   - consume-messages        -> a real Consumer that subscribes and runs, using
#                                 an ephemeral groupId: nodeCrypto.randomUUID()
#                                 per call, with enable.auto.commit hardcoded
#                                 false (direct-client-manager.js).
set -euo pipefail

BOOTSTRAP="kafka:9093"
CMD_CONFIG="/etc/kafka/admin.properties"
ACLS=(/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG")

echo "Waiting for the broker to accept admin connections..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CMD_CONFIG" >/dev/null 2>&1; do
  sleep 2
done

echo "Granting diagnostic-agent-ro: Describe+Read on topic 'factures'..."
"${ACLS[@]}" --add --allow-principal User:diagnostic-agent-ro \
  --operation Describe --operation Read \
  --topic factures

echo "Granting diagnostic-agent-ro: Describe on group 'facturation' (OffsetFetch, no join)..."
"${ACLS[@]}" --add --allow-principal User:diagnostic-agent-ro \
  --operation Describe \
  --group facturation

echo "Granting diagnostic-agent-ro: Describe+Read on ALL groups (prefixed empty match)..."
echo "  -- required because consume-messages joins a throwaway consumer group with a"
echo "     random UUID id on every call; there is no fixed group name to scope this to."
"${ACLS[@]}" --add --allow-principal User:diagnostic-agent-ro \
  --operation Describe --operation Read \
  --group "" --resource-pattern-type prefixed

echo "Granting diagnostic-agent-ro: Describe+Write on topic 'incidents' (the one deliberate exception)..."
echo "  -- the diagnostic agent's own direct producer publishes its finding + proposed"
echo "     CLI command here; this is the only Write this identity has anywhere."
"${ACLS[@]}" --add --allow-principal User:diagnostic-agent-ro \
  --operation Describe --operation Write \
  --topic incidents

echo ""
echo "=== diagnostic-agent-ro ACL bindings ==="
"${ACLS[@]}" --list --principal User:diagnostic-agent-ro

echo ""
echo "ACL bootstrap complete."
