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
#
# Includes one explicit DENY, not just ALLOWs: the wildcard Read-on-all-groups
# ALLOW that ephemeral consume-messages groups need would, on its own, also
# authorize Read on the literal 'facturation' group — which combined with the
# Read already granted on topic 'factures' is exactly what Kafka's OffsetCommit
# protocol (and therefore AdminClient.alter_consumer_group_offsets(), the exact
# call Article 2's apply_fix_simulated() made) requires. See the DENY grant
# below for the full explanation of why this couldn't be scoped any tighter.
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

echo "Granting diagnostic-agent-ro: Describe+Read on ALL groups (literal wildcard '*')..."
echo "  -- required because consume-messages joins a throwaway consumer group with a"
echo "     random UUID id on every call; there is no fixed group name to scope this to."
echo "     '*' is Kafka's own documented ACL wildcard (a LITERAL resource named"
echo "     exactly '*' matches every resource of that type) — not a prefix trick."
"${ACLS[@]}" --add --allow-principal User:diagnostic-agent-ro \
  --operation Describe --operation Read \
  --group '*'

echo "Denying diagnostic-agent-ro: Read on group 'facturation' specifically..."
echo "  -- UNAVOIDABLE CONSTRAINT, stated precisely: the wildcard grant above is"
echo "     necessary (consume-messages' random-UUID groups can't be scoped any"
echo "     tighter — see the comment above) but it would ALSO grant Read on the one"
echo "     group name that actually matters: 'facturation'. Read on a Group plus Read"
echo "     on a Topic is exactly what Kafka's OffsetCommit protocol requires — and"
echo "     AdminClient.alter_consumer_group_offsets() (the call Article 2's"
echo "     apply_fix_simulated() used to make) is built on OffsetCommit. Combined with"
echo "     the Read already granted on topic 'factures' above, the wildcard alone would"
echo "     have silently re-authorized the exact mutation this repo exists to block."
echo "     A Kafka DENY ACL always wins over a matching ALLOW, regardless of which is"
echo "     more specific (this is documented core StandardAuthorizer/AclAuthorizer"
echo "     behavior, not a pattern-specificity trick) — so this explicit, narrower DENY"
echo "     removes Read from 'facturation' alone while leaving it granted on every other"
echo "     (ephemeral, random-UUID) group name the wildcard ALLOW still covers. The"
echo "     Describe grant on 'facturation' above is untouched: get-consumer-group-lag's"
echo "     OffsetFetch still works, since it only ever needed Describe."
"${ACLS[@]}" --add --deny-principal User:diagnostic-agent-ro \
  --operation Read \
  --group facturation

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
