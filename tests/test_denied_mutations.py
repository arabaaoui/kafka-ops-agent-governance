#!/usr/bin/env python3
"""
LIVE negative-path test — requires the real broker stack running
(`make test-stack`, which applies ACLs automatically via the kafka-acl-init
service; or `make demo-denied`, which starts the stack and runs this file).

Proves, against a real StandardAuthorizer-backed broker, that the
diagnostic-agent-ro identity — the SAME credentials the diagnostic agent and
mcp-confluent use — cannot produce to 'factures', cannot create or delete
topics, cannot delete a consumer group, and cannot alter a consumer group's
committed offsets (the exact call Article 2's apply_fix_simulated() used to
make). Positive controls in the same run prove the ACLs aren't just denying
everything: the identity's actual observation path (Describe/Read on
'factures'/'facturation', Write on 'incidents') must still work.

This is the SECOND, independent enforcement layer. tests/test_deterministic_flow.py
proves the mutation capability is structurally absent from the application code;
this test proves that even bypassing the application entirely and talking to the
broker directly with the same credentials, the broker itself refuses.

Skips cleanly (exit 0) if the broker isn't reachable, the same way
mcp-confluent's own `@cp`-tagged integration tests skip when their env vars are
unset — this is a live-infrastructure test, not a unit test, and its absence
from a plain `python -m pytest` run on a Docker-less host is expected, not a
failure.

Usage: python tests/test_denied_mutations.py
"""

import json
import os
import sys
import time
import uuid

from confluent_kafka import Producer, KafkaException, TopicPartition, ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, NewTopic

# Port 9095 (the "HOST" listener, docker-compose.test.yml), not 9093: the
# broker advertises "kafka:9093" as the reconnect address for its internal
# listener, which only resolves inside the Docker network. This test runs
# from the host on purpose (see module docstring), so it needs the listener
# advertised as "localhost:9095".
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9095")
AGENT_USERNAME = os.environ.get("KAFKA_AGENT_SASL_USERNAME", "diagnostic-agent-ro")
AGENT_PASSWORD = os.environ.get("KAFKA_AGENT_SASL_PASSWORD", "agent-secret")

CONNECT_TIMEOUT_S = 6

RESULTS: list[tuple[str, bool]] = []


def agent_client_config(**overrides) -> dict:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": AGENT_USERNAME,
        "sasl.password": AGENT_PASSWORD,
    }
    conf.update(overrides)
    return conf


def check(label: str, condition: bool) -> bool:
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


def broker_reachable() -> bool:
    """Best-effort connectivity + SASL-handshake probe. Any failure here means
    "the live stack isn't usable from this host right now" — could be the
    stack not running, or the network not reachable — either way this test
    cannot run and must skip rather than fail.

    Deliberately scoped to the single topic diagnostic-agent-ro is granted
    Describe on ('factures'), NOT a bare list_topics() cluster-wide metadata
    listing: that identity has no cluster-wide Describe grant (see
    kafka/init-acls.sh — every ACL is scoped to a specific resource), so an
    unscoped listing could itself be denied on a live, fully-working,
    correctly-provisioned stack — which would make this probe return False
    and the whole suite SKIP without ever actually proving anything. Scoping
    to 'factures' tests exactly the permission this identity is supposed to
    have, so a false "not reachable" here would itself be a finding, not a
    reason to skip quietly.
    """
    try:
        admin = AdminClient(agent_client_config())
        admin.list_topics(topic="factures", timeout=CONNECT_TIMEOUT_S)
        return True
    except Exception as e:
        print(f"  (broker not reachable at {BOOTSTRAP}: {e})")
        return False


def is_authz_error(exc: BaseException) -> bool:
    from confluent_kafka import KafkaError
    if not isinstance(exc, KafkaException):
        return False
    code = exc.args[0].code()
    return code in (
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
        KafkaError.CLUSTER_AUTHORIZATION_FAILED,
    )


def is_partition_authz_error(err) -> bool:
    from confluent_kafka import KafkaError
    if err is None:
        return False
    return err.code() in (
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
        KafkaError.CLUSTER_AUTHORIZATION_FAILED,
    )


# ---------------------------------------------------------------------------
# Negative path — every mutation type Article 2's agent could perform
# ---------------------------------------------------------------------------

def test_produce_to_factures_denied() -> bool:
    """diagnostic-agent-ro must not be able to write to 'factures' — the seed
    data topic it only ever reads from."""
    producer = Producer(agent_client_config())
    delivery_errors = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(err)

    producer.produce("factures", key="attack", value=b'{"attack": true}', on_delivery=on_delivery)
    producer.flush(timeout=10)

    ok = len(delivery_errors) == 1
    ok &= check("produce to 'factures' was rejected by the broker", ok)
    if delivery_errors:
        err = delivery_errors[0]
        ok &= check(f"rejection is TOPIC_AUTHORIZATION_FAILED (got: {err})", err.code() == err.TOPIC_AUTHORIZATION_FAILED)
    return ok


def test_create_topic_denied() -> bool:
    """diagnostic-agent-ro must not be able to create new topics."""
    admin = AdminClient(agent_client_config())
    topic_name = f"contraband-{uuid.uuid4().hex[:8]}"
    futures = admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
    try:
        futures[topic_name].result(timeout=10)
        return check("create_topics() was rejected by the broker", False)
    except KafkaException as e:
        return check(f"create_topics() rejected with an authorization error (got: {e})", is_authz_error(e))


def test_delete_topic_denied() -> bool:
    """diagnostic-agent-ro must not be able to delete the seed data topic."""
    admin = AdminClient(agent_client_config())
    futures = admin.delete_topics(["factures"])
    try:
        futures["factures"].result(timeout=10)
        return check("delete_topics(['factures']) was rejected by the broker", False)
    except KafkaException as e:
        return check(f"delete_topics() rejected with an authorization error (got: {e})", is_authz_error(e))


def test_alter_consumer_group_offsets_denied() -> bool:
    """The exact call Article 2's apply_fix_simulated() made for real:
    AdminClient.alter_consumer_group_offsets() on 'facturation'. Must now be
    denied — this is the specific mutation this repo exists to structurally
    remove from the agent's reach."""
    admin = AdminClient(agent_client_config())
    request = [ConsumerGroupTopicPartitions("facturation", [TopicPartition("factures", 0, 1)])]
    futures = admin.alter_consumer_group_offsets(request)
    try:
        result = futures["facturation"].result(timeout=10)
        has_authz_error = False
        for tp in result.topic_partitions:
            if is_partition_authz_error(tp.error):
                has_authz_error = True
                break
        return check("alter_consumer_group_offsets() rejected with an authorization error on partitions", has_authz_error)
    except KafkaException as e:
        return check(f"alter_consumer_group_offsets() rejected with an authorization error (got: {e})", is_authz_error(e))


def test_delete_consumer_group_denied() -> bool:
    """diagnostic-agent-ro must not be able to delete the consumer group it diagnoses."""
    admin = AdminClient(agent_client_config())
    futures = admin.delete_consumer_groups(["facturation"])
    try:
        futures["facturation"].result(timeout=10)
        return check("delete_consumer_groups(['facturation']) was rejected by the broker", False)
    except KafkaException as e:
        return check(f"delete_consumer_groups() rejected with an authorization error (got: {e})", is_authz_error(e))


# ---------------------------------------------------------------------------
# Positive controls — prove the ACLs aren't just denying everything
# ---------------------------------------------------------------------------

def test_write_to_incidents_allowed() -> bool:
    """The one deliberate exception: diagnostic-agent-ro CAN write to 'incidents'."""
    producer = Producer(agent_client_config())
    delivery_errors = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(err)

    producer.produce(
        "incidents",
        key="test-denied-mutations",
        value=json.dumps({"event": "test_probe", "ts": time.time()}).encode("utf-8"),
        on_delivery=on_delivery,
    )
    producer.flush(timeout=10)
    return check("produce to 'incidents' succeeded (no delivery error)", len(delivery_errors) == 0)


def test_describe_facturation_lag_allowed() -> bool:
    """The observation path: diagnostic-agent-ro CAN read committed offsets
    for 'facturation' (the same OffsetFetch get-consumer-group-lag relies on)."""
    admin = AdminClient(agent_client_config())
    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions("facturation")]
    )
    try:
        result = futures["facturation"].result(timeout=10)
        return check(f"list_consumer_group_offsets('facturation') succeeded ({len(result.topic_partitions)} partition row(s))", True)
    except KafkaException as e:
        return check(f"list_consumer_group_offsets('facturation') unexpectedly failed: {e}", False)


def main() -> None:
    print("=" * 70)
    print("LIVE NEGATIVE-PATH TEST — requires the real broker stack")
    print(f"Bootstrap: {BOOTSTRAP}  Identity: {AGENT_USERNAME}")
    print("=" * 70)

    if not broker_reachable():
        print("\nSKIP: broker not reachable — start the stack first:")
        print("  make test-stack")
        print("(ACLs are applied automatically by the kafka-acl-init service as part of")
        print(" test-stack; `make demo-denied` starts the stack and runs these checks)")
        sys.exit(0)

    run_test("Denied: produce to 'factures'", test_produce_to_factures_denied)
    run_test("Denied: create a topic", test_create_topic_denied)
    run_test("Denied: delete 'factures'", test_delete_topic_denied)
    run_test("Denied: alter_consumer_group_offsets('facturation')", test_alter_consumer_group_offsets_denied)
    run_test("Denied: delete_consumer_groups(['facturation'])", test_delete_consumer_group_denied)
    run_test("Allowed: produce to 'incidents'", test_write_to_incidents_allowed)
    run_test("Allowed: read 'facturation' committed offsets", test_describe_facturation_lag_allowed)

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
