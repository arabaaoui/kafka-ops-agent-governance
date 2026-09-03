"""
Problem Injector — One-shot script that seeds the poison message scenario
described in the article: a single malformed invoice blocks the 'facturation'
consumer group on the 'factures' topic.

Runs as the `admin` Kafka principal (KAFKA_SASL_USERNAME=admin in
docker-compose.app.yml) — never as diagnostic-agent-ro. Provisioning data and
simulating the crashed 'facturation' consumer are operator actions, not
diagnostic ones; keeping them on a separate identity is what makes
diagnostic-agent-ro's restriction to Describe/Read (+ Write on 'incidents'
only) a real broker-enforced boundary rather than a convention two services
happen to share.

Creates the 'factures' topic (if missing), produces a run of valid invoice
messages, one poison message at POISON_OFFSET (missing its 'siret' field),
then more valid messages past it — so the diagnostic agent can confirm a
single isolated bad message rather than a burst. A throwaway consumer then
plays the role of the real 'facturation' service: it processes messages in
order and requires 'siret' on each one, so it crashes exactly on the poison
message and disconnects without committing it — leaving the consumer
group's committed offset stuck at POISON_OFFSET, forever retrying the same
message on every restart. Runs once and exits; not a long-running service.
"""

import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import (
    TOPIC_FACTURES,
    FACTURATION_CONSUMER_GROUP,
    FACTURATION_PARTITION,
    POISON_OFFSET,
    MESSAGES_AFTER_POISON,
    kafka_client_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROBLEM-INJECTOR] %(levelname)s %(message)s",
)
logger = logging.getLogger("problem_injector")

TOTAL_MESSAGES = POISON_OFFSET + 1 + MESSAGES_AFTER_POISON


def ensure_topic(admin: AdminClient, topic: str) -> None:
    """Create the topic if it doesn't already exist."""
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info(f"Topic '{topic}' already exists — skipping creation")
        return

    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    futures = admin.create_topics([new_topic])
    for name, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{name}' created")
        except Exception as e:
            logger.warning(f"Could not create topic '{name}' (may already exist): {e}")


def make_message(msg_id: int) -> dict:
    """A valid invoice message — always carries 'siret'."""
    return {
        "id": msg_id,
        "siret": f"{100000000 + (msg_id % 900000000):09d}00013",
        "client_id": f"CLI-{msg_id % 500:04d}",
        "montant": round(50 + (msg_id % 100) * 3.37, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_poison_message(msg_id: int) -> dict:
    """The poison message: valid JSON, but missing the required 'siret' field —
    simulates an upstream producer bug rather than a wire-format corruption."""
    return {
        "id": msg_id,
        "client_id": f"CLI-{msg_id % 500:04d}",
        "montant": round(50 + (msg_id % 100) * 3.37, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def produce_batch(producer: Producer) -> None:
    """Produce the full 'factures' run: valid messages, the poison message at
    POISON_OFFSET, then more valid messages — in a single partition, so
    message id == offset."""
    for msg_id in range(TOTAL_MESSAGES):
        msg = make_poison_message(msg_id) if msg_id == POISON_OFFSET else make_message(msg_id)
        producer.produce(
            TOPIC_FACTURES,
            key=str(msg_id),
            value=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            on_delivery=lambda err, m: logger.error(f"Delivery failed: {err}") if err else None,
        )
    producer.flush(timeout=10)


def run_facturation_consumer() -> None:
    """
    Plays the role of the real 'facturation' consumer: reads messages in
    order and requires a 'siret' field on each one. Crashes (KeyError, caught
    here to emulate a process crash rather than tearing down the injector)
    on the poison message and disconnects WITHOUT committing it — a real
    restart of this consumer would refetch the same offset and crash again,
    which is exactly why the committed offset never advances past it.
    """
    consumer = Consumer(kafka_client_config(**{
        "group.id": FACTURATION_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }))
    consumer.subscribe([TOPIC_FACTURES])

    last_good_msg = None
    deadline = time.time() + 60

    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        offset = msg.offset()
        try:
            data = json.loads(msg.value().decode("utf-8"))
            _siret = data["siret"]
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(
                f"CRASH (simulated): consumer '{FACTURATION_CONSUMER_GROUP}' crashed "
                f"processing offset {offset} — {type(e).__name__}: {e}"
            )
            break

        last_good_msg = msg

    if last_good_msg is not None:
        consumer.commit(
            offsets=[TopicPartition(TOPIC_FACTURES, last_good_msg.partition(), last_good_msg.offset() + 1)],
            asynchronous=False,
        )
    consumer.close()
    logger.info(
        f"Consumer group '{FACTURATION_CONSUMER_GROUP}' committed up to offset "
        f"{(last_good_msg.offset() + 1) if last_good_msg else 0} — stuck on the poison message"
    )


def main() -> None:
    admin = AdminClient(kafka_client_config())
    ensure_topic(admin, TOPIC_FACTURES)

    # Give the broker a moment to propagate topic metadata before producing.
    time.sleep(2)

    producer = Producer(kafka_client_config())

    logger.info(
        f"Producing {TOTAL_MESSAGES} messages into '{TOPIC_FACTURES}' "
        f"(poison message at offset {POISON_OFFSET}, missing 'siret')..."
    )
    produce_batch(producer)

    run_facturation_consumer()

    lag = TOTAL_MESSAGES - POISON_OFFSET
    logger.info(
        f"{TOTAL_MESSAGES} messages in '{TOPIC_FACTURES}' — "
        f"consumer group '{FACTURATION_CONSUMER_GROUP}' stuck at offset {POISON_OFFSET} "
        f"({lag} messages behind, including 1 poison message)"
    )


if __name__ == "__main__":
    main()
