import json
import time
import uuid
import pytest
from confluent_kafka import Consumer

from services.order_service.config import settings
from services.order_service.db import SyncSessionLocal
from services.order_service.models import OutboxMessage
from shared.events.models import Event
from shared.kafka.producer import KafkaProducer
from shared.outbox import publish_next_outbox_message


TEST_TOPIC = "orders.events"

@pytest.mark.outbox_recovery
def test_outbox_event_survives_until_published() -> None:
    order_id = 888888

    event = Event.create(
        event_type="order.created",
        version=2,
        data={
            "order_id": order_id,
            "customer_id": 888888,
            "total": "42.00",
            "currency": "GBP",
        },
    )

    outbox_message = OutboxMessage(
        event_id=event.event_id,
        topic=TEST_TOPIC,
        event_key=str(order_id),
        payload=event.to_dict(),
    )

    # Simulate business code saving an event while
    # no outbox publisher is running.
    with SyncSessionLocal() as db:
        db.add(outbox_message)
        db.commit()

    # Prove the event is safely waiting in PostgreSQL.
    with SyncSessionLocal() as db:
        stored = (
            db.query(OutboxMessage)
            .filter(
                OutboxMessage.event_id
                == event.event_id
            )
            .one()
        )

        assert stored.published_at is None

    consumer = Consumer(
        {
            "bootstrap.servers":
                settings.kafka_bootstrap_servers,
            "group.id":
                f"outbox-test-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
        }
    )

    consumer.subscribe([TEST_TOPIC])

    # Give Kafka time to assign this consumer
    # before publishing.
    consumer.poll(1.0)

    producer = KafkaProducer(
        bootstrap_servers=(
            settings.kafka_bootstrap_servers
        )
    )

    # Simulate the outbox publisher returning later.
    published = publish_next_outbox_message(
        session_factory=SyncSessionLocal,
        outbox_model=OutboxMessage,
        producer=producer,
        event_id=event.event_id,
    )   

    assert published is True

    deadline = time.time() + 10
    received = False

    while time.time() < deadline:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            continue

        received_event = json.loads(
            message.value().decode("utf-8")
        )

        if received_event.get("event_id") == event.event_id:
            received = True
            break

    consumer.close()

    assert received, (
        "Outbox event was not received from Kafka"
    )

    # Prove the same outbox row is now marked published.
    with SyncSessionLocal() as db:
        stored = (
            db.query(OutboxMessage)
            .filter(
                OutboxMessage.event_id
                == event.event_id
            )
            .one()
        )

        assert stored.published_at is not None