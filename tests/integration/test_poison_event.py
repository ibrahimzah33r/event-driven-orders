import json
import time
import uuid

from confluent_kafka import Consumer

from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


DLQ_TOPIC = "payment-service.dlq"
ORDER_TOPIC = "orders.events"


def test_poison_event_reaches_payment_dlq() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": (
                f"payment-dlq-test-{uuid.uuid4()}"
            ),
            "auto.offset.reset": "latest",
        }
    )

    consumer.subscribe([DLQ_TOPIC])

    # Poll once so Kafka assigns the consumer
    # before we publish the poison event.
    consumer.poll(1.0)

    poison_event = Event.create(
        event_type="order.created",
        version=2,
        data={
            "order_id": 999999,
            "customer_id": 999999,
            "total": "definitely-not-a-number",
            "currency": "GBP",
        },
    )

    producer = KafkaProducer()

    producer.publish(
        topic=ORDER_TOPIC,
        event=poison_event,
        key="999999",
    )

    deadline = time.time() + 15

    found = False

    while time.time() < deadline:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            continue

        dlq_event = json.loads(
            message.value().decode("utf-8")
        )

        original_event = (
            dlq_event
            .get("data", {})
            .get("original_event", {})
        )

        if (
            original_event.get("event_id")
            == poison_event.event_id
        ):
            found = True
            break

    consumer.close()

    assert found, (
        "Poison event did not reach "
        "payment-service.dlq"
    )