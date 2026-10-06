from services.payment_service.config import settings
from services.payment_service.db import SessionLocal
from services.payment_service.models import (
    OutboxMessage,
    Payment,
)
from shared.events.models import Event
from shared.events.order_events import (
    parse_order_created,
)
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.logging_config import get_logger


logger = get_logger("payment-service")


consumer = KafkaConsumer(
    group_id="payment-service",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)


dead_letter = DeadLetterPublisher(
    topic="payment-service.dlq",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)


def handle_event(event: dict) -> None:
    if event["event_type"] != "order.created":
        return

    order_event = parse_order_created(event)
    order_id = order_event.order_id

    logger.info(
        "Processing order.created",
        extra={
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "order_id": order_id,
        },
    )

    with SessionLocal() as db:
        existing_payment = (
            db.query(Payment)
            .filter(
                Payment.order_id == order_id
            )
            .first()
        )

        if existing_payment is not None:
            logger.warning(
                "Duplicate order event ignored",
                extra={
                    "event_id": event["event_id"],
                    "order_id": order_id,
                },
            )
            return

        payment = Payment(
            order_id=order_id,
            amount=order_event.total,
            status="COMPLETED",
        )

        db.add(payment)

        # Get payment.id while keeping the transaction open.
        db.flush()

        payment_event = Event.create(
            event_type="payment.completed",
            data={
                "payment_id": payment.id,
                "order_id": payment.order_id,
                "amount": str(payment.amount),
                "status": payment.status,
            },
        )

        outbox_message = OutboxMessage(
            event_id=payment_event.event_id,
            topic=settings.payment_events_topic,
            event_key=str(payment.order_id),
            payload=payment_event.to_dict(),
        )

        db.add(outbox_message)

        # Payment + outgoing event committed atomically.
        db.commit()

        logger.info(
            "Payment completed",
            extra={
                "event_id": payment_event.event_id,
                "event_type": (
                    payment_event.event_type
                ),
                "order_id": payment.order_id,
            },
        )


if __name__ == "__main__":
    consumer.subscribe(
        settings.order_events_topic
    )

    logger.info(
        "Payment Service started",
        extra={
            "topic": settings.order_events_topic,
        },
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )