from services.order_service.config import settings
from services.order_service.db import SyncSessionLocal
from services.order_service.models import (
    Order,
    OutboxMessage,
)
from shared.events.models import Event
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.logging_config import get_logger


logger = get_logger("order-service-status")


consumer = KafkaConsumer(
    group_id="order-service-status",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


dead_letter = DeadLetterPublisher(
    topic="order-service-status.dlq",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


def handle_event(event: dict) -> None:
    event_type = event["event_type"]

    if event_type not in {
        "payment.completed",
        "inventory.reserved",
    }:
        return

    data = event["data"]
    order_id = data["order_id"]

    logger.info(
        "Processing status event",
        extra={
            "event_id": event["event_id"],
            "event_type": event_type,
            "order_id": order_id,
        },
    )

    with SyncSessionLocal() as db:
        order = db.get(Order, order_id)

        if order is None:
            logger.warning(
                "Order not found for event",
                extra={
                    "event_id": event["event_id"],
                    "event_type": event_type,
                    "order_id": order_id,
                },
            )
            return

        was_confirmed = order.status == "CONFIRMED"

        if event_type == "payment.completed":
            order.payment_status = "COMPLETED"

        elif event_type == "inventory.reserved":
            order.inventory_status = "RESERVED"

        if (
            order.payment_status == "COMPLETED"
            and order.inventory_status == "RESERVED"
        ):
            order.status = "CONFIRMED"

        if (
            order.status == "CONFIRMED"
            and not was_confirmed
        ):
            confirmed_event = Event.create(
                event_type="order.confirmed",
                data={
                    "order_id": order.id,
                    "customer_id": order.customer_id,
                    "total": str(order.total),
                },
            )

            outbox_message = OutboxMessage(
                event_id=confirmed_event.event_id,
                topic=settings.notification_events_topic,
                event_key=str(order.id),
                payload=confirmed_event.to_dict(),
            )

            db.add(outbox_message)

            logger.info(
                "Order confirmed",
                extra={
                    "event_id": confirmed_event.event_id,
                    "event_type": confirmed_event.event_type,
                    "order_id": order.id,
                },
            )

        db.commit()

        logger.info(
            "Order status updated",
            extra={
                "event_id": event["event_id"],
                "event_type": event_type,
                "order_id": order.id,
            },
        )


if __name__ == "__main__":
    consumer.subscribe(
        [
            settings.payment_events_topic,
            settings.inventory_events_topic,
        ]
    )

    logger.info(
        "Order status worker started"
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )