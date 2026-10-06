from services.inventory_service.config import settings
from services.inventory_service.db import SessionLocal
from services.inventory_service.models import (
    InventoryReservation,
    OutboxMessage,
)
from shared.events.models import Event
from shared.events.order_events import parse_order_created
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.logging_config import get_logger


logger = get_logger("inventory-service")


consumer = KafkaConsumer(
    group_id="inventory-service",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


dead_letter = DeadLetterPublisher(
    topic="inventory-service.dlq",
    bootstrap_servers=settings.kafka_bootstrap_servers,
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
        existing_reservation = (
            db.query(InventoryReservation)
            .filter(
                InventoryReservation.order_id == order_id
            )
            .first()
        )

        if existing_reservation is not None:
            logger.warning(
                "Duplicate order event ignored",
                extra={
                    "event_id": event["event_id"],
                    "order_id": order_id,
                },
            )
            return

        reservation = InventoryReservation(
            order_id=order_id,
            status="RESERVED",
        )

        db.add(reservation)
        db.flush()

        inventory_event = Event.create(
            event_type="inventory.reserved",
            data={
                "reservation_id": reservation.id,
                "order_id": reservation.order_id,
                "status": reservation.status,
            },
        )

        outbox_message = OutboxMessage(
            event_id=inventory_event.event_id,
            topic=settings.inventory_events_topic,
            event_key=str(reservation.order_id),
            payload=inventory_event.to_dict(),
        )

        db.add(outbox_message)
        db.commit()

        logger.info(
            "Inventory reserved",
            extra={
                "event_id": inventory_event.event_id,
                "event_type": inventory_event.event_type,
                "order_id": reservation.order_id,
            },
        )


if __name__ == "__main__":
    consumer.subscribe(
        settings.order_events_topic
    )

    logger.info(
        "Inventory Service started",
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