from services.notification_service.config import settings
from services.notification_service.db import SessionLocal
from services.notification_service.models import Notification
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.logging_config import get_logger


logger = get_logger("notification-service")


consumer = KafkaConsumer(
    group_id="notification-service",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


dead_letter = DeadLetterPublisher(
    topic="notification-service.dlq",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


def handle_event(event: dict) -> None:
    if event["event_type"] != "order.confirmed":
        return

    data = event["data"]

    order_id = data["order_id"]
    customer_id = data["customer_id"]

    logger.info(
        "Processing order.confirmed",
        extra={
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "order_id": order_id,
        },
    )

    with SessionLocal() as db:
        existing_notification = (
            db.query(Notification)
            .filter(
                Notification.order_id == order_id
            )
            .first()
        )

        if existing_notification is not None:
            logger.warning(
                "Duplicate notification event ignored",
                extra={
                    "event_id": event["event_id"],
                    "order_id": order_id,
                },
            )
            return

        notification = Notification(
            order_id=order_id,
            customer_id=customer_id,
            channel="EMAIL",
            status="SENT",
        )

        db.add(notification)
        db.commit()

        logger.info(
            "Notification sent",
            extra={
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "order_id": order_id,
            },
        )


if __name__ == "__main__":
    consumer.subscribe(
        settings.notification_events_topic
    )

    logger.info(
        "Notification Service started",
        extra={
            "topic": settings.notification_events_topic,
        },
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )