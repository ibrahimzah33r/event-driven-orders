from services.notification_service.config import (
    settings,
)
from services.notification_service.db import (
    SessionLocal,
)
from services.notification_service.models import (
    Notification,
)
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.dead_letter import DeadLetterPublisher

consumer = KafkaConsumer(
    group_id="notification-service",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)

dead_letter = DeadLetterPublisher(
    topic="notification-service.dlq",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)

def handle_event(event: dict) -> None:
    if event["event_type"] != "order.confirmed":
        return

    data = event["data"]

    with SessionLocal() as db:
        notification = Notification(
            order_id=data["order_id"],
            customer_id=data["customer_id"],
            channel="EMAIL",
            status="SENT",
        )

        db.add(notification)
        db.commit()
        db.refresh(notification)

        print(
            f"Notification {notification.id} sent "
            f"for order {notification.order_id}"
        )


if __name__ == "__main__":
    consumer.subscribe(
        settings.notification_events_topic
    )

    print(
        "Notification Service listening "
        "for confirmed orders..."
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )