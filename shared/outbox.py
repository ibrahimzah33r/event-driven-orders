import time
from datetime import UTC, datetime

from shared.events.models import Event
from shared.kafka.producer import KafkaProducer
from shared.logging_config import get_logger


logger = get_logger("outbox-publisher")


def run_outbox_publisher(
    session_factory,
    outbox_model,
    bootstrap_servers: str,
    poll_interval_seconds: float = 1.0,
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers
    )

    logger.info(
        "Outbox publisher started"
    )

    while True:
        with session_factory() as db:
            message = (
                db.query(outbox_model)
                .filter(
                    outbox_model.published_at.is_(None)
                )
                .order_by(outbox_model.id)
                .first()
            )

            if message is None:
                time.sleep(
                    poll_interval_seconds
                )
                continue

            event = Event(**message.payload)

            order_id = (
                event.data.get("order_id")
                if isinstance(event.data, dict)
                else None
            )

            log_context = {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "order_id": order_id,
                "topic": message.topic,
            }

            try:
                logger.info(
                    "Publishing outbox event",
                    extra=log_context,
                )

                producer.publish(
                    topic=message.topic,
                    event=event,
                    key=message.event_key,
                )

                message.published_at = (
                    datetime.now(UTC)
                )

                db.commit()

                logger.info(
                    "Outbox event published",
                    extra=log_context,
                )

            except Exception:
                db.rollback()

                logger.error(
                    "Outbox publish failed",
                    extra=log_context,
                    exc_info=True,
                )

                time.sleep(
                    poll_interval_seconds
                )