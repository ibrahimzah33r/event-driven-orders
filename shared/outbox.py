import time
from datetime import UTC, datetime

from shared.events.models import Event
from shared.kafka.producer import KafkaProducer
from shared.logging_config import get_logger


logger = get_logger("outbox-publisher")

def publish_next_outbox_message(
    session_factory,
    outbox_model,
    producer: KafkaProducer,
    event_id: str | None = None,
) -> bool:
    with session_factory() as db:
        query = (
            db.query(outbox_model)
            .filter(
                outbox_model.published_at.is_(None)
            )
        )

        if event_id is not None:
            query = query.filter(
                outbox_model.event_id == event_id
            )

        message = (
            query
            .order_by(outbox_model.id)
            .first()
        )

        if message is None:
            return False

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

            message.published_at = datetime.now(UTC)

            db.commit()

            logger.info(
                "Outbox event published",
                extra=log_context,
            )

            return True

        except Exception:
            db.rollback()

            logger.error(
                "Outbox publish failed",
                extra=log_context,
                exc_info=True,
            )

            raise

            
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
        published = publish_next_outbox_message(
            session_factory=session_factory,
            outbox_model=outbox_model,
            producer=producer,
        )

        if not published:
            time.sleep(
                poll_interval_seconds
            )