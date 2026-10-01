import time
from datetime import UTC, datetime

from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


def run_outbox_publisher(
    session_factory,
    outbox_model,
    bootstrap_servers: str,
    poll_interval_seconds: float = 1.0,
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
    )

    print("Outbox publisher started.")

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

            try:
                event = Event(
                    **message.payload
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

                print(
                    f"Published outbox event "
                    f"{message.event_id} "
                    f"to {message.topic}"
                )

            except Exception as exc:
                db.rollback()

                print(
                    f"Outbox publish failed: "
                    f"{exc}"
                )

                time.sleep(
                    poll_interval_seconds
                )