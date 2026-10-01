from services.inventory_service.config import settings
from services.inventory_service.db import SessionLocal
from services.inventory_service.models import InventoryReservation
from shared.events.models import Event
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.events.order_events import parse_order_created

consumer = KafkaConsumer(
    group_id="inventory-service",
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


producer = KafkaProducer(
    bootstrap_servers=settings.kafka_bootstrap_servers,
)

dead_letter = DeadLetterPublisher(
    topic="inventory-service.dlq",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)

def handle_event(event: dict) -> None:
    if event["event_type"] != "order.created":
        return

    order_event = parse_order_created(event)
    order_id = order_event.order_id

    with SessionLocal() as db:
        existing_reservation = (
            db.query(InventoryReservation)
            .filter(
                InventoryReservation.order_id
                == order_id
            )
            .first()
        )

        if existing_reservation is not None:
            print(
                f"Duplicate order event ignored: "
                f"inventory already reserved "
                f"for order {order_id}"
            )
            return

        reservation = InventoryReservation(
            order_id=order_id,
            status="RESERVED",
        )

        db.add(reservation)
        db.commit()
        db.refresh(reservation)

        inventory_event = Event.create(
            event_type="inventory.reserved",
            data={
                "reservation_id": reservation.id,
                "order_id": reservation.order_id,
                "status": reservation.status,
            },
        )

        producer.publish(
            topic=settings.inventory_events_topic,
            event=inventory_event,
            key=str(reservation.order_id),
        )

        print(
            f"Inventory reserved for "
            f"order {reservation.order_id}"
        )

if __name__ == "__main__":
    consumer.subscribe(
        settings.order_events_topic
    )

    print(
        "Inventory Service listening "
        "for order events..."
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )