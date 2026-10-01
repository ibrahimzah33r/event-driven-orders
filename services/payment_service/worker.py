from decimal import Decimal

from services.payment_service.config import settings
from services.payment_service.db import SessionLocal
from services.payment_service.models import Payment
from shared.events.models import Event
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.kafka.dead_letter import DeadLetterPublisher
from shared.events.order_events import (
    parse_order_created,
)

consumer = KafkaConsumer(
    group_id="payment-service",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)


producer = KafkaProducer(
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

    with SessionLocal() as db:
        existing_payment = (
            db.query(Payment)
            .filter(
                Payment.order_id == order_id
            )
            .first()
        )

        if existing_payment is not None:
            print(
                f"Duplicate order event ignored: "
                f"payment already exists for "
                f"order {order_id}"
            )
            return

        payment = Payment(
            order_id=order_id,
            amount=Decimal(
                str(order_event.total)
            ),
            status="COMPLETED",
        )

        db.add(payment)
        db.commit()
        db.refresh(payment)

        payment_event = Event.create(
            event_type="payment.completed",
            data={
                "payment_id": payment.id,
                "order_id": payment.order_id,
                "amount": str(payment.amount),
                "status": payment.status,
            },
        )

        producer.publish(
            topic=settings.payment_events_topic,
            event=payment_event,
            key=str(payment.order_id),
        )

        print(
            f"Payment {payment.id} completed "
            f"for order {payment.order_id}"
        )
        
if __name__ == "__main__":
    consumer.subscribe(
        settings.order_events_topic
    )

    print(
        "Payment Service listening "
        "for order events..."
    )

    consumer.run(
        handler=handle_event,
        on_failure=dead_letter.publish,
        max_retries=3,
        initial_backoff_seconds=1.0,
    )