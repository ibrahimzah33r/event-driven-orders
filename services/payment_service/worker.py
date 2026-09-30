from decimal import Decimal

from services.payment_service.config import settings
from services.payment_service.db import SessionLocal
from services.payment_service.models import Payment
from shared.events.models import Event
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer


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


def handle_event(event: dict) -> None:
    if event["event_type"] != "order.created":
        return

    data = event["data"]

    with SessionLocal() as db:
        payment = Payment(
            order_id=data["order_id"],
            amount=Decimal(
                str(data["total"])
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
        handler=handle_event
    )