from services.order_service.config import settings
from services.order_service.db import SyncSessionLocal
from services.order_service.models import Order
from shared.kafka.consumer import KafkaConsumer


consumer = KafkaConsumer(
    group_id="order-service-status",
    bootstrap_servers=(
        settings.kafka_bootstrap_servers
    ),
)


def handle_event(event: dict) -> None:
    event_type = event["event_type"]
    data = event["data"]

    if event_type not in {
        "payment.completed",
        "inventory.reserved",
    }:
        return

    order_id = data["order_id"]

    with SyncSessionLocal() as db:
        order = db.get(
            Order,
            order_id,
        )

        if order is None:
            print(
                f"Order {order_id} not found"
            )
            return

        if event_type == "payment.completed":
            order.payment_status = "COMPLETED"

        if event_type == "inventory.reserved":
            order.inventory_status = "RESERVED"

        if (
            order.payment_status == "COMPLETED"
            and order.inventory_status == "RESERVED"
        ):
            order.status = "CONFIRMED"

        db.commit()

        print(
            f"Order {order.id}: "
            f"status={order.status}, "
            f"payment={order.payment_status}, "
            f"inventory={order.inventory_status}"
        )


if __name__ == "__main__":
    consumer.subscribe(
        [
            "payments.events",
            "inventory.events",
        ]
    )

    print(
        "Order Service worker listening "
        "for payment and inventory events..."
    )

    consumer.run(
        handler=handle_event
    )