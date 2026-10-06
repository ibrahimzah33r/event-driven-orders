import json
import time
from urllib.request import Request, urlopen

from sqlalchemy import func

from services.inventory_service.db import SessionLocal as InventorySession
from services.inventory_service.models import InventoryReservation
from services.payment_service.db import SessionLocal as PaymentSession
from services.payment_service.models import Payment
from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


API_URL = "http://127.0.0.1:8000"


def create_order() -> dict:
    payload = json.dumps(
        {
            "customer_id": 1100,
            "total": 300.00,
        }
    ).encode("utf-8")

    request = Request(
        f"{API_URL}/orders",
        data=payload,
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urlopen(request, timeout=5) as response:
        return json.loads(
            response.read().decode("utf-8")
        )


def get_order(order_id: int) -> dict:
    with urlopen(
        f"{API_URL}/orders/{order_id}",
        timeout=5,
    ) as response:
        return json.loads(
            response.read().decode("utf-8")
        )


def wait_for_confirmation(
    order_id: int,
    timeout_seconds: int = 20,
) -> dict:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        order = get_order(order_id)

        if order["status"] == "CONFIRMED":
            return order

        time.sleep(0.5)

    raise AssertionError(
        f"Order {order_id} was not confirmed in time"
    )


def test_duplicate_order_created_is_idempotent() -> None:
    created_order = create_order()

    order_id = created_order["id"]

    confirmed_order = wait_for_confirmation(
        order_id
    )

    duplicate_event = Event.create(
        event_type="order.created",
        version=2,
        data={
            "order_id": order_id,
            "customer_id": confirmed_order["customer_id"],
            "total": confirmed_order["total"],
            "currency": "GBP",
        },
    )

    producer = KafkaProducer()

    # Publish the exact same event twice.
    producer.publish(
        topic="orders.events",
        event=duplicate_event,
        key=str(order_id),
    )

    producer.publish(
        topic="orders.events",
        event=duplicate_event,
        key=str(order_id),
    )

    # Give both consumers time to receive the duplicates.
    time.sleep(3)

    with PaymentSession() as db:
        payment_count = (
            db.query(func.count(Payment.id))
            .filter(
                Payment.order_id == order_id
            )
            .scalar()
        )

    with InventorySession() as db:
        reservation_count = (
            db.query(
                func.count(
                    InventoryReservation.id
                )
            )
            .filter(
                InventoryReservation.order_id
                == order_id
            )
            .scalar()
        )

    assert payment_count == 1
    assert reservation_count == 1