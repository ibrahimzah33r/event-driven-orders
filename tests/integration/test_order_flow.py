import json
import time
from urllib.request import Request, urlopen

from services.notification_service.db import SessionLocal
from services.notification_service.models import Notification


API_URL = "http://127.0.0.1:8000"


def create_order() -> dict:
    payload = json.dumps(
        {
            "customer_id": 1001,
            "total": 250.00,
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

    with urlopen(
        request,
        timeout=5,
    ) as response:
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
        f"Order {order_id} was not confirmed "
        f"within {timeout_seconds} seconds"
    )


def test_complete_order_flow() -> None:
    created_order = create_order()

    order_id = created_order["id"]

    confirmed_order = wait_for_confirmation(
        order_id
    )

    assert (
        confirmed_order["payment_status"]
        == "COMPLETED"
    )

    assert (
        confirmed_order["inventory_status"]
        == "RESERVED"
    )

    assert (
        confirmed_order["status"]
        == "CONFIRMED"
    )

    with SessionLocal() as db:
        notification = (
            db.query(Notification)
            .filter(
                Notification.order_id == order_id
            )
            .first()
        )

        assert notification is not None
        assert notification.status == "SENT"