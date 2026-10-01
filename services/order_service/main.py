from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.order_service.db import get_db
from services.order_service.models import (
    Order,
    OutboxMessage,
)
from services.order_service.schemas import (
    OrderCreate,
    OrderRead,
)
from shared.events.models import Event


app = FastAPI(
    title="Order Service",
    version="0.1.0",
)


DatabaseSession = Annotated[
    AsyncSession,
    Depends(get_db),
]


@app.get("/health")
async def health(
    db: DatabaseSession,
) -> dict[str, str]:
    await db.execute(
        text("SELECT 1")
    )

    return {
        "status": "healthy",
        "database": "connected",
    }


@app.post(
    "/orders",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_order(
    payload: OrderCreate,
    db: DatabaseSession,
) -> Order:
    order = Order(
        customer_id=payload.customer_id,
        total=payload.total,
    )

    db.add(order)

    await db.flush()

    event = Event.create(
        event_type="order.created",
        version=2,
        data={
            "order_id": order.id,
            "customer_id": order.customer_id,
            "total": str(order.total),
            "currency": "GBP",
        },
    )

    outbox_message = OutboxMessage(
        event_id=event.event_id,
        topic="orders.events",
        event_key=str(order.id),
        payload=event.to_dict(),
    )

    db.add(outbox_message)

    await db.commit()
    await db.refresh(order)

    return order


@app.get(
    "/orders/{order_id}",
    response_model=OrderRead,
)
async def get_order(
    order_id: int,
    db: DatabaseSession,
) -> Order:
    order = await db.get(
        Order,
        order_id,
    )

    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found",
        )

    return order