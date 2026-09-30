import asyncio
from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.order_service.config import settings
from services.order_service.db import get_db
from services.order_service.models import Order
from services.order_service.schemas import (
    OrderCreate,
    OrderRead,
)
from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


app = FastAPI(
    title="Order Service",
    version="0.1.0",
)


DatabaseSession = Annotated[
    AsyncSession,
    Depends(get_db),
]


kafka_producer = KafkaProducer(
    bootstrap_servers=settings.kafka_bootstrap_servers,
)


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

    await db.commit()
    await db.refresh(order)

    event = Event.create(
        event_type="order.created",
        data={
            "order_id": order.id,
            "customer_id": order.customer_id,
            "total": str(order.total),
        },
    )

    await asyncio.to_thread(
        kafka_producer.publish,
        "orders.events",
        event,
        str(order.id),
    )

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