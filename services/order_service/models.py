from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
)

from services.order_service.db import Base


class Order(Base):
    __tablename__ = "orders"

    __table_args__ = {
        "schema": "order_service",
    }

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    customer_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    total: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="CREATED",
        server_default="CREATED",
    )

    payment_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="PENDING",
        server_default="PENDING",
    )

    inventory_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="PENDING",
        server_default="PENDING",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    __table_args__ = {
        "schema": "order_service",
    }

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    event_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        unique=True,
    )

    topic: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    event_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    payload: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    published_at: Mapped[
        datetime | None
    ] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )