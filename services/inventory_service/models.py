from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
)

from services.inventory_service.db import Base


class InventoryReservation(Base):
    __tablename__ = "reservations"

    __table_args__ = {
        "schema": "inventory_service",
    }

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    order_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        unique=True,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    __table_args__ = {
        "schema": "inventory_service",
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
        server_default=func.now(),
        nullable=False,
    )

    published_at: Mapped[
        datetime | None
    ] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )