from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
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

from services.payment_service.db import Base


class Payment(Base):
    __tablename__ = "payments"

    __table_args__ = {
        "schema": "payment_service",
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

    amount: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
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