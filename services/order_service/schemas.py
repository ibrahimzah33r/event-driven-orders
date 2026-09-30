from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    customer_id: int = Field(gt=0)

    total: Decimal = Field(
        gt=0,
        max_digits=12,
        decimal_places=2,
    )


class OrderRead(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
    )

    id: int
    customer_id: int
    total: Decimal
    status: str
    payment_status: str
    inventory_status: str
    created_at: datetime