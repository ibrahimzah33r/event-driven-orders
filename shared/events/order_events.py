from decimal import Decimal

from pydantic import BaseModel


class OrderCreatedV1(BaseModel):
    order_id: int
    customer_id: int
    total: Decimal


class OrderCreatedV2(BaseModel):
    order_id: int
    customer_id: int
    total: Decimal
    currency: str


def parse_order_created(
    event: dict,
) -> OrderCreatedV1 | OrderCreatedV2:
    version = event["version"]
    data = event["data"]

    if version == 1:
        return OrderCreatedV1.model_validate(data)

    if version == 2:
        return OrderCreatedV2.model_validate(data)

    raise ValueError(
        f"Unsupported order.created "
        f"version: {version}"
    )