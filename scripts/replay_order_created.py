import sys

from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


if len(sys.argv) != 4:
    raise SystemExit(
        "Usage: python -m scripts.replay_order_created "
        "<order_id> <customer_id> <total>"
    )


order_id = int(sys.argv[1])
customer_id = int(sys.argv[2])
total = sys.argv[3]


producer = KafkaProducer()


event = Event.create(
    event_type="order.created",
    data={
        "order_id": order_id,
        "customer_id": customer_id,
        "total": total,
    },
)


producer.publish(
    topic="orders.events",
    event=event,
    key=str(order_id),
)