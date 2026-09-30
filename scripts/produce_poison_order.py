from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


producer = KafkaProducer()


event = Event.create(
    event_type="order.created",
    data={
        "order_id": 999999,
        "customer_id": 999,
        "total": "definitely-not-a-number",
    },
)


producer.publish(
    topic="orders.events",
    event=event,
    key="999999",
)