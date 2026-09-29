from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


producer = KafkaProducer()

event = Event.create(
    event_type="order.created",
    data={
        "order_id": 123,
        "customer_id": 42,
        "total": 59.99,
    },
)

producer.publish(
    topic="orders.events",
    event=event,
)