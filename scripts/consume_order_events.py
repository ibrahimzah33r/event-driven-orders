from shared.kafka.consumer import KafkaConsumer


consumer = KafkaConsumer(
    group_id="order-event-demo",
)

consumer.subscribe("orders.events")
consumer.run()