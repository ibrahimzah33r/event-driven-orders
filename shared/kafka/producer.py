import json

from confluent_kafka import Producer

from shared.events.models import Event


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def publish(
        self,
        topic: str,
        event: Event,
    ) -> None:
        payload = json.dumps(event.to_dict())

        self.producer.produce(
            topic=topic,
            key=event.event_id,
            value=payload,
            callback=self._delivery_report,
        )

        self.producer.flush()

    @staticmethod
    def _delivery_report(error, message) -> None:
        if error is not None:
            raise RuntimeError(f"Message delivery failed: {error}")

        print(
            f"Produced event to "
            f"{message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )