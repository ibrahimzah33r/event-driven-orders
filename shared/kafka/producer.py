import json

from confluent_kafka import Producer

from shared.events.models import Event


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
    ):
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def publish(
        self,
        topic: str,
        event: Event,
        key: str | None = None,
    ) -> None:
        payload = json.dumps(
            event.to_dict()
        )

        self.producer.produce(
            topic=topic,
            key=key or event.event_id,
            value=payload,
            callback=self._delivery_report,
        )

        remaining = self.producer.flush(
            timeout=10,
        )

        if remaining != 0:
            raise RuntimeError(
                f"{remaining} Kafka message(s) "
                "were not delivered"
            )

    @staticmethod
    def _delivery_report(
        error,
        message,
    ) -> None:
        if error is not None:
            print(
                f"Kafka delivery failed: {error}"
            )
            return

        print(
            f"Produced event to "
            f"{message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )