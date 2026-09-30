from shared.events.models import Event
from shared.kafka.producer import KafkaProducer


class DeadLetterPublisher:
    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
    ):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
        )

    def publish(
        self,
        event: dict,
        error: Exception,
        source_topic: str,
        partition: int,
        offset: int,
    ) -> None:
        dead_letter_event = Event.create(
            event_type="dead_letter.created",
            data={
                "source_topic": source_topic,
                "source_partition": partition,
                "source_offset": offset,
                "error": str(error),
                "original_event": event,
            },
        )

        self.producer.publish(
            topic=self.topic,
            event=dead_letter_event,
            key=event.get("event_id"),
        )

        print(
            f"Event sent to dead-letter topic "
            f"{self.topic}"
        )