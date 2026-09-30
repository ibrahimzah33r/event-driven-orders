import json
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError


EventHandler = Callable[[dict], None]


class KafkaConsumer:
    def __init__(
        self,
        group_id: str,
        bootstrap_servers: str = "localhost:9092",
    ):
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def subscribe(self, topic: str) -> None:
        self.consumer.subscribe([topic])

    def run(
        self,
        handler: EventHandler | None = None,
    ) -> None:
        try:
            while True:
                message = self.consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    if (
                        message.error().code()
                        == KafkaError._PARTITION_EOF
                    ):
                        continue

                    raise RuntimeError(
                        message.error()
                    )

                event = json.loads(
                    message.value().decode("utf-8")
                )

                print(
                    f"\nConsumed from "
                    f"{message.topic()} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )

                if handler is None:
                    print(
                        json.dumps(
                            event,
                            indent=2,
                        )
                    )
                else:
                    handler(event)

                self.consumer.commit(
                    message=message,
                    asynchronous=False,
                )

        except KeyboardInterrupt:
            print("\nConsumer stopped.")

        finally:
            self.consumer.close()