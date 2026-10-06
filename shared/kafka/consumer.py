import json
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError

from shared.logging_config import get_logger


EventHandler = Callable[[dict], None]

FailureHandler = Callable[
    [dict, Exception, str, int, int],
    None,
]


class KafkaConsumer:
    def __init__(
        self,
        group_id: str,
        bootstrap_servers: str,
    ) -> None:
        self.group_id = group_id

        self.logger = get_logger(
            f"kafka-consumer:{group_id}"
        )

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def subscribe(
        self,
        topics: str | list[str],
    ) -> None:
        if isinstance(topics, str):
            topics = [topics]

        self.consumer.subscribe(topics)

        self.logger.info(
            "Kafka consumer subscribed",
            extra={
                "topic": ",".join(topics),
            },
        )

    def run(
        self,
        handler: EventHandler | None = None,
        on_failure: FailureHandler | None = None,
        max_retries: int = 3,
        initial_backoff_seconds: float = 1.0,
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

                event_id = event.get("event_id")
                event_type = event.get("event_type")

                order_id = (
                    event.get("data", {})
                    .get("order_id")
                )

                log_context = {
                    "event_id": event_id,
                    "event_type": event_type,
                    "order_id": order_id,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }

                self.logger.info(
                    "Kafka event consumed",
                    extra=log_context,
                )

                attempt = 0

                while True:
                    try:
                        if handler is not None:
                            handler(event)

                        break

                    except Exception as exc:
                        attempt += 1

                        if attempt >= max_retries:
                            self.logger.error(
                                "Kafka event processing failed",
                                extra=log_context,
                                exc_info=True,
                            )

                            if on_failure is None:
                                raise

                            on_failure(
                                event,
                                exc,
                                message.topic(),
                                message.partition(),
                                message.offset(),
                            )

                            break

                        delay = (
                            initial_backoff_seconds
                            * (2 ** (attempt - 1))
                        )

                        self.logger.warning(
                            "Retrying Kafka event",
                            extra=log_context,
                        )

                        time.sleep(delay)

                self.consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                self.logger.info(
                    "Kafka offset committed",
                    extra=log_context,
                )

        except KeyboardInterrupt:
            self.logger.info(
                "Kafka consumer stopping"
            )

        finally:
            self.consumer.close()