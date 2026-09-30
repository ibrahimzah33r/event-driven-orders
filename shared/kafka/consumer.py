import json
from collections.abc import Callable
import time
from confluent_kafka import Consumer, KafkaError


EventHandler = Callable[[dict], None]
FailureHandler = Callable[
    [dict, Exception, str, int, int],
    None,
]

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

    def subscribe(
        self,
        topics: str | list[str],
    ) -> None:
        if isinstance(topics, str):
            topics = [topics]

        self.consumer.subscribe(topics)

    
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

                print(
                    f"\nConsumed from "
                    f"{message.topic()} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )

                attempt = 0

                while True:
                    try:
                        if handler is None:
                            print(
                                json.dumps(
                                    event,
                                    indent=2,
                                )
                            )
                        else:
                            handler(event)

                        break

                    except Exception as exc:
                        if attempt >= max_retries:
                            print(
                                f"Processing failed after "
                                f"{max_retries} retries: "
                                f"{exc}"
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
                            * (2 ** attempt)
                        )

                        attempt += 1

                        print(
                            f"Processing failed: {exc}"
                        )

                        print(
                            f"Retry {attempt}/"
                            f"{max_retries} "
                            f"in {delay:.1f}s"
                        )

                        time.sleep(delay)

                self.consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    f"Committed offset "
                    f"{message.offset()}"
                )

        except KeyboardInterrupt:
            print("\nConsumer stopped.")

        finally:
            self.consumer.close()