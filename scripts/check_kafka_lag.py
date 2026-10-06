from confluent_kafka import (
    ConsumerGroupTopicPartitions,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    OffsetSpec,
)

from services.order_service.config import settings


GROUP_IDS = [
    "payment-service",
    "inventory-service",
    "order-service-status",
    "notification-service",
]


def get_group_lag(
    admin: AdminClient,
    group_id: str,
) -> list[dict]:
    request = [
        ConsumerGroupTopicPartitions(group_id)
    ]

    futures = admin.list_consumer_group_offsets(
        request,
        request_timeout=10,
    )

    result = futures[group_id].result()

    committed_partitions = (
        result.topic_partitions or []
    )

    if not committed_partitions:
        return []

    latest_requests = {}

    for partition in committed_partitions:
        if partition.error is not None:
            continue

        latest_requests[
            TopicPartition(
                partition.topic,
                partition.partition,
            )
        ] = OffsetSpec.latest()

    if not latest_requests:
        return []

    latest_futures = admin.list_offsets(
        latest_requests,
        request_timeout=10,
    )

    latest_offsets = {}

    for partition, future in latest_futures.items():
        latest = future.result()

        latest_offsets[
            (
                partition.topic,
                partition.partition,
            )
        ] = latest.offset

    rows = []

    for partition in committed_partitions:
        if partition.error is not None:
            continue

        committed = partition.offset

        if committed < 0:
            continue

        latest = latest_offsets.get(
            (
                partition.topic,
                partition.partition,
            )
        )

        if latest is None:
            continue

        lag = max(
            latest - committed,
            0,
        )

        rows.append(
            {
                "group": group_id,
                "topic": partition.topic,
                "partition": partition.partition,
                "committed": committed,
                "latest": latest,
                "lag": lag,
            }
        )

    return rows


def main() -> None:
    admin = AdminClient(
        {
            "bootstrap.servers":
                settings.kafka_bootstrap_servers,
        }
    )

    print()
    print("Kafka consumer lag")
    print("=" * 72)

    total_lag = 0

    for group_id in GROUP_IDS:
        rows = get_group_lag(
            admin,
            group_id,
        )

        if not rows:
            print(
                f"{group_id}: "
                "no committed offsets found"
            )
            continue

        for row in rows:
            total_lag += row["lag"]

            print(
                f'{row["group"]:<24} '
                f'{row["topic"]:<22} '
                f'partition={row["partition"]} '
                f'committed={row["committed"]} '
                f'latest={row["latest"]} '
                f'lag={row["lag"]}'
            )

    print("=" * 72)
    print(f"Total lag: {total_lag}")
    print()


if __name__ == "__main__":
    main()