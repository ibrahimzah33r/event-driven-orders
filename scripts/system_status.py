import json
from urllib.error import URLError
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient

from scripts.check_kafka_lag import (
    GROUP_IDS,
    get_group_lag,
)
from services.order_service.config import settings


API_BASE_URL = "http://127.0.0.1:8000"


def get_health(path: str) -> dict:
    try:
        with urlopen(
            f"{API_BASE_URL}{path}",
            timeout=3,
        ) as response:
            return json.loads(
                response.read().decode("utf-8")
            )

    except URLError as exc:
        return {
            "status": "unreachable",
            "error": str(exc),
        }


def main() -> None:
    print()
    print("Event-Driven Orders — System Status")
    print("=" * 72)

    live = get_health("/health/live")
    ready = get_health("/health/ready")

    print()
    print("API")
    print("-" * 72)

    print(
        "Liveness: ",
        live.get("status"),
    )

    print(
        "Readiness:",
        ready.get("status"),
    )

    checks = ready.get("checks", {})

    if checks:
        print(
            "Postgres: ",
            "OK" if checks.get("postgres") else "FAIL",
        )

        print(
            "Kafka:    ",
            "OK" if checks.get("kafka") else "FAIL",
        )

    print()
    print("Kafka Consumer Lag")
    print("-" * 72)

    admin = AdminClient(
        {
            "bootstrap.servers":
                settings.kafka_bootstrap_servers,
        }
    )

    total_lag = 0

    for group_id in GROUP_IDS:
        rows = get_group_lag(
            admin,
            group_id,
        )

        if not rows:
            print(
                f"{group_id:<24} "
                "no committed offsets"
            )
            continue

        group_lag = sum(
            row["lag"]
            for row in rows
        )

        total_lag += group_lag

        print(
            f"{group_id:<24} "
            f"lag={group_lag}"
        )

    print()
    print("-" * 72)
    print(f"Total consumer lag: {total_lag}")

    healthy = (
        live.get("status") == "alive"
        and ready.get("status") == "ready"
        and total_lag == 0
    )

    print()
    print(
        "Overall:",
        "HEALTHY" if healthy else "ATTENTION NEEDED",
    )

    print()


if __name__ == "__main__":
    main()