from services.inventory_service.config import settings
from services.inventory_service.db import SessionLocal
from services.inventory_service.models import OutboxMessage
from shared.outbox import run_outbox_publisher


if __name__ == "__main__":
    run_outbox_publisher(
        session_factory=SessionLocal,
        outbox_model=OutboxMessage,
        bootstrap_servers=(
            settings.kafka_bootstrap_servers
        ),
    )