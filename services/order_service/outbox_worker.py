from services.order_service.config import settings
from services.order_service.db import SyncSessionLocal
from services.order_service.models import OutboxMessage
from shared.outbox import run_outbox_publisher


if __name__ == "__main__":
    run_outbox_publisher(
        session_factory=SyncSessionLocal,
        outbox_model=OutboxMessage,
        bootstrap_servers=(
            settings.kafka_bootstrap_servers
        ),
    )