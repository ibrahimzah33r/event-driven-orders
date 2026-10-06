import json
import logging
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": getattr(
                record,
                "service",
                "unknown-service",
            ),
            "message": record.getMessage(),
        }

        for field in (
            "event_id",
            "event_type",
            "order_id",
            "topic",
            "partition",
            "offset",
        ):
            value = getattr(record, field, None)

            if value is not None:
                log_data[field] = value

        if record.exc_info:
            log_data["exception"] = (
                self.formatException(record.exc_info)
            )

        return json.dumps(
            log_data,
            default=str,
        )


def get_logger(
    service_name: str,
) -> logging.LoggerAdapter:
    logger = logging.getLogger(service_name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())

        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logging.LoggerAdapter(
        logger,
        {
            "service": service_name,
        },
        merge_extra=True,
    )