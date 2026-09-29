from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from uuid import uuid4


@dataclass
class Event:
    event_id: str
    event_type: str
    version: int
    occurred_at: str
    data: dict

    @classmethod
    def create(
        cls,
        event_type: str,
        data: dict,
        version: int = 1,
    ) -> "Event":
        return cls(
            event_id=str(uuid4()),
            event_type=event_type,
            version=version,
            occurred_at=datetime.now(UTC).isoformat(),
            data=data,
        )

    def to_dict(self) -> dict:
        return asdict(self)