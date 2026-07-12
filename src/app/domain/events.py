import json
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, field_validator
from uuid6 import uuid7

from app.core.config import get_settings

_settings = get_settings()
MAX_METADATA_BYTES = _settings.metadata_max_bytes
MAX_FUTURE_SKEW = timedelta(seconds=_settings.timestamp_max_future_skew_seconds)


def new_event_id() -> str:
    return str(uuid7())


class Event(BaseModel):
    event_id: str = Field(default_factory=new_event_id)
    event_type: str
    timestamp: datetime
    ingested_at: datetime | None = None
    user_id: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type")
    @classmethod
    def normalize_event_type(cls, value: str) -> str:
        value = value.strip().lower()
        if not value:
            raise ValueError("event_type must not be empty")
        return value

    @field_validator("timestamp")
    @classmethod
    def reject_future_event(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        else:
            value = value.astimezone(UTC)
        if value > datetime.now(UTC) + MAX_FUTURE_SKEW:
            raise ValueError(f"timestamp more than {MAX_FUTURE_SKEW} in the future")
        return value

    @field_validator("metadata")
    @classmethod
    def cap_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        size = len(json.dumps(value, separators=(",", ":")).encode())
        if size > MAX_METADATA_BYTES:
            raise ValueError(f"metadata exceeds {MAX_METADATA_BYTES} bytes")
        return value
