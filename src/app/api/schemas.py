from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EventAccepted(BaseModel):
    event_id: str
    status: str = "queued"
