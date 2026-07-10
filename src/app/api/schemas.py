from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.domain.events import Event


class EventIn(BaseModel):
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EventAccepted(BaseModel):
    event_id: str
    status: Literal["queued"] = "queued"


class EventList(BaseModel):
    events: list[Event]


class SearchResults(BaseModel):
    events: list[Event]
    total: int


class StatsBucketOut(BaseModel):
    event_type: str
    bucket_start: datetime
    count: int


class StatsList(BaseModel):
    stats: list[StatsBucketOut]
