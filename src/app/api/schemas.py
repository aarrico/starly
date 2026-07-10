from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.domain.events import Event


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


class RealtimeStatsOut(BaseModel):
    window_seconds: int
    total: int
    counts_by_type: dict[str, int]
    computed_at: datetime


class DLQEntryOut(BaseModel):
    message_id: str
    receive_count: int
    error: str
    body: dict[str, Any]


class DLQList(BaseModel):
    entries: list[DLQEntryOut]
    total: int


class ReadinessOut(BaseModel):
    status: Literal["ready", "degraded"]
    dependencies: dict[str, str]
