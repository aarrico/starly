from datetime import UTC, datetime, timedelta
from enum import IntEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.api.schemas import (
    EventList,
    RealtimeStatsOut,
    SearchResults,
    StatsBucketOut,
    StatsList,
)
from app.cache.realtime import RealtimeStatsCache
from app.core.config import get_settings
from app.storage.es import EventSearchIndex
from app.storage.mongo import Bucket, EventRepository
from app.storage.types import EventFilters

router = APIRouter(prefix="/events", tags=["events"])

_settings = get_settings()


class RealtimeWindow(IntEnum):
    ONE_MINUTE = 60
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900


def get_repository(request: Request) -> EventRepository:
    return request.app.state.repository


def get_search_index(request: Request) -> EventSearchIndex:
    return request.app.state.search_index


def get_cache(request: Request) -> RealtimeStatsCache:
    return request.app.state.cache


RepositoryDep = Annotated[EventRepository, Depends(get_repository)]
SearchIndexDep = Annotated[EventSearchIndex, Depends(get_search_index)]
CacheDep = Annotated[RealtimeStatsCache, Depends(get_cache)]


def _as_utc(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _normalize(filters: EventFilters) -> EventFilters | None:
    if filters.event_type:
        filters.event_type = filters.event_type.strip().lower()
    filters.since = _as_utc(filters.since)
    filters.until = _as_utc(filters.until)
    if not any(
        (
            filters.event_type,
            filters.user_id,
            filters.source_url,
            filters.since,
            filters.until,
        )
    ):
        return None
    return filters


class EventFilterParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str | None = Field(None, alias="type")
    user_id: str | None = None
    source_url: str | None = None
    since: datetime | None = Field(None, alias="from")
    until: datetime | None = Field(None, alias="to")

    def to_filters(self) -> EventFilters | None:
        return _normalize(
            EventFilters(
                event_type=self.event_type,
                user_id=self.user_id,
                source_url=self.source_url,
                since=self.since,
                until=self.until,
            )
        )


class ListEventsParams(EventFilterParams):
    limit: int = Field(
        _settings.query_default_limit, ge=1, le=_settings.query_max_limit
    )
    offset: int = Field(0, ge=0, le=_settings.query_max_offset)


class SearchParams(EventFilterParams):
    q: str = Field(min_length=1, max_length=1024)
    limit: int = Field(
        _settings.query_default_limit, ge=1, le=_settings.search_max_size
    )


class StatsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bucket: Bucket
    event_type: str | None = Field(None, alias="type")
    since: datetime | None = Field(None, alias="from")
    until: datetime | None = Field(None, alias="to")

    def to_filters(self) -> EventFilters | None:
        return _normalize(
            EventFilters(event_type=self.event_type, since=self.since, until=self.until)
        )


class RealtimeParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    window: RealtimeWindow = RealtimeWindow.FIVE_MINUTES


@router.get("")
async def list_events(
    repo: RepositoryDep, params: Annotated[ListEventsParams, Query()]
) -> EventList:
    events = await repo.find(
        filters=params.to_filters(), limit=params.limit, offset=params.offset
    )
    return EventList(events=events)


@router.get("/search")
async def search_events(
    index: SearchIndexDep, params: Annotated[SearchParams, Query()]
) -> SearchResults:
    result = await index.search(
        params.q, filters=params.to_filters(), size=params.limit
    )
    return SearchResults(events=result.hits, total=result.total)


@router.get("/stats/realtime")
async def realtime_stats(
    repo: RepositoryDep,
    cache: CacheDep,
    params: Annotated[RealtimeParams, Query()],
) -> RealtimeStatsOut:
    window = params.window
    snapshot = await cache.get_or_compute(
        int(window), lambda: repo.realtime_summary(timedelta(seconds=int(window)))
    )
    return RealtimeStatsOut(
        window_seconds=snapshot.window_seconds,
        total=snapshot.total,
        counts_by_type=snapshot.counts_by_type,
        computed_at=snapshot.computed_at,
    )


@router.get("/stats")
async def event_stats(
    repo: RepositoryDep, params: Annotated[StatsParams, Query()]
) -> StatsList:
    buckets = await repo.stats(params.bucket, filters=params.to_filters())
    return StatsList(
        stats=[
            StatsBucketOut(
                event_type=b.event_type, bucket_start=b.bucket_start, count=b.count
            )
            for b in buckets
        ]
    )
