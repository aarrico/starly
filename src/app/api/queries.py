from datetime import UTC, datetime, timedelta
from enum import IntEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from app.api.schemas import (
    EventList,
    RealtimeStatsOut,
    SearchResults,
    StatsBucketOut,
    StatsList,
)
from app.cache.realtime import RealtimeStatsCache
from app.storage.es import EventSearchIndex
from app.storage.mongo import Bucket, EventRepository
from app.storage.types import EventFilters

router = APIRouter(prefix="/events", tags=["events"])


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

EventTypeParam = Annotated[str | None, Query(alias="type")]
SinceParam = Annotated[datetime | None, Query(alias="from")]
UntilParam = Annotated[datetime | None, Query(alias="to")]


def _as_utc(value: datetime | None) -> datetime | None:
    # Same policy as domain.Event: naive datetimes are UTC.
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _build_filters(
    event_type: str | None = None,
    user_id: str | None = None,
    source_url: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> EventFilters | None:
    if event_type:
        event_type = event_type.strip().lower()
    since = _as_utc(since)
    until = _as_utc(until)
    if not any((event_type, user_id, source_url, since, until)):
        return None
    return EventFilters(
        event_type=event_type,
        user_id=user_id,
        source_url=source_url,
        since=since,
        until=until,
    )


@router.get("")
async def list_events(
    repo: RepositoryDep,
    event_type: EventTypeParam = None,
    user_id: str | None = None,
    source_url: str | None = None,
    since: SinceParam = None,
    until: UntilParam = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
) -> EventList:
    filters = _build_filters(event_type, user_id, source_url, since, until)
    events = await repo.find(filters=filters, limit=limit, offset=offset)
    return EventList(events=events)


@router.get("/search")
async def search_events(
    index: SearchIndexDep,
    q: Annotated[str, Query(min_length=1, max_length=1024)],
    event_type: EventTypeParam = None,
    user_id: str | None = None,
    source_url: str | None = None,
    since: SinceParam = None,
    until: UntilParam = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> SearchResults:
    filters = _build_filters(event_type, user_id, source_url, since, until)
    result = await index.search(q, filters=filters, size=limit)
    return SearchResults(events=result.hits, total=result.total)


@router.get("/stats/realtime")
async def realtime_stats(
    repo: RepositoryDep,
    cache: CacheDep,
    window: Annotated[RealtimeWindow, Query()] = RealtimeWindow.FIVE_MINUTES,
) -> RealtimeStatsOut:
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
    repo: RepositoryDep,
    bucket: Bucket,
    event_type: EventTypeParam = None,
    since: SinceParam = None,
    until: UntilParam = None,
) -> StatsList:
    filters = _build_filters(event_type, since=since, until=until)
    buckets = await repo.stats(bucket, filters=filters)
    return StatsList(
        stats=[
            StatsBucketOut(
                event_type=b.event_type, bucket_start=b.bucket_start, count=b.count
            )
            for b in buckets
        ]
    )
