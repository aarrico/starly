import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest
from elastic_transport import ConnectionError as ESConnectionError
from pymongo.errors import ServerSelectionTimeoutError

from app.cache.realtime import RealtimeSnapshot, RealtimeStatsCache
from app.domain.events import Event
from app.main import create_app
from app.queue.protocol import MAX_BATCH_SIZE, Message
from app.storage.es import EventSearchIndex, SearchResult
from app.storage.mongo import Bucket, EventRepository, StatsBucket
from app.storage.types import EventFilters


class IdleQueue:
    async def send(self, body: dict[str, Any]) -> Message:
        return Message(id="1", body=body)

    async def receive_batch(
        self, max_n: int = MAX_BATCH_SIZE, wait: float = 0.0
    ) -> list[Message]:
        await asyncio.sleep(wait)
        return []

    async def ack(self, message: Message) -> None:
        pass

    async def nack(self, message: Message, error: str) -> None:
        pass

    async def reject(self, message: Message, error: str) -> None:
        pass


class FakeRepository:
    def __init__(
        self,
        events: list[Event] | None = None,
        buckets: list[StatsBucket] | None = None,
    ) -> None:
        self.events = events or []
        self.buckets = buckets or []
        self.find_calls: list[tuple[EventFilters | None, int, int]] = []
        self.stats_calls: list[tuple[Bucket, EventFilters | None]] = []

    async def find(
        self,
        filters: EventFilters | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Event]:
        self.find_calls.append((filters, limit, offset))
        return self.events

    async def stats(
        self, bucket: Bucket, filters: EventFilters | None = None
    ) -> list[StatsBucket]:
        self.stats_calls.append((bucket, filters))
        return self.buckets


class DownRepository(FakeRepository):
    async def find(
        self,
        filters: EventFilters | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Event]:
        raise ServerSelectionTimeoutError("mongo down")

    async def stats(
        self, bucket: Bucket, filters: EventFilters | None = None
    ) -> list[StatsBucket]:
        raise ServerSelectionTimeoutError("mongo down")


class UntouchedStore:
    async def upsert_many(self, events):
        raise AssertionError("store must not be reached in API unit tests")

    index_many = upsert_many


class FakeSearchIndex:
    def __init__(self, hits: list[Event] | None = None, total: int | None = None):
        self.hits = hits or []
        self.total = len(self.hits) if total is None else total
        self.search_calls: list[tuple[str, EventFilters | None, int]] = []

    async def search(
        self, q: str, filters: EventFilters | None = None, size: int = 50
    ) -> SearchResult:
        self.search_calls.append((q, filters, size))
        return SearchResult(hits=self.hits, total=self.total)


class DownSearchIndex(FakeSearchIndex):
    async def search(
        self, q: str, filters: EventFilters | None = None, size: int = 50
    ) -> SearchResult:
        raise ESConnectionError("es down")


class FakeCache:
    def __init__(self, snapshot: RealtimeSnapshot | None = None) -> None:
        self.snapshot = snapshot
        self.windows: list[int] = []

    async def get_or_compute(self, window, compute) -> RealtimeSnapshot:
        self.windows.append(window)
        assert self.snapshot is not None, "cache must not be reached"
        return self.snapshot


@asynccontextmanager
async def api_client(
    repo: FakeRepository,
    search: FakeSearchIndex | UntouchedStore | None = None,
    cache: FakeCache | None = None,
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        queue=IdleQueue(),
        repository=cast(EventRepository, repo),
        search_index=cast(EventSearchIndex, search or UntouchedStore()),
        cache=cast(RealtimeStatsCache, cache or FakeCache()),
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


def make_event(**overrides: Any) -> Event:
    fields: dict[str, Any] = {
        "event_type": "pageview",
        "timestamp": datetime(2026, 7, 9, 12, 0, tzinfo=UTC),
        "ingested_at": datetime(2026, 7, 9, 12, 0, 1, tzinfo=UTC),
        "user_id": "u_1",
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox"},
    }
    fields.update(overrides)
    return Event(**fields)


class TestListEvents:
    async def test_returns_events_envelope_with_full_event_fields(self):
        event = make_event()
        async with api_client(FakeRepository(events=[event])) as client:
            resp = await client.get("/events")

        assert resp.status_code == 200
        [returned] = resp.json()["events"]
        assert returned["event_id"] == event.event_id
        assert returned["event_type"] == "pageview"
        assert returned["timestamp"] == "2026-07-09T12:00:00Z"
        assert returned["ingested_at"] == "2026-07-09T12:00:01Z"
        assert returned["user_id"] == "u_1"
        assert returned["source_url"] == "https://example.com/pricing"
        assert returned["metadata"] == {"browser": "firefox"}

    async def test_default_paging_and_no_filters(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get("/events")

        assert resp.status_code == 200
        assert resp.json() == {"events": []}
        [(filters, limit, offset)] = repo.find_calls
        assert filters is None
        assert (limit, offset) == (50, 0)

    async def test_filters_are_translated_to_repository(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get(
                "/events",
                params={
                    "type": "PageView",
                    "user_id": "u_1",
                    "source_url": "https://example.com/pricing",
                    "from": "2026-07-01T00:00:00Z",
                    "to": "2026-07-09T00:00:00Z",
                    "limit": 500,
                    "offset": 10_000,
                },
            )

        assert resp.status_code == 200
        [(filters, limit, offset)] = repo.find_calls
        assert filters == EventFilters(
            event_type="pageview",
            user_id="u_1",
            source_url="https://example.com/pricing",
            since=datetime(2026, 7, 1, tzinfo=UTC),
            until=datetime(2026, 7, 9, tzinfo=UTC),
        )
        assert (limit, offset) == (500, 10_000)

    async def test_naive_datetime_filters_are_treated_as_utc(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get(
                "/events",
                params={"from": "2026-07-01T00:00:00", "to": "2026-07-09T00:00:00"},
            )

        assert resp.status_code == 200
        [(filters, _, _)] = repo.find_calls
        assert filters.since == datetime(2026, 7, 1, tzinfo=UTC)
        assert filters.until == datetime(2026, 7, 9, tzinfo=UTC)

    async def test_limit_over_cap_returns_422_envelope(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get("/events", params={"limit": 501})

        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
        assert repo.find_calls == []

    async def test_offset_over_cap_returns_422(self):
        async with api_client(FakeRepository()) as client:
            resp = await client.get("/events", params={"offset": 10_001})

        assert resp.status_code == 422

    async def test_zero_limit_returns_422(self):
        async with api_client(FakeRepository()) as client:
            resp = await client.get("/events", params={"limit": 0})

        assert resp.status_code == 422

    async def test_unparseable_date_returns_422(self):
        async with api_client(FakeRepository()) as client:
            resp = await client.get("/events", params={"from": "not-a-date"})

        assert resp.status_code == 422

    async def test_unknown_filter_param_returns_422(self):
        # A mistyped filter must fail noisy, never silently return the
        # unfiltered superset.
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get("/events", params={"event_type": "smoke"})

        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
        assert repo.find_calls == []


class TestStats:
    async def test_returns_stats_envelope(self):
        buckets = [
            StatsBucket(
                event_type="pageview",
                bucket_start=datetime(2026, 7, 9, tzinfo=UTC),
                count=3,
            ),
            StatsBucket(
                event_type="signup",
                bucket_start=datetime(2026, 7, 9, tzinfo=UTC),
                count=1,
            ),
        ]
        repo = FakeRepository(buckets=buckets)
        async with api_client(repo) as client:
            resp = await client.get("/events/stats", params={"bucket": "day"})

        assert resp.status_code == 200
        assert resp.json() == {
            "stats": [
                {
                    "event_type": "pageview",
                    "bucket_start": "2026-07-09T00:00:00Z",
                    "count": 3,
                },
                {
                    "event_type": "signup",
                    "bucket_start": "2026-07-09T00:00:00Z",
                    "count": 1,
                },
            ]
        }
        [(bucket, filters)] = repo.stats_calls
        assert bucket == "day"
        assert filters is None

    async def test_filters_are_translated_to_repository(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get(
                "/events/stats",
                params={
                    "bucket": "hour",
                    "type": "Signup",
                    "from": "2026-07-01T00:00:00Z",
                    "to": "2026-07-09T00:00:00Z",
                },
            )

        assert resp.status_code == 200
        [(bucket, filters)] = repo.stats_calls
        assert bucket == "hour"
        assert filters == EventFilters(
            event_type="signup",
            since=datetime(2026, 7, 1, tzinfo=UTC),
            until=datetime(2026, 7, 9, tzinfo=UTC),
        )

    async def test_missing_bucket_returns_422(self):
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get("/events/stats")

        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
        assert repo.stats_calls == []

    async def test_invalid_bucket_returns_422(self):
        async with api_client(FakeRepository()) as client:
            resp = await client.get("/events/stats", params={"bucket": "month"})

        assert resp.status_code == 422

    async def test_unsupported_filter_returns_422(self):
        # user_id is deliberately not a /stats facet; it must not be
        # silently ignored.
        repo = FakeRepository()
        async with api_client(repo) as client:
            resp = await client.get(
                "/events/stats", params={"bucket": "day", "user_id": "u1"}
            )

        assert resp.status_code == 422
        assert repo.stats_calls == []


class TestRealtimeStats:
    def make_snapshot(self, **overrides: Any) -> RealtimeSnapshot:
        fields: dict[str, Any] = {
            "window_seconds": 300,
            "total": 3,
            "counts_by_type": {"pageview": 2, "click": 1},
            "computed_at": datetime(2026, 7, 10, 12, 0, tzinfo=UTC),
        }
        fields.update(overrides)
        return RealtimeSnapshot(**fields)

    async def test_returns_snapshot_envelope_with_default_window(self):
        cache = FakeCache(self.make_snapshot())
        async with api_client(FakeRepository(), cache=cache) as client:
            resp = await client.get("/events/stats/realtime")

        assert resp.status_code == 200
        assert resp.json() == {
            "window_seconds": 300,
            "total": 3,
            "counts_by_type": {"pageview": 2, "click": 1},
            "computed_at": "2026-07-10T12:00:00Z",
        }
        assert cache.windows == [300]

    async def test_window_param_is_passed_to_cache(self):
        cache = FakeCache(self.make_snapshot(window_seconds=60))
        async with api_client(FakeRepository(), cache=cache) as client:
            resp = await client.get("/events/stats/realtime", params={"window": 60})

        assert resp.status_code == 200
        assert cache.windows == [60]

    @pytest.mark.parametrize("window", [0, 120, 3600, "5m"])
    async def test_window_outside_allowlist_returns_422(self, window):
        cache = FakeCache()
        async with api_client(FakeRepository(), cache=cache) as client:
            resp = await client.get("/events/stats/realtime", params={"window": window})

        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
        assert cache.windows == []

    async def test_unknown_param_returns_422(self):
        cache = FakeCache()
        async with api_client(FakeRepository(), cache=cache) as client:
            resp = await client.get("/events/stats/realtime", params={"windw": 300})

        assert resp.status_code == 422
        assert cache.windows == []


class TestMongoUnavailable:
    @pytest.mark.parametrize("path", ["/events", "/events/stats?bucket=day"])
    async def test_returns_503_envelope(self, path):
        async with api_client(DownRepository()) as client:
            resp = await client.get(path)

        assert resp.status_code == 503
        error = resp.json()["error"]
        assert error["code"] == "storage_unavailable"
        assert "mongo down" not in resp.text


class TestSearch:
    async def test_returns_events_and_total_envelope(self):
        event = make_event()
        search = FakeSearchIndex(hits=[event], total=1)
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get("/events/search", params={"q": "firefox"})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        [returned] = body["events"]
        assert returned["event_id"] == event.event_id
        assert returned["metadata"] == {"browser": "firefox"}

    async def test_total_reports_matches_beyond_returned_page(self):
        search = FakeSearchIndex(hits=[make_event(), make_event()], total=4000)
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get("/events/search", params={"q": "firefox"})

        body = resp.json()
        assert len(body["events"]) == 2
        assert body["total"] == 4000

    async def test_defaults_no_filters_limit_50(self):
        search = FakeSearchIndex()
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get("/events/search", params={"q": "firefox"})

        assert resp.status_code == 200
        [(q, filters, size)] = search.search_calls
        assert q == "firefox"
        assert filters is None
        assert size == 50

    async def test_filters_are_translated_to_search_index(self):
        search = FakeSearchIndex()
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get(
                "/events/search",
                params={
                    "q": "campaign",
                    "type": "PageView",
                    "user_id": "u_1",
                    "source_url": "https://example.com/pricing",
                    "from": "2026-07-01T00:00:00Z",
                    "to": "2026-07-09T00:00:00Z",
                    "limit": 100,
                },
            )

        assert resp.status_code == 200
        [(q, filters, size)] = search.search_calls
        assert q == "campaign"
        assert filters == EventFilters(
            event_type="pageview",
            user_id="u_1",
            source_url="https://example.com/pricing",
            since=datetime(2026, 7, 1, tzinfo=UTC),
            until=datetime(2026, 7, 9, tzinfo=UTC),
        )
        assert size == 100

    async def test_missing_q_returns_422_envelope(self):
        search = FakeSearchIndex()
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get("/events/search")

        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"
        assert search.search_calls == []

    async def test_empty_q_returns_422(self):
        async with api_client(FakeRepository(), FakeSearchIndex()) as client:
            resp = await client.get("/events/search", params={"q": ""})

        assert resp.status_code == 422

    async def test_q_over_max_length_returns_422(self):
        async with api_client(FakeRepository(), FakeSearchIndex()) as client:
            resp = await client.get("/events/search", params={"q": "x" * 1025})

        assert resp.status_code == 422

    @pytest.mark.parametrize("limit", [0, 101])
    async def test_limit_out_of_range_returns_422(self, limit):
        async with api_client(FakeRepository(), FakeSearchIndex()) as client:
            resp = await client.get(
                "/events/search", params={"q": "firefox", "limit": limit}
            )

        assert resp.status_code == 422

    async def test_unknown_filter_param_returns_422(self):
        search = FakeSearchIndex()
        async with api_client(FakeRepository(), search) as client:
            resp = await client.get(
                "/events/search", params={"q": "firefox", "device": "ios"}
            )

        assert resp.status_code == 422
        assert search.search_calls == []

    async def test_es_down_returns_503_envelope(self):
        async with api_client(FakeRepository(), DownSearchIndex()) as client:
            resp = await client.get("/events/search", params={"q": "firefox"})

        assert resp.status_code == 503
        error = resp.json()["error"]
        assert error["code"] == "search_unavailable"
        assert "es down" not in resp.text
