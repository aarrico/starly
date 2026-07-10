from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from redis.asyncio import Redis

from app.cache.realtime import KEY_PREFIX, RealtimeStatsCache
from app.core.config import get_settings
from app.domain.events import Event
from app.main import create_app
from app.queue.simulated import SimulatedQueue

pytestmark = pytest.mark.integration

ALL_KEYS = [f"{KEY_PREFIX}:{w}" for w in (60, 300, 900)]


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    settings = get_settings()
    client = Redis.from_url(
        settings.redis_url, socket_connect_timeout=1, socket_timeout=1
    )
    await client.delete(*ALL_KEYS)
    yield client
    await client.delete(*ALL_KEYS)
    await client.aclose()


@asynccontextmanager
async def realtime_client(
    repo, search_index, cache
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        queue=SimulatedQueue(max_depth=100),
        repository=repo,
        search_index=search_index,
        cache=cache,
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


def make_event(event_type: str, age: timedelta) -> Event:
    return Event(
        event_type=event_type,
        timestamp=datetime.now(UTC) - age,
        user_id="u_realtime",
        source_url="https://example.com/realtime",
        metadata={},
    )


async def seed(repo) -> None:
    await repo.upsert_many(
        [
            make_event("pageview", timedelta(seconds=30)),
            make_event("click", timedelta(seconds=60)),
            make_event("pageview", timedelta(hours=2)),
        ]
    )


async def test_second_call_inside_ttl_is_served_from_cache(
    repo, search_index, redis_client
):
    await seed(repo)
    cache = RealtimeStatsCache(redis_client, ttl=30)

    async with realtime_client(repo, search_index, cache) as client:
        first = await client.get("/events/stats/realtime")
        assert first.status_code == 200
        body = first.json()
        assert body["window_seconds"] == 300
        assert body["total"] == 2
        assert body["counts_by_type"] == {"pageview": 1, "click": 1}

        assert 0 < await redis_client.ttl(f"{KEY_PREFIX}:300") <= 30

        second = await client.get("/events/stats/realtime")
        assert second.status_code == 200
        assert second.json()["computed_at"] == body["computed_at"]
        assert second.json() == body


async def test_redis_unavailable_still_serves_from_mongo(repo, search_index):
    await seed(repo)
    unreachable = Redis.from_url(
        "redis://localhost:1", socket_connect_timeout=0.2, socket_timeout=0.2
    )
    cache = RealtimeStatsCache(unreachable, ttl=30)

    async with realtime_client(repo, search_index, cache) as client:
        resp = await client.get("/events/stats/realtime", params={"window": 900})

        assert resp.status_code == 200
        body = resp.json()
        assert body["window_seconds"] == 900
        assert body["total"] == 2
        assert body["counts_by_type"] == {"pageview": 1, "click": 1}

    await unreachable.aclose()
