from typing import Any, cast

import httpx
import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from app.cache.realtime import RealtimeStatsCache
from app.core.middleware import RateLimiter
from app.main import create_app
from app.queue.simulated import SimulatedQueue
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository


class Clock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def server() -> FakeServer:
    return FakeServer()


@pytest.fixture
def redis(server: FakeServer) -> FakeAsyncRedis:
    return FakeAsyncRedis(server=server)


@pytest.fixture
def clock() -> Clock:
    return Clock()


def make_limiter(redis: FakeAsyncRedis, clock: Clock, **overrides: Any) -> RateLimiter:
    params: dict[str, Any] = {
        "window_seconds": 60,
        "write_limit": 2,
        "read_limit": 3,
        "clock": clock,
    }
    params.update(overrides)
    return RateLimiter(redis, **params)


class TestRateLimiter:
    async def test_under_limit_allows(self, redis, clock):
        limiter = make_limiter(redis, clock)

        for _ in range(2):
            result = await limiter.check("1.2.3.4", "write")
            assert result.allowed

    async def test_over_limit_denies_with_retry_after(self, redis, clock):
        clock.now = 1_000_040.0  # 20s into the 60s window, 40s left
        limiter = make_limiter(redis, clock)

        for _ in range(2):
            await limiter.check("1.2.3.4", "write")
        result = await limiter.check("1.2.3.4", "write")

        assert not result.allowed
        assert result.retry_after == 40

    async def test_retry_after_is_at_least_one_second(self, redis, clock):
        clock.now = 1_000_019.5  # 0.5s left in the window
        limiter = make_limiter(redis, clock, write_limit=0)

        result = await limiter.check("1.2.3.4", "write")

        assert not result.allowed
        assert result.retry_after == 1

    async def test_window_rollover_readmits(self, redis, clock):
        limiter = make_limiter(redis, clock)

        for _ in range(3):
            await limiter.check("1.2.3.4", "write")
        clock.advance(60)
        result = await limiter.check("1.2.3.4", "write")

        assert result.allowed

    async def test_read_and_write_budgets_are_independent(self, redis, clock):
        limiter = make_limiter(redis, clock, write_limit=1)

        await limiter.check("1.2.3.4", "write")
        denied = await limiter.check("1.2.3.4", "write")
        read = await limiter.check("1.2.3.4", "read")

        assert not denied.allowed
        assert read.allowed

    async def test_clients_are_independent(self, redis, clock):
        limiter = make_limiter(redis, clock, write_limit=1)

        await limiter.check("1.2.3.4", "write")
        denied = await limiter.check("1.2.3.4", "write")
        other = await limiter.check("5.6.7.8", "write")

        assert not denied.allowed
        assert other.allowed

    async def test_redis_error_fails_open(self, clock, caplog):
        broken = FakeAsyncRedis(connected=False)
        limiter = make_limiter(broken, clock, write_limit=0)

        result = await limiter.check("1.2.3.4", "write")

        assert result.allowed

    async def test_keys_expire(self, redis, clock):
        limiter = make_limiter(redis, clock)

        await limiter.check("1.2.3.4", "write")

        [key] = await redis.keys()
        assert 0 < await redis.ttl(key) <= 120


class UntouchedStore:
    async def upsert_many(self, events):
        raise AssertionError("store must not be reached in rate-limit tests")

    index_many = upsert_many


def make_app(limiter: RateLimiter | None) -> Any:
    return create_app(
        queue=SimulatedQueue(),
        repository=cast(EventRepository, UntouchedStore()),
        search_index=cast(EventSearchIndex, UntouchedStore()),
        cache=cast(RealtimeStatsCache, object()),
        rate_limiter=limiter,
    )


async def request(app: Any, method: str, path: str, **kwargs: Any) -> httpx.Response:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            return await client.request(method, path, **kwargs)


class TestRateLimitMiddleware:
    async def test_write_over_limit_returns_429_envelope(self, redis, clock):
        app = make_app(make_limiter(redis, clock, write_limit=1))

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                first = await client.post("/events", json={})
                second = await client.post("/events", json={})

        assert first.status_code == 422  # invalid body still consumes budget
        assert second.status_code == 429
        assert second.headers["retry-after"]
        assert second.headers["x-request-id"]
        error = second.json()["error"]
        assert error["code"] == "rate_limited"
        assert error["message"]

    async def test_reads_limited_independently_of_writes(self, redis, clock):
        app = make_app(make_limiter(redis, clock, write_limit=5, read_limit=0))

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                read = await client.get("/events")
                write = await client.post("/events", json={})

        assert read.status_code == 429
        assert write.status_code == 422

    async def test_exempt_paths_never_touch_redis(self, redis, clock):
        app = make_app(make_limiter(redis, clock, write_limit=0, read_limit=0))

        resp = await request(app, "GET", "/health")

        assert resp.status_code == 200
        assert await redis.keys() == []

    async def test_no_limiter_configured_is_a_noop(self, redis):
        app = make_app(None)

        resp = await request(app, "GET", "/health")

        assert resp.status_code == 200
