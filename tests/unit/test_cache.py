import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from app.cache.realtime import KEY_PREFIX, RealtimeStatsCache
from app.storage.mongo import RealtimeStats


class ComputeSpy:
    def __init__(self, stats: RealtimeStats) -> None:
        self.stats = stats
        self.calls = 0

    async def __call__(self) -> RealtimeStats:
        self.calls += 1
        return self.stats


def make_stats(**overrides) -> RealtimeStats:
    fields = {
        "window_seconds": 300,
        "total": 3,
        "counts_by_type": {"pageview": 2, "click": 1},
    }
    fields.update(overrides)
    return RealtimeStats(**fields)


@pytest.fixture
def server() -> FakeServer:
    return FakeServer()


@pytest.fixture
def redis(server: FakeServer) -> FakeAsyncRedis:
    return FakeAsyncRedis(server=server)


async def test_miss_computes_and_sets_with_ttl(redis):
    cache = RealtimeStatsCache(redis, ttl=30)
    compute = ComputeSpy(make_stats())

    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.window_seconds == 300
    assert snapshot.total == 3
    assert snapshot.counts_by_type == {"pageview": 2, "click": 1}
    assert snapshot.computed_at is not None
    assert 0 < await redis.ttl(f"{KEY_PREFIX}:300") <= 30


async def test_hit_skips_compute_and_preserves_computed_at(redis):
    cache = RealtimeStatsCache(redis, ttl=30)
    first = await cache.get_or_compute(300, ComputeSpy(make_stats()))

    compute = ComputeSpy(make_stats(total=999))
    second = await cache.get_or_compute(300, compute)

    assert compute.calls == 0
    assert second == first


async def test_windows_are_cached_separately(redis):
    cache = RealtimeStatsCache(redis, ttl=30)
    await cache.get_or_compute(60, ComputeSpy(make_stats(window_seconds=60)))

    compute = ComputeSpy(make_stats(window_seconds=300))
    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.window_seconds == 300


async def test_redis_down_falls_through_to_compute(server, redis):
    server.connected = False
    cache = RealtimeStatsCache(redis, ttl=30)
    compute = ComputeSpy(make_stats())

    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.total == 3


async def test_corrupt_payload_is_treated_as_miss(redis):
    await redis.set(f"{KEY_PREFIX}:300", "not-json")
    cache = RealtimeStatsCache(redis, ttl=30)
    compute = ComputeSpy(make_stats())

    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.total == 3


async def test_failed_set_still_returns_computed_result(server, redis):
    cache = RealtimeStatsCache(redis, ttl=30)
    stats = make_stats()

    async def compute() -> RealtimeStats:
        server.connected = False
        return stats

    snapshot = await cache.get_or_compute(300, compute)

    assert snapshot.total == stats.total
    server.connected = True
    assert await redis.get(f"{KEY_PREFIX}:300") is None


async def test_compute_errors_propagate(redis):
    cache = RealtimeStatsCache(redis, ttl=30)

    async def compute() -> RealtimeStats:
        raise RuntimeError("mongo exploded")

    with pytest.raises(RuntimeError, match="mongo exploded"):
        await cache.get_or_compute(300, compute)
