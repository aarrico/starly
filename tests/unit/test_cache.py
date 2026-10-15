import asyncio
import json

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


@pytest.mark.parametrize(
    "invalid_fields",
    [
        {"window_seconds": "invalid"},
        {"total": "invalid"},
        {"counts_by_type": [1, 2]},
        {"counts_by_type": {"pageview": "invalid"}},
        {"computed_at": "not-a-date"},
        {"computed_at": None},
    ],
    ids=[
        "invalid-window",
        "invalid-total",
        "invalid-counts-shape",
        "invalid-nested-count",
        "malformed-timestamp",
        "null-timestamp",
    ],
)
async def test_invalid_snapshot_is_recomputed_and_cache_is_repaired(
    redis, invalid_fields
):
    payload = {
        "window_seconds": 300,
        "total": 999,
        "counts_by_type": {"stale": 999},
        "computed_at": "2026-07-10T12:00:00+00:00",
    }
    payload.update(invalid_fields)
    key = f"{KEY_PREFIX}:300"
    await redis.set(key, json.dumps(payload))
    cache = RealtimeStatsCache(redis, ttl=30)
    compute = ComputeSpy(make_stats())

    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.total == 3
    assert snapshot.counts_by_type == {"pageview": 2, "click": 1}
    assert json.loads(await redis.get(key)) == {
        "window_seconds": 300,
        "total": 3,
        "counts_by_type": {"pageview": 2, "click": 1},
        "computed_at": snapshot.computed_at.isoformat(),
    }
    assert 0 < await redis.ttl(key) <= 30

    unexpected_compute = ComputeSpy(make_stats(total=999))
    assert await cache.get_or_compute(300, unexpected_compute) == snapshot
    assert unexpected_compute.calls == 0


@pytest.mark.parametrize(
    "missing_field", ["window_seconds", "total", "counts_by_type", "computed_at"]
)
async def test_missing_snapshot_field_is_treated_as_miss(redis, missing_field):
    payload = {
        "window_seconds": 300,
        "total": 999,
        "counts_by_type": {"stale": 999},
        "computed_at": "2026-07-10T12:00:00+00:00",
    }
    del payload[missing_field]
    await redis.set(f"{KEY_PREFIX}:300", json.dumps(payload))
    cache = RealtimeStatsCache(redis, ttl=30)
    compute = ComputeSpy(make_stats())

    snapshot = await cache.get_or_compute(300, compute)

    assert compute.calls == 1
    assert snapshot.total == 3


@pytest.mark.parametrize("payload", ["null", "[]", '"snapshot"', "42"])
async def test_non_object_payload_is_treated_as_miss(redis, payload):
    await redis.set(f"{KEY_PREFIX}:300", payload)
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


async def test_concurrent_misses_compute_once(redis):
    cache = RealtimeStatsCache(redis, ttl=30)
    gate = asyncio.Event()
    calls = 0

    async def compute() -> RealtimeStats:
        nonlocal calls
        calls += 1
        await gate.wait()
        return make_stats()

    tasks = [asyncio.create_task(cache.get_or_compute(300, compute)) for _ in range(5)]
    await asyncio.sleep(0.01)
    gate.set()
    snapshots = await asyncio.gather(*tasks)

    assert calls == 1
    assert all(s.total == 3 for s in snapshots)


@pytest.mark.parametrize("corrupt_cache", [False, True])
async def test_compute_errors_propagate(redis, corrupt_cache):
    if corrupt_cache:
        await redis.set(
            f"{KEY_PREFIX}:300",
            json.dumps(
                {
                    "window_seconds": 300,
                    "total": "invalid",
                    "counts_by_type": {},
                    "computed_at": "2026-07-10T12:00:00+00:00",
                }
            ),
        )
    cache = RealtimeStatsCache(redis, ttl=30)

    async def compute() -> RealtimeStats:
        raise RuntimeError("mongo exploded")

    with pytest.raises(RuntimeError, match="mongo exploded"):
        await cache.get_or_compute(300, compute)
