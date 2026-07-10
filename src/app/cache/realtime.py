import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.storage.mongo import RealtimeStats

logger = logging.getLogger(__name__)

KEY_PREFIX = "stats:realtime:v1"


@dataclass
class RealtimeSnapshot:
    window_seconds: int
    total: int
    counts_by_type: dict[str, int]
    computed_at: datetime


def _dump(snapshot: RealtimeSnapshot) -> str:
    return json.dumps(
        {
            "window_seconds": snapshot.window_seconds,
            "total": snapshot.total,
            "counts_by_type": snapshot.counts_by_type,
            "computed_at": snapshot.computed_at.isoformat(),
        }
    )


def _parse(payload: str | bytes) -> RealtimeSnapshot:
    data = json.loads(payload)
    return RealtimeSnapshot(
        window_seconds=data["window_seconds"],
        total=data["total"],
        counts_by_type=data["counts_by_type"],
        computed_at=datetime.fromisoformat(data["computed_at"]),
    )


class RealtimeStatsCache:
    def __init__(self, redis: Redis, ttl: int) -> None:
        self._redis = redis
        self._ttl = ttl

    async def get_or_compute(
        self, window: int, compute: Callable[[], Awaitable[RealtimeStats]]
    ) -> RealtimeSnapshot:
        key = f"{KEY_PREFIX}:{window}"

        try:
            cached = await self._redis.get(key)
        except RedisError as exc:
            logger.warning("redis get failed for %s: %s", key, exc)
            cached = None

        if cached is not None:
            try:
                return _parse(cached)
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning("corrupt cache payload for %s: %s", key, exc)

        stats = await compute()
        snapshot = RealtimeSnapshot(
            window_seconds=stats.window_seconds,
            total=stats.total,
            counts_by_type=stats.counts_by_type,
            computed_at=datetime.now(UTC),
        )

        try:
            await self._redis.set(key, _dump(snapshot), ex=self._ttl)
        except RedisError as exc:
            logger.warning("redis set failed for %s: %s", key, exc)

        return snapshot
