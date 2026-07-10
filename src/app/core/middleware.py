import logging
import math
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.logging import request_id_var

logger = logging.getLogger(__name__)

KEY_PREFIX = "ratelimit"


@dataclass
class RateLimitResult:
    allowed: bool
    retry_after: int = 0


class RateLimiter:
    def __init__(
        self,
        redis: Redis,
        *,
        window_seconds: int,
        write_limit: int,
        read_limit: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._window = window_seconds
        self._limits = {"write": write_limit, "read": read_limit}
        self._clock = clock

    async def check(self, client: str, route_class: str) -> RateLimitResult:
        now = self._clock()
        window_index = int(now // self._window)
        key = f"{KEY_PREFIX}:{client}:{route_class}:{window_index}"

        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self._window * 2)
            count, _ = await pipe.execute()
        except RedisError as exc:
            logger.warning("rate limiter unavailable, allowing request: %s", exc)
            return RateLimitResult(allowed=True)

        if count > self._limits[route_class]:
            retry_after = math.ceil(self._window - now % self._window)
            return RateLimitResult(allowed=False, retry_after=max(retry_after, 1))
        return RateLimitResult(allowed=True)


def _route_class(request: Request) -> str | None:
    path = request.url.path
    if path != "/events" and not path.startswith("/events/"):
        return None
    if request.method == "POST":
        return "write"
    if request.method == "GET":
        return "read"
    return None


async def rate_limit_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    limiter: RateLimiter | None = getattr(request.app.state, "rate_limiter", None)
    route_class = _route_class(request)
    if limiter is None or route_class is None:
        return await call_next(request)

    client = request.client.host if request.client else "unknown"
    result = await limiter.check(client, route_class)
    if not result.allowed:
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "code": "rate_limited",
                    "message": "rate limit exceeded",
                    "details": None,
                }
            },
            headers={"Retry-After": str(result.retry_after)},
        )
    return await call_next(request)


async def request_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request.state.request_id = request_id
    token = request_id_var.set(request_id)

    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)

    response.headers["X-Request-ID"] = request_id
    return response
