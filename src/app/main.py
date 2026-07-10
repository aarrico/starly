import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from elastic_transport import TransportError
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from pymongo.errors import ConnectionFailure
from redis.asyncio import Redis

from app.api import admin, ingest, queries
from app.cache.realtime import RealtimeStatsCache
from app.core.config import get_settings
from app.core.logging import configure_logging, request_id_var
from app.core.middleware import (
    RateLimiter,
    rate_limit_middleware,
    request_id_middleware,
)
from app.queue.protocol import EventQueue, QueueFullError
from app.queue.simulated import SimulatedQueue
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository
from app.worker.consumer import EventWorker

logger = logging.getLogger(__name__)


def _error_response(
    status_code: int,
    code: str,
    message: str,
    details: Any = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details}},
        headers=headers,
    )


def _log_worker_exit(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("worker task crashed", exc_info=exc)


def create_app(
    *,
    queue: EventQueue | None = None,
    repository: EventRepository | None = None,
    search_index: EventSearchIndex | None = None,
    cache: RealtimeStatsCache | None = None,
    rate_limiter: RateLimiter | None = None,
) -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        mongo_client: AsyncMongoClient[dict[str, Any]] | None = None
        es_client: AsyncElasticsearch | None = None
        redis_client: Redis | None = None
        worker_tasks: list[asyncio.Task[None]] = []

        try:
            app_queue = queue
            if app_queue is None:
                app_queue = SimulatedQueue(
                    max_depth=settings.queue_max_depth,
                    base_delay=settings.retry_base_delay,
                    max_receive_count=settings.max_receive_count,
                )

            repo = repository
            if repo is None:
                mongo_client = AsyncMongoClient(settings.mongo_url)
                repo = EventRepository(mongo_client[settings.mongo_db])
                await repo.ensure_indexes()

            index = search_index
            if index is None:
                es_client = AsyncElasticsearch(settings.es_url)
                index = EventSearchIndex(
                    es_client, settings.es_index, field_limit=settings.es_field_limit
                )
                await index.ensure_index()

            app_cache = cache
            if app_cache is None:
                redis_client = Redis.from_url(
                    settings.redis_url,
                    socket_connect_timeout=settings.redis_socket_timeout,
                    socket_timeout=settings.redis_socket_timeout,
                )
                app_cache = RealtimeStatsCache(
                    redis_client, ttl=settings.realtime_cache_ttl
                )

            limiter = rate_limiter
            if limiter is None and redis_client is not None:
                limiter = RateLimiter(
                    redis_client,
                    window_seconds=settings.rate_limit_window_seconds,
                    write_limit=settings.rate_limit_writes_per_window,
                    read_limit=settings.rate_limit_reads_per_window,
                )

            app.state.rate_limiter = limiter
            app.state.queue = app_queue
            app.state.repository = repo
            app.state.search_index = index
            app.state.cache = app_cache
            worker = EventWorker(
                app_queue, repo, index, batch_size=settings.worker_batch_size
            )

            for _ in range(settings.worker_concurrency):
                task = asyncio.create_task(worker.run())
                task.add_done_callback(_log_worker_exit)
                worker_tasks.append(task)

            yield
        finally:
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            if mongo_client is not None:
                await mongo_client.close()
            if es_client is not None:
                await es_client.close()
            if redis_client is not None:
                await redis_client.aclose()

    app = FastAPI(title="Event Processing Platform", lifespan=lifespan)
    # Registration order matters: last-registered runs outermost, so
    # request_id wraps rate limiting and 429s still carry X-Request-ID.
    app.middleware("http")(rate_limit_middleware)
    app.middleware("http")(request_id_middleware)
    app.include_router(ingest.router)
    app.include_router(queries.router)
    app.include_router(admin.router)

    async def on_request_validation(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, RequestValidationError)
        return _error_response(
            422, "validation_error", "invalid request", jsonable_encoder(exc.errors())
        )

    async def on_queue_full(request: Request, exc: Exception) -> JSONResponse:
        return _error_response(
            503, "queue_full", str(exc), headers={"Retry-After": "1"}
        )

    async def on_storage_unavailable(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "mongodb unavailable on %s %s: %s", request.method, request.url.path, exc
        )
        return _error_response(503, "storage_unavailable", "event store unavailable")

    async def on_search_unavailable(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "elasticsearch unavailable on %s %s: %s",
            request.method,
            request.url.path,
            exc,
        )
        return _error_response(503, "search_unavailable", "search unavailable")

    async def on_unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Runs outside request_id_middleware (the exception unwound through
        # it), so the contextvar is restored from request.state for the log.
        request_id = getattr(request.state, "request_id", "-")
        token = request_id_var.set(request_id)
        try:
            logger.error(
                "unhandled error on %s %s",
                request.method,
                request.url.path,
                exc_info=exc,
            )
        finally:
            request_id_var.reset(token)

        return _error_response(
            500,
            "internal",
            "internal server error",
            headers={"X-Request-ID": request_id},
        )

    app.add_exception_handler(RequestValidationError, on_request_validation)
    app.add_exception_handler(QueueFullError, on_queue_full)
    app.add_exception_handler(ConnectionFailure, on_storage_unavailable)
    app.add_exception_handler(TransportError, on_search_unavailable)
    app.add_exception_handler(Exception, on_unhandled)

    return app


configure_logging(get_settings().log_level)
app = create_app()
