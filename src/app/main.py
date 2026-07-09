import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient

from app.api import ingest
from app.core.config import get_settings
from app.core.logging import configure_logging, request_id_var
from app.core.middleware import request_id_middleware
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
) -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Builds (and owns) only the dependencies not injected; injected ones
        # are set up and torn down by the caller.
        mongo_client: AsyncMongoClient[dict[str, Any]] | None = None
        es_client: AsyncElasticsearch | None = None
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

            app.state.queue = app_queue
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

    app = FastAPI(title="Event Processing Platform", lifespan=lifespan)
    app.middleware("http")(request_id_middleware)
    app.include_router(ingest.router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def on_request_validation(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, RequestValidationError)
        return _error_response(
            422, "validation_error", "invalid request", jsonable_encoder(exc.errors())
        )

    async def on_queue_full(request: Request, exc: Exception) -> JSONResponse:
        return _error_response(
            503, "queue_full", str(exc), headers={"Retry-After": "1"}
        )

    async def on_unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Runs outside request_id_middleware (the exception unwound through
        # it), so the contextvar is restored from request.state for the log.
        request_id = getattr(request.state, "request_id", "-")
        token = request_id_var.set(request_id)
        try:
            logger.error(
                "unhandled error on %s %s", request.method, request.url.path,
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
    app.add_exception_handler(Exception, on_unhandled)

    return app


app = create_app()
