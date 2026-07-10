import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, cast

from fastapi import APIRouter, Depends, Request, Response

from app.api.queries import CacheDep, RepositoryDep, SearchIndexDep
from app.api.schemas import DLQEntryOut, DLQList, ReadinessOut
from app.queue.simulated import SimulatedQueue

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])

CHECK_TIMEOUT_SECONDS = 2.0


def get_queue(request: Request) -> SimulatedQueue:
    return cast(SimulatedQueue, request.app.state.queue)


QueueDep = Annotated[SimulatedQueue, Depends(get_queue)]


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _check(name: str, ping: Callable[[], Awaitable[None]]) -> tuple[str, str]:
    try:
        await asyncio.wait_for(ping(), CHECK_TIMEOUT_SECONDS)
    except Exception as exc:
        logger.warning("readiness check failed for %s: %s", name, exc)
        return name, "error"
    return name, "ok"


@router.get("/health/ready")
async def readiness(
    repo: RepositoryDep,
    index: SearchIndexDep,
    cache: CacheDep,
    response: Response,
) -> ReadinessOut:
    checks = {
        "mongodb": repo.ping,
        "elasticsearch": index.ping,
        "redis": cache.ping,
    }
    results = await asyncio.gather(
        *(_check(name, ping) for name, ping in checks.items())
    )
    dependencies = dict(results)

    ready = all(status == "ok" for status in dependencies.values())
    if not ready:
        response.status_code = 503

    return ReadinessOut(
        status="ready" if ready else "degraded", dependencies=dependencies
    )


@router.get("/admin/dlq")
async def dlq(queue: QueueDep) -> DLQList:
    entries = [
        DLQEntryOut(
            message_id=entry.message.id,
            receive_count=entry.message.receive_count,
            error=entry.error,
            body=entry.message.body,
        )
        for entry in queue.dlq
    ]
    return DLQList(entries=entries, total=len(entries))
