import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx

from app.cache.realtime import RealtimeStatsCache
from app.main import create_app
from app.queue.protocol import MAX_BATCH_SIZE, EventQueue, Message
from app.queue.simulated import SimulatedQueue
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository


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


class OkPing:
    async def ping(self) -> None:
        pass


class FailPing:
    async def ping(self) -> None:
        raise RuntimeError("dependency down")


@asynccontextmanager
async def api_client(
    queue: EventQueue | None = None,
    repo: Any = None,
    search: Any = None,
    cache: Any = None,
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        queue=queue or IdleQueue(),
        repository=cast(EventRepository, repo or OkPing()),
        search_index=cast(EventSearchIndex, search or OkPing()),
        cache=cast(RealtimeStatsCache, cache or OkPing()),
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


async def test_health():
    async with api_client() as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


async def test_readiness_all_dependencies_ok():
    async with api_client() as client:
        resp = await client.get("/health/ready")
        assert resp.status_code == 200
        assert resp.json() == {
            "status": "ready",
            "dependencies": {
                "mongodb": "ok",
                "elasticsearch": "ok",
                "redis": "ok",
            },
        }


async def test_readiness_reports_failing_dependency():
    async with api_client(repo=FailPing()) as client:
        resp = await client.get("/health/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["dependencies"]["mongodb"] == "error"
        assert body["dependencies"]["elasticsearch"] == "ok"
        assert body["dependencies"]["redis"] == "ok"


async def test_dlq_empty():
    queue = SimulatedQueue(max_depth=10)
    async with api_client(queue=queue) as client:
        resp = await client.get("/admin/dlq")
        assert resp.status_code == 200
        assert resp.json() == {"entries": [], "total": 0}


async def test_dlq_lists_rejected_message():
    queue = SimulatedQueue(max_depth=10)
    await queue.send({"event_id": "evt-1", "bad": "shape"})
    [message] = await queue.receive_batch(1)
    await queue.reject(message, "poison: missing field")

    async with api_client(queue=queue) as client:
        resp = await client.get("/admin/dlq")
        assert resp.status_code == 200
        [entry] = resp.json()["entries"]
        assert entry["message_id"] == message.id
        assert entry["receive_count"] == 1
        assert entry["error"] == "poison: missing field"
        assert entry["body"] == {"event_id": "evt-1", "bad": "shape"}
        assert resp.json()["total"] == 1
