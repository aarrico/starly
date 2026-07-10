from typing import cast

import httpx
import pytest

from app.cache.realtime import RealtimeStatsCache
from app.domain.events import Event
from app.main import create_app
from app.queue.simulated import SimulatedQueue
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository
from app.storage.types import BulkResult

pytestmark = pytest.mark.integration

MAX_RECEIVE_COUNT = 3


class FailingRepository:
    def __init__(self) -> None:
        self.attempts = 0

    async def upsert_many(self, events: list[Event]) -> BulkResult:
        self.attempts += len(events)
        return BulkResult(
            ok_ids=[],
            errors={e.event_id: "injected mongo failure" for e in events},
        )


class UntouchedIndex:
    async def index_many(self, events: list[Event]) -> BulkResult:
        raise AssertionError("es must not be reached when mongo writes fail")


async def test_exhausted_retries_land_in_dlq(eventually):
    queue = SimulatedQueue(
        max_depth=100, base_delay=0.01, max_receive_count=MAX_RECEIVE_COUNT
    )
    repo = FailingRepository()
    app = create_app(
        queue=queue,
        repository=cast(EventRepository, repo),
        search_index=cast(EventSearchIndex, UntouchedIndex()),
        cache=cast(RealtimeStatsCache, object()),
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.post(
                "/events",
                json={
                    "event_type": "purchase",
                    "timestamp": "2026-07-10T12:00:00Z",
                    "user_id": "u_dlq",
                    "source_url": "https://example.com/checkout",
                    "metadata": {"amount": 42},
                },
            )
            assert resp.status_code == 202
            event_id = resp.json()["event_id"]

            async def dlq_has_entry() -> bool:
                dlq = await client.get("/admin/dlq")
                return dlq.json()["total"] == 1

            await eventually(dlq_has_entry)

            dlq = await client.get("/admin/dlq")
            assert dlq.status_code == 200
            [entry] = dlq.json()["entries"]
            assert entry["body"]["event_id"] == event_id
            assert entry["receive_count"] == MAX_RECEIVE_COUNT
            assert "mongo" in entry["error"]

    assert repo.attempts == MAX_RECEIVE_COUNT
