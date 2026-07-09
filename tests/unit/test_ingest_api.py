import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx

from app.main import create_app
from app.queue.protocol import MAX_BATCH_SIZE, Message, QueueFullError
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository


class FakeQueue:
    def __init__(self, *, full: bool = False, boom: bool = False) -> None:
        self.sent: list[dict[str, Any]] = []
        self.full = full
        self.boom = boom

    async def send(self, body: dict[str, Any]) -> Message:
        if self.boom:
            raise RuntimeError("wires crossed")
        if self.full:
            raise QueueFullError("queue at max depth 10000")
        self.sent.append(body)
        return Message(id=str(len(self.sent)), body=body)

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


class CrashingQueue(FakeQueue):
    async def receive_batch(
        self, max_n: int = MAX_BATCH_SIZE, wait: float = 0.0
    ) -> list[Message]:
        raise RuntimeError("queue wedged")


class UntouchedStore:
    async def upsert_many(self, events):
        raise AssertionError("store must not be reached in API unit tests")

    index_many = upsert_many


def make_app(queue: FakeQueue):
    return create_app(
        queue=queue,
        repository=cast(EventRepository, UntouchedStore()),
        search_index=cast(EventSearchIndex, UntouchedStore()),
    )


@asynccontextmanager
async def api_client(
    queue: FakeQueue, **transport_kwargs: Any
) -> AsyncIterator[httpx.AsyncClient]:
    app = make_app(queue)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, **transport_kwargs)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


def valid_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "event_type": "PageView",
        "timestamp": datetime.now(UTC).isoformat(),
        "user_id": "u_1",
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox"},
    }
    payload.update(overrides)
    return payload


class TestIngestAccepted:
    async def test_returns_202_with_event_id_and_queued_status(self):
        queue = FakeQueue()
        async with api_client(queue) as client:
            resp = await client.post("/events", json=valid_payload())

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "queued"
        [sent] = queue.sent
        assert sent["event_id"] == body["event_id"]
        assert sent["event_type"] == "pageview"
        assert sent["user_id"] == "u_1"
        assert sent["metadata"] == {"browser": "firefox"}

    async def test_client_supplied_event_id_is_ignored(self):
        queue = FakeQueue()
        async with api_client(queue) as client:
            resp = await client.post("/events", json=valid_payload(event_id="spoofed"))

        assert resp.status_code == 202
        assert resp.json()["event_id"] != "spoofed"
        [sent] = queue.sent
        assert sent["event_id"] != "spoofed"


class TestQueueFull:
    async def test_returns_503_with_retry_after_envelope(self):
        queue = FakeQueue(full=True)
        async with api_client(queue) as client:
            resp = await client.post("/events", json=valid_payload())

        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "1"
        error = resp.json()["error"]
        assert error["code"] == "queue_full"
        assert error["message"]


class TestValidationErrors:
    async def test_malformed_payload_returns_422_envelope(self):
        queue = FakeQueue()
        async with api_client(queue) as client:
            resp = await client.post("/events", json={"event_type": "pageview"})

        assert resp.status_code == 422
        error = resp.json()["error"]
        assert error["code"] == "validation_error"
        assert error["details"]
        assert queue.sent == []

    async def test_domain_rule_violation_returns_422_envelope(self):
        far_future = (datetime.now(UTC) + timedelta(hours=2)).isoformat()
        queue = FakeQueue()
        async with api_client(queue) as client:
            resp = await client.post(
                "/events", json=valid_payload(timestamp=far_future)
            )

        assert resp.status_code == 422
        error = resp.json()["error"]
        assert error["code"] == "validation_error"
        assert queue.sent == []


class TestUnhandledError:
    async def test_returns_500_envelope_without_leaking_detail(self):
        queue = FakeQueue(boom=True)
        async with api_client(queue, raise_app_exceptions=False) as client:
            resp = await client.post("/events", json=valid_payload())

        assert resp.status_code == 500
        error = resp.json()["error"]
        assert error["code"] == "internal"
        assert "wires crossed" not in resp.text

    async def test_500_carries_request_id(self):
        queue = FakeQueue(boom=True)
        async with api_client(queue, raise_app_exceptions=False) as client:
            resp = await client.post(
                "/events",
                json=valid_payload(),
                headers={"X-Request-ID": "req-500"},
            )

        assert resp.status_code == 500
        assert resp.headers.get("x-request-id") == "req-500"


class TestWorkerCrash:
    async def test_shutdown_survives_and_logs_crashed_worker(self, caplog):
        app = make_app(CrashingQueue())

        with caplog.at_level(logging.ERROR, logger="app.main"):
            async with app.router.lifespan_context(app):
                await asyncio.sleep(0.01)

        assert any("worker task crashed" in r.message for r in caplog.records)


class TestRequestId:
    async def test_incoming_header_is_echoed(self):
        async with api_client(FakeQueue()) as client:
            resp = await client.get("/health", headers={"X-Request-ID": "req-123"})

        assert resp.headers["x-request-id"] == "req-123"

    async def test_generated_when_absent(self):
        async with api_client(FakeQueue()) as client:
            resp = await client.get("/health")

        assert resp.headers["x-request-id"]
