import asyncio
import contextlib
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from app.domain.events import Event
from app.queue.simulated import SimulatedQueue
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository
from app.storage.types import BulkResult, WriteError
from app.worker.consumer import EventWorker


def make_body(**overrides: Any) -> dict[str, Any]:
    event = Event(
        event_type="pageview",
        timestamp=datetime.now(UTC),
        user_id="u_1",
        source_url="https://example.com/pricing",
        metadata={"browser": "firefox"},
    )
    body = event.model_dump(mode="json")
    body.update(overrides)
    return body


class FakeBulkStore:
    def __init__(self) -> None:
        self.batches: list[list[Event]] = []
        self.failures_remaining: dict[str, int] = {}
        self.poison_ids: set[str] = set()
        self.exceptions_remaining = 0
        self.gate: asyncio.Event | None = None
        self.entered = 0

    @property
    def seen_ids(self) -> list[str]:
        return [event.event_id for batch in self.batches for event in batch]

    async def _bulk(self, events: list[Event]) -> BulkResult:
        self.entered += 1

        if self.gate is not None:
            await self.gate.wait()

        if self.exceptions_remaining > 0:
            self.exceptions_remaining -= 1
            raise ConnectionError("store down")

        self.batches.append(list(events))
        errors: dict[str, WriteError] = {}

        for event in events:
            if event.event_id in self.poison_ids:
                errors[event.event_id] = WriteError(
                    "permanent item failure", permanent=True
                )
            elif self.failures_remaining.get(event.event_id, 0) > 0:
                self.failures_remaining[event.event_id] -= 1
                errors[event.event_id] = WriteError("bulk item failed")

        ok_ids = [e.event_id for e in events if e.event_id not in errors]

        return BulkResult(ok_ids=ok_ids, errors=errors)

    upsert_many = _bulk
    index_many = _bulk


async def eventually(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)

    raise AssertionError("condition not met within timeout")


@pytest.fixture
def queue() -> SimulatedQueue:
    return SimulatedQueue(max_depth=100, base_delay=0.01)


@pytest.fixture
def repo() -> FakeBulkStore:
    return FakeBulkStore()


@pytest.fixture
def index() -> FakeBulkStore:
    return FakeBulkStore()


@pytest.fixture
def worker(queue: SimulatedQueue, repo: FakeBulkStore, index: FakeBulkStore):
    return EventWorker(
        queue,
        cast(EventRepository, repo),
        cast(EventSearchIndex, index),
        batch_size=10,
        poll_wait=0.05,
    )


@asynccontextmanager
async def running(worker: EventWorker):
    task = asyncio.create_task(worker.run())
    try:
        yield task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def assert_drained(queue: SimulatedQueue) -> None:
    assert await queue.receive_batch(max_n=10) == []


class TestHappyPath:
    async def test_stores_indexes_and_acks(self, queue, repo, index, worker):
        body = make_body()
        await queue.send(body)

        async with running(worker):
            await eventually(lambda: index.seen_ids == [body["event_id"]])

        [stored] = repo.batches[0]
        assert stored.event_id == body["event_id"]
        assert stored.ingested_at is not None
        assert queue.dlq == []
        await assert_drained(queue)


class TestPartialBatchFailure:
    async def test_acks_successes_and_nacks_failures(self, queue, repo, index, worker):
        ok_body, bad_body = make_body(), make_body()
        repo.failures_remaining[bad_body["event_id"]] = 1
        await queue.send(ok_body)
        await queue.send(bad_body)

        async with running(worker):
            await eventually(lambda: repo.seen_ids.count(bad_body["event_id"]) == 2)
            await eventually(lambda: index.seen_ids.count(bad_body["event_id"]) == 1)

        assert queue.dlq == []
        await assert_drained(queue)

    async def test_mongo_failed_events_excluded_from_es_bulk(
        self, queue, repo, index, worker
    ):
        ok_body, bad_body = make_body(), make_body()
        repo.failures_remaining[bad_body["event_id"]] = 1
        await queue.send(ok_body)
        await queue.send(bad_body)

        async with running(worker):
            await eventually(lambda: len(index.batches) >= 1)
            assert [e.event_id for e in index.batches[0]] == [ok_body["event_id"]]


class TestEsFailureRetries:
    async def test_nack_then_success_on_redelivery(self, queue, repo, index, worker):
        body = make_body()
        index.failures_remaining[body["event_id"]] = 1
        await queue.send(body)

        async with running(worker):
            await eventually(lambda: index.seen_ids.count(body["event_id"]) == 2)
            await eventually(lambda: repo.seen_ids.count(body["event_id"]) == 2)

        assert queue.dlq == []
        await assert_drained(queue)


class TestPoisonPill:
    async def test_invalid_shape_rejected_to_dlq_without_retries(
        self, queue, repo, index, worker
    ):
        await queue.send({"event_type": "pageview"})

        async with running(worker):
            await eventually(lambda: len(queue.dlq) == 1)

        [entry] = queue.dlq
        assert entry.message.receive_count == 1
        assert repo.batches == []
        assert index.batches == []

    async def test_missing_event_id_is_poison(self, queue, repo, index, worker):
        body = make_body()
        del body["event_id"]
        await queue.send(body)

        async with running(worker):
            await eventually(lambda: len(queue.dlq) == 1)

        assert repo.batches == []

    async def test_poison_does_not_block_valid_batchmates(
        self, queue, repo, index, worker
    ):
        good = make_body()
        await queue.send({"bad": "shape"})
        await queue.send(good)

        async with running(worker):
            await eventually(lambda: len(queue.dlq) == 1)
            await eventually(lambda: index.seen_ids == [good["event_id"]])

        await assert_drained(queue)


class TestPermanentStoreFailures:
    async def test_mongo_permanent_error_goes_straight_to_dlq(
        self, queue, repo, index, worker
    ):
        body = make_body()
        repo.poison_ids.add(body["event_id"])
        await queue.send(body)

        async with running(worker):
            await eventually(lambda: len(queue.dlq) == 1)

        [entry] = queue.dlq
        assert entry.message.receive_count == 1
        assert "mongo" in entry.error
        assert index.batches == []
        await assert_drained(queue)

    async def test_es_permanent_error_goes_straight_to_dlq(
        self, queue, repo, index, worker
    ):
        body = make_body()
        index.poison_ids.add(body["event_id"])
        await queue.send(body)

        async with running(worker):
            await eventually(lambda: len(queue.dlq) == 1)

        [entry] = queue.dlq
        assert entry.message.receive_count == 1
        assert "es" in entry.error
        assert repo.seen_ids == [body["event_id"]]
        await assert_drained(queue)


class TestUnhandledException:
    async def test_batch_nacked_not_leaked(self, queue, repo, index, worker):
        repo.exceptions_remaining = 1
        first, second = make_body(), make_body()
        await queue.send(first)
        await queue.send(second)

        async with running(worker):
            await eventually(
                lambda: (
                    sorted(repo.seen_ids)
                    == sorted([first["event_id"], second["event_id"]])
                )
            )
            await eventually(lambda: len(index.seen_ids) == 2)

        assert queue.dlq == []
        await assert_drained(queue)


class TestShutdown:
    async def test_cancel_while_idle_exits_promptly(self, worker):
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.01)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)

    async def test_cancel_mid_batch_finishes_in_flight_work(
        self, queue, repo, index, worker
    ):
        repo.gate = asyncio.Event()
        body = make_body()
        await queue.send(body)

        task = asyncio.create_task(worker.run())
        await eventually(lambda: repo.entered == 1)

        task.cancel()
        repo.gate.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)

        assert index.seen_ids == [body["event_id"]]
        assert queue.dlq == []
        await assert_drained(queue)
