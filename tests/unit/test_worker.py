import asyncio
import contextlib
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from bson import BSON
from pymongo.common import validate_ok_for_replace
from pymongo.errors import BulkWriteError

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


class TestBatchConflicts:
    @pytest.mark.parametrize(
        "changed_field",
        [
            {"metadata": {"browser": "safari"}},
            {"event_type": "click"},
            {"timestamp": "2026-01-01T00:00:00Z"},
            {"user_id": "other-user"},
            {"source_url": "https://example.com/other"},
        ],
    )
    async def test_conflicting_delivery_is_rejected_without_replacing_first_event(
        self, repo, index, changed_field
    ):
        queue = SimulatedQueue(max_depth=2, max_receive_count=1)
        worker = EventWorker(
            queue,
            cast(EventRepository, repo),
            cast(EventSearchIndex, index),
            poll_wait=0.05,
        )
        body = make_body(metadata={"browser": "firefox"})
        await queue.send(body)
        conflict = await queue.send({**body, **changed_field})

        async with running(worker):
            await eventually(lambda: index.seen_ids == [body["event_id"]])

        [entry] = queue.dlq
        assert entry.message.id == conflict.id
        assert entry.message.receive_count == 1
        assert "conflict" in entry.error
        assert body["event_id"] in entry.error
        [stored] = repo.batches[0]
        [indexed] = index.batches[0]
        assert stored.metadata == indexed.metadata == {"browser": "firefox"}
        await assert_drained(queue)
        await queue.send(make_body())
        await queue.send(make_body())

    async def test_matching_deliveries_ignore_ingested_at_and_allow_other_events(
        self, repo, index
    ):
        queue = SimulatedQueue(max_depth=3, max_receive_count=1)
        worker = EventWorker(
            queue,
            cast(EventRepository, repo),
            cast(EventSearchIndex, index),
            poll_wait=0.05,
        )
        body = make_body(metadata={"browser": "firefox", "device": "desktop"})
        other = make_body()
        await queue.send(body)
        await queue.send(
            {
                **body,
                "ingested_at": "2026-01-01T00:00:00Z",
                "event_type": " PageView ",
                "metadata": {"device": "desktop", "browser": "firefox"},
            }
        )
        await queue.send(other)

        async with running(worker):
            await eventually(lambda: len(index.seen_ids) == 2)

        assert repo.seen_ids == index.seen_ids == [body["event_id"], other["event_id"]]
        assert queue.dlq == []
        await assert_drained(queue)
        for _ in range(3):
            await queue.send(make_body())

    @pytest.mark.parametrize("store_name", ["mongo", "es"])
    @pytest.mark.parametrize("permanent", [False, True])
    async def test_conflict_stays_rejected_when_matching_deliveries_fail(
        self, repo, index, store_name, permanent
    ):
        queue = SimulatedQueue(max_depth=3, base_delay=0, max_receive_count=2)
        worker = EventWorker(
            queue,
            cast(EventRepository, repo),
            cast(EventSearchIndex, index),
            poll_wait=0.05,
        )
        body = make_body()
        failing_store = repo if store_name == "mongo" else index
        if permanent:
            failing_store.poison_ids.add(body["event_id"])
        else:
            failing_store.failures_remaining[body["event_id"]] = 1
        first = await queue.send(body)
        conflict = await queue.send({**body, "metadata": {"browser": "safari"}})
        duplicate = await queue.send(body)

        async with running(worker):
            if permanent:
                await eventually(lambda: len(queue.dlq) == 3)
            else:
                successful_attempts = 1 if store_name == "mongo" else 2
                await eventually(lambda: len(index.seen_ids) == successful_attempts)

        conflicts = [entry for entry in queue.dlq if entry.message.id == conflict.id]
        [entry] = conflicts
        assert "conflict" in entry.error
        assert entry.message.receive_count == 1
        if permanent:
            failures = [entry for entry in queue.dlq if entry.message.id != conflict.id]
            assert {entry.message.id for entry in failures} == {first.id, duplicate.id}
            assert all(store_name in entry.error for entry in failures)
        else:
            assert queue.dlq == conflicts
        assert all(
            event.metadata == body["metadata"]
            for batch in repo.batches + index.batches
            for event in batch
        )
        await assert_drained(queue)
        for _ in range(3):
            await queue.send(make_body())


class TestMongoWriteConcern:
    @pytest.mark.parametrize("with_document_error", [False, True])
    async def test_uncertain_deliveries_retry_before_indexing_and_acknowledging(
        self, index, monkeypatch, with_document_error
    ):
        queue = SimulatedQueue(base_delay=0)
        ack = AsyncMock(wraps=queue.ack)
        nack = AsyncMock(wraps=queue.nack)
        reject = AsyncMock(wraps=queue.reject)
        monkeypatch.setattr(queue, "ack", ack)
        monkeypatch.setattr(queue, "nack", nack)
        monkeypatch.setattr(queue, "reject", reject)

        db = Mock()
        bulk_write = AsyncMock(
            side_effect=[
                BulkWriteError(
                    {
                        "writeErrors": (
                            [
                                {
                                    "index": 2,
                                    "code": 121,
                                    "errmsg": "document failed validation",
                                }
                            ]
                            if with_document_error
                            else []
                        ),
                        "writeConcernErrors": [
                            {
                                "code": 64,
                                "errmsg": "waiting for replication timed out",
                                "errInfo": {"wtimeout": True},
                            }
                        ],
                    }
                ),
                None,
            ]
        )
        db.get_collection.return_value.bulk_write = bulk_write
        repo = EventRepository(db)
        upsert_many = AsyncMock(wraps=repo.upsert_many)
        monkeypatch.setattr(repo, "upsert_many", upsert_many)
        worker = EventWorker(queue, repo, cast(EventSearchIndex, index))

        bodies = [make_body() for _ in range(3 if with_document_error else 2)]
        deliveries = [await queue.send(body) for body in bodies]
        uncertain_ids = [body["event_id"] for body in bodies[:2]]

        await worker._process_batch(await queue.receive_batch())

        assert index.batches == []
        ack.assert_not_awaited()
        assert nack.await_count == 2
        assert {call.args[0].id for call in nack.await_args_list} == {
            delivery.id for delivery in deliveries[:2]
        }
        assert all(
            "waiting for replication timed out" in call.args[1]
            for call in nack.await_args_list
        )
        if with_document_error:
            reject.assert_awaited_once_with(
                deliveries[2], "mongo: document failed validation"
            )
            assert [entry.message.id for entry in queue.dlq] == [deliveries[2].id]
        else:
            reject.assert_not_awaited()
            assert queue.dlq == []

        retries = await queue.receive_batch()
        assert [message.id for message in retries] == [
            delivery.id for delivery in deliveries[:2]
        ]
        assert [message.body["event_id"] for message in retries] == uncertain_ids
        assert all(message.receive_count == 2 for message in retries)

        await worker._process_batch(retries)

        assert [event.event_id for event in upsert_many.await_args_list[1].args[0]] == (
            uncertain_ids
        )
        assert index.seen_ids == uncertain_ids
        assert ack.await_count == 2
        assert {call.args[0].id for call in ack.await_args_list} == {
            delivery.id for delivery in deliveries[:2]
        }
        assert nack.await_count == 2
        assert bulk_write.await_count == 2
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


@pytest.mark.parametrize(
    "metadata",
    [
        {"value": 2**64},
        {"nested": [{"value": -(2**63) - 1}]},
        {"bad\0key": 1},
        {"nested": [{"bad\0key": 1}]},
    ],
)
async def test_bson_poison_rejected_while_valid_batchmate_stored_indexed_and_acked(
    index, metadata
):
    queue = SimulatedQueue(max_depth=2, max_receive_count=1)
    stored = []

    async def encode_and_store(ops, *, ordered):
        for op in ops:
            validate_ok_for_replace(op._doc)
            stored.append(BSON(BSON.encode(op._doc)).decode())

    db = Mock()
    db.get_collection.return_value.bulk_write = encode_and_store
    worker = EventWorker(queue, EventRepository(db), cast(EventSearchIndex, index))
    bad = await queue.send(make_body(metadata=metadata))
    metadata_good = {"nested": [{"min": -(2**63), "max": 2**63 - 1}]}
    good_body = make_body(metadata=metadata_good)
    await queue.send(good_body)

    await worker._process_batch(await queue.receive_batch())

    assert [doc["_id"] for doc in stored] == [good_body["event_id"]]
    assert stored[0]["metadata"] == metadata_good
    assert index.seen_ids == [good_body["event_id"]]
    [entry] = queue.dlq
    assert entry.message.id == bad.id
    assert entry.message.receive_count == 1
    assert entry.error.startswith("poison:")
    assert "BSON" in entry.error
    await assert_drained(queue)
    # Reclaiming both capacity slots proves no delivery remains in flight.
    await queue.send(make_body())
    await queue.send(make_body())
