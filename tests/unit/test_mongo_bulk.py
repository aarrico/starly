from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from bson import BSON
from bson.errors import InvalidDocument
from pymongo.common import validate_ok_for_replace
from pymongo.errors import BulkWriteError

from app.domain.events import Event
from app.storage.mongo import EventRepository
from app.storage.types import BulkResult, WriteError


@pytest.fixture
def events() -> list[Event]:
    return [
        Event(
            event_type="pageview",
            timestamp=datetime.now(UTC),
            user_id=f"u_{i}",
            source_url="https://example.com/pricing",
        )
        for i in range(3)
    ]


@pytest.fixture
def bulk_write() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def repo(bulk_write: AsyncMock) -> EventRepository:
    db = Mock()
    db.get_collection.return_value.bulk_write = bulk_write
    return EventRepository(db)


async def test_success_returns_all_event_ids(repo, bulk_write, events):
    result = await repo.upsert_many(events)

    assert result == BulkResult(ok_ids=[e.event_id for e in events], errors={})
    bulk_write.assert_awaited_once()


async def test_empty_batch_does_not_write(repo, bulk_write):
    assert await repo.upsert_many([]) == BulkResult(ok_ids=[], errors={})
    bulk_write.assert_not_awaited()


async def test_write_concern_timeout_marks_every_event_retryable(
    repo, bulk_write, events
):
    bulk_write.side_effect = BulkWriteError(
        {
            "writeErrors": [],
            "writeConcernErrors": [
                {
                    "code": 64,
                    "errmsg": "waiting for replication timed out",
                    "errInfo": {"wtimeout": True},
                }
            ],
        }
    )

    result = await repo.upsert_many(events)

    assert result == BulkResult(
        ok_ids=[],
        errors={
            event.event_id: WriteError(
                "waiting for replication timed out", permanent=False
            )
            for event in events
        },
    )


@pytest.mark.parametrize("with_concern_error", [False, True])
async def test_document_errors_keep_their_reason_and_retry_policy(
    repo, bulk_write, events, with_concern_error
):
    bulk_write.side_effect = BulkWriteError(
        {
            "writeErrors": [
                {"index": 0, "code": 121, "errmsg": "document failed validation"},
                {"index": 1, "code": 91, "errmsg": "primary shutting down"},
            ],
            "writeConcernErrors": (
                [{"code": 64, "errmsg": "waiting for replication timed out"}]
                if with_concern_error
                else []
            ),
        }
    )

    result = await repo.upsert_many(events)

    expected_errors = {
        events[0].event_id: WriteError("document failed validation", permanent=True),
        events[1].event_id: WriteError("primary shutting down", permanent=False),
    }
    if with_concern_error:
        expected_errors[events[2].event_id] = WriteError(
            "waiting for replication timed out", permanent=False
        )
    assert result == BulkResult(
        ok_ids=[] if with_concern_error else [events[2].event_id],
        errors=expected_errors,
    )


@pytest.mark.parametrize(
    "metadata, encoding_error",
    [
        ({"value": 2**64}, OverflowError),
        ({"nested": [{"value": -(2**63) - 1}]}, OverflowError),
        ({"bad\0key": 1}, InvalidDocument),
        ({"nested": [{"bad\0key": 1}]}, InvalidDocument),
    ],
)
async def test_encoding_failure_isolated_from_valid_documents(
    repo, bulk_write, events, metadata, encoding_error
):
    # Bypass domain validation to exercise storage's defensive boundary.
    bad = events[0].model_copy(update={"metadata": metadata})
    with pytest.raises(encoding_error):
        BSON.encode({"metadata": metadata})
    stored = []

    async def encode_and_store(ops, *, ordered):
        assert ordered is False
        for op in ops:
            validate_ok_for_replace(op._doc)
            stored.append(BSON(BSON.encode(op._doc)).decode())

    bulk_write.side_effect = encode_and_store
    result = await repo.upsert_many([bad, *events[1:]])

    assert result.ok_ids == [event.event_id for event in events[1:]]
    assert set(result.errors) == {bad.event_id}
    assert result.errors[bad.event_id].permanent
    assert [doc["_id"] for doc in stored] == result.ok_ids
    assert all("event_id" not in doc for doc in stored)


async def test_all_encoding_failures_return_without_database_call(
    repo, bulk_write, events
):
    invalid = [
        events[0].model_copy(update={"metadata": {"value": 2**64}}),
        events[1].model_copy(update={"metadata": {"bad\0key": 1}}),
    ]
    result = await repo.upsert_many(invalid)

    assert result.ok_ids == []
    assert set(result.errors) == {event.event_id for event in invalid}
    assert all(error.permanent for error in result.errors.values())
    bulk_write.assert_not_awaited()


@pytest.mark.parametrize("with_concern_error", [False, True])
async def test_mongo_errors_map_to_filtered_operation_positions(
    repo, bulk_write, events, with_concern_error
):
    bad = events[0].model_copy(update={"metadata": {"bad\0key": 1}})
    bulk_write.side_effect = BulkWriteError(
        {
            "writeErrors": [
                {"index": 0, "code": 121, "errmsg": "document failed validation"}
            ],
            "writeConcernErrors": (
                [{"code": 64, "errmsg": "replication timed out"}]
                if with_concern_error
                else []
            ),
        }
    )
    result = await repo.upsert_many([bad, *events[1:]])

    assert result.errors[bad.event_id].permanent
    assert "BSON" in result.errors[bad.event_id].reason
    assert result.errors[events[1].event_id] == WriteError(
        "document failed validation", permanent=True
    )
    if with_concern_error:
        assert result.ok_ids == []
        assert result.errors[events[2].event_id] == WriteError(
            "replication timed out", permanent=False
        )
    else:
        assert result.ok_ids == [events[2].event_id]
        assert events[2].event_id not in result.errors
