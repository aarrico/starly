from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
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
