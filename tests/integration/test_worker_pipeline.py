import asyncio
import contextlib
from datetime import UTC, datetime

import pytest

from app.domain.events import Event
from app.queue.simulated import SimulatedQueue
from app.worker.consumer import EventWorker

pytestmark = pytest.mark.integration


async def test_duplicate_delivery_yields_one_document(repo, search_index, eventually):
    queue = SimulatedQueue(max_depth=100)
    worker = EventWorker(queue, repo, search_index, poll_wait=0.05)

    event = Event(
        event_type="conversion",
        timestamp=datetime.now(UTC),
        user_id="u_dup",
        source_url="https://example.com/checkout",
        metadata={"campaign": "summer-launch"},
    )
    body = event.model_dump(mode="json")

    # Both sends land before the worker starts, so they arrive in a single
    # batch; cancellation lets that batch settle before the task exits.
    await queue.send(body)
    await queue.send(body)

    task = asyncio.create_task(worker.run())
    try:

        async def processed() -> bool:
            return len(await repo.find()) >= 1

        await eventually(processed)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert await queue.receive_batch(max_n=10) == []
    stored = await repo.find()
    assert [e.event_id for e in stored] == [event.event_id]
    assert stored[0].ingested_at is not None

    await search_index.refresh()
    result = await search_index.search("summer-launch")
    assert [hit.event_id for hit in result.hits] == [event.event_id]
    assert queue.dlq == []
