import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from app.domain.events import Event
from app.queue.protocol import EventQueue, Message
from app.storage.es import EventSearchIndex
from app.storage.mongo import EventRepository

logger = logging.getLogger(__name__)


def _deserialize(body: dict[str, Any]) -> Event:
    if "event_id" not in body:
        raise ValueError("missing event_id")
    return Event.model_validate(body)


class EventWorker:
    def __init__(
        self,
        queue: EventQueue,
        repository: EventRepository,
        search_index: EventSearchIndex,
        *,
        batch_size: int = 10,
        poll_wait: float = 1.0,
    ) -> None:
        self._queue = queue
        self._repository = repository
        self._search_index = search_index
        self._batch_size = batch_size
        self._poll_wait = poll_wait

    async def run(self) -> None:
        while True:
            messages = await self._queue.receive_batch(
                self._batch_size, self._poll_wait
            )
            if not messages:
                continue

            batch = asyncio.ensure_future(self._process_batch(messages))
            try:
                await asyncio.shield(batch)
            except asyncio.CancelledError:
                await batch
                raise

    async def _process_batch(self, messages: list[Message]) -> None:
        settled: set[str] = set()
        batch_error = "unhandled worker error"

        async def nack(message: Message, error: str) -> None:
            await self._queue.nack(message, error)
            settled.add(message.id)
            logger.warning("nacked message %s: %s", message.id, error)

        try:
            events: list[Event] = []
            by_event: dict[str, Message] = {}
            now = datetime.now(UTC)

            for message in messages:
                try:
                    event = _deserialize(message.body)
                except ValueError as exc:
                    await self._queue.reject(message, f"poison: {exc}")
                    settled.add(message.id)
                    logger.warning("rejected poison message %s: %s", message.id, exc)
                    continue
                event.ingested_at = now
                events.append(event)
                by_event[event.event_id] = message

            if not events:
                return

            stored = await self._repository.upsert_many(events)
            for event_id, error in stored.errors.items():
                await nack(by_event[event_id], f"mongo: {error}")

            to_index = [e for e in events if e.event_id not in stored.errors]
            if not to_index:
                return

            indexed = await self._search_index.index_many(to_index)
            for event_id, error in indexed.errors.items():
                await nack(by_event[event_id], f"es: {error}")

            for event in to_index:
                if event.event_id not in indexed.errors:
                    message = by_event[event.event_id]
                    await self._queue.ack(message)
                    settled.add(message.id)

            logger.info(
                "processed batch: %d acked, %d nacked, %d rejected",
                len(to_index) - len(indexed.errors),
                len(stored.errors) + len(indexed.errors),
                len(messages) - len(events),
            )
        except Exception as exc:
            batch_error = f"unhandled: {exc!r}"
            logger.exception("unhandled error processing batch of %d", len(messages))
        finally:
            for message in messages:
                if message.id not in settled:
                    await self._queue.nack(message, batch_error)
