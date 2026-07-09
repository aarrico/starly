import asyncio
import contextlib
import heapq
import itertools
import random
import time
import uuid
from collections import deque
from collections.abc import Callable
from typing import Any, Protocol

from app.queue.protocol import MAX_BATCH_SIZE, DLQEntry, Message, QueueFullError


class _Jitter(Protocol):
    def uniform(self, low: float, high: float, /) -> float: ...


class SimulatedQueue:
    def __init__(
        self,
        max_depth: int = 10_000,
        base_delay: float = 1.0,
        max_receive_count: int = 5,
        clock: Callable[[], float] = time.monotonic,
        rng: _Jitter | None = None,
    ) -> None:
        self._max_depth = max_depth
        self._base_delay = base_delay
        self._max_receive_count = max_receive_count
        self._clock = clock
        self._rng: _Jitter = rng if rng is not None else random.Random()
        self._ready: deque[Message] = deque()
        self._delayed: list[tuple[float, int, Message]] = []
        self._in_flight: dict[str, Message] = {}
        self._dlq: list[DLQEntry] = []
        self._seq = itertools.count()
        self._cond = asyncio.Condition()

    async def send(self, body: dict[str, Any]) -> Message:
        async with self._cond:
            total_depth = len(self._ready) + len(self._delayed) + len(self._in_flight)

            if total_depth >= self._max_depth:
                raise QueueFullError(f"queue at max depth {self._max_depth}")

            message = Message(id=uuid.uuid4().hex, body=body)
            self._ready.append(message)
            self._cond.notify()

            return message

    async def receive_batch(
        self, max_n: int = MAX_BATCH_SIZE, wait: float = 0.0
    ) -> list[Message]:
        max_n = min(max_n, MAX_BATCH_SIZE)
        deadline = self._clock() + wait

        async with self._cond:
            while True:
                self._promote_due()

                if self._ready:
                    count = min(max_n, len(self._ready))
                    batch = [self._ready.popleft() for _ in range(count)]

                    for message in batch:
                        message.receive_count += 1
                        self._in_flight[message.id] = message

                    return batch

                now = self._clock()
                remaining = deadline - now
                if remaining <= 0:
                    return []

                timeout = remaining

                if self._delayed:
                    timeout = min(timeout, max(self._delayed[0][0] - now, 0))

                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._cond.wait(), timeout)

    async def ack(self, message: Message) -> None:
        async with self._cond:
            if message.id not in self._in_flight:
                raise ValueError(f"message {message.id} is not in flight")
            del self._in_flight[message.id]

    async def nack(self, message: Message, error: str) -> None:
        async with self._cond:
            if message.id not in self._in_flight:
                raise ValueError(f"message {message.id} is not in flight")

            del self._in_flight[message.id]

            if message.receive_count >= self._max_receive_count:
                self._dlq.append(DLQEntry(message=message, error=error))
                return

            delay = self._base_delay * 2 ** (message.receive_count - 1)
            delay += self._rng.uniform(0, self._base_delay)
            ready_at = self._clock() + delay
            heapq.heappush(self._delayed, (ready_at, next(self._seq), message))
            self._cond.notify()

    async def reject(self, message: Message, error: str) -> None:
        async with self._cond:
            if message.id not in self._in_flight:
                raise ValueError(f"message {message.id} is not in flight")

            del self._in_flight[message.id]
            self._dlq.append(DLQEntry(message=message, error=error))

    @property
    def dlq(self) -> list[DLQEntry]:
        return list(self._dlq)

    def _promote_due(self) -> None:
        now = self._clock()
        while self._delayed and self._delayed[0][0] <= now:
            _, _, message = heapq.heappop(self._delayed)
            self._ready.append(message)
