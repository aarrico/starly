from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

MAX_BATCH_SIZE = 10


class QueueFullError(Exception):
    pass


@dataclass
class Message:
    id: str
    body: dict[str, Any]
    receive_count: int = 0


@dataclass
class DLQEntry:
    message: Message
    error: str


@runtime_checkable
class EventQueue(Protocol):
    async def send(self, body: dict[str, Any]) -> Message: ...

    async def receive_batch(
        self, max_n: int = MAX_BATCH_SIZE, wait: float = 0.0
    ) -> list[Message]: ...

    async def ack(self, message: Message) -> None: ...

    async def nack(self, message: Message, error: str) -> None: ...

    async def reject(self, message: Message, error: str) -> None: ...
