import asyncio
import random
import time

import pytest

from app.queue.protocol import EventQueue, QueueFullError
from app.queue.simulated import SimulatedQueue


def test_simulated_queue_satisfies_protocol():
    assert isinstance(SimulatedQueue(), EventQueue)


class FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class ZeroJitter:
    def uniform(self, low: float, high: float) -> float:
        return 0.0


class TestRoundtrip:
    async def test_send_receive_roundtrip(self):
        queue = SimulatedQueue(max_depth=10)
        payload = {"event_type": "pageview", "user_id": "u_1"}

        sent = await queue.send(payload)
        batch = await queue.receive_batch(max_n=10)

        assert len(batch) == 1
        assert batch[0].id == sent.id
        assert batch[0].body == payload


class TestBoundedSend:
    async def test_send_raises_when_full(self):
        queue = SimulatedQueue(max_depth=2)
        await queue.send({"n": 1})
        await queue.send({"n": 2})

        with pytest.raises(QueueFullError):
            await queue.send({"n": 3})

    async def test_depth_counts_delayed_and_in_flight(self):
        clock = FakeClock()
        queue = SimulatedQueue(max_depth=3, clock=clock, rng=ZeroJitter())

        await queue.send({"n": 1})
        await queue.send({"n": 2})
        await queue.send({"n": 3})

        [m1, m2, m3] = await queue.receive_batch(max_n=3)

        await queue.nack(m1, error="boom")

        with pytest.raises(QueueFullError):
            await queue.send({"n": 4})

        await queue.ack(m2)
        await queue.send({"n": 4})


class TestBatchCap:
    async def test_receive_rejects_over_sqs_cap(self):
        queue = SimulatedQueue(max_depth=100)

        with pytest.raises(ValueError, match="between 1 and 10"):
            await queue.receive_batch(max_n=50)

    async def test_receive_at_cap(self):
        queue = SimulatedQueue(max_depth=100)

        for n in range(12):
            await queue.send({"n": n})

        batch = await queue.receive_batch(max_n=10)

        assert len(batch) == 10


class TestLongPoll:
    async def test_returns_early_when_message_arrives(self):
        queue = SimulatedQueue(max_depth=10)

        async def send_soon():
            await asyncio.sleep(0.02)
            await queue.send({"n": 1})

        start = time.monotonic()
        batch, _ = await asyncio.gather(
            queue.receive_batch(max_n=10, wait=5.0), send_soon()
        )
        elapsed = time.monotonic() - start

        assert len(batch) == 1
        assert elapsed < 1.0

    async def test_returns_empty_after_wait_expires(self):
        queue = SimulatedQueue(max_depth=10)

        start = time.monotonic()
        batch = await queue.receive_batch(max_n=10, wait=0.05)
        elapsed = time.monotonic() - start

        assert batch == []
        assert elapsed >= 0.05


class TestAck:
    async def test_acked_message_is_gone(self):
        queue = SimulatedQueue(max_depth=10)

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.ack(message)

        assert await queue.receive_batch(max_n=10) == []

    async def test_double_ack_raises(self):
        queue = SimulatedQueue(max_depth=10)

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.ack(message)

        with pytest.raises(ValueError):
            await queue.ack(message)


class TestNackRedelivery:
    async def test_not_visible_before_delay(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10, base_delay=1.0, clock=clock, rng=ZeroJitter()
        )

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.nack(message, error="boom")

        assert await queue.receive_batch(max_n=10) == []
        clock.now += 0.99
        assert await queue.receive_batch(max_n=10) == []

    async def test_visible_after_delay_with_incremented_count(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10, base_delay=1.0, clock=clock, rng=ZeroJitter()
        )

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        assert message.receive_count == 1

        await queue.nack(message, error="boom")
        clock.now += 1.0

        [redelivered] = await queue.receive_batch(max_n=10)
        assert redelivered.id == message.id
        assert redelivered.receive_count == 2

    async def test_long_poll_wakes_for_due_redelivery(self):
        queue = SimulatedQueue(max_depth=10, base_delay=0.03, rng=ZeroJitter())

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.nack(message, error="boom")

        start = time.monotonic()
        batch = await queue.receive_batch(max_n=10, wait=5.0)
        elapsed = time.monotonic() - start

        assert [m.id for m in batch] == [message.id]
        assert elapsed < 1.0


class TestBackoffSchedule:
    async def test_delay_doubles_per_receive(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10,
            base_delay=1.0,
            max_receive_count=10,
            clock=clock,
            rng=ZeroJitter(),
        )

        await queue.send({"n": 1})

        for expected_delay in (1.0, 2.0, 4.0, 8.0):
            [message] = await queue.receive_batch(max_n=1)
            await queue.nack(message, error="boom")
            clock.now += expected_delay - 0.01
            assert await queue.receive_batch(max_n=10) == []
            clock.now += 0.01

        [message] = await queue.receive_batch(max_n=1)
        assert message.receive_count == 5

    async def test_jitter_stays_within_base_delay_bound(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10, base_delay=1.0, clock=clock, rng=random.Random(7)
        )

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.nack(message, error="boom")

        clock.now += 1.0
        assert await queue.receive_batch(max_n=10) == []

        clock.now += 1.0
        assert len(await queue.receive_batch(max_n=10)) == 1


class TestDeadLetterQueue:
    async def test_exhausted_message_lands_in_dlq_and_never_returns(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10,
            base_delay=1.0,
            max_receive_count=2,
            clock=clock,
            rng=ZeroJitter(),
        )

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.nack(message, error="first failure")

        clock.now += 1.0

        [message] = await queue.receive_batch(max_n=1)
        assert message.receive_count == 2
        await queue.nack(message, error="terminal failure")

        clock.now += 100.0

        assert await queue.receive_batch(max_n=10) == []

        [entry] = queue.dlq
        assert entry.message.id == message.id
        assert entry.message.receive_count == 2
        assert entry.error == "terminal failure"

    async def test_dlq_empty_below_exhaustion(self):
        clock = FakeClock()
        queue = SimulatedQueue(
            max_depth=10, max_receive_count=2, clock=clock, rng=ZeroJitter()
        )

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.nack(message, error="first failure")

        assert queue.dlq == []


class TestReject:
    async def test_rejected_message_lands_in_dlq_immediately(self):
        clock = FakeClock()
        queue = SimulatedQueue(max_depth=10, clock=clock, rng=ZeroJitter())

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.reject(message, error="poison: bad shape")

        clock.now += 100.0

        assert await queue.receive_batch(max_n=10) == []
        [entry] = queue.dlq
        assert entry.message.id == message.id
        assert entry.message.receive_count == 1
        assert entry.error == "poison: bad shape"

    async def test_reject_of_message_not_in_flight_raises(self):
        queue = SimulatedQueue(max_depth=10)

        await queue.send({"n": 1})
        [message] = await queue.receive_batch(max_n=1)
        await queue.ack(message)

        with pytest.raises(ValueError):
            await queue.reject(message, error="boom")


class TestConcurrentConsumers:
    async def test_consumers_never_share_messages(self):
        queue = SimulatedQueue(max_depth=100)

        for n in range(20):
            await queue.send({"n": n})

        batches = await asyncio.gather(
            queue.receive_batch(max_n=10), queue.receive_batch(max_n=10)
        )

        ids = [m.id for batch in batches for m in batch]
        assert len(ids) == 20
        assert len(set(ids)) == 20

    async def test_single_message_goes_to_exactly_one_waiter(self):
        queue = SimulatedQueue(max_depth=10)

        async def send_soon():
            await asyncio.sleep(0.02)
            await queue.send({"n": 1})

        batch_a, batch_b, _ = await asyncio.gather(
            queue.receive_batch(max_n=10, wait=0.15),
            queue.receive_batch(max_n=10, wait=0.15),
            send_soon(),
        )

        assert len(batch_a) + len(batch_b) == 1
