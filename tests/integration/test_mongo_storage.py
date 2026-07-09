from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.domain.events import Event
from app.storage.mongo import EventFilters, EventRepository, StatsBucket

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC)
STATS_BASE = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def _event(
    event_type: str = "pageview",
    user_id: str = "u_1",
    source_url: str = "https://example.com/a",
    *,
    timestamp: datetime,
) -> Event:
    return Event(
        event_type=event_type,
        timestamp=timestamp,
        user_id=user_id,
        source_url=source_url,
    )


def _filter_dataset() -> list[Event]:
    return [
        _event(timestamp=NOW - timedelta(minutes=10)),
        _event(
            "click",
            source_url="https://example.com/b",
            timestamp=NOW - timedelta(hours=2),
        ),
        _event(user_id="u_2", timestamp=NOW - timedelta(hours=26)),
    ]


def _stats_dataset() -> list[Event]:
    return [
        _event(timestamp=STATS_BASE + timedelta(minutes=5)),
        _event(timestamp=STATS_BASE + timedelta(minutes=20)),
        _event(timestamp=STATS_BASE + timedelta(hours=1, minutes=10)),
        _event("click", timestamp=STATS_BASE + timedelta(hours=2, minutes=30)),
    ]


def _index_names_in_plan(plan: Any) -> set[str]:
    names: set[str] = set()
    stack = [plan]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "indexName" in node:
                names.add(node["indexName"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return names


async def test_upsert_same_batch_twice_one_doc_each(repo: EventRepository) -> None:
    events = _filter_dataset()

    first = await repo.upsert_many(events)
    second = await repo.upsert_many(events)

    expected_ids = [event.event_id for event in events]
    assert first.ok_ids == second.ok_ids == expected_ids
    assert not first.errors
    assert not second.errors
    assert await repo._collection.count_documents({}) == len(events)


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        (EventFilters(), {0, 1, 2}),
        (EventFilters(event_type="pageview"), {0, 2}),
        (EventFilters(user_id="u_1"), {0, 1}),
        (EventFilters(source_url="https://example.com/a"), {0, 2}),
        (EventFilters(since=NOW - timedelta(hours=3)), {0, 1}),
        (EventFilters(until=NOW - timedelta(hours=3)), {2}),
        (
            EventFilters(
                since=NOW - timedelta(hours=3), until=NOW - timedelta(hours=1)
            ),
            {1},
        ),
        (EventFilters(event_type="pageview", user_id="u_1"), {0}),
    ],
)
async def test_find_filter_combinations(
    repo: EventRepository, filters: EventFilters, expected: set[int]
) -> None:
    events = _filter_dataset()
    await repo.upsert_many(events)

    found = await repo.find(filters)

    assert {event.event_id for event in found} == {events[i].event_id for i in expected}


async def test_find_sorts_desc_and_paginates(repo: EventRepository) -> None:
    events = _filter_dataset()
    await repo.upsert_many(events)
    expected_ids = [event.event_id for event in events]

    found = await repo.find()
    assert [event.event_id for event in found] == expected_ids

    first_two = await repo.find(limit=2)
    rest = await repo.find(offset=2)
    assert [event.event_id for event in first_two] == expected_ids[:2]
    assert [event.event_id for event in rest] == expected_ids[2:]


async def test_stats_hour_buckets(repo: EventRepository) -> None:
    await repo.upsert_many(_stats_dataset())

    result = await repo.stats("hour")

    assert result == [
        StatsBucket("pageview", STATS_BASE, 2),
        StatsBucket("pageview", STATS_BASE + timedelta(hours=1), 1),
        StatsBucket("click", STATS_BASE + timedelta(hours=2), 1),
    ]


async def test_stats_day_bucket_and_type_filter(repo: EventRepository) -> None:
    await repo.upsert_many(_stats_dataset())
    day = STATS_BASE.replace(hour=0)

    assert await repo.stats("day") == [
        StatsBucket("click", day, 1),
        StatsBucket("pageview", day, 3),
    ]
    assert await repo.stats("hour", EventFilters(event_type="click")) == [
        StatsBucket("click", STATS_BASE + timedelta(hours=2), 1),
    ]


async def test_type_range_query_uses_compound_index(repo: EventRepository) -> None:
    now = datetime.now(UTC)
    events = [
        Event(
            event_type="pageview" if i % 2 else "click",
            timestamp=now - timedelta(minutes=i),
            user_id=f"u_{i}",
            source_url="https://example.com/pricing",
        )
        for i in range(20)
    ]
    result = await repo.upsert_many(events)
    assert not result.errors

    cursor = repo._collection.find(
        {"event_type": "pageview", "timestamp": {"$gte": now - timedelta(hours=1)}}
    ).sort([("timestamp", -1), ("_id", -1)])
    plan = await cursor.explain()

    winning = plan["queryPlanner"]["winningPlan"]
    assert "idx_event_type" in _index_names_in_plan(winning)
