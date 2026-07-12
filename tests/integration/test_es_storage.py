from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.domain.events import Event
from app.storage.es import EventSearchIndex
from app.storage.types import EventFilters

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC)


def _event(
    event_type: str = "pageview",
    metadata: dict[str, Any] | None = None,
    *,
    source_url: str = "https://example.com/a",
    timestamp: datetime = NOW,
) -> Event:
    return Event(
        event_type=event_type,
        timestamp=timestamp,
        user_id="u_1",
        source_url=source_url,
        metadata=metadata or {},
    )


async def test_metadata_string_searchable(search_index: EventSearchIndex) -> None:
    event = _event(metadata={"campaign": "spring-launch", "retries": 3})
    result = await search_index.index_many([event])
    assert result.ok_ids == [event.event_id]
    assert result.errors == {}

    await search_index.refresh()
    found = await search_index.search("spring-launch")

    assert found.total == 1
    assert found.hits[0].event_id == event.event_id
    assert found.hits[0].metadata == event.metadata


async def test_conflicting_metadata_types_both_index(
    search_index: EventSearchIndex,
) -> None:
    first = _event(metadata={"amount": 42})
    second = _event(metadata={"amount": "forty-two"})
    result = await search_index.index_many([first, second])
    assert result.errors == {}

    await search_index.refresh()
    found = await search_index.search("forty-two")

    assert found.total == 1
    assert found.hits[0].event_id == second.event_id


async def test_nested_metadata_strings_searchable(
    search_index: EventSearchIndex,
) -> None:
    event = _event(metadata={"utm": {"campaign": "zubat-blitz"}, "tags": ["golden"]})
    await search_index.index_many([event])
    await search_index.refresh()

    assert (await search_index.search("zubat-blitz")).total == 1
    assert (await search_index.search("golden")).total == 1


async def test_reindex_same_id_no_duplicate(search_index: EventSearchIndex) -> None:
    event = _event(metadata={"feature": "checkout"})
    await search_index.index_many([event])
    await search_index.index_many([event])

    await search_index.refresh()
    found = await search_index.search("checkout")

    assert found.total == 1


async def test_filter_context_narrows(search_index: EventSearchIndex) -> None:
    old = NOW - timedelta(days=7)
    events = [
        _event("pageview", {"feature": "checkout"}),
        _event("click", {"feature": "checkout"}),
        _event("click", {"feature": "checkout"}, timestamp=old),
    ]
    await search_index.index_many(events)
    await search_index.refresh()

    by_type = await search_index.search("checkout", EventFilters(event_type="click"))
    assert {hit.event_id for hit in by_type.hits} == {
        events[1].event_id,
        events[2].event_id,
    }

    recent_clicks = await search_index.search(
        "checkout",
        EventFilters(event_type="click", since=NOW - timedelta(days=1)),
    )
    assert [hit.event_id for hit in recent_clicks.hits] == [events[1].event_id]


async def test_standard_analyzer_preserves_tokens(
    search_index: EventSearchIndex,
) -> None:
    event = _event(metadata={"device_os": "iOS 17", "action": "running"})
    await search_index.index_many([event])
    await search_index.refresh()

    assert (await search_index.search("iOS")).total == 1
    assert (await search_index.search("ios")).total == 1
    assert (await search_index.search("run")).total == 0


async def test_source_url_fragment_searchable(
    search_index: EventSearchIndex,
) -> None:
    event = _event(source_url="https://example.com/pricing/enterprise")
    await search_index.index_many([event])
    await search_index.refresh()

    found = await search_index.search("pricing")
    assert found.total == 1
    assert found.hits[0].event_id == event.event_id
