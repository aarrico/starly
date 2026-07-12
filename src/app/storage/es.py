from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from elasticsearch import AsyncElasticsearch

from app.domain.events import Event
from app.storage.types import BulkResult, EventFilters, WriteError

_SEARCH_FIELDS = ["search_text", "source_url.text"]

_INDEX_MAPPINGS: dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "event_type": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "user_id": {"type": "keyword"},
        "source_url": {
            "type": "keyword",
            "fields": {"text": {"type": "text"}},
        },
        # Full-text search over metadata happens via search_text, assembled
        # from metadata string values at index time in _to_doc.
        "search_text": {"type": "text"},
        "metadata": {"type": "object"},
    },
}


@dataclass
class SearchResult:
    hits: list[Event]
    total: int


def _string_values(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _string_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _string_values(item)


def _to_doc(event: Event) -> dict[str, Any]:
    doc = event.model_dump(mode="json")
    doc.pop("event_id")
    search_text = " ".join(_string_values(doc["metadata"]))
    if search_text:
        doc["search_text"] = search_text
    return doc


def _to_event(hit: dict[str, Any]) -> Event:
    return Event.model_validate({**hit["_source"], "event_id": hit["_id"]})


def _build_filters(filters: EventFilters | None) -> list[dict[str, Any]]:
    if filters is None:
        return []

    clauses: list[dict[str, Any]] = []

    if filters.event_type:
        clauses.append({"term": {"event_type": filters.event_type}})
    if filters.user_id:
        clauses.append({"term": {"user_id": filters.user_id}})
    if filters.source_url:
        clauses.append({"term": {"source_url": filters.source_url}})

    timestamp = {
        op: value
        for op, value in (("gte", filters.since), ("lte", filters.until))
        if value is not None
    }
    if timestamp:
        clauses.append({"range": {"timestamp": timestamp}})

    return clauses


class EventSearchIndex:
    def __init__(
        self,
        client: AsyncElasticsearch,
        index_name: str,
        *,
        field_limit: int = 200,
        max_size: int = 100,
    ) -> None:
        self._client = client
        self._index = index_name
        self._field_limit = field_limit
        self._max_size = max_size

    async def ping(self) -> None:
        await self._client.info()

    async def ensure_index(self) -> None:
        if await self._client.indices.exists(index=self._index):
            return

        await self._client.indices.create(
            index=self._index,
            settings={
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.mapping.total_fields.limit": self._field_limit,
            },
            mappings=_INDEX_MAPPINGS,
        )

    async def index_many(self, events: list[Event]) -> BulkResult:
        if not events:
            return BulkResult(ok_ids=[], errors={})

        operations: list[dict[str, Any]] = []
        for event in events:
            operations.append({"index": {"_id": event.event_id}})
            operations.append(_to_doc(event))

        response = await self._client.bulk(index=self._index, operations=operations)

        errors: dict[str, WriteError] = {}
        if response["errors"]:
            for event, item in zip(events, response["items"], strict=True):
                result = item["index"]
                error = result.get("error")
                if error:
                    status = result.get("status", 500)
                    errors[event.event_id] = WriteError(
                        error.get("reason", "bulk index failed"),
                        permanent=400 <= status < 500 and status != 429,
                    )

        ok_ids = [event.event_id for event in events if event.event_id not in errors]
        return BulkResult(ok_ids=ok_ids, errors=errors)

    async def search(
        self,
        q: str,
        filters: EventFilters | None = None,
        size: int = 50,
    ) -> SearchResult:
        query: dict[str, Any] = {
            "bool": {
                "must": [{"multi_match": {"query": q, "fields": _SEARCH_FIELDS}}],
                "filter": _build_filters(filters),
            }
        }

        response = await self._client.search(
            index=self._index,
            query=query,
            size=min(size, self._max_size),
        )

        hits = response["hits"]
        return SearchResult(
            hits=[_to_event(hit) for hit in hits["hits"]],
            total=hits["total"]["value"],
        )

    async def refresh(self) -> None:
        await self._client.indices.refresh(index=self._index)
