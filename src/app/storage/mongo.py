from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from bson.codec_options import CodecOptions
from pymongo import ReplaceOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import BulkWriteError
from pymongo.operations import IndexModel

from app.domain.events import Event

COLLECTION_NAME = "events"

Bucket = Literal["hour", "day", "week"]


@dataclass
class EventFilters:
    event_type: str | None = None
    user_id: str | None = None
    source_url: str | None = None
    since: datetime | None = None
    until: datetime | None = None


@dataclass
class BulkResult:
    ok_ids: list[str]
    errors: dict[str, str]


@dataclass
class StatsBucket:
    event_type: str
    bucket_start: datetime
    count: int


@dataclass
class RealtimeStats:
    window_seconds: int
    total: int
    counts_by_type: dict[str, int]


def _to_doc(event: Event) -> dict[str, Any]:
    doc = event.model_dump()
    doc["_id"] = doc.pop("event_id")
    return doc


def _to_event(doc: dict[str, Any]) -> Event:
    event = doc.copy()
    event["event_id"] = event.pop("_id")
    return Event.model_construct(**event)


def _build_match(filters: EventFilters | None) -> dict[str, Any]:
    if filters is None:
        return {}

    match: dict[str, Any] = {}

    if filters.event_type:
        match["event_type"] = filters.event_type
    if filters.user_id:
        match["user_id"] = filters.user_id
    if filters.source_url:
        match["source_url"] = filters.source_url

    timestamp = {
        op: value
        for op, value in (("$gte", filters.since), ("$lte", filters.until))
        if value is not None
    }
    if timestamp:
        match["timestamp"] = timestamp

    return match


class EventRepository:
    def __init__(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        self._collection = db.get_collection(
            COLLECTION_NAME,
            codec_options=CodecOptions(tz_aware=True, tzinfo=UTC),
        )

    async def ensure_indexes(self) -> None:
        await self._collection.create_indexes(
            [
                IndexModel(
                    [("event_type", 1), ("timestamp", -1)], name="idx_event_type"
                ),
                IndexModel([("user_id", 1), ("timestamp", -1)], name="idx_user_id"),
                IndexModel(
                    [("source_url", 1), ("timestamp", -1)], name="idx_source_url"
                ),
                IndexModel([("timestamp", -1)], name="idx_timestamp"),
            ]
        )

    async def upsert_many(self, events: list[Event]) -> BulkResult:
        if not events:
            return BulkResult(ok_ids=[], errors={})

        ops = [
            ReplaceOne({"_id": event.event_id}, _to_doc(event), upsert=True)
            for event in events
        ]
        errors: dict[str, str] = {}

        try:
            await self._collection.bulk_write(ops, ordered=False)
        except BulkWriteError as exc:
            for write_error in exc.details.get("writeErrors", []):
                event_id = events[write_error["index"]].event_id
                errors[event_id] = write_error.get("errmsg", "bulk write failed")

        ok_ids = [event.event_id for event in events if event.event_id not in errors]
        return BulkResult(ok_ids=ok_ids, errors=errors)

    async def find(
        self,
        filters: EventFilters | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Event]:
        match = _build_match(filters)
        cursor = (
            self._collection.find(match)
            .sort([("timestamp", -1), ("_id", -1)])
            .skip(offset)
            .limit(limit)
        )
        return [_to_event(doc) async for doc in cursor]

    async def stats(
        self, bucket: Bucket, filters: EventFilters | None = None
    ) -> list[StatsBucket]:
        pipeline: list[dict[str, Any]] = []

        match = _build_match(filters)
        if match:
            pipeline.append({"$match": match})

        pipeline += [
            {
                "$group": {
                    "_id": {
                        "event_type": "$event_type",
                        "bucket": {
                            "$dateTrunc": {
                                "date": "$timestamp",
                                "unit": bucket,
                                "startOfWeek": "monday",
                            }
                        },
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id.bucket": 1, "_id.event_type": 1}},
        ]

        cursor = await self._collection.aggregate(pipeline)
        return [
            StatsBucket(
                event_type=doc["_id"]["event_type"],
                bucket_start=doc["_id"]["bucket"],
                count=doc["count"],
            )
            async for doc in cursor
        ]

    async def realtime_summary(self, window: timedelta) -> RealtimeStats:
        since = datetime.now(UTC) - window

        pipeline: list[dict[str, Any]] = [
            {"$match": {"timestamp": {"$gte": since}}},
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
        ]

        cursor = await self._collection.aggregate(pipeline)
        counts = {doc["_id"]: doc["count"] async for doc in cursor}

        return RealtimeStats(
            window_seconds=int(window.total_seconds()),
            total=sum(counts.values()),
            counts_by_type=counts,
        )
