import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from app.domain.events import MAX_FUTURE_SKEW, MAX_METADATA_BYTES, Event


def make_event(**overrides: Any) -> Event:
    payload: dict[str, Any] = {
        "event_type": "pageview",
        "timestamp": datetime.now(UTC),
        "user_id": "u_123",
        "source_url": "https://example.com/pricing",
    }
    payload.update(overrides)
    return Event(**payload)


class TestEventTypeNormalization:
    def test_trims_and_lowercases(self):
        assert make_event(event_type="  PageView  ").event_type == "pageview"

    def test_empty_after_trim_rejected(self):
        with pytest.raises(ValidationError):
            make_event(event_type="   ")


class TestTimestamp:
    def test_iso_string_parses(self):
        event = make_event(timestamp="2026-07-08T12:00:00+00:00")
        assert event.timestamp == datetime(2026, 7, 8, 12, 0, 0, tzinfo=UTC)

    def test_naive_timestamp_treated_as_utc(self):
        event = make_event(timestamp=datetime(2026, 7, 8, 12, 0, 0))
        assert event.timestamp.tzinfo is not None
        assert event.timestamp == datetime(2026, 7, 8, 12, 0, 0, tzinfo=UTC)

    def test_aware_offset_normalized_to_utc(self):
        event = make_event(timestamp="2026-07-08T12:00:00+05:00")
        assert event.timestamp == datetime(2026, 7, 8, 7, 0, 0, tzinfo=UTC)
        assert event.model_dump(mode="json")["timestamp"] == "2026-07-08T07:00:00Z"

    def test_future_within_skew_accepted(self):
        inside = datetime.now(UTC) + MAX_FUTURE_SKEW - timedelta(seconds=10)
        assert make_event(timestamp=inside).timestamp == inside

    def test_future_beyond_skew_rejected(self):
        outside = datetime.now(UTC) + MAX_FUTURE_SKEW + timedelta(seconds=10)
        with pytest.raises(ValidationError):
            make_event(timestamp=outside)


class TestMetadata:
    def test_oversized_rejected(self):
        with pytest.raises(ValidationError):
            make_event(metadata={"blob": "x" * MAX_METADATA_BYTES})

    def test_defaults_to_empty_dict(self):
        assert make_event().metadata == {}


class TestEventId:
    def test_generated_ids_are_uuid7(self):
        assert uuid.UUID(make_event().event_id).version == 7

    def test_ids_time_ordered_across_events(self):
        first = make_event()
        second = make_event()
        assert first.event_id < second.event_id


class TestDefaults:
    def test_ingested_at_defaults_to_none(self):
        assert make_event().ingested_at is None
