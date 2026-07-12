from dataclasses import dataclass
from datetime import datetime


@dataclass
class EventFilters:
    event_type: str | None = None
    user_id: str | None = None
    source_url: str | None = None
    since: datetime | None = None
    until: datetime | None = None


@dataclass
class WriteError:
    reason: str
    permanent: bool = False


@dataclass
class BulkResult:
    ok_ids: list[str]
    errors: dict[str, WriteError]
