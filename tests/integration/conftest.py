from collections.abc import AsyncIterator
from typing import Any

import pytest
from pymongo import AsyncMongoClient

from app.core.config import get_settings
from app.storage.mongo import COLLECTION_NAME, EventRepository


@pytest.fixture
async def repo() -> AsyncIterator[EventRepository]:
    settings = get_settings()
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(settings.mongo_url)
    db = client[f"{settings.mongo_db}_test"]
    await db.drop_collection(COLLECTION_NAME)

    repository = EventRepository(db)
    await repository.ensure_indexes()

    yield repository
    await client.close()
