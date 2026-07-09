from collections.abc import AsyncIterator
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient

from app.core.config import get_settings
from app.storage.es import EventSearchIndex
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


@pytest.fixture
async def search_index() -> AsyncIterator[EventSearchIndex]:
    settings = get_settings()
    client = AsyncElasticsearch(settings.es_url)
    index_name = f"{settings.es_index}_test"
    await client.options(ignore_status=404).indices.delete(index=index_name)

    index = EventSearchIndex(client, index_name, field_limit=settings.es_field_limit)
    await index.ensure_index()

    yield index
    await client.close()
