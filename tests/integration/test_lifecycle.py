import httpx
import pytest

from app.main import create_app
from app.queue.simulated import SimulatedQueue

pytestmark = pytest.mark.integration


async def test_post_event_is_processed_into_mongo(repo, search_index, eventually):
    app = create_app(
        queue=SimulatedQueue(max_depth=100),
        repository=repo,
        search_index=search_index,
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.post(
                "/events",
                json={
                    "event_type": "Signup",
                    "timestamp": "2026-07-09T12:00:00Z",
                    "user_id": "u_lifecycle",
                    "source_url": "https://example.com/signup",
                    "metadata": {"plan": "pro"},
                },
            )

        assert resp.status_code == 202
        event_id = resp.json()["event_id"]
        assert resp.json()["status"] == "queued"

        async def stored() -> bool:
            return len(await repo.find()) == 1

        await eventually(stored)

    [event] = await repo.find()
    assert event.event_id == event_id
    assert event.event_type == "signup"
    assert event.ingested_at is not None
