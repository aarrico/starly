import httpx
import pytest

from app.main import create_app
from app.queue.simulated import SimulatedQueue

pytestmark = pytest.mark.integration


async def test_posted_event_is_searchable_by_metadata(repo, search_index, eventually):
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
                    "user_id": "u_search",
                    "source_url": "https://example.com/signup",
                    "metadata": {"campaign": "quasar launch"},
                },
            )
            assert resp.status_code == 202
            event_id = resp.json()["event_id"]

            async def indexed() -> bool:
                await search_index.refresh()
                found = await client.get("/events/search", params={"q": "quasar"})
                return found.json()["total"] == 1

            await eventually(indexed)

            found = await client.get("/events/search", params={"q": "quasar"})
            assert found.status_code == 200
            body = found.json()
            assert body["total"] == 1
            [hit] = body["events"]
            assert hit["event_id"] == event_id
            assert hit["event_type"] == "signup"
            assert hit["metadata"] == {"campaign": "quasar launch"}

            matched_type = await client.get(
                "/events/search", params={"q": "quasar", "type": "Signup"}
            )
            assert matched_type.json()["total"] == 1

            excluded = await client.get(
                "/events/search", params={"q": "quasar", "type": "pageview"}
            )
            assert excluded.status_code == 200
            assert excluded.json() == {"events": [], "total": 0}

            no_match = await client.get("/events/search", params={"q": "nebula"})
            assert no_match.json()["total"] == 0
