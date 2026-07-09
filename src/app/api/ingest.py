import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from app.api.schemas import EventAccepted, EventIn
from app.domain.events import Event
from app.queue.protocol import EventQueue

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])


def get_queue(request: Request) -> EventQueue:
    return request.app.state.queue


QueueDep = Annotated[EventQueue, Depends(get_queue)]


@router.post("", status_code=202)
async def ingest_event(payload: EventIn, queue: QueueDep) -> EventAccepted:
    try:
        event = Event(**payload.model_dump())
    except ValidationError as exc:
        raise RequestValidationError(
            exc.errors(include_url=False, include_context=False)
        ) from exc

    await queue.send(event.model_dump(mode="json"))
    logger.info("queued event %s", event.event_id)
    return EventAccepted(event_id=event.event_id)
