import logging
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar

from fastapi import Request, Response

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


class RequestIdLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


async def request_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    token = request_id_var.set(request_id)

    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)

    response.headers["X-Request-ID"] = request_id
    return response
