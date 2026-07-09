import logging

from app.core.middleware import RequestIdLogFilter


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s [%(request_id)s] %(message)s",
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(RequestIdLogFilter())
