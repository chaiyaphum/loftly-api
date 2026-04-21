"""structlog configuration — JSON in prod/staging, human-friendly in dev."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from loftly.core.settings import Settings


def configure_logging(settings: Settings) -> None:
    """Wire structlog + stdlib logging together.

    Dev: colored key=value. Prod/staging/test: JSON lines for log-drain shipping.
    """
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    renderer: Any
    if settings.loftly_env == "dev":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Bridge stdlib → structlog so third-party libs (uvicorn, sqlalchemy) share format.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a module-scoped bound logger."""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
