"""Sentry bootstrap — called from the app lifespan.

No-op unless `settings.sentry_dsn` is set. Keeps test runs quiet + avoids
network calls in CI. Tags every outbound event with the current trace_id from
our request middleware so the Sentry event links back to the Loftly log line.
"""

from __future__ import annotations

from typing import Any

from loftly.core.logging import get_logger
from loftly.core.settings import Settings

log = get_logger(__name__)


def init_sentry(settings: Settings) -> bool:
    """Initialize Sentry if DSN configured. Returns True if enabled."""
    if not settings.sentry_dsn:
        log.info("sentry_disabled")
        return False

    # Lazy import so the dep is only loaded when actually in use.
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration

    def _before_send(event: Any, _hint: Any) -> Any:
        # Pull trace_id off structlog contextvars if bound by RequestLoggingMiddleware.
        import structlog

        ctx = structlog.contextvars.get_contextvars()
        trace_id = ctx.get("trace_id")
        if trace_id:
            tags = event.setdefault("tags", {})
            tags["trace_id"] = trace_id
        return event

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.loftly_env,
        traces_sample_rate=0.1,
        integrations=[
            StarletteIntegration(),
            FastApiIntegration(),
        ],
        before_send=_before_send,
    )
    log.info("sentry_enabled", env=settings.loftly_env)
    return True


__all__ = ["init_sentry"]
