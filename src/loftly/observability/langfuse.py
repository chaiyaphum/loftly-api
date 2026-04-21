"""Langfuse bootstrap + `@observe` decorator wrapper.

Every LLM path (deterministic + Anthropic) wraps its entry point with
`@observe_llm` so Langfuse sees prompt, response, token counts, and latency.
When `LANGFUSE_SECRET_KEY` is unset the wrapper becomes a no-op decorator —
zero cost, zero network, tests pass deterministically.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar, cast

from loftly.core.logging import get_logger
from loftly.core.settings import Settings

log = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])

_LANGFUSE_CLIENT: Any | None = None


def init_langfuse(settings: Settings) -> bool:
    """Initialize the Langfuse client if key + host set. Returns True if enabled."""
    global _LANGFUSE_CLIENT
    if not settings.langfuse_secret_key or not settings.langfuse_host:
        log.info("langfuse_disabled")
        return False
    try:
        from langfuse import Langfuse
    except ImportError:  # pragma: no cover — dep installed in prod
        log.warning("langfuse_import_failed")
        return False

    _LANGFUSE_CLIENT = Langfuse(
        secret_key=settings.langfuse_secret_key,
        host=settings.langfuse_host,
    )
    log.info("langfuse_enabled", host=settings.langfuse_host)
    return True


def observe_llm(name: str) -> Callable[[F], F]:
    """Wrap an async LLM call with Langfuse tracing + structlog metadata.

    Emits a structlog event with name + latency even when Langfuse is off, so
    unified dashboards (PostHog / Grafana) still see every call.
    """

    def decorator(fn: F) -> F:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            import time

            start = time.perf_counter()
            try:
                result = await fn(*args, **kwargs)
                return result
            finally:
                latency_ms = (time.perf_counter() - start) * 1000.0
                log.info("llm_call", name=name, latency_ms=round(latency_ms, 2))
                if _LANGFUSE_CLIENT is not None:
                    try:
                        _LANGFUSE_CLIENT.trace(
                            name=name,
                            metadata={"latency_ms": round(latency_ms, 2)},
                        )
                    except Exception as exc:
                        log.warning("langfuse_trace_failed", error=str(exc)[:200])

        return cast("F", wrapper)

    return decorator


__all__ = ["init_langfuse", "observe_llm"]
