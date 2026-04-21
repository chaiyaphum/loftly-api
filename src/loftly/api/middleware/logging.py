"""Request logging + trace propagation.

Responsibilities (in order, per request):
1. Assign a trace_id (use incoming `X-Trace-Id` if set, else mint a new one).
2. Stash it in structlog contextvars so every log line emitted during the
   request is tagged.
3. Observe handler latency.
4. Emit a single `request.completed` event with method, path, status,
   latency_ms, trace_id, and user_id (if the route stashed one on
   `request.state.user_id`).
5. Add `X-Trace-Id` to every response so the caller can quote it when
   reporting issues.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from loftly.core.logging import get_logger
from loftly.observability.prometheus import http_request_observer

log = get_logger(__name__)


def _route_template(request: Request) -> str:
    """Best-effort route template for metric labels.

    Prefer the matched route's `path` (e.g. `/v1/cards/{card_id}`) so the
    cardinality stays bounded. If FastAPI's route matcher hasn't populated
    the scope yet (rare — happens on 404 before dispatch), fall back to the
    raw URL path, which at worst balloons Prometheus storage but doesn't
    break the scrape.
    """
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return request.url.path


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Emit `request.completed` with trace_id + latency per request."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        trace_id = request.headers.get("X-Trace-Id") or uuid.uuid4().hex

        # Bind into contextvars so any log inside the handler inherits it.
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(trace_id=trace_id)

        request.state.trace_id = trace_id
        start = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_sec = time.perf_counter() - start
            latency_ms = duration_sec * 1000.0
            user_id = getattr(request.state, "user_id", None)
            # Prometheus export — templated route avoids cardinality blowup
            # from ids-in-paths. `/metrics` scrape is excluded to prevent
            # self-referential counter churn during rapid scrape intervals.
            route_label = _route_template(request)
            if route_label != "/metrics":
                try:
                    http_request_observer(
                        route=route_label,
                        method=request.method,
                        status_code=status_code,
                        duration_seconds=duration_sec,
                    )
                except Exception as exc:
                    # Never let an observability failure break the request.
                    log.warning("metrics_observer_failed", exc_info=True, err=str(exc))
            log.info(
                "request.completed",
                method=request.method,
                path=request.url.path,
                status=status_code,
                latency_ms=round(latency_ms, 2),
                trace_id=trace_id,
                user_id=str(user_id) if user_id else None,
            )

    # Starlette's BaseHTTPMiddleware doesn't auto-add response headers — we do
    # that here by wrapping call_next's return. But BaseHTTPMiddleware already
    # handles response streaming; inject the header via a thin override below.


async def _inject_trace_header(request: Request, response: Response) -> Response:
    """Stamp `X-Trace-Id` on outbound response."""
    trace_id = getattr(request.state, "trace_id", None)
    if trace_id:
        response.headers["X-Trace-Id"] = trace_id
    return response


class TraceHeaderMiddleware(BaseHTTPMiddleware):
    """Add X-Trace-Id to every response."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        response = await call_next(request)
        return await _inject_trace_header(request, response)


__all__ = ["RequestLoggingMiddleware", "TraceHeaderMiddleware"]
