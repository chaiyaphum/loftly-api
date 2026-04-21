"""Sentry bootstrap — called from the app lifespan.

No-op unless `settings.sentry_dsn` is set. Keeps test runs quiet + avoids
network calls in CI. Tags every outbound event with the current trace_id from
our request middleware so the Sentry event links back to the Loftly log line.

## Noise policy (Week 14 — see docs/SLO.md in loftly-docs)

- Drop `asyncio.CancelledError`, `starlette.requests.ClientDisconnect` and
  anything whose exception type name matches `AbortError`.
- Drop transactions on `/healthz`, `/readyz`, `/metrics` — these are probe
  noise and never point to user-visible failure.
- Downsample 4xx events to 10%; 5xx is always kept at 100%.
- `traces_sample_rate` is env-driven: prod defaults to 0.1, staging/dev to
  1.0, override via `LOFTLY_SENTRY_TRACES_SAMPLE`.

The drop helpers are exported so the test suite can assert against them
without having to spin up a full Sentry transport mock.
"""

from __future__ import annotations

import os
import random
from typing import Any

from loftly.core.logging import get_logger
from loftly.core.settings import Settings

log = get_logger(__name__)


# --- Tunables ----------------------------------------------------------------

# Exception *type names* (not fully-qualified) we always drop before send.
# Matched against `event["exception"]["values"][*]["type"]` + `hint["exc_info"]`.
_DROPPED_EXCEPTION_TYPES: frozenset[str] = frozenset(
    {
        "AbortError",
        "CancelledError",  # asyncio.CancelledError
        "ClientDisconnect",  # starlette.requests.ClientDisconnect
    }
)

# URL *path suffixes* whose transactions we always drop. FastAPI mounts the
# health probes at the root, Prometheus exporter at /metrics.
_DROPPED_TX_PATHS: tuple[str, ...] = ("/healthz", "/readyz", "/metrics")

# 4xx sampling rate. Must stay in sync with docs/SLO.md §"Sentry noise".
_FOURXX_SAMPLE_RATE: float = 0.1


# --- Helpers (exported for tests) -------------------------------------------


def _exception_type_name(event: dict[str, Any], hint: dict[str, Any] | None) -> str | None:
    """Return the exception class name if the event looks like an error event."""
    if hint is not None:
        exc_info = hint.get("exc_info")
        if exc_info and len(exc_info) >= 1 and exc_info[0] is not None:
            return getattr(exc_info[0], "__name__", None)
    values = (event.get("exception") or {}).get("values") or []
    if values:
        t = values[0].get("type")
        if isinstance(t, str):
            return t
    return None


def _is_4xx(event: dict[str, Any]) -> bool:
    """Heuristic: does this event correspond to an HTTP 4xx?

    Sentry's FastAPI integration stashes the response status under
    `contexts.response.status_code`. Missing key → treat as non-4xx so we
    default to keeping the event.
    """
    ctx = (event.get("contexts") or {}).get("response") or {}
    code = ctx.get("status_code")
    if isinstance(code, int):
        return 400 <= code < 500
    # Fallback: some integrations stuff it under `tags.status_code`.
    tags = event.get("tags") or {}
    code = tags.get("status_code") if isinstance(tags, dict) else None
    try:
        code_int = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_int = None
    return code_int is not None and 400 <= code_int < 500


def _is_5xx(event: dict[str, Any]) -> bool:
    ctx = (event.get("contexts") or {}).get("response") or {}
    code = ctx.get("status_code")
    if isinstance(code, int):
        return 500 <= code < 600
    tags = event.get("tags") or {}
    code = tags.get("status_code") if isinstance(tags, dict) else None
    try:
        code_int = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_int = None
    return code_int is not None and 500 <= code_int < 600


def _tx_path(event: dict[str, Any]) -> str | None:
    """Extract the URL path from a transaction event, if any."""
    req = event.get("request") or {}
    url = req.get("url")
    if isinstance(url, str):
        # Sentry occasionally hands us the full URL — pull the path out.
        if "://" in url:
            # naive split to avoid dragging in urllib for a hot path
            try:
                url = "/" + url.split("://", 1)[1].split("/", 1)[1]
            except IndexError:
                return None
        return url
    tx = event.get("transaction")
    return tx if isinstance(tx, str) else None


def should_drop_event(event: dict[str, Any], hint: dict[str, Any] | None) -> bool:
    """Return True if `before_send` should drop this event.

    Rules (in order):
      1. Exception type in _DROPPED_EXCEPTION_TYPES → drop.
      2. 5xx response → keep (never dropped).
      3. 4xx response → sample at _FOURXX_SAMPLE_RATE (10%).
      4. Otherwise → keep.
    """
    exc_name = _exception_type_name(event, hint)
    if exc_name and exc_name in _DROPPED_EXCEPTION_TYPES:
        return True
    if _is_5xx(event):
        return False
    if _is_4xx(event):
        # Keep ~10% of 4xx events.
        return random.random() >= _FOURXX_SAMPLE_RATE
    return False


def should_drop_transaction(event: dict[str, Any]) -> bool:
    """Return True if `before_send_transaction` should drop this transaction."""
    path = _tx_path(event)
    if path is None:
        return False
    return any(path.endswith(suffix) for suffix in _DROPPED_TX_PATHS)


def _resolve_traces_sample_rate(settings: Settings) -> float:
    """Env-driven sample rate, with sensible per-env defaults."""
    override = os.environ.get("LOFTLY_SENTRY_TRACES_SAMPLE")
    if override is not None:
        try:
            return max(0.0, min(1.0, float(override)))
        except ValueError:
            log.warning("sentry_traces_sample_invalid", value=override)
    # Default: prod 0.1, everything else 1.0 (we want full traces in dev/staging).
    return 0.1 if settings.is_prod else 1.0


# --- Init --------------------------------------------------------------------


def init_sentry(settings: Settings) -> bool:
    """Initialize Sentry if DSN configured. Returns True if enabled."""
    if not settings.sentry_dsn:
        log.info("sentry_disabled")
        return False

    # Lazy import so the dep is only loaded when actually in use.
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration

    def _before_send(event: Any, hint: Any) -> Any:
        # Drop noise per `should_drop_event`.
        if should_drop_event(event, hint):
            return None
        # Pull trace_id off structlog contextvars if bound by
        # RequestLoggingMiddleware.
        import structlog

        ctx = structlog.contextvars.get_contextvars()
        trace_id = ctx.get("trace_id")
        if trace_id:
            tags = event.setdefault("tags", {})
            tags["trace_id"] = trace_id
        return event

    def _before_send_transaction(event: Any, _hint: Any) -> Any:
        if should_drop_transaction(event):
            return None
        return event

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.loftly_env,
        traces_sample_rate=_resolve_traces_sample_rate(settings),
        integrations=[
            StarletteIntegration(),
            FastApiIntegration(),
        ],
        before_send=_before_send,
        before_send_transaction=_before_send_transaction,
        ignore_errors=[
            "asyncio.CancelledError",
            "starlette.requests.ClientDisconnect",
        ],
    )
    log.info("sentry_enabled", env=settings.loftly_env)
    return True


__all__ = [
    "init_sentry",
    "should_drop_event",
    "should_drop_transaction",
]
