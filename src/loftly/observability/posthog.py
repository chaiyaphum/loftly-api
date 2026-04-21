"""Server-side PostHog event capture.

Thin async wrapper around the PostHog `/capture` endpoint. Used by the API to
emit product-analytics events for LLM paths (e.g. `typhoon_nlu_parsed` in
W19). Mirrors the defensive posture of `core/feature_flags.py`:

- Silent no-op when `POSTHOG_PROJECT_API_KEY` is unset (dev/test default).
- 2s hard timeout; network errors logged, never raised.
- `distinct_id` is expected to be a **hashed** user/session id — callers hash
  before passing in to avoid leaking raw user UUIDs to PostHog.

This lives alongside `langfuse.py` / `sentry.py` because PostHog is our
product-analytics sink (Langfuse = LLM traces, Sentry = errors, PostHog =
funnels).
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from typing import Any

import httpx

from loftly.core.logging import get_logger
from loftly.core.settings import get_settings

log = get_logger(__name__)

_CAPTURE_PATH = "/capture/"
_DEFAULT_HOST = "https://app.posthog.com"
_TIMEOUT_SEC = 2.0


def hash_distinct_id(user_id: str | None, *, salt: str = "loftly") -> str:
    """SHA-256 the user id with a salt so PostHog never sees raw UUIDs.

    For anon sessions where `user_id is None`, returns a stable `anon:{salt}`
    so aggregate counts still work without fabricating identities.
    """
    if not user_id:
        return f"anon:{salt}"
    return hashlib.sha256(f"{salt}:{user_id}".encode()).hexdigest()[:32]


async def capture(
    event: str,
    distinct_id: str,
    properties: dict[str, Any] | None = None,
) -> None:
    """Fire-and-forget event capture. Never raises; logs on failure.

    PostHog's capture endpoint is append-only + idempotent on the server side,
    so a one-shot POST with no retry is the right trade-off here.
    """
    settings = get_settings()
    api_key = settings.posthog_project_api_key
    if not api_key:
        return

    host = os.environ.get("POSTHOG_HOST") or _DEFAULT_HOST
    url = f"{host.rstrip('/')}{_CAPTURE_PATH}"
    payload = {
        "api_key": api_key,
        "event": event,
        "distinct_id": distinct_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "properties": dict(properties or {}),
    }
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        log.warning(
            "posthog_capture_failed",
            # structlog reserves `event` as the positional slot; use `event_name`
            # to carry the PostHog event key.
            event_name=event,
            error=str(exc)[:200],
        )


__all__ = ["capture", "hash_distinct_id"]
