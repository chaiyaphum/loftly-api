"""Server-side PostHog feature-flag evaluator.

Thin async wrapper around the PostHog `/decide` HTTP endpoint. Used by the API
to read rollout state for experiments like the Selector CTA A/B test (W15 in
DEV_PLAN.md).

Design:
- Keyed by `distinct_id` (anon user_id / session_id / hashed email).
- 2s hard timeout; network / HTTP errors **never** raise — callers get the
  supplied `default` back and a log warning. This keeps MVP hot paths free of
  feature-flag coupling.
- When `POSTHOG_PROJECT_API_KEY` is unset (common in dev / test), every call
  short-circuits to the default without touching the network, consistent with
  the `settings.py` optional-key pattern.

Usage:

    flags = FeatureFlags()
    if await flags.is_enabled("selector_streaming", user_id):
        ...
    variant = await flags.variant("selector_cta_copy", user_id, default="control")
"""

from __future__ import annotations

from typing import Any

import httpx

from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings

log = get_logger(__name__)

_DECIDE_PATH = "/decide?v=3"
_DEFAULT_HOST = "https://app.posthog.com"
_TIMEOUT_SEC = 2.0


class FeatureFlags:
    """PostHog server-side feature-flag evaluator.

    Stateless — safe to instantiate per-request or reuse. The underlying
    `httpx.AsyncClient` is short-lived per call so a stalled PostHog never
    holds connections open in the app's main loop.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    @property
    def _api_key(self) -> str | None:
        return self._settings.posthog_project_api_key

    @property
    def _host(self) -> str:
        # PostHog uses `NEXT_PUBLIC_POSTHOG_HOST` on the web side. For server,
        # we re-read the env var directly so dev can override without bloating
        # Settings. Fallback to the public cloud endpoint.
        import os

        return os.environ.get("POSTHOG_HOST") or _DEFAULT_HOST

    async def _decide(self, user_id: str) -> dict[str, Any] | None:
        """Call PostHog `/decide` for a single distinct_id. Returns None on any failure."""
        api_key = self._api_key
        if not api_key:
            # Quiet in dev/test — the caller's `default` is the intended path.
            return None

        url = f"{self._host.rstrip('/')}{_DECIDE_PATH}"
        payload = {
            "api_key": api_key,
            "distinct_id": user_id,
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            log.warning(
                "feature_flags_decide_failed",
                error=str(exc),
                user_id=user_id,
            )
            return None
        if not isinstance(data, dict):
            return None
        return data

    async def is_enabled(
        self,
        flag_key: str,
        user_id: str,
        default: bool = False,
    ) -> bool:
        """Return True if `flag_key` is enabled for `user_id`.

        Boolean flags return True; multivariate flags return True when the
        assigned variant is anything other than the PostHog-reserved "false".
        """
        data = await self._decide(user_id)
        if data is None:
            return default
        flags = data.get("featureFlags")
        if not isinstance(flags, dict) or flag_key not in flags:
            return default
        value = flags[flag_key]
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            # Multivariate flags return the variant key. Treat every non-"false"
            # value as enabled — PostHog itself returns `False` (bool) when the
            # flag is off, so any string means the user is in an active bucket.
            return value != "false"
        return default

    async def variant(
        self,
        flag_key: str,
        user_id: str,
        default: str = "control",
    ) -> str:
        """Return the variant key (e.g. "control", "variant_a") for a multivariate flag."""
        data = await self._decide(user_id)
        if data is None:
            return default
        flags = data.get("featureFlags")
        if not isinstance(flags, dict) or flag_key not in flags:
            return default
        value = flags[flag_key]
        if isinstance(value, str) and value:
            return value
        if value is True:
            # Boolean-on flag asked as a variant — surface "control" so callers
            # don't get surprised by a boolean coming out of a string-typed API.
            return default
        return default


__all__ = ["FeatureFlags"]
