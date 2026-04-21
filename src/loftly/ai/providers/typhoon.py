"""Typhoon (SambaNova) Thai-optimized LLM provider.

Used by `POST /v1/selector/parse-nlu` to parse free-text Thai into a structured
`SpendProfile`. See `mvp/AI_PROMPTS.md §Prompt 3 typhoon_nlu_spend` for the
contract and `prompts/typhoon_nlu_spend.py` for the versioned Thai prompt.

SambaNova exposes an OpenAI-compatible chat-completions API at
`https://api.sambanova.ai/v1`. We speak the raw HTTP so the app doesn't pull
in `openai` as a dependency — the surface we use (`/chat/completions`) is tiny
and stable.

Timeouts / retries (per W19 spec):
- 5s hard timeout on the HTTP call.
- Single retry on 429 / 503 only — these are transient SambaNova states.
  Other 4xx/5xx are surfaced immediately so the route can decide the
  response code (502 for malformed, 504 for timeout).

Failure modes:
- `TyphoonUnavailableError` — missing API key or any network error; route
  returns 501 (unset key) or propagates (network).
- `TyphoonMalformedOutputError` — model returned non-JSON; route maps to 502.
- `httpx.TimeoutException` — request exceeded 5s; route maps to 504.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.prompts.typhoon_nlu_spend import (
    SYSTEM_PROMPT_TH,
    USER_PROMPT_TEMPLATE_TH,
    prompt_slug,
)
from loftly.schemas.spend_nlu import SpendProfile

log = get_logger(__name__)

# SambaNova catalog names change; this is the current Thai-optimized Typhoon.
# If SambaNova retires the tag, wire a `settings.typhoon_model` override — a
# one-line change here upgrades the whole pipeline.
TYPHOON_MODEL = "typhoon-v1.5x-70b-instruct"

_TIMEOUT_SEC = 5.0
_RETRY_STATUSES = {429, 503}


class TyphoonUnavailableError(RuntimeError):
    """Raised when the Typhoon key is unset or the API is unreachable.

    The `POST /v1/selector/parse-nlu` handler catches this and returns 501
    Not Implemented so clients can degrade to the structured Selector form.
    """


class TyphoonMalformedOutputError(RuntimeError):
    """Raised when the model returned something that wasn't valid SpendProfile JSON."""


def _build_messages(text_th: str) -> list[dict[str, str]]:
    """Chat-completions message array. System + single user turn."""
    return [
        {"role": "system", "content": SYSTEM_PROMPT_TH},
        {"role": "user", "content": USER_PROMPT_TEMPLATE_TH.format(text_th=text_th)},
    ]


def _extract_json_payload(content: str) -> dict[str, Any]:
    """Parse the assistant message as a JSON object.

    Tolerates a leading ```json fence in case the model ignored instructions.
    Anything else → TyphoonMalformedOutputError.
    """
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    try:
        data = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise TyphoonMalformedOutputError(
            f"Typhoon response was not valid JSON: {exc.msg}"
        ) from exc
    if not isinstance(data, dict):
        raise TyphoonMalformedOutputError(
            "Typhoon response was valid JSON but not an object."
        )
    return data


def _normalize_categories(raw: Any) -> dict[str, float]:
    """Ensure category fractions are floats in [0, 1]; re-normalize if drift ≤5%.

    Typhoon occasionally returns category values that sum to 0.99 / 1.02. We
    rescale silently within ±5% of 1.0 so the strict `SpendProfile` validator
    (±1%) passes. Beyond ±5%, surface as malformed — that's a real misparse.
    """
    if not isinstance(raw, dict):
        raise TyphoonMalformedOutputError("spend_categories was not a JSON object.")
    out: dict[str, float] = {}
    for key, value in raw.items():
        if not isinstance(value, int | float):
            raise TyphoonMalformedOutputError(
                f"spend_categories[{key}] was not numeric (got {type(value).__name__})."
            )
        out[str(key)] = max(0.0, min(1.0, float(value)))
    total = sum(out.values())
    if total <= 0:
        raise TyphoonMalformedOutputError("spend_categories summed to 0 — unparseable.")
    if abs(total - 1.0) > 0.05:
        raise TyphoonMalformedOutputError(
            f"spend_categories sum drifted too far from 1.0 (got {total:.3f})."
        )
    return {k: v / total for k, v in out.items()}


def _parse_profile(data: dict[str, Any]) -> tuple[SpendProfile, float]:
    """Validate the model payload against `SpendProfile`. Returns (profile, confidence)."""
    if "spend_categories" in data:
        data["spend_categories"] = _normalize_categories(data.get("spend_categories"))
    confidence_raw = data.pop("confidence", None)
    try:
        profile = SpendProfile.model_validate(data)
    except Exception as exc:
        raise TyphoonMalformedOutputError(
            f"SpendProfile validation failed: {exc}"
        ) from exc
    if isinstance(confidence_raw, int | float):
        confidence = max(0.0, min(1.0, float(confidence_raw)))
    else:
        confidence = 0.5  # Mid confidence when the model omitted the field.
    return profile, confidence


class TyphoonProvider:
    """SambaNova-hosted Typhoon client. Stateless, safe to instantiate per-request."""

    name = "typhoon"
    model = TYPHOON_MODEL

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        # Tests may inject a pre-configured client. Prod opens a fresh client
        # per call so a stalled SambaNova doesn't hold the app event loop.
        self._http_client = http_client

    async def parse_spend_nlu(self, text_th: str) -> tuple[SpendProfile, float, int]:
        """Parse free-text Thai → (SpendProfile, confidence, duration_ms).

        Raises:
            TyphoonUnavailableError: key unset OR underlying network error.
            TyphoonMalformedOutputError: model returned non-JSON or invalid shape.
            httpx.TimeoutException: request exceeded 5s (route maps to 504).
        """
        settings = get_settings()
        api_key = (settings.typhoon_api_key or "").strip()
        if not api_key or api_key.lower() in {"stub", "test", "none"}:
            raise TyphoonUnavailableError(
                "TYPHOON_API_KEY is unset or a stub sentinel; Typhoon path is disabled."
            )

        url = f"{settings.typhoon_api_base.rstrip('/')}/chat/completions"
        payload = {
            "model": TYPHOON_MODEL,
            "messages": _build_messages(text_th),
            "temperature": 0.2,
            "max_tokens": 400,
            # SambaNova supports this (OpenAI-compatible). If the server ignores
            # it, we still defend via `_extract_json_payload`.
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        start = time.perf_counter()
        content = await self._post_with_retry(url, payload, headers)
        duration_ms = int((time.perf_counter() - start) * 1000)

        data = _extract_json_payload(content)
        profile, confidence = _parse_profile(data)

        log.info(
            "typhoon_nlu_parsed",
            prompt=prompt_slug(),
            duration_ms=duration_ms,
            confidence=round(confidence, 3),
            chars_in=len(text_th),
        )
        return profile, confidence, duration_ms

    async def _post_with_retry(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> str:
        """HTTP POST with single retry on 429/503. Returns assistant content string."""
        last_exc: Exception | None = None
        for attempt in (1, 2):
            try:
                resp = await self._post(url, payload, headers)
            except httpx.TimeoutException:
                # Don't retry on timeout — that doubles user-visible latency
                # on a bad day. Route maps to 504 directly.
                raise
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt == 2:
                    raise TyphoonUnavailableError(
                        f"Typhoon request failed: {exc}"
                    ) from exc
                continue
            if resp.status_code in _RETRY_STATUSES and attempt == 1:
                log.warning(
                    "typhoon_retryable_status",
                    status=resp.status_code,
                    attempt=attempt,
                )
                continue
            if resp.status_code >= 400:
                raise TyphoonUnavailableError(
                    f"Typhoon returned HTTP {resp.status_code}: {resp.text[:200]}"
                )
            return _extract_content_from_response(resp.json())
        raise TyphoonUnavailableError(
            f"Typhoon retryable errors exhausted; last_exc={last_exc}"
        )

    async def _post(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> httpx.Response:
        """One HTTP POST with the configured timeout. Honors injected client for tests."""
        if self._http_client is not None:
            return await self._http_client.post(url, json=payload, headers=headers)
        async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
            return await client.post(url, json=payload, headers=headers)


def _extract_content_from_response(body: dict[str, Any]) -> str:
    """Pluck `choices[0].message.content` from an OpenAI-compatible response."""
    try:
        choices = body["choices"]
        if not isinstance(choices, list) or not choices:
            raise KeyError("choices")
        message = choices[0]["message"]
        content = message["content"]
    except (KeyError, TypeError, IndexError) as exc:
        raise TyphoonMalformedOutputError(
            f"Typhoon response envelope missing choices[0].message.content: {exc}"
        ) from exc
    if not isinstance(content, str):
        raise TyphoonMalformedOutputError("Typhoon message.content was not a string.")
    return content


__all__ = [
    "TYPHOON_MODEL",
    "TyphoonMalformedOutputError",
    "TyphoonProvider",
    "TyphoonUnavailableError",
]
