"""POST_V1 §2 — consent-gated email tracking pixel endpoint.

Contract asserted here:
- Invalid / tampered token → 400 (no PostHog emit)
- Valid token + no Analytics consent → 200 GIF, no PostHog emit
- Valid token + consent granted → 200 GIF, PostHog `welcome_email_opened`
- Token replay: second call still returns 200 (dedupe is PostHog's problem)
- Signature tampering → 400
- Unknown user hash (no match) → 200 GIF, no PostHog emit
- Response never 500s even on unexpected internal error
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient

from loftly.db.engine import get_sessionmaker
from loftly.db.models.consent import UserConsent
from loftly.notifications.welcome_email import build_tracking_token, verify_tracking_token
from loftly.observability.posthog import hash_distinct_id
from tests.conftest import TEST_USER_ID


async def _grant_analytics(user_id: UUID) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        s.add(
            UserConsent(
                user_id=user_id,
                purpose="analytics",
                granted=True,
                policy_version="2026-04-01",
                source="account_settings",
            )
        )
        await s.commit()


async def test_invalid_token_returns_400(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/tracking/email/not-a-token/open")
    assert resp.status_code == 400


async def test_tampered_signature_returns_400(seeded_client: AsyncClient) -> None:
    user_hash = hash_distinct_id(str(TEST_USER_ID))
    token = build_tracking_token(user_hash, "welcome_personalized")
    bad = token[:-4] + "abcd"
    resp = await seeded_client.get(f"/v1/tracking/email/{bad}/open")
    assert resp.status_code == 400


async def test_valid_token_no_consent_returns_gif_no_posthog(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_hash = hash_distinct_id(str(TEST_USER_ID))
    token = build_tracking_token(user_hash, "welcome_personalized")

    events: list[dict[str, Any]] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append({"event": event})

    monkeypatch.setattr(
        "loftly.api.routes.tracking.posthog_capture",
        _fake_capture,
    )

    resp = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/gif"
    assert resp.headers["cache-control"].startswith("no-store")
    assert len(resp.content) > 0
    assert events == []  # no consent → no emit


async def test_valid_token_with_consent_emits_posthog(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _grant_analytics(TEST_USER_ID)

    user_hash = hash_distinct_id(str(TEST_USER_ID))
    token = build_tracking_token(user_hash, "welcome_personalized")

    events: list[dict[str, Any]] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append(
            {"event": event, "distinct_id": distinct_id, "properties": dict(properties or {})}
        )

    monkeypatch.setattr(
        "loftly.api.routes.tracking.posthog_capture",
        _fake_capture,
    )

    resp = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    assert resp.status_code == 200
    assert len(events) == 1
    ev = events[0]
    assert ev["event"] == "welcome_email_opened"
    assert ev["distinct_id"] == user_hash  # hashed — not the raw UUID
    assert ev["properties"]["user_id_hash"] == user_hash
    assert ev["properties"]["email_type"] == "welcome_personalized"
    # Sanity: no raw user_id leaked into properties.
    assert str(TEST_USER_ID) not in str(ev)


async def test_token_replay_still_returns_200(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _grant_analytics(TEST_USER_ID)

    user_hash = hash_distinct_id(str(TEST_USER_ID))
    token = build_tracking_token(user_hash, "welcome_personalized")

    events: list[str] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append(event)

    monkeypatch.setattr(
        "loftly.api.routes.tracking.posthog_capture",
        _fake_capture,
    )

    first = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    second = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    assert first.status_code == 200
    assert second.status_code == 200
    # Pixel fires per open — dedupe is PostHog's concern.
    assert events == ["welcome_email_opened", "welcome_email_opened"]


async def test_unknown_user_hash_returns_200_no_emit(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Hash that doesn't correspond to any seeded user.
    token = build_tracking_token("nonexistent-hash-xyz", "welcome_personalized")

    events: list[str] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append(event)

    monkeypatch.setattr(
        "loftly.api.routes.tracking.posthog_capture",
        _fake_capture,
    )

    resp = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/gif"
    assert events == []


async def test_endpoint_never_500s_on_internal_error(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the consent lookup to raise — endpoint must still serve a GIF."""
    user_hash = hash_distinct_id(str(TEST_USER_ID))
    token = build_tracking_token(user_hash, "welcome_personalized")

    async def _boom(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("simulated db fault")

    monkeypatch.setattr(
        "loftly.api.routes.tracking._has_analytics_consent",
        _boom,
    )

    resp = await seeded_client.get(f"/v1/tracking/email/{token}/open")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/gif"


def test_verify_tracking_token_handles_garbage() -> None:
    assert verify_tracking_token("") is None
    assert verify_tracking_token(".x") is None
    assert verify_tracking_token("x.") is None
