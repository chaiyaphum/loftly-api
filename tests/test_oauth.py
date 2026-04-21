"""OAuth callback tests — stub mode + mocked provider happy path."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.auth.oauth import OAuthUserInfo
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.user import User


async def test_stub_mode_returns_503_for_google(seeded_client: AsyncClient) -> None:
    """No GOOGLE_CLIENT_ID → 503 oauth_provider_unavailable."""
    resp = await seeded_client.post(
        "/v1/auth/oauth/callback",
        json={
            "provider": "google",
            "code": "anything",
            "redirect_uri": "https://loftly.co.th/cb",
        },
    )
    assert resp.status_code == 503, resp.text
    assert resp.json()["error"]["code"] == "oauth_provider_unavailable"


async def test_stub_mode_returns_503_for_apple(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post(
        "/v1/auth/oauth/callback",
        json={
            "provider": "apple",
            "code": "anything",
            "redirect_uri": "https://loftly.co.th/cb",
        },
    )
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oauth_provider_unavailable"


async def test_stub_mode_returns_503_for_line(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post(
        "/v1/auth/oauth/callback",
        json={
            "provider": "line",
            "code": "anything",
            "redirect_uri": "https://loftly.co.th/cb",
        },
    )
    assert resp.status_code == 503


async def test_google_happy_path_creates_user(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With Google env set and exchange mocked, we mint a token pair + upsert user."""
    monkeypatch.setenv("LOFTLY_OAUTH_GOOGLE_CLIENT_ID", "fake-google-id")
    monkeypatch.setenv("LOFTLY_OAUTH_GOOGLE_CLIENT_SECRET", "fake-google-secret")
    get_settings.cache_clear()

    async def _fake_exchange(_code: str, _redirect_uri: str) -> OAuthUserInfo:
        return OAuthUserInfo(provider="google", subject="google-sub-abc", email="hi@example.com")

    import loftly.auth.oauth.google as google_mod

    monkeypatch.setattr(google_mod, "exchange_code", _fake_exchange)

    resp = await seeded_client.post(
        "/v1/auth/oauth/callback",
        json={
            "provider": "google",
            "code": "code-xyz",
            "redirect_uri": "https://loftly.co.th/cb",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["access_token"]
    assert body["refresh_token"]
    assert body["user"]["email"] == "hi@example.com"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        user = (
            (await s.execute(select(User).where(User.oauth_subject == "google-sub-abc")))
            .scalars()
            .one_or_none()
        )
    assert user is not None
    assert user.oauth_provider == "google"


async def test_oauth_upsert_does_not_duplicate(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOFTLY_OAUTH_GOOGLE_CLIENT_ID", "fake")
    monkeypatch.setenv("LOFTLY_OAUTH_GOOGLE_CLIENT_SECRET", "fake")
    get_settings.cache_clear()

    async def _fake_exchange(_code: str, _redirect_uri: str) -> OAuthUserInfo:
        return OAuthUserInfo(provider="google", subject="google-sub-dup", email="dup@example.com")

    import loftly.auth.oauth.google as google_mod

    monkeypatch.setattr(google_mod, "exchange_code", _fake_exchange)

    for _ in range(2):
        resp = await seeded_client.post(
            "/v1/auth/oauth/callback",
            json={"provider": "google", "code": "c", "redirect_uri": "r"},
        )
        assert resp.status_code == 200

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        count = len(
            (await s.execute(select(User).where(User.oauth_subject == "google-sub-dup")))
            .scalars()
            .all()
        )
    assert count == 1
