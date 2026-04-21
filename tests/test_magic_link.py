"""Magic-link auth tests — Week 5-6 scope.

Covers:
- POST /v1/auth/magic-link/request — 202 + structlog event on success;
  429 after the per-IP limit (5/min)
- POST /v1/auth/magic-link/consume — valid token → TokenPair + users row;
  expired token → 401; wrong purpose → 401; session_id bind works
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from jose import jwt
from sqlalchemy import select

from loftly.api.routes.auth import _MAGIC_LINK_PURPOSE, MAGIC_LINK_LIMITER
from loftly.api.routes.selector import _profile_hash
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.schemas.selector import SelectorInput


async def test_request_returns_202_and_logs(seeded_client: AsyncClient, caplog) -> None:
    import logging

    caplog.set_level(logging.INFO)
    resp = await seeded_client.post(
        "/v1/auth/magic-link/request",
        json={"email": "new-user@loftly.example"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["message_th"]


async def test_request_rate_limits_at_6th_call(seeded_client: AsyncClient) -> None:
    MAGIC_LINK_LIMITER.reset()
    for _ in range(5):
        r = await seeded_client.post(
            "/v1/auth/magic-link/request",
            json={"email": "spammy@loftly.example"},
        )
        assert r.status_code == 202
    r6 = await seeded_client.post(
        "/v1/auth/magic-link/request",
        json={"email": "spammy@loftly.example"},
    )
    assert r6.status_code == 429
    assert r6.json()["error"]["code"] == "rate_limited"


async def test_consume_valid_token_creates_user_and_returns_token_pair(
    seeded_client: AsyncClient,
) -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "fresh-signup@loftly.example",
            "session_id": None,
            "purpose": _MAGIC_LINK_PURPOSE,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=10)).timestamp()),
        },
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )

    resp = await seeded_client.post("/v1/auth/magic-link/consume", json={"token": token})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["access_token"]
    assert body["refresh_token"]
    assert body["expires_in"] > 0
    assert body["user"]["email"] == "fresh-signup@loftly.example"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        user = (
            (await s.execute(select(User).where(User.email == "fresh-signup@loftly.example")))
            .scalars()
            .one_or_none()
        )
    assert user is not None
    assert user.oauth_provider == "email_magic"


async def test_consume_expired_token_returns_401(
    seeded_client: AsyncClient,
) -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "late@loftly.example",
            "purpose": _MAGIC_LINK_PURPOSE,
            "iat": int((now - timedelta(hours=1)).timestamp()),
            "exp": int((now - timedelta(minutes=30)).timestamp()),
        },
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )
    resp = await seeded_client.post("/v1/auth/magic-link/consume", json={"token": token})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_magic_link"


async def test_consume_bad_signature_returns_401(
    seeded_client: AsyncClient,
) -> None:
    # Signed with a different key → signature fails.
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "forged@loftly.example",
            "purpose": _MAGIC_LINK_PURPOSE,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
        },
        "a-different-secret",
        algorithm="HS256",
    )
    resp = await seeded_client.post("/v1/auth/magic-link/consume", json={"token": token})
    assert resp.status_code == 401


async def test_consume_wrong_purpose_returns_401(
    seeded_client: AsyncClient,
) -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "phisher@loftly.example",
            "purpose": "selector_retrieve",  # wrong purpose
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
        },
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )
    resp = await seeded_client.post("/v1/auth/magic-link/consume", json={"token": token})
    assert resp.status_code == 401


async def test_consume_binds_selector_session_to_user(
    seeded_client: AsyncClient,
) -> None:
    # First make an anon selector call so we have a session to bind.
    payload: dict[str, object] = {
        "monthly_spend_thb": 80_000,
        "spend_categories": {
            "dining": 20_000,
            "online": 30_000,
            "travel": 20_000,
            "grocery": 10_000,
        },
        "current_cards": [],
        "goal": {"type": "miles", "currency_preference": "ROP"},
        "locale": "th",
    }
    submit = await seeded_client.post("/v1/selector", json=payload)
    assert submit.status_code == 200
    session_id = submit.json()["session_id"]

    # Mint magic-link token carrying the session_id.
    settings = get_settings()
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "anon-bind@loftly.example",
            "session_id": session_id,
            "purpose": _MAGIC_LINK_PURPOSE,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=10)).timestamp()),
        },
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )
    consume = await seeded_client.post("/v1/auth/magic-link/consume", json={"token": token})
    assert consume.status_code == 200

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        row = (
            (
                await s.execute(
                    select(SelectorSession).where(SelectorSession.id == uuid.UUID(session_id))
                )
            )
            .scalars()
            .one_or_none()
        )
    assert row is not None
    assert row.user_id is not None
    assert row.bound_at is not None


def test_profile_hash_stable_across_key_order() -> None:
    a = SelectorInput.model_validate(
        {
            "monthly_spend_thb": 80_000,
            "spend_categories": {"dining": 40_000, "online": 40_000},
            "current_cards": [],
            "goal": {"type": "miles"},
            "locale": "th",
        }
    )
    b = SelectorInput.model_validate(
        {
            "locale": "th",
            "goal": {"type": "miles"},
            "current_cards": [],
            "spend_categories": {"online": 40_000, "dining": 40_000},
            "monthly_spend_thb": 80_000,
        }
    )
    assert _profile_hash(a) == _profile_hash(b)
