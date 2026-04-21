"""`GET /v1/me` — account settings identity echo.

Covers:
- Unauthenticated requests get 401 (no bearer token).
- Authenticated requests get 200 + the full field set.
- No PII beyond the documented fields leaks into the response.
- `last_login_at` is null on first login and populated after a subsequent
  token issuance (magic-link consume path).
- `locale` reflects `users.preferred_locale` (default `th`, overridable).
"""

from __future__ import annotations

from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from loftly.api.auth import get_current_user_id
from loftly.db.engine import get_sessionmaker
from loftly.db.models.user import User
from tests.conftest import TEST_USER_ID

# The exact response keys we expose. If you add a field to MeResponse, update
# this set — test_me_no_extra_pii will fail on drift, which is the point.
_EXPECTED_KEYS = {
    "id",
    "email",
    "email_verified",
    "created_at",
    "last_login_at",
    "locale",
    "auth_provider",
}


async def test_me_unauthenticated_returns_401(seeded_db: object) -> None:
    """No bearer header -> 401 with the Loftly error envelope."""
    # The `app` fixture auto-overrides get_current_user_id for convenience.
    # Clear it so we exercise the real JWT-requiring dependency here.
    seeded_db.dependency_overrides.pop(get_current_user_id, None)  # type: ignore[attr-defined]

    transport = ASGITransport(app=seeded_db)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/v1/me")
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "unauthorized"


async def test_me_invalid_token_returns_401(seeded_db: object) -> None:
    """Malformed bearer token -> 401."""
    seeded_db.dependency_overrides.pop(get_current_user_id, None)  # type: ignore[attr-defined]

    transport = ASGITransport(app=seeded_db)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v1/me", headers={"Authorization": "Bearer not-a-real-jwt"}
        )
    assert resp.status_code == 401


async def test_me_returns_profile_with_expected_fields(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Signed-in user sees exactly the documented fields — no more, no less."""
    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert set(body.keys()) == _EXPECTED_KEYS
    assert body["id"] == str(TEST_USER_ID)
    assert body["email"] == "test@loftly.test"
    # TEST_USER_ID is seeded with oauth_provider='google' -> trusted, verified.
    assert body["email_verified"] is True
    assert body["auth_provider"] == "google"
    assert body["locale"] == "th"
    # Timestamps present + parseable.
    assert body["created_at"] is not None
    datetime.fromisoformat(body["created_at"].replace("Z", "+00:00"))


async def test_me_no_extra_pii_beyond_contract(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Response must NOT leak phone, role, oauth_subject, or deleted_at."""
    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200
    body = resp.json()
    for leak in ("phone", "role", "oauth_subject", "deleted_at", "preferred_locale"):
        assert leak not in body, f"field `{leak}` leaked to /v1/me response"


async def test_me_last_login_null_on_fresh_account(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Seeded test user has never logged in via a token-issuance path -> null."""
    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200
    assert resp.json()["last_login_at"] is None


async def test_me_last_login_populated_after_magic_link_consume(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """After the magic-link flow stamps last_login_at, /v1/me surfaces it."""
    # Directly stamp the column — the magic-link consume path does exactly this
    # and we don't want to depend on the full email flow in this test.
    sessionmaker = get_sessionmaker()
    stamped = datetime.now(UTC)
    async with sessionmaker() as session:
        user = (
            await session.execute(select(User).where(User.id == TEST_USER_ID))
        ).scalars().one()
        user.last_login_at = stamped
        await session.commit()

    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200
    last = resp.json()["last_login_at"]
    assert last is not None
    # SQLite strips tz on roundtrip; re-attach UTC for a safe subtraction.
    parsed = datetime.fromisoformat(last.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    # Allow a wide tolerance — SQLite timestamp roundtrip is second-level.
    assert abs((parsed - stamped).total_seconds()) < 5


async def test_me_locale_reflects_preferred_locale(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Default is 'th'; flipping the column to 'en' is echoed back."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (
            await session.execute(select(User).where(User.id == TEST_USER_ID))
        ).scalars().one()
        user.preferred_locale = "en"
        await session.commit()

    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200
    assert resp.json()["locale"] == "en"


async def test_me_magic_link_user_is_verified(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Users arriving via magic link count as email_verified.

    Rationale: they had to click a link in their inbox to exist, so the email
    is demonstrably theirs — same verification bar as Google/Apple/Line.
    """
    # Flip the seeded TEST_USER_ID row to an email_magic provider and observe
    # the computed verification bit follow. Doing it on the existing row keeps
    # the `get_current_user_id` override from conftest relevant.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (
            await session.execute(select(User).where(User.id == TEST_USER_ID))
        ).scalars().one()
        user.oauth_provider = "email_magic"
        await session.commit()

    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["email_verified"] is True
    assert body["auth_provider"] == "email_magic"


async def test_me_deleted_user_returns_404(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Token valid but user soft-deleted -> 404 (client should drop tokens)."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (
            await session.execute(select(User).where(User.id == TEST_USER_ID))
        ).scalars().one()
        user.deleted_at = datetime.now(UTC)
        await session.commit()

    resp = await seeded_client.get("/v1/me", headers=user_headers)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "user_not_found"
