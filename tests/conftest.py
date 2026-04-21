"""Pytest fixtures — aiosqlite test DB + httpx AsyncClient against the FastAPI app."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

# Force test settings *before* any `loftly.*` import evaluates environment.
os.environ.setdefault("LOFTLY_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("JWT_SIGNING_KEY", "test-secret")
os.environ.setdefault(
    "AFFILIATE_PARTNER_SECRETS",
    '{"test-partner": "shhh-test-secret"}',
)

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from loftly.api.app import create_app
from loftly.api.auth import get_current_user_id
from loftly.core.settings import get_settings
from loftly.db.engine import get_engine, get_session, get_sessionmaker
from loftly.db.models import Base
from loftly.db.models.user import User
from loftly.db.seed import seed_all

# A stable user UUID the consent tests pretend to be authenticated as. Inserted
# into the `users` table by the `seeded_db` fixture so FK-ed rows can reference
# it without violating the users_id FK.
TEST_USER_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
TEST_ADMIN_ID = uuid.UUID("00000000-0000-4000-8000-000000000009")
# Matches `SYSTEM_USER_ID` in `routes/webhooks.py` + migration 012. Inserted
# by the `seeded_db` fixture so audit rows on webhook rejection don't trip the
# FK constraint under SQLite tests.
SYSTEM_USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    """Ensure each test sees a fresh settings snapshot + clean rate-limit state."""
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()

    # Clear in-memory rate-limiter between tests (it's module-global).
    from loftly.api.rate_limit import AFFILIATE_CLICK_LIMITER
    from loftly.api.routes.account import DATA_EXPORT_LIMITER
    from loftly.api.routes.auth import MAGIC_LINK_LIMITER
    from loftly.api.routes.waitlist import reset_limiter as reset_waitlist_limiter

    AFFILIATE_CLICK_LIMITER.reset()
    MAGIC_LINK_LIMITER.reset()
    DATA_EXPORT_LIMITER.reset()
    reset_waitlist_limiter()

    # Cache + provider singletons are process-global; reset so each test
    # picks up fresh state from settings.
    from loftly.ai import set_provider
    from loftly.core.cache import set_cache

    set_cache(None)
    set_provider(None)


@pytest_asyncio.fixture
async def app() -> AsyncIterator[object]:
    """Build a fresh FastAPI app + create aiosqlite schema for tests that need it."""
    instance = create_app()
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Always bypass real JWT in tests — `get_current_user_id` returns TEST_USER_ID.
    instance.dependency_overrides[get_current_user_id] = lambda: TEST_USER_ID  # type: ignore[attr-defined]

    try:
        yield instance
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_db(app: object) -> AsyncIterator[object]:
    """Seed banks + currencies + sample cards + the test users. Depends on `app`."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Insert the fake authenticated users first so later FKs (consent,
        # audit_log) work.
        session.add(
            User(
                id=TEST_USER_ID,
                email="test@loftly.test",
                oauth_provider="google",
                oauth_subject="test-subject",
            )
        )
        session.add(
            User(
                id=TEST_ADMIN_ID,
                email="admin@loftly.test",
                oauth_provider="google",
                oauth_subject="admin-subject",
                role="admin",
            )
        )
        session.add(
            User(
                id=SYSTEM_USER_ID,
                email="system@loftly.co.th",
                oauth_provider="email_magic",
                oauth_subject="__system__",
                role="admin",
            )
        )
        await session.commit()
        await seed_all(session)
    yield app


@pytest_asyncio.fixture
async def client(app: object) -> AsyncIterator[AsyncClient]:
    """Async HTTP client bound to the app via ASGI transport (no network)."""
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def seeded_client(seeded_db: object) -> AsyncIterator[AsyncClient]:
    """Async HTTP client with the catalog + test user pre-seeded."""
    transport = ASGITransport(app=seeded_db)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _mint_pair(client: AsyncClient, *, user_id: uuid.UUID, role: str) -> dict[str, str]:
    """Hit the test-only token issuer and return auth headers {"Authorization": ...}."""
    resp = await client.post(
        "/v1/auth/_test/issue",
        json={"user_id": str(user_id), "role": role, "locale": "th"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    return {"Authorization": f"Bearer {body['access_token']}"}


@pytest_asyncio.fixture
async def admin_headers(seeded_client: AsyncClient) -> dict[str, str]:
    """Auth header for a real admin JWT minted via /v1/auth/_test/issue."""
    return await _mint_pair(seeded_client, user_id=TEST_ADMIN_ID, role="admin")


@pytest_asyncio.fixture
async def user_headers(seeded_client: AsyncClient) -> dict[str, str]:
    """Auth header for a real non-admin JWT."""
    return await _mint_pair(seeded_client, user_id=TEST_USER_ID, role="user")


# Re-export for test modules that want direct session access.
__all__ = ["SYSTEM_USER_ID", "TEST_ADMIN_ID", "TEST_USER_ID", "get_session"]
