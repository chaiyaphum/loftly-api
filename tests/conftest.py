"""Pytest fixtures — aiosqlite test DB + httpx AsyncClient against the FastAPI app."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

# Force test settings *before* any `loftly.*` import evaluates environment.
os.environ.setdefault("LOFTLY_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("JWT_SIGNING_KEY", "test-secret")

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


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    """Ensure each test sees a fresh settings snapshot."""
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()


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
    """Seed banks + currencies + sample cards + the test user. Depends on `app`."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Insert the fake authenticated user first so later FKs (consent) work.
        session.add(
            User(
                id=TEST_USER_ID,
                email="test@loftly.test",
                oauth_provider="google",
                oauth_subject="test-subject",
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


# Re-export for test modules that want direct session access.
__all__ = ["TEST_USER_ID", "get_session"]
