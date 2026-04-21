"""Pytest fixtures — aiosqlite test DB + httpx AsyncClient against the FastAPI app."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

# Force test settings *before* any `loftly.*` import evaluates environment.
os.environ.setdefault("LOFTLY_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("JWT_SIGNING_KEY", "test-secret")

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from loftly.api.app import create_app
from loftly.core.settings import get_settings
from loftly.db.engine import get_engine, get_sessionmaker
from loftly.db.models import Base


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
    try:
        yield instance
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture
async def client(app: object) -> AsyncIterator[AsyncClient]:
    """Async HTTP client bound to the app via ASGI transport (no network)."""
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
