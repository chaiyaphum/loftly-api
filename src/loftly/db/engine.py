"""Async SQLAlchemy engine + session factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loftly.core.settings import Settings, get_settings


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    """Process-wide async engine, built lazily from settings."""
    settings: Settings = get_settings()
    # SQLite (test) can't share the connection pool the way Postgres does.
    connect_args: dict[str, object] = {}
    if settings.database_url.startswith("sqlite+aiosqlite"):
        connect_args = {"check_same_thread": False}

    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


@lru_cache(maxsize=1)
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=get_engine(),
        expire_on_commit=False,
        autoflush=False,
    )


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: yields a transactional session."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        yield session
