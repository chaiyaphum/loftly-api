"""Tests for `loftly.selector.session_cache` — typed accessors over the cache Protocol.

Uses `InMemoryCache` via the `set_cache` override so we don't need Redis.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio

from loftly.core.cache import InMemoryCache, set_cache
from loftly.selector.session_cache import (
    SessionMeta,
    archive_session,
    chat_cap,
    get_chat_count,
    increment_chat_count,
    read_context,
    read_session_meta,
    write_context,
    write_session_meta,
)


@pytest_asyncio.fixture
async def fresh_cache() -> AsyncIterator[InMemoryCache]:
    """Swap in a clean InMemoryCache for each test."""
    cache = InMemoryCache()
    set_cache(cache)
    try:
        yield cache
    finally:
        cache.clear()
        set_cache(None)


def _meta(session_id: str = "s1") -> SessionMeta:
    return SessionMeta(
        card_name="KBank Platinum",
        card_id="kbank-platinum",
        profile_hash="abc123",
        last_seen_at="2026-04-21T09:00:00+00:00",
    )


async def test_write_and_read_session_meta_roundtrip(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    meta = _meta()
    await write_session_meta("s1", meta)

    got = await read_session_meta("s1")
    assert got is not None
    assert got.card_name == "KBank Platinum"
    assert got.card_id == "kbank-platinum"
    assert got.profile_hash == "abc123"
    assert got.last_seen_at == "2026-04-21T09:00:00+00:00"


async def test_read_session_meta_missing_returns_none(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    assert await read_session_meta("missing") is None


async def test_idempotent_overwrite(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    first = _meta()
    await write_session_meta("s1", first)
    second = SessionMeta(
        card_name="SCB PRIME",
        card_id="scb-prime",
        profile_hash="def456",
        last_seen_at="2026-04-21T10:00:00+00:00",
    )
    await write_session_meta("s1", second)

    got = await read_session_meta("s1")
    assert got is not None
    assert got.card_id == "scb-prime"


async def test_archive_session_renames_meta_key(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    await write_session_meta("s1", _meta())
    result = await archive_session("s1")
    assert result is True

    # Source key gone.
    assert await read_session_meta("s1") is None
    # Archived key exists under the archived namespace — look it up via the raw cache.
    keys = list(fresh_cache._store.keys())
    archived_keys = [k for k in keys if k.startswith("selector:session:archived:s1:")]
    assert len(archived_keys) == 1


async def test_archive_session_returns_false_when_absent(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    assert await archive_session("nonexistent") is False


async def test_archive_session_preserves_ttl(fresh_cache: InMemoryCache) -> None:
    """The archived key should carry a 24h TTL, not be evicted immediately."""
    import time

    _ = fresh_cache
    await write_session_meta("s1", _meta())
    await archive_session("s1")

    archived_keys = [k for k in fresh_cache._store if k.startswith("selector:session:archived:s1:")]
    assert archived_keys, "archive should produce exactly one archived key"
    _, expires_at = fresh_cache._store[archived_keys[0]]
    # TTL should be roughly 24h ahead. Allow generous slack so CI timing is stable.
    remaining = expires_at - time.monotonic()
    assert 86_300 < remaining <= 86_400


async def test_increment_chat_count_from_zero(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    assert await get_chat_count("s1") == 0
    assert await increment_chat_count("s1") == 1
    assert await increment_chat_count("s1") == 2
    assert await increment_chat_count("s1") == 3
    assert await get_chat_count("s1") == 3


async def test_chat_cap_is_ten() -> None:
    assert chat_cap() == 10


async def test_write_and_read_context(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    payload = "a" * 100_000  # stand-in for a 50k-token block
    await write_context("s1", payload)

    got = await read_context("s1")
    assert got == payload


async def test_read_context_missing(fresh_cache: InMemoryCache) -> None:
    _ = fresh_cache
    assert await read_context("missing") is None


async def test_session_keys_are_namespaced(fresh_cache: InMemoryCache) -> None:
    """Ensure every accessor writes under selector:session:{id}:* so ops can
    grep / TTL-scan them cleanly."""
    _ = fresh_cache
    await write_session_meta("s1", _meta())
    await increment_chat_count("s1")
    await write_context("s1", "ctx")

    keys = list(fresh_cache._store.keys())
    assert "selector:session:s1:meta" in keys
    assert "selector:session:s1:chat_count" in keys
    assert "selector:session:s1:context" in keys
