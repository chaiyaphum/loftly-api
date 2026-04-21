"""Rate-limiter tests — both the in-memory fallback and Redis-backed impl."""

from __future__ import annotations

from typing import Any

import pytest

from loftly.api.rate_limit import (
    FixedWindowLimiter,
    RedisFixedWindowLimiter,
    resolve_limiter,
)
from loftly.core.settings import get_settings


def test_resolve_limiter_defaults_to_in_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    get_settings.cache_clear()
    lim = resolve_limiter("test", max_calls=5, window_sec=60)
    assert isinstance(lim, FixedWindowLimiter)


def test_resolve_limiter_switches_to_redis_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    get_settings.cache_clear()
    lim = resolve_limiter("test", max_calls=5, window_sec=60)
    assert isinstance(lim, RedisFixedWindowLimiter)


async def test_redis_limiter_allows_then_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the redis client so no real Redis is required in CI."""

    class _FakeRedis:
        def __init__(self) -> None:
            self.counts: dict[str, int] = {}
            self.expires: dict[str, int] = {}

        async def incr(self, key: str) -> int:
            self.counts[key] = self.counts.get(key, 0) + 1
            return self.counts[key]

        async def expire(self, key: str, ttl: int) -> None:
            self.expires[key] = ttl

    fake = _FakeRedis()

    import redis.asyncio as redis_async  # type: ignore[import-not-found]

    def _from_url(_url: str, **_kw: Any) -> Any:
        return fake

    monkeypatch.setattr(redis_async, "from_url", _from_url)

    lim = RedisFixedWindowLimiter(name="t", max_calls=2, window_sec=60, redis_url="redis://fake")
    assert await lim.async_allow("ip-1") is True
    assert await lim.async_allow("ip-1") is True
    assert await lim.async_allow("ip-1") is False  # capped
    assert await lim.async_allow("ip-2") is True  # different key still clears
