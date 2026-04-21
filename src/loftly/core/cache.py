"""Cache abstraction — in-memory by default, Redis when `REDIS_URL` is set.

Shields the app from the Redis import when running tests or local dev without
Upstash. Interface is intentionally tiny (`get` / `set` / `delete`) because
we only cache JSON-serializable payloads on short TTLs (24h max).

Usage:
    cache = get_cache()
    hit = await cache.get("selector:abc")
    if hit is None:
        value = compute()
        await cache.set("selector:abc", value, ttl_seconds=86_400)
"""

from __future__ import annotations

import json
import time
from typing import Any, Protocol


class Cache(Protocol):
    """Minimal async cache interface. JSON-serializable values only."""

    async def get(self, key: str) -> Any | None: ...

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None: ...

    async def delete(self, key: str) -> None: ...


class InMemoryCache:
    """Process-local dict with TTL expiry. Not shared across workers.

    Fine for tests + single-worker dev. Prod uses `RedisCache` so multiple
    Fly.io workers see the same cache.
    """

    def __init__(self) -> None:
        # value, expires_at (monotonic seconds)
        self._store: dict[str, tuple[Any, float]] = {}

    async def get(self, key: str) -> Any | None:
        row = self._store.get(key)
        if row is None:
            return None
        value, expires_at = row
        if expires_at <= time.monotonic():
            self._store.pop(key, None)
            return None
        # Re-serialize/deserialize to mimic cross-process round-trip.
        return json.loads(json.dumps(value))

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._store[key] = (value, time.monotonic() + ttl_seconds)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        """Reset all entries. Tests use this to isolate between cases."""
        self._store.clear()


class RedisCache:
    """Thin wrapper around `redis.asyncio.Redis`.

    We import lazily so the app boots without the `redis` dep when `REDIS_URL`
    isn't configured. Values are JSON-encoded for portability.
    """

    def __init__(self, url: str) -> None:
        # Lazy import: `redis` is not a hard dependency Phase 1.
        try:
            import redis.asyncio as redis_async  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover — requires redis optional
            raise RuntimeError(
                "REDIS_URL is set but the `redis` package is not installed. "
                "Add it with `uv add redis` or unset REDIS_URL."
            ) from exc
        self._client: Any = redis_async.from_url(url, decode_responses=True)

    async def get(self, key: str) -> Any | None:
        raw = await self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        await self._client.set(key, json.dumps(value), ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self._client.delete(key)


_CACHE: Cache | None = None


def get_cache() -> Cache:
    """Return the process-wide cache singleton.

    Picks `RedisCache` when `REDIS_URL` is configured, else `InMemoryCache`.
    Tests override via `set_cache()`.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    from loftly.core.settings import get_settings

    settings = get_settings()
    _CACHE = RedisCache(settings.redis_url) if settings.redis_url else InMemoryCache()
    return _CACHE


def set_cache(cache: Cache | None) -> None:
    """Override the cache singleton (used by lifespan init + tests)."""
    global _CACHE
    _CACHE = cache


__all__ = ["Cache", "InMemoryCache", "RedisCache", "get_cache", "set_cache"]
