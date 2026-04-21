"""Rate limiter — in-memory by default, Redis-backed when `REDIS_URL` is set.

Two tiers:
- `FixedWindowLimiter` (sync): original Phase-1 implementation. Kept for the
  affiliate-click / magic-link / data-export call-sites that were written
  against it. Safe on single-process dev; trivially swappable test fixture.
- `RedisFixedWindowLimiter` (async): INCR + EXPIRE per `(key, window)` pair.
  Used when we scale to multiple Fly.io machines so the counter is shared.

Both expose `.allow(key)` / `.async_allow(key)` respectively. The selector
route uses `resolve_limiter()` to pick the right backend based on settings.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Any


class FixedWindowLimiter:
    """Simple fixed-window counter keyed on an arbitrary identity string.

    Not thread-safe; FastAPI runs one event loop per worker process so this
    is fine for our single-worker deploy. Keep it that way until we scale.
    """

    def __init__(self, *, max_calls: int, window_sec: int) -> None:
        self._max = max_calls
        self._window = window_sec
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, *, now: float | None = None) -> bool:
        """Return `True` if this call is within quota, `False` otherwise."""
        current = now if now is not None else time.monotonic()
        bucket = self._hits[key]
        # Expire old timestamps.
        while bucket and bucket[0] <= current - self._window:
            bucket.popleft()
        if len(bucket) >= self._max:
            return False
        bucket.append(current)
        return True

    def reset(self) -> None:
        """Wipe all counters (tests lean on this)."""
        self._hits.clear()


class RedisFixedWindowLimiter:
    """Redis-backed fixed-window limiter.

    Uses `INCR` against a per-window key (`rl:{name}:{key}:{window_bucket}`)
    followed by `EXPIRE` on first increment. Races on EXPIRE are fine — worst
    case we set it twice to the same TTL.
    """

    def __init__(self, *, name: str, max_calls: int, window_sec: int, redis_url: str) -> None:
        self._name = name
        self._max = max_calls
        self._window = window_sec
        self._url = redis_url
        self._client: Any | None = None  # lazy; importing redis here keeps tests fast

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import redis.asyncio as redis_async
        except ImportError as exc:  # pragma: no cover — requires optional dep
            raise RuntimeError(
                "REDIS_URL set but `redis` package missing. `uv add redis` or unset."
            ) from exc
        self._client = redis_async.from_url(self._url, decode_responses=True)
        return self._client

    async def async_allow(self, key: str) -> bool:
        client = self._ensure_client()
        bucket = int(time.time() // self._window)
        redis_key = f"rl:{self._name}:{key}:{bucket}"
        count = await client.incr(redis_key)
        if count == 1:
            await client.expire(redis_key, self._window)
        return bool(count <= self._max)

    async def reset(self) -> None:  # pragma: no cover — tests use in-memory path
        client = self._ensure_client()
        # Wipes just this limiter's namespace.
        async for key in client.scan_iter(match=f"rl:{self._name}:*"):
            await client.delete(key)


def resolve_limiter(
    name: str,
    max_calls: int,
    window_sec: int,
) -> FixedWindowLimiter | RedisFixedWindowLimiter:
    """Return Redis-backed limiter if settings.redis_url is set, else in-memory."""
    from loftly.core.settings import get_settings

    settings = get_settings()
    if settings.redis_url:
        return RedisFixedWindowLimiter(
            name=name,
            max_calls=max_calls,
            window_sec=window_sec,
            redis_url=settings.redis_url,
        )
    return FixedWindowLimiter(max_calls=max_calls, window_sec=window_sec)


# Global singleton for affiliate-click route. Tests reset via `.reset()`.
AFFILIATE_CLICK_LIMITER = FixedWindowLimiter(max_calls=10, window_sec=60)


__all__ = [
    "AFFILIATE_CLICK_LIMITER",
    "FixedWindowLimiter",
    "RedisFixedWindowLimiter",
    "resolve_limiter",
]
