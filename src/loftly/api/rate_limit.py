"""In-memory rate limiter for Phase 1.

We run on a single Fly.io instance; a dict keyed on client fingerprint is
safe here. When we scale horizontally we'll swap this for a Redis-backed
sliding window (the interface stays the same).

Usage:
    limiter = FixedWindowLimiter(max_calls=10, window_sec=60)
    if not limiter.allow("1.2.3.4"):
        raise rate_limit_exceeded()
"""

from __future__ import annotations

import time
from collections import defaultdict, deque


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


# Global singleton for affiliate-click route. Tests reset via `.reset()`.
AFFILIATE_CLICK_LIMITER = FixedWindowLimiter(max_calls=10, window_sec=60)


__all__ = ["AFFILIATE_CLICK_LIMITER", "FixedWindowLimiter"]
