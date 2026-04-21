"""Typed accessors for selector session cache.

Three keys per session (all 24h TTL):
  - meta: {card_name, card_id, profile_hash, last_seen_at}
    — written by _run_with_fallback, read by /v1/selector/recent + email composer
  - chat_count: int (§1 rate-limit cap = 10)
  - context: cached 50k-token block (§1 reuse)

Why a wrapper: `loftly.core.cache.Cache` is a deliberately tiny Protocol
(`get`/`set`/`delete`) shared across the app. This module centralises the three
POST_V1 §1-§3 keys so callers don't rebuild the namespace convention nor the
24h TTL math. Rename is emulated via get+set+delete because the Protocol does
not expose a native `RENAME` — that is sufficient for the archive use case
(a single slow background call on "start over", not a hot path).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal

from loftly.core.cache import get_cache

_TTL_SECONDS = 86_400
_CHAT_CAP = 10

_KeySuffix = Literal["meta", "chat_count", "context"]


@dataclass
class SessionMeta:
    """Non-sensitive session snapshot for personalized landing + email composer.

    Intentionally omits PII (no email, no profile body). `profile_hash` is an
    opaque fingerprint the §1 chat path uses to decide whether a cached
    50k-token context is still valid for a revised profile.
    """

    card_name: str
    card_id: str
    profile_hash: str
    last_seen_at: str  # ISO 8601 with tz

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> SessionMeta:
        data = json.loads(raw)
        return cls(
            card_name=data["card_name"],
            card_id=data["card_id"],
            profile_hash=data["profile_hash"],
            last_seen_at=data["last_seen_at"],
        )


def _key(session_id: str, suffix: _KeySuffix) -> str:
    """Namespaced cache key for one session slot."""
    return f"selector:session:{session_id}:{suffix}"


def _archived_key(session_id: str, ts: str) -> str:
    """Post-archive key preserving original session_id + archive timestamp."""
    return f"selector:session:archived:{session_id}:{ts}"


def _now_iso() -> str:
    """UTC ISO-8601 second precision, safe for key suffixes."""
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# meta
# ---------------------------------------------------------------------------


async def write_session_meta(session_id: str, meta: SessionMeta) -> None:
    """Persist `SessionMeta` under `selector:session:{id}:meta` with 24h TTL.

    Idempotent — callers may re-write on every result page render.
    """
    cache = get_cache()
    # Store as a JSON-serialisable dict so both InMemoryCache + RedisCache
    # round-trip cleanly.
    await cache.set(_key(session_id, "meta"), asdict(meta), ttl_seconds=_TTL_SECONDS)


async def read_session_meta(session_id: str) -> SessionMeta | None:
    """Fetch `SessionMeta` or `None` if the slot is empty / expired."""
    cache = get_cache()
    raw = await cache.get(_key(session_id, "meta"))
    if raw is None:
        return None
    if isinstance(raw, str):
        return SessionMeta.from_json(raw)
    # dict path (both InMemoryCache + RedisCache return dicts after json.loads)
    return SessionMeta(
        card_name=raw["card_name"],
        card_id=raw["card_id"],
        profile_hash=raw["profile_hash"],
        last_seen_at=raw["last_seen_at"],
    )


async def archive_session(session_id: str) -> bool:
    """Rename `selector:session:{id}:meta` → `selector:session:archived:{id}:{ts}`.

    The Protocol in `loftly.core.cache` does not expose a native RENAME, so we
    implement it as get+set+delete. This is fine for the §3 "ทำ Selector ใหม่"
    flow: a single call at most once per session, off the hot path. The 24h
    TTL is preserved because `set(..., ttl_seconds=_TTL_SECONDS)` re-arms it
    on the destination key.

    Returns True if there was something to archive, False otherwise (caller
    can decide whether that's an error or a no-op).
    """
    cache = get_cache()
    src = _key(session_id, "meta")
    existing = await cache.get(src)
    if existing is None:
        return False
    dst = _archived_key(session_id, _now_iso())
    await cache.set(dst, existing, ttl_seconds=_TTL_SECONDS)
    await cache.delete(src)
    return True


# ---------------------------------------------------------------------------
# chat_count
# ---------------------------------------------------------------------------


async def increment_chat_count(session_id: str) -> int:
    """Bump the chat-message counter. Returns the new value.

    "Atomic-ish" — the underlying `Cache` Protocol doesn't expose INCR, so we
    read-modify-write through `get`/`set`. Two concurrent callers could both
    read N and both write N+1 (losing one increment). For the §1 chat cap of
    10 messages per session, this is acceptable: the worst case is that a user
    sends an 11th message before the 10th has persisted, which an idempotent
    upper check in the route layer catches on the next request.

    Every increment re-arms the 24h TTL so the counter dies with the session.
    """
    cache = get_cache()
    k = _key(session_id, "chat_count")
    current = await cache.get(k)
    if current is None:
        new_value = 1
    else:
        try:
            new_value = int(current) + 1
        except (TypeError, ValueError):
            # Corrupted value — reset to 1 rather than throw; the cap check in
            # the route layer will still enforce the 10-message ceiling.
            new_value = 1
    await cache.set(k, new_value, ttl_seconds=_TTL_SECONDS)
    return new_value


async def get_chat_count(session_id: str) -> int:
    """Current message count for this session. Zero when absent."""
    cache = get_cache()
    raw = await cache.get(_key(session_id, "chat_count"))
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def chat_cap() -> int:
    """Per-session cap enforced by `POST /v1/selector/{id}/chat`."""
    return _CHAT_CAP


# ---------------------------------------------------------------------------
# context (50k-token reuse block)
# ---------------------------------------------------------------------------


async def write_context(session_id: str, context: str) -> None:
    """Cache the 50k-token context block the §1 Haiku call reuses across turns."""
    cache = get_cache()
    await cache.set(_key(session_id, "context"), context, ttl_seconds=_TTL_SECONDS)


async def read_context(session_id: str) -> str | None:
    """Return the cached context block, or `None` on miss."""
    cache = get_cache()
    raw = await cache.get(_key(session_id, "context"))
    if raw is None:
        return None
    return str(raw)


__all__ = [
    "SessionMeta",
    "archive_session",
    "chat_cap",
    "get_chat_count",
    "increment_chat_count",
    "read_context",
    "read_session_meta",
    "write_context",
    "write_session_meta",
]
