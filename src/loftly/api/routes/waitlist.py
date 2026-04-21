"""Waitlist endpoints — public POST + admin list (W24).

Flow on `POST /v1/waitlist`:
1. Rate-limit by client IP (10 / 5 min). Backend is Upstash Redis when
   `REDIS_URL` is set, in-memory fallback otherwise — shared helper
   `resolve_limiter()` owns that switch.
2. Hash the client IP + user-agent with SHA-256 (hex). The raw values are
   PDPA-sensitive, and the hashed pair is enough for forensics without
   letting us de-anonymize a leaker.
3. Check `uq_waitlist_email_source` — if the pair already exists, return
   204 (idempotent re-join). Otherwise insert and return 201.
4. Fire PostHog `waitlist_joined` (no-op in dev/test) and append an audit
   row with `{source, variant, tier}` — **no email** because the audit log
   has a longer retention than waitlist entries under PDPA.

Flow on `GET /v1/admin/waitlist`:
- Admin JWT required.
- Offset pagination (limit/offset) with `total` so the founder UI can
  render "Page N of M". Sorted `created_at DESC` so freshest rows come first.
- Optional `source` filter for segmenting.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import (
    FixedWindowLimiter,
    RedisFixedWindowLimiter,
    resolve_limiter,
)
from loftly.db.audit import log_action
from loftly.db.engine import get_session
from loftly.db.models.waitlist import Waitlist
from loftly.observability.posthog import capture, hash_distinct_id
from loftly.schemas.waitlist import (
    WaitlistJoinRequest,
    WaitlistJoinResponse,
    WaitlistList,
    WaitlistRow,
)

router = APIRouter(tags=["waitlist"])

# 10 req / 5 min / IP. Resolved on first use so tests that swap the
# REDIS_URL environment var between cases see the right backend.
_WAITLIST_LIMITER: FixedWindowLimiter | RedisFixedWindowLimiter | None = None
_WAITLIST_LIMIT_MAX = 10
_WAITLIST_LIMIT_WINDOW_SEC = 5 * 60

# Stable actor for the audit row — the public endpoint has no user JWT, so
# we attribute the row to the seeded system user (migration 012) exactly
# like `routes/webhooks.py` does for rejected postbacks.
SYSTEM_USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


def _get_limiter() -> FixedWindowLimiter | RedisFixedWindowLimiter:
    """Lazy singleton so `REDIS_URL` changes between tests are honored."""
    global _WAITLIST_LIMITER
    if _WAITLIST_LIMITER is None:
        _WAITLIST_LIMITER = resolve_limiter(
            "waitlist",
            max_calls=_WAITLIST_LIMIT_MAX,
            window_sec=_WAITLIST_LIMIT_WINDOW_SEC,
        )
    return _WAITLIST_LIMITER


def reset_limiter() -> None:
    """Drop the cached limiter — used by tests that swap backends mid-run."""
    global _WAITLIST_LIMITER
    _WAITLIST_LIMITER = None


def _client_ip(request: Request) -> str:
    """Best-effort client IP. Upstream of this we trust the reverse proxy
    set `request.client.host` correctly — Fly.io handles this, and in tests
    the ASGI transport fills in `testclient` so dedupe still works.
    """
    return request.client.host if request.client else "unknown"


def _sha256_hex(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _allow(limiter: FixedWindowLimiter | RedisFixedWindowLimiter, key: str) -> bool:
    """Unified allow() — the two limiters have different signatures."""
    if isinstance(limiter, RedisFixedWindowLimiter):
        return await limiter.async_allow(key)
    return limiter.allow(key)


@router.post(
    "/v1/waitlist",
    summary="Join the waitlist (public, email capture)",
    status_code=status.HTTP_201_CREATED,
    response_model=WaitlistJoinResponse,
    responses={
        204: {"description": "Already on the waitlist for this source (idempotent)."},
        429: {"description": "Rate limited (10 req / 5 min / IP)."},
    },
)
async def join_waitlist(
    payload: WaitlistJoinRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Capture a waitlist entry. 201 on first join, 204 on duplicate."""
    ip = _client_ip(request)
    if not await _allow(_get_limiter(), ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many waitlist requests — please try again later.",
            message_th="ขอเข้าคิวถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )

    ip_hash = _sha256_hex(ip)
    ua_hash = _sha256_hex(request.headers.get("user-agent"))

    # Idempotency check — (email, source) is unique at the DB level, but we
    # look up first so we can return 204 cleanly instead of catching IntegrityError.
    existing = (
        await session.execute(
            select(Waitlist).where(
                Waitlist.email == payload.email,
                Waitlist.source == payload.source,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    row = Waitlist(
        email=payload.email,
        variant=payload.variant,
        tier=payload.tier,
        monthly_price_thb=payload.monthly_price_thb,
        source=payload.source,
        meta=payload.meta or {},
        ip_hash=ip_hash,
        user_agent_hash=ua_hash,
    )
    session.add(row)

    # Audit the join WITHOUT storing the raw email — PII-minimizing.
    # Retention: audit_log is purged at N days per migration 013; the
    # waitlist table itself is the long-lived copy.
    await log_action(
        session,
        actor_id=SYSTEM_USER_ID,
        action="waitlist.joined",
        subject_type="waitlist",
        subject_id=None,
        metadata={
            "source": payload.source,
            "variant": payload.variant,
            "tier": payload.tier,
            "monthly_price_thb": payload.monthly_price_thb,
        },
    )
    await session.commit()
    await session.refresh(row)

    # Fire PostHog event. Hashed distinct_id — we never pass raw email out.
    await capture(
        "waitlist_joined",
        distinct_id=hash_distinct_id(payload.email, salt="waitlist"),
        properties={
            "source": payload.source,
            "variant": payload.variant,
            "tier": payload.tier,
            "monthly_price_thb": payload.monthly_price_thb,
        },
    )

    body = WaitlistJoinResponse(
        id=int(row.id),
        source=row.source,
        created_at=row.created_at,
    )
    return Response(
        status_code=status.HTTP_201_CREATED,
        content=body.model_dump_json(),
        media_type="application/json",
    )


# ---------------------------------------------------------------------------
# Admin — list entries for founder export
# ---------------------------------------------------------------------------


@router.get(
    "/v1/admin/waitlist",
    summary="List waitlist entries (admin, paginated)",
    response_model=WaitlistList,
)
async def list_waitlist(
    source: str | None = Query(default=None, description="Filter by capture source"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> WaitlistList:
    """Return paginated waitlist rows, newest first.

    The founder exports this to the data room via the admin UI. Rows include
    raw `email` because the whole point is outreach; the admin JWT + gate in
    front of this path is what keeps that PII from leaking.
    """
    filters: list[Any] = []
    if source:
        filters.append(Waitlist.source == source)

    total_stmt = select(func.count(Waitlist.id))
    if filters:
        total_stmt = total_stmt.where(*filters)
    total = int((await session.execute(total_stmt)).scalar_one())

    stmt = select(Waitlist)
    if filters:
        stmt = stmt.where(*filters)
    stmt = stmt.order_by(Waitlist.created_at.desc(), Waitlist.id.desc()).offset(offset).limit(limit)

    rows = list((await session.execute(stmt)).scalars().all())

    return WaitlistList(
        data=[
            WaitlistRow(
                id=int(r.id),
                email=r.email,
                variant=r.variant,
                tier=r.tier,
                monthly_price_thb=r.monthly_price_thb,
                source=r.source,
                meta=r.meta or {},
                created_at=r.created_at,
            )
            for r in rows
        ],
        total=total,
        limit=limit,
        offset=offset,
        has_more=(offset + len(rows)) < total,
    )


__all__ = ["SYSTEM_USER_ID", "reset_limiter", "router"]
