"""Pydantic schemas for `/v1/waitlist` (public POST) and `/v1/admin/waitlist`
(admin list).

Kept separate from `schemas/common.py` because waitlist is Phase-1.5 scope —
the pricing-tier stub on loftly-web needs somewhere to post before Phase 2
builds the real subscription flow.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field


class WaitlistJoinRequest(BaseModel):
    """Body for `POST /v1/waitlist`.

    `source` defaults to `"pricing"` because the loftly-web pricing page is
    the only call-site today; future surfaces (coming-soon banner, blog
    footer) will pass their own source string so we can segment in the
    admin export.
    """

    email: EmailStr
    variant: str | None = Field(default=None, max_length=64)
    tier: str | None = Field(default=None, max_length=32)
    monthly_price_thb: int | None = Field(default=None, ge=0, le=100_000)
    source: str = Field(default="pricing", max_length=64)
    meta: dict[str, Any] = Field(default_factory=dict)


class WaitlistJoinResponse(BaseModel):
    """Body for a 201 response; 204 re-joins return no body."""

    id: int
    source: str
    created_at: datetime


class WaitlistRow(BaseModel):
    """Admin projection — raw email exposed deliberately because the founder
    needs it to reach out. Non-PII hashes kept as opaque strings.
    """

    id: int
    email: EmailStr
    variant: str | None
    tier: str | None
    monthly_price_thb: int | None
    source: str
    meta: dict[str, Any]
    created_at: datetime


class WaitlistList(BaseModel):
    """Envelope for `GET /v1/admin/waitlist`.

    Offset pagination (not cursor) because the founder-facing UI is a plain
    table with page numbers and the table stays under ~10k rows for the
    foreseeable future.
    """

    data: list[WaitlistRow]
    total: int
    limit: int
    offset: int
    has_more: bool
