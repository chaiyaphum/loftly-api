"""Affiliate click tracking — `POST /v1/affiliate/click/{card_id}`.

Flow:
1. Resolve the active affiliate_link row for (card_id, any partner_id). Phase 1
   picks the most-recently-created one. Future phases will pick by predicted
   commission or by user affinity.
2. Insert an `affiliate_clicks` row (click_id = new uuid). The click_id is what
   partner postbacks will echo back in the webhook.
3. Render the partner URL by interpolating `{click_id}` into `url_template`.
4. 302 to the partner URL and set a first-party `loftly_click_id` cookie so the
   user journey stays attributable even through SPA navigations.

Rate limit: 10/min per ip_hash. In-memory token bucket (single-instance Fly.io).
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Literal
from urllib.parse import quote

from fastapi import APIRouter, Depends, Query, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import AFFILIATE_CLICK_LIMITER
from loftly.db.engine import get_session
from loftly.db.models.affiliate import AffiliateClick, AffiliateLink
from loftly.db.models.card import Card as CardModel

router = APIRouter(prefix="/v1/affiliate", tags=["affiliate"])

Placement = Literal["review", "selector_result", "cards_index", "promo"]

CLICK_COOKIE_NAME = "loftly_click_id"
CLICK_COOKIE_MAX_AGE = 86_400  # 24h


def _hash_ip(ip: str | None) -> bytes | None:
    if not ip:
        return None
    return hashlib.sha256(ip.encode("utf-8")).digest()


def _render_url(template: str, *, click_id: uuid.UUID, utm_campaign: str | None) -> str:
    out = template.replace("{click_id}", quote(str(click_id), safe=""))
    if utm_campaign:
        out = out.replace("{utm_campaign}", quote(utm_campaign, safe=""))
    return out


@router.post(
    "/click/{card_id}",
    summary="Record click and 302 to partner URL",
    status_code=status.HTTP_302_FOUND,
)
async def record_click(
    card_id: uuid.UUID,
    request: Request,
    placement: Placement = Query(..., description="Surface that produced the click"),
    utm_campaign: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Rate-limited; returns 302 with `Location` + sets the click-id cookie."""
    # Rate-limit first — cheap check, avoids DB round-trips on abuse.
    client_ip = request.client.host if request.client else "unknown"
    ip_hash = _hash_ip(client_ip)
    if not AFFILIATE_CLICK_LIMITER.allow(client_ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many clicks — please slow down.",
            message_th="คลิกถี่เกินไป กรุณาลองใหม่อีกครั้ง",
        )

    # Confirm the card exists (nicer 404 than silently 404ing on "no link").
    card = (
        (await session.execute(select(CardModel).where(CardModel.id == card_id)))
        .scalars()
        .unique()
        .one_or_none()
    )
    if card is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with id {card_id}.",
            message_th="ไม่พบบัตรที่ระบุ",
            details={"card_id": str(card_id)},
        )

    link_stmt = (
        select(AffiliateLink)
        .where(AffiliateLink.card_id == card_id)
        .where(AffiliateLink.active.is_(True))
        # MVP policy: most recently configured link wins. Tune once we have data.
        .order_by(AffiliateLink.id.desc())
        .limit(1)
    )
    link = (await session.execute(link_stmt)).scalars().one_or_none()
    if link is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="no_active_affiliate_link",
            message_en="No active signup channel is configured for this card.",
            message_th="ยังไม่มีช่องทางสมัครสำหรับบัตรนี้",
            details={"card_id": str(card_id)},
        )

    click_id = uuid.uuid4()
    click = AffiliateClick(
        click_id=click_id,
        user_id=None,  # anonymous clicks permitted
        affiliate_link_id=link.id,
        card_id=card_id,
        partner_id=link.partner_id,
        placement=placement,
        utm_campaign=utm_campaign,
        referrer=request.headers.get("referer"),
        ip_hash=ip_hash,
        user_agent=request.headers.get("user-agent"),
    )
    session.add(click)
    await session.commit()

    location = _render_url(
        link.url_template,
        click_id=click_id,
        utm_campaign=utm_campaign,
    )

    response = Response(status_code=status.HTTP_302_FOUND)
    response.headers["Location"] = location
    response.set_cookie(
        key=CLICK_COOKIE_NAME,
        value=str(click_id),
        max_age=CLICK_COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


__all__ = ["router"]
