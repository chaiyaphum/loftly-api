"""Affiliate click endpoint — Week 4 Backend scope.

Seeds an `affiliate_links` row tied to the `kbank-wisdom` card and exercises:
- 302 redirect with Location + click_id cookie set
- 404 when no active link exists for a card
- 404 when card itself is unknown
- rate limit kicks in at 11th request (10/min window)
"""

from __future__ import annotations

import uuid

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateClick, AffiliateLink
from loftly.db.models.card import Card as CardModel


@pytest_asyncio.fixture
async def click_client(seeded_db: object) -> AsyncClient:
    """An httpx client that does NOT follow redirects (we assert on 302 directly)."""
    transport = ASGITransport(app=seeded_db)  # type: ignore[arg-type]
    return AsyncClient(transport=transport, base_url="http://test", follow_redirects=False)


async def _seed_affiliate_link(card_slug: str, partner_id: str = "test-partner") -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == card_slug))
        ).scalar_one()
        link = AffiliateLink(
            card_id=card_id,
            partner_id=partner_id,
            url_template="https://partner.example.com/apply?cid={click_id}&utm={utm_campaign}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        await session.commit()
        return uuid.UUID(str(card_id))


async def test_click_redirects_and_sets_cookie(click_client: AsyncClient) -> None:
    card_id = await _seed_affiliate_link("kbank-wisdom")
    resp = await click_client.post(
        f"/v1/affiliate/click/{card_id}",
        params={"placement": "review", "utm_campaign": "homepage-hero"},
    )
    assert resp.status_code == 302
    location = resp.headers.get("location")
    assert location is not None
    assert "cid=" in location
    assert "utm=homepage-hero" in location

    # Cookie set?
    set_cookie = resp.headers.get("set-cookie", "")
    assert "loftly_click_id=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "Secure" in set_cookie

    # Click row written?
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(AffiliateClick))).scalars().all())
    assert len(rows) == 1
    assert rows[0].placement == "review"
    assert rows[0].utm_campaign == "homepage-hero"


async def test_click_404_when_no_active_link(click_client: AsyncClient) -> None:
    # `uob-prvi-miles` has no affiliate_link in this test.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "uob-prvi-miles"))
        ).scalar_one()

    resp = await click_client.post(
        f"/v1/affiliate/click/{card_id}",
        params={"placement": "review"},
    )
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "no_active_affiliate_link"
    assert body["error"]["message_th"] == "ยังไม่มีช่องทางสมัครสำหรับบัตรนี้"


async def test_click_404_when_card_unknown(click_client: AsyncClient) -> None:
    resp = await click_client.post(
        "/v1/affiliate/click/00000000-0000-4000-8000-0000000000ab",
        params={"placement": "review"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "card_not_found"


async def test_click_validation_rejects_bad_placement(click_client: AsyncClient) -> None:
    card_id = await _seed_affiliate_link("kbank-wisdom")
    resp = await click_client.post(
        f"/v1/affiliate/click/{card_id}",
        params={"placement": "definitely-wrong"},
    )
    # Pydantic/Enum validation => 422 via our handler.
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_click_rate_limit_blocks_11th_request(click_client: AsyncClient) -> None:
    card_id = await _seed_affiliate_link("kbank-wisdom")
    # 10 allowed within the 60s window.
    for _ in range(10):
        r = await click_client.post(
            f"/v1/affiliate/click/{card_id}",
            params={"placement": "cards_index"},
        )
        assert r.status_code == 302, r.text
    # 11th should be rate limited.
    over = await click_client.post(
        f"/v1/affiliate/click/{card_id}",
        params={"placement": "cards_index"},
    )
    assert over.status_code == 429
    body = over.json()
    assert body["error"]["code"] == "rate_limited"
    assert body["error"]["message_th"]
