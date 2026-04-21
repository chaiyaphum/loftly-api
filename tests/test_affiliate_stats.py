"""Admin affiliate stats — 30-day funnel aggregation."""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import (
    AffiliateClick,
    AffiliateConversion,
    AffiliateLink,
)
from loftly.db.models.card import Card as CardModel

_PARTNER = "test-partner"


async def _seed_stats_fixtures() -> None:
    """2 clicks on kbank-wisdom, 1 conversion (confirmed, 500 THB)."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one()
        link = AffiliateLink(
            card_id=card_id,
            partner_id=_PARTNER,
            url_template="https://p.example.com/?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        await session.flush()

        click_ids = [uuid.uuid4() for _ in range(2)]
        for cid in click_ids:
            session.add(
                AffiliateClick(
                    click_id=cid,
                    affiliate_link_id=link.id,
                    card_id=card_id,
                    partner_id=_PARTNER,
                    placement="cards_index",
                )
            )
        session.add(
            AffiliateConversion(
                click_id=click_ids[0],
                partner_id=_PARTNER,
                conversion_type="application_approved",
                status="confirmed",
                commission_thb=Decimal("500.00"),
                raw_payload={},
            )
        )
        await session.commit()


@pytest_asyncio.fixture
async def stats_seeded(seeded_db: object) -> None:
    _ = seeded_db
    await _seed_stats_fixtures()


async def test_stats_aggregates_last_30_days(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    stats_seeded: None,
) -> None:
    _ = stats_seeded
    resp = await seeded_client.get("/v1/admin/affiliate/stats", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["period_days"] == 30
    assert body["clicks"] == 2
    assert body["conversions"] == 1
    assert body["conversion_rate"] == 0.5
    assert body["commission_confirmed_thb"] == 500.0
    assert body["commission_pending_thb"] == 0.0
    assert body["commission_paid_thb"] == 0.0

    by_card = body["by_card"]
    assert len(by_card) == 1
    assert by_card[0]["card_slug"] == "kbank-wisdom"
    assert by_card[0]["clicks"] == 2
    assert by_card[0]["conversions"] == 1
    assert by_card[0]["commission_thb"] == 500.0


async def test_stats_requires_admin(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/affiliate/stats")
    assert resp.status_code == 401


async def test_stats_empty_when_no_data(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get("/v1/admin/affiliate/stats", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["clicks"] == 0
    assert body["conversions"] == 0
    assert body["conversion_rate"] == 0.0
    assert body["by_card"] == []
