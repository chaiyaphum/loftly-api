"""Compare endpoint tests — `GET /v1/cards/compare?slugs=a,b,c`.

Seeds sample cards via the shared `seeded_client` fixture. For the valuation +
transfer-partner portions of the payload, the test inserts a small number of
rows directly through the session fixture to exercise the enrichment paths
without depending on the full data pipeline.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.transfer_ratio import TransferRatio


async def test_compare_two_cards_happy_path(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare", params={"slugs": "kbank-wisdom,uob-prvi-miles"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "data" in body
    assert len(body["data"]) == 2
    slugs = [entry["card"]["slug"] for entry in body["data"]]
    # Response preserves request order.
    assert slugs == ["kbank-wisdom", "uob-prvi-miles"]
    first = body["data"][0]
    # Core card fields surface through the nested `card` key.
    assert first["card"]["display_name"] == "KBank WISDOM"
    assert first["card"]["earn_currency"]["code"] == "K_POINT"
    # Earn rate for each category exposed as before.
    assert first["card"]["earn_rate_local"]["dining"] == pytest.approx(2.0)
    # New compare-only fields present (may be None/empty when no data seeded).
    assert "transfer_partners" in first
    assert "valuation" in first
    assert "loftly_score" in first
    assert first["loftly_score"] is not None
    assert 0.0 <= first["loftly_score"] <= 5.0


async def test_compare_three_cards_max(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare",
        params={"slugs": "kbank-wisdom,uob-prvi-miles,scb-thai-airways"},
    )
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 3


async def test_compare_rejects_more_than_three(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare",
        params={"slugs": "a,b,c,d"},
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "too_many_slugs"
    assert body["error"]["details"]["max"] == 3


async def test_compare_invalid_slug_returns_404(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare",
        params={"slugs": "kbank-wisdom,does-not-exist"},
    )
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "card_not_found"
    assert "does-not-exist" in body["error"]["details"]["missing_slugs"]


async def test_compare_empty_slugs_returns_400(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/compare", params={"slugs": ""})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "slugs_required"


async def test_compare_enriches_with_valuation_and_transfer_partners(
    seeded_client: AsyncClient,
) -> None:
    """Seed a point_valuation + transfer_ratio and check they surface."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Source = K_POINT (for kbank-wisdom), destination = ROP.
        k_point = (
            await session.execute(select(LoyaltyCurrency).where(LoyaltyCurrency.code == "K_POINT"))
        ).scalar_one()
        rop = (
            await session.execute(select(LoyaltyCurrency).where(LoyaltyCurrency.code == "ROP"))
        ).scalar_one()
        session.add(
            PointValuation(
                id=uuid.uuid4(),
                currency_id=k_point.id,
                thb_per_point=Decimal("0.38"),
                methodology="percentile_80",
                percentile=80,
                sample_size=24,
                confidence=Decimal("0.80"),
                computed_at=datetime(2026, 4, 10, 12, 0, 0),
            )
        )
        session.add(
            TransferRatio(
                id=uuid.uuid4(),
                source_currency_id=k_point.id,
                destination_currency_id=rop.id,
                ratio_source=Decimal("1000"),
                ratio_destination=Decimal("400"),
                bonus_percentage=Decimal("0"),
                effective_from=date(2026, 1, 1),
                source_url="https://example.test/k-rop",
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/cards/compare", params={"slugs": "kbank-wisdom"})
    assert resp.status_code == 200
    entry = resp.json()["data"][0]
    assert entry["valuation"] is not None
    assert entry["valuation"]["thb_per_point"] == pytest.approx(0.38)
    assert entry["valuation"]["sample_size"] == 24
    assert len(entry["transfer_partners"]) == 1
    assert entry["transfer_partners"][0]["destination_code"] == "ROP"
    assert entry["transfer_partners"][0]["ratio_source"] == pytest.approx(1000)
    assert entry["transfer_partners"][0]["ratio_destination"] == pytest.approx(400)


async def test_compare_dedupes_whitespace_slugs(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare", params={"slugs": " kbank-wisdom , uob-prvi-miles "}
    )
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 2


async def test_compare_card_order_matches_request(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/cards/compare",
        params={"slugs": "uob-prvi-miles,kbank-wisdom"},
    )
    assert resp.status_code == 200
    slugs = [e["card"]["slug"] for e in resp.json()["data"]]
    assert slugs == ["uob-prvi-miles", "kbank-wisdom"]


async def test_compare_card_row_touched(seeded_client: AsyncClient) -> None:
    """Sanity — the seeded `kbank-wisdom` is present in the catalog."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = (
            await session.execute(select(CardModel).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one_or_none()
        assert row is not None
