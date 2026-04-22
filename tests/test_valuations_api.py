"""Valuations API tests — `GET /v1/valuations` + `/v1/valuations/{code}`.

Covers:
- List returns data (fallback path when `point_valuations` is empty).
- List respects `?limit=` + `?order=` (updated_at_desc / thb_per_point_desc / code_asc).
- Detail 404s on an unknown currency code.
- Detail returns distribution_summary + non-empty history array when rows exist.
- Detail accepts case-insensitive codes *and* the display-name alias
  (`KRISFLYER` → `KF`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation

# --- List endpoint ---------------------------------------------------------


async def test_list_valuations_fallback_when_db_empty(seeded_client: AsyncClient) -> None:
    """With no point_valuations rows, the fixture-backed fallback kicks in."""
    resp = await seeded_client.get("/v1/valuations")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "data" in body
    assert isinstance(body["data"], list)
    # Starter set is KF/AM/BONVOY/ROP — all four have shipped fixtures.
    assert len(body["data"]) >= 3
    codes = {v["currency"]["code"] for v in body["data"]}
    # At minimum ROP must be present since the ROP fixture is asserted by the
    # existing valuation-algorithm tests.
    assert "ROP" in codes

    sample = body["data"][0]
    assert set(sample.keys()) >= {
        "currency",
        "thb_per_point",
        "methodology",
        "percentile",
        "sample_size",
        "confidence",
        "computed_at",
    }
    assert 0.0 <= sample["confidence"] <= 1.0
    assert sample["percentile"] == 80
    assert set(sample["currency"].keys()) >= {
        "code",
        "display_name_en",
        "display_name_th",
        "currency_type",
    }


async def test_list_valuations_respects_limit(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/valuations", params={"limit": 2})
    assert resp.status_code == 200
    assert len(resp.json()["data"]) <= 2


async def test_list_valuations_order_code_asc(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/valuations", params={"order": "code_asc", "limit": 50})
    assert resp.status_code == 200
    codes = [v["currency"]["code"] for v in resp.json()["data"]]
    assert codes == sorted(codes)


async def test_list_valuations_order_thb_per_point_desc(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get(
        "/v1/valuations",
        params={"order": "thb_per_point_desc", "limit": 50},
    )
    assert resp.status_code == 200
    values = [v["thb_per_point"] for v in resp.json()["data"]]
    assert values == sorted(values, reverse=True)


async def test_list_valuations_rejects_unknown_order(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/valuations", params={"order": "random"})
    # FastAPI/pydantic validates the pattern → 422.
    assert resp.status_code == 422


# --- Detail endpoint -------------------------------------------------------


async def test_detail_404_on_unknown_code(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/valuations/TOTALLY_MADE_UP")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "currency_not_found"


async def test_detail_fallback_returns_distribution_and_history(
    seeded_client: AsyncClient,
) -> None:
    """With no point_valuations rows, detail still returns a usable payload."""
    resp = await seeded_client.get("/v1/valuations/ROP")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["currency"]["code"] == "ROP"
    # distribution_summary recomputed from fixtures — includes p10/p25/p50/p75/p90.
    assert body["distribution_summary"] is not None
    assert set(body["distribution_summary"].keys()) >= {"p10", "p50", "p90"}
    # Fallback ships a single synthetic history point so the chart renders.
    assert isinstance(body["history"], list)
    assert len(body["history"]) >= 1


async def test_detail_case_insensitive_code(seeded_client: AsyncClient) -> None:
    resp_lower = await seeded_client.get("/v1/valuations/rop")
    resp_upper = await seeded_client.get("/v1/valuations/ROP")
    assert resp_lower.status_code == 200
    assert resp_upper.status_code == 200
    assert resp_lower.json()["currency"]["code"] == "ROP"
    assert resp_upper.json()["currency"]["code"] == "ROP"


async def test_detail_accepts_display_name_alias(seeded_client: AsyncClient) -> None:
    """Frontend sometimes surfaces 'KRISFLYER' rather than the short code 'KF'."""
    resp = await seeded_client.get("/v1/valuations/KRISFLYER")
    assert resp.status_code == 200, resp.text
    assert resp.json()["currency"]["code"] == "KF"


# --- DB-backed path (with rows inserted) ----------------------------------


@pytest.mark.asyncio
async def test_detail_returns_history_array_when_data_exists(
    seeded_client: AsyncClient,
) -> None:
    """Insert 5 weekly rows; detail returns the latest 4 as history."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rop = (
            (await session.execute(select(LoyaltyCurrency).where(LoyaltyCurrency.code == "ROP")))
            .scalars()
            .one()
        )
        now = datetime.now(UTC)
        for weeks_ago in range(5):
            session.add(
                PointValuation(
                    currency_id=rop.id,
                    thb_per_point=Decimal("0.6200") + Decimal("0.01") * weeks_ago,
                    methodology="p80_award_chart_vs_cash",
                    percentile=80,
                    sample_size=12,
                    confidence=Decimal("0.70"),
                    top_redemption_example="BKK→NRT economy: 30,000 points for THB 20,000",
                    computed_at=now - timedelta(weeks=weeks_ago),
                )
            )
        await session.commit()

    resp = await seeded_client.get("/v1/valuations/ROP")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Latest row (weeks_ago=0) has thb_per_point = 0.62 + 0.00 = 0.62.
    assert abs(body["thb_per_point"] - 0.62) < 1e-6
    assert body["sample_size"] == 12
    # History is capped at 4 weekly observations, newest first.
    assert len(body["history"]) == 4
    hist_values = [h["thb_per_point"] for h in body["history"]]
    # Values increase as we go further back in time, so the newest-first
    # ordering means values ascend through the list.
    assert hist_values == sorted(hist_values)


@pytest.mark.asyncio
async def test_list_uses_latest_row_per_currency(seeded_client: AsyncClient) -> None:
    """Two rows for ROP → only the most recent one is surfaced by the list."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rop = (
            (await session.execute(select(LoyaltyCurrency).where(LoyaltyCurrency.code == "ROP")))
            .scalars()
            .one()
        )
        now = datetime.now(UTC)
        session.add(
            PointValuation(
                currency_id=rop.id,
                thb_per_point=Decimal("0.5000"),
                methodology="p80_award_chart_vs_cash",
                percentile=80,
                sample_size=10,
                confidence=Decimal("0.60"),
                computed_at=now - timedelta(weeks=2),
            )
        )
        session.add(
            PointValuation(
                currency_id=rop.id,
                thb_per_point=Decimal("0.7000"),
                methodology="p80_award_chart_vs_cash",
                percentile=80,
                sample_size=15,
                confidence=Decimal("0.80"),
                computed_at=now,
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/valuations", params={"limit": 50})
    assert resp.status_code == 200
    rop_rows = [v for v in resp.json()["data"] if v["currency"]["code"] == "ROP"]
    assert len(rop_rows) == 1
    assert abs(rop_rows[0]["thb_per_point"] - 0.70) < 1e-6
