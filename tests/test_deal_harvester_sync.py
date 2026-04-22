"""Deal-harvester sync — mocked upstream happy path + checksum dedup + soft-delete."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock
from sqlalchemy import select

from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import SyncRun
from loftly.db.models.promo import Promo, promo_card_map
from loftly.jobs.deal_harvester_sync import run_sync


def _upstream_payload(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "items": items,
        "total": len(items),
        "page": 1,
        "page_size": 100,
        "pages": 1,
    }


async def _client_factory_with_mock(httpx_mock: HTTPXMock) -> Any:
    """Returns an `httpx.AsyncClient` pre-wired with `pytest-httpx` mocked routes."""

    def factory() -> httpx.AsyncClient:
        return httpx.AsyncClient()

    return factory


async def test_sync_happy_path(seeded_db: object, httpx_mock: HTTPXMock) -> None:
    _ = seeded_db  # seeded banks/cards
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload(
            [
                {
                    "id": "ktc-1",
                    "bank": "ktc",
                    "source_id": "ktc-1",
                    "source_url": "https://ktc.example/1",
                    "title": "Starbucks 15%",
                    "description": "โปร Starbucks",
                    "card_types": ["KBank WISDOM"],
                    "category": "dining-restaurants",
                    "merchant_name": "Starbucks",
                    "discount_type": "cashback",
                    "discount_value": "15%",
                    "minimum_spend": 300,
                    "start_date": "2026-01-01",
                    "end_date": "2026-06-30",
                    "terms_and_conditions": "TCs",
                    "is_active": True,
                    "scraped_at": "2026-04-21T10:30:00",
                    "checksum": "deadbeef",
                }
            ]
        ),
    )

    factory = await _client_factory_with_mock(httpx_mock)
    result = await run_sync(client_factory=factory)
    assert result["status"] == "success"
    assert result["upstream_count"] == 1
    assert result["inserted_count"] == 1

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        promo = (
            (await session.execute(select(Promo).where(Promo.external_source_id == "ktc-1")))
            .scalars()
            .unique()
            .one()
        )
        assert promo.discount_amount is not None
        assert float(promo.discount_amount) == 15.0
        # Unit normalized to the enum allowed by promos_discount_unit_check
        # (migration 006): "percent", not the raw "%" captured by the regex.
        assert promo.discount_unit == "percent"
        assert promo.category == "dining"
        mapped_count = len(
            list(
                (
                    await session.execute(
                        select(promo_card_map.c.card_id).where(
                            promo_card_map.c.promo_id == promo.id
                        )
                    )
                )
                .scalars()
                .all()
            )
        )
        assert mapped_count == 1  # auto-mapped to KBank WISDOM


async def test_sync_dedup_checksum(seeded_db: object, httpx_mock: HTTPXMock) -> None:
    _ = seeded_db
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    page = _upstream_payload(
        [
            {
                "id": "ktc-1",
                "bank": "ktc",
                "source_id": "ktc-1",
                "source_url": "https://x/1",
                "title": "v1",
                "card_types": [],
                "discount_type": "cashback",
                "discount_value": "10%",
                "is_active": True,
                "checksum": "sameconst",
            }
        ]
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=page,
    )
    factory = await _client_factory_with_mock(httpx_mock)
    r1 = await run_sync(client_factory=factory)
    assert r1["inserted_count"] == 1

    # Second call — same checksum so nothing updates.
    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=page,
    )
    r2 = await run_sync(client_factory=factory)
    assert r2["updated_count"] == 0
    assert r2["inserted_count"] == 0


async def test_sync_soft_deletes_missing(seeded_db: object, httpx_mock: HTTPXMock) -> None:
    _ = seeded_db
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload(
            [
                {
                    "id": "ktc-A",
                    "bank": "ktc",
                    "source_id": "ktc-A",
                    "source_url": "https://x",
                    "title": "A",
                    "card_types": [],
                    "discount_type": "cashback",
                    "discount_value": "5%",
                    "is_active": True,
                    "checksum": "c1",
                },
                {
                    "id": "ktc-B",
                    "bank": "ktc",
                    "source_id": "ktc-B",
                    "source_url": "https://y",
                    "title": "B",
                    "card_types": [],
                    "discount_type": "cashback",
                    "discount_value": "5%",
                    "is_active": True,
                    "checksum": "c2",
                },
            ]
        ),
    )
    factory = await _client_factory_with_mock(httpx_mock)
    r1 = await run_sync(client_factory=factory)
    assert r1["inserted_count"] == 2

    # Next sync only returns A; B should be deactivated.
    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload(
            [
                {
                    "id": "ktc-A",
                    "bank": "ktc",
                    "source_id": "ktc-A",
                    "source_url": "https://x",
                    "title": "A",
                    "card_types": [],
                    "discount_type": "cashback",
                    "discount_value": "5%",
                    "is_active": True,
                    "checksum": "c1",
                }
            ]
        ),
    )
    r2 = await run_sync(client_factory=factory)
    assert r2["deactivated_count"] == 1

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        b = (
            (await session.execute(select(Promo).where(Promo.external_source_id == "ktc-B")))
            .scalars()
            .unique()
            .one()
        )
        assert b.active is False


async def test_sync_schema_drift_tolerant(seeded_db: object, httpx_mock: HTTPXMock) -> None:
    """Unknown upstream fields land in raw_data, don't crash the sync."""
    _ = seeded_db
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload(
            [
                {
                    "id": "ktc-9",
                    "bank": "ktc",
                    "source_id": "ktc-9",
                    "source_url": "https://x",
                    "title": "Has extra fields",
                    "card_types": [],
                    "discount_type": "cashback",
                    "discount_value": "7.5%",
                    "is_active": True,
                    "checksum": "abc",
                    # drift fields
                    "weird_new_field": [1, 2, 3],
                    "nested_thing": {"ok": True},
                }
            ]
        ),
    )
    factory = await _client_factory_with_mock(httpx_mock)
    result = await run_sync(client_factory=factory)
    assert result["status"] == "success"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = (
            (await session.execute(select(Promo).where(Promo.external_source_id == "ktc-9")))
            .scalars()
            .unique()
            .one()
        )
        assert row.raw_data.get("weird_new_field") == [1, 2, 3]
        assert row.raw_data.get("nested_thing") == {"ok": True}


async def test_sync_failure_records_run(seeded_db: object, httpx_mock: HTTPXMock) -> None:
    _ = seeded_db
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    # 3 consecutive failures → retries exhaust → sync failed.
    for _ in range(3):
        httpx_mock.add_response(
            method="GET",
            url=f"{base}/promotions?is_active=true&page_size=100&page=1",
            status_code=503,
        )

    factory = await _client_factory_with_mock(httpx_mock)
    result = await run_sync(client_factory=factory)
    assert result["status"] == "failed"
    assert result["error_message"]

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        runs = list((await session.execute(select(SyncRun))).scalars().all())
    assert any(r.status == "failed" for r in runs)


async def test_sync_internal_endpoint_requires_key(
    seeded_client: Any,
) -> None:
    # Missing X-API-Key.
    resp = await seeded_client.post("/v1/internal/sync/deal-harvester")
    assert resp.status_code == 401


async def test_sync_last_endpoint_returns_latest(seeded_client: Any, httpx_mock: HTTPXMock) -> None:
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")
    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload([]),
    )
    factory = await _client_factory_with_mock(httpx_mock)
    await run_sync(client_factory=factory)

    resp = await seeded_client.get(
        "/v1/internal/sync/deal-harvester/last",
        headers={"X-API-Key": settings.jwt_signing_key},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "deal_harvester"


async def test_sync_parse_discount_value_normalizes_unit_to_enum() -> None:
    """Captured units must map to the CHECK-constraint enum, not raw strings."""
    from loftly.jobs.deal_harvester_sync import _parse_discount_value

    assert _parse_discount_value("25%") == (Decimal("25"), "percent")
    assert _parse_discount_value("100 baht") == (Decimal("100"), "thb")
    assert _parse_discount_value("1,000 THB") == (Decimal("1000"), "thb")
    assert _parse_discount_value("2 เท่า") == (Decimal("2"), "x_multiplier")
    assert _parse_discount_value("5x") == (Decimal("5"), "x_multiplier")
    assert _parse_discount_value("500 points") == (Decimal("500"), "points")
    assert _parse_discount_value("no number") == (None, None)
    assert _parse_discount_value(None) == (None, None)


async def test_sync_maps_promo_type_from_discount_type(
    seeded_db: object, httpx_mock: HTTPXMock
) -> None:
    """Verify discount_type → promo_type mapping per mvp/SCHEMA.md §9."""
    _ = seeded_db
    settings = get_settings()
    base = settings.deal_harvester_base.rstrip("/")

    httpx_mock.add_response(
        method="GET",
        url=f"{base}/promotions?is_active=true&page_size=100&page=1",
        json=_upstream_payload(
            [
                {  # cashback → cashback (pass-through)
                    "id": "map-1",
                    "bank": "ktc",
                    "source_id": "map-1",
                    "source_url": "https://x/1",
                    "title": "a",
                    "card_types": [],
                    "discount_type": "cashback",
                    "discount_value": "5%",
                    "is_active": True,
                    "checksum": "c1",
                },
                {  # percentage → category_bonus
                    "id": "map-2",
                    "bank": "ktc",
                    "source_id": "map-2",
                    "source_url": "https://x/2",
                    "title": "b",
                    "card_types": [],
                    "discount_type": "percentage",
                    "discount_value": "10%",
                    "is_active": True,
                    "checksum": "c2",
                },
                {  # discount → category_bonus
                    "id": "map-3",
                    "bank": "ktc",
                    "source_id": "map-3",
                    "source_url": "https://x/3",
                    "title": "c",
                    "card_types": [],
                    "discount_type": "discount",
                    "discount_value": "100 baht",
                    "is_active": True,
                    "checksum": "c3",
                },
                {  # points → category_bonus
                    "id": "map-4",
                    "bank": "ktc",
                    "source_id": "map-4",
                    "source_url": "https://x/4",
                    "title": "d",
                    "card_types": [],
                    "discount_type": "points",
                    "discount_value": "2x points",
                    "is_active": True,
                    "checksum": "c4",
                },
                {  # null discount_type → null (never "category_bonus")
                    "id": "map-5",
                    "bank": "ktc",
                    "source_id": "map-5",
                    "source_url": "https://x/5",
                    "title": "e",
                    "card_types": [],
                    "discount_type": None,
                    "discount_value": None,
                    "is_active": True,
                    "checksum": "c5",
                },
            ]
        ),
    )
    factory = await _client_factory_with_mock(httpx_mock)
    result = await run_sync(client_factory=factory)
    assert result["status"] == "success"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = {
            r.external_source_id: r
            for r in (
                (await session.execute(select(Promo).where(Promo.external_bank_key == "ktc")))
                .scalars()
                .unique()
                .all()
            )
            if r.external_source_id.startswith("map-")
        }
    assert rows["map-1"].promo_type == "cashback"
    assert rows["map-2"].promo_type == "category_bonus"
    assert rows["map-3"].promo_type == "category_bonus"
    assert rows["map-4"].promo_type == "category_bonus"
    assert rows["map-5"].promo_type is None


@pytest.fixture
def non_mocked_hosts() -> list[str]:
    """Let ASGI test traffic through; only mock deal-harvester."""
    return ["test"]
