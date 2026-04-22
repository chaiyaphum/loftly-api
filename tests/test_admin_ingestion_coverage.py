"""Admin ingestion coverage + per-bank resync — W16 follow-up.

Backs the ingestion viewer page in ``loftly-web#10``. Covers:

* Status classification thresholds (``full`` / ``partial`` / ``gap``).
* Unmapped-promo count joins against ``promo_card_map``.
* Resync dispatch for manual-catalog banks (``uob`` / ``krungsri``) and
  the adapter path for deal-harvester banks.
* Admin-JWT guard (401 without a bearer, 403 for non-admins).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card as CardModel
from loftly.db.models.promo import Promo, promo_card_map
from loftly.schemas.manual_catalog import IngestResult

# ---------------------------------------------------------------------------
# Fixture helpers — each test inserts a deterministic promo mix so status
# classification is unambiguous.
# ---------------------------------------------------------------------------


def _promo(
    *,
    bank_id: Any,
    title: str,
    external_bank_key: str,
    external_source_id: str,
    active: bool = True,
) -> Promo:
    return Promo(
        bank_id=bank_id,
        external_source_id=external_source_id,
        external_bank_key=external_bank_key,
        source_url=f"https://example.test/{external_source_id}",
        promo_type="category_bonus",
        title_th=title,
        active=active,
        last_synced_at=datetime.now(UTC),
    )


async def _seed_coverage_mix(session: Any) -> dict[str, Bank]:
    """Seed 3 banks with known active-promo counts:

    * ``uob``      → 8 active manual-catalog rows → ``full``
    * ``krungsri`` → 4 manual + 1 inactive         → ``partial``
    * ``ktc``      → 2 deal-harvester rows         → ``gap``
    """
    banks = {b.slug: b for b in (await session.execute(select(Bank))).scalars().all()}
    # uob: 8 active manual
    for i in range(8):
        session.add(
            _promo(
                bank_id=banks["uob"].id,
                title=f"UOB promo {i}",
                external_bank_key="manual:uob",
                external_source_id=f"manual:uob:src:{i}",
            )
        )
    # krungsri: 4 active + 1 inactive manual
    for i in range(4):
        session.add(
            _promo(
                bank_id=banks["krungsri"].id,
                title=f"KS promo {i}",
                external_bank_key="manual:krungsri",
                external_source_id=f"manual:krungsri:src:{i}",
            )
        )
    session.add(
        _promo(
            bank_id=banks["krungsri"].id,
            title="KS inactive",
            external_bank_key="manual:krungsri",
            external_source_id="manual:krungsri:src:inactive",
            active=False,
        )
    )
    # ktc: 2 active deal-harvester
    for i in range(2):
        session.add(
            _promo(
                bank_id=banks["ktc"].id,
                title=f"KTC promo {i}",
                external_bank_key="ktc",
                external_source_id=f"harvester-ktc-{i}",
            )
        )
    await session.commit()
    return banks


# ---------------------------------------------------------------------------
# Coverage endpoint
# ---------------------------------------------------------------------------


async def test_coverage_classifies_banks_by_thresholds(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await _seed_coverage_mix(session)

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    by_slug = {row["bank_slug"]: row for row in body["banks"]}

    # uob: 8 manual → full
    assert by_slug["uob"]["manual_catalog_count"] == 8
    assert by_slug["uob"]["deal_harvester_count"] == 0
    assert by_slug["uob"]["active_promos_count"] == 8
    assert by_slug["uob"]["coverage_status"] == "full"
    assert by_slug["uob"]["last_synced_at"] is not None

    # krungsri: 4 active manual (inactive row excluded) → partial
    assert by_slug["krungsri"]["manual_catalog_count"] == 4
    assert by_slug["krungsri"]["active_promos_count"] == 4
    assert by_slug["krungsri"]["coverage_status"] == "partial"

    # ktc: 2 harvester → gap
    assert by_slug["ktc"]["manual_catalog_count"] == 0
    assert by_slug["ktc"]["deal_harvester_count"] == 2
    assert by_slug["ktc"]["active_promos_count"] == 2
    assert by_slug["ktc"]["coverage_status"] == "gap"

    # Banks with no promos still surface as "gap".
    assert by_slug["kbank"]["active_promos_count"] == 0
    assert by_slug["kbank"]["coverage_status"] == "gap"


async def test_coverage_unmapped_count(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        banks = await _seed_coverage_mix(session)
        # Map one of the ktc promos to a card so the unmapped count
        # excludes it.
        card = (await session.execute(select(CardModel).limit(1))).scalars().one()
        promo = (
            (
                await session.execute(
                    select(Promo).where(
                        Promo.bank_id == banks["ktc"].id,
                        Promo.external_source_id == "harvester-ktc-0",
                    )
                )
            )
            .scalars()
            .one()
        )
        await session.execute(promo_card_map.insert().values(promo_id=promo.id, card_id=card.id))
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # 8 uob + 4 krungsri + 2 ktc = 14 active; 1 mapped; 13 unmapped.
    assert body["unmapped_promos_count"] == 13


async def test_coverage_overall_pct_weights_full_and_partial(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await _seed_coverage_mix(session)

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()

    # Seed has 8 banks total; 2 are full/partial (uob, krungsri).
    # 2/8 = 25.0
    assert body["overall_coverage_pct"] == pytest.approx(25.0, abs=0.1)


# ---------------------------------------------------------------------------
# Resync endpoint
# ---------------------------------------------------------------------------


async def test_resync_unknown_bank_returns_404(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post("/v1/admin/ingestion/not-a-bank/resync", headers=admin_headers)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "bank_not_found"


async def test_resync_uob_calls_manual_ingest(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    fake_result = IngestResult(
        bank_slug="uob",
        dry_run=False,
        upstream_count=5,
        inserted=3,
        updated=1,
        archived=0,
        unchanged=1,
        errors=[],
    )
    with patch(
        "loftly.jobs.manual_catalog_ingest.run_ingest",
        new=AsyncMock(return_value=fake_result),
    ) as mock:
        resp = await seeded_client.post("/v1/admin/ingestion/uob/resync", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "ok": True,
        "counts": {
            "inserted": 3,
            "updated": 1,
            "archived": 0,
            "unchanged": 1,
        },
    }
    mock.assert_awaited_once_with("uob", dry_run=False)


async def test_resync_krungsri_calls_manual_ingest(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    fake_result = IngestResult(
        bank_slug="krungsri",
        dry_run=False,
        upstream_count=2,
        inserted=2,
        updated=0,
        archived=0,
        unchanged=0,
        errors=[],
    )
    with patch(
        "loftly.jobs.manual_catalog_ingest.run_ingest",
        new=AsyncMock(return_value=fake_result),
    ):
        resp = await seeded_client.post(
            "/v1/admin/ingestion/krungsri/resync", headers=admin_headers
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["counts"]["inserted"] == 2


async def test_resync_harvester_bank_calls_adapter(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Non-manual banks route through the deal-harvester adapter.

    The adapter wraps ``run_sync`` (all-banks sweep) and projects its
    counts shape. We patch ``run_sync`` to avoid real HTTP traffic.
    """
    fake_run: dict[str, Any] = {
        "inserted_count": 4,
        "updated_count": 2,
        "deactivated_count": 1,
    }
    with patch(
        "loftly.jobs.deal_harvester_sync.run_sync",
        new=AsyncMock(return_value=fake_run),
    ) as mock:
        resp = await seeded_client.post("/v1/admin/ingestion/ktc/resync", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "ok": True,
        "counts": {
            "inserted": 4,
            "updated": 2,
            "archived": 1,
            "unchanged": 0,
        },
    }
    mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------


async def test_coverage_requires_admin_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/ingestion/coverage")
    assert resp.status_code == 401


async def test_resync_requires_admin_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post("/v1/admin/ingestion/uob/resync")
    assert resp.status_code == 401


async def test_coverage_forbids_non_admin(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=user_headers)
    assert resp.status_code == 403
