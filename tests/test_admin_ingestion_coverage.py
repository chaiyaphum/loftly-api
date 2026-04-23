"""Admin ingestion coverage + per-bank resync — W16 follow-up.

Backs the ingestion viewer page in ``loftly-web#10``. Covers:

* Status classification thresholds (``full`` / ``partial`` / ``gap``).
* Unmapped-promo count joins against ``promo_card_map``.
* Resync dispatch for manual-catalog banks (``uob`` / ``krungsri``) and
  the adapter path for deal-harvester banks.
* Admin-JWT guard (401 without a bearer, 403 for non-admins).

v3 additions (admin-dashboard polish):

* Extended bank-row fields: ``source_key``, ``display_name_th``,
  ``merchant_name_coverage``, ``staleness_hours`` + ``staleness_bucket``.
* ``sync_summary`` block surfacing latest ``deal_harvester`` and
  ``merchant_canonicalizer`` SyncRun rows.
* ``alerts[]`` for silent banks, banks idle with zero promos, and the
  case where the canonicalizer has never run.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import SyncRun
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


# ---------------------------------------------------------------------------
# v3 — extended bank fields, sync_summary, alerts
# ---------------------------------------------------------------------------


def _promo_at(
    *,
    bank_id: Any,
    title: str,
    external_bank_key: str,
    external_source_id: str,
    last_synced_at: datetime,
    merchant_name: str | None = None,
    active: bool = True,
) -> Promo:
    """Variant of ``_promo`` that lets the caller pin ``last_synced_at``.

    The default helper stamps ``datetime.now(UTC)``; the staleness-bucket
    tests need backdated rows to exercise the warming/stale/silent ladder
    without resorting to ``freezegun`` (not vendored here).
    """
    return Promo(
        bank_id=bank_id,
        external_source_id=external_source_id,
        external_bank_key=external_bank_key,
        source_url=f"https://example.test/{external_source_id}",
        promo_type="category_bonus",
        title_th=title,
        merchant_name=merchant_name,
        active=active,
        last_synced_at=last_synced_at,
    )


async def test_coverage_returns_extended_bank_fields(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Each bank row carries the v3 fields the admin dashboard renders."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await _seed_coverage_mix(session)

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    by_slug = {row["bank_slug"]: row for row in body["banks"]}

    uob = by_slug["uob"]
    # New field set — these are the dashboard-visible columns.
    assert uob["slug"] == "uob"
    assert uob["display_name_th"] == "ยูโอบี"
    assert uob["active_promos"] == 8
    # `merchant_name_coverage` is 0.0 because `_promo` helper leaves
    # `merchant_name` NULL; the v3 ratio is sum(merchant_name IS NOT NULL)
    # / count(active).
    assert uob["merchant_name_coverage"] == 0.0
    assert uob["staleness_bucket"] == "fresh"
    assert isinstance(uob["staleness_hours"], (int, float))
    assert uob["staleness_hours"] < 1.0

    # Top-level v3 keys.
    assert "sync_summary" in body
    assert set(body["sync_summary"].keys()) == {"deal_harvester", "merchant_canonicalizer"}
    assert "alerts" in body
    assert isinstance(body["alerts"], list)
    assert "generated_at" in body


async def test_coverage_includes_all_seven_bank_adapters(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """The handler must enumerate every bank, not just historical 3."""
    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    slugs = {row["bank_slug"] for row in body["banks"]}
    # Seed currently ships 8 bank rows (kbank/scb/ktc/krungsri/uob/bbl/
    # amex-th/ttb). The contract requires the handler to surface every
    # bank present in `banks` regardless of whether the adapter has
    # produced promos yet — this guards the "7 adapters" expansion.
    expected = {"kbank", "scb", "ktc", "krungsri", "uob", "bbl", "amex-th"}
    assert expected.issubset(slugs)


async def test_coverage_merchant_name_coverage_ratio(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """``merchant_name_coverage`` = named / active for that bank."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        banks = {b.slug: b for b in (await session.execute(select(Bank))).scalars().all()}
        # 4 active SCB promos, 3 with merchant_name → ratio 0.75.
        for i, name in enumerate(["Starbucks", "Lazada", "Shopee", None]):
            session.add(
                _promo_at(
                    bank_id=banks["scb"].id,
                    title=f"SCB {i}",
                    external_bank_key="scb",
                    external_source_id=f"scb-{i}",
                    merchant_name=name,
                    last_synced_at=datetime.now(UTC),
                )
            )
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    by_slug = {row["bank_slug"]: row for row in body["banks"]}
    assert by_slug["scb"]["active_promos"] == 4
    assert by_slug["scb"]["merchant_name_coverage"] == pytest.approx(0.75, abs=0.001)


async def test_coverage_staleness_buckets_across_ladder(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Time-traveled promos exercise fresh/warming/stale/silent buckets.

    No ``freezegun`` available — backdate ``last_synced_at`` directly.
    """
    sessionmaker = get_sessionmaker()
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        banks = {b.slug: b for b in (await session.execute(select(Bank))).scalars().all()}
        # ktc → fresh (10 minutes ago)
        session.add(
            _promo_at(
                bank_id=banks["ktc"].id,
                title="ktc fresh",
                external_bank_key="ktc",
                external_source_id="ktc-fresh",
                last_synced_at=now - timedelta(minutes=10),
            )
        )
        # scb → warming (5 hours ago)
        session.add(
            _promo_at(
                bank_id=banks["scb"].id,
                title="scb warming",
                external_bank_key="scb",
                external_source_id="scb-warming",
                last_synced_at=now - timedelta(hours=5),
            )
        )
        # kbank → stale (48 hours ago)
        session.add(
            _promo_at(
                bank_id=banks["kbank"].id,
                title="kbank stale",
                external_bank_key="kbank",
                external_source_id="kbank-stale",
                last_synced_at=now - timedelta(hours=48),
            )
        )
        # krungsri → silent (96 hours ago, well past 72h)
        session.add(
            _promo_at(
                bank_id=banks["krungsri"].id,
                title="krungsri silent",
                external_bank_key="manual:krungsri",
                external_source_id="krungsri-silent",
                last_synced_at=now - timedelta(hours=96),
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    by_slug = {row["bank_slug"]: row for row in body["banks"]}
    assert by_slug["ktc"]["staleness_bucket"] == "fresh"
    assert by_slug["scb"]["staleness_bucket"] == "warming"
    assert by_slug["kbank"]["staleness_bucket"] == "stale"
    assert by_slug["krungsri"]["staleness_bucket"] == "silent"

    # Banks with zero promos report `staleness_hours=None` and bucket=silent.
    assert by_slug["uob"]["staleness_hours"] is None
    assert by_slug["uob"]["staleness_bucket"] == "silent"


async def test_coverage_sync_summary_surfaces_latest_runs(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """``sync_summary`` returns the most-recent SyncRun per source."""
    sessionmaker = get_sessionmaker()
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        # Two harvester runs — newest must win.
        session.add(
            SyncRun(
                source="deal_harvester",
                started_at=now - timedelta(hours=2),
                finished_at=now - timedelta(hours=2),
                status="success",
                upstream_count=10,
                inserted_count=4,
                updated_count=2,
                deactivated_count=1,
                mapping_queue_added=0,
            )
        )
        session.add(
            SyncRun(
                source="deal_harvester",
                started_at=now - timedelta(minutes=15),
                finished_at=now - timedelta(minutes=10),
                status="success",
                upstream_count=12,
                inserted_count=5,
                updated_count=3,
                deactivated_count=0,
                mapping_queue_added=1,
            )
        )
        session.add(
            SyncRun(
                source="merchant_canonicalizer",
                started_at=now - timedelta(minutes=30),
                finished_at=now - timedelta(minutes=25),
                status="success",
                upstream_count=20,
                inserted_count=18,
                updated_count=2,
                deactivated_count=0,
                mapping_queue_added=3,
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    summary = body["sync_summary"]

    dh = summary["deal_harvester"]
    assert dh is not None
    # Newest harvester run wins → upstream_count=12, not 10.
    assert dh["upstream_count"] == 12
    assert dh["inserted_count"] == 5
    assert dh["status"] == "success"
    assert dh["source"] == "deal_harvester"

    mc = summary["merchant_canonicalizer"]
    assert mc is not None
    assert mc["mapping_queue_added"] == 3
    assert mc["source"] == "merchant_canonicalizer"

    # Canonicalizer-never-ran alert must NOT appear when a row exists.
    kinds = {a["kind"] for a in body["alerts"]}
    assert "canonicalizer_never_ran" not in kinds


async def test_coverage_alert_canonicalizer_never_ran(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """When ``merchant_canonicalizer`` has zero SyncRun rows, alert fires."""
    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    assert body["sync_summary"]["merchant_canonicalizer"] is None
    kinds = {a["kind"] for a in body["alerts"]}
    assert "canonicalizer_never_ran" in kinds


async def test_coverage_alert_silent_bank(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """A bank whose newest active promo is ≥72h old surfaces a silent_bank alert."""
    sessionmaker = get_sessionmaker()
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        banks = {b.slug: b for b in (await session.execute(select(Bank))).scalars().all()}
        session.add(
            _promo_at(
                bank_id=banks["bbl"].id,
                title="bbl silent",
                external_bank_key="bbl",
                external_source_id="bbl-silent-1",
                last_synced_at=now - timedelta(hours=80),
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()

    silent_alerts = [a for a in body["alerts"] if a["kind"] == "silent_bank"]
    bbl_alert = next((a for a in silent_alerts if a["bank_slug"] == "bbl"), None)
    assert bbl_alert is not None, body["alerts"]
    assert bbl_alert["staleness_hours"] is not None
    assert bbl_alert["staleness_hours"] >= 72.0


async def test_coverage_alert_zero_promos_long_idle(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Banks with 0 active promos AND no recent sync raise zero_promos alert.

    Seed banks like ``ttb`` ship with zero promos and no SyncRun history,
    so they should fire the alert immediately (staleness_hours is None →
    treated as long-idle).
    """
    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()

    zero_alerts = [a for a in body["alerts"] if a["kind"] == "zero_promos"]
    zero_slugs = {a["bank_slug"] for a in zero_alerts}
    # Every seeded bank starts with 0 active promos in this test (no
    # `_seed_coverage_mix` call), so all should be flagged.
    assert "ttb" in zero_slugs
    assert "amex-th" in zero_slugs


async def test_coverage_no_zero_promo_alert_for_freshly_synced_empty_bank(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """A bank with one fresh active promo should NOT raise zero_promos.

    Guards against the alert firing for banks that recently synced + have
    coverage — the threshold is on (active==0) AND (idle ≥24h).
    """
    sessionmaker = get_sessionmaker()
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        banks = {b.slug: b for b in (await session.execute(select(Bank))).scalars().all()}
        session.add(
            _promo_at(
                bank_id=banks["amex-th"].id,
                title="amex fresh",
                external_bank_key="amex",
                external_source_id="amex-fresh-1",
                last_synced_at=now - timedelta(minutes=5),
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/admin/ingestion/coverage", headers=admin_headers)
    body = resp.json()
    zero_alerts = [a for a in body["alerts"] if a["kind"] == "zero_promos"]
    amex_alert = next((a for a in zero_alerts if a["bank_slug"] == "amex-th"), None)
    assert amex_alert is None
