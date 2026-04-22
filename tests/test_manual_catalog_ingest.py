"""Manual-catalog ingest — fixture loader, diff/apply semantics, CSV upload."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.promo import Promo
from loftly.jobs.manual_catalog_ingest import (
    MAX_CSV_ROWS,
    REQUIRED_CSV_COLUMNS,
    load_fixture,
    parse_csv,
    run_ingest,
)
from loftly.schemas.manual_catalog import ManualPromo

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _sample_promo(title: str = "Dining 10% cashback") -> ManualPromo:
    return ManualPromo(
        title=title,
        bank="uob",
        card_types=["UOB PRVI Miles"],
        category="dining",
        start_date=date(2026, 4, 21),
        end_date=date(2026, 7, 20),
        discount_pct=Decimal("10"),
        min_spend_thb=Decimal("1000"),
        cashback_thb=None,
        cap_thb=Decimal("500"),
        source_url="https://example.test/uob/dining",
        notes=None,
    )


# ---------------------------------------------------------------------------
# Fixture file loads + validates
# ---------------------------------------------------------------------------


def test_uob_fixture_loads() -> None:
    fixture = load_fixture("uob")
    assert fixture.bank == "uob"
    assert len(fixture.promos) >= 5
    # Every row marked TBD per task spec.
    assert all("TBD" in (p.notes or "") for p in fixture.promos)


def test_krungsri_fixture_loads() -> None:
    fixture = load_fixture("krungsri")
    assert fixture.bank == "krungsri"
    assert len(fixture.promos) >= 5


def test_missing_fixture_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_fixture("nonexistent-bank")


# ---------------------------------------------------------------------------
# run_ingest — dry-run / insert / idempotency / archival
# ---------------------------------------------------------------------------


async def test_dry_run_reports_without_writing(seeded_db: object) -> None:
    _ = seeded_db
    result = await run_ingest("uob", dry_run=True, promos=[_sample_promo()])
    assert result.dry_run is True
    assert result.inserted == 1
    assert result.upstream_count == 1

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        count = len(
            list(
                (
                    await session.execute(
                        select(Promo).where(Promo.external_bank_key == "manual:uob")
                    )
                )
                .scalars()
                .unique()
                .all()
            )
        )
    assert count == 0  # dry-run didn't write


async def test_execute_inserts_new_promos(seeded_db: object) -> None:
    _ = seeded_db
    promos = [_sample_promo("Dining 10%"), _sample_promo("Shopping 15%")]
    result = await run_ingest("uob", dry_run=False, promos=promos)
    assert result.inserted == 2
    assert result.updated == 0
    assert result.archived == 0

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(Promo).where(Promo.external_bank_key == "manual:uob")))
            .scalars()
            .unique()
            .all()
        )
    assert len(rows) == 2
    for row in rows:
        assert row.active is True
        assert row.external_source_id is not None
        assert row.external_source_id.startswith("manual:uob:")
        assert row.discount_amount == Decimal("10")
        assert row.discount_unit == "percent"


async def test_rerun_is_idempotent(seeded_db: object) -> None:
    _ = seeded_db
    promos = [_sample_promo("Dining 10%")]
    first = await run_ingest("uob", dry_run=False, promos=promos)
    assert first.inserted == 1

    second = await run_ingest("uob", dry_run=False, promos=promos)
    assert second.inserted == 0
    assert second.updated == 0
    assert second.archived == 0
    assert second.unchanged == 1


async def test_update_when_content_drifts(seeded_db: object) -> None:
    _ = seeded_db
    orig = _sample_promo("Dining 10%")
    await run_ingest("uob", dry_run=False, promos=[orig])

    # Change the discount pct — same natural key.
    drift = orig.model_copy(update={"discount_pct": Decimal("12")})
    result = await run_ingest("uob", dry_run=False, promos=[drift])
    assert result.inserted == 0
    assert result.updated == 1
    assert result.unchanged == 0

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = (
            (await session.execute(select(Promo).where(Promo.external_bank_key == "manual:uob")))
            .scalars()
            .unique()
            .one()
        )
    assert row.discount_amount == Decimal("12")


async def test_archival_flips_active_not_hard_delete(seeded_db: object) -> None:
    _ = seeded_db
    a = _sample_promo("Dining 10%")
    b = _sample_promo("Shopping 15%")
    await run_ingest("uob", dry_run=False, promos=[a, b])

    # Now only `a` is in the fixture — `b` should archive.
    result = await run_ingest("uob", dry_run=False, promos=[a])
    assert result.archived == 1
    assert result.unchanged == 1

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(Promo).where(Promo.external_bank_key == "manual:uob")))
            .scalars()
            .unique()
            .all()
        )
    # Row still present — archival is soft (active=False), not a DELETE.
    assert len(rows) == 2
    archived = [r for r in rows if r.title_th == "Shopping 15%"]
    assert len(archived) == 1
    assert archived[0].active is False


async def test_archival_does_not_touch_deal_harvester_rows(seeded_db: object) -> None:
    """Deal-harvester rows for the same bank have a different external_bank_key."""
    _ = seeded_db
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == "uob"))).scalars().one()
        session.add(
            Promo(
                bank_id=bank.id,
                external_source_id="harvester-xyz",
                external_bank_key="uob",  # NOT the "manual:uob" prefix
                source_url="https://harvester.example/uob/1",
                promo_type="category_bonus",
                title_th="From deal-harvester",
                active=True,
            )
        )
        await session.commit()

    # Ingest empty fixture — should archive nothing since harvester row uses
    # a different external_bank_key.
    result = await run_ingest("uob", dry_run=False, promos=[])
    assert result.archived == 0

    async with sessionmaker() as session:
        harvester = (
            (
                await session.execute(
                    select(Promo).where(Promo.external_source_id == "harvester-xyz")
                )
            )
            .scalars()
            .one()
        )
    assert harvester.active is True


async def test_unknown_bank_raises(seeded_db: object) -> None:
    _ = seeded_db
    with pytest.raises(ValueError, match="not found"):
        await run_ingest("not-a-bank", dry_run=True, promos=[])


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def _valid_csv_row(title: str = "Test promo") -> str:
    cols = ",".join(REQUIRED_CSV_COLUMNS)
    return (
        f"{cols}\n"
        f"{title},uob,UOB PRVI Miles,dining,2026-04-21,2026-07-20,10,1000,,500,https://e.test/1,tbd\n"
    )


def test_parse_csv_happy_path() -> None:
    rows = parse_csv(_valid_csv_row())
    assert len(rows) == 1
    assert rows[0].title == "Test promo"
    assert rows[0].card_types == ["UOB PRVI Miles"]
    assert rows[0].discount_pct == Decimal("10")
    assert rows[0].cashback_thb is None
    assert rows[0].cap_thb == Decimal("500")


def test_parse_csv_missing_column_rejected() -> None:
    bad = "title,bank,card_types\nX,uob,PRVI\n"
    with pytest.raises(ValueError, match="missing required columns"):
        parse_csv(bad)


def test_parse_csv_enforces_row_limit() -> None:
    header = ",".join(REQUIRED_CSV_COLUMNS)
    row = "T,uob,A,dining,2026-04-21,2026-07-20,10,1000,,500,https://e.test/1,"
    body = "\n".join([header] + [row] * (MAX_CSV_ROWS + 5))
    with pytest.raises(ValueError, match="exceeds max rows"):
        parse_csv(body)


def test_parse_csv_row_error_includes_row_number() -> None:
    header = ",".join(REQUIRED_CSV_COLUMNS)
    # end_date before start_date → Pydantic validation fails.
    bad = f"{header}\nX,uob,A,dining,2026-07-20,2026-04-21,10,1000,,500,https://e.test/1,\n"
    with pytest.raises(ValueError, match="row 2"):
        parse_csv(bad)


# ---------------------------------------------------------------------------
# Admin API — JSON ingest + CSV upload
# ---------------------------------------------------------------------------


async def test_admin_ingest_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post("/v1/admin/manual-catalog/uob/ingest")
    assert resp.status_code == 401


async def test_admin_ingest_json_dry_run(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/uob/ingest",
        headers=admin_headers,
        json={"dry_run": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is True
    assert body["bank_slug"] == "uob"
    assert body["inserted"] >= 1

    # Nothing should have been persisted.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(Promo).where(Promo.external_bank_key == "manual:uob")))
            .scalars()
            .unique()
            .all()
        )
    assert len(rows) == 0


async def test_admin_ingest_json_execute(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/krungsri/ingest",
        headers=admin_headers,
        json={"dry_run": False},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is False
    assert body["inserted"] >= 1

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (
                await session.execute(
                    select(Promo).where(Promo.external_bank_key == "manual:krungsri")
                )
            )
            .scalars()
            .unique()
            .all()
        )
    assert len(rows) == body["inserted"]


async def test_admin_ingest_missing_fixture_404(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/nosuchbank/ingest",
        headers=admin_headers,
        json={"dry_run": True},
    )
    assert resp.status_code == 404


async def test_admin_csv_upload_happy_path(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    csv = _valid_csv_row("Via upload")
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/uob/upload",
        headers={**admin_headers, "Content-Type": "text/csv"},
        content=csv,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["inserted"] == 1
    assert body["upstream_count"] == 1


async def test_admin_csv_upload_rejects_missing_columns(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    bad = "title,bank\nX,uob\n"
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/uob/upload",
        headers={**admin_headers, "Content-Type": "text/csv"},
        content=bad,
    )
    assert resp.status_code == 422
    assert "missing required columns" in resp.json()["error"]["message_en"]


async def test_admin_csv_upload_row_limit(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    header = ",".join(REQUIRED_CSV_COLUMNS)
    row = "T,uob,A,dining,2026-04-21,2026-07-20,10,1000,,500,https://e.test/1,"
    body = "\n".join([header] + [row] * (MAX_CSV_ROWS + 5))
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/uob/upload",
        headers={**admin_headers, "Content-Type": "text/csv"},
        content=body,
    )
    assert resp.status_code == 422
    assert "max rows" in resp.json()["error"]["message_en"].lower()


async def test_admin_csv_upload_wrong_content_type(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/manual-catalog/uob/upload",
        headers={**admin_headers, "Content-Type": "application/json"},
        content=b"{}",
    )
    assert resp.status_code == 415


async def test_admin_logs_audit_on_ingest(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    from loftly.db.models.audit import AuditLog

    await seeded_client.post(
        "/v1/admin/manual-catalog/uob/ingest",
        headers=admin_headers,
        json={"dry_run": True},
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        logs: list[Any] = list(
            (
                await session.execute(
                    select(AuditLog).where(AuditLog.action == "manual_catalog.ingested")
                )
            )
            .scalars()
            .all()
        )
    assert len(logs) == 1
    assert logs[0].meta["bank_slug"] == "uob"
