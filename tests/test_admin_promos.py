"""Admin promo CRUD + manual-only filter."""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.db.models.promo import Promo


async def test_promos_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/promos")
    assert resp.status_code == 401


async def test_admin_create_manual_promo(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/promos",
        headers=admin_headers,
        json={
            "bank_id": "kbank",
            "source_url": "https://kbank.example/promo/1",
            "promo_type": "category_bonus",
            "title_th": "โปรโมชั่นดีๆ",
            "merchant_name": "Starbucks",
            "discount_type": "cashback",
            "discount_value": "15%",
            "active": True,
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title_th"] == "โปรโมชั่นดีๆ"
    assert body["external_source_id"] is None  # manual promo
    assert body["active"] is True

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        logs = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == "promo.created")))
            .scalars()
            .all()
        )
    assert len(logs) == 1


async def test_admin_list_promos_manual_only(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # Manual
    m = await seeded_client.post(
        "/v1/admin/promos",
        headers=admin_headers,
        json={
            "bank_id": "kbank",
            "source_url": "https://ex/1",
            "promo_type": "cashback",
            "title_th": "Manual",
        },
    )
    assert m.status_code == 201
    # Synced (via direct insert)
    sessionmaker = get_sessionmaker()
    from loftly.db.models.bank import Bank

    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == "kbank"))).scalars().one()
        session.add(
            Promo(
                bank_id=bank.id,
                external_bank_key="kasikorn",
                external_source_id="upstream-1",
                source_url="https://ex/2",
                promo_type="cashback",
                title_th="Synced",
            )
        )
        await session.commit()

    # Full list — both
    full = await seeded_client.get("/v1/admin/promos", headers=admin_headers)
    all_titles = {p["title_th"] for p in full.json()["data"]}
    assert {"Manual", "Synced"}.issubset(all_titles)

    # Manual only
    manual = await seeded_client.get("/v1/admin/promos?manual_only=true", headers=admin_headers)
    titles = {p["title_th"] for p in manual.json()["data"]}
    assert titles == {"Manual"}


async def test_admin_patch_promo(seeded_client: AsyncClient, admin_headers: dict[str, str]) -> None:
    created = await seeded_client.post(
        "/v1/admin/promos",
        headers=admin_headers,
        json={
            "bank_id": "ktc",
            "source_url": "https://x",
            "promo_type": "cashback",
            "title_th": "orig",
        },
    )
    promo_id = created.json()["id"]
    resp = await seeded_client.patch(
        f"/v1/admin/promos/{promo_id}",
        headers=admin_headers,
        json={"title_th": "updated", "active": False},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["title_th"] == "updated"
    assert resp.json()["active"] is False


async def test_admin_promo_missing_fields(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/promos",
        headers=admin_headers,
        json={"bank_id": "kbank"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "missing_fields"
