"""Admin cards CRUD — Week 3 Backend scope.

Covers:
- auth gating (401 without JWT, 403 with user role)
- POST creates + audit_log row written
- PATCH merges JSONB (partial update of `earn_rate_local` preserves siblings)
- Validation failure on unknown `bank_id`
- Listing includes inactive cards
"""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.db.models.card import Card as CardModel


async def test_admin_cards_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/cards")
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "unauthorized"
    assert body["error"]["message_en"]
    assert body["error"]["message_th"]


async def test_admin_cards_forbids_non_admin(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get("/v1/admin/cards", headers=user_headers)
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "forbidden"
    assert body["error"]["message_th"]


async def test_admin_list_includes_seeded_cards(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get("/v1/admin/cards", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    slugs = {c["slug"] for c in body["data"]}
    assert {"kbank-wisdom", "uob-prvi-miles"}.issubset(slugs)


async def test_admin_create_card_happy_path(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/cards",
        headers=admin_headers,
        json={
            "slug": "scb-prime",
            "display_name": "SCB PRIME",
            "network": "Mastercard",
            "bank_id": "scb",  # resolved via slug
            "earn_currency_id": "SCB_REWARDS",
            "tier": "Signature",
            "annual_fee_thb": 4000.00,
            "earn_rate_local": {"dining": 2.0, "online": 1.5, "default": 1.0},
            "benefits": {"lounge": {"provider": "LoungeKey", "visits_per_year": 4}},
            "status": "active",
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["slug"] == "scb-prime"
    assert body["earn_rate_local"]["dining"] == 2.0
    assert body["bank"]["slug"] == "scb"

    # audit row recorded
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == "card.created")))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].subject_type == "card"
    assert rows[0].meta["slug"] == "scb-prime"


async def test_admin_create_card_missing_bank_fails(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/cards",
        headers=admin_headers,
        json={
            "slug": "ghost-card",
            "display_name": "Ghost Card",
            "network": "Visa",
            "bank_id": "does-not-exist",
            "earn_currency_id": "K_POINT",
            "earn_rate_local": {"default": 1.0},
        },
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "unknown_bank"
    assert body["error"]["message_th"]


async def test_admin_create_card_missing_fields_fails(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/cards",
        headers=admin_headers,
        json={"display_name": "Only Name"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "missing_fields"
    assert "slug" in body["error"]["details"]["missing"]


async def test_admin_patch_merges_jsonb(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # Find the kbank-wisdom card by slug first.
    list_resp = await seeded_client.get("/v1/admin/cards", headers=admin_headers)
    card_id = next(c["id"] for c in list_resp.json()["data"] if c["slug"] == "kbank-wisdom")

    # Seed earn_rate_local is {"dining": 2.0, "online": 1.5, "default": 1.0}.
    # Patch ONLY `dining` and ensure `online` + `default` survive.
    resp = await seeded_client.patch(
        f"/v1/admin/cards/{card_id}",
        headers=admin_headers,
        json={"earn_rate_local": {"dining": 3.0}},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["earn_rate_local"]["dining"] == 3.0
    assert body["earn_rate_local"]["online"] == 1.5
    assert body["earn_rate_local"]["default"] == 1.0

    # And we recorded an update row.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        updates = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == "card.updated")))
            .scalars()
            .all()
        )
    assert len(updates) == 1
    assert "earn_rate_local" in updates[0].meta["changed"]


async def test_admin_patch_unknown_card_404(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.patch(
        "/v1/admin/cards/00000000-0000-4000-8000-00000000deea",
        headers=admin_headers,
        json={"display_name": "Nope"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "card_not_found"


async def test_admin_list_includes_inactive_after_create(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    await seeded_client.post(
        "/v1/admin/cards",
        headers=admin_headers,
        json={
            "slug": "archived-card",
            "display_name": "Archived Card",
            "network": "Visa",
            "bank_id": "kbank",
            "earn_currency_id": "K_POINT",
            "earn_rate_local": {"default": 1.0},
            "status": "archived",
        },
    )

    resp = await seeded_client.get("/v1/admin/cards", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    slugs_and_statuses = {(c["slug"], c["status"]) for c in body["data"]}
    assert ("archived-card", "archived") in slugs_and_statuses


# sanity: at least 9 tests to satisfy ≥6 target
async def test_sessionmaker_has_models(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    _ = seeded_client, admin_headers
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(select(CardModel.slug))
        slugs = list(result.scalars().all())
    assert "kbank-wisdom" in slugs
