"""Mapping queue auto-match + assignment."""

from __future__ import annotations

import uuid

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card as CardModel
from loftly.db.models.promo import Promo, promo_card_map


async def _insert_synced_promo(card_types: list[str], *, bank_slug: str = "kbank") -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == bank_slug))).scalars().one()
        promo = Promo(
            bank_id=bank.id,
            external_bank_key="kasikorn",
            external_source_id=f"upstream-{uuid.uuid4()}",
            source_url="https://ex/promo",
            promo_type="cashback",
            title_th="Test promo",
            raw_data={"card_types": card_types},
        )
        session.add(promo)
        await session.commit()
        return promo.id


async def test_mapping_queue_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/mapping-queue")
    assert resp.status_code == 401


async def test_mapping_queue_exact_match(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # "KBank WISDOM" matches seeded card display_name exactly.
    promo_id = await _insert_synced_promo(["KBank WISDOM"])
    resp = await seeded_client.get("/v1/admin/mapping-queue", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 1
    match = next(item for item in body["data"] if item["promo_id"] == str(promo_id))
    assert len(match["suggested_card_ids"]) == 1


async def test_mapping_queue_slug_match(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # Slugified "KBANK wisdom" should still match.
    promo_id = await _insert_synced_promo(["kbank-wisdom"])
    resp = await seeded_client.get("/v1/admin/mapping-queue", headers=admin_headers)
    body = resp.json()
    match = next(item for item in body["data"] if item["promo_id"] == str(promo_id))
    assert len(match["suggested_card_ids"]) == 1


async def test_mapping_queue_fuzzy_low_confidence(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # Typo in "WISDM" (Levenshtein 1) — same bank, same tier-token territory.
    promo_id = await _insert_synced_promo(["KBank WISDM"])
    resp = await seeded_client.get("/v1/admin/mapping-queue", headers=admin_headers)
    body = resp.json()
    match = next(item for item in body["data"] if item["promo_id"] == str(promo_id))
    # Fuzzy should populate the low-confidence list, not suggested_card_ids.
    assert match["suggested_card_ids"] == []
    assert match["low_confidence"] is True


async def test_mapping_queue_excludes_already_mapped(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    promo_id = await _insert_synced_promo(["KBank WISDOM"])
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (await session.execute(select(CardModel))).scalars().first()
        assert card is not None
        await session.execute(promo_card_map.insert().values(promo_id=promo_id, card_id=card.id))
        await session.commit()

    resp = await seeded_client.get("/v1/admin/mapping-queue", headers=admin_headers)
    body = resp.json()
    ids = {item["promo_id"] for item in body["data"]}
    assert str(promo_id) not in ids


async def test_mapping_queue_assign(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    promo_id = await _insert_synced_promo(["unrelated brand"])
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        cards = list((await session.execute(select(CardModel))).scalars().all())
    card_ids = [str(c.id) for c in cards[:2]]

    resp = await seeded_client.post(
        f"/v1/admin/mapping-queue/{promo_id}/assign",
        headers=admin_headers,
        json={"card_ids": card_ids},
    )
    assert resp.status_code == 204, resp.text

    async with sessionmaker() as session:
        mapped = list(
            (
                await session.execute(
                    select(promo_card_map.c.card_id).where(promo_card_map.c.promo_id == promo_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(mapped) == len(card_ids)

    # Assigning again should be idempotent.
    again = await seeded_client.post(
        f"/v1/admin/mapping-queue/{promo_id}/assign",
        headers=admin_headers,
        json={"card_ids": card_ids},
    )
    assert again.status_code == 204


async def test_mapping_queue_assign_validates_card_ids(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    promo_id = await _insert_synced_promo(["x"])
    bad = await seeded_client.post(
        f"/v1/admin/mapping-queue/{promo_id}/assign",
        headers=admin_headers,
        json={"card_ids": ["00000000-0000-4000-8000-00000000aabb"]},
    )
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "unknown_cards"


async def test_mapping_queue_assign_unknown_promo(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/mapping-queue/00000000-0000-4000-8000-00000000aaaa/assign",
        headers=admin_headers,
        json={"card_ids": ["00000000-0000-4000-8000-00000000bbbb"]},
    )
    assert resp.status_code == 404
