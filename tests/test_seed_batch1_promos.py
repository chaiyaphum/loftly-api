"""Tests for `seed_batch1_promos` — the 8 SCB + 6 KTC staging promo seed.

Verifies:
- All 14 Batch-1 promo rows insert into a fresh in-memory DB
- Re-running the seeder inserts zero additional rows (idempotent)
- Distribution lands 8 SCB + 6 KTC, bank FKs resolve correctly
- Every row carries the required Promo fields (title_th, source_url, etc.)
"""

from __future__ import annotations

from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.promo import Promo
from loftly.db.seed import BATCH_1_PROMOS, seed_all, seed_batch1_cards, seed_batch1_promos


async def test_batch1_promos_data_shape() -> None:
    """Smoke-check the static list before any DB work — fast feedback if a
    new promo is missing a required column."""
    assert len(BATCH_1_PROMOS) == 14
    scb = [p for p in BATCH_1_PROMOS if p["bank_slug"] == "scb"]
    ktc = [p for p in BATCH_1_PROMOS if p["bank_slug"] == "ktc"]
    assert len(scb) == 8, f"expected 8 SCB promos, got {len(scb)}"
    assert len(ktc) == 6, f"expected 6 KTC promos, got {len(ktc)}"

    # external_source_id unique across the batch — idempotency key.
    ids = [p["external_source_id"] for p in BATCH_1_PROMOS]
    assert len(set(ids)) == len(ids), "duplicate external_source_id in BATCH_1_PROMOS"

    required = {
        "external_source_id",
        "external_bank_key",
        "bank_slug",
        "source_url",
        "title_th",
        "promo_type",
        "merchant_name",
        "category",
        "discount_type",
        "discount_value",
        "discount_amount",
        "discount_unit",
        "valid_from",
        "valid_until",
        "relevance_tags",
    }
    for row in BATCH_1_PROMOS:
        missing = required - row.keys()
        assert not missing, f"{row['external_source_id']} missing: {missing}"
        # No example.com / obvious demo URLs slipped in.
        assert "example.com" not in row["source_url"]
        # Dates make sense: valid_from <= valid_until.
        assert row["valid_from"] <= row["valid_until"], (
            f"{row['external_source_id']}: valid_from after valid_until"
        )


async def test_seed_batch1_promos_inserts_all_rows(app: object) -> None:
    _ = app  # app fixture builds the schema against in-memory aiosqlite

    sessionmaker = get_sessionmaker()
    # Base banks first — promos FK on banks.id. `seed_batch1_promos` re-seeds
    # banks defensively, but running `seed_all` here matches real deploy order.
    async with sessionmaker() as session:
        await seed_all(session)
    async with sessionmaker() as session:
        await seed_batch1_cards(session)

    async with sessionmaker() as session:
        inserted = await seed_batch1_promos(session)
    assert inserted == 14

    async with sessionmaker() as session:
        promos = (await session.scalars(select(Promo))).all()
        assert len(promos) == 14

        bank_by_id = {b.id: b.slug for b in (await session.scalars(select(Bank))).all()}
        by_bank: dict[str, int] = {}
        for p in promos:
            slug = bank_by_id[p.bank_id]
            by_bank[slug] = by_bank.get(slug, 0) + 1
        assert by_bank.get("scb") == 8
        assert by_bank.get("ktc") == 6

        # Spot-check: each promo has the identifying scalars the `/promos`
        # contract relies on.
        for p in promos:
            assert p.external_source_id
            assert p.title_th
            assert p.source_url.startswith("https://")
            assert p.active is True
            assert p.valid_until is not None


async def test_seed_batch1_promos_is_idempotent(app: object) -> None:
    _ = app
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await seed_all(session)
    async with sessionmaker() as session:
        first = await seed_batch1_promos(session)
    assert first == 14

    async with sessionmaker() as session:
        second = await seed_batch1_promos(session)
    assert second == 0

    # DB still holds exactly 14 — no duplicates crept in.
    async with sessionmaker() as session:
        count = len((await session.scalars(select(Promo))).all())
    assert count == 14
