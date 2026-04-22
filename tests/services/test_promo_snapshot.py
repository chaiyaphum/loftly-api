"""Unit tests for `loftly.selector.promo_snapshot`.

Covers POST_V1 §3 Tier A (2026-04-22) Promo-Aware Card Selector acceptance:

- Filtering by `active` + `valid_until`
- Ranking (has_cards first, highest discount_amount next, nearest valid_until last)
- MAX_PROMOS_IN_SNAPSHOT cap truncation
- Digest stability (identical rows -> identical digest; checksum change -> new digest)
- Empty result shape (status='ok', entries=[])
- `serialize_snapshot_for_prompt` output format matches AI_PROMPTS.md §Prompt 1 spec
- `degraded_snapshot` returns a sentinel consumable by the serializer

Uses the existing `seeded_db` fixture from `tests/conftest.py` so banks + cards
are available. We add promos directly via the ORM — no upstream mocking.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.promo import Promo
from loftly.selector.promo_snapshot import (
    MAX_PROMOS_IN_SNAPSHOT,
    build_promo_snapshot,
    degraded_snapshot,
    serialize_snapshot_for_prompt,
    snapshot_to_dict,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_seed_bank_and_card() -> tuple[Bank, Card | None]:
    """Pull the seeded kbank + the first kbank card for promo FK satisfaction."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == "kbank"))).scalars().one()
        card = (
            (await session.execute(select(Card).where(Card.bank_id == bank.id))).scalars().first()
        )
    return bank, card


def _make_promo(
    bank_id: uuid.UUID,
    *,
    title_th: str = "Starbucks 15%",
    active: bool = True,
    valid_until: date | None = None,
    valid_from: date | None = None,
    discount_amount: Decimal | None = Decimal("15.00"),
    external_checksum: str | None = "chk-a",
    merchant_name: str = "Starbucks",
    category: str = "dining",
    discount_type: str = "cashback",
    discount_value: str = "15%",
    minimum_spend: Decimal | None = None,
) -> Promo:
    return Promo(
        id=uuid.uuid4(),
        bank_id=bank_id,
        source_url="https://example.test/promo",
        promo_type="category_bonus",
        title_th=title_th,
        merchant_name=merchant_name,
        category=category,
        discount_type=discount_type,
        discount_value=discount_value,
        discount_amount=discount_amount,
        minimum_spend=minimum_spend,
        valid_from=valid_from,
        valid_until=valid_until,
        active=active,
        external_checksum=external_checksum,
    )


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


async def test_excludes_inactive_promos(seeded_db: object) -> None:
    _ = seeded_db
    bank, _ = await _get_seed_bank_and_card()
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        session.add(_make_promo(bank.id, title_th="Active", active=True))
        session.add(_make_promo(bank.id, title_th="Inactive", active=False))
        await session.commit()

    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)

    titles = [e.title_th for e in snap.entries]
    assert "Active" in titles
    assert "Inactive" not in titles


async def test_excludes_expired_promos(seeded_db: object) -> None:
    _ = seeded_db
    bank, _ = await _get_seed_bank_and_card()
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        session.add(_make_promo(bank.id, title_th="Valid", valid_until=today + timedelta(days=30)))
        session.add(_make_promo(bank.id, title_th="Expired", valid_until=today - timedelta(days=1)))
        # valid_until IS NULL is always valid (evergreen bank promo).
        session.add(_make_promo(bank.id, title_th="Evergreen", valid_until=None))
        # valid_from in the future excludes.
        session.add(
            _make_promo(
                bank.id,
                title_th="FutureStart",
                valid_from=today + timedelta(days=5),
                valid_until=today + timedelta(days=30),
            )
        )
        await session.commit()

    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)

    titles = {e.title_th for e in snap.entries}
    assert "Valid" in titles
    assert "Evergreen" in titles
    assert "Expired" not in titles
    assert "FutureStart" not in titles


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------


async def test_ranking_cards_first_then_discount_then_expiry(seeded_db: object) -> None:
    _ = seeded_db
    bank, card = await _get_seed_bank_and_card()
    assert card is not None, "seed_all should insert at least one kbank card"
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # p1: mapped card, discount 10, expires in 30d — should rank above p2
        #     because discount_amount > p2.
        p1 = _make_promo(
            bank.id,
            title_th="Mapped-10-30d",
            discount_amount=Decimal("10.00"),
            valid_until=today + timedelta(days=30),
            external_checksum="p1",
        )
        p1.cards.append(card)
        session.add(p1)

        # p2: mapped card, discount 5, expires in 5d. Lower discount than p1 but
        # still ranks above unmapped promos.
        p2 = _make_promo(
            bank.id,
            title_th="Mapped-5-5d",
            discount_amount=Decimal("5.00"),
            valid_until=today + timedelta(days=5),
            external_checksum="p2",
        )
        p2.cards.append(card)
        session.add(p2)

        # p3: unmapped (cards=[]), discount 50 — ranks LAST despite largest discount.
        p3 = _make_promo(
            bank.id,
            title_th="Unmapped-50",
            discount_amount=Decimal("50.00"),
            valid_until=today + timedelta(days=30),
            external_checksum="p3",
        )
        session.add(p3)
        await session.commit()

    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)

    titles = [e.title_th for e in snap.entries]
    assert titles.index("Mapped-10-30d") < titles.index("Mapped-5-5d")
    assert titles.index("Mapped-5-5d") < titles.index("Unmapped-50")


# ---------------------------------------------------------------------------
# Cap
# ---------------------------------------------------------------------------


async def test_cap_truncates_at_max_entries(seeded_db: object) -> None:
    _ = seeded_db
    bank, _ = await _get_seed_bank_and_card()
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        for i in range(MAX_PROMOS_IN_SNAPSHOT + 5):
            session.add(
                _make_promo(
                    bank.id,
                    title_th=f"p{i}",
                    external_checksum=f"chk-{i}",
                )
            )
        await session.commit()

    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)

    assert len(snap.entries) == MAX_PROMOS_IN_SNAPSHOT
    assert snap.total_count_before_cap == MAX_PROMOS_IN_SNAPSHOT + 5


# ---------------------------------------------------------------------------
# Digest
# ---------------------------------------------------------------------------


async def test_digest_is_stable_for_same_rows(seeded_db: object) -> None:
    _ = seeded_db
    bank, _ = await _get_seed_bank_and_card()
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        session.add(_make_promo(bank.id, title_th="stable", external_checksum="same"))
        await session.commit()

    async with sessionmaker() as session:
        snap_a = await build_promo_snapshot(session, as_of=today)
    async with sessionmaker() as session:
        snap_b = await build_promo_snapshot(session, as_of=today)

    assert snap_a.digest == snap_b.digest
    assert snap_a.digest != "degraded"


async def test_digest_changes_when_checksum_changes(seeded_db: object) -> None:
    _ = seeded_db
    bank, _ = await _get_seed_bank_and_card()
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        promo = _make_promo(bank.id, title_th="changing", external_checksum="v1")
        session.add(promo)
        await session.commit()

    async with sessionmaker() as session:
        snap_v1 = await build_promo_snapshot(session, as_of=today)

    # Mutate the upstream checksum (deal-harvester re-sync would do this).
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = (
            (await session.execute(select(Promo).where(Promo.title_th == "changing")))
            .scalars()
            .one()
        )
        row.external_checksum = "v2"
        await session.commit()

    async with sessionmaker() as session:
        snap_v2 = await build_promo_snapshot(session, as_of=today)

    assert snap_v1.digest != snap_v2.digest


# ---------------------------------------------------------------------------
# Empty + degraded
# ---------------------------------------------------------------------------


async def test_empty_snapshot_is_ok_not_degraded(seeded_db: object) -> None:
    _ = seeded_db
    today = date(2026, 4, 22)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)
    assert snap.entries == []
    assert snap.status == "ok"
    assert snap.total_count_before_cap == 0


def test_degraded_snapshot_sentinel() -> None:
    snap = degraded_snapshot(reason="query_failed")
    assert snap.entries == []
    assert snap.digest == "degraded"
    assert snap.status == "query_failed"

    # With reason="stale" the status reads "degraded" per the function contract.
    snap_stale = degraded_snapshot(reason="stale")
    assert snap_stale.status == "degraded"


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


async def test_serialize_matches_prompt_format(seeded_db: object) -> None:
    _ = seeded_db
    bank, card = await _get_seed_bank_and_card()
    assert card is not None
    today = date(2026, 4, 22)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        p = _make_promo(
            bank.id,
            title_th="Starbucks 15%",
            merchant_name="Starbucks",
            category="dining",
            discount_type="cashback",
            discount_value="15%",
            discount_amount=Decimal("15.00"),
            minimum_spend=Decimal("500.00"),
            valid_until=today + timedelta(days=20),
            external_checksum="promo-a",
        )
        p.cards.append(card)
        session.add(p)
        await session.commit()

    async with sessionmaker() as session:
        snap = await build_promo_snapshot(session, as_of=today)
    rendered = serialize_snapshot_for_prompt(snap)

    # Envelope must include as_of, count, closing tag.
    assert '<active_promos as_of="2026-04-22" count="1">' in rendered
    assert rendered.rstrip().endswith("</active_promos>")

    # Row format per AI_PROMPTS.md §Prompt 1:
    # [id] title | merchant | category | type=.. value=.. min_spend=.. valid_until=.. cards=[..]
    assert "Starbucks 15%" in rendered
    assert "| Starbucks |" in rendered
    assert "| dining |" in rendered
    assert "type=cashback value=15%" in rendered
    assert "min_spend=500" in rendered
    assert "valid_until=" + (today + timedelta(days=20)).isoformat() in rendered
    assert f"cards=[{card.id}]" in rendered


def test_serialize_empty_snapshot_human_readable() -> None:
    from loftly.selector.promo_snapshot import PromoSnapshot

    empty = PromoSnapshot(
        as_of=date(2026, 4, 22),
        entries=[],
        digest="empty",
        total_count_before_cap=0,
        approx_tokens=0,
        status="ok",
    )
    rendered = serialize_snapshot_for_prompt(empty)
    assert 'count="0"' in rendered
    assert "No active promos" in rendered


def test_serialize_degraded_snapshot_emits_sentinel() -> None:
    snap = degraded_snapshot(reason="query_failed")
    rendered = serialize_snapshot_for_prompt(snap)
    assert "PROMO_CONTEXT_UNAVAILABLE" in rendered
    assert 'status="query_failed"' in rendered


def test_snapshot_to_dict_includes_observability_fields() -> None:
    snap = degraded_snapshot(reason="stale")
    d = snapshot_to_dict(snap)
    assert d["digest"] == "degraded"
    assert d["status"] == "degraded"
    assert d["count"] == 0
    assert "as_of" in d
    assert "approx_tokens" in d
