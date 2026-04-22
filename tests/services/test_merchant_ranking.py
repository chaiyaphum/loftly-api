"""Unit tests for `loftly.services.merchant_ranking.rank_cards_for_merchant`.

Covers the Q18 Merchant Reverse Lookup ranking contract (API_CONTRACT.md
§merchants, 2026-04-22). The ranking service is a pure function of DB state
— no LLM at query time — so every test seeds fixtures and asserts exact
ordering / numeric output. A golden-fixture test pins the full dict for
regression coverage.

Edge cases under test:
- Merchant with zero mapped promos → base-rate-only ranking
- Promo with `valid_until < today` excluded
- Promo with `valid_from > today` excluded
- `category_bonus` with NULL category → chip only, no uplift
- `category_bonus` with mismatched category → chip only, no uplift
- Card whose `earn_rate_local` lacks both `default` and the category → 0
- `minimum_spend` above the THB 1,000 threshold → chip only, no uplift
- Low-confidence valuation surfaces via `applied_rules`
- `promo_card_map` empty → applies to all cards
- `promo_card_map` scoped → only listed cards get the uplift
- Two promos on the same card stack *additively* (with stack cap)
- `user_card_ids` filters to wallet + sets `user_owns=true`
- Tie-break: equal est_value → confidence desc → card_slug asc
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.merchant import (
    MerchantCanonical,
    PromoMerchantCanonicalMap,
)
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.promo import Promo
from loftly.services.merchant_ranking import (
    BASE_SPEND_THB,
    MIN_SPEND_HEADLINE_THRESHOLD,
    MIN_VALUATION_CONFIDENCE_FOR_FIRM_DISPLAY,
    STACK_CAP_MULTIPLIER,
    rank_cards_for_merchant,
)

# A stable "today" for all tests — matches the seed fixtures in the repo.
TODAY = date(2026, 4, 22)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


async def _get_seed_bank_and_currency() -> tuple[Bank, LoyaltyCurrency]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == "kbank"))).scalars().one()
        cur = (
            (
                await session.execute(
                    select(LoyaltyCurrency).where(LoyaltyCurrency.code == "K_POINT")
                )
            )
            .scalars()
            .one()
        )
    return bank, cur


async def _insert_merchant(
    *,
    slug: str = "starbucks",
    display_name_th: str = "สตาร์บัคส์",
    display_name_en: str = "Starbucks",
    category_default: str | None = "dining",
    status: str = "active",
    merchant_type: str = "fnb",
) -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        m = MerchantCanonical(
            slug=slug,
            display_name_th=display_name_th,
            display_name_en=display_name_en,
            category_default=category_default,
            merchant_type=merchant_type,
            status=status,
            alt_names=[],
        )
        session.add(m)
        await session.commit()
        return m.id


async def _insert_card(
    *,
    slug: str,
    display_name: str,
    bank: Bank,
    currency: LoyaltyCurrency,
    earn_rate_local: dict[str, float],
) -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        c = Card(
            slug=slug,
            display_name=display_name,
            bank_id=bank.id,
            earn_currency_id=currency.id,
            network="Visa",
            earn_rate_local=earn_rate_local,
            earn_rate_foreign={"default": 1.0},
            benefits={},
        )
        session.add(c)
        await session.commit()
        return c.id


async def _insert_valuation(
    currency: LoyaltyCurrency,
    *,
    thb_per_point: str,
    confidence: str = "0.8",
) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        session.add(
            PointValuation(
                currency_id=currency.id,
                thb_per_point=Decimal(thb_per_point),
                methodology="p80_award_chart_vs_cash",
                percentile=80,
                sample_size=10,
                confidence=Decimal(confidence),
            )
        )
        await session.commit()


async def _insert_promo_and_map(
    *,
    bank: Bank,
    merchant_id: uuid.UUID,
    title_th: str,
    promo_type: str = "category_bonus",
    category: str | None = "dining",
    discount_amount: Decimal | None = Decimal("10.00"),
    discount_unit: str | None = "percent",
    discount_value: str | None = "10%",
    minimum_spend: Decimal | None = None,
    valid_from: date | None = None,
    valid_until: date | None = None,
    active: bool = True,
    card_ids: list[uuid.UUID] | None = None,
) -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        p = Promo(
            bank_id=bank.id,
            source_url="https://example.test/promo",
            promo_type=promo_type,
            title_th=title_th,
            merchant_name="Starbucks",
            category=category,
            discount_type="percentage",
            discount_value=discount_value,
            discount_amount=discount_amount,
            discount_unit=discount_unit,
            minimum_spend=minimum_spend,
            valid_from=valid_from,
            valid_until=valid_until,
            active=active,
        )
        # Attach cards if requested.
        if card_ids:
            rows = (
                (await session.execute(select(Card).where(Card.id.in_(card_ids)))).scalars().all()
            )
            p.cards.extend(list(rows))
        session.add(p)
        await session.commit()
        promo_id = p.id

        session.add(
            PromoMerchantCanonicalMap(
                promo_id=promo_id,
                merchant_canonical_id=merchant_id,
                confidence=Decimal("1.00"),
                method="exact",
            )
        )
        await session.commit()
        return promo_id


# ---------------------------------------------------------------------------
# Basic paths
# ---------------------------------------------------------------------------


async def test_empty_merchant_returns_cards_by_base_rate(seeded_db: object) -> None:
    """Merchant with zero mapped promos → still ranks every active card."""
    _ = seeded_db
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    # seeded_db inserts 3 sample cards — all should surface, ordered by est_value.
    assert len(ranked) == 3
    # No promos → no uplift, no rule chatter except possibly valuation notes.
    for row in ranked:
        assert row.applicable_promos == []
        assert all(not r.startswith("promo_uplift:") for r in row.applied_rules)


async def test_missing_merchant_returns_empty(seeded_db: object) -> None:
    _ = seeded_db
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, uuid.uuid4(), as_of=TODAY)
    assert ranked == []


async def test_disabled_merchant_returns_empty(seeded_db: object) -> None:
    _ = seeded_db
    merchant_id = await _insert_merchant(slug="disabled", status="disabled")
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)
    assert ranked == []


# ---------------------------------------------------------------------------
# Expiry & validity window
# ---------------------------------------------------------------------------


async def test_expired_promo_is_excluded(seeded_db: object) -> None:
    """`valid_until < today` → the promo must not surface at all."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="ExpiredPromo",
        valid_until=TODAY - timedelta(days=1),
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        titles = [p.title_th for p in row.applicable_promos]
        assert "ExpiredPromo" not in titles


async def test_future_start_promo_is_excluded(seeded_db: object) -> None:
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="FuturePromo",
        valid_from=TODAY + timedelta(days=7),
        valid_until=TODAY + timedelta(days=30),
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        titles = [p.title_th for p in row.applicable_promos]
        assert "FuturePromo" not in titles


async def test_evergreen_promo_included(seeded_db: object) -> None:
    """`valid_until IS NULL` → always active (bank evergreen promo)."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="EvergreenPromo",
        valid_until=None,
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    # It shows up on every card (promo_card_map is empty → applies to all).
    assert ranked, "expected non-empty result"
    for row in ranked:
        titles = [p.title_th for p in row.applicable_promos]
        assert "EvergreenPromo" in titles


# ---------------------------------------------------------------------------
# Category-bonus guards
# ---------------------------------------------------------------------------


async def test_category_bonus_null_category_is_chip_only(seeded_db: object) -> None:
    """promo_type='category_bonus' with category=NULL → chip, no uplift."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="NullCategoryBonus",
        promo_type="category_bonus",
        category=None,
        discount_amount=Decimal("50.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        titles = [p.title_th for p in row.applicable_promos]
        assert "NullCategoryBonus" in titles
        # No `promo_uplift:` rule fired — this promo was skipped for scoring.
        assert any(r.startswith("category_mismatch:") for r in row.applied_rules)
        assert all(not r.startswith("promo_uplift:") for r in row.applied_rules)


async def test_category_bonus_mismatched_category_is_chip_only(seeded_db: object) -> None:
    """Promo category='online' vs merchant.category_default='dining' → chip only."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant(category_default="dining")

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="WrongCategoryPromo",
        promo_type="category_bonus",
        category="online",
        discount_amount=Decimal("25.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        assert any(r.startswith("category_mismatch:") for r in row.applied_rules)


async def test_cashback_promo_with_null_category_still_applies(
    seeded_db: object,
) -> None:
    """Non-category_bonus promo types bypass the category guard."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()
    await _insert_valuation((await _get_seed_bank_and_currency())[1], thb_per_point="0.25")

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="CashbackPromo",
        promo_type="cashback",
        category=None,
        discount_amount=Decimal("5.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    # At least one card should show the promo uplift rule (K_POINT card).
    kpoint_rows = [r for r in ranked if r.card_slug == "kbank-wisdom"]
    assert kpoint_rows, "expected K_POINT card to be present"
    assert any(r.startswith("promo_uplift:") for r in kpoint_rows[0].applied_rules)


# ---------------------------------------------------------------------------
# Base earn-rate handling
# ---------------------------------------------------------------------------


async def test_zero_base_rate_card_still_ranks(seeded_db: object) -> None:
    """Card with no `default` and no matching category key → base_rate=0."""
    _ = seeded_db
    bank, cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant(category_default="travel")

    await _insert_card(
        slug="zero-rate-card",
        display_name="Zero Rate Card",
        bank=bank,
        currency=cur,
        earn_rate_local={"dining": 1.0},  # no 'default', no 'travel'
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    zero = [r for r in ranked if r.card_slug == "zero-rate-card"]
    assert zero, "expected zero-rate card to be present"
    assert zero[0].base_earn_rate == 0.0
    assert zero[0].est_value_per_1000_thb == 0.0


# ---------------------------------------------------------------------------
# Minimum-spend guard
# ---------------------------------------------------------------------------


async def test_high_minimum_spend_filters_uplift(seeded_db: object) -> None:
    """`minimum_spend > THB 1,000` → chip but no uplift."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="HighMinSpend",
        minimum_spend=MIN_SPEND_HEADLINE_THRESHOLD + Decimal("1"),
        discount_amount=Decimal("20.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        assert any(r.startswith("min_spend_filter:") for r in row.applied_rules)
        assert all(not r.startswith("promo_uplift:") for r in row.applied_rules)


async def test_minimum_spend_at_threshold_allows_uplift(seeded_db: object) -> None:
    """`minimum_spend == THB 1,000` (equal, not greater) still counts."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="BoundaryMinSpend",
        minimum_spend=MIN_SPEND_HEADLINE_THRESHOLD,
        discount_amount=Decimal("10.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    # At least one card should have seen the uplift rule.
    any_uplift = any(
        any(r.startswith("promo_uplift:") for r in row.applied_rules) for row in ranked
    )
    assert any_uplift


# ---------------------------------------------------------------------------
# promo_card_map scoping
# ---------------------------------------------------------------------------


async def test_promo_card_map_scopes_uplift_to_listed_cards(
    seeded_db: object,
) -> None:
    """Promo with `cards=[X]` → only card X gets the uplift rule; chip shown only there."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        kbank_wisdom = (
            (await session.execute(select(Card).where(Card.slug == "kbank-wisdom"))).scalars().one()
        )

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="ScopedPromo",
        card_ids=[kbank_wisdom.id],
        discount_amount=Decimal("10.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        has_chip = any(p.title_th == "ScopedPromo" for p in row.applicable_promos)
        if row.card_slug == "kbank-wisdom":
            assert has_chip
        else:
            assert not has_chip, f"promo leaked onto unrelated card {row.card_slug}"


async def test_multiple_promos_same_card_stack_additively(seeded_db: object) -> None:
    """Two promos, both scoped to the same card → uplift sums (up to stack cap)."""
    _ = seeded_db
    bank, cur = await _get_seed_bank_and_currency()
    await _insert_valuation(cur, thb_per_point="0.25", confidence="0.85")
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (
            (await session.execute(select(Card).where(Card.slug == "kbank-wisdom"))).scalars().one()
        )

    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="Stack-A-5pct",
        card_ids=[card.id],
        discount_amount=Decimal("5.00"),
        discount_unit="percent",
    )
    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="Stack-B-10pct",
        card_ids=[card.id],
        discount_amount=Decimal("10.00"),
        discount_unit="percent",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    row = next(r for r in ranked if r.card_slug == "kbank-wisdom")
    uplift_rules = [r for r in row.applied_rules if r.startswith("promo_uplift:")]
    assert len(uplift_rules) == 2


async def test_stack_cap_clamps_extreme_uplift(seeded_db: object) -> None:
    """Sum of uplifts > cap → cap applied rule fires + uplift_total clamped."""
    _ = seeded_db
    bank, cur = await _get_seed_bank_and_currency()
    await _insert_valuation(cur, thb_per_point="0.25", confidence="0.85")
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (
            (await session.execute(select(Card).where(Card.slug == "kbank-wisdom"))).scalars().one()
        )
    # base_rate for 'dining' is 2.0 on kbank-wisdom seed → cap = 5 × 2 = 10.
    # Two 'x_multiplier' 4x promos → each uplift=(4-1)*2=6 → sum=12 > 10.
    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="4x-A",
        card_ids=[card.id],
        discount_amount=Decimal("4.00"),
        discount_unit="x_multiplier",
        promo_type="category_bonus",
        category="dining",
    )
    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="4x-B",
        card_ids=[card.id],
        discount_amount=Decimal("4.00"),
        discount_unit="x_multiplier",
        promo_type="category_bonus",
        category="dining",
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)
    row = next(r for r in ranked if r.card_slug == "kbank-wisdom")
    assert any(r.startswith("stack_cap_applied:") for r in row.applied_rules)
    # Effective rate = base (2) + uplift (capped at 10) = 12; × 0.25 × 1000 = 3000.
    expected = (2.0 + STACK_CAP_MULTIPLIER * 2.0) * 0.25 * BASE_SPEND_THB
    assert row.est_value_per_1000_thb == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Valuation confidence surfacing
# ---------------------------------------------------------------------------


async def test_missing_valuation_emits_rule_and_zero_confidence(
    seeded_db: object,
) -> None:
    """No PointValuation row for the card's currency → rule + confidence=0."""
    _ = seeded_db
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    for row in ranked:
        assert "missing_valuation" in row.applied_rules
        assert row.confidence == 0.0
        assert row.est_value_per_1000_thb == 0.0


async def test_low_confidence_valuation_surfaces_rule(seeded_db: object) -> None:
    """confidence < 0.6 → explicit `low_valuation_confidence` rule (UI uses "~")."""
    _ = seeded_db
    _bank, cur = await _get_seed_bank_and_currency()
    await _insert_valuation(cur, thb_per_point="0.20", confidence="0.40")
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    kbank_row = next(r for r in ranked if r.card_slug == "kbank-wisdom")
    assert 0.0 < kbank_row.confidence < MIN_VALUATION_CONFIDENCE_FOR_FIRM_DISPLAY
    assert any(r.startswith("low_valuation_confidence:") for r in kbank_row.applied_rules)


# ---------------------------------------------------------------------------
# user_card_ids wallet scoping
# ---------------------------------------------------------------------------


async def test_user_scope_filters_to_owned_cards_and_marks_ownership(
    seeded_db: object,
) -> None:
    _ = seeded_db
    merchant_id = await _insert_merchant()
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (
            (await session.execute(select(Card).where(Card.slug == "kbank-wisdom"))).scalars().one()
        )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(
            session,
            merchant_id,
            user_card_ids=[card.id],
            as_of=TODAY,
        )

    assert len(ranked) == 1
    assert ranked[0].card_slug == "kbank-wisdom"
    assert ranked[0].user_owns is True


async def test_anon_scope_marks_no_card_as_owned(seeded_db: object) -> None:
    _ = seeded_db
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)
    assert all(r.user_owns is False for r in ranked)


# ---------------------------------------------------------------------------
# Sort stability / tie-break
# ---------------------------------------------------------------------------


async def test_tiebreak_on_equal_value_prefers_higher_confidence(
    seeded_db: object,
) -> None:
    """Same est_value → higher-confidence valuation ranks first."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant(category_default="dining")

    # Two fresh currencies — same thb_per_point, different confidence.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        hi = LoyaltyCurrency(
            code="TIE_HI",
            display_name_en="Tie Hi",
            display_name_th="Tie Hi",
            currency_type="bank_proprietary",
        )
        lo = LoyaltyCurrency(
            code="TIE_LO",
            display_name_en="Tie Lo",
            display_name_th="Tie Lo",
            currency_type="bank_proprietary",
        )
        session.add_all([hi, lo])
        await session.commit()

    await _insert_valuation(hi, thb_per_point="0.10", confidence="0.90")
    await _insert_valuation(lo, thb_per_point="0.10", confidence="0.50")

    # Two cards with identical base rate — only the valuation differs.
    await _insert_card(
        slug="tie-hi-card",
        display_name="Tie Hi",
        bank=bank,
        currency=hi,
        earn_rate_local={"dining": 1.0, "default": 1.0},
    )
    await _insert_card(
        slug="tie-lo-card",
        display_name="Tie Lo",
        bank=bank,
        currency=lo,
        earn_rate_local={"dining": 1.0, "default": 1.0},
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    rows = [r for r in ranked if r.card_slug in {"tie-hi-card", "tie-lo-card"}]
    assert [r.card_slug for r in rows] == ["tie-hi-card", "tie-lo-card"]


async def test_tiebreak_on_equal_value_and_confidence_uses_slug_asc(
    seeded_db: object,
) -> None:
    """Fully identical rows fall back to card_slug alphabetical."""
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()
    merchant_id = await _insert_merchant()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        cur = LoyaltyCurrency(
            code="TIE_EQ",
            display_name_en="Tie Eq",
            display_name_th="Tie Eq",
            currency_type="bank_proprietary",
        )
        session.add(cur)
        await session.commit()

    await _insert_valuation(cur, thb_per_point="0.10", confidence="0.90")

    await _insert_card(
        slug="z-card",
        display_name="Z Card",
        bank=bank,
        currency=cur,
        earn_rate_local={"default": 1.0},
    )
    await _insert_card(
        slug="a-card",
        display_name="A Card",
        bank=bank,
        currency=cur,
        earn_rate_local={"default": 1.0},
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    rows = [r for r in ranked if r.card_slug in {"a-card", "z-card"}]
    assert [r.card_slug for r in rows] == ["a-card", "z-card"]


# ---------------------------------------------------------------------------
# Golden fixture — end-to-end snapshot test
# ---------------------------------------------------------------------------


async def test_golden_ranking_snapshot(seeded_db: object) -> None:
    """Hand-crafted merchant + 2 promos + 3 seed cards + valuations.

    Asserts the full dict shape so any drift in the ranking algorithm shows
    up as a single diff. Values are chosen to be exact floats (no float
    jitter) so we can compare to 2-decimal rounding directly.
    """
    _ = seeded_db
    bank, _cur = await _get_seed_bank_and_currency()

    # Valuations for all three seed cards. Values picked to give clean decimals.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        all_currs = (await session.execute(select(LoyaltyCurrency))).scalars().all()
        currs_by_code = {c.code: c for c in all_currs}
    await _insert_valuation(currs_by_code["K_POINT"], thb_per_point="0.25", confidence="0.85")
    await _insert_valuation(currs_by_code["UOB_REWARDS"], thb_per_point="0.20", confidence="0.75")
    await _insert_valuation(currs_by_code["ROP"], thb_per_point="0.50", confidence="0.80")

    merchant_id = await _insert_merchant(
        slug="golden-cafe", display_name_en="Golden Cafe", category_default="dining"
    )

    # Promo 1: 10% dining applies to all cards.
    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="GoldenDining10",
        promo_type="category_bonus",
        category="dining",
        discount_amount=Decimal("10.00"),
        discount_unit="percent",
        discount_value="10%",
    )

    # Promo 2: +THB 50 on THB 1,000 scoped only to kbank-wisdom.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        kb = (
            (await session.execute(select(Card).where(Card.slug == "kbank-wisdom"))).scalars().one()
        )
    await _insert_promo_and_map(
        bank=bank,
        merchant_id=merchant_id,
        title_th="GoldenKB-THB50",
        promo_type="category_bonus",
        category="dining",
        discount_amount=Decimal("50.00"),
        discount_unit="thb",
        discount_value="THB 50",
        card_ids=[kb.id],
    )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        ranked = await rank_cards_for_merchant(session, merchant_id, as_of=TODAY)

    assert len(ranked) == 3
    # Condense to a golden dict — keep only fields we assert on.
    compact = [
        {
            "card_slug": r.card_slug,
            "base_earn_rate": r.base_earn_rate,
            "est_value_per_1000_thb": r.est_value_per_1000_thb,
            "confidence": round(r.confidence, 2),
            "applicable_promo_titles": sorted([p.title_th for p in r.applicable_promos]),
            "user_owns": r.user_owns,
        }
        for r in ranked
    ]

    # Expected values derived by hand:
    # kbank-wisdom: base=2.0 (dining), +0.10 (percent) + 50/1000=0.05 → rate=2.15
    #   est = 2.15 × 0.25 × 1000 = 537.50
    # scb-thai-airways: base=1.2 (dining), +0.10 → rate=1.30
    #   est = 1.30 × 0.50 × 1000 = 650.00
    # uob-prvi-miles: base=1.4 (default; dining absent), +0.10 → rate=1.50
    #   est = 1.50 × 0.20 × 1000 = 300.00
    expected = [
        {
            "card_slug": "scb-thai-airways",
            "base_earn_rate": 1.2,
            "est_value_per_1000_thb": 650.0,
            "confidence": 0.80,
            "applicable_promo_titles": ["GoldenDining10"],
            "user_owns": False,
        },
        {
            "card_slug": "kbank-wisdom",
            "base_earn_rate": 2.0,
            "est_value_per_1000_thb": 537.5,
            "confidence": 0.85,
            "applicable_promo_titles": ["GoldenDining10", "GoldenKB-THB50"],
            "user_owns": False,
        },
        {
            "card_slug": "uob-prvi-miles",
            "base_earn_rate": 1.4,
            "est_value_per_1000_thb": 300.0,
            "confidence": 0.75,
            "applicable_promo_titles": ["GoldenDining10"],
            "user_owns": False,
        },
    ]
    assert compact == expected
