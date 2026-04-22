"""Deterministic card ranking for a given canonical merchant.

Pure function of DB state + a latest-valuation snapshot. No LLM — the value
of this endpoint is *trust*: "we show numbers, we show the rules we applied,
you can verify it". That's what makes it defensible against AI Overview
competition (Risk 1 in STRATEGY.md).

Formula per `mvp/API_CONTRACT.md §merchants`:

    score(card, merchant) =
      (base_earn_rate(card, merchant.category_default)
        + Σ applicable_promo_uplift(promo, merchant, card))
      × thb_per_point × 1000

Where:
- `base_earn_rate` is `card.earn_rate_local[merchant.category_default]` if
  the card declares a category-specific rate, else `earn_rate_local["default"]`.
- `applicable_promo_uplift` is a naive multiplier derived from the promo's
  declared `discount_amount` / `discount_unit`. Only promos currently mapped
  to this merchant in `promos_merchant_canonical_map` count.
- Minimum-spend and stacking caps are enforced by zeroing out promos whose
  `minimum_spend` exceeds THB 1,000 (our unit of comparison) and by capping
  the cumulative uplift at 5× the base earn rate (conservative stack cap).
- `confidence` collapses to 0 when we have no valuation for the card's earn
  currency — the UI degrades gracefully to "base earn only".
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.merchant import (
    MerchantCanonical,
    PromoMerchantCanonicalMap,
)
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.promo import Promo
from loftly.schemas.merchants import MerchantRankedCard, PromoSummary

# Stacking cap: cumulative promo uplift cannot exceed this multiple of the
# card's base earn rate. Conservative — the goal is to keep the "est value"
# number believable even when several overlapping promos are active.
_STACK_CAP_MULTIPLIER = 5.0
# THB_PER_POINT × POINTS headline: we price against THB 1,000 of spend.
_HEADLINE_SPEND_THB = 1000.0
# Minimum-spend threshold above which a promo is filtered out of the
# headline math (still surfaced as an informational chip).
_MIN_SPEND_HEADLINE_THRESHOLD = Decimal("1000")


async def _fetch_valuations(
    session: AsyncSession,
) -> dict[str, PointValuation]:
    """Map currency code → most-recent PointValuation."""
    stmt = (
        select(PointValuation, LoyaltyCurrency.code)
        .join(LoyaltyCurrency, PointValuation.currency_id == LoyaltyCurrency.id)
        .order_by(PointValuation.computed_at.desc())
    )
    rows = (await session.execute(stmt)).all()
    out: dict[str, PointValuation] = {}
    for row in rows:
        valuation, code = row[0], row[1]
        if code not in out:
            out[code] = valuation
    return out


async def _fetch_merchant_promos(
    session: AsyncSession,
    merchant_id: uuid.UUID,
) -> list[Promo]:
    """Load active promos mapped to this merchant (via §16 map)."""
    stmt = (
        select(Promo)
        .join(
            PromoMerchantCanonicalMap,
            PromoMerchantCanonicalMap.promo_id == Promo.id,
        )
        .where(
            PromoMerchantCanonicalMap.merchant_canonical_id == merchant_id,
            Promo.active.is_(True),
        )
        .options(selectinload(Promo.cards))
    )
    return list((await session.execute(stmt)).scalars().unique().all())


def _base_earn_rate_for_category(card: CardModel, category: str | None) -> float:
    """Pull the card's earn rate for this category; fall back to `default`."""
    rates = card.earn_rate_local or {}
    if category and category in rates:
        return float(rates[category])
    return float(rates.get("default", 0.0))


def _promo_uplift_multiplier(promo: Promo, base_rate: float) -> float:
    """Approximate incremental earn multiplier from a promo.

    Promo schemas are messy — upstream gives us either a percentage
    ("10%"), a flat THB discount, or an "X times points" multiplier. We
    normalize to a multiplicative uplift on `base_rate`:

    - `discount_unit == 'x_multiplier'` → `(amount - 1) * base_rate`
    - `discount_unit == 'percent'` → `(amount/100)` THB/THB uplift, which we
      convert to points by dividing by the current card's effective value.
      Since we don't know the redemption value here, we treat 1% ≈ 0.01 extra
      points/THB as a conservative proxy. (Cashback is usually THB-on-THB,
      miles are higher variance — the Selector surface handles that precisely.)
    - Anything else → 0 (surfaced in `applicable_promos` only).
    """
    amount = float(promo.discount_amount or 0)
    unit = (promo.discount_unit or "").lower()
    if amount <= 0:
        return 0.0
    if unit == "x_multiplier":
        # "3x points" → uplift = (3 - 1) * base = 2 * base
        if amount <= 1.0:
            return 0.0
        return (amount - 1.0) * base_rate
    if unit == "percent":
        return amount / 100.0
    if unit == "thb":
        # THB-per-THB on a THB 1,000 headline => amount / 1000 per THB.
        return amount / _HEADLINE_SPEND_THB
    return 0.0


async def rank_cards_for_merchant(
    session: AsyncSession,
    merchant_id: uuid.UUID,
    *,
    user_card_ids: list[uuid.UUID] | None = None,
) -> list[MerchantRankedCard]:
    """Rank all active cards for this merchant; return sorted desc.

    Args:
        session: SQLAlchemy async session.
        merchant_id: canonical merchant UUID.
        user_card_ids: if provided, restrict to cards the user already owns
            (authed path — "what do I earn at Starbucks with my wallet?").

    Returns:
        A list of `MerchantRankedCard` — sorted by
        `est_value_per_1000_thb` desc. Empty list if the merchant has no
        active catalog rows.
    """
    merchant = (
        await session.execute(select(MerchantCanonical).where(MerchantCanonical.id == merchant_id))
    ).scalar_one_or_none()
    if merchant is None or merchant.status != "active":
        return []

    cards_stmt = (
        select(CardModel)
        .where(CardModel.status == "active")
        .options(selectinload(CardModel.bank), selectinload(CardModel.earn_currency))
    )
    if user_card_ids:
        cards_stmt = cards_stmt.where(CardModel.id.in_(user_card_ids))
    cards = list((await session.execute(cards_stmt)).scalars().unique().all())

    valuations = await _fetch_valuations(session)
    promos = await _fetch_merchant_promos(session, merchant_id)

    # Group promos by applicable card_id — a promo with an empty card list
    # counts as "applies to all" (the bank hasn't scoped it); otherwise
    # only cards mentioned in `promo.cards` qualify for the uplift.
    promos_any: list[Promo] = []
    promos_by_card: dict[uuid.UUID, list[Promo]] = {}
    for promo in promos:
        card_ids = [c.id for c in (promo.cards or [])]
        if not card_ids:
            promos_any.append(promo)
        else:
            for cid in card_ids:
                promos_by_card.setdefault(cid, []).append(promo)

    ranked: list[MerchantRankedCard] = []
    for card in cards:
        base_rate = _base_earn_rate_for_category(card, merchant.category_default)
        applicable = [*promos_any, *promos_by_card.get(card.id, [])]

        applied_rules: list[str] = []
        uplift_total = 0.0
        applicable_summaries: list[PromoSummary] = []
        for promo in applicable:
            # Surface every promo that applies — even if it doesn't move
            # the headline — so the user sees the full set of active deals.
            applicable_summaries.append(
                PromoSummary(
                    id=str(promo.id),
                    title_th=promo.title_th,
                    title_en=promo.title_en,
                    discount_value=promo.discount_value,
                    valid_until=(promo.valid_until.isoformat() if promo.valid_until else None),
                )
            )

            # Minimum spend filter — don't let a "THB 5,000 minimum" promo
            # inflate the "earn per THB 1,000" headline.
            if (
                promo.minimum_spend is not None
                and promo.minimum_spend > _MIN_SPEND_HEADLINE_THRESHOLD
            ):
                applied_rules.append(f"min_spend_filter:{promo.id}")
                continue

            uplift = _promo_uplift_multiplier(promo, base_rate)
            if uplift > 0:
                uplift_total += uplift
                applied_rules.append(f"promo_uplift:{promo.id}:{round(uplift, 4)}")

        # Stacking cap — keep the headline believable.
        cap = _STACK_CAP_MULTIPLIER * max(base_rate, 1.0)
        if uplift_total > cap:
            applied_rules.append(f"stack_cap_applied:{round(cap, 4)}")
            uplift_total = cap

        currency_code = card.earn_currency.code if card.earn_currency else None
        valuation = valuations.get(currency_code or "")
        thb_per_point = float(valuation.thb_per_point) if valuation else 0.0
        if valuation is None:
            applied_rules.append("missing_valuation")

        # Points earned per THB of spend × THB_PER_POINT × 1000
        effective_rate = base_rate + uplift_total
        est_value = effective_rate * thb_per_point * _HEADLINE_SPEND_THB
        confidence = float(valuation.confidence) if valuation else 0.0

        ranked.append(
            MerchantRankedCard(
                card_slug=card.slug,
                display_name=card.display_name,
                bank_display_name_th=card.bank.display_name_th if card.bank else None,
                base_earn_rate=base_rate,
                applicable_promos=applicable_summaries,
                est_value_per_1000_thb=round(est_value, 2),
                confidence=confidence,
                applied_rules=applied_rules,
                affiliate_apply_url=f"/apply/{card.id}",
                user_owns=bool(user_card_ids and card.id in set(user_card_ids)),
            )
        )

    ranked.sort(key=lambda r: r.est_value_per_1000_thb, reverse=True)
    return ranked


__all__ = ["rank_cards_for_merchant"]
