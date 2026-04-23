"""Shared fallback valuation + category-key aliases.

Extracted from `services/merchant_ranking.py` (PR #38, commit 8220c70) so the
selector path (`ai/providers/*`, `api/routes/selector.py`) can apply the same
"DB-empty fallback" discipline without duplicating the starter table.

Contract:
- `FallbackValuation` duck-types `db.models.point_valuation.PointValuation`:
  callers access `.thb_per_point` and `.confidence` as `Decimal`. Not persisted.
- `FALLBACK_VALUATIONS_BY_CODE` holds directional THB/point estimates for every
  seeded `loyalty_currency.code`. Values follow `docs/VALUATION_METHOD.md` §seed
  + publicly posted redemptions. Confidence is clamped to 0.55-0.70 so downstream
  `low_valuation_confidence` rules can still fire where appropriate.
- `CATEGORY_KEY_ALIASES` bridges deal-harvester merchant categories
  (`grocery`, `dining-restaurants`, `ecommerce`, …) to card `earn_rate_local`
  keys (`supermarket`, `dining`, `online`, …). Unmapped categories fall
  through to `default`.

This module is intentionally dependency-light (stdlib Decimal only) so it can
be imported by provider code without pulling SQLAlchemy.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

__all__ = [
    "CATEGORY_KEY_ALIASES",
    "FALLBACK_VALUATIONS_BY_CODE",
    "FallbackValuation",
    "apply_fallback_valuations",
    "resolve_earn_rate_key",
]


class FallbackValuation:
    """Duck-typed stand-in for `PointValuation` when the DB row is missing.

    Callers access `.thb_per_point`, `.confidence`, and `.methodology` — the
    union of fields read across `services/merchant_ranking.py` and
    `ai/providers/anthropic.py` when serializing the valuations payload for
    the LLM. Not persisted, not committed, just a value object.

    `methodology` is stamped as the literal string `"fallback_starter"` so the
    prompt + Langfuse traces can distinguish a synthesized row from a real
    weekly-job output.
    """

    __slots__ = ("confidence", "methodology", "thb_per_point")

    def __init__(
        self,
        thb_per_point: float,
        confidence: float,
        *,
        methodology: str = "fallback_starter",
    ) -> None:
        self.thb_per_point = Decimal(str(thb_per_point))
        self.confidence = Decimal(str(confidence))
        self.methodology = methodology


# Per-currency fallback valuations. Mirrors the `_STARTER_FALLBACK_CODES`
# pattern in `routes/valuations.py` but extended to cover every currency
# declared in `db/seed.py CURRENCIES`. Values are directional THB/point
# estimates compiled from publicly posted redemptions + `VALUATION_METHOD.md`
# §seed numbers. Confidence clamped to 0.55 for the DB-empty path so the UI
# still shows real-feeling headlines; once the valuation weekly job
# populates `point_valuations`, the DB reads override these.
FALLBACK_VALUATIONS_BY_CODE: dict[str, tuple[float, float]] = {
    # (thb_per_point, confidence)
    "KF": (0.82, 0.6),  # KrisFlyer
    "AM": (0.80, 0.6),  # Asia Miles
    "ROP": (0.35, 0.7),  # Royal Orchid Plus
    "BONVOY": (0.35, 0.6),  # Marriott Bonvoy
    "K_POINT": (0.15, 0.55),  # KBank Rewards
    "UOB_REWARDS": (0.12, 0.55),  # UOB Rewards
    "KTC_FOREVER": (0.20, 0.55),  # KTC Forever Points
    "SCB_REWARDS": (0.18, 0.55),  # SCB M Points
    "MEMBERSHIP_REWARDS": (1.0, 0.6),  # Amex MR — higher because transferable to airlines
}


# Merchant-category → card-earn-rate-key canonicalization. Merchant categories
# come from the deal-harvester ontology (`retail`, `dining-restaurants`,
# `ecommerce`, `grocery`, `travel`, …) and from user-supplied selector
# `spend_categories` (`grocery`, `petrol`, …). Card `earn_rate_local` maps use
# their own keys (`dining`, `supermarket`, `online`, `travel`, `fuel`,
# `default`). This table bridges the two so a "grocery" merchant gets the
# card's "supermarket" rate, a "dining-restaurants" merchant gets "dining",
# etc. Unmapped categories fall through to `default` (canonical fallback rate).
CATEGORY_KEY_ALIASES: dict[str, str] = {
    "dining-restaurants": "dining",
    "dining-cafe": "dining",
    "dining-fastfood": "dining",
    "dining-buffet": "dining",
    "fnb": "dining",
    "food-and-beverage": "dining",
    "grocery": "supermarket",
    "supermarket": "supermarket",
    "convenience": "supermarket",
    "ecommerce": "online",
    "online-shopping": "online",
    "online": "online",
    "retail": "retail",
    "department-store": "retail",
    "retail-fashion": "retail",
    "travel": "travel",
    "travel-hotel": "travel",
    "travel-flight": "travel",
    "fuel": "fuel",
    "petrol": "fuel",
    "transport": "default",
    "service": "default",
    "entertainment": "entertainment",
}


def apply_fallback_valuations(
    valuations_by_code: dict[str, Any],
) -> dict[str, Any]:
    """Fill missing currency codes in `valuations_by_code` with fallback rows.

    Mutates + returns the same dict for caller convenience. Codes already
    present (from a real `PointValuation` row) are left untouched — DB rows
    always override the starter table.

    Typed as `Any` because callers hand us a `dict[str, PointValuation]` but we
    write `FallbackValuation` values that duck-type the same interface.
    Runtime code only ever reads `.thb_per_point` / `.confidence`, so the
    mixed-type dict is safe for the selector context + merchant ranker.
    """
    for code, (thb, conf) in FALLBACK_VALUATIONS_BY_CODE.items():
        if code not in valuations_by_code:
            valuations_by_code[code] = FallbackValuation(thb, conf)
    return valuations_by_code


def resolve_earn_rate_key(
    category: str | None,
    rates: dict[str, float],
) -> str | None:
    """Resolve a merchant/user category to the best matching earn_rate_local key.

    Resolution order:
      1. Exact `category` match in `rates` (e.g. card has `grocery` rate).
      2. Canonicalized alias (e.g. `grocery` → `supermarket`) if the aliased
         key is present in `rates`.
      3. `default` if present.
      4. `None` — caller decides the zero-rate behavior.
    """
    if not category:
        return "default" if "default" in rates else None
    if category in rates:
        return category
    aliased = CATEGORY_KEY_ALIASES.get(category)
    if aliased and aliased in rates:
        return aliased
    if "default" in rates:
        return "default"
    return None
