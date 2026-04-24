"""Selector cache-warm personas — 120 payload matrix.

DEVLOG 2026-04-24 §Known Issue §1 — cold `/v1/selector` hits land on a full
Sonnet round-trip from DO SGP and routinely time out at 15s. Caching the
results under `selector:{profile_hash}` is already done (24h TTL), but the
first hit still pays the full latency. This module emits a fixed list of 120
"typical" persona payloads; the cache-warm cron iterates them once per tick
so most real user submissions match an already-cached profile and return
<0.3s instead of 15s+.

Matrix (4 × 3 × 5 × 2 = 120):
  - monthly_spend_thb: 30_000 / 60_000 / 100_000 / 200_000
  - goal.type       : miles / cashback / benefits
  - spend_profile   : grocery-heavy / dining-heavy / travel-heavy /
                      online-heavy / balanced
  - locale          : th / en

Fixed choices, called out in the task spec:
  - current_cards             = [] (baseline, no existing-card dilution)
  - goal.currency_preference  = None (let the selector pick)

The five named `SpendProfile` splits are expressed as *percentages* that sum
to 100%, then scaled + int-rounded against each `monthly_spend_thb` bucket.
Residual after rounding (can be up to ±3 THB on a 6-category split) is pushed
into `other` so the total equals `monthly_spend_thb` *exactly* — inside the
100 THB tolerance `_validate_category_sum` allows, but we prefer the tighter
shape for cache-key stability across runs.
"""

from __future__ import annotations

from typing import Final

from loftly.schemas.selector import GoalType, Locale, SelectorGoal, SelectorInput

# ---------------------------------------------------------------------------
# Axis 1 — monthly_spend_thb buckets.
# ---------------------------------------------------------------------------
# Covers the four statistical modes of the Loftly target audience per
# STRATEGY.md §2: entry (30k), middle-income (60k), affluent (100k), UHNWI
# edge-case (200k). `SelectorInput` accepts [5_000, 2_000_000] — all four
# are well within range.
MONTHLY_SPEND_BUCKETS: Final[tuple[int, ...]] = (30_000, 60_000, 100_000, 200_000)

# ---------------------------------------------------------------------------
# Axis 2 — goal.type.
# ---------------------------------------------------------------------------
GOAL_TYPES: Final[tuple[GoalType, ...]] = ("miles", "cashback", "benefits")

# ---------------------------------------------------------------------------
# Axis 3 — named spend profiles.
# ---------------------------------------------------------------------------
# Each profile is a percentage split across the six allowed categories;
# `SelectorInput.spend_categories` only accepts keys in
# {dining, online, travel, grocery, petrol, other}. All five profiles include
# every key (even at 0%) so the serialized `profile_hash` shape is uniform
# across personas.
#
# Judgment calls on the splits:
#   - grocery-heavy : household-spend dominant (~45% grocery); reflects the
#                     "Central Food Hall / Tops" persona.
#   - dining-heavy  : restaurant-led (~45% dining); Bangkok urbanite.
#   - travel-heavy  : travel-led (~40% travel + petrol stack); the "miles
#                     chaser" persona that's the Selector's hero audience.
#   - online-heavy  : e-commerce led (~45% online); Shopee/Lazada power user.
#   - balanced      : even-ish split used as the "no strong preference" cell.
# Sums = 100 for each profile (verified in unit tests).
SPEND_PROFILES: Final[dict[str, dict[str, int]]] = {
    "grocery-heavy": {
        "grocery": 45,
        "dining": 15,
        "online": 15,
        "travel": 5,
        "petrol": 10,
        "other": 10,
    },
    "dining-heavy": {
        "dining": 45,
        "grocery": 15,
        "online": 15,
        "travel": 10,
        "petrol": 5,
        "other": 10,
    },
    "travel-heavy": {
        "travel": 40,
        "dining": 20,
        "online": 10,
        "grocery": 10,
        "petrol": 10,
        "other": 10,
    },
    "online-heavy": {
        "online": 45,
        "dining": 15,
        "grocery": 15,
        "travel": 10,
        "petrol": 5,
        "other": 10,
    },
    "balanced": {
        "dining": 20,
        "online": 20,
        "grocery": 20,
        "travel": 15,
        "petrol": 10,
        "other": 15,
    },
}

# ---------------------------------------------------------------------------
# Axis 4 — locale.
# ---------------------------------------------------------------------------
LOCALES: Final[tuple[Locale, ...]] = ("th", "en")

# Allowed category keys per SPEC.md §2 + `SelectorInput` docstring.
_ALLOWED_CATEGORIES: Final[frozenset[str]] = frozenset(
    {"dining", "online", "travel", "grocery", "petrol", "other"},
)


def _split_to_thb(percentages: dict[str, int], monthly_spend_thb: int) -> dict[str, int]:
    """Turn a %-split into absolute THB that sums *exactly* to `monthly_spend_thb`.

    Int-rounding on 6 categories typically leaves a residual of ±0-3 THB.
    We push that residual into `other` so the caller's cache key is stable
    across Python/CPython minor-version differences and so the
    `_validate_category_sum` 100-THB tolerance never fires on warm-up traffic.
    """
    allocations: dict[str, int] = {}
    running = 0
    for key, pct in percentages.items():
        amount = (monthly_spend_thb * pct) // 100
        allocations[key] = amount
        running += amount
    residual = monthly_spend_thb - running
    # "other" is always in every profile above; belt-and-braces if a future
    # split omits it.
    if residual != 0:
        allocations["other"] = allocations.get("other", 0) + residual
    return allocations


def build_persona_payloads() -> list[SelectorInput]:
    """Return the full 120-persona warm-up matrix.

    Order is stable (monthly_spend → goal_type → spend_profile → locale) so
    a `profile_hash` diff between runs cleanly identifies *which* persona
    changed, not a spurious key-ordering noise.
    """
    payloads: list[SelectorInput] = []
    for monthly_spend in MONTHLY_SPEND_BUCKETS:
        for goal_type in GOAL_TYPES:
            for profile_name, percentages in SPEND_PROFILES.items():
                # profile_name retained only for readability of the test
                # output; the payload itself is wholly described by the
                # allocations dict.
                del profile_name
                categories = _split_to_thb(percentages, monthly_spend)
                # Defensive: enforce the `SelectorInput` allowed-keys contract.
                assert set(categories).issubset(_ALLOWED_CATEGORIES)
                for locale in LOCALES:
                    payloads.append(
                        SelectorInput(
                            monthly_spend_thb=monthly_spend,
                            spend_categories=categories,
                            current_cards=[],
                            goal=SelectorGoal(
                                type=goal_type,
                                currency_preference=None,
                            ),
                            locale=locale,
                        ),
                    )
    return payloads


__all__ = [
    "GOAL_TYPES",
    "LOCALES",
    "MONTHLY_SPEND_BUCKETS",
    "SPEND_PROFILES",
    "build_persona_payloads",
]
