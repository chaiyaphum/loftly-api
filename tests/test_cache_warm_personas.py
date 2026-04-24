"""Unit tests for `jobs.selector_warm_personas.build_persona_payloads`.

The persona matrix is fixed (4 × 3 × 5 × 2 = 120). These tests lock in:
- Count is exactly 120.
- Each payload validates cleanly against `SelectorInput`.
- Category sums equal `monthly_spend_thb` exactly (so the 100 THB tolerance
  in `_validate_category_sum` never bites on warm-up traffic).
- All axes are represented in full (no silently-dropped combinations).
"""

from __future__ import annotations

from loftly.jobs.selector_warm_personas import (
    GOAL_TYPES,
    LOCALES,
    MONTHLY_SPEND_BUCKETS,
    SPEND_PROFILES,
    build_persona_payloads,
)
from loftly.schemas.selector import SelectorInput

_ALLOWED_CATEGORIES = {"dining", "online", "travel", "grocery", "petrol", "other"}


def test_persona_count_is_exactly_120() -> None:
    """The matrix spec is 4 × 3 × 5 × 2 = 120; anything else is a regression."""
    payloads = build_persona_payloads()
    assert len(payloads) == 120
    # Sanity on the axis cardinalities themselves.
    assert len(MONTHLY_SPEND_BUCKETS) == 4
    assert len(GOAL_TYPES) == 3
    assert len(SPEND_PROFILES) == 5
    assert len(LOCALES) == 2


def test_every_persona_validates_as_selector_input() -> None:
    """Round-trip through `SelectorInput(**p)` — catches any drift from the
    schema's `ge=5_000, le=2_000_000` bound or required-field shape.
    """
    payloads = build_persona_payloads()
    for persona in payloads:
        # build_persona_payloads already returns SelectorInput instances, but
        # the spec asks us to validate via the constructor — round-trip via
        # model_dump to exercise the actual validator.
        reparsed = SelectorInput(**persona.model_dump())
        assert reparsed.monthly_spend_thb == persona.monthly_spend_thb
        assert reparsed.goal.type == persona.goal.type
        assert reparsed.locale == persona.locale


def test_category_sums_equal_monthly_spend_exactly() -> None:
    """Sum(spend_categories) MUST equal monthly_spend_thb on every persona.

    The selector endpoint allows a ±100 THB tolerance, but warm-up payloads
    should hit the bullseye so the 120 cache keys remain stable across runs.
    """
    payloads = build_persona_payloads()
    for persona in payloads:
        total = sum(persona.spend_categories.values())
        assert total == persona.monthly_spend_thb, (
            f"Category sum {total} != monthly_spend_thb {persona.monthly_spend_thb} "
            f"for persona goal={persona.goal.type} locale={persona.locale}"
        )


def test_category_keys_are_subset_of_allowed() -> None:
    """`SelectorInput` docstring constrains keys to the 6-category set."""
    payloads = build_persona_payloads()
    for persona in payloads:
        assert set(persona.spend_categories).issubset(_ALLOWED_CATEGORIES)


def test_all_axis_values_represented() -> None:
    """Each axis value shows up at least once — catches accidental filtering."""
    payloads = build_persona_payloads()

    seen_spends = {p.monthly_spend_thb for p in payloads}
    seen_goals = {p.goal.type for p in payloads}
    seen_locales = {p.locale for p in payloads}

    assert seen_spends == set(MONTHLY_SPEND_BUCKETS)
    assert seen_goals == set(GOAL_TYPES)
    assert seen_locales == set(LOCALES)


def test_baseline_defaults_are_fixed() -> None:
    """Every persona uses empty `current_cards` and null `currency_preference`.

    Warm-up is intentionally baseline — no existing-card dilution, no
    currency steering. If a future change wants persona variants with cards
    already in hand, it'll break this assertion and force a spec review.
    """
    payloads = build_persona_payloads()
    for persona in payloads:
        assert persona.current_cards == []
        assert persona.goal.currency_preference is None


def test_spend_profile_percentages_sum_to_100() -> None:
    """Each named profile's percentage split MUST total 100; the int-round
    residual pushed into `other` relies on this invariant.
    """
    for name, profile in SPEND_PROFILES.items():
        total = sum(profile.values())
        assert total == 100, f"Profile {name!r} sums to {total}, expected 100"
        assert set(profile).issubset(_ALLOWED_CATEGORIES)
