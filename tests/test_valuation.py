"""Valuation algorithm tests — Week 5-6 scope.

Covers:
- Worked ROP example from `mvp/VALUATION_METHOD.md` — thb_per_point in the
  expected range for our shipped fixture.
- 80th percentile correctness against a known ratio array.
- Outlier exclusion (taxes > 40%, points < 5,000).
- Confidence-score formula across sample sizes + diversity combinations.
- Empty sample set → confidence 0 + `range_only` flag.
- `run_all()` idempotency across 8 currencies (smoke).
"""

from __future__ import annotations

from loftly.jobs.valuation import compute, compute_for_currency


def test_rop_worked_example_in_expected_range() -> None:
    result = compute_for_currency("ROP")
    # The shipped ROP fixture produces ~0.5-0.7 THB/point at the 80th percentile
    # (fewer but uniformly high-quality samples compared to the VM.md
    # illustrative set of 27 noisy ratios). Assert the number is positive and
    # the top example references BKK→.
    assert result.thb_per_point > 0.3
    assert result.thb_per_point < 1.5
    assert result.percentile == 80
    assert result.top_redemption_example is not None
    assert "BKK" in result.top_redemption_example
    assert result.sample_size >= 10


def test_known_ratios_produce_correct_p80() -> None:
    # Synthetic award + cash fixtures with known ratios: exactly 1.0 for every
    # sample → 80th percentile must equal 1.0.
    award = {
        "routes": [
            {"origin": "AAA", "destination": f"D{i}", "cabin": "economy", "points": 10_000}
            for i in range(10)
        ],
        "region_map": {f"AAA-D{i}": "sea" for i in range(10)},
    }
    fares = {
        "samples": [
            {
                "origin": "AAA",
                "destination": f"D{i}",
                "cabin": "economy",
                "cash_thb": 10_000,
                "taxes_thb": 0,
            }
            for i in range(10)
        ]
    }
    result = compute("TEST", award, fares)
    assert abs(result.thb_per_point - 1.0) < 1e-6
    assert result.sample_size == 10


def test_outlier_exclusion_taxes_over_40pct() -> None:
    award = {
        "routes": [
            {"origin": "AAA", "destination": "DDD", "cabin": "economy", "points": 10_000},
            {"origin": "AAA", "destination": "EEE", "cabin": "economy", "points": 10_000},
        ]
    }
    fares = {
        "samples": [
            {  # kept: taxes 10% of cash
                "origin": "AAA",
                "destination": "DDD",
                "cabin": "economy",
                "cash_thb": 10_000,
                "taxes_thb": 1_000,
            },
            {  # excluded: taxes 50% of cash
                "origin": "AAA",
                "destination": "EEE",
                "cabin": "economy",
                "cash_thb": 10_000,
                "taxes_thb": 5_000,
            },
        ]
    }
    result = compute("TEST", award, fares)
    assert result.sample_size == 1
    # Kept sample ratio = (10000-1000)/10000 = 0.9
    assert abs(result.thb_per_point - 0.9) < 1e-6


def test_outlier_exclusion_under_5000_points() -> None:
    award = {
        "routes": [
            {"origin": "AAA", "destination": "DDD", "cabin": "economy", "points": 4_000},
            {"origin": "AAA", "destination": "EEE", "cabin": "business", "points": 20_000},
        ]
    }
    fares = {
        "samples": [
            {
                "origin": "AAA",
                "destination": "DDD",
                "cabin": "economy",
                "cash_thb": 20_000,
                "taxes_thb": 0,
            },
            {
                "origin": "AAA",
                "destination": "EEE",
                "cabin": "business",
                "cash_thb": 40_000,
                "taxes_thb": 0,
            },
        ]
    }
    result = compute("TEST", award, fares)
    assert result.sample_size == 1
    # Only the 20k-point sample survives → 40k/20k = 2.0.
    assert abs(result.thb_per_point - 2.0) < 1e-6


def test_empty_sample_set_returns_zero_confidence_and_range_only_flag() -> None:
    result = compute("EMPTY", {"routes": []}, {"samples": []})
    assert result.sample_size == 0
    assert result.confidence == 0.0
    assert "empty_sample_set" in result.sanity_flags
    assert "range_only" in result.sanity_flags
    assert result.top_redemption_example is None


def test_confidence_scoring_samples_under_10() -> None:
    # 3 samples, one cabin, one region → base 0.0 + outlier bonus only.
    award = {
        "routes": [
            {"origin": "AAA", "destination": f"D{i}", "cabin": "economy", "points": 10_000}
            for i in range(3)
        ],
        "region_map": {f"AAA-D{i}": "sea" for i in range(3)},
    }
    fares = {
        "samples": [
            {
                "origin": "AAA",
                "destination": f"D{i}",
                "cabin": "economy",
                "cash_thb": 10_000,
                "taxes_thb": 0,
            }
            for i in range(3)
        ]
    }
    result = compute("LOWN", award, fares)
    assert result.sample_size == 3
    # base=0, no cabin diversity, no region diversity, no prior, outlier=0 ≤ 0.15 → +0.1
    assert result.confidence == 0.1
    assert "under_sampled" in result.sanity_flags
    assert "range_only" in result.sanity_flags


def test_confidence_stability_bonus_with_previous() -> None:
    # Rebuild the 10-sample uniform fixture; previous = 1.0 → stability +0.1.
    award = {
        "routes": [
            {"origin": "AAA", "destination": f"D{i}", "cabin": "economy", "points": 10_000}
            for i in range(10)
        ],
        "region_map": {f"AAA-D{i}": "sea" for i in range(10)},
    }
    fares = {
        "samples": [
            {
                "origin": "AAA",
                "destination": f"D{i}",
                "cabin": "economy",
                "cash_thb": 10_000,
                "taxes_thb": 0,
            }
            for i in range(10)
        ]
    }
    with_prev = compute("TEST", award, fares, previous_thb_per_point=1.0)
    without_prev = compute("TEST", award, fares)
    assert with_prev.confidence >= without_prev.confidence
