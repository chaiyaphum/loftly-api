"""Valuation algorithm — 80th-percentile thb_per_point per currency.

Numeric-only path (no LLM) per `mvp/VALUATION_METHOD.md`. The AI prompt for
`valuation_engine` is the Phase-2 upgrade (richer rationale text, transfer-bonus
detection); Phase 1 ships a deterministic implementation that:

1. Joins award-chart points to cash-fare samples by (origin, destination, cabin).
2. Computes `ratio = (cash_thb - taxes_thb) / points` per sample.
3. Excludes outliers per `VALUATION_METHOD.md §Outlier exclusion`:
   - taxes_thb > 40% of cash_thb → fuel-surcharge-heavy
   - points < 5_000 → promotional / partner-only
4. Takes the 80th percentile via NumPy `method='linear'` (matches the worked
   ROP example in VALUATION_METHOD.md).
5. Computes confidence per the methodology document (base + cabin diversity +
   region diversity + stability vs previous + outlier-exclusion share).

Exports:
- `load_fixtures(code)` — read award + cash fixtures for a currency.
- `compute(code, award, fares, previous_thb_per_point=None)` — run the math.
- `compute_for_currency(currency_code, previous_thb_per_point=None)` — convenience
  wrapper that pairs `load_fixtures` with `compute`.
- `run_all(session)` — load every currency in DB, compute, upsert a
  `point_valuations` row per currency within the current ISO week.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation

log = get_logger(__name__)

_METHODOLOGY = "p80_award_chart_vs_cash"
_PERCENTILE = 80
_MIN_POINTS = 5_000
_MAX_TAX_FRACTION = 0.40
_OUTLIER_SHARE_THRESHOLD = 0.15
_STABILITY_DELTA_THRESHOLD = 0.20


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------


_FIXTURE_ROOT = Path(__file__).resolve().parent.parent / "data"


def load_fixtures(currency_code: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (award_chart, cash_fares) JSON for `currency_code`.

    Raises `FileNotFoundError` if either fixture is missing — callers should
    catch that and skip the currency (log `valuation_skipped` per
    `AI_PROMPTS.md §Failure policy`).
    """
    award_path = _FIXTURE_ROOT / "award_charts" / f"{currency_code}.json"
    cash_path = _FIXTURE_ROOT / "cash_fares" / f"{currency_code}.json"
    with award_path.open("r", encoding="utf-8") as f:
        award = json.load(f)
    with cash_path.open("r", encoding="utf-8") as f:
        fares = json.load(f)
    return award, fares


# ---------------------------------------------------------------------------
# Core compute
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValuationResult:
    """Return shape for `compute()`. Matches SCHEMA `point_valuations` + API."""

    currency_code: str
    thb_per_point: float
    sample_size: int
    confidence: float
    percentile: int
    top_redemption_example: str | None
    distribution_summary: dict[str, float]
    sanity_flags: list[str]
    range_low: float | None
    range_high: float | None


def _pair_samples(award: dict[str, Any], fares: dict[str, Any]) -> list[dict[str, Any]]:
    """Inner-join award routes with cash fares on (origin, destination, cabin)."""
    award_idx = {(r["origin"], r["destination"], r["cabin"]): r for r in award.get("routes", [])}
    pairs: list[dict[str, Any]] = []
    for sample in fares.get("samples", []):
        key = (sample["origin"], sample["destination"], sample["cabin"])
        award_row = award_idx.get(key)
        if award_row is None:
            continue
        pairs.append(
            {
                "origin": sample["origin"],
                "destination": sample["destination"],
                "cabin": sample["cabin"],
                "points": int(award_row["points"]),
                "cash_thb": float(sample["cash_thb"]),
                "taxes_thb": float(sample.get("taxes_thb", 0.0)),
            }
        )
    return pairs


def _region_of(award: dict[str, Any], origin: str, destination: str) -> str | None:
    region_map: dict[str, str] = award.get("region_map", {}) or {}
    return region_map.get(f"{origin}-{destination}")


def _confidence(
    *,
    sample_size: int,
    cabins: set[str],
    regions: set[str | None],
    previous_thb_per_point: float | None,
    thb_per_point: float,
    excluded_share: float,
) -> float:
    """Per `VALUATION_METHOD.md §Confidence score`."""
    if sample_size >= 20:
        base = 0.6
    elif sample_size >= 10:
        base = 0.4
    else:
        base = 0.0
    score = base
    if len([c for c in cabins if c]) >= 3:
        score += 0.1
    if len([r for r in regions if r]) >= 3:
        score += 0.1
    if previous_thb_per_point is not None and thb_per_point > 0:
        delta = abs(thb_per_point - previous_thb_per_point) / previous_thb_per_point
        if delta < _STABILITY_DELTA_THRESHOLD:
            score += 0.1
    if excluded_share <= _OUTLIER_SHARE_THRESHOLD:
        score += 0.1
    return min(1.0, round(score, 2))


def compute(
    currency_code: str,
    award: dict[str, Any],
    fares: dict[str, Any],
    *,
    previous_thb_per_point: float | None = None,
) -> ValuationResult:
    """Run the valuation math. Safe on empty inputs (confidence=0)."""
    raw_samples = _pair_samples(award, fares)
    total_raw = len(raw_samples)

    # Outlier exclusion.
    filtered: list[dict[str, Any]] = []
    for s in raw_samples:
        if s["points"] < _MIN_POINTS:
            continue
        if s["cash_thb"] <= 0:
            continue
        if s["taxes_thb"] / s["cash_thb"] > _MAX_TAX_FRACTION:
            continue
        filtered.append(s)

    sanity_flags: list[str] = []
    excluded_share = 1.0 - (len(filtered) / total_raw) if total_raw > 0 else 0.0

    if not filtered:
        # Nothing to compute — return a confidence=0 placeholder. Caller decides
        # whether to persist a row (we do, so the public API can surface the flag).
        log.warning("valuation_empty_sample_set", currency=currency_code)
        return ValuationResult(
            currency_code=currency_code,
            thb_per_point=0.0,
            sample_size=0,
            confidence=0.0,
            percentile=_PERCENTILE,
            top_redemption_example=None,
            distribution_summary={},
            sanity_flags=["empty_sample_set", "range_only"],
            range_low=None,
            range_high=None,
        )

    ratios = np.array(
        [(s["cash_thb"] - s["taxes_thb"]) / s["points"] for s in filtered],
        dtype=float,
    )
    thb_per_point = float(np.percentile(ratios, _PERCENTILE, method="linear"))

    # Distribution for public methodology page.
    distribution = {
        "p10": float(np.percentile(ratios, 10)),
        "p25": float(np.percentile(ratios, 25)),
        "p50": float(np.percentile(ratios, 50)),
        "p75": float(np.percentile(ratios, 75)),
        "p90": float(np.percentile(ratios, 90)),
    }

    # Top redemption example = highest ratio.
    top_idx = int(np.argmax(ratios))
    top = filtered[top_idx]
    top_ratio = float(ratios[top_idx])
    top_example = (
        f"{top['origin']}→{top['destination']} {top['cabin']}: "
        f"{top['points']:,} points for "
        f"THB {int(top['cash_thb'] - top['taxes_thb']):,} net "
        f"({top_ratio:.3f} THB/point)"
    )

    cabins = {s["cabin"] for s in filtered}
    regions = {_region_of(award, s["origin"], s["destination"]) for s in filtered}

    confidence = _confidence(
        sample_size=len(filtered),
        cabins=cabins,
        regions=regions,
        previous_thb_per_point=previous_thb_per_point,
        thb_per_point=thb_per_point,
        excluded_share=excluded_share,
    )

    # Sanity flags.
    if previous_thb_per_point is not None and previous_thb_per_point > 0 and thb_per_point > 0:
        delta_pct = abs(thb_per_point - previous_thb_per_point) / previous_thb_per_point
        if delta_pct > 0.30:
            sanity_flags.append(f"delta_vs_previous_{int(delta_pct * 100)}pct")
    if len(filtered) < 10:
        sanity_flags.append("under_sampled")
    if confidence < 0.4:
        sanity_flags.append("range_only")

    # Range for low-confidence UI treatment.
    range_low = float(np.percentile(ratios, 25))
    range_high = float(np.percentile(ratios, 95))

    return ValuationResult(
        currency_code=currency_code,
        thb_per_point=thb_per_point,
        sample_size=len(filtered),
        confidence=confidence,
        percentile=_PERCENTILE,
        top_redemption_example=top_example,
        distribution_summary=distribution,
        sanity_flags=sanity_flags,
        range_low=range_low,
        range_high=range_high,
    )


def compute_for_currency(
    currency_code: str,
    *,
    previous_thb_per_point: float | None = None,
) -> ValuationResult:
    """Load fixtures then compute. Raises `FileNotFoundError` if fixtures missing."""
    award, fares = load_fixtures(currency_code)
    return compute(
        currency_code,
        award,
        fares,
        previous_thb_per_point=previous_thb_per_point,
    )


# ---------------------------------------------------------------------------
# DB upsert wrapper used by `scripts/run_valuation.py`.
# ---------------------------------------------------------------------------


def _iso_week_key(at: datetime | None = None) -> tuple[int, int]:
    """(iso_year, iso_week) for the given datetime. Idempotency key per currency."""
    d = at or datetime.now(UTC)
    iso = d.isocalendar()
    return (iso.year, iso.week)


async def _latest_valuation(session: AsyncSession, currency_id: Any) -> PointValuation | None:
    stmt = (
        select(PointValuation)
        .where(PointValuation.currency_id == currency_id)
        .order_by(PointValuation.computed_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().one_or_none()


async def run_all(session: AsyncSession) -> list[ValuationResult]:
    """Compute + upsert a valuation row per currency in DB. Idempotent per ISO week.

    Returns the computed `ValuationResult` list (includes ones we skipped
    writing because this week's row already exists).
    """
    currencies = list((await session.execute(select(LoyaltyCurrency))).scalars().all())
    results: list[ValuationResult] = []
    this_week = _iso_week_key()
    for currency in currencies:
        try:
            award, fares = load_fixtures(currency.code)
        except FileNotFoundError:
            log.warning("valuation_skipped_no_fixtures", currency=currency.code)
            continue
        previous = await _latest_valuation(session, currency.id)
        prev_value = float(previous.thb_per_point) if previous is not None else None
        result = compute(currency.code, award, fares, previous_thb_per_point=prev_value)
        results.append(result)

        # Idempotency within the same ISO week — skip if we already have a row.
        if previous is not None and _iso_week_key(previous.computed_at) == this_week:
            log.info(
                "valuation_already_current_week",
                currency=currency.code,
                thb_per_point=float(previous.thb_per_point),
            )
            continue

        row = PointValuation(
            currency_id=currency.id,
            thb_per_point=_dec(result.thb_per_point),
            methodology=_METHODOLOGY,
            percentile=result.percentile,
            sample_size=result.sample_size,
            confidence=_dec(result.confidence, places=2),
            top_redemption_example=result.top_redemption_example,
        )
        session.add(row)
        log.info(
            "valuation_computed",
            currency=currency.code,
            thb_per_point=result.thb_per_point,
            sample_size=result.sample_size,
            confidence=result.confidence,
            flags=result.sanity_flags,
        )

    await session.commit()
    return results


def _dec(v: float, *, places: int = 4) -> Decimal:
    """Convert float → Decimal rounded to `places` — matches Numeric(8,4)/Numeric(3,2)."""
    if math.isnan(v) or math.isinf(v):
        return Decimal("0")
    q = Decimal(10) ** -places
    return Decimal(str(v)).quantize(q)


__all__ = [
    "ValuationResult",
    "compute",
    "compute_for_currency",
    "load_fixtures",
    "run_all",
]
