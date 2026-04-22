"""Valuations endpoints — `/v1/valuations`, `/v1/valuations/{currency_code}`.

Both routes are DB-backed against `point_valuations` (latest `computed_at` per
currency), joined to `loyalty_currencies` for the embedded `Currency` object.

Detail endpoint additionally:
- recomputes `distribution_summary` (p10/p25/p50/p75/p90) on the fly from the
  shipped fixtures via `jobs.valuation.compute_for_currency`. This is cheap
  (< 10ms on an 8-route fixture) and avoids adding a DB column for a value
  that's a pure function of the award + cash inputs.
- surfaces the last 4 weekly observations from `point_valuations` as `history`.

Fallback path: when the `point_valuations` table is empty (fresh install, no
weekly job run yet), we synthesize a directional payload from
`compute_for_currency()` for a starter set of 4 currencies so the `/valuations`
page still renders real-feeling numbers. The fallback is clearly flagged in
the log + code comment so it's obvious when we're serving it.

Frontend contract mirror: `loftly-web/src/lib/api/types.ts` §Valuation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.core.logging import get_logger
from loftly.db.engine import get_session
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.jobs.valuation import compute_for_currency
from loftly.schemas.cards import Currency
from loftly.schemas.valuation import (
    Valuation,
    ValuationDetail,
    ValuationHistoryPoint,
    ValuationList,
)

log = get_logger(__name__)

router = APIRouter(prefix="/v1/valuations", tags=["valuations"])


# Starter set for fallback when the DB has no rows yet. These mirror the
# seeded currency codes in `db/seed.py` — KF/AM/BONVOY/ROP — and use the
# fixtures shipped under `src/loftly/data/` so numbers are directional, not
# fabricated. Once `jobs/valuation.run_all()` has been run even once, the
# DB-backed path takes over and this list is never consulted.
_STARTER_FALLBACK_CODES = ("KF", "AM", "BONVOY", "ROP")
_HISTORY_WEEKS = 4


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------


def _currency_schema(row: LoyaltyCurrency) -> Currency:
    return Currency(
        code=row.code,
        display_name_en=row.display_name_en,
        display_name_th=row.display_name_th,
        currency_type=row.currency_type,
        issuing_entity=row.issuing_entity,
    )


def _ensure_aware(dt: datetime) -> datetime:
    """Normalize naive timestamps (SQLite) to UTC-aware — frontend expects ISO-8601."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _effective_thb_per_point(val: PointValuation) -> float:
    """Prefer the manual override when set (per `VALUATION_METHOD.md §Overrides`)."""
    if val.override_thb_per_point is not None:
        return float(val.override_thb_per_point)
    return float(val.thb_per_point)


def _valuation_schema(val: PointValuation, currency: LoyaltyCurrency) -> Valuation:
    return Valuation(
        currency=_currency_schema(currency),
        thb_per_point=_effective_thb_per_point(val),
        methodology=val.methodology,
        percentile=val.percentile,
        sample_size=val.sample_size,
        confidence=float(val.confidence),
        top_redemption_example=val.top_redemption_example,
        computed_at=_ensure_aware(val.computed_at),
    )


async def _latest_per_currency(session: AsyncSession) -> list[tuple[PointValuation, LoyaltyCurrency]]:
    """Return (valuation, currency) tuples — one row per currency, latest `computed_at`."""
    # Subquery: max(computed_at) grouped by currency_id.
    latest_subq = (
        select(
            PointValuation.currency_id,
            func.max(PointValuation.computed_at).label("max_computed_at"),
        )
        .group_by(PointValuation.currency_id)
        .subquery()
    )
    stmt = (
        select(PointValuation, LoyaltyCurrency)
        .join(
            latest_subq,
            (PointValuation.currency_id == latest_subq.c.currency_id)
            & (PointValuation.computed_at == latest_subq.c.max_computed_at),
        )
        .join(LoyaltyCurrency, LoyaltyCurrency.id == PointValuation.currency_id)
    )
    rows = list((await session.execute(stmt)).all())
    return [(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# Fallback — synthesize valuations from shipped fixtures.
# ---------------------------------------------------------------------------


def _fallback_from_fixtures(
    currencies_by_code: dict[str, LoyaltyCurrency] | None = None,
) -> list[Valuation]:
    """Synthesize a starter payload from `jobs.valuation.compute_for_currency`.

    Returns an empty list if *no* fixture loads successfully (don't 500 — let
    the client render the empty state).
    """
    out: list[Valuation] = []
    now = datetime.now(UTC)
    for code in _STARTER_FALLBACK_CODES:
        try:
            result = compute_for_currency(code)
        except (FileNotFoundError, KeyError, ValueError) as exc:
            log.warning("valuation_fallback_fixture_missing", currency=code, error=str(exc))
            continue
        currency_row = (currencies_by_code or {}).get(code)
        if currency_row is not None:
            currency = _currency_schema(currency_row)
        else:
            # No DB row for this currency — synthesize a minimal Currency object
            # so the list still renders. display_name_th is required, so we
            # fall back to the EN name (staging-only; seeded DB will always
            # have the TH name).
            currency = _currency_schema_for_code(code)
        out.append(
            Valuation(
                currency=currency,
                thb_per_point=round(result.thb_per_point, 4),
                methodology="p80_award_chart_vs_cash",
                percentile=result.percentile,
                sample_size=result.sample_size,
                confidence=result.confidence,
                top_redemption_example=result.top_redemption_example,
                computed_at=now,
            )
        )
    return out


def _currency_schema_for_code(code: str) -> Currency:
    """Minimal Currency shape when the DB row is missing — last-resort fallback."""
    # Matches the seed rows in `db/seed.py`. Kept tight because it's only hit
    # on a completely un-seeded DB (empty loyalty_currencies table).
    meta: dict[str, dict[str, Any]] = {
        "KF": {
            "display_name_en": "KrisFlyer",
            "display_name_th": "คริสฟลายเออร์",
            "currency_type": "airline",
            "issuing_entity": "Singapore Airlines",
        },
        "AM": {
            "display_name_en": "Asia Miles",
            "display_name_th": "เอเชียไมล์ส",
            "currency_type": "airline",
            "issuing_entity": "Cathay Pacific",
        },
        "BONVOY": {
            "display_name_en": "Marriott Bonvoy",
            "display_name_th": "มาริออท บอนวอย",
            "currency_type": "hotel",
            "issuing_entity": "Marriott",
        },
        "ROP": {
            "display_name_en": "Royal Orchid Plus",
            "display_name_th": "รอยัล ออร์คิด พลัส",
            "currency_type": "airline",
            "issuing_entity": "Thai Airways",
        },
    }
    m = meta.get(code, {})
    return Currency(
        code=code,
        display_name_en=m.get("display_name_en", code),
        display_name_th=m.get("display_name_th", code),
        currency_type=m.get("currency_type", "airline"),
        issuing_entity=m.get("issuing_entity"),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=ValuationList,
    summary="All current valuations (one per currency)",
)
async def list_valuations(
    limit: int = Query(default=20, ge=1, le=100),
    order: str = Query(
        default="updated_at_desc",
        pattern="^(updated_at_desc|thb_per_point_desc|code_asc)$",
    ),
    session: AsyncSession = Depends(get_session),
) -> ValuationList:
    """Return the latest valuation per currency, newest first by default.

    Orders:
    - `updated_at_desc` (default): most-recently computed valuations first.
    - `thb_per_point_desc`: highest value per point first.
    - `code_asc`: alphabetic by currency code.
    """
    pairs = await _latest_per_currency(session)

    if not pairs:
        # DB-empty fallback — fixtures-based starter. Clearly flagged in logs
        # so we notice if this path sticks around past the first weekly job run.
        log.info("valuations_db_empty_serving_fallback", count=len(_STARTER_FALLBACK_CODES))

        # Hydrate any loyalty_currencies rows that *do* exist so we get proper
        # TH names even though no valuation has been computed yet.
        cur_rows = list((await session.execute(select(LoyaltyCurrency))).scalars().all())
        currencies_by_code = {c.code: c for c in cur_rows}
        items = _fallback_from_fixtures(currencies_by_code)
    else:
        items = [_valuation_schema(v, c) for v, c in pairs]

    if order == "thb_per_point_desc":
        items.sort(key=lambda v: v.thb_per_point, reverse=True)
    elif order == "code_asc":
        items.sort(key=lambda v: v.currency.code)
    else:  # updated_at_desc
        items.sort(key=lambda v: v.computed_at, reverse=True)

    return ValuationList(data=items[:limit])


@router.get(
    "/{currency_code}",
    response_model=ValuationDetail,
    summary="Valuation + methodology + history for one currency",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Unknown currency code"}},
)
async def get_valuation(
    currency_code: str,
    session: AsyncSession = Depends(get_session),
) -> ValuationDetail:
    """Return one currency's current valuation plus distribution + last 4 weeks.

    Lookup is case-insensitive (`krisflyer` == `KRISFLYER` == `KF` if either
    variant matches). 404 when no `loyalty_currencies` row matches.
    """
    normalized = currency_code.strip().upper()

    # Accept both the canonical code (e.g. "KF") and the display_name_en
    # uppercased (e.g. "KRISFLYER") as a convenience for the frontend, which
    # sometimes surfaces the name rather than the DB code.
    cur_stmt = select(LoyaltyCurrency).where(
        (func.upper(LoyaltyCurrency.code) == normalized)
        | (func.upper(LoyaltyCurrency.display_name_en) == normalized)
        | (
            func.upper(func.replace(LoyaltyCurrency.display_name_en, " ", ""))
            == normalized.replace(" ", "")
        )
    )
    currency = (await session.execute(cur_stmt)).scalars().unique().one_or_none()
    if currency is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="currency_not_found",
            message_en=f"No loyalty currency with code {currency_code!r}.",
            message_th=f"ไม่พบสกุลคะแนนรหัส {currency_code}",
            details={"currency_code": currency_code},
        )

    hist_stmt = (
        select(PointValuation)
        .where(PointValuation.currency_id == currency.id)
        .order_by(PointValuation.computed_at.desc())
        .limit(_HISTORY_WEEKS + 1)
    )
    history_rows = list((await session.execute(hist_stmt)).scalars().all())

    distribution = _distribution_for_code(currency.code)

    if history_rows:
        latest = history_rows[0]
        base = _valuation_schema(latest, currency)
        history = [
            ValuationHistoryPoint(
                thb_per_point=_effective_thb_per_point(r),
                computed_at=_ensure_aware(r.computed_at),
            )
            for r in history_rows[:_HISTORY_WEEKS]
        ]
    else:
        # DB-empty fallback for this currency. Compute from fixtures so the
        # page still renders. Single synthetic history point = the current
        # value; richer history arrives once the weekly job runs.
        log.info("valuation_detail_fallback", currency=currency.code)
        try:
            result = compute_for_currency(currency.code)
        except FileNotFoundError:
            raise LoftlyError(
                status_code=status.HTTP_404_NOT_FOUND,
                code="valuation_unavailable",
                message_en=(
                    f"No valuation data for {currency.code!r} yet — weekly job "
                    "has not run and no fixtures are available."
                ),
                message_th=f"ยังไม่มีข้อมูลการประเมินสำหรับ {currency.code}",
                details={"currency_code": currency.code},
            ) from None
        now = datetime.now(UTC)
        base = Valuation(
            currency=_currency_schema(currency),
            thb_per_point=round(result.thb_per_point, 4),
            methodology="p80_award_chart_vs_cash",
            percentile=result.percentile,
            sample_size=result.sample_size,
            confidence=result.confidence,
            top_redemption_example=result.top_redemption_example,
            computed_at=now,
        )
        history = [
            ValuationHistoryPoint(
                thb_per_point=round(result.thb_per_point, 4),
                computed_at=now,
            )
        ]
        distribution = distribution or result.distribution_summary or None

    return ValuationDetail(
        **base.model_dump(),
        distribution_summary=distribution,
        history=history,
    )


def _distribution_for_code(code: str) -> dict[str, float] | None:
    """Recompute p10/p25/p50/p75/p90 from shipped fixtures.

    `point_valuations` doesn't persist the distribution, and recomputing it
    from the static award+cash fixtures is deterministic and fast (< 10 ms).
    Returns `None` if the fixtures for `code` aren't present — caller decides
    whether to omit the field or use a fallback.
    """
    try:
        result = compute_for_currency(code)
    except FileNotFoundError:
        return None
    return result.distribution_summary or None


__all__ = ["router"]
