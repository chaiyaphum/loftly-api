"""Cards catalog — `GET /v1/cards`, `GET /v1/cards/{slug}`.

DB-backed in Week 2. Seed via `uv run python -m scripts.seed_catalog`.
Contract lives in `../loftly/mvp/artifacts/openapi.yaml`.
"""

from __future__ import annotations

import base64
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.transfer_ratio import TransferRatio
from loftly.schemas.cards import (
    BankMini,
    Card,
    CardComparison,
    CardComparisonList,
    CardList,
    CardSimilarList,
    CardValuationSnapshot,
    Currency,
    SignupBonus,
    TransferPartner,
)
from loftly.schemas.common import Pagination

router = APIRouter(prefix="/v1/cards", tags=["cards"])


# --- Cursor helpers --------------------------------------------------------


def _encode_cursor(card_id: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(str(card_id).encode("ascii")).decode("ascii")


def _decode_cursor(cursor: str) -> uuid.UUID:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("ascii")
        return uuid.UUID(raw)
    except (ValueError, TypeError) as exc:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_cursor",
            message_en="Cursor is malformed.",
            message_th="ตัวระบุตำแหน่งไม่ถูกต้อง",
            details={"cursor": cursor},
        ) from exc


# --- Projection ------------------------------------------------------------


def _to_schema(row: CardModel) -> Card:
    """Map an ORM Card (with eager-loaded bank + currency) to the public schema."""
    signup_bonus = None
    if row.signup_bonus:
        sb: dict[str, Any] = row.signup_bonus
        signup_bonus = SignupBonus(
            bonus_points=int(sb.get("bonus_points", 0)),
            spend_required=float(sb.get("spend_required", 0)),
            timeframe_days=int(sb.get("timeframe_days", 0)),
        )
    return Card(
        id=str(row.id),
        slug=row.slug,
        display_name=row.display_name,
        bank=BankMini(
            slug=row.bank.slug,
            display_name_en=row.bank.display_name_en,
            display_name_th=row.bank.display_name_th,
        ),
        tier=row.tier,
        network=row.network,
        annual_fee_thb=float(row.annual_fee_thb) if row.annual_fee_thb is not None else None,
        annual_fee_waiver=row.annual_fee_waiver,
        min_income_thb=(float(row.min_income_thb) if row.min_income_thb is not None else None),
        min_age=row.min_age,
        earn_currency=Currency(
            code=row.earn_currency.code,
            display_name_en=row.earn_currency.display_name_en,
            display_name_th=row.earn_currency.display_name_th,
            currency_type=row.earn_currency.currency_type,
            issuing_entity=row.earn_currency.issuing_entity,
        ),
        earn_rate_local={k: float(v) for k, v in (row.earn_rate_local or {}).items()},
        earn_rate_foreign=(
            {k: float(v) for k, v in row.earn_rate_foreign.items()}
            if row.earn_rate_foreign
            else None
        ),
        benefits=row.benefits or {},
        signup_bonus=signup_bonus,
        description_th=row.description_th,
        description_en=row.description_en,
        status=row.status,
    )


# --- Routes ----------------------------------------------------------------


@router.get(
    "",
    response_model=CardList,
    summary="List cards (public catalog)",
)
async def list_cards(
    issuer: str | None = Query(default=None, description="Bank slug filter"),
    network: str | None = Query(default=None),
    tier: str | None = Query(default=None),
    max_annual_fee: float | None = Query(default=None, ge=0),
    earn_currency: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> CardList:
    """Return active cards with optional filters and cursor pagination.

    Cursor is a base64(uuid) of the last-seen card id. Stable because results
    are ordered by id ascending.
    """
    from loftly.db.models.bank import Bank as BankModel

    stmt = select(CardModel).where(CardModel.status == "active")

    if issuer:
        stmt = stmt.join(BankModel, CardModel.bank_id == BankModel.id).where(
            BankModel.slug == issuer
        )
    if network:
        stmt = stmt.where(CardModel.network == network)
    if tier:
        stmt = stmt.where(CardModel.tier == tier)
    if max_annual_fee is not None:
        stmt = stmt.where(CardModel.annual_fee_thb <= max_annual_fee)
    if earn_currency:
        stmt = stmt.join(LoyaltyCurrency, CardModel.earn_currency_id == LoyaltyCurrency.id).where(
            LoyaltyCurrency.code == earn_currency
        )

    if cursor:
        after_id = _decode_cursor(cursor)
        stmt = stmt.where(CardModel.id > after_id)

    # +1 sentinel to know if there's another page
    stmt = stmt.order_by(CardModel.id.asc()).limit(limit + 1)

    result = await session.execute(stmt)
    rows = list(result.scalars().unique().all())
    has_more = len(rows) > limit
    page = rows[:limit]
    cursor_next = _encode_cursor(page[-1].id) if has_more and page else None

    return CardList(
        data=[_to_schema(r) for r in page],
        pagination=Pagination(
            cursor_next=cursor_next,
            has_more=has_more,
            total_estimate=None,
        ),
    )


# --- Compare + similar (W17 widget) ---------------------------------------

COMPARE_MAX_SLUGS = 3


def _compute_loftly_score(card: CardModel, valuation: PointValuation | None) -> float:
    """Toy 0–5 scoring — placeholder until the dedicated pipeline lands.

    Inputs are deterministic and unit-testable: default earn rate, annual fee
    banding, and valuation confidence when available. Scores stay in [0, 5]
    with one decimal of precision; the widget treats this as directional.
    """
    base = 2.5
    default_rate = float((card.earn_rate_local or {}).get("default", 1.0))
    base += min(1.5, max(0.0, (default_rate - 1.0)) * 1.5)
    fee = float(card.annual_fee_thb) if card.annual_fee_thb is not None else 0.0
    if fee == 0:
        base += 0.3
    elif fee > 5000:
        base -= 0.5
    if valuation is not None:
        base += float(valuation.confidence) * 0.5
    return round(max(0.0, min(5.0, base)), 1)


def _to_comparison(
    card: CardModel,
    transfer_rows: list[tuple[TransferRatio, LoyaltyCurrency]],
    valuation: PointValuation | None,
) -> CardComparison:
    partners = [
        TransferPartner(
            destination_code=dest.code,
            destination_display_name_en=dest.display_name_en,
            destination_display_name_th=dest.display_name_th,
            ratio_source=float(tr.ratio_source),
            ratio_destination=float(tr.ratio_destination),
            bonus_percentage=float(tr.bonus_percentage),
        )
        for tr, dest in transfer_rows
    ]
    snapshot: CardValuationSnapshot | None = None
    if valuation is not None:
        effective = (
            valuation.override_thb_per_point
            if valuation.override_thb_per_point is not None
            else valuation.thb_per_point
        )
        snapshot = CardValuationSnapshot(
            thb_per_point=float(effective),
            methodology=valuation.methodology,
            confidence=float(valuation.confidence),
            sample_size=valuation.sample_size,
        )
    return CardComparison(
        card=_to_schema(card),
        transfer_partners=partners,
        valuation=snapshot,
        loftly_score=_compute_loftly_score(card, valuation),
    )


@router.get(
    "/compare",
    response_model=CardComparisonList,
    summary="Compare up to 3 cards side-by-side",
)
async def compare_cards(
    slugs: str = Query(
        ...,
        description="Comma-separated list of up to 3 card slugs.",
    ),
    session: AsyncSession = Depends(get_session),
) -> CardComparisonList:
    """Return enriched CardComparison rows for the `/cards/[slug]` compare widget.

    Order of the response matches the order of `slugs` in the request. 404 if
    any slug is missing; 400 if more than 3 slugs are requested.
    """
    slug_list = [s.strip() for s in slugs.split(",") if s.strip()]
    if not slug_list:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="slugs_required",
            message_en="At least one slug is required.",
            message_th="ต้องระบุรหัสบัตรอย่างน้อย 1 รายการ",
            details={"slugs": slugs},
        )
    if len(slug_list) > COMPARE_MAX_SLUGS:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="too_many_slugs",
            message_en=f"At most {COMPARE_MAX_SLUGS} slugs may be compared at once.",
            message_th=f"เปรียบเทียบได้ไม่เกิน {COMPARE_MAX_SLUGS} บัตรต่อครั้ง",
            details={"slugs": slug_list, "max": COMPARE_MAX_SLUGS},
        )

    card_stmt = select(CardModel).where(CardModel.slug.in_(slug_list))
    card_rows = list((await session.execute(card_stmt)).scalars().unique().all())
    by_slug = {c.slug: c for c in card_rows}
    missing = [s for s in slug_list if s not in by_slug]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with slug {missing[0]!r}.",
            message_th=f"ไม่พบบัตรรหัส {missing[0]}",
            details={"missing_slugs": missing},
        )

    currency_ids = {c.earn_currency_id for c in card_rows}

    tr_stmt = (
        select(TransferRatio, LoyaltyCurrency)
        .join(LoyaltyCurrency, TransferRatio.destination_currency_id == LoyaltyCurrency.id)
        .where(TransferRatio.source_currency_id.in_(currency_ids))
    )
    tr_rows = list((await session.execute(tr_stmt)).all())
    partners_by_currency: dict[uuid.UUID, list[tuple[TransferRatio, LoyaltyCurrency]]] = {}
    for tr_row in tr_rows:
        tr, dest = tr_row[0], tr_row[1]
        partners_by_currency.setdefault(tr.source_currency_id, []).append((tr, dest))

    val_stmt = (
        select(PointValuation)
        .where(PointValuation.currency_id.in_(currency_ids))
        .order_by(PointValuation.computed_at.desc())
    )
    val_rows = list((await session.execute(val_stmt)).scalars().all())
    valuation_by_currency: dict[uuid.UUID, PointValuation] = {}
    for v in val_rows:
        if v.currency_id not in valuation_by_currency:
            valuation_by_currency[v.currency_id] = v

    ordered = [by_slug[s] for s in slug_list]
    data = [
        _to_comparison(
            c,
            partners_by_currency.get(c.earn_currency_id, []),
            valuation_by_currency.get(c.earn_currency_id),
        )
        for c in ordered
    ]
    return CardComparisonList(data=data)


@router.get(
    "/similar/{slug}",
    response_model=CardSimilarList,
    summary="Similar cards for the compare-picker autocomplete",
)
async def similar_cards(
    slug: str,
    limit: int = Query(default=5, ge=1, le=20),
    session: AsyncSession = Depends(get_session),
) -> CardSimilarList:
    """Return up to `limit` active cards sharing issuer, earn currency, or tier.

    Excludes the source card itself. Ranking is heuristic: issuer match > earn
    currency match > tier match, tiebreak by slug for determinism.
    """
    src_stmt = select(CardModel).where(CardModel.slug == slug)
    src = (await session.execute(src_stmt)).scalars().unique().one_or_none()
    if src is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with slug {slug!r}.",
            message_th=f"ไม่พบบัตรรหัส {slug}",
            details={"slug": slug},
        )

    cand_stmt = select(CardModel).where(CardModel.status == "active").where(CardModel.id != src.id)
    cand_rows = list((await session.execute(cand_stmt)).scalars().unique().all())

    def _score(row: CardModel) -> tuple[int, str]:
        score = 0
        if row.bank_id == src.bank_id:
            score += 4
        if row.earn_currency_id == src.earn_currency_id:
            score += 2
        if row.tier is not None and row.tier == src.tier:
            score += 1
        return (-score, row.slug)

    ranked = sorted(
        [r for r in cand_rows if _score(r)[0] < 0],
        key=_score,
    )[:limit]

    return CardSimilarList(data=[_to_schema(r) for r in ranked])


@router.get(
    "/{slug}",
    response_model=Card,
    summary="Card detail by slug",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
    },
)
async def get_card(
    slug: str,
    session: AsyncSession = Depends(get_session),
) -> Card:
    stmt = select(CardModel).where(CardModel.slug == slug)
    result = await session.execute(stmt)
    row = result.scalars().unique().one_or_none()
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with slug {slug!r}.",
            message_th=f"ไม่พบบัตรรหัส {slug}",
            details={"slug": slug},
        )
    return _to_schema(row)


__all__ = ["router"]
