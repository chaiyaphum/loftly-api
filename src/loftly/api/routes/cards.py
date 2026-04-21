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
from loftly.schemas.cards import BankMini, Card, CardList, Currency, SignupBonus
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
    from loftly.db.models.loyalty_currency import LoyaltyCurrency

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
