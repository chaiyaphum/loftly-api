"""Admin (CMS) endpoints — Week 3 scope: `/v1/admin/cards` CRUD with audit logging.

All admin routes sit behind `get_current_admin_id` (requires JWT with
`role=admin`). Writes append an `audit_log` row in the same transaction as
the business write so we never have "change without actor".

CardUpsert is declared `additionalProperties: true` in openapi.yaml, so we
accept an open dict and pull known fields; unknown top-level keys are ignored.
JSONB fields (`earn_rate_local`, `earn_rate_foreign`, `benefits`, `signup_bonus`)
are **merged** on PATCH — patching `earn_rate_local.dining` must not wipe
`earn_rate_local.online`.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.db.audit import log_action
from loftly.db.engine import get_session
from loftly.db.models.bank import Bank as BankModel
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.schemas.cards import BankMini, Card, CardList, Currency, SignupBonus
from loftly.schemas.common import Pagination

router = APIRouter(prefix="/v1/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Shared projection (mirrors the one in routes/cards.py but kept local to
# keep admin + public coupling minimal — same shape, no status filter).
# ---------------------------------------------------------------------------


def _to_schema(row: CardModel) -> Card:
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


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `patch` into a copy of `base`. `None` in `patch` deletes."""
    out = dict(base)
    for k, v in patch.items():
        if v is None:
            out.pop(k, None)
            continue
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


async def _resolve_bank(session: AsyncSession, bank_field: Any) -> uuid.UUID:
    """Accept either a bank UUID or slug in the payload. Raise 422 on miss."""
    if isinstance(bank_field, str):
        # UUID-shaped?
        try:
            candidate = uuid.UUID(bank_field)
            stmt = select(BankModel.id).where(BankModel.id == candidate)
            result = await session.execute(stmt)
            got = result.scalar_one_or_none()
            if got:
                return candidate
        except ValueError:
            pass
        # Fall through: treat as slug.
        stmt_slug = select(BankModel.id).where(BankModel.slug == bank_field)
        result_slug = await session.execute(stmt_slug)
        got_slug = result_slug.scalar_one_or_none()
        if got_slug:
            return uuid.UUID(str(got_slug))
    raise LoftlyError(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="unknown_bank",
        message_en=f"bank_id {bank_field!r} does not exist.",
        message_th="ไม่พบธนาคารที่อ้างอิง",
        details={"bank_id": str(bank_field) if bank_field is not None else None},
    )


async def _resolve_currency(session: AsyncSession, cur_field: Any) -> uuid.UUID:
    if isinstance(cur_field, str):
        try:
            candidate = uuid.UUID(cur_field)
            stmt = select(LoyaltyCurrency.id).where(LoyaltyCurrency.id == candidate)
            got = (await session.execute(stmt)).scalar_one_or_none()
            if got:
                return candidate
        except ValueError:
            pass
        stmt_code = select(LoyaltyCurrency.id).where(LoyaltyCurrency.code == cur_field)
        got_code = (await session.execute(stmt_code)).scalar_one_or_none()
        if got_code:
            return uuid.UUID(str(got_code))
    raise LoftlyError(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="unknown_currency",
        message_en=f"earn_currency_id {cur_field!r} does not exist.",
        message_th="ไม่พบสกุลคะแนนที่อ้างอิง",
        details={"earn_currency_id": str(cur_field) if cur_field is not None else None},
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "/cards",
    response_model=CardList,
    summary="List cards (all states)",
)
async def list_cards(
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> CardList:
    """Return every card including `inactive` + `archived`. No pagination for MVP."""
    stmt = select(CardModel).order_by(CardModel.created_at.asc())
    rows = list((await session.execute(stmt)).scalars().unique().all())
    return CardList(
        data=[_to_schema(r) for r in rows],
        pagination=Pagination(cursor_next=None, has_more=False, total_estimate=len(rows)),
    )


@router.post(
    "/cards",
    response_model=Card,
    status_code=status.HTTP_201_CREATED,
    summary="Create card",
)
async def create_card(
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> Card:
    """Create a new card. `bank_id` + `earn_currency_id` required and must exist."""
    required = ["slug", "display_name", "network", "bank_id", "earn_currency_id"]
    missing = [k for k in required if payload.get(k) in (None, "")]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="missing_fields",
            message_en=f"Missing required fields: {', '.join(missing)}.",
            message_th="กรุณากรอกข้อมูลให้ครบถ้วน",
            details={"missing": missing},
        )

    bank_id = await _resolve_bank(session, payload["bank_id"])
    currency_id = await _resolve_currency(session, payload["earn_currency_id"])

    card = CardModel(
        slug=payload["slug"],
        display_name=payload["display_name"],
        network=payload["network"],
        bank_id=bank_id,
        earn_currency_id=currency_id,
        tier=payload.get("tier"),
        annual_fee_thb=(
            Decimal(str(payload["annual_fee_thb"]))
            if payload.get("annual_fee_thb") is not None
            else None
        ),
        annual_fee_waiver=payload.get("annual_fee_waiver"),
        min_income_thb=(
            Decimal(str(payload["min_income_thb"]))
            if payload.get("min_income_thb") is not None
            else None
        ),
        min_age=payload.get("min_age"),
        earn_rate_local=payload.get("earn_rate_local") or {},
        earn_rate_foreign=payload.get("earn_rate_foreign"),
        benefits=payload.get("benefits") or {},
        signup_bonus=payload.get("signup_bonus"),
        description_th=payload.get("description_th"),
        description_en=payload.get("description_en"),
        status=payload.get("status", "active"),
    )
    session.add(card)
    await session.flush()

    await log_action(
        session,
        actor_id=admin_id,
        action="card.created",
        subject_type="card",
        subject_id=card.id,
        metadata={"slug": card.slug, "display_name": card.display_name},
    )
    await session.commit()
    # Reload with joined bank + currency.
    fresh = (
        (await session.execute(select(CardModel).where(CardModel.id == card.id)))
        .scalars()
        .unique()
        .one()
    )
    return _to_schema(fresh)


@router.patch(
    "/cards/{card_id}",
    response_model=Card,
    summary="Update card (partial, JSONB-aware merge)",
)
async def update_card(
    card_id: uuid.UUID,
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> Card:
    """Partial update. JSONB columns merge, scalar columns replace."""
    row = (
        (await session.execute(select(CardModel).where(CardModel.id == card_id)))
        .scalars()
        .unique()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with id {card_id}.",
            message_th="ไม่พบบัตรตามรหัสที่ระบุ",
            details={"id": str(card_id)},
        )

    changed: list[str] = []

    scalar_fields = (
        "slug",
        "display_name",
        "tier",
        "network",
        "annual_fee_waiver",
        "min_age",
        "description_th",
        "description_en",
        "status",
    )
    for field in scalar_fields:
        if field in payload:
            setattr(row, field, payload[field])
            changed.append(field)

    if "annual_fee_thb" in payload:
        row.annual_fee_thb = (
            Decimal(str(payload["annual_fee_thb"]))
            if payload["annual_fee_thb"] is not None
            else None
        )
        changed.append("annual_fee_thb")
    if "min_income_thb" in payload:
        row.min_income_thb = (
            Decimal(str(payload["min_income_thb"]))
            if payload["min_income_thb"] is not None
            else None
        )
        changed.append("min_income_thb")

    if "bank_id" in payload:
        row.bank_id = await _resolve_bank(session, payload["bank_id"])
        changed.append("bank_id")
    if "earn_currency_id" in payload:
        row.earn_currency_id = await _resolve_currency(session, payload["earn_currency_id"])
        changed.append("earn_currency_id")

    # JSONB fields — merge rather than replace so partial patches don't wipe siblings.
    for json_field in ("earn_rate_local", "earn_rate_foreign", "benefits", "signup_bonus"):
        if json_field in payload:
            patch_value = payload[json_field]
            if patch_value is None:
                setattr(row, json_field, None)
            else:
                current = getattr(row, json_field) or {}
                if not isinstance(patch_value, dict):
                    raise LoftlyError(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        code="invalid_json_patch",
                        message_en=f"`{json_field}` must be an object.",
                        message_th=f"`{json_field}` ต้องเป็นอ็อบเจกต์",
                    )
                setattr(row, json_field, _deep_merge(current, patch_value))
            changed.append(json_field)

    await session.flush()
    await log_action(
        session,
        actor_id=admin_id,
        action="card.updated",
        subject_type="card",
        subject_id=row.id,
        metadata={"changed": changed},
    )
    await session.commit()

    fresh = (
        (await session.execute(select(CardModel).where(CardModel.id == row.id)))
        .scalars()
        .unique()
        .one()
    )
    return _to_schema(fresh)


# ---------------------------------------------------------------------------
# Remaining admin stubs — unchanged from Week 2 except they now auth-gate.
# ---------------------------------------------------------------------------


@router.get("/articles", summary="List articles (all states)")
async def list_articles(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/articles", summary="Create article", status_code=status.HTTP_201_CREATED)
async def create_article(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.patch("/articles/{article_id}", summary="Update article (state transitions)")
async def update_article(
    article_id: str,
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
) -> None:
    _ = article_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/promos", summary="List promos")
async def list_promos(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/promos", summary="Create manual promo", status_code=status.HTTP_201_CREATED)
async def create_promo(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/mapping-queue", summary="Unresolved promo → card mappings")
async def mapping_queue(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/mapping-queue/{promo_id}/assign", summary="Bind promo to card(s)")
async def assign_mapping(
    promo_id: str,
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
) -> None:
    _ = promo_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/affiliate/export.csv", summary="CSV dump of last 30d clicks + conversions")
async def affiliate_export(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
