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

import re
import unicodedata
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.db.audit import log_action
from loftly.db.engine import get_session
from loftly.db.models.article import Article
from loftly.db.models.bank import Bank as BankModel
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.promo import Promo, promo_card_map
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


# ---------------------------------------------------------------------------
# Articles
# ---------------------------------------------------------------------------


DEFAULT_POLICY_VERSION = "2026-04-01"
_ARTICLE_STATES = ("draft", "review", "published", "archived")
_ARTICLE_TYPES = ("card_review", "guide", "news", "comparison")


def _article_to_dict(row: Article) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "slug": row.slug,
        "card_id": str(row.card_id) if row.card_id else None,
        "article_type": row.article_type,
        "title_th": row.title_th,
        "title_en": row.title_en,
        "summary_th": row.summary_th,
        "summary_en": row.summary_en,
        "body_th": row.body_th,
        "body_en": row.body_en,
        "best_for_tags": list(row.best_for_tags or []),
        "state": row.state,
        "policy_version": row.policy_version,
        "published_at": row.published_at.isoformat() if row.published_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "seo_meta": dict(row.seo_meta or {}),
    }


@router.get("/articles", summary="List articles (all states)")
async def list_articles(
    state: str | None = Query(default=None),
    card_id: uuid.UUID | None = Query(default=None),
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    stmt = select(Article).order_by(Article.updated_at.desc())
    if state:
        stmt = stmt.where(Article.state == state)
    if card_id:
        stmt = stmt.where(Article.card_id == card_id)
    rows = list((await session.execute(stmt)).scalars().all())
    return {
        "data": [_article_to_dict(r) for r in rows],
        "pagination": {"cursor_next": None, "has_more": False, "total_estimate": len(rows)},
    }


@router.post("/articles", summary="Create article", status_code=status.HTTP_201_CREATED)
async def create_article(
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    required = ["slug", "article_type", "title_th", "summary_th", "body_th"]
    missing = [k for k in required if not payload.get(k)]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="missing_fields",
            message_en=f"Missing required fields: {', '.join(missing)}.",
            message_th="กรุณากรอกข้อมูลให้ครบถ้วน",
            details={"missing": missing},
        )
    article_type = payload["article_type"]
    if article_type not in _ARTICLE_TYPES:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="invalid_article_type",
            message_en=f"article_type must be one of {_ARTICLE_TYPES}.",
            message_th="ประเภทบทความไม่ถูกต้อง",
        )

    slug = payload["slug"]
    clash = (
        (await session.execute(select(Article.id).where(Article.slug == slug)))
        .scalars()
        .one_or_none()
    )
    if clash is not None:
        raise LoftlyError(
            status_code=status.HTTP_409_CONFLICT,
            code="slug_conflict",
            message_en=f"Slug '{slug}' is already in use.",
            message_th="slug นี้ถูกใช้งานแล้ว",
        )

    state = payload.get("state", "draft")
    if state not in _ARTICLE_STATES:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="invalid_state",
            message_en=f"state must be one of {_ARTICLE_STATES}.",
            message_th="สถานะบทความไม่ถูกต้อง",
        )

    row = Article(
        slug=slug,
        card_id=_optional_uuid(payload.get("card_id")),
        article_type=article_type,
        title_th=payload["title_th"],
        title_en=payload.get("title_en"),
        summary_th=payload["summary_th"],
        summary_en=payload.get("summary_en"),
        body_th=payload["body_th"],
        body_en=payload.get("body_en"),
        best_for_tags=list(payload.get("best_for_tags") or []),
        state=state,
        author_id=admin_id,
        policy_version=payload.get("policy_version", DEFAULT_POLICY_VERSION),
        seo_meta=dict(payload.get("seo_meta") or {}),
    )
    if state == "published":
        row.published_at = datetime.now(UTC)
    session.add(row)
    await session.flush()
    await log_action(
        session,
        actor_id=admin_id,
        action="article.created",
        subject_type="article",
        subject_id=row.id,
        metadata={"slug": slug, "state": state},
    )
    await session.commit()
    fresh = (await session.execute(select(Article).where(Article.id == row.id))).scalars().one()
    return _article_to_dict(fresh)


@router.patch("/articles/{article_id}", summary="Update article (state transitions)")
async def update_article(
    article_id: uuid.UUID,
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    row = (
        (await session.execute(select(Article).where(Article.id == article_id)))
        .scalars()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="article_not_found",
            message_en=f"No article with id {article_id}.",
            message_th="ไม่พบบทความ",
        )

    changed: list[str] = []
    transition: tuple[str, str] | None = None

    scalar_fields = (
        "slug",
        "title_th",
        "title_en",
        "summary_th",
        "summary_en",
        "body_th",
        "body_en",
        "policy_version",
    )
    for field in scalar_fields:
        if field in payload:
            setattr(row, field, payload[field])
            changed.append(field)

    if "card_id" in payload:
        row.card_id = _optional_uuid(payload["card_id"])
        changed.append("card_id")
    if "article_type" in payload:
        if payload["article_type"] not in _ARTICLE_TYPES:
            raise LoftlyError(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                code="invalid_article_type",
                message_en=f"article_type must be one of {_ARTICLE_TYPES}.",
                message_th="ประเภทบทความไม่ถูกต้อง",
            )
        row.article_type = payload["article_type"]
        changed.append("article_type")
    if "best_for_tags" in payload:
        row.best_for_tags = list(payload["best_for_tags"] or [])
        changed.append("best_for_tags")
    if "seo_meta" in payload:
        row.seo_meta = dict(payload["seo_meta"] or {})
        changed.append("seo_meta")

    if "state" in payload:
        new_state = payload["state"]
        if new_state not in _ARTICLE_STATES:
            raise LoftlyError(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                code="invalid_state",
                message_en=f"state must be one of {_ARTICLE_STATES}.",
                message_th="สถานะไม่ถูกต้อง",
            )
        if new_state != row.state:
            transition = (row.state, new_state)
        if new_state == "published":
            if not row.policy_version:
                raise LoftlyError(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    code="missing_policy_version",
                    message_en="policy_version must be set before publishing.",
                    message_th="ต้องระบุ policy_version ก่อนเผยแพร่",
                )
            if row.published_at is None:
                row.published_at = datetime.now(UTC)
        row.state = new_state
        changed.append("state")

    # Slug collision check if slug was touched.
    if "slug" in payload:
        clash = (
            (
                await session.execute(
                    select(Article.id).where(
                        Article.slug == payload["slug"],
                        Article.id != row.id,
                    )
                )
            )
            .scalars()
            .one_or_none()
        )
        if clash is not None:
            raise LoftlyError(
                status_code=status.HTTP_409_CONFLICT,
                code="slug_conflict",
                message_en=f"Slug '{payload['slug']}' is already in use.",
                message_th="slug นี้ถูกใช้งานแล้ว",
            )

    row.updated_at = datetime.now(UTC)
    await session.flush()

    await log_action(
        session,
        actor_id=admin_id,
        action="article.updated",
        subject_type="article",
        subject_id=row.id,
        metadata={"changed": changed},
    )
    if transition is not None:
        await log_action(
            session,
            actor_id=admin_id,
            action=f"article.state.{transition[1]}",
            subject_type="article",
            subject_id=row.id,
            metadata={"from": transition[0], "to": transition[1]},
        )
    await session.commit()
    fresh = (await session.execute(select(Article).where(Article.id == row.id))).scalars().one()
    return _article_to_dict(fresh)


def _optional_uuid(value: Any) -> uuid.UUID | None:
    if value in (None, ""):
        return None
    return uuid.UUID(str(value))


# ---------------------------------------------------------------------------
# Promos
# ---------------------------------------------------------------------------


def _promo_to_dict(row: Promo, *, card_ids: list[uuid.UUID] | None = None) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "bank_slug": row.bank.slug if getattr(row, "bank", None) else None,
        "external_source_id": row.external_source_id,
        "source_url": row.source_url,
        "promo_type": row.promo_type,
        "title_th": row.title_th,
        "title_en": row.title_en,
        "description_th": row.description_th,
        "merchant_name": row.merchant_name,
        "category": row.category,
        "discount_type": row.discount_type,
        "discount_value": row.discount_value,
        "discount_amount": float(row.discount_amount) if row.discount_amount is not None else None,
        "discount_unit": row.discount_unit,
        "minimum_spend": float(row.minimum_spend) if row.minimum_spend is not None else None,
        "valid_from": row.valid_from.isoformat() if row.valid_from else None,
        "valid_until": row.valid_until.isoformat() if row.valid_until else None,
        "card_ids": [str(c) for c in (card_ids or [])],
        "active": bool(row.active),
    }


@router.get("/promos", summary="List promos")
async def list_promos(
    bank_id: uuid.UUID | None = Query(default=None),
    active: bool | None = Query(default=None),
    manual_only: bool = Query(default=False),
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    stmt = select(Promo).options(joinedload(Promo.bank)).order_by(Promo.last_synced_at.desc())
    if bank_id is not None:
        stmt = stmt.where(Promo.bank_id == bank_id)
    if active is not None:
        stmt = stmt.where(Promo.active == active)
    if manual_only:
        stmt = stmt.where(Promo.external_source_id.is_(None))
    rows = list((await session.execute(stmt)).scalars().unique().all())

    data: list[dict[str, Any]] = []
    for row in rows:
        card_id_rows = list(
            (
                await session.execute(
                    select(promo_card_map.c.card_id).where(promo_card_map.c.promo_id == row.id)
                )
            )
            .scalars()
            .all()
        )
        data.append(_promo_to_dict(row, card_ids=[uuid.UUID(str(c)) for c in card_id_rows]))
    return {
        "data": data,
        "pagination": {"cursor_next": None, "has_more": False, "total_estimate": len(data)},
    }


@router.post("/promos", summary="Create manual promo", status_code=status.HTTP_201_CREATED)
async def create_promo(
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    required = ["bank_id", "source_url", "promo_type", "title_th"]
    missing = [k for k in required if not payload.get(k)]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="missing_fields",
            message_en=f"Missing required fields: {', '.join(missing)}.",
            message_th="กรุณากรอกข้อมูลให้ครบถ้วน",
            details={"missing": missing},
        )
    bank_id = await _resolve_bank(session, payload["bank_id"])
    row = Promo(
        bank_id=bank_id,
        external_source_id=None,
        external_bank_key=None,
        external_checksum=None,
        source_url=payload["source_url"],
        promo_type=payload["promo_type"],
        title_th=payload["title_th"],
        title_en=payload.get("title_en"),
        description_th=payload.get("description_th"),
        description_en=payload.get("description_en"),
        merchant_name=payload.get("merchant_name"),
        category=payload.get("category"),
        discount_type=payload.get("discount_type"),
        discount_value=payload.get("discount_value"),
        discount_amount=(
            Decimal(str(payload["discount_amount"]))
            if payload.get("discount_amount") is not None
            else None
        ),
        discount_unit=payload.get("discount_unit"),
        minimum_spend=(
            Decimal(str(payload["minimum_spend"]))
            if payload.get("minimum_spend") is not None
            else None
        ),
        valid_from=_parse_date(payload.get("valid_from")),
        valid_until=_parse_date(payload.get("valid_until")),
        terms_and_conditions=payload.get("terms_and_conditions"),
        raw_data=dict(payload.get("raw_data") or {}),
        relevance_tags=list(payload.get("relevance_tags") or []),
        active=bool(payload.get("active", True)),
    )
    session.add(row)
    await session.flush()
    await log_action(
        session,
        actor_id=admin_id,
        action="promo.created",
        subject_type="promo",
        subject_id=row.id,
        metadata={"title_th": row.title_th, "manual": True},
    )
    await session.commit()
    fresh = (
        (
            await session.execute(
                select(Promo).options(joinedload(Promo.bank)).where(Promo.id == row.id)
            )
        )
        .scalars()
        .unique()
        .one()
    )
    return _promo_to_dict(fresh, card_ids=[])


@router.patch("/promos/{promo_id}", summary="Update promo")
async def update_promo(
    promo_id: uuid.UUID,
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                select(Promo).options(joinedload(Promo.bank)).where(Promo.id == promo_id)
            )
        )
        .scalars()
        .unique()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="promo_not_found",
            message_en=f"No promo with id {promo_id}.",
            message_th="ไม่พบโปรโมชั่น",
        )

    changed: list[str] = []
    scalar_fields = (
        "source_url",
        "promo_type",
        "title_th",
        "title_en",
        "description_th",
        "description_en",
        "merchant_name",
        "category",
        "discount_type",
        "discount_value",
        "discount_unit",
        "terms_and_conditions",
    )
    for f in scalar_fields:
        if f in payload:
            setattr(row, f, payload[f])
            changed.append(f)

    if "bank_id" in payload:
        row.bank_id = await _resolve_bank(session, payload["bank_id"])
        changed.append("bank_id")
    if "discount_amount" in payload:
        row.discount_amount = (
            Decimal(str(payload["discount_amount"]))
            if payload["discount_amount"] is not None
            else None
        )
        changed.append("discount_amount")
    if "minimum_spend" in payload:
        row.minimum_spend = (
            Decimal(str(payload["minimum_spend"])) if payload["minimum_spend"] is not None else None
        )
        changed.append("minimum_spend")
    if "valid_from" in payload:
        row.valid_from = _parse_date(payload["valid_from"])
        changed.append("valid_from")
    if "valid_until" in payload:
        row.valid_until = _parse_date(payload["valid_until"])
        changed.append("valid_until")
    if "active" in payload:
        row.active = bool(payload["active"])
        changed.append("active")
    if "relevance_tags" in payload:
        row.relevance_tags = list(payload["relevance_tags"] or [])
        changed.append("relevance_tags")
    if "raw_data" in payload:
        row.raw_data = dict(payload["raw_data"] or {})
        changed.append("raw_data")

    await session.flush()
    await log_action(
        session,
        actor_id=admin_id,
        action="promo.updated",
        subject_type="promo",
        subject_id=row.id,
        metadata={"changed": changed},
    )
    await session.commit()
    fresh = (
        (
            await session.execute(
                select(Promo).options(joinedload(Promo.bank)).where(Promo.id == row.id)
            )
        )
        .scalars()
        .unique()
        .one()
    )
    card_id_rows = list(
        (
            await session.execute(
                select(promo_card_map.c.card_id).where(promo_card_map.c.promo_id == row.id)
            )
        )
        .scalars()
        .all()
    )
    return _promo_to_dict(fresh, card_ids=[uuid.UUID(str(c)) for c in card_id_rows])


def _parse_date(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        # fromisoformat handles 'YYYY-MM-DD' and timestamps
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            from datetime import date as _date

            return _date.fromisoformat(value)
    return value


# ---------------------------------------------------------------------------
# Mapping queue
# ---------------------------------------------------------------------------


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_only.lower()).strip("-")


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


_TIER_TOKENS = (
    "platinum",
    "signature",
    "infinite",
    "visa",
    "mastercard",
    "amex",
    "jcb",
    "titanium",
    "gold",
    "world",
    "elite",
    "prvi",
    "miles",
)


def _tier_tokens(name: str) -> set[str]:
    lower = name.lower()
    return {t for t in _TIER_TOKENS if t in lower}


def _auto_match(
    card_types: list[str],
    cards: list[CardModel],
    bank_id: uuid.UUID | None,
) -> tuple[list[uuid.UUID], list[tuple[uuid.UUID, str]], bool]:
    """Return (confident_ids, fuzzy_suggestions, low_confidence_flag)."""
    name_index: dict[str, CardModel] = {c.display_name.lower(): c for c in cards}
    slug_index: dict[str, CardModel] = {_slugify(c.display_name): c for c in cards}

    confident: list[uuid.UUID] = []
    fuzzy: list[tuple[uuid.UUID, str]] = []
    for raw in card_types:
        if not isinstance(raw, str):
            continue
        lowered = raw.lower().strip()
        hit = name_index.get(lowered)
        if hit is not None:
            confident.append(hit.id)
            continue
        slug_hit = slug_index.get(_slugify(raw))
        if slug_hit is not None:
            confident.append(slug_hit.id)
            continue

        # Fuzzy step — same bank + token overlap + Levenshtein ≤ 3.
        raw_tokens = _tier_tokens(raw)
        best: tuple[int, CardModel] | None = None
        for c in cards:
            if bank_id is not None and c.bank_id != bank_id:
                continue
            cand_tokens = _tier_tokens(c.display_name)
            if raw_tokens and not (raw_tokens & cand_tokens):
                continue
            distance = _levenshtein(_slugify(raw), _slugify(c.display_name))
            if distance <= 3 and (best is None or distance < best[0]):
                best = (distance, c)
        if best is not None:
            fuzzy.append((best[1].id, raw))

    # Dedup preserving order
    seen: set[uuid.UUID] = set()
    unique_confident: list[uuid.UUID] = []
    for cid in confident:
        if cid not in seen:
            seen.add(cid)
            unique_confident.append(cid)
    return unique_confident, fuzzy, bool(fuzzy)


@router.get("/mapping-queue", summary="Unresolved promo → card mappings")
async def mapping_queue(
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Return promos synced from upstream that still lack a card mapping.

    Manual promos (external_source_id NULL) are excluded — admins map those
    directly via the promo-edit form when they create them.
    """
    # Promos without any row in promo_card_map.
    mapped_ids_stmt = select(promo_card_map.c.promo_id).distinct()
    stmt = (
        select(Promo)
        .options(joinedload(Promo.bank))
        .where(
            Promo.external_source_id.is_not(None),
            Promo.id.not_in(mapped_ids_stmt),
        )
        .order_by(Promo.last_synced_at.desc())
    )
    rows = list((await session.execute(stmt)).scalars().unique().all())
    if not rows:
        return {"data": [], "total": 0}

    cards = list((await session.execute(select(CardModel))).scalars().unique().all())
    data: list[dict[str, Any]] = []
    for promo in rows:
        card_types_raw = list((promo.raw_data or {}).get("card_types") or [])
        confident, fuzzy, low_confidence = _auto_match(card_types_raw, cards, bank_id=promo.bank_id)
        data.append(
            {
                "promo_id": str(promo.id),
                "title_th": promo.title_th,
                "bank_slug": promo.bank.slug if promo.bank else None,
                "card_types_raw": card_types_raw,
                "suggested_card_ids": [str(cid) for cid in confident],
                "fuzzy_suggestions": [
                    {"card_id": str(cid), "card_type_raw": src} for cid, src in fuzzy
                ],
                "low_confidence": low_confidence and not confident,
            }
        )
    return {"data": data, "total": len(data)}


@router.post(
    "/mapping-queue/{promo_id}/assign",
    summary="Bind promo to card(s)",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def assign_mapping(
    promo_id: uuid.UUID,
    payload: dict[str, Any] = Body(...),
    admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> Response:
    card_ids_raw = payload.get("card_ids") or []
    if not isinstance(card_ids_raw, list) or not card_ids_raw:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="missing_card_ids",
            message_en="Provide at least one card_id.",
            message_th="ต้องระบุ card_ids อย่างน้อยหนึ่งรายการ",
        )
    try:
        card_ids = [uuid.UUID(str(c)) for c in card_ids_raw]
    except ValueError as exc:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="invalid_card_id",
            message_en="card_ids must be UUIDs.",
            message_th="รหัสบัตรต้องเป็น UUID",
        ) from exc

    promo = (
        (
            await session.execute(
                select(Promo).options(joinedload(Promo.bank)).where(Promo.id == promo_id)
            )
        )
        .scalars()
        .unique()
        .one_or_none()
    )
    if promo is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="promo_not_found",
            message_en=f"No promo with id {promo_id}.",
            message_th="ไม่พบโปรโมชั่น",
        )
    # Validate target cards exist.
    existing_cards = set(
        (await session.execute(select(CardModel.id).where(CardModel.id.in_(card_ids))))
        .scalars()
        .all()
    )
    missing = [str(c) for c in card_ids if uuid.UUID(str(c)) not in existing_cards]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="unknown_cards",
            message_en="Some card_ids do not exist.",
            message_th="ไม่พบบัตรบางใบ",
            details={"missing": missing},
        )

    # Idempotent upsert into promo_card_map.
    already_mapped = set(
        (
            await session.execute(
                select(promo_card_map.c.card_id).where(promo_card_map.c.promo_id == promo_id)
            )
        )
        .scalars()
        .all()
    )
    new_links = [cid for cid in card_ids if cid not in already_mapped]
    for cid in new_links:
        await session.execute(promo_card_map.insert().values(promo_id=promo_id, card_id=cid))

    await log_action(
        session,
        actor_id=admin_id,
        action="promo.mapped",
        subject_type="promo",
        subject_id=promo_id,
        metadata={"promo_id": str(promo_id), "card_ids": [str(c) for c in card_ids]},
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/affiliate/stats",
    summary="30-day affiliate funnel",
)
async def affiliate_stats(
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Aggregate the last 30d of clicks + conversions into `AffiliateStats`.

    Structure matches `openapi.yaml#AffiliateStats`. Commission buckets derive
    from `affiliate_conversions.status`:
      - pending   -> commission_pending_thb
      - confirmed -> commission_confirmed_thb
      - paid      -> commission_paid_thb
    """
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import func

    from loftly.db.models.affiliate import AffiliateClick, AffiliateConversion

    period_days = 30
    cutoff = datetime.now(UTC) - timedelta(days=period_days)

    clicks_total = (
        await session.execute(
            select(func.count(AffiliateClick.click_id)).where(AffiliateClick.created_at >= cutoff)
        )
    ).scalar_one()

    conversions_total = (
        await session.execute(
            select(func.count(AffiliateConversion.id)).where(
                AffiliateConversion.received_at >= cutoff
            )
        )
    ).scalar_one()

    async def _commission_sum(status_: str) -> float:
        value = (
            await session.execute(
                select(func.coalesce(func.sum(AffiliateConversion.commission_thb), 0)).where(
                    AffiliateConversion.received_at >= cutoff,
                    AffiliateConversion.status == status_,
                )
            )
        ).scalar_one()
        return float(value or 0)

    commission_pending = await _commission_sum("pending")
    commission_confirmed = await _commission_sum("confirmed")
    commission_paid = await _commission_sum("paid")

    conv_rate = float(conversions_total) / float(clicks_total) if clicks_total else 0.0

    # Top-10 by card (slug) — join affiliate_clicks to cards to surface slug.
    by_card_rows = (
        await session.execute(
            select(
                CardModel.slug,
                func.count(AffiliateClick.click_id).label("clicks"),
            )
            .join(CardModel, AffiliateClick.card_id == CardModel.id)
            .where(AffiliateClick.created_at >= cutoff)
            .group_by(CardModel.slug)
            .order_by(func.count(AffiliateClick.click_id).desc())
            .limit(10)
        )
    ).all()

    by_card: list[dict[str, Any]] = []
    for slug, clicks in by_card_rows:
        # Conversions + commission per card.
        conv_count = (
            await session.execute(
                select(func.count(AffiliateConversion.id))
                .join(
                    AffiliateClick,
                    AffiliateConversion.click_id == AffiliateClick.click_id,
                )
                .join(CardModel, AffiliateClick.card_id == CardModel.id)
                .where(
                    CardModel.slug == slug,
                    AffiliateConversion.received_at >= cutoff,
                )
            )
        ).scalar_one()
        commission_sum = (
            await session.execute(
                select(func.coalesce(func.sum(AffiliateConversion.commission_thb), 0))
                .join(
                    AffiliateClick,
                    AffiliateConversion.click_id == AffiliateClick.click_id,
                )
                .join(CardModel, AffiliateClick.card_id == CardModel.id)
                .where(
                    CardModel.slug == slug,
                    AffiliateConversion.received_at >= cutoff,
                )
            )
        ).scalar_one()
        by_card.append(
            {
                "card_slug": slug,
                "clicks": int(clicks),
                "conversions": int(conv_count),
                "commission_thb": float(commission_sum or 0),
            }
        )

    return {
        "period_days": period_days,
        "clicks": int(clicks_total),
        "conversions": int(conversions_total),
        "conversion_rate": conv_rate,
        "commission_pending_thb": commission_pending,
        "commission_confirmed_thb": commission_confirmed,
        "commission_paid_thb": commission_paid,
        "by_card": by_card,
    }


@router.get("/affiliate/export.csv", summary="CSV dump of last 30d clicks + conversions")
async def affiliate_export(_admin_id: uuid.UUID = Depends(get_current_admin_id)) -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
