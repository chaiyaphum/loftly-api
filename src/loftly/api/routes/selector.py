"""Card Selector — `POST /v1/selector`, `GET /v1/selector/{session_id}`.

Phase 1 wiring (Week 5-6):
- Validates the spend profile (category sum must equal monthly_spend_thb ±100)
- Hashes the profile into a stable cache key; 24h Redis/in-memory cache
- Dispatches to the configured `LLMProvider` (deterministic by default)
- Persists every call as a `selector_sessions` row
- Anonymous callers receive `partial_unlock=true` to drive the email-gate UI

SSE streaming is **deferred to Week 7** per DEV_PLAN.md — today we return the
full JSON envelope synchronously, which still matches openapi.yaml's
`application/json` content type.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, status
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from loftly.ai import SelectorContext, get_provider
from loftly.api.errors import LoftlyError
from loftly.core.cache import get_cache
from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.selector_session import SelectorSession
from loftly.schemas.selector import SelectorInput, SelectorResult

router = APIRouter(prefix="/v1/selector", tags=["selector"])
log = get_logger(__name__)

# 24h cache per SPEC.md §2 AC ("cached against their profile hash for 24h").
_CACHE_TTL_SECONDS = 86_400
# JWT-subject purpose for selector retrieval links.
_SELECTOR_LINK_PURPOSE = "selector_retrieve"
# `selector_invalid_categories` tolerance — SPEC.md §2 + API_CONTRACT.md.
_CATEGORY_SUM_TOLERANCE = 100


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_category_sum(payload: SelectorInput) -> None:
    total = sum(payload.spend_categories.values())
    diff = total - payload.monthly_spend_thb
    if abs(diff) > _CATEGORY_SUM_TOLERANCE:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="selector_invalid_categories",
            message_en=(
                f"Category total must equal monthly_spend_thb ± THB {_CATEGORY_SUM_TOLERANCE}."
            ),
            message_th=(f"ผลรวมหมวดต้องเท่ากับยอดรวม ±THB {_CATEGORY_SUM_TOLERANCE}"),
            details={"diff_thb": diff},
        )


def _profile_hash(payload: SelectorInput) -> str:
    """SHA-256 of the canonicalized input — stable across key-ordering noise."""
    canonical = json.dumps(
        payload.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Context loader — pull active cards + latest valuations.
# ---------------------------------------------------------------------------


async def _load_context(session: AsyncSession) -> SelectorContext:
    cards_stmt = (
        select(CardModel)
        .where(CardModel.status == "active")
        .options(
            selectinload(CardModel.bank),
            selectinload(CardModel.earn_currency),
        )
    )
    cards = list((await session.execute(cards_stmt)).scalars().unique().all())

    val_stmt = (
        select(PointValuation, LoyaltyCurrency.code)
        .join(
            LoyaltyCurrency,
            PointValuation.currency_id == LoyaltyCurrency.id,
        )
        .order_by(PointValuation.computed_at.desc())
    )
    rows = (await session.execute(val_stmt)).all()
    valuations_by_code: dict[str, PointValuation] = {}
    for row in rows:
        valuation, code = row[0], row[1]
        # Keep most-recent per currency.
        if code not in valuations_by_code:
            valuations_by_code[code] = valuation

    return SelectorContext(
        cards=cards,
        valuations_by_currency_code=valuations_by_code,
    )


# ---------------------------------------------------------------------------
# Session token helpers (for GET /v1/selector/{session_id}).
# ---------------------------------------------------------------------------


def issue_session_token(session_id: uuid.UUID, settings: Settings) -> str:
    """Short-lived (1h) signed token granting read access to one session."""
    now = int(datetime.now(UTC).timestamp())
    payload = {
        "sub": str(session_id),
        "purpose": _SELECTOR_LINK_PURPOSE,
        "iat": now,
        "exp": now + 3600,
    }
    return jwt.encode(
        payload,
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )


def _verify_session_token(token: str, settings: Settings) -> uuid.UUID:
    try:
        claims = jwt.decode(
            token,
            settings.jwt_signing_key,
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
            message_en="Session token is invalid or expired.",
            message_th="โทเคนหมดอายุหรือไม่ถูกต้อง",
        ) from exc
    if claims.get("purpose") != _SELECTOR_LINK_PURPOSE:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
            message_en="Token purpose mismatch.",
            message_th="โทเคนไม่ถูกต้อง",
        )
    try:
        return uuid.UUID(str(claims["sub"]))
    except (KeyError, ValueError) as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
            message_en="Token subject is malformed.",
            message_th="โทเคนไม่ถูกต้อง",
        ) from exc


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=SelectorResult,
    summary="Submit spend profile; receive ranked card stack",
)
async def submit(
    payload: SelectorInput,
    session: AsyncSession = Depends(get_session),
) -> SelectorResult:
    """Validate → cache-check → call provider → persist → return."""
    _validate_category_sum(payload)
    cache = get_cache()
    provider = get_provider()
    key = f"selector:{_profile_hash(payload)}"

    # Cache hit — serialize existing SelectorResult back out.
    cached = await cache.get(key)
    if cached is not None:
        log.info("selector_cache_hit", profile_hash=key)
        return SelectorResult.model_validate(cached)

    # Miss — build context and invoke provider.
    context = await _load_context(session)
    raw_result = await provider.card_selector(payload, context)

    # Persist selector_sessions row (user_id=None for anon; bound later).
    row = SelectorSession(
        user_id=None,
        profile_hash=_profile_hash(payload),
        input=payload.model_dump(mode="json"),
        output=raw_result.model_dump(mode="json"),
        provider=provider.name,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)

    # Stamp session_id + partial_unlock, then cache.
    result = raw_result.model_copy(update={"session_id": str(row.id), "partial_unlock": True})
    await cache.set(key, result.model_dump(mode="json"), _CACHE_TTL_SECONDS)
    log.info(
        "selector_computed",
        session_id=str(row.id),
        provider=provider.name,
        cards=len(result.stack),
    )
    return result


@router.get(
    "/{session_id}",
    response_model=SelectorResult,
    summary="Retrieve previously computed result",
)
async def get_session_result(
    session_id: uuid.UUID,
    token: str = Query(..., description="Signed selector retrieval token"),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> SelectorResult:
    """Verify token, look up the saved row, return the stored envelope."""
    token_sub = _verify_session_token(token, settings)
    if token_sub != session_id:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
            message_en="Token does not grant access to this session.",
            message_th="โทเคนไม่มีสิทธิ์เข้าถึง session นี้",
        )
    row = (
        (await session.execute(select(SelectorSession).where(SelectorSession.id == session_id)))
        .scalars()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="selector_session_not_found",
            message_en="Selector session not found.",
            message_th="ไม่พบ session",
            details={"session_id": str(session_id)},
        )
    # Output is stored with session_id already stamped.
    output_data: dict[str, Any] = dict(row.output)
    # Ensure session_id matches the row's id even if envelope drifts.
    output_data["session_id"] = str(row.id)
    return SelectorResult.model_validate(output_data)


__all__ = ["issue_session_token", "router"]
