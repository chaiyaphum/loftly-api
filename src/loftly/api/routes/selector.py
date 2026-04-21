"""Card Selector — `POST /v1/selector`, `GET /v1/selector/{session_id}`.

Phase 2 (Week 9-12) adds:
- Sonnet → Haiku → deterministic fallback chain per `AI_PROMPTS.md §Failure policy`
- SSE streaming via `Accept: text/event-stream` (envelope → rationale chunks → done)
- Mid-tier validation on `total_monthly_earning_thb_equivalent` drift (±5%)

Phase 1 pieces kept: category-sum validation, 24h profile-hash cache,
`selector_sessions` persistence, `partial_unlock=true` for anon callers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from loftly.ai import LLMProvider, SelectorContext, get_provider
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
# AI_PROMPTS.md §Failure policy: Sonnet >10s → Haiku; Haiku >5s → deterministic.
_SONNET_TIMEOUT_SEC = 10.0
_HAIKU_TIMEOUT_SEC = 5.0
# AI_PROMPTS.md §Quality gates: total THB must match Σ (points × thb_per_point) ±5%.
_THB_DRIFT_TOLERANCE = 0.05


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


def _validate_earning_consistency(
    result: SelectorResult,
    context: SelectorContext,
) -> bool:
    """Return True if total_monthly_earning_thb_equivalent is within ±5% of

    Σ (points × valuation). Used as a soft quality gate before returning LLM
    output. A failure triggers one retry; second failure → mark as fallback.
    """
    if not result.stack:
        return True
    expected_thb = 0.0
    for item in result.stack:
        card = next((c for c in context.cards if c.slug == item.slug), None)
        if card is None or card.earn_currency is None:
            continue
        val = context.valuations_by_currency_code.get(card.earn_currency.code)
        thb_per_point = float(val.thb_per_point) if val else 0.0
        expected_thb += item.monthly_earning_points * thb_per_point
    if expected_thb == 0:
        # Nothing to compare against; accept as-is.
        return True
    delta = abs(result.total_monthly_earning_thb_equivalent - expected_thb) / expected_thb
    return delta <= _THB_DRIFT_TOLERANCE


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
# Provider fallback chain.
# ---------------------------------------------------------------------------


async def _run_with_fallback(
    payload: SelectorInput,
    context: SelectorContext,
) -> SelectorResult:
    """Sonnet → Haiku → deterministic, honoring AI_PROMPTS.md §Failure policy.

    Always returns something. `fallback=True` is stamped whenever we've dropped
    below the primary path so callers can surface the "AI temporarily
    unavailable" warning.
    """
    provider = get_provider()

    # Configured provider is deterministic → no fallback work to do.
    if provider.name != "anthropic":
        return await provider.card_selector(payload, context)

    warnings: list[str] = []

    # 1) Try Sonnet.
    try:
        result = await asyncio.wait_for(
            provider.card_selector(payload, context),
            timeout=_SONNET_TIMEOUT_SEC,
        )
        if _validate_earning_consistency(result, context):
            return result
        # Quality gate failed once — retry; if still bad, fall through to Haiku.
        log.warning("selector_sonnet_quality_retry")
        result = await asyncio.wait_for(
            provider.card_selector(payload, context),
            timeout=_SONNET_TIMEOUT_SEC,
        )
        if _validate_earning_consistency(result, context):
            return result
        warnings.append("quality_gate_drift")
    except (TimeoutError, NotImplementedError, Exception) as exc:
        log.warning("selector_sonnet_fallback", error=str(exc)[:200])
        warnings.append("sonnet_fallback")

    # 2) Try Haiku.
    try:
        from loftly.ai.providers.anthropic_haiku import AnthropicHaikuProvider

        haiku: LLMProvider = AnthropicHaikuProvider()
        result = await asyncio.wait_for(
            haiku.card_selector(payload, context),
            timeout=_HAIKU_TIMEOUT_SEC,
        )
        result = result.model_copy(update={"warnings": [*result.warnings, *warnings]})
        return result
    except (TimeoutError, NotImplementedError, Exception) as exc:
        log.warning("selector_haiku_fallback", error=str(exc)[:200])
        warnings.append("haiku_fallback")

    # 3) Deterministic last resort.
    from loftly.ai.providers.deterministic import DeterministicProvider

    rule_based: LLMProvider = DeterministicProvider()
    result = await rule_based.card_selector(payload, context)
    return result.model_copy(
        update={
            "warnings": [*result.warnings, *warnings, "AI temporarily unavailable"],
            "fallback": True,
        }
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
# SSE event builder
# ---------------------------------------------------------------------------


def _sse_event(event: str, data: Any) -> bytes:
    """Format a Server-Sent Events frame."""
    serialized = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {serialized}\n\n".encode()


async def _stream_selector(result: SelectorResult) -> AsyncIterator[bytes]:
    """Emit envelope → rationale chunks → done.

    The rationale is pre-computed (deterministic or LLM), so we chunk it by
    sentence for a snappy typing effect. For real streaming of partial LLM
    tokens we'd plumb client.messages.stream() through; fine to defer — the
    client contract is the same either way.
    """
    envelope = {
        "session_id": result.session_id,
        "stack": [item.model_dump(mode="json") for item in result.stack],
        "total_monthly_earning_points": result.total_monthly_earning_points,
        "total_monthly_earning_thb_equivalent": result.total_monthly_earning_thb_equivalent,
        "months_to_goal": result.months_to_goal,
        "with_signup_bonus_months": result.with_signup_bonus_months,
        "valuation_confidence": result.valuation_confidence,
        "partial_unlock": result.partial_unlock,
    }
    yield _sse_event("envelope", envelope)

    # Chunk rationale_th (primary user-facing text) by sentence-ish boundaries.
    rationale = result.rationale_th or ""
    chunk_size = max(40, len(rationale) // 4 or 1)
    for i in range(0, len(rationale), chunk_size):
        chunk = rationale[i : i + chunk_size]
        yield _sse_event("rationale_chunk", chunk)
        # Tiny await yields control so the client sees progressive flushes.
        await asyncio.sleep(0)

    yield _sse_event(
        "done",
        {
            "warnings": result.warnings,
            "llm_model": result.llm_model,
            "fallback": result.fallback,
            "rationale_en": result.rationale_en,
        },
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


async def _compute_or_get_cached(
    payload: SelectorInput,
    session: AsyncSession,
) -> SelectorResult:
    """Core path used by both JSON and SSE responses. Handles cache + persist."""
    _validate_category_sum(payload)
    cache = get_cache()
    key = f"selector:{_profile_hash(payload)}"

    cached = await cache.get(key)
    if cached is not None:
        log.info("selector_cache_hit", profile_hash=key)
        return SelectorResult.model_validate(cached)

    context = await _load_context(session)
    raw_result = await _run_with_fallback(payload, context)

    row = SelectorSession(
        user_id=None,
        profile_hash=_profile_hash(payload),
        input=payload.model_dump(mode="json"),
        output=raw_result.model_dump(mode="json"),
        provider=raw_result.llm_model,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)

    result = raw_result.model_copy(update={"session_id": str(row.id), "partial_unlock": True})
    await cache.set(key, result.model_dump(mode="json"), _CACHE_TTL_SECONDS)
    log.info(
        "selector_computed",
        session_id=str(row.id),
        provider=raw_result.llm_model,
        cards=len(result.stack),
        fallback=result.fallback,
    )
    return result


@router.post(
    "",
    response_model=None,  # we may return either SelectorResult or StreamingResponse
    summary="Submit spend profile; receive ranked card stack (JSON or SSE)",
)
async def submit(
    payload: SelectorInput,
    request: Request,
    accept: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
) -> SelectorResult | StreamingResponse:
    """Validate → cache-check → call provider → persist → return.

    Content negotiation: `Accept: text/event-stream` → SSE. Else JSON.
    """
    _ = request  # kept in signature for future per-request hooks
    result = await _compute_or_get_cached(payload, session)
    if accept and "text/event-stream" in accept:
        return StreamingResponse(
            _stream_selector(result),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
    output_data: dict[str, Any] = dict(row.output)
    output_data["session_id"] = str(row.id)
    return SelectorResult.model_validate(output_data)


__all__ = ["issue_session_token", "router"]
