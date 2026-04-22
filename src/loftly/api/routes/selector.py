"""Card Selector — `POST /v1/selector`, `GET /v1/selector/{session_id}`.

Phase 2 (Week 9-12) adds:
- Sonnet → Haiku → deterministic fallback chain per `AI_PROMPTS.md §Failure policy`
- SSE streaming via `Accept: text/event-stream` (envelope → rationale chunks → done)
- Mid-tier validation on `total_monthly_earning_thb_equivalent` drift (±5%)

Phase 1 pieces kept: category-sum validation, 24h profile-hash cache,
`selector_sessions` persistence, `partial_unlock=true` for anon callers.

POST_V1 §3 (Tier A) adds:
- `_compute_or_get_cached` writes `selector:session:{id}:meta` on every result
  for returning-user landing + email composer (non-sensitive shape only).
- `GET /v1/selector/recent` — public, IP-rate-limited (30/min), returns the
  four-field meta snapshot for the landing hydration island. Uses `expired:true`
  (never 404) so client fetch logs stay clean.
- `POST /v1/selector/{id}/archive` — public, IP-rate-limited (10/min). Renames
  `selector:session:{id}:meta` → `selector:session:archived:{id}:{ts}` preserving
  the 24h TTL. The unchanged `GET /v1/selector/{id}` uses the DB, **not** Redis
  meta, so a direct `/selector/results/[id]` link still re-hydrates for the full
  24h after archive per the §3 acceptance criterion.
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
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from loftly.ai import LLMProvider, SelectorContext, get_provider
from loftly.ai.providers.typhoon import (
    TyphoonMalformedOutputError,
    TyphoonProvider,
    TyphoonUnavailableError,
)
from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import FixedWindowLimiter
from loftly.core.cache import get_cache
from loftly.core.feature_flags import FeatureFlags
from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.card import Card as CardModel
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.selector_session import SelectorSession
from loftly.observability.posthog import capture as posthog_capture
from loftly.observability.posthog import hash_distinct_id
from loftly.prompts.typhoon_nlu_spend import prompt_slug as nlu_prompt_slug
from loftly.schemas.selector import (
    FallbackReason,
    PromoChipPayload,
    SelectorInput,
    SelectorResult,
)
from loftly.schemas.spend_nlu import SpendNLURequest, SpendNLUResponse
from loftly.selector.promo_snapshot import (
    PromoSnapshot,
    build_promo_snapshot,
    degraded_snapshot,
)
from loftly.selector.session_cache import (
    SessionMeta,
    archive_session,
    read_session_meta,
    write_session_meta,
)

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
# POST_V1 §3 Tier A (2026-04-22): promo snapshot must be cheap — 500ms budget.
# Any failure falls back to `degraded_snapshot(reason="query_failed")` so the
# Selector never hard-fails on a flaky index.
_PROMO_SNAPSHOT_TIMEOUT_SEC = 0.5
# AI_PROMPTS.md §Quality gates: total THB must match Σ (points × thb_per_point) ±5%.
_THB_DRIFT_TOLERANCE = 0.05
# 429 retry policy — one retry with fixed backoff before falling back.
_SONNET_RETRY_BACKOFF_SEC = 1.0
# Haiku cost cap. Only pathological contexts (catalog explosion, huge prompt)
# should blow this — normal calls estimate well under the cap. If the estimate
# exceeds, skip Haiku entirely and drop straight to deterministic.
_HAIKU_COST_CAP_THB = 0.50
# FX used for the cost cap comparison. Directional — pulled from the same
# quarterly review as valuation config.
_USD_TO_THB = 35.0

# POST_V1 §3 rate limiters — per-IP, in-memory (resets between workers but one
# worker is our current deploy). 30/min for /recent (landing hydration, cheap),
# 10/min for /archive (user-initiated "ทำ Selector ใหม่" CTA — rare hot-path).
# Pattern mirrors `MAGIC_LINK_LIMITER` in `routes/auth.py`.
RECENT_LIMITER = FixedWindowLimiter(max_calls=30, window_sec=60)
ARCHIVE_LIMITER = FixedWindowLimiter(max_calls=10, window_sec=60)


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


async def _load_promo_snapshot(session: AsyncSession) -> PromoSnapshot | None:
    """Load the active-promo snapshot if the feature flag is ON.

    POST_V1 §3 Tier A (2026-04-22). Behind `LOFTLY_FF_SELECTOR_PROMO_CONTEXT`
    so we can flip promo context on/off in staging without a redeploy.

    - Flag OFF -> returns None (providers skip the block, backward-compat).
    - Flag ON + happy path -> returns the snapshot.
    - Flag ON + 500ms timeout or exception -> returns a degraded sentinel so
      the Selector still runs; the prompt gets `PROMO_CONTEXT_UNAVAILABLE`.
    """
    settings = get_settings()
    if not getattr(settings, "loftly_ff_selector_promo_context", False):
        return None
    try:
        return await asyncio.wait_for(
            build_promo_snapshot(session),
            timeout=_PROMO_SNAPSHOT_TIMEOUT_SEC,
        )
    except BaseException as exc:  # timeout, DB error, etc.
        log.warning(
            "selector_promo_snapshot_failed",
            error=str(exc)[:200],
        )
        return degraded_snapshot(reason="query_failed")


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

    active_promos = await _load_promo_snapshot(session)

    return SelectorContext(
        cards=cards,
        valuations_by_currency_code=valuations_by_code,
        active_promos=active_promos,
    )


# ---------------------------------------------------------------------------
# Provider fallback chain.
# ---------------------------------------------------------------------------


def _classify_sonnet_error(exc: BaseException) -> FallbackReason:
    """Map an exception raised by the Sonnet call to a classified fallback reason.

    We distinguish three upstream-signal buckets + a catch-all:
    - `rate_limit`   → HTTP 429 / `anthropic.RateLimitError`
    - `upstream_503` → HTTP 5xx / `anthropic.APIStatusError` / `InternalServerError`
    - `timeout`      → `asyncio.TimeoutError` (wait_for tripped) or SDK
                       `anthropic.APITimeoutError`
    - `both_failed`  → anything else (NotImplementedError in stub mode, parser
                       errors, network errors that don't match the above)

    Pure function — keeps the 8 chaos-test cases verifiable without a full
    route stand-up.
    """
    # asyncio.TimeoutError is aliased to TimeoutError in Python 3.11+.
    if isinstance(exc, TimeoutError):
        return "timeout"

    # Lazy import: anthropic is a heavyweight dep; callers that never touch
    # Sonnet (deterministic-only tests) shouldn't pay the import cost here.
    try:
        import anthropic as _anthropic
    except ImportError:  # pragma: no cover — SDK is a pinned dep
        return "both_failed"

    if isinstance(exc, _anthropic.APITimeoutError):
        return "timeout"
    if isinstance(exc, _anthropic.RateLimitError):
        return "rate_limit"
    if isinstance(exc, _anthropic.APIStatusError):
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return "rate_limit"
        if status_code is not None and 500 <= int(status_code) < 600:
            return "upstream_503"
    return "both_failed"


def _estimate_haiku_cost_thb(context: SelectorContext) -> float:
    """Rough cost estimate for the Haiku call, in THB.

    Prices the compact card catalog serialization as input tokens (at
    ~4 chars/token) + the max_tokens output budget Haiku is configured with,
    using published Haiku 4.5 pricing (not Sonnet). Deliberately pessimistic
    on the output side — if the cap fires we'd rather skip Haiku than
    surprise ourselves on the bill.
    """
    # Haiku 4.5 pricing (USD per 1M tokens). Sonnet pricing constants in
    # anthropic.py are for the Sonnet path; don't reuse them here.
    haiku_price_per_mil_input_usd = 1.00
    haiku_price_per_mil_output_usd = 5.00

    from loftly.ai.providers.anthropic_haiku import _compact_context

    input_chars = len(_compact_context(context))
    # Add a rough system + profile overhead (~500 tokens worth of chars).
    input_tokens_est = (input_chars // 4) + 500
    # Haiku's configured max_tokens in anthropic_haiku.py.
    output_tokens_est = 1_500
    cost_usd = (
        input_tokens_est / 1_000_000 * haiku_price_per_mil_input_usd
        + output_tokens_est / 1_000_000 * haiku_price_per_mil_output_usd
    )
    return cost_usd * _USD_TO_THB


async def _call_sonnet_with_retry(
    provider: LLMProvider,
    payload: SelectorInput,
    context: SelectorContext,
) -> SelectorResult:
    """Call Sonnet once; on `rate_limit`, sleep 1s and retry exactly once.

    Any other error propagates to the caller for classification.
    """
    try:
        return await asyncio.wait_for(
            provider.card_selector(payload, context),
            timeout=_SONNET_TIMEOUT_SEC,
        )
    except BaseException as exc:
        if _classify_sonnet_error(exc) != "rate_limit":
            raise
        log.warning("selector_sonnet_429_retry", backoff_sec=_SONNET_RETRY_BACKOFF_SEC)
        await asyncio.sleep(_SONNET_RETRY_BACKOFF_SEC)
        return await asyncio.wait_for(
            provider.card_selector(payload, context),
            timeout=_SONNET_TIMEOUT_SEC,
        )


async def _deterministic_fallback(
    payload: SelectorInput,
    context: SelectorContext,
    *,
    warnings: list[str],
    fallback_reason: FallbackReason,
) -> SelectorResult:
    """Run the deterministic provider and stamp fallback metadata."""
    from loftly.ai.providers.deterministic import DeterministicProvider

    rule_based: LLMProvider = DeterministicProvider()
    result = await rule_based.card_selector(payload, context)
    return result.model_copy(
        update={
            "warnings": [*result.warnings, *warnings],
            "fallback": True,
            "used_fallback": True,
            "used_deterministic": True,
            "fallback_reason": fallback_reason,
        }
    )


async def _run_with_fallback(
    payload: SelectorInput,
    context: SelectorContext,
) -> SelectorResult:
    """Sonnet → Haiku → deterministic, honoring AI_PROMPTS.md §Failure policy.

    Policy:
    1. Call Sonnet under a 10s asyncio timeout.
    2. Classify any exception via `_classify_sonnet_error`.
       - `rate_limit` → retry once after `_SONNET_RETRY_BACKOFF_SEC` seconds.
         If the retry also fails, drop to Haiku with `fallback_reason="rate_limit"`.
       - Everything else → drop straight to Haiku with the classified reason.
    3. Before calling Haiku, estimate THB cost. If > `_HAIKU_COST_CAP_THB`,
       skip Haiku entirely and land on deterministic with
       `fallback_reason="cost_cap"`.
    4. Call Haiku under a 5s timeout. Any error → deterministic with
       `fallback_reason="both_failed"`.
    5. Always return a result (deterministic provider is always available).
    """
    provider = get_provider()

    # Configured provider is deterministic → no fallback work to do.
    if provider.name != "anthropic":
        return await provider.card_selector(payload, context)

    warnings: list[str] = []
    fallback_reason: FallbackReason | None = None

    # 1) Try Sonnet (with one 429-retry baked in).
    try:
        result = await _call_sonnet_with_retry(provider, payload, context)
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
        fallback_reason = "both_failed"
    except BaseException as exc:
        fallback_reason = _classify_sonnet_error(exc)
        log.warning(
            "selector_sonnet_fallback",
            error=str(exc)[:200],
            reason=fallback_reason,
        )
        warnings.append(f"sonnet_fallback:{fallback_reason}")

    # 2) Cost-cap gate: skip Haiku if the estimated bill exceeds the cap.
    estimated_thb = _estimate_haiku_cost_thb(context)
    if estimated_thb > _HAIKU_COST_CAP_THB:
        log.warning(
            "selector_haiku_cost_cap_skipped",
            estimated_thb=round(estimated_thb, 4),
            cap_thb=_HAIKU_COST_CAP_THB,
        )
        return await _deterministic_fallback(
            payload,
            context,
            warnings=[*warnings, f"haiku_cost_cap:{estimated_thb:.3f}THB"],
            fallback_reason="cost_cap",
        )

    # 3) Try Haiku.
    try:
        from loftly.ai.providers.anthropic_haiku import AnthropicHaikuProvider

        haiku: LLMProvider = AnthropicHaikuProvider()
        result = await asyncio.wait_for(
            haiku.card_selector(payload, context),
            timeout=_HAIKU_TIMEOUT_SEC,
        )
        # Haiku succeeded: stamp used_fallback + the classified Sonnet reason.
        return result.model_copy(
            update={
                "warnings": [*result.warnings, *warnings],
                "used_fallback": True,
                "fallback_reason": fallback_reason,
                "fallback": True,
            }
        )
    except BaseException as exc:
        log.warning("selector_haiku_fallback", error=str(exc)[:200])
        warnings.append("haiku_fallback")

    # 4) Deterministic last resort — both LLMs failed.
    return await _deterministic_fallback(
        payload,
        context,
        warnings=[*warnings, "AI temporarily unavailable"],
        fallback_reason="both_failed",
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
            "used_fallback": result.used_fallback,
            "fallback_reason": result.fallback_reason,
            "used_deterministic": result.used_deterministic,
            "rationale_en": result.rationale_en,
        },
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _apply_promo_context(
    result: SelectorResult,
    context: SelectorContext,
) -> SelectorResult:
    """Server-side validation + denormalization of promo-context fields.

    POST_V1 §3 Tier A (2026-04-22). Contract:
    1. If `active_promos` is None (flag OFF), leave fields empty + status='ok'.
    2. If snapshot is degraded, stamp status='degraded' and strip any LLM-cited
       ids — the prompt told it PROMO_CONTEXT_UNAVAILABLE, but belt + braces.
    3. For an OK snapshot, filter each stack item's `cited_promo_ids` to only
       ids that exist in `snapshot.entries`. Log invalid cites so Langfuse can
       alert on hallucination rate.
    4. Union the filtered ids into top-level `cited_promo_ids` and build the
       chip payload from snapshot entries for the frontend.
    """
    snapshot = context.active_promos
    if snapshot is None:
        # Flag OFF — feature disabled. Empty fields.
        return result.model_copy(
            update={
                "cited_promo_ids": [],
                "promo_context_status": "ok",
                "promo_snapshot_digest": None,
                "promo_chips": [],
            }
        )

    if snapshot.status != "ok":
        # Degraded / stale — LLM was instructed not to cite. Strip anything
        # that slipped through.
        stripped_stack = [item.model_copy(update={"cited_promo_ids": []}) for item in result.stack]
        return result.model_copy(
            update={
                "stack": stripped_stack,
                "cited_promo_ids": [],
                "promo_context_status": snapshot.status,
                "promo_snapshot_digest": snapshot.digest,
                "promo_chips": [],
            }
        )

    # Happy path — filter hallucinated ids.
    valid_ids = {e.promo_id for e in snapshot.entries}
    cleaned_stack: list[Any] = []
    union_cited: list[str] = []
    seen: set[str] = set()
    for item in result.stack:
        kept: list[str] = []
        dropped: list[str] = []
        for pid in item.cited_promo_ids:
            if pid in valid_ids:
                kept.append(pid)
                if pid not in seen:
                    seen.add(pid)
                    union_cited.append(pid)
            else:
                dropped.append(pid)
        if dropped:
            log.warning(
                "selector_invalid_promo_cite",
                dropped_ids=dropped,
                card_id=item.card_id,
                snapshot_digest=snapshot.digest,
            )
        cleaned_stack.append(item.model_copy(update={"cited_promo_ids": kept}))

    chips = [
        PromoChipPayload(
            promo_id=e.promo_id,
            merchant=e.merchant,
            discount_value=e.discount_value,
            discount_type=e.discount_type,
            valid_until=e.valid_until,
            min_spend=e.minimum_spend,
        )
        for e in snapshot.entries
        if e.promo_id in union_cited
    ]

    return result.model_copy(
        update={
            "stack": cleaned_stack,
            "cited_promo_ids": union_cited,
            "promo_context_status": "ok",
            "promo_snapshot_digest": snapshot.digest,
            "promo_chips": chips,
        }
    )


async def _persist_session_meta(
    result: SelectorResult,
    context: SelectorContext,
    profile_hash: str,
) -> None:
    """POST_V1 §3 — write `selector:session:{id}:meta` after each result.

    Idempotent; a cache-hit rewrite is cheap and refreshes `last_seen_at` so the
    returning-user landing hero stays tied to "most recent visit" rather than
    "first computation". Keys + shape owned by `selector.session_cache`.
    """
    primary = next((it for it in result.stack if it.role == "primary"), None)
    if primary is None:
        # No primary card (e.g. empty stack from a niche filter) — nothing to
        # personalize with. Skip silently rather than write a half-populated
        # meta that the landing page would have to special-case.
        return
    card = next((c for c in context.cards if c.slug == primary.slug), None)
    card_name = card.display_name if card is not None else primary.slug
    await write_session_meta(
        session_id=result.session_id,
        meta=SessionMeta(
            card_name=card_name,
            card_id=primary.card_id,
            profile_hash=profile_hash,
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )


async def _compute_or_get_cached(
    payload: SelectorInput,
    session: AsyncSession,
) -> SelectorResult:
    """Core path used by both JSON and SSE responses. Handles cache + persist."""
    _validate_category_sum(payload)
    cache = get_cache()
    profile_hash = _profile_hash(payload)
    key = f"selector:{profile_hash}"

    cached = await cache.get(key)
    if cached is not None:
        log.info("selector_cache_hit", profile_hash=key)
        result = SelectorResult.model_validate(cached)
        # Refresh session-meta last_seen_at on every cache hit so the landing
        # hero reflects "returned within 24h" cleanly. Context load is cheap
        # (active cards only) and off the critical write path.
        context = await _load_context(session)
        await _persist_session_meta(result, context, profile_hash)
        return result

    context = await _load_context(session)
    raw_result = await _run_with_fallback(payload, context)
    # Validate + denormalize promo fields before persistence so the cached
    # envelope + DB row share a single shape.
    raw_result = _apply_promo_context(raw_result, context)

    row = SelectorSession(
        user_id=None,
        profile_hash=profile_hash,
        input=payload.model_dump(mode="json"),
        output=raw_result.model_dump(mode="json"),
        provider=raw_result.llm_model,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)

    result = raw_result.model_copy(update={"session_id": str(row.id), "partial_unlock": True})
    await cache.set(key, result.model_dump(mode="json"), _CACHE_TTL_SECONDS)
    await _persist_session_meta(result, context, profile_hash)
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


# ---------------------------------------------------------------------------
# W19: Typhoon NLU spend parser — `POST /v1/selector/parse-nlu`.
#
# Free-text Thai → structured SpendProfile. Behind the `typhoon_nlu_spend`
# feature flag. Flag OFF or key unset → 501 so the frontend knows to hide the
# free-text option and fall back to the structured form.
#
# Declared before `GET /{session_id}` so the literal path wins route matching
# even though uuid.UUID parsing would reject "parse-nlu" anyway — belt + braces.
# ---------------------------------------------------------------------------

_TYPHOON_FLAG_KEY = "typhoon_nlu_spend"
# Env-var fallback when the PostHog A/B harness isn't reachable or when a
# developer wants a quick flip. Flag integration order:
#   1) `LOFTLY_TYPHOON_NLU_ENABLED` env var — boolean override (dev + staging A/B)
#   2) PostHog (if POSTHOG_PROJECT_API_KEY set) — sampled / gradual rollout
#   3) default OFF
_TYPHOON_ENV_OVERRIDE = "LOFTLY_TYPHOON_NLU_ENABLED"


async def _typhoon_flag_enabled(request: Request) -> bool:
    """Return True if the `typhoon_nlu_spend` flag is ON for this caller."""
    import os

    override = os.environ.get(_TYPHOON_ENV_OVERRIDE)
    if override is not None:
        return override.strip().lower() in {"1", "true", "yes", "on"}

    client_host = request.client.host if request.client else "anon"
    distinct_id = hash_distinct_id(client_host, salt="typhoon-nlu")
    flags = FeatureFlags()
    return await flags.is_enabled(_TYPHOON_FLAG_KEY, distinct_id, default=False)


@router.post(
    "/parse-nlu",
    response_model=SpendNLUResponse,
    summary="Parse free-text Thai spend description into a structured profile",
)
async def parse_nlu(
    body: SpendNLURequest,
    request: Request,
) -> SpendNLUResponse:
    """Behind `typhoon_nlu_spend`. Returns 501 when flag OFF or key unset."""
    if not await _typhoon_flag_enabled(request):
        raise LoftlyError(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            code="typhoon_nlu_disabled",
            message_en="Free-text Thai spend parser is not enabled.",
            message_th="โหมดกรอกข้อความอิสระยังไม่เปิดให้ใช้งาน",
        )

    provider = TyphoonProvider()
    import httpx as _httpx

    try:
        profile, confidence, duration_ms = await provider.parse_spend_nlu(body.text_th)
    except TyphoonUnavailableError as exc:
        log.warning("typhoon_unavailable", error=str(exc)[:200])
        raise LoftlyError(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            code="typhoon_nlu_disabled",
            message_en="Free-text Thai spend parser is not available right now.",
            message_th="โหมดกรอกข้อความอิสระไม่พร้อมใช้งานในขณะนี้",
        ) from exc
    except TyphoonMalformedOutputError as exc:
        log.warning("typhoon_malformed_output", error=str(exc)[:200])
        raise LoftlyError(
            status_code=status.HTTP_502_BAD_GATEWAY,
            code="typhoon_malformed_output",
            message_en="Typhoon returned an output we could not parse. Try rewording.",
            message_th="ไม่สามารถตีความข้อความได้ กรุณาลองใหม่",
        ) from exc
    except _httpx.TimeoutException as exc:
        log.warning("typhoon_timeout_httpx")
        raise LoftlyError(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            code="typhoon_timeout",
            message_en="Typhoon parser timed out. Please try again.",
            message_th="หมดเวลารอการประมวลผล กรุณาลองใหม่",
        ) from exc
    except TimeoutError as exc:
        log.warning("typhoon_timeout")
        raise LoftlyError(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            code="typhoon_timeout",
            message_en="Typhoon parser timed out. Please try again.",
            message_th="หมดเวลารอการประมวลผล กรุณาลองใหม่",
        ) from exc

    # Fire-and-forget PostHog event (hashed anon id).
    client_host = request.client.host if request.client else None
    await posthog_capture(
        "typhoon_nlu_parsed",
        distinct_id=hash_distinct_id(client_host, salt="typhoon-nlu"),
        properties={
            "prompt": nlu_prompt_slug(),
            "duration_ms": duration_ms,
            "confidence": round(confidence, 3),
            "chars_in": len(body.text_th),
        },
    )

    return SpendNLUResponse(
        profile=profile,
        confidence=confidence,
        model=provider.model,
        duration_ms=duration_ms,
    )


# ---------------------------------------------------------------------------
# POST_V1 §3 — returning-user landing endpoints.
#
# Both routes are public (the session_id in hand IS the capability). They
# share the IP-based FixedWindowLimiter pattern from `routes/auth.py`.
# Declared before `GET /{session_id}` so the literal `/recent` path wins
# route matching; FastAPI would otherwise try to parse "recent" as a UUID
# and 422 before reaching the handler.
# ---------------------------------------------------------------------------


class RecentSessionResponse(BaseModel):
    """Shape returned by `GET /v1/selector/recent`.

    `expired=true` signals either "no session_id provided" or "meta gone / archived".
    Using a 200 with `expired:true` (rather than 404) keeps the frontend's fetch
    logs clean — a 404 would fire error-reporting hooks in dev/prod.
    """

    card_name: str | None = None
    card_id: str | None = None
    hours_since_last_session: float | None = None
    expired: bool


class ArchiveResponse(BaseModel):
    """Shape returned by `POST /v1/selector/{session_id}/archive`.

    `archived=false` means there was nothing to archive (already archived,
    expired, or never existed). Idempotent on the caller's side.
    """

    archived: bool


def _expired_recent() -> RecentSessionResponse:
    """Canonical empty response for "no session / expired / archived"."""
    return RecentSessionResponse(
        card_name=None,
        card_id=None,
        hours_since_last_session=None,
        expired=True,
    )


def _hours_since(iso_ts: str) -> float | None:
    """Convert an ISO-8601 `last_seen_at` into hours elapsed.

    Returns None on unparseable input rather than raising — the landing hero
    gracefully degrades to hiding the "X hours ago" copy.
    """
    try:
        parsed = datetime.fromisoformat(iso_ts)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # Stored without tzinfo (shouldn't happen — session_cache writes UTC
        # ISO strings — but be defensive).
        parsed = parsed.replace(tzinfo=UTC)
    delta = datetime.now(UTC) - parsed
    return round(delta.total_seconds() / 3600, 2)


@router.get(
    "/recent",
    response_model=RecentSessionResponse,
    summary="Non-sensitive meta for returning-user landing hydration",
)
async def get_recent_session(
    request: Request,
    session_id: str | None = Query(default=None, description="Client's stored session UUID"),
) -> RecentSessionResponse:
    """Public — IP rate-limited (30/min). Returns the four-field meta snapshot.

    Contract for the landing island (`POST_V1 §3`):
    - No `session_id`: `{expired:true, ...null}` — fresh visitor or cookie gone
    - Valid UUID + meta present: four-field populated shape, `expired:false`
    - Valid UUID + meta absent (expired or archived): `{expired:true, ...null}`
    - Malformed UUID: 400 (frontend bug — we want to see it)
    - Over quota: 429 per `API_CONTRACT.md §Rate limits`
    """
    ip = request.client.host if request.client else "unknown"
    if not RECENT_LIMITER.allow(ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many /recent requests — slow down.",
            message_th="เรียกข้อมูลถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )

    if session_id is None or session_id == "":
        return _expired_recent()

    # Validate UUID shape before hitting Redis — a malformed cookie is a
    # frontend bug we want to catch, not silently swallow as "expired".
    try:
        parsed = uuid.UUID(session_id)
    except (ValueError, AttributeError) as exc:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_session_id",
            message_en="session_id must be a valid UUID.",
            message_th="session_id ไม่ถูกต้อง",
        ) from exc

    meta = await read_session_meta(str(parsed))
    if meta is None:
        return _expired_recent()

    return RecentSessionResponse(
        card_name=meta.card_name,
        card_id=meta.card_id,
        hours_since_last_session=_hours_since(meta.last_seen_at),
        expired=False,
    )


@router.post(
    "/{session_id}/archive",
    response_model=ArchiveResponse,
    summary="Archive a prior session (ทำ Selector ใหม่ CTA)",
)
async def archive_selector_session(
    session_id: uuid.UUID,
    request: Request,
) -> ArchiveResponse:
    """Public — IP rate-limited (10/min). Idempotent.

    Renames `selector:session:{id}:meta` → `selector:session:archived:{id}:{ts}`
    preserving the 24h TTL via the `archive_session` wrapper from PR-1. The
    direct `GET /v1/selector/{id}?token=...` path still reads from Postgres
    (`selector_sessions` table), so a user's deep-link to `/selector/results/[id]`
    continues to work for the full 24h after archive — the rename only hides
    the session from the returning-user landing hero.
    """
    ip = request.client.host if request.client else "unknown"
    if not ARCHIVE_LIMITER.allow(ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many archive requests — slow down.",
            message_th="เรียกคำสั่งถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )

    archived = await archive_session(str(session_id))
    log.info("selector_session_archived", session_id=str(session_id), archived=archived)
    return ArchiveResponse(archived=archived)


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
