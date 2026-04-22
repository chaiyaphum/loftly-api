"""POST_V1 §1 — `POST /v1/selector/{session_id}/chat`.

Follow-up Q&A on Selector results. Two question shapes:
- **explain** — "why is card X ranked first?" → Haiku answer, cards unchanged
- **what-if** — "if I add THB 20,000 to dining, does it change?" → deterministic
  pre-flight modifies the cached profile, re-runs the selector, compares the
  stacks, and returns an updated top-3

Key contracts (see `mvp/POST_V1.md §1 Acceptance criteria`):
- Flag-gated by `post_v1_selector_chat` — 404 when OFF (crawler safety; don't
  advertise the surface)
- Email-gate mirrors v1 — anon sessions with `partial_unlock=true` and no
  bound user must capture email first → 403 `email_gate_required`
- Rate-limit = 10 questions / session. 11th → 429 with a static Thai string
- Haiku timeout = 5s hard. Endpoint budget = 6s. Post-timeout, return a static
  Thai fallback and do NOT bill the user (no retry on the same call)
- Cost cap = THB 0.10 per call. Pre-flight estimate > cap → rejected before
  Haiku is contacted. Cheaper than learning about the bill post-hoc
- Instrumentation via PostHog — one-shot `selector_chat_opened` per session
  (sentinel cache key), plus per-call `asked` / `rerank_delivered` /
  `rate_limited`
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.ai import LLMProvider, SelectorContext, get_provider
from loftly.api.errors import LoftlyError
from loftly.core.cache import get_cache
from loftly.core.feature_flags import FeatureFlags
from loftly.core.logging import get_logger
from loftly.db.engine import get_session
from loftly.db.models.selector_session import SelectorSession
from loftly.observability.posthog import capture as posthog_capture
from loftly.observability.posthog import hash_distinct_id
from loftly.prompts import selector_chat_followup as chat_prompt
from loftly.schemas.selector import SelectorInput, SelectorResult
from loftly.selector.chat_classifier import classify, extract_whatif_params
from loftly.selector.session_cache import (
    chat_cap,
    get_chat_count,
    increment_chat_count,
    read_context,
)

log = get_logger(__name__)
router = APIRouter(prefix="/v1/selector", tags=["selector"])

_FEATURE_FLAG = "post_v1_selector_chat"

# Haiku 5s budget per POST_V1.md §1 AC-5. The route's overhead (DB + cache +
# PostHog) is well under the 1s buffer that keeps the total endpoint budget
# at ~6s end-to-end.
_HAIKU_TIMEOUT_SEC = 5.0

# Cost cap per plan + AI_PROMPTS.md cost table (post-v1 chat budgeted at
# ~$0.003 / call → well under THB 0.10).
_CHAT_COST_CAP_THB = 0.10
_USD_TO_THB = 35.0
_HAIKU_PRICE_PER_MIL_INPUT_USD = 1.00
_HAIKU_PRICE_PER_MIL_OUTPUT_USD = 5.00
_HAIKU_MAX_OUTPUT_TOKENS = 600  # per AI_PROMPTS.md Prompt 5 "51k / 600"
# Typical answer length (~400 chars / ~130 tokens). AC bounds the answer at
# ≤ 400 chars so the cost-cap estimate uses this as the expected output, not
# the 600-token hard cap. A real answer that somehow hits the cap is still
# bounded by the SDK — this estimate only governs the pre-flight skip.
_HAIKU_EXPECTED_OUTPUT_TOKENS = 200

# Static fallback copy — must match the Thai string in POST_V1.md §1 AC-5.
_HAIKU_FALLBACK_TH = "ขออภัย ลองใหม่อีกครั้งได้เลย"
_HAIKU_FALLBACK_EN = "Sorry, please try again in a moment."

# Rate-limit copy — must match the Thai string in POST_V1.md §1 AC-4.
_RATE_LIMIT_TH = "คำถามต่อเซสชันครบแล้ว เริ่ม Selector ใหม่ได้ที่ /selector"


# Sentinel cache key marking that `selector_chat_opened` has fired once for
# this session. Guarantees at-most-once semantics across worker restarts
# within the 24h session lifetime.
def _opened_sentinel_key(session_id: str) -> str:
    return f"selector:session:{session_id}:chat_opened_emitted"


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """`POST /v1/selector/{session_id}/chat` body."""

    question: str = Field(
        min_length=1,
        max_length=500,
        description="User's follow-up question. Max 500 chars per AI_PROMPTS.md Prompt 5.",
    )


class ChatStackItem(BaseModel):
    """Slim card-level diff entry for `new_stack`."""

    card_id: str
    slug: str
    role: str
    monthly_earning_points: int
    monthly_earning_thb_equivalent: int
    reason_th: str


class ChatResponse(BaseModel):
    """`POST /v1/selector/{session_id}/chat` response envelope."""

    answer_th: str
    answer_en: str | None = None
    cards_changed: bool
    new_stack: list[ChatStackItem] | None = None
    rationale_diff_bullets: list[str] = Field(default_factory=list)
    category: str  # "explain" | "what-if" | "other"
    remaining_questions: int


# ---------------------------------------------------------------------------
# Feature-flag + email-gate guards
# ---------------------------------------------------------------------------


async def _flag_enabled(session_id: str) -> bool:
    """Return True when `post_v1_selector_chat` is ON for this session."""
    distinct_id = hash_distinct_id(session_id, salt="selector-chat")
    flags = FeatureFlags()
    return await flags.is_enabled(_FEATURE_FLAG, distinct_id, default=False)


async def _load_original_session(
    session_id: uuid.UUID,
    db: AsyncSession,
) -> SelectorSession | None:
    row = (
        (await db.execute(select(SelectorSession).where(SelectorSession.id == session_id)))
        .scalars()
        .one_or_none()
    )
    return row


def _requires_email_gate(row: SelectorSession) -> bool:
    """Anon sessions must clear the email gate before chatting.

    `partial_unlock` is stamped on the response envelope in
    `routes/selector.py::_compute_or_get_cached`, not on the persisted
    `output` JSON, so the authoritative signal is "no user bound":
    `user_id is None AND bound_at is None`. Magic-link consume sets both,
    which is exactly what the §1 AC requires to unlock follow-up chat.
    """
    return row.user_id is None and row.bound_at is None


# ---------------------------------------------------------------------------
# Haiku call + cost cap
# ---------------------------------------------------------------------------


def _estimate_chat_cost_thb(system: str, user: str) -> float:
    """Rough pre-flight estimate for the chat Haiku call, in THB.

    Priced at Haiku 4.5 rates for the *fresh* per-request tokens only. The
    50k-token Selector context block is cached upstream by the Sonnet
    provider (`cache_control: ephemeral`) — its read cost is amortized on
    the Selector bill, not the chat bill. Double-counting it here would
    push every warm call over the cap.

    Per `AI_PROMPTS.md §Prompt cost summary`, the budgeted `selector_chat_followup`
    cost is ~$0.003/call ≈ THB 0.105. The cap is THB 0.10, so we budget
    tightly and skip anything that smells pathological (e.g., a 100k-char
    prompt from a catalog explosion).

    Deliberately pessimistic on token count — Thai runs ~3 chars/token,
    not the Latin 4, so use 3 to over-estimate.
    """
    fresh_input_chars = len(system) + len(user)
    fresh_tokens_est = max(1, fresh_input_chars // 3)
    output_tokens_est = _HAIKU_EXPECTED_OUTPUT_TOKENS
    cost_usd = (
        fresh_tokens_est / 1_000_000 * _HAIKU_PRICE_PER_MIL_INPUT_USD
        + output_tokens_est / 1_000_000 * _HAIKU_PRICE_PER_MIL_OUTPUT_USD
    )
    return cost_usd * _USD_TO_THB


async def _call_haiku_chat(
    system: str,
    user: str,
) -> dict[str, str | None]:
    """Call Haiku 4.5 for the follow-up answer. Returns `{"answer_th", "answer_en"}`.

    Raises the SDK's exception types on failure (caller converts to static
    fallback). The provider here is a chat-mode shim — the selector path's
    Haiku provider uses tool-use for structured stack output, which is the
    wrong shape for a free-form Q&A answer.
    """
    from loftly.ai.providers.anthropic import _should_use_real_anthropic
    from loftly.ai.providers.anthropic_haiku import HAIKU_MODEL

    if not _should_use_real_anthropic():
        # Stub path: deterministic canned answer so tests without ANTHROPIC_API_KEY
        # can still exercise the route end-to-end.
        return {
            "answer_th": "นี่คือคำตอบจำลอง (ยังไม่ได้ต่อ Haiku จริง)",
            "answer_en": "Stubbed answer (real Haiku not wired).",
        }

    from typing import cast

    from anthropic import AsyncAnthropic

    from loftly.core.settings import get_settings

    settings = get_settings()
    client = AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=0)

    response: Any = await cast(Any, client.messages.create)(
        model=HAIKU_MODEL,
        max_tokens=_HAIKU_MAX_OUTPUT_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user}],
    )

    # Flatten any text blocks Haiku returns. Chat mode doesn't use tool_use.
    chunks: list[str] = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            chunks.append(getattr(block, "text", ""))
    answer = "\n".join(chunks).strip()
    if not answer:
        answer = _HAIKU_FALLBACK_TH
    return {"answer_th": answer, "answer_en": None}


# ---------------------------------------------------------------------------
# What-if re-rank
# ---------------------------------------------------------------------------


def _apply_whatif_delta(
    original_input: SelectorInput,
    params: dict[str, Any],
) -> SelectorInput:
    """Return a new `SelectorInput` with the delta applied to the category.

    Preserves the `monthly_spend_thb` invariant by also bumping the total so
    the category-sum validator stays happy when we re-run the selector.
    Negative deltas clamp category spend at 0.
    """
    category = str(params["category"])
    delta = int(params["amount_thb_delta"])
    new_categories = dict(original_input.spend_categories)
    current = int(new_categories.get(category, 0))
    new_categories[category] = max(0, current + delta)
    new_total = int(original_input.monthly_spend_thb) + delta
    new_total = max(5_000, new_total)  # respect SelectorInput.ge=5_000
    return original_input.model_copy(
        update={
            "spend_categories": new_categories,
            "monthly_spend_thb": new_total,
        }
    )


async def _rerank_for_whatif(
    original_input: SelectorInput,
    params: dict[str, Any],
    db: AsyncSession,
) -> SelectorResult | None:
    """Run the selector with a modified profile. Returns None on any failure.

    We deliberately swallow errors and return None so the main chat path can
    still deliver the explain-level answer rather than fail the whole call.
    """
    try:
        # Use the same context loader as the primary selector route.
        from loftly.api.routes.selector import _load_context

        context: SelectorContext = await _load_context(db)
        new_input = _apply_whatif_delta(original_input, params)
        provider: LLMProvider = get_provider()
        return await provider.card_selector(new_input, context)
    except Exception as exc:
        log.warning("selector_chat_whatif_rerank_failed", error=str(exc)[:200])
        return None


def _diff_stacks(
    old: list[dict[str, Any]],
    new: SelectorResult,
) -> tuple[bool, list[ChatStackItem], list[str]]:
    """Return `(cards_changed, new_stack_items, rationale_diff_bullets)`.

    `cards_changed` is True when the set of card slugs differs OR when the
    top card's slug changed (rank flip). The diff bullets call out the
    primary card delta in Thai for direct inclusion in the response.
    """
    old_slugs: list[str] = [str(item.get("slug", "")) for item in (old or [])]
    new_stack = [
        ChatStackItem(
            card_id=item.card_id,
            slug=item.slug,
            role=item.role,
            monthly_earning_points=item.monthly_earning_points,
            monthly_earning_thb_equivalent=item.monthly_earning_thb_equivalent,
            reason_th=item.reason_th,
        )
        for item in new.stack
    ]
    new_slugs: list[str] = [item.slug for item in new_stack]
    cards_changed = bool(
        set(old_slugs) != set(new_slugs)
        or (old_slugs and new_slugs and old_slugs[0] != new_slugs[0])
    )
    bullets: list[str] = []
    if cards_changed and new_slugs:
        if old_slugs and new_slugs[0] != old_slugs[0]:
            bullets.append(f"อันดับ 1 เปลี่ยนจาก {old_slugs[0]} → {new_slugs[0]}")
        added = [s for s in new_slugs if s not in old_slugs]
        removed = [s for s in old_slugs if s not in new_slugs]
        if added:
            bullets.append(f"เพิ่ม: {', '.join(added)}")
        if removed:
            bullets.append(f"ออก: {', '.join(removed)}")
    return cards_changed, new_stack, bullets


# ---------------------------------------------------------------------------
# Main route
# ---------------------------------------------------------------------------


async def _emit_opened_once(session_id: str, auth_state: str) -> None:
    """Fire `selector_chat_opened` exactly once per session_id.

    Uses a cache sentinel — at-most-once semantics survive worker restarts
    because the sentinel itself is in the shared cache.
    """
    cache = get_cache()
    sentinel = _opened_sentinel_key(session_id)
    if await cache.get(sentinel) is not None:
        return
    await cache.set(sentinel, True, ttl_seconds=86_400)
    await posthog_capture(
        "selector_chat_opened",
        distinct_id=hash_distinct_id(session_id, salt="selector-chat"),
        properties={"session_id": session_id, "auth_state": auth_state},
    )


@router.post(
    "/{session_id}/chat",
    response_model=ChatResponse,
    summary="POST_V1 §1 — follow-up question on a Selector result",
)
async def chat(
    session_id: str,
    body: ChatRequest,
    request: Request,
    db: AsyncSession = Depends(get_session),
) -> ChatResponse:
    _ = request  # reserved for future per-request hooks

    # 0) session_id must be a valid UUID — reject malformed IDs before flag lookup
    #    so we don't burn a flag evaluation on garbage input.
    try:
        uuid_val = uuid.UUID(session_id)
    except ValueError as exc:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_session_id",
            message_en="session_id must be a valid UUID.",
            message_th="รหัส session ไม่ถูกต้อง",
        ) from exc

    # 1) Feature-flag gate — 404 when OFF (don't advertise the surface).
    if not await _flag_enabled(session_id):
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
            message_en="Not found.",
            message_th="ไม่พบ",
        )

    # 2) Session must exist in DB (the authoritative source of user binding).
    row = await _load_original_session(uuid_val, db)
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_410_GONE,
            code="session_expired",
            message_en="Session expired or not found.",
            message_th="Session หมดอายุหรือไม่พบ",
        )

    # 3) Email-gate — anon + partial_unlock → 403, prompt to capture email.
    if _requires_email_gate(row):
        raise LoftlyError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="email_gate_required",
            message_en="Please provide your email to unlock follow-up chat.",
            message_th="กรุณาใส่อีเมลเพื่อปลดล็อคการถามต่อ",
        )

    # 4) Rate-limit — increment and check the cap. The §1 spec defines cap=10;
    #    11th request → 429 with the static Thai message + PostHog event.
    new_count = await increment_chat_count(session_id)
    cap = chat_cap()
    if new_count > cap:
        await posthog_capture(
            "selector_chat_rate_limited",
            distinct_id=hash_distinct_id(session_id, salt="selector-chat"),
            properties={"session_id": session_id},
        )
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="chat_rate_limited",
            message_en="Chat limit reached for this session. Start a new Selector at /selector.",
            message_th=_RATE_LIMIT_TH,
        )

    # 5) Classify.
    category = classify(body.question)

    # Fire the one-shot `opened` event after we're past the gates but before
    # we bill any LLM work. Auth state is "authed" when the session has been
    # bound to a user; otherwise "anon".
    auth_state = "authed" if (row.user_id or row.bound_at) else "anon"
    await _emit_opened_once(session_id, auth_state)

    # Ask-event fires on every successful entry to the LLM path, even if we
    # subsequently fall back (tokens_in is the pre-flight char count / 3).
    tokens_in = max(1, len(body.question) // 3)
    await posthog_capture(
        "selector_chat_question_asked",
        distinct_id=hash_distinct_id(session_id, salt="selector-chat"),
        properties={
            "session_id": session_id,
            "category": category,
            "tokens_in": tokens_in,
        },
    )

    # 6) Cached-context check — if the 50k block is gone, 410. (The §1 spec
    #    ties chat to the same 24h Redis TTL as the selector result.)
    context_blob = await read_context(session_id)
    if context_blob is None:
        raise LoftlyError(
            status_code=status.HTTP_410_GONE,
            code="session_expired",
            message_en="Selector context expired; please re-run Selector.",
            message_th="Session หมดอายุ กรุณาทำ Selector ใหม่",
        )

    # 7) For what-if, attempt the deterministic re-rank *before* Haiku so the
    #    prompt can include the diff.
    try:
        original_input = SelectorInput.model_validate(row.input)
    except Exception as exc:
        log.error("selector_chat_input_reparse_failed", error=str(exc)[:200])
        raise LoftlyError(
            status_code=status.HTTP_410_GONE,
            code="session_expired",
            message_en="Selector session is corrupted; please re-run.",
            message_th="Session เสียหาย กรุณาทำ Selector ใหม่",
        ) from exc

    rerank_result: SelectorResult | None = None
    rerank_params: dict[str, Any] | None = None
    if category == "what-if":
        rerank_params = extract_whatif_params(body.question)
        if rerank_params is not None:
            rerank_result = await _rerank_for_whatif(original_input, rerank_params, db)

    # 8) Build the Haiku prompt.
    import json as _json

    stored_stack: list[dict[str, Any]] = list((row.output or {}).get("stack", []))
    rationale_th = str((row.output or {}).get("rationale_th", ""))
    stack_json = _json.dumps(stored_stack, ensure_ascii=False)
    prompt = chat_prompt.load(
        {  # type: ignore[arg-type]
            "locale": original_input.locale,
            "rationale_th": rationale_th,
            "stack_json": stack_json,
            "category": category,
            "question": body.question,
        }
    )

    # Cost cap — pre-flight estimate. Over the cap → skip the Haiku call and
    # surface the static fallback. No billable work.
    estimated_thb = _estimate_chat_cost_thb(prompt["system"], prompt["user"])  # type: ignore[index]
    answer_th = _HAIKU_FALLBACK_TH
    answer_en: str | None = _HAIKU_FALLBACK_EN
    if estimated_thb > _CHAT_COST_CAP_THB:
        log.warning(
            "selector_chat_cost_cap_skipped",
            estimated_thb=round(estimated_thb, 4),
            cap_thb=_CHAT_COST_CAP_THB,
        )
    else:
        # 9) Haiku call under a 5s timeout. Any failure → static fallback.
        try:
            reply = await asyncio.wait_for(
                _call_haiku_chat(prompt["system"], prompt["user"]),  # type: ignore[index]
                timeout=_HAIKU_TIMEOUT_SEC,
            )
            answer_th = reply.get("answer_th") or _HAIKU_FALLBACK_TH
            answer_en = reply.get("answer_en")
        except (TimeoutError, Exception) as exc:
            log.warning("selector_chat_haiku_fallback", error=str(exc)[:200])

    # 10) Build diff + response.
    cards_changed = False
    new_stack: list[ChatStackItem] | None = None
    bullets: list[str] = []
    if rerank_result is not None:
        cards_changed, new_stack_list, bullets = _diff_stacks(stored_stack, rerank_result)
        new_stack = new_stack_list if cards_changed else None

    await posthog_capture(
        "selector_chat_rerank_delivered",
        distinct_id=hash_distinct_id(session_id, salt="selector-chat"),
        properties={"session_id": session_id, "cards_changed": cards_changed},
    )

    current_count = await get_chat_count(session_id)
    remaining = max(0, cap - current_count)
    return ChatResponse(
        answer_th=answer_th,
        answer_en=answer_en,
        cards_changed=cards_changed,
        new_stack=new_stack,
        rationale_diff_bullets=bullets,
        category=category,
        remaining_questions=remaining,
    )


__all__ = ["ChatRequest", "ChatResponse", "router"]
