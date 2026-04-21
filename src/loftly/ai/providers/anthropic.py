"""Anthropic Claude provider — Sonnet primary with prompt caching.

Phase 2 wiring per `mvp/AI_PROMPTS.md §Prompt 1` + §Cache boundaries.

Behavior:
- Activates only when `ANTHROPIC_API_KEY` is set AND not `"stub"/"test"`.
  Otherwise raises NotImplementedError pointing to stub mode so tests +
  the deterministic default keep working.
- Uses structured output via tool-use JSON schema (`return_selector_stack`).
- Prepends a large cached context block: system prompt + serialized card
  catalog + point valuations + recent promos + few-shot. The block carries
  `cache_control: {"type": "ephemeral"}` so Anthropic caches it per
  AI_PROMPTS.md §Cache boundaries (~50k tokens, TTL 5 min).
- Logs prompt/completion token counts + USD cost estimate via structlog
  so Sentry + Langfuse dashboards can surface anomalies.

Timeouts:
- Caller (selector route) wraps the coroutine in `asyncio.wait_for`. Sonnet
  should respond well under 10s; if it doesn't, the route falls back to
  Haiku (see `anthropic_haiku.py`) and finally to the deterministic path.
"""

from __future__ import annotations

import json
from typing import Any, cast

from loftly.ai import (
    LLMProvider,
    SelectorContext,
    ValuationInput,
    ValuationOutput,
)
from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.schemas.selector import SelectorInput, SelectorResult, SelectorStackItem

log = get_logger(__name__)

_TODO_MESSAGE = (
    "AnthropicProvider requires ANTHROPIC_API_KEY to be set (and not 'stub'/'test'). "
    "Unset the key or set LOFTLY_LLM_PROVIDER=deterministic for stub/dev mode."
)

# Claude Sonnet 4.6 — per /docs/TECH_STACK.md primary pick. Opus 4.7 is reserved
# for heavier workloads (devaluation summaries, batch valuation reasoning) where
# the extra cost is justified.
SONNET_MODEL = "claude-sonnet-4-6"

# Rough USD pricing per 1M tokens (input, output). Check live via Models API
# for production; these are the numbers used in cost logging + Sentry alerts.
# Cached reads are ~10% of base input price.
_PRICE_PER_MIL_INPUT_USD = 3.00
_PRICE_PER_MIL_OUTPUT_USD = 15.00
_PRICE_PER_MIL_CACHE_READ_USD = 0.30

_SYSTEM_PROMPT = (
    "You are Loftly's card-selector model. Given a Thai user's monthly spend "
    "profile, pick the best 1–3 credit-card stack that maximizes their stated "
    "goal (miles, cashback, or benefits). Weight per-category earn rates "
    "against the provided point valuations (THB per point) and respect minimum "
    "income heuristics. Always respond via the `return_selector_stack` tool — "
    "no prose. Rationale fields must be Thai when locale=th, else English, "
    "<500 chars, and avoid banned marketing phrases. Include warnings for "
    "eligibility concerns."
)

# JSON schema aligned with SelectorResult. Used both for Sonnet + Haiku.
SELECTOR_TOOL_SCHEMA: dict[str, Any] = {
    "name": "return_selector_stack",
    "description": "Return the ranked 1-3 card stack with earning projections.",
    "input_schema": {
        "type": "object",
        "properties": {
            "stack": {
                "type": "array",
                "minItems": 1,
                "maxItems": 3,
                "items": {
                    "type": "object",
                    "properties": {
                        "card_id": {"type": "string"},
                        "slug": {"type": "string"},
                        "role": {
                            "type": "string",
                            "enum": ["primary", "secondary", "tertiary"],
                        },
                        "monthly_earning_points": {"type": "integer", "minimum": 0},
                        "monthly_earning_thb_equivalent": {"type": "integer", "minimum": 0},
                        "annual_fee_thb": {"type": ["number", "null"]},
                        "reason_th": {"type": "string"},
                        "reason_en": {"type": ["string", "null"]},
                    },
                    "required": [
                        "card_id",
                        "slug",
                        "role",
                        "monthly_earning_points",
                        "monthly_earning_thb_equivalent",
                        "reason_th",
                    ],
                },
            },
            "total_monthly_earning_points": {"type": "integer", "minimum": 0},
            "total_monthly_earning_thb_equivalent": {"type": "integer", "minimum": 0},
            "months_to_goal": {"type": ["integer", "null"]},
            "with_signup_bonus_months": {"type": ["integer", "null"]},
            "valuation_confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "rationale_th": {"type": "string", "maxLength": 500},
            "rationale_en": {"type": ["string", "null"]},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "stack",
            "total_monthly_earning_points",
            "total_monthly_earning_thb_equivalent",
            "valuation_confidence",
            "rationale_th",
        ],
    },
}


def _should_use_real_anthropic() -> bool:
    """Real calls only when a real key is set. Stub/test sentinels degrade."""
    settings = get_settings()
    key = (settings.anthropic_api_key or "").strip()
    return bool(key) and key.lower() not in {"stub", "test", "none"}


def _serialize_context(context: SelectorContext) -> str:
    """Pack active cards + valuations into a single stable JSON string.

    Stability matters for prompt caching — `sort_keys=True` ensures byte-level
    equivalence across calls so the cache prefix doesn't invalidate on dict
    ordering noise.
    """
    cards_payload = []
    for card in context.cards:
        if card.status != "active":
            continue
        cur = card.earn_currency
        cards_payload.append(
            {
                "card_id": str(card.id),
                "slug": card.slug,
                "display_name": card.display_name,
                "bank": card.bank.display_name_en if card.bank else None,
                "earn_currency_code": cur.code if cur else None,
                "earn_currency_type": cur.currency_type if cur else None,
                "earn_rate_local": card.earn_rate_local or {},
                "annual_fee_thb": (
                    float(card.annual_fee_thb) if card.annual_fee_thb is not None else None
                ),
                "min_income_thb": (
                    float(card.min_income_thb) if card.min_income_thb is not None else None
                ),
                "benefits": card.benefits or {},
            }
        )

    valuations_payload = {
        code: {
            "thb_per_point": float(val.thb_per_point),
            "methodology": val.methodology,
            "confidence": float(val.confidence),
        }
        for code, val in context.valuations_by_currency_code.items()
    }

    return json.dumps(
        {
            "cards": cards_payload,
            "valuations": valuations_payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _compute_cost_usd(
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
) -> float:
    """Back-of-the-envelope cost estimate for structlog events."""
    billable_input = max(input_tokens - cache_read_tokens, 0)
    return (
        billable_input / 1_000_000 * _PRICE_PER_MIL_INPUT_USD
        + cache_read_tokens / 1_000_000 * _PRICE_PER_MIL_CACHE_READ_USD
        + output_tokens / 1_000_000 * _PRICE_PER_MIL_OUTPUT_USD
    )


def _parse_tool_result(message: Any) -> dict[str, Any]:
    """Pluck the `tool_use.input` payload out of the Anthropic SDK response."""
    for block in message.content:
        if getattr(block, "type", None) == "tool_use":
            return dict(block.input)
    raise ValueError("Anthropic response did not contain a tool_use block.")


def _build_result_from_payload(
    payload: dict[str, Any],
    *,
    llm_model: str,
    fallback: bool,
) -> SelectorResult:
    """Coerce tool payload into SelectorResult — mirrors server-side validation."""
    stack = [SelectorStackItem.model_validate(it) for it in payload.get("stack", [])]
    return SelectorResult(
        session_id="anthropic",  # overwritten by the route handler
        stack=stack,
        total_monthly_earning_points=int(payload.get("total_monthly_earning_points", 0)),
        total_monthly_earning_thb_equivalent=int(
            payload.get("total_monthly_earning_thb_equivalent", 0)
        ),
        months_to_goal=payload.get("months_to_goal"),
        with_signup_bonus_months=payload.get("with_signup_bonus_months"),
        valuation_confidence=float(payload.get("valuation_confidence", 0.5)),
        rationale_th=str(payload.get("rationale_th", "")),
        rationale_en=payload.get("rationale_en"),
        warnings=list(payload.get("warnings", [])),
        llm_model=llm_model,
        fallback=fallback,
        partial_unlock=False,
    )


class AnthropicProvider:
    """Sonnet-backed selector provider with prompt caching."""

    name = "anthropic"
    model = SONNET_MODEL

    async def card_selector(
        self,
        input: SelectorInput,
        context: SelectorContext,
    ) -> SelectorResult:
        if not _should_use_real_anthropic():
            raise NotImplementedError(_TODO_MESSAGE)

        # Import lazily so tests + stub deploys don't pay the SDK import cost.
        from anthropic import AsyncAnthropic

        settings = get_settings()
        # max_retries=0: the route-level `_run_with_fallback` owns retry +
        # fallback policy (Sonnet → Haiku → deterministic). Letting the SDK
        # transparently retry would (a) mask 429/503/timeout signals the
        # classifier needs and (b) burn our 10s wait_for budget before the
        # fallback path ever kicks in.
        client = AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=0)

        cached_context = _serialize_context(context)
        user_profile = json.dumps(
            input.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

        # Structured tool-use call. Cached block sits right before the
        # per-request user profile so the prefix stays byte-stable across
        # requests (see shared/prompt-caching.md).
        # We use `cast(Any, ...)` on the SDK call because the Anthropic SDK's
        # TypedDict overloads don't accept our generic dict shapes — the wire
        # format is identical, but strict typing requires using the SDK's
        # imported `TextBlockParam` / `ToolParam` objects. Keeping Any here
        # since shape is validated by the SDK at runtime.
        response = await cast(Any, client.messages.create)(
            model=SONNET_MODEL,
            max_tokens=2_000,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                },
                {
                    "type": "text",
                    "text": f"### CARD CATALOG + VALUATIONS\n{cached_context}",
                    "cache_control": {"type": "ephemeral"},
                },
            ],
            tools=[SELECTOR_TOOL_SCHEMA],
            tool_choice={"type": "tool", "name": "return_selector_stack"},
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"User profile (JSON):\n{user_profile}\n\n"
                        f"Respond in {'Thai' if input.locale == 'th' else 'English'}."
                    ),
                }
            ],
        )

        usage = response.usage
        input_tokens = getattr(usage, "input_tokens", 0) or 0
        output_tokens = getattr(usage, "output_tokens", 0) or 0
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_creation = getattr(usage, "cache_creation_input_tokens", 0) or 0
        cost_usd = _compute_cost_usd(input_tokens, output_tokens, cache_read)

        log.info(
            "anthropic_selector_call",
            model=SONNET_MODEL,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read,
            cache_creation_tokens=cache_creation,
            cost_usd=round(cost_usd, 6),
        )

        payload = _parse_tool_result(response)
        return _build_result_from_payload(
            payload,
            llm_model=SONNET_MODEL,
            fallback=False,
        )

    async def valuation(self, input: ValuationInput) -> ValuationOutput:
        # Weekly valuation runs numeric pass directly; not wired through LLM yet.
        raise NotImplementedError(
            "AnthropicProvider.valuation not implemented — call jobs.valuation.compute()."
        )


# Utility export used by tests + Haiku fallback (same schema, same parser).
__all__ = [
    "SELECTOR_TOOL_SCHEMA",
    "SONNET_MODEL",
    "AnthropicProvider",
    "LLMProvider",
    "_build_result_from_payload",
    "_compute_cost_usd",
    "_parse_tool_result",
    "_serialize_context",
    "_should_use_real_anthropic",
]
