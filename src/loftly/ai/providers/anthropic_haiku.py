"""Anthropic Claude Haiku fallback provider.

Used by the selector route when the Sonnet call times out (>10s) or fails
quality gates. Haiku's context budget is tighter (200k tokens, not 1M), so we
skip the 50k cached catalog block and instead pass a compact card list inline.

Activation mirrors the Sonnet provider: real calls require ANTHROPIC_API_KEY
to be set and not `"stub"/"test"`. Otherwise raises NotImplementedError so the
route can fall through to the deterministic rule-based path per
AI_PROMPTS.md §Failure policy.
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
from loftly.ai.providers.anthropic import (
    SELECTOR_TOOL_SCHEMA,
    _build_result_from_payload,
    _compute_cost_usd,
    _parse_tool_result,
    _should_use_real_anthropic,
)
from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.schemas.selector import SelectorInput, SelectorResult

log = get_logger(__name__)

# Haiku 4.5 — per TECH_STACK.md fast-path for fallbacks.
HAIKU_MODEL = "claude-haiku-4-5-20251001"

_HAIKU_SYSTEM_PROMPT = (
    "You are Loftly's fast fallback selector. The Sonnet primary timed out. "
    "Pick the best 1-3 credit cards for the user's spend profile from the "
    "compact list provided. Respond via the `return_selector_stack` tool with "
    "a brief Thai rationale. Prioritize correctness over richness. "
    # Mirrors the Sonnet promo-context contract so the fallback produces a
    # consistent shape (stack items may include `cited_promo_ids`). See
    # POST_V1 §3 Tier A (2026-04-22).
    "When an `ACTIVE PROMOS` block is supplied and a promo fits the user's "
    "category + spend + card stack, cite the promo by title in reason_th and "
    "populate `cited_promo_ids` on that stack item. Never invent promos; if "
    "none fit, omit mention. Never cite a promo whose `cards=[]` is empty."
)


def _compact_context(context: SelectorContext) -> str:
    """Cheaper inline serialization — just the fields Haiku needs to rank."""
    cards = []
    for card in context.cards:
        if card.status != "active":
            continue
        cur = card.earn_currency
        val = context.valuations_by_currency_code.get(cur.code) if cur else None
        cards.append(
            {
                "id": str(card.id),
                "slug": card.slug,
                "name": card.display_name,
                "currency": cur.code if cur else None,
                "currency_type": cur.currency_type if cur else None,
                "earn_rate": card.earn_rate_local or {},
                "thb_per_point": float(val.thb_per_point) if val else 0.0,
                "annual_fee": (
                    float(card.annual_fee_thb) if card.annual_fee_thb is not None else None
                ),
                "min_income": (
                    float(card.min_income_thb) if card.min_income_thb is not None else None
                ),
            }
        )
    return json.dumps(cards, separators=(",", ":"), ensure_ascii=False)


class AnthropicHaikuProvider:
    """Haiku-backed fallback selector. Same tool schema as Sonnet."""

    name = "anthropic_haiku"
    model = HAIKU_MODEL

    async def card_selector(
        self,
        input: SelectorInput,
        context: SelectorContext,
    ) -> SelectorResult:
        if not _should_use_real_anthropic():
            raise NotImplementedError(
                "AnthropicHaikuProvider requires ANTHROPIC_API_KEY. "
                "Route will fall through to the deterministic provider."
            )

        from anthropic import AsyncAnthropic

        settings = get_settings()
        # max_retries=0 — the route-level `_run_with_fallback` is the
        # authoritative policy (retry-once on Sonnet 429, then Haiku, then
        # deterministic). SDK-level retries would double-bill + mask signals.
        client = AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=0)

        cards_json = _compact_context(context)
        profile_json = json.dumps(
            input.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        # POST_V1 §3 Tier A: pass through the promo block inline (no prompt
        # caching on the Haiku path — spec says tighter context budget). When
        # `active_promos` is None the block is omitted entirely.
        from loftly.selector.promo_snapshot import serialize_snapshot_for_prompt

        promos_block = (
            serialize_snapshot_for_prompt(context.active_promos)
            if context.active_promos is not None
            else None
        )

        user_content = f"Cards: {cards_json}\nProfile: {profile_json}\nLocale: {input.locale}"
        if promos_block is not None:
            user_content = f"{user_content}\n\n{promos_block}"

        response: Any = await cast(Any, client.messages.create)(
            model=HAIKU_MODEL,
            max_tokens=1_500,
            system=_HAIKU_SYSTEM_PROMPT,
            tools=[SELECTOR_TOOL_SCHEMA],
            tool_choice={"type": "tool", "name": "return_selector_stack"},
            messages=[
                {
                    "role": "user",
                    "content": user_content,
                }
            ],
        )

        usage = response.usage
        input_tokens = getattr(usage, "input_tokens", 0) or 0
        output_tokens = getattr(usage, "output_tokens", 0) or 0
        cost_usd = _compute_cost_usd(input_tokens, output_tokens, 0)
        log.info(
            "anthropic_haiku_selector_call",
            model=HAIKU_MODEL,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost_usd, 6),
        )

        payload = _parse_tool_result(response)
        return _build_result_from_payload(
            payload,
            llm_model=HAIKU_MODEL,
            fallback=True,  # AI_PROMPTS.md §Failure policy — Haiku path is fallback
        )

    async def valuation(self, input: ValuationInput) -> ValuationOutput:
        raise NotImplementedError("AnthropicHaikuProvider.valuation not implemented.")


__all__ = ["HAIKU_MODEL", "AnthropicHaikuProvider", "LLMProvider"]
