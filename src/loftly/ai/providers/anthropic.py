"""Anthropic Claude provider — stub for Phase 1.

Real wiring lands Week 7 per `mvp/DEV_PLAN.md`. Today this module raises
`NotImplementedError` on any call so code paths that would hit the network
fail loudly and deterministically.

Why a stub and not just "pick deterministic":
- The Protocol + env-var switch must exist end-to-end so we can flip without
  a deploy once the real client is ready.
- Keeps the structure + cache-control plumbing in one place for reviewers.

Implementation outline (Week 7 checklist — do NOT delete):

1. `messages.create(...)` with:
   - `model="claude-sonnet-4-5-20250929"` (primary)
   - `system=[{"type": "text", "text": SYSTEM_PROMPT,
               "cache_control": {"type": "ephemeral"}}]`
   - `messages=[{"role": "user", "content": USER_PROMPT}]`
   - Enforce structured output via JSON mode + Pydantic validation.
2. On Sonnet timeout >10s → retry once with Haiku
   (`claude-haiku-4-5-20251001`). See `AI_PROMPTS.md §Failure policy`.
3. On Haiku failure → fall through to DeterministicProvider
   (`fallback=true` on `SelectorResult`).
4. Wrap with Langfuse `@observe` for token/cost/latency telemetry.
5. Use cached-context block for the 50k card catalog + rubric
   (see `AI_PROMPTS.md §Cache boundaries`).

Until Week 7, `settings.loftly_llm_provider="anthropic"` is still accepted
(so ops can flip the flag when ready); selecting it without the wiring will
cause the first request to crash with NotImplementedError — visible, not
silent.
"""

from __future__ import annotations

from loftly.ai import (
    LLMProvider,
    SelectorContext,
    ValuationInput,
    ValuationOutput,
)
from loftly.schemas.selector import SelectorInput, SelectorResult

_TODO_MESSAGE = (
    "AnthropicProvider is a Phase-1 stub. Wire `anthropic` SDK per "
    "`mvp/AI_PROMPTS.md §Prompt 1` + §Cache boundaries (50k ephemeral block) "
    "before flipping LOFTLY_LLM_PROVIDER=anthropic in prod."
)


class AnthropicProvider:
    """Placeholder — always raises. See module docstring for the Week-7 plan."""

    name = "anthropic"

    async def card_selector(
        self,
        input: SelectorInput,
        context: SelectorContext,
    ) -> SelectorResult:
        # TODO(week-7): implement per AI_PROMPTS.md §Prompt 1.
        raise NotImplementedError(_TODO_MESSAGE)

    async def valuation(self, input: ValuationInput) -> ValuationOutput:
        # TODO(week-7): implement per AI_PROMPTS.md §Prompt 2.
        raise NotImplementedError(_TODO_MESSAGE)


__all__ = ["AnthropicProvider", "LLMProvider"]
