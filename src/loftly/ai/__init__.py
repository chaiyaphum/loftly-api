"""LLM provider abstraction.

Every LLM-backed call in Loftly goes through a `LLMProvider` implementation so
we can:
- swap models / vendors without touching call-sites
- run deterministic (no-LLM) fallback in tests and during outages
- centralize prompt-caching hints (see `mvp/AI_PROMPTS.md §Cache boundaries`)

Two implementations ship Phase 1:
- `DeterministicProvider` — rule-based, no network; always available. Returns
  `fallback=true` on `SelectorOutput` so clients treat it as the rule-based
  path mandated by SPEC.md §2 acceptance criteria.
- `AnthropicProvider` — stub that raises `NotImplementedError`. The real
  Claude Sonnet / Haiku wiring lands in Week 7; the schema + cache_control
  hints are in place so swap is a localized change.

Selection is driven by `settings.loftly_llm_provider`. Tests monkey-patch
the module-level `_PROVIDER` singleton via `set_provider(...)`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from loftly.db.models.card import Card as CardModel
from loftly.db.models.point_valuation import PointValuation
from loftly.schemas.selector import SelectorInput, SelectorResult


@dataclass(frozen=True)
class SelectorContext:
    """Data the selector provider needs beyond user input.

    Resolved by the route handler (DB read) and handed to the provider so the
    provider stays pure / testable. Cards are eager-loaded with bank +
    earn_currency so providers can render display fields without extra I/O.
    """

    cards: list[CardModel]
    valuations_by_currency_code: dict[str, PointValuation]


@dataclass(frozen=True)
class ValuationInput:
    """Input for the valuation provider — one currency's award + cash fixtures.

    The `currency_code` is canonical (`loyalty_currencies.code`). Fixtures are
    pre-loaded JSON; see `data/award_charts/` + `data/cash_fares/`.
    """

    currency_code: str
    award_chart: dict[str, object]
    cash_fares: dict[str, object]
    previous_thb_per_point: float | None = None


@dataclass(frozen=True)
class ValuationOutput:
    """Result of a valuation run. Mirrors `AI_PROMPTS.md §Prompt 2 output`.

    `distribution_summary` holds {p10,p25,p50,p75,p90} for the public
    methodology page histogram.
    """

    thb_per_point: float
    methodology: str
    percentile: int
    sample_size: int
    confidence: float
    top_redemption_example: str | None
    distribution_summary: dict[str, float]
    sanity_flags: list[str]


class LLMProvider(Protocol):
    """Protocol every provider implements. Selector + Valuation are the two v1 calls."""

    name: str

    async def card_selector(
        self,
        input: SelectorInput,
        context: SelectorContext,
    ) -> SelectorResult: ...

    async def valuation(self, input: ValuationInput) -> ValuationOutput: ...


# Module-level singleton. Resolved lazily on first access; tests override via
# `set_provider()`.
_PROVIDER: LLMProvider | None = None


def get_provider() -> LLMProvider:
    """Return the configured LLM provider singleton. Lazy-init on first call."""
    global _PROVIDER
    if _PROVIDER is not None:
        return _PROVIDER
    from loftly.ai.providers.anthropic import AnthropicProvider
    from loftly.ai.providers.deterministic import DeterministicProvider
    from loftly.core.settings import get_settings

    settings = get_settings()
    if settings.loftly_llm_provider == "anthropic":
        _PROVIDER = AnthropicProvider()
    else:
        _PROVIDER = DeterministicProvider()
    return _PROVIDER


def set_provider(provider: LLMProvider | None) -> None:
    """Override the provider singleton (used by lifespan init + tests)."""
    global _PROVIDER
    _PROVIDER = provider


__all__ = [
    "LLMProvider",
    "SelectorContext",
    "ValuationInput",
    "ValuationOutput",
    "get_provider",
    "set_provider",
]
