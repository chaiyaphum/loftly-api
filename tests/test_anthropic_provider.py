"""AnthropicProvider tests.

Covers stub-mode (no key → NotImplementedError with helpful message) and
the happy path with a mocked SDK response — happy path proves the tool_use
parsing + cost logging work without actually hitting the network.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from loftly.ai import SelectorContext
from loftly.ai.providers.anthropic import AnthropicProvider
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.schemas.selector import SelectorInput


def _base_payload() -> SelectorInput:
    return SelectorInput.model_validate(
        {
            "monthly_spend_thb": 80_000,
            "spend_categories": {
                "dining": 20_000,
                "online": 20_000,
                "travel": 20_000,
                "grocery": 10_000,
                "other": 10_000,
            },
            "current_cards": [],
            "goal": {"type": "miles", "currency_preference": "UOB_REWARDS"},
            "locale": "th",
        }
    )


async def _load_context() -> SelectorContext:
    # Reuse the route's loader to avoid re-implementing joins.
    from loftly.api.routes.selector import _load_context

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await _load_context(session)


async def test_stub_mode_raises_not_implemented(seeded_db: object) -> None:
    _ = seeded_db
    # ANTHROPIC_API_KEY is unset in test env — stub mode should engage.
    context = await _load_context()
    provider = AnthropicProvider()
    with pytest.raises(NotImplementedError) as exc:
        await provider.card_selector(_base_payload(), context)
    assert "ANTHROPIC_API_KEY" in str(exc.value)


async def test_stub_sentinel_also_raises(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = seeded_db
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stub")
    get_settings.cache_clear()
    context = await _load_context()
    provider = AnthropicProvider()
    with pytest.raises(NotImplementedError):
        await provider.card_selector(_base_payload(), context)


async def test_happy_path_with_mocked_sdk(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock the anthropic.AsyncAnthropic client so no network is touched."""
    _ = seeded_db
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake-real-key")
    get_settings.cache_clear()
    context = await _load_context()

    # Pull one card slug from the seeded catalog so the mocked tool_use input
    # references a real row.
    card = next(iter(context.cards))

    mock_tool_use = SimpleNamespace(
        type="tool_use",
        name="return_selector_stack",
        input={
            "stack": [
                {
                    "card_id": str(card.id),
                    "slug": card.slug,
                    "role": "primary",
                    "monthly_earning_points": 1200,
                    "monthly_earning_thb_equivalent": 150,
                    "annual_fee_thb": (
                        float(card.annual_fee_thb) if card.annual_fee_thb is not None else None
                    ),
                    "reason_th": "เหตุผลหลัก",
                    "reason_en": "Primary reason",
                }
            ],
            "total_monthly_earning_points": 1200,
            "total_monthly_earning_thb_equivalent": 150,
            "months_to_goal": None,
            "with_signup_bonus_months": None,
            "valuation_confidence": 0.8,
            "rationale_th": "ลองใช้บัตรหลัก",
            "rationale_en": "Try the primary card",
            "warnings": [],
        },
    )
    mock_response = SimpleNamespace(
        content=[mock_tool_use],
        usage=SimpleNamespace(
            input_tokens=100,
            output_tokens=50,
            cache_read_input_tokens=80,
            cache_creation_input_tokens=0,
        ),
    )

    class _MockMessages:
        async def create(self, **_kwargs: Any) -> Any:
            return mock_response

    class _MockClient:
        def __init__(self, **_kwargs: Any) -> None:
            self.messages = _MockMessages()

    import anthropic as anthropic_mod

    monkeypatch.setattr(anthropic_mod, "AsyncAnthropic", _MockClient)

    result = await AnthropicProvider().card_selector(_base_payload(), context)
    assert result.llm_model.startswith("claude-sonnet")
    assert result.fallback is False
    assert len(result.stack) == 1
    assert result.stack[0].slug == card.slug
    assert result.rationale_th == "ลองใช้บัตรหลัก"


async def test_invalid_tool_response_raises(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = seeded_db
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake-real-key")
    get_settings.cache_clear()
    context = await _load_context()

    # Response with no tool_use block → parser raises ValueError.
    mock_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="no tool call")],
        usage=SimpleNamespace(
            input_tokens=10,
            output_tokens=5,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )

    class _MockMessages:
        async def create(self, **_kwargs: Any) -> Any:
            return mock_response

    class _MockClient:
        def __init__(self, **_kwargs: Any) -> None:
            self.messages = _MockMessages()

    import anthropic as anthropic_mod

    monkeypatch.setattr(anthropic_mod, "AsyncAnthropic", _MockClient)

    with pytest.raises(ValueError, match="tool_use"):
        await AnthropicProvider().card_selector(_base_payload(), context)
