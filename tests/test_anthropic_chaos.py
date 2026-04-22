"""Haiku/Sonnet fallback-chain chaos tests — W16.

Exercises `_run_with_fallback` under controlled provider failures and asserts:
- the correct model in the chain wins
- `used_fallback` / `used_deterministic` flags are stamped correctly
- `fallback_reason` is classified accurately (upstream_503, timeout,
  rate_limit, both_failed, cost_cap)

No real network is touched — we drive failure modes by either:
1. Monkey-patching `provider.card_selector` to raise a specific SDK exception
2. Using `respx` to intercept HTTP-level calls (Anthropic SDK → respx → 503)

Pairs with the production changes in:
- `src/loftly/schemas/selector.py`  (new SelectorResult fields)
- `src/loftly/api/routes/selector.py::_run_with_fallback`
- `src/loftly/ai/providers/anthropic*.py` (max_retries=0)
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import anthropic
import httpx
import pytest
import respx

from loftly.ai import SelectorContext, set_provider
from loftly.ai.providers.anthropic import AnthropicProvider
from loftly.api.routes.selector import (
    _HAIKU_COST_CAP_THB,
    _classify_sonnet_error,
    _run_with_fallback,
)
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.schemas.selector import SelectorInput, SelectorResult

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


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
            "goal": {"type": "miles", "currency_preference": "ROP"},
            "locale": "th",
        }
    )


async def _load_context() -> SelectorContext:
    from loftly.api.routes.selector import _load_context as _load

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return await _load(session)


def _fake_request() -> httpx.Request:
    return httpx.Request("POST", "https://api.anthropic.com/v1/messages")


def _fake_response(status_code: int) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        request=_fake_request(),
        json={"type": "error", "error": {"type": "error", "message": "boom"}},
    )


def _sonnet_503() -> anthropic.InternalServerError:
    return anthropic.InternalServerError("upstream 503", response=_fake_response(503), body=None)


def _sonnet_429() -> anthropic.RateLimitError:
    return anthropic.RateLimitError("rate limited", response=_fake_response(429), body=None)


def _sonnet_sdk_timeout() -> anthropic.APITimeoutError:
    return anthropic.APITimeoutError(request=_fake_request())


def _build_mock_haiku_result(card_id: str, slug: str) -> SelectorResult:
    """Construct a valid-looking Haiku SelectorResult the route can forward."""
    return SelectorResult.model_validate(
        {
            "session_id": "anthropic_haiku",
            "stack": [
                {
                    "card_id": card_id,
                    "slug": slug,
                    "role": "primary",
                    "monthly_earning_points": 1200,
                    "monthly_earning_thb_equivalent": 150,
                    "annual_fee_thb": None,
                    "reason_th": "บัตรหลัก Haiku",
                    "reason_en": "Primary pick (Haiku)",
                }
            ],
            "total_monthly_earning_points": 1200,
            "total_monthly_earning_thb_equivalent": 150,
            "months_to_goal": None,
            "with_signup_bonus_months": None,
            "valuation_confidence": 0.7,
            "rationale_th": "คำอธิบาย Haiku",
            "rationale_en": "Haiku rationale",
            "warnings": [],
            "llm_model": "claude-haiku-4-5-20251001",
            "fallback": True,
            "partial_unlock": False,
        }
    )


def _build_mock_sonnet_result(card_id: str, slug: str) -> SelectorResult:
    """Construct a valid-looking Sonnet SelectorResult (no fallback flags)."""
    return SelectorResult.model_validate(
        {
            "session_id": "anthropic",
            "stack": [
                {
                    "card_id": card_id,
                    "slug": slug,
                    "role": "primary",
                    "monthly_earning_points": 1500,
                    "monthly_earning_thb_equivalent": 200,
                    "annual_fee_thb": None,
                    "reason_th": "บัตรหลัก Sonnet",
                    "reason_en": "Primary pick (Sonnet)",
                }
            ],
            "total_monthly_earning_points": 1500,
            "total_monthly_earning_thb_equivalent": 200,
            "months_to_goal": None,
            "with_signup_bonus_months": None,
            "valuation_confidence": 0.85,
            "rationale_th": "คำอธิบาย Sonnet",
            "rationale_en": "Sonnet rationale",
            "warnings": [],
            "llm_model": "claude-sonnet-4-6",
            "fallback": False,
            "partial_unlock": False,
        }
    )


@pytest.fixture
def activate_anthropic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the route in 'provider.name == anthropic' mode so fallback kicks in."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake-real-key")
    monkeypatch.setenv("LOFTLY_LLM_PROVIDER", "anthropic")
    get_settings.cache_clear()
    set_provider(AnthropicProvider())


# ---------------------------------------------------------------------------
# Case a — Sonnet 503 → Haiku wins, reason=upstream_503
# ---------------------------------------------------------------------------


async def test_a_sonnet_503_falls_back_to_haiku(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    sonnet_mock = AsyncMock(side_effect=_sonnet_503())
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(return_value=_build_mock_haiku_result(str(card.id), card.slug))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.used_deterministic is False
    assert result.fallback_reason == "upstream_503"
    assert result.llm_model == "claude-haiku-4-5-20251001"
    sonnet_mock.assert_awaited_once()
    haiku_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Case b — Sonnet asyncio.TimeoutError → Haiku wins, reason=timeout
# ---------------------------------------------------------------------------


async def test_b_sonnet_asyncio_timeout_falls_back_to_haiku(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    async def _hang(*_args: Any, **_kwargs: Any) -> None:
        # Sleep longer than the patched timeout below — asyncio.wait_for will
        # cancel us. The short patched timeout keeps the test fast.
        await asyncio.sleep(5)

    monkeypatch.setattr(AnthropicProvider, "card_selector", _hang)
    monkeypatch.setattr("loftly.api.routes.selector._SONNET_TIMEOUT_SEC", 0.05)

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(return_value=_build_mock_haiku_result(str(card.id), card.slug))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.used_deterministic is False
    assert result.fallback_reason == "timeout"
    assert result.llm_model == "claude-haiku-4-5-20251001"


# ---------------------------------------------------------------------------
# Case c — Sonnet SDK APITimeoutError → Haiku wins, reason=timeout
# ---------------------------------------------------------------------------


async def test_c_sonnet_sdk_timeout_falls_back_to_haiku(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    sonnet_mock = AsyncMock(side_effect=_sonnet_sdk_timeout())
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(return_value=_build_mock_haiku_result(str(card.id), card.slug))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.fallback_reason == "timeout"
    assert result.llm_model == "claude-haiku-4-5-20251001"


# ---------------------------------------------------------------------------
# Case d — Sonnet + Haiku both fail → deterministic, used_deterministic=True
# ---------------------------------------------------------------------------


async def test_d_both_fail_lands_on_deterministic(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()

    sonnet_mock = AsyncMock(side_effect=_sonnet_503())
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(side_effect=_sonnet_503())
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.used_deterministic is True
    assert result.fallback_reason == "both_failed"
    assert result.llm_model == "deterministic"
    assert any("AI temporarily unavailable" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Case e — Sonnet 429 → retry once → still 429 → Haiku wins, reason=rate_limit
# ---------------------------------------------------------------------------


async def test_e_sonnet_429_retry_then_haiku(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    sonnet_mock = AsyncMock(side_effect=_sonnet_429())
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)
    # Keep backoff short so the test is fast but still exercises the retry path.
    monkeypatch.setattr("loftly.api.routes.selector._SONNET_RETRY_BACKOFF_SEC", 0.01)

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(return_value=_build_mock_haiku_result(str(card.id), card.slug))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.fallback_reason == "rate_limit"
    assert result.llm_model == "claude-haiku-4-5-20251001"
    # Exactly one retry → Sonnet invoked twice total.
    assert sonnet_mock.await_count == 2


# ---------------------------------------------------------------------------
# Case f — Sonnet 429 → retry once → success → no fallback
# ---------------------------------------------------------------------------


async def test_f_sonnet_429_retry_succeeds(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    sonnet_mock = AsyncMock(
        side_effect=[_sonnet_429(), _build_mock_sonnet_result(str(card.id), card.slug)]
    )
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)
    monkeypatch.setattr("loftly.api.routes.selector._SONNET_RETRY_BACKOFF_SEC", 0.01)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is False
    assert result.used_deterministic is False
    assert result.fallback_reason is None
    assert result.llm_model == "claude-sonnet-4-6"
    assert sonnet_mock.await_count == 2


# ---------------------------------------------------------------------------
# Case g — Haiku cost cap → deterministic, reason=cost_cap
# ---------------------------------------------------------------------------


async def test_g_haiku_cost_cap_skips_to_deterministic(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()

    # Sonnet fails → would normally hand off to Haiku.
    sonnet_mock = AsyncMock(side_effect=_sonnet_503())
    monkeypatch.setattr(AnthropicProvider, "card_selector", sonnet_mock)

    # Force the cost estimate over the cap so Haiku is skipped.
    monkeypatch.setattr(
        "loftly.api.routes.selector._estimate_haiku_cost_thb",
        lambda _ctx: _HAIKU_COST_CAP_THB + 0.10,
    )

    # Make sure Haiku is NOT called — fail loudly if it is.
    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(side_effect=AssertionError("Haiku should be skipped by cost cap"))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.used_deterministic is True
    assert result.fallback_reason == "cost_cap"
    assert result.llm_model == "deterministic"
    haiku_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Case h — Sonnet HTTP 503 via respx → Haiku wins
# ---------------------------------------------------------------------------


async def test_h_sonnet_http_503_via_respx(
    seeded_db: object,
    activate_anthropic: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP-level chaos: real anthropic SDK call → respx returns 503.

    Asserts the classifier correctly maps the SDK's `InternalServerError` to
    `upstream_503` without relying on us constructing the exception manually.
    """
    _ = seeded_db
    _ = activate_anthropic
    context = await _load_context()
    card = next(iter(context.cards))

    from loftly.ai.providers import anthropic_haiku

    haiku_mock = AsyncMock(return_value=_build_mock_haiku_result(str(card.id), card.slug))
    monkeypatch.setattr(anthropic_haiku.AnthropicHaikuProvider, "card_selector", haiku_mock)

    # respx intercepts the outgoing POST to api.anthropic.com and serves 503.
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://api.anthropic.com/v1/messages").mock(
            return_value=httpx.Response(
                503,
                json={
                    "type": "error",
                    "error": {"type": "overloaded_error", "message": "upstream unavailable"},
                },
            )
        )
        result = await _run_with_fallback(_base_payload(), context)

    assert result.used_fallback is True
    assert result.fallback_reason == "upstream_503"
    assert result.llm_model == "claude-haiku-4-5-20251001"
    haiku_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Classifier unit tests — cheap and fast, pin the error → reason mapping.
# ---------------------------------------------------------------------------


def test_classify_asyncio_timeout() -> None:
    assert _classify_sonnet_error(TimeoutError("deadline")) == "timeout"


def test_classify_sdk_timeout() -> None:
    assert _classify_sonnet_error(_sonnet_sdk_timeout()) == "timeout"


def test_classify_rate_limit() -> None:
    assert _classify_sonnet_error(_sonnet_429()) == "rate_limit"


def test_classify_503() -> None:
    assert _classify_sonnet_error(_sonnet_503()) == "upstream_503"


def test_classify_other() -> None:
    assert _classify_sonnet_error(ValueError("boom")) == "both_failed"
