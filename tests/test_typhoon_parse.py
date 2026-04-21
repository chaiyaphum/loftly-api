"""Tests for `typhoon_nlu_spend` free-text Selector path (W19).

Covers:
- Happy path: 3 sample Thai inputs → expected structured output (mocked via pytest-httpx).
- Missing key: route returns 501 when `TYPHOON_API_KEY` unset.
- Flag gating: route returns 501 when flag is OFF even with key set.
- Malformed LLM output: route returns 502.
- Timeout: route returns 504.
- Provider unit tests: fraction normalization, stub sentinel, happy-path provider call.

No real network calls — `pytest_httpx` intercepts every `httpx.AsyncClient` request.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pytest_httpx import HTTPXMock

from loftly.ai.providers.typhoon import (
    TYPHOON_MODEL,
    TyphoonMalformedOutputError,
    TyphoonProvider,
    TyphoonUnavailableError,
)
from loftly.core.settings import Settings, get_settings

# --- Fixtures --------------------------------------------------------------


@pytest_asyncio.fixture
async def typhoon_configured() -> AsyncIterator[Settings]:
    """Swap in a fake Typhoon key + clear the `get_settings` cache so the
    route sees it. Restored on teardown.
    """
    settings = get_settings()
    previous_key = settings.typhoon_api_key
    settings.typhoon_api_key = "sk-fake-typhoon"
    try:
        yield settings
    finally:
        settings.typhoon_api_key = previous_key


@pytest_asyncio.fixture
async def flag_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flip the `LOFTLY_TYPHOON_NLU_ENABLED` env override ON.

    This bypasses PostHog and flips the flag deterministically for the test.
    """
    monkeypatch.setenv("LOFTLY_TYPHOON_NLU_ENABLED", "true")


def _sambanova_response(content: str, status_code: int = 200) -> dict[str, object]:
    """Build an OpenAI-compatible chat-completions response envelope."""
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "model": TYPHOON_MODEL,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 500, "completion_tokens": 150, "total_tokens": 650},
    }


# --- Provider-level tests ---------------------------------------------------


async def test_provider_raises_unavailable_when_key_unset() -> None:
    """Stub sentinel / missing key → TyphoonUnavailableError (route maps to 501)."""
    provider = TyphoonProvider()
    with pytest.raises(TyphoonUnavailableError):
        await provider.parse_spend_nlu("ผมใช้จ่ายเดือนละ 80k")


async def test_provider_stub_key_still_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`stub`/`test`/`none` sentinels should behave like an unset key."""
    monkeypatch.setenv("TYPHOON_API_KEY", "stub")
    get_settings.cache_clear()
    provider = TyphoonProvider()
    with pytest.raises(TyphoonUnavailableError):
        await provider.parse_spend_nlu("ทดสอบ")


@pytest.mark.parametrize(
    ("text_th", "model_json", "expected_goal", "expected_dominant"),
    [
        # Case 1: miles-seeker, dining-heavy
        (
            "ผมใช้จ่ายเดือนละ 80k ส่วนใหญ่กินข้าวข้างนอก อยากเก็บไมล์",
            {
                "monthly_spend_thb": 80000,
                "spend_categories": {
                    "dining": 0.6,
                    "online": 0.1,
                    "grocery": 0.1,
                    "travel": 0.05,
                    "petrol": 0.05,
                    "default": 0.1,
                },
                "goal": "miles",
                "confidence": 0.85,
            },
            "miles",
            "dining",
        ),
        # Case 2: cashback-seeker, online-heavy
        (
            "เดือนละแสน ช้อปปิ้งออนไลน์เยอะมาก อยากได้เงินคืน",
            {
                "monthly_spend_thb": 100000,
                "spend_categories": {
                    "dining": 0.1,
                    "online": 0.7,
                    "grocery": 0.05,
                    "travel": 0.05,
                    "petrol": 0.0,
                    "default": 0.1,
                },
                "goal": "cashback",
                "confidence": 0.9,
            },
            "cashback",
            "online",
        ),
        # Case 3: flexible, travel-heavy with drift needing rescale
        (
            "ใช้เดือนละ 50000 เดินทางบ่อย ยังไม่แน่ใจว่าอยากได้อะไร",
            {
                "monthly_spend_thb": 50000,
                # Sum = 1.03 — provider should silently renormalize.
                "spend_categories": {
                    "dining": 0.15,
                    "online": 0.10,
                    "grocery": 0.10,
                    "travel": 0.50,
                    "petrol": 0.08,
                    "default": 0.10,
                },
                "goal": "flexible",
                "confidence": 0.7,
            },
            "flexible",
            "travel",
        ),
    ],
)
async def test_provider_happy_path_parses_three_sample_inputs(
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
    text_th: str,
    model_json: dict[str, object],
    expected_goal: str,
    expected_dominant: str,
) -> None:
    _ = typhoon_configured
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response(json.dumps(model_json, ensure_ascii=False)),
    )

    provider = TyphoonProvider()
    profile, confidence, duration_ms = await provider.parse_spend_nlu(text_th)

    assert profile.goal == expected_goal
    # Dominant category check — whichever the model returned as highest should
    # remain the highest after our ±5% rescale.
    dominant = max(profile.spend_categories.items(), key=lambda kv: kv[1])[0]
    assert dominant == expected_dominant
    # Fractions sum to ~1.0 after normalization.
    assert abs(sum(profile.spend_categories.values()) - 1.0) < 0.001
    assert 0.0 <= confidence <= 1.0
    assert duration_ms >= 0


async def test_provider_malformed_non_json(
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = typhoon_configured
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response("sorry I couldn't parse that"),
    )
    provider = TyphoonProvider()
    with pytest.raises(TyphoonMalformedOutputError):
        await provider.parse_spend_nlu("ทดสอบ")


async def test_provider_tolerates_code_fence(
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    """Typhoon sometimes wraps JSON in ```json despite being told not to."""
    _ = typhoon_configured
    payload = {
        "monthly_spend_thb": 30000,
        "spend_categories": {"dining": 0.5, "default": 0.5},
        "goal": "cashback",
        "confidence": 0.8,
    }
    content = f"```json\n{json.dumps(payload)}\n```"
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response(content),
    )
    provider = TyphoonProvider()
    profile, _conf, _ms = await provider.parse_spend_nlu("เดือนละ 30000")
    assert profile.monthly_spend_thb == 30000


async def test_provider_retries_once_on_503(
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = typhoon_configured
    # First call: 503. Second call: success.
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        status_code=503,
        json={"error": "Service Unavailable"},
    )
    good_payload = {
        "monthly_spend_thb": 60000,
        "spend_categories": {"dining": 0.3, "online": 0.3, "default": 0.4},
        "goal": "miles",
        "confidence": 0.75,
    }
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response(json.dumps(good_payload)),
    )
    provider = TyphoonProvider()
    profile, _conf, _ms = await provider.parse_spend_nlu("เดือนละ 60000 กินข้าว ช้อป")
    assert profile.monthly_spend_thb == 60000


async def test_provider_timeout_propagates(
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = typhoon_configured
    httpx_mock.add_exception(httpx.ReadTimeout("slow upstream"))
    provider = TyphoonProvider()
    with pytest.raises(httpx.TimeoutException):
        await provider.parse_spend_nlu("ทดสอบ timeout")


# --- Route-level tests ------------------------------------------------------


async def test_route_returns_501_when_flag_off(seeded_client: AsyncClient) -> None:
    """Default (no flag, no key) → 501 Not Implemented."""
    resp = await seeded_client.post(
        "/v1/selector/parse-nlu",
        json={"text_th": "ผมใช้จ่ายเดือนละ 80k อยากเก็บไมล์"},
    )
    assert resp.status_code == 501, resp.text
    body = resp.json()
    assert body["error"]["code"] == "typhoon_nlu_disabled"


async def test_route_returns_501_when_flag_on_but_key_unset(
    seeded_client: AsyncClient,
    flag_enabled: None,
) -> None:
    """Flag ON but `TYPHOON_API_KEY` unset → still 501 (provider raises unavailable)."""
    _ = flag_enabled
    resp = await seeded_client.post(
        "/v1/selector/parse-nlu",
        json={"text_th": "ผมใช้จ่ายเดือนละ 80k"},
    )
    assert resp.status_code == 501, resp.text
    assert resp.json()["error"]["code"] == "typhoon_nlu_disabled"


async def test_route_happy_path(
    seeded_client: AsyncClient,
    flag_enabled: None,
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = flag_enabled, typhoon_configured
    payload = {
        "monthly_spend_thb": 80000,
        "spend_categories": {
            "dining": 0.6,
            "online": 0.1,
            "grocery": 0.1,
            "travel": 0.05,
            "petrol": 0.05,
            "default": 0.1,
        },
        "goal": "miles",
        "confidence": 0.85,
    }
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response(json.dumps(payload, ensure_ascii=False)),
    )

    resp = await seeded_client.post(
        "/v1/selector/parse-nlu",
        json={"text_th": "ผมใช้จ่ายเดือนละ 80k ส่วนใหญ่กินข้าวข้างนอก อยากเก็บไมล์"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["profile"]["monthly_spend_thb"] == 80000
    assert body["profile"]["goal"] == "miles"
    assert body["confidence"] == pytest.approx(0.85)
    assert body["model"] == TYPHOON_MODEL
    assert isinstance(body["duration_ms"], int)


async def test_route_502_on_malformed_llm_output(
    seeded_client: AsyncClient,
    flag_enabled: None,
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = flag_enabled, typhoon_configured
    httpx_mock.add_response(
        url="https://api.sambanova.ai/v1/chat/completions",
        json=_sambanova_response("I don't know how to answer this question."),
    )
    resp = await seeded_client.post(
        "/v1/selector/parse-nlu",
        json={"text_th": "ประโยคนี้ไม่มีข้อมูลเลย"},
    )
    assert resp.status_code == 502, resp.text
    body = resp.json()
    assert body["error"]["code"] == "typhoon_malformed_output"
    # Safe error message — no upstream content leaks into the user-facing string.
    assert "don't know" not in body["error"]["message_en"]


async def test_route_504_on_timeout(
    seeded_client: AsyncClient,
    flag_enabled: None,
    typhoon_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = flag_enabled, typhoon_configured
    httpx_mock.add_exception(httpx.ReadTimeout("timeout"))
    resp = await seeded_client.post(
        "/v1/selector/parse-nlu",
        json={"text_th": "ทดสอบ timeout"},
    )
    assert resp.status_code == 504, resp.text
    assert resp.json()["error"]["code"] == "typhoon_timeout"
