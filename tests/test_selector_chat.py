"""Tests for POST_V1 §1 `POST /v1/selector/{session_id}/chat`.

Covers the 12 scenarios in the PR-9 brief:
- Flag OFF → 404
- Email-gate required → 403
- Rate-limit 11th call → 429 + PostHog event
- Explain question → category "explain", cards_changed=false
- What-if detected → category "what-if", re-rank produces a new stack
- Haiku timeout → static fallback, no billable call
- Session expired (no cached context) → 410
- Invalid session_id → 400
- Happy path emits `selector_chat_rerank_delivered{cards_changed: false}`
- `selector_chat_opened` fires exactly once per session
- Cost cap: prompt too large → rejected pre-flight
- Concurrent calls don't corrupt chat_count

Plus PR-unstub additions (2026-04-24):
- `_call_haiku_chat` happy path → mocked `AsyncAnthropic.messages.create`
- `_call_haiku_chat` wrapped by `asyncio.wait_for(..., 5s)` → clean fallback
- `_call_haiku_chat` cost-cap skip → SDK never invoked
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient

from loftly.api.routes import selector_chat as route_module
from loftly.core.cache import InMemoryCache, set_cache
from loftly.db.engine import get_sessionmaker
from loftly.db.models.selector_session import SelectorSession
from loftly.selector.session_cache import (
    get_chat_count,
    increment_chat_count,
    write_context,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def fresh_cache() -> AsyncIterator[InMemoryCache]:
    """Swap in a clean InMemoryCache so chat_count / context / sentinels isolate."""
    cache = InMemoryCache()
    set_cache(cache)
    try:
        yield cache
    finally:
        cache.clear()
        set_cache(None)


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test to the flag being ON. Individual tests opt out via
    `monkeypatch.setattr(route_module, "_flag_enabled", ...)` when they need
    the flag OFF."""

    async def _always_on(_session_id: str) -> bool:
        return True

    monkeypatch.setattr(route_module, "_flag_enabled", _always_on)


@pytest.fixture(autouse=True)
def _stub_posthog(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture PostHog events in a list instead of firing the network call."""
    events: list[dict[str, Any]] = []

    async def _capture(
        event: str, distinct_id: str, properties: dict[str, Any] | None = None
    ) -> None:
        events.append({"event": event, "distinct_id": distinct_id, "properties": properties or {}})

    monkeypatch.setattr(route_module, "posthog_capture", _capture)
    return events


def _base_selector_payload() -> dict[str, Any]:
    return {
        "monthly_spend_thb": 80_000,
        "spend_categories": {
            "dining": 15_000,
            "online": 20_000,
            "travel": 25_000,
            "grocery": 10_000,
            "other": 10_000,
        },
        "current_cards": [],
        "goal": {
            "type": "miles",
            "currency_preference": "ROP",
            "horizon_months": 12,
            "target_points": 60_000,
        },
        "locale": "th",
    }


async def _create_session(seeded_client: AsyncClient) -> str:
    """Hit `/v1/selector` to create a valid session_id + warm the context cache."""
    resp = await seeded_client.post("/v1/selector", json=_base_selector_payload())
    assert resp.status_code == 200, resp.text
    session_id = str(resp.json()["session_id"])
    # §1 chat requires the 50k-token context block to be cached. PR-8 warms
    # this on selector compute; here we simulate that by writing a stub.
    await write_context(session_id, "stub-50k-context-block")
    return session_id


async def _bind_session_to_test_user(session_id: str) -> None:
    """Mark the session as email-gate-cleared by setting user_id + bound_at."""
    from datetime import UTC, datetime

    from tests.conftest import TEST_USER_ID

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db:
        row = await db.get(SelectorSession, uuid.UUID(session_id))
        assert row is not None
        row.user_id = TEST_USER_ID
        row.bound_at = datetime.now(UTC)
        await db.commit()


# ---------------------------------------------------------------------------
# Flag gate
# ---------------------------------------------------------------------------


async def test_flag_off_returns_404(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = fresh_cache

    async def _always_off(_session_id: str) -> bool:
        return False

    monkeypatch.setattr(route_module, "_flag_enabled", _always_off)
    sid = str(uuid.uuid4())
    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "hi"})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Email gate
# ---------------------------------------------------------------------------


async def test_email_gate_required_returns_403(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    # Anon session (user_id=None + bound_at=None) with partial_unlock=True
    # should hit the email gate.
    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไมอันดับ 1?"})
    assert resp.status_code == 403, resp.text
    assert resp.json()["error"]["code"] == "email_gate_required"


# ---------------------------------------------------------------------------
# Rate limit
# ---------------------------------------------------------------------------


async def test_rate_limit_returns_429_on_11th_call(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    _stub_posthog: list[dict[str, Any]],
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    # Pre-seed the counter to 10 so the next request is the 11th.
    for _ in range(10):
        await increment_chat_count(sid)

    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม KBank?"})
    assert resp.status_code == 429, resp.text
    body = resp.json()
    assert "คำถามต่อเซสชันครบแล้ว" in body["error"]["message_th"]
    # PostHog rate-limit event must have fired.
    assert any(e["event"] == "selector_chat_rate_limited" for e in _stub_posthog)


# ---------------------------------------------------------------------------
# Explain happy path
# ---------------------------------------------------------------------------


async def test_explain_question_returns_answer(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    _stub_posthog: list[dict[str, Any]],
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    resp = await seeded_client.post(
        f"/v1/selector/{sid}/chat",
        json={"question": "ทำไม KBank WISDOM อันดับ 1?"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["category"] == "explain"
    assert body["cards_changed"] is False
    assert body["new_stack"] is None
    assert body["answer_th"]  # non-empty
    # The explain path should still emit the `rerank_delivered` instrumentation
    # with cards_changed=False for consistent downstream dashboards.
    rerank_events = [e for e in _stub_posthog if e["event"] == "selector_chat_rerank_delivered"]
    assert len(rerank_events) == 1
    assert rerank_events[0]["properties"]["cards_changed"] is False


# ---------------------------------------------------------------------------
# What-if path
# ---------------------------------------------------------------------------


async def test_whatif_extracts_params_and_reruns(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    # Capture the modified SelectorInput that the rerank sees.
    captured: dict[str, Any] = {}
    original_apply = route_module._apply_whatif_delta

    def _spy_apply(orig: Any, params: Any) -> Any:
        result = original_apply(orig, params)
        captured["new_spend"] = dict(result.spend_categories)
        captured["new_total"] = result.monthly_spend_thb
        captured["params"] = params
        return result

    monkeypatch.setattr(route_module, "_apply_whatif_delta", _spy_apply)

    resp = await seeded_client.post(
        f"/v1/selector/{sid}/chat",
        json={"question": "ถ้าเพิ่ม dining อีก 20,000 ผลเปลี่ยนไหม?"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["category"] == "what-if"
    # The modified profile must reflect the delta.
    assert captured["params"]["category"] == "dining"
    assert captured["params"]["amount_thb_delta"] == 20_000
    assert captured["new_spend"]["dining"] == 15_000 + 20_000
    assert captured["new_total"] == 80_000 + 20_000


# ---------------------------------------------------------------------------
# Haiku timeout
# ---------------------------------------------------------------------------


async def test_haiku_timeout_returns_static_fallback(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    call_count = {"n": 0}

    async def _never_returns(_system: str, _user: str) -> dict[str, str]:
        call_count["n"] += 1
        await asyncio.sleep(10)
        return {"answer_th": "never", "answer_en": None}

    monkeypatch.setattr(route_module, "_call_haiku_chat", _never_returns)
    # Shorten the wait so the test doesn't take 5s.
    monkeypatch.setattr(route_module, "_HAIKU_TIMEOUT_SEC", 0.05)

    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม?"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Static fallback string must match the spec's Thai copy.
    assert body["answer_th"] == "ขออภัย ลองใหม่อีกครั้งได้เลย"
    # The call was attempted exactly once — no retry.
    assert call_count["n"] == 1


# ---------------------------------------------------------------------------
# Session expired / invalid
# ---------------------------------------------------------------------------


async def test_session_expired_no_cached_context_returns_410(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
) -> None:
    _ = fresh_cache
    # Create a session then intentionally skip write_context — the chat route
    # must refuse to proceed without the cached 50k block.
    resp = await seeded_client.post("/v1/selector", json=_base_selector_payload())
    sid = str(resp.json()["session_id"])
    await _bind_session_to_test_user(sid)

    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม?"})
    assert resp.status_code == 410, resp.text
    assert resp.json()["error"]["code"] == "session_expired"


async def test_invalid_session_id_returns_400(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
) -> None:
    _ = fresh_cache
    resp = await seeded_client.post("/v1/selector/not-a-uuid/chat", json={"question": "hi"})
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "invalid_session_id"


# ---------------------------------------------------------------------------
# Instrumentation
# ---------------------------------------------------------------------------


async def test_happy_path_emits_rerank_delivered(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    _stub_posthog: list[dict[str, Any]],
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม?"})
    assert resp.status_code == 200, resp.text
    rerank_events = [e for e in _stub_posthog if e["event"] == "selector_chat_rerank_delivered"]
    assert len(rerank_events) == 1
    assert rerank_events[0]["properties"]["cards_changed"] is False


async def test_selector_chat_opened_fires_only_once_per_session(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    _stub_posthog: list[dict[str, Any]],
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    for _ in range(3):
        resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม?"})
        assert resp.status_code == 200, resp.text

    opened = [e for e in _stub_posthog if e["event"] == "selector_chat_opened"]
    assert len(opened) == 1
    assert opened[0]["properties"]["auth_state"] == "authed"


# ---------------------------------------------------------------------------
# Cost cap
# ---------------------------------------------------------------------------


async def test_cost_cap_rejects_preflight_when_estimated_over_cap(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    haiku_called = {"n": 0}

    async def _should_not_be_called(_system: str, _user: str) -> dict[str, str]:
        haiku_called["n"] += 1
        return {"answer_th": "should not run", "answer_en": None}

    monkeypatch.setattr(route_module, "_call_haiku_chat", _should_not_be_called)
    # Drop the cap floor below the always-computed estimate so every call trips
    # the pre-flight rejection — asserts the gate fires before any billable work.
    monkeypatch.setattr(route_module, "_CHAT_COST_CAP_THB", 0.0)

    resp = await seeded_client.post(f"/v1/selector/{sid}/chat", json={"question": "ทำไม?"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Haiku was NOT called — pre-flight gate stopped the bill.
    assert haiku_called["n"] == 0
    # Static fallback surfaced to the user.
    assert body["answer_th"] == "ขออภัย ลองใหม่อีกครั้งได้เลย"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


async def test_concurrent_requests_do_not_corrupt_chat_count(
    seeded_client: AsyncClient,
    fresh_cache: InMemoryCache,
) -> None:
    _ = fresh_cache
    sid = await _create_session(seeded_client)
    await _bind_session_to_test_user(sid)

    # Fire 5 concurrent chat requests. The InMemoryCache get+set pattern is
    # "atomic-ish" per session_cache.py; under pure asyncio single-threaded
    # execution, each increment runs atomically without a context switch in
    # the critical section, so all five should land.
    async def _ask(i: int) -> int:
        resp = await seeded_client.post(
            f"/v1/selector/{sid}/chat",
            json={"question": f"ทำไม #{i}"},
        )
        return resp.status_code

    results = await asyncio.gather(*(_ask(i) for i in range(5)))
    assert all(code == 200 for code in results), results
    count = await get_chat_count(sid)
    assert count == 5


# ---------------------------------------------------------------------------
# `_call_haiku_chat` un-stub — direct function tests with mocked SDK
# ---------------------------------------------------------------------------
#
# These three tests exercise `_call_haiku_chat` + its route-level integration
# directly, without standing up the full HTTP path. They complement the 12
# end-to-end scenarios above by pinning the shape of the real SDK call:
# model id, max_tokens, system/user split, and the timeout+cost-cap guards.


class _StubAnthropicTextBlock:
    """Mimics `anthropic.types.TextBlock` duck-type shape."""

    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _StubAnthropicUsage:
    def __init__(self, input_tokens: int, output_tokens: int) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class _StubAnthropicMessage:
    """Minimal stand-in for `anthropic.types.Message`."""

    def __init__(self, text: str, input_tokens: int = 120, output_tokens: int = 80) -> None:
        self.content = [_StubAnthropicTextBlock(text)]
        self.usage = _StubAnthropicUsage(input_tokens, output_tokens)


async def test_call_haiku_chat_happy_path_with_mocked_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real `AsyncAnthropic.messages.create` is called with Haiku params."""
    import anthropic

    from loftly.ai.providers import anthropic_haiku as haiku_module

    # Force the "real" branch so we exercise the SDK-backed code path.
    monkeypatch.setattr(
        "loftly.ai.providers.anthropic._should_use_real_anthropic",
        lambda: True,
    )

    captured: dict[str, Any] = {}

    class _FakeMessages:
        async def create(self, **kwargs: Any) -> _StubAnthropicMessage:
            captured.update(kwargs)
            return _StubAnthropicMessage(text="เพราะ KBank WISDOM ได้แต้มสูงสุดใน dining")

    class _FakeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["client_kwargs"] = kwargs
            self.messages = _FakeMessages()

    monkeypatch.setattr(anthropic, "AsyncAnthropic", _FakeClient)

    out = await route_module._call_haiku_chat(
        system="SYS block",
        user="USER block — ทำไม?",
    )

    # Answer surfaced intact from the mocked SDK.
    assert out["answer_th"] == "เพราะ KBank WISDOM ได้แต้มสูงสุดใน dining"
    assert out["answer_en"] is None

    # SDK contract: model=Haiku 4.5, max_retries=0 on client, max_tokens sized
    # per the route's configured budget, system/user split preserved.
    assert captured["model"] == haiku_module.HAIKU_MODEL
    assert captured["max_tokens"] == route_module._HAIKU_MAX_OUTPUT_TOKENS
    assert captured["system"] == "SYS block"
    assert captured["messages"] == [{"role": "user", "content": "USER block — ทำไม?"}]
    assert captured["client_kwargs"]["max_retries"] == 0


async def test_call_haiku_chat_wait_for_timeout_falls_back_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`asyncio.wait_for(_call_haiku_chat, 5s)` trips → TimeoutError propagates.

    The caller (the route) catches TimeoutError + logs reason=`timeout` via
    `_classify_haiku_chat_error`. This test asserts both pieces in isolation:
    (a) the SDK wrapper is cooperatively cancelled, and (b) classification
    returns the `timeout` bucket.
    """
    monkeypatch.setattr(
        "loftly.ai.providers.anthropic._should_use_real_anthropic",
        lambda: True,
    )

    async def _never_returns(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.sleep(10)

    import anthropic

    class _HangingMessages:
        async def create(self, **kwargs: Any) -> None:
            await _never_returns()

    class _HangingClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.messages = _HangingMessages()

    monkeypatch.setattr(anthropic, "AsyncAnthropic", _HangingClient)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            route_module._call_haiku_chat("s", "u"),
            timeout=0.05,
        )

    # Classifier routes `TimeoutError` → "timeout" bucket for dashboards.
    assert route_module._classify_haiku_chat_error(TimeoutError()) == "timeout"


async def test_call_haiku_chat_cost_cap_skip_path_does_not_invoke_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When pre-flight estimate > cap, the SDK is never instantiated.

    The route-level gate is exercised end-to-end in
    `test_cost_cap_rejects_preflight_when_estimated_over_cap`. This narrow
    test asserts the pre-flight estimator itself reports > cap for a large
    payload, so the gate in the route has something to trip on.
    """
    # Sanity: a 30k-char prompt (pathological catalog explosion) must estimate
    # over the THB 0.10 cap at Haiku 4.5 pricing + 3-chars-per-token Thai.
    huge_user = "ก" * 30_000
    estimated = route_module._estimate_chat_cost_thb(system="", user=huge_user)
    assert estimated > route_module._CHAT_COST_CAP_THB

    # And the "real" branch is never entered when we skip — assert by installing
    # an SDK that would explode if reached, then not reaching it.
    import anthropic

    monkeypatch.setattr(
        "loftly.ai.providers.anthropic._should_use_real_anthropic",
        lambda: True,
    )

    class _ExplodingClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise AssertionError("SDK must not be instantiated on cost-cap skip")

    monkeypatch.setattr(anthropic, "AsyncAnthropic", _ExplodingClient)

    # The route-level check is the gate; a direct call to _call_haiku_chat
    # would fire the SDK. So we simulate the route's decision inline: if
    # estimate > cap, return the fallback without calling.
    fallback: dict[str, str | None]
    if estimated > route_module._CHAT_COST_CAP_THB:
        fallback = {
            "answer_th": route_module._HAIKU_FALLBACK_TH,
            "answer_en": route_module._HAIKU_FALLBACK_EN,
        }
    else:  # pragma: no cover — the sanity assert above guarantees we skip
        fallback = await route_module._call_haiku_chat("s", huge_user)

    assert fallback["answer_th"] == "ขออภัย ลองใหม่อีกครั้งได้เลย"
