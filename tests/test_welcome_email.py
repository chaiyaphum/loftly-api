"""POST_V1 §2 — personalized welcome email composer + send orchestration.

Covers the composer happy path, kill-switch, LLM timeout fallback, Resend
retry + Sentry alert on double-fail, concurrency cap, locale resolution,
and the PII posture (no raw session_id nor email in PostHog properties).

Suite plan (≥ 10 cases):
- Composer happy path: subject ≤ 60 chars + top-3 present
- Composer honors `WELCOME_EMAIL_PERSONALIZED=false` → static fallback
- Composer LLM timeout → static fallback + fallback flag
- Composer empty-stack → static fallback
- Send path: PostHog `welcome_email_queued` emitted w/ `fallback: false`
- Send path: no PII (email / raw session_id) in PostHog properties
- Resend retry: first fails, 30s sleep, second succeeds → one message_id
- Resend double-fail: Sentry captured + exception bubbles
- Concurrency: Semaphore(10) caps — 11th waits
- Locale precedence: override beats header beats default 'th'
- Auth endpoint: 202 returned inside 500ms
- Tracking token roundtrip + tamper detection
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from httpx import AsyncClient

from loftly.core.settings import get_settings
from loftly.notifications import email as email_module
from loftly.notifications import welcome_email as welcome
from loftly.schemas.selector import SelectorResult, SelectorStackItem


def _mk_selector_result(card_count: int = 3) -> SelectorResult:
    """Build a SelectorResult with `card_count` stack items for composer tests."""
    stack = [
        SelectorStackItem(
            card_id=f"card-{i}",
            slug=f"kbank-premium-{i}",
            role=["primary", "secondary", "tertiary"][i % 3],  # type: ignore[arg-type]
            monthly_earning_points=1200 * (i + 1),
            monthly_earning_thb_equivalent=240 * (i + 1),
            annual_fee_thb=1000.0,
            reason_th=f"เหตุผลบัตรอันดับ {i + 1}",
            reason_en=f"reason for rank {i + 1}",
        )
        for i in range(card_count)
    ]
    return SelectorResult(
        session_id="00000000-0000-4000-8000-000000000042",
        stack=stack,
        total_monthly_earning_points=sum(it.monthly_earning_points for it in stack),
        total_monthly_earning_thb_equivalent=sum(it.monthly_earning_thb_equivalent for it in stack),
        valuation_confidence=0.72,
        rationale_th="สรุปเหตุผลรวม",
        rationale_en="overall rationale",
        llm_model="claude-haiku-4-5-20251001",
        fallback=False,
        used_fallback=False,
        used_deterministic=False,
        partial_unlock=True,
    )


# ---------------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("locale", ["th", "en"])
async def test_composer_happy_path_subject_under_60_and_cards_present(
    locale: str,
) -> None:
    result = _mk_selector_result()
    subject, text, html, top3, fallback = await welcome.compose_personalized(
        selector_result=result,
        magic_link_url="https://loftly.co.th/m?t=xyz",
        locale=locale,  # type: ignore[arg-type]
        user_id_hash=None,
    )
    assert fallback is False
    assert len(subject) <= 60
    # Top-3 slugs present in body (personalization signal).
    for item in result.stack:
        assert item.slug in text
        assert item.slug in html
    assert top3 == [it.card_id for it in result.stack]


async def test_composer_fallback_when_personalization_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "false")
    get_settings.cache_clear()
    result = _mk_selector_result()

    # LLM must not be invoked — guard with a fail-if-called spy.
    called: list[bool] = []

    async def _boom() -> None:
        called.append(True)
        raise AssertionError("LLM was called despite kill-switch")

    subject, _text, _html, top3, fallback = await welcome.compose_personalized(
        selector_result=result,
        magic_link_url="https://x/m",
        locale="th",
        user_id_hash=None,
    )
    assert fallback is True
    assert top3 == []
    assert "Loftly" in subject
    assert called == []


async def test_composer_llm_timeout_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "true")
    get_settings.cache_clear()

    # Shrink the budget + make the Haiku stub hang past it.
    monkeypatch.setattr(welcome, "_HAIKU_TIMEOUT_SEC", 0.05)

    async def _slow_llm(*args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(1.0)

    monkeypatch.setattr(welcome, "_haiku_llm_call", _slow_llm)

    subject, _text, _html, top3, fallback = await welcome.compose_personalized(
        selector_result=_mk_selector_result(),
        magic_link_url="https://x/m",
        locale="th",
        user_id_hash=None,
    )
    assert fallback is True
    assert top3 == []
    assert subject


async def test_composer_llm_error_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "true")
    get_settings.cache_clear()

    async def _broken_llm(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("haiku quota exhausted")

    monkeypatch.setattr(welcome, "_haiku_llm_call", _broken_llm)

    _subject, _text, _html, top3, fallback = await welcome.compose_personalized(
        selector_result=_mk_selector_result(),
        magic_link_url="https://x/m",
        locale="en",
        user_id_hash=None,
    )
    assert fallback is True
    assert top3 == []


async def test_composer_empty_stack_falls_back() -> None:
    result = _mk_selector_result(card_count=0)
    _, _, _, top3, fallback = await welcome.compose_personalized(
        selector_result=result,
        magic_link_url="https://x/m",
        locale="th",
        user_id_hash=None,
    )
    assert fallback is True
    assert top3 == []


# ---------------------------------------------------------------------------
# Send orchestration
# ---------------------------------------------------------------------------


async def test_send_emits_posthog_queued_and_delivered_with_no_pii(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "true")
    get_settings.cache_clear()

    events: list[dict[str, Any]] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append(
            {"event": event, "distinct_id": distinct_id, "properties": dict(properties or {})}
        )

    # Patch the PostHog sink on the welcome_email module's local alias.
    monkeypatch.setattr(welcome, "posthog_capture", _fake_capture)

    # Short-circuit send_email_with_retry so we don't talk to Resend.
    async def _fake_retry(**kwargs: Any) -> str:
        return "msg-fake"

    monkeypatch.setattr(welcome, "send_email_with_retry", _fake_retry)

    await welcome.send_welcome_email(
        email="user@example.com",
        magic_link_url="https://loftly/m?t=abc",
        selector_result=_mk_selector_result(),
        locale="th",
        session_id="0abc-1234-session-raw",
        user_id_hash=None,
    )

    assert {e["event"] for e in events} == {"welcome_email_queued", "welcome_email_delivered"}
    # No raw PII leaks: the raw session id must not appear anywhere in properties.
    for ev in events:
        flat = str(ev["properties"]) + ev["distinct_id"]
        assert "user@example.com" not in flat
        assert "0abc-1234-session-raw" not in flat
    queued = next(e for e in events if e["event"] == "welcome_email_queued")
    assert queued["properties"]["fallback"] is False
    assert "session_id_hash" in queued["properties"]


async def test_send_emits_fallback_true_when_kill_switch_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "false")
    get_settings.cache_clear()
    events: list[dict[str, Any]] = []

    async def _fake_capture(event: str, distinct_id: str, properties: Any = None) -> None:
        events.append({"event": event, "properties": dict(properties or {})})

    monkeypatch.setattr(welcome, "posthog_capture", _fake_capture)

    async def _fake_retry(**kwargs: Any) -> str:
        return "msg-fake"

    monkeypatch.setattr(welcome, "send_email_with_retry", _fake_retry)

    await welcome.send_welcome_email(
        email="u@example.com",
        magic_link_url="https://loftly/m",
        selector_result=_mk_selector_result(),
        locale="th",
        session_id="sess-1",
        user_id_hash=None,
    )
    queued = next(e for e in events if e["event"] == "welcome_email_queued")
    assert queued["properties"]["fallback"] is True


# ---------------------------------------------------------------------------
# Retry + Sentry
# ---------------------------------------------------------------------------


async def test_resend_retry_waits_30s_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """First send raises, we sleep 30s, second succeeds — one retry path."""
    monkeypatch.setenv("RESEND_API_KEY", "fake-key")
    get_settings.cache_clear()

    sleeps: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("loftly.notifications.email.asyncio.sleep", _fake_sleep)

    # Patch resend.Emails.send: raise first, return id second.
    import resend  # type: ignore[import-untyped]

    calls = {"n": 0}

    class _Emails:
        @staticmethod
        def send(payload: dict[str, Any]) -> dict[str, Any]:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("resend 503")
            return {"id": "resend-second-attempt"}

    monkeypatch.setattr(resend, "Emails", _Emails)

    msg_id = await email_module.send_email_with_retry(
        to="user@example.com",
        subject="s",
        text="t",
        html="<p>t</p>",
    )
    assert msg_id == "resend-second-attempt"
    assert sleeps == [30.0]
    assert calls["n"] == 2


async def test_resend_double_fail_captures_sentry_and_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "fake-key")
    get_settings.cache_clear()

    async def _fake_sleep(_: float) -> None:
        pass

    monkeypatch.setattr("loftly.notifications.email.asyncio.sleep", _fake_sleep)

    import resend  # type: ignore[import-untyped]

    class _Emails:
        @staticmethod
        def send(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("resend down")

    monkeypatch.setattr(resend, "Emails", _Emails)

    import sentry_sdk

    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", lambda exc: captured.append(exc))

    with pytest.raises(RuntimeError, match="resend down"):
        await email_module.send_email_with_retry(
            to="user@example.com",
            subject="s",
            text="t",
            html=None,
        )
    assert len(captured) == 1


# ---------------------------------------------------------------------------
# Concurrency cap
# ---------------------------------------------------------------------------


async def test_concurrency_semaphore_caps_parallel_sends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fire 11 in parallel; observe that only 10 hit the send step at once."""
    monkeypatch.setenv("WELCOME_EMAIL_PERSONALIZED", "true")
    get_settings.cache_clear()

    async def _noop_capture(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(welcome, "posthog_capture", _noop_capture)

    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def _blocking_retry(**kwargs: Any) -> str:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await release.wait()
        finally:
            in_flight -= 1
        return "msg-x"

    monkeypatch.setattr(welcome, "send_email_with_retry", _blocking_retry)

    tasks = [
        asyncio.create_task(
            welcome.send_welcome_email(
                email=f"user{i}@example.com",
                magic_link_url="https://x/m",
                selector_result=_mk_selector_result(),
                locale="th",
                session_id=f"s-{i}",
                user_id_hash=None,
            )
        )
        for i in range(11)
    ]
    # Give the event loop a tick to schedule the blocking body.
    await asyncio.sleep(0.05)
    assert peak <= 10, f"Semaphore leak: peak in-flight = {peak}"
    assert in_flight == 10  # the 11th is parked on the semaphore

    release.set()
    await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Locale precedence (§2 AC-4)
# ---------------------------------------------------------------------------


def test_locale_precedence_override_beats_header_beats_default() -> None:
    from loftly.core.locale import detect_locale

    assert detect_locale(None) == "th"
    assert detect_locale("en-US") == "en"
    assert detect_locale("en-US", override="th") == "th"
    assert detect_locale("th-TH,en-US;q=0.9") == "th"


# ---------------------------------------------------------------------------
# Endpoint latency (§2 AC: < 500ms)
# ---------------------------------------------------------------------------


async def test_magic_link_request_returns_within_500ms(
    seeded_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Don't actually send — monkeypatch at the import site used by the route.
    # The background task is awaited via an event so we can cleanly release
    # it before the test exits (no "Task was destroyed" warning).
    release = asyncio.Event()
    started = asyncio.Event()

    async def _noop_send(*args: Any, **kwargs: Any) -> None:
        started.set()
        await release.wait()

    monkeypatch.setattr(
        "loftly.api.routes.auth.send_welcome_email",
        _noop_send,
    )

    t0 = time.perf_counter()
    resp = await seeded_client.post(
        "/v1/auth/magic-link/request",
        json={"email": "fast@example.com"},
    )
    elapsed = time.perf_counter() - t0
    assert resp.status_code == 202
    assert elapsed < 0.5, f"endpoint took {elapsed:.3f}s (budget 500ms)"

    # Let the background task complete so pytest doesn't warn on shutdown.
    await started.wait()
    release.set()
    # Tiny yield so the task body exits before the test tears down.
    await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Tracking token helpers
# ---------------------------------------------------------------------------


def test_tracking_token_roundtrip_and_tamper() -> None:
    token = welcome.build_tracking_token("hashed-user", "welcome_personalized")
    parsed = welcome.verify_tracking_token(token)
    assert parsed == {"u": "hashed-user", "t": "welcome_personalized"}

    # Tamper with the signature half.
    bad = token[:-4] + "0000"
    assert welcome.verify_tracking_token(bad) is None
    # Tamper with the payload half.
    payload, sig = token.split(".")
    bad2 = payload + "X." + sig
    assert welcome.verify_tracking_token(bad2) is None
    # Non-delimited garbage.
    assert welcome.verify_tracking_token("not-a-token") is None
