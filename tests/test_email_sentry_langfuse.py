"""Resend email stub + Sentry / Langfuse gating."""

from __future__ import annotations

import pytest

from loftly.core.settings import get_settings
from loftly.notifications.email import send_magic_link
from loftly.observability.langfuse import init_langfuse, observe_llm
from loftly.observability.sentry import init_sentry


async def test_send_magic_link_stub_logs_only(capsys: pytest.CaptureFixture[str]) -> None:
    """No RESEND_API_KEY → no import, no send — just structlog event.

    structlog writes through PrintLoggerFactory → stdout, so we check capsys
    rather than caplog (which only hooks stdlib handlers).
    """
    await send_magic_link("u@example.com", "https://x/magic?t=abc", locale="th")
    out = capsys.readouterr().out
    assert "magic_link_email_stub" in out


async def test_send_magic_link_real_invokes_resend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESEND_API_KEY", "fake-resend-key")
    get_settings.cache_clear()

    import resend  # type: ignore[import-untyped]

    calls: list[dict[str, object]] = []

    class _FakeEmails:
        @staticmethod
        def send(payload: dict[str, object]) -> dict[str, object]:
            calls.append(payload)
            return {"id": "email_fake_id"}

    monkeypatch.setattr(resend, "Emails", _FakeEmails)

    await send_magic_link("user@example.com", "https://loftly/m?t=xyz", locale="en")
    assert len(calls) == 1
    assert calls[0]["to"] == ["user@example.com"]
    assert "Loftly" in str(calls[0]["subject"])


def test_sentry_init_noop_without_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    get_settings.cache_clear()
    settings = get_settings()
    assert init_sentry(settings) is False


def test_langfuse_init_noop_without_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_HOST", raising=False)
    get_settings.cache_clear()
    settings = get_settings()
    assert init_langfuse(settings) is False


async def test_observe_llm_decorator_still_runs_the_function(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Langfuse off → decorator just times + logs, doesn't break the call."""

    @observe_llm("test-selector")
    async def _fake_call(x: int) -> int:
        return x * 2

    result = await _fake_call(3)
    assert result == 6
    out = capsys.readouterr().out
    assert "llm_call" in out
