"""Week 14 — Sentry noise-drop policy.

We don't actually spin up the Sentry transport; instead we exercise the
`before_send` / `before_send_transaction` helpers directly. That's the
recommended pattern from the Sentry docs for unit-level coverage and avoids
leaking events to a real DSN in CI.
"""

from __future__ import annotations

import random
from typing import Any

import pytest

from loftly.core.settings import get_settings
from loftly.observability.sentry import (
    _resolve_traces_sample_rate,
    init_sentry,
    should_drop_event,
    should_drop_transaction,
)

# --- AbortError / CancelledError ---------------------------------------------


class AbortError(Exception):
    """Stand-in for fetch-style AbortError that might bubble up from httpx."""


def test_abort_error_dropped_before_send() -> None:
    event: dict[str, Any] = {"exception": {"values": [{"type": "AbortError"}]}}
    assert should_drop_event(event, None) is True


def test_cancelled_error_dropped_via_hint() -> None:
    import asyncio

    event: dict[str, Any] = {"exception": {"values": [{"type": "CancelledError"}]}}
    hint = {"exc_info": (asyncio.CancelledError, asyncio.CancelledError(), None)}
    assert should_drop_event(event, hint) is True


def test_client_disconnect_dropped() -> None:
    event: dict[str, Any] = {"exception": {"values": [{"type": "ClientDisconnect"}]}}
    assert should_drop_event(event, None) is True


# --- health-probe transactions -----------------------------------------------


@pytest.mark.parametrize("path", ["/healthz", "/readyz", "/metrics"])
def test_health_route_transaction_dropped(path: str) -> None:
    event: dict[str, Any] = {"transaction": path, "request": {"url": path}}
    assert should_drop_transaction(event) is True


def test_real_route_transaction_kept() -> None:
    event: dict[str, Any] = {"transaction": "/v1/cards", "request": {"url": "/v1/cards"}}
    assert should_drop_transaction(event) is False


def test_full_url_extracted_correctly() -> None:
    # Sentry may hand us an absolute URL; our path extractor must still match.
    event: dict[str, Any] = {"request": {"url": "https://api.loftly.co.th/healthz"}}
    assert should_drop_transaction(event) is True


# --- 4xx / 5xx sampling ------------------------------------------------------


def test_5xx_always_kept() -> None:
    event: dict[str, Any] = {"contexts": {"response": {"status_code": 500}}}
    # Run many times so a miswired `random.random()` gate would be caught.
    for _ in range(50):
        assert should_drop_event(event, None) is False


def test_4xx_sampled_at_roughly_10pct() -> None:
    event: dict[str, Any] = {"contexts": {"response": {"status_code": 404}}}
    rng = random.Random(0xC0FFEE)
    random.seed(0xC0FFEE)  # make the module-level random.random() deterministic too
    # Patch module random via seed — we just check the coarse ratio.
    kept = 0
    total = 2_000
    for _ in range(total):
        # Reseed between calls would defeat the test; instead run a long roll
        # and assert the kept ratio is in a loose band around 10%.
        if not should_drop_event(event, None):
            kept += 1
    ratio = kept / total
    # Loose band — we only care that the drop is real and roughly targets 10%.
    assert 0.06 <= ratio <= 0.14, f"4xx kept ratio={ratio:.3f} outside 6–14% band"
    # Silence unused-var lint (we keep rng around for future extensions).
    assert rng is not None


def test_non_http_event_kept_by_default() -> None:
    event: dict[str, Any] = {"message": "hello"}
    assert should_drop_event(event, None) is False


# --- traces_sample_rate env resolution ---------------------------------------


def test_traces_sample_rate_defaults_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOFTLY_SENTRY_TRACES_SAMPLE", raising=False)
    monkeypatch.setenv("LOFTLY_ENV", "prod")
    monkeypatch.setenv("JWT_SIGNING_KEY", "prod-not-the-default-placeholder")
    get_settings.cache_clear()
    settings = get_settings()
    assert _resolve_traces_sample_rate(settings) == pytest.approx(0.1)


def test_traces_sample_rate_staging_full_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOFTLY_SENTRY_TRACES_SAMPLE", raising=False)
    monkeypatch.setenv("LOFTLY_ENV", "staging")
    get_settings.cache_clear()
    settings = get_settings()
    assert _resolve_traces_sample_rate(settings) == pytest.approx(1.0)


def test_traces_sample_rate_env_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOFTLY_ENV", "prod")
    monkeypatch.setenv("JWT_SIGNING_KEY", "prod-not-the-default-placeholder")
    monkeypatch.setenv("LOFTLY_SENTRY_TRACES_SAMPLE", "0.42")
    get_settings.cache_clear()
    settings = get_settings()
    assert _resolve_traces_sample_rate(settings) == pytest.approx(0.42)


def test_traces_sample_rate_invalid_value_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOFTLY_ENV", "prod")
    monkeypatch.setenv("JWT_SIGNING_KEY", "prod-not-the-default-placeholder")
    monkeypatch.setenv("LOFTLY_SENTRY_TRACES_SAMPLE", "not-a-number")
    get_settings.cache_clear()
    settings = get_settings()
    assert _resolve_traces_sample_rate(settings) == pytest.approx(0.1)


# --- init_sentry end-to-end (no real network) -------------------------------


def test_init_sentry_noop_without_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    get_settings.cache_clear()
    settings = get_settings()
    assert init_sentry(settings) is False


def test_init_sentry_sets_ignore_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """init_sentry() with a DSN should call into sentry_sdk.init with our policy."""
    captured: dict[str, Any] = {}

    def _fake_init(**kwargs: Any) -> None:
        captured.update(kwargs)

    import sentry_sdk

    monkeypatch.setattr(sentry_sdk, "init", _fake_init)
    monkeypatch.setenv("SENTRY_DSN", "https://public@fake.ingest.sentry.io/123")
    get_settings.cache_clear()
    settings = get_settings()

    assert init_sentry(settings) is True
    assert "before_send" in captured
    assert "before_send_transaction" in captured
    assert "asyncio.CancelledError" in captured["ignore_errors"]
    assert "starlette.requests.ClientDisconnect" in captured["ignore_errors"]
    # The drop policy is exercised via the injected hook.
    drop_hook = captured["before_send"]
    abort_event = {"exception": {"values": [{"type": "AbortError"}]}}
    assert drop_hook(abort_event, None) is None
    # 5xx should survive.
    kept = drop_hook({"contexts": {"response": {"status_code": 503}}}, None)
    assert kept is not None
    # Transaction hook drops health probes.
    tx_hook = captured["before_send_transaction"]
    assert tx_hook({"request": {"url": "/healthz"}}, None) is None
