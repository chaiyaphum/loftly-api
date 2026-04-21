"""Feature-flag evaluator tests — fallback path + PostHog happy path + admin route."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pytest_httpx import HTTPXMock

from loftly.core.feature_flags import FeatureFlags
from loftly.core.settings import Settings, get_settings


@pytest_asyncio.fixture
async def posthog_configured() -> AsyncIterator[Settings]:
    """Override the PostHog key just for the test body, then restore."""
    settings = get_settings()
    previous = settings.posthog_project_api_key
    settings.posthog_project_api_key = "phc_test_key"
    try:
        yield settings
    finally:
        settings.posthog_project_api_key = previous


async def test_is_enabled_returns_default_when_key_unset() -> None:
    flags = FeatureFlags()
    # Test env has posthog_project_api_key unset — we never hit the network.
    assert await flags.is_enabled("anything", "user-1", default=False) is False
    assert await flags.is_enabled("anything", "user-1", default=True) is True


async def test_variant_returns_default_when_key_unset() -> None:
    flags = FeatureFlags()
    assert await flags.variant("selector_cta_copy", "user-1") == "control"
    assert await flags.variant("selector_cta_copy", "user-1", default="variant_a") == "variant_a"


async def test_is_enabled_falls_back_on_network_error(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    # Simulate a PostHog outage by raising from the mock transport.
    httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
    flags = FeatureFlags()
    result = await flags.is_enabled("selector_streaming", "user-1", default=True)
    # No throw — falls back to the provided default.
    assert result is True


async def test_variant_falls_back_on_http_500(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    httpx_mock.add_response(status_code=500, json={"error": "boom"})
    flags = FeatureFlags()
    result = await flags.variant("selector_cta_copy", "user-1", default="control")
    assert result == "control"


async def test_variant_returns_posthog_value(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    httpx_mock.add_response(
        json={
            "featureFlags": {
                "selector_cta_copy": "variant_a",
                "some_bool_flag": True,
            }
        }
    )
    flags = FeatureFlags()
    assert await flags.variant("selector_cta_copy", "user-1") == "variant_a"


async def test_is_enabled_reads_bool_flag(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    httpx_mock.add_response(json={"featureFlags": {"selector_streaming": True}})
    flags = FeatureFlags()
    assert await flags.is_enabled("selector_streaming", "user-1") is True


async def test_is_enabled_unknown_flag_uses_default(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    httpx_mock.add_response(json={"featureFlags": {}})
    flags = FeatureFlags()
    assert await flags.is_enabled("missing_flag", "user-1", default=True) is True


async def test_is_enabled_malformed_response_uses_default(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
) -> None:
    _ = posthog_configured
    # PostHog returning a non-JSON body — we must not crash.
    httpx_mock.add_response(
        content=b"not-json",
        headers={"content-type": "text/plain"},
    )
    flags = FeatureFlags()
    assert await flags.is_enabled("anything", "user-1", default=False) is False


async def test_admin_flags_endpoint_lists_known_flags(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    resp = await seeded_client.get("/v1/admin/feature-flags", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["posthog_configured"] is False  # test env has no key set
    keys = [f["key"] for f in body["flags"]]
    assert "selector_cta_copy" in keys
    assert "landing_hero_cta" in keys
    # Probe value falls back to "control" when PostHog isn't configured.
    cta = next(f for f in body["flags"] if f["key"] == "selector_cta_copy")
    assert cta["probe_value"] == "control"
    # `landing_hero_cta` is also multivariate with "control" as its default, so
    # the probe falls back to "control" in the unconfigured-PostHog test env.
    hero = next(f for f in body["flags"] if f["key"] == "landing_hero_cta")
    assert hero["type"] == "multivariate"
    assert hero["probe_value"] == "control"
    assert hero["expected_variants"] == ["control", "variant_benefit_led", "variant_urgency"]


async def test_admin_flags_requires_admin(
    seeded_client: AsyncClient,
) -> None:
    # No auth header at all → 401.
    resp = await seeded_client.get("/v1/admin/feature-flags")
    assert resp.status_code == 401


@pytest.mark.parametrize(
    ("payload_variant", "expected"),
    [
        ("control", "control"),
        ("variant_a", "variant_a"),
        (False, "control"),
    ],
)
async def test_variant_bool_false_falls_back(
    posthog_configured: Settings,
    httpx_mock: HTTPXMock,
    payload_variant: object,
    expected: str,
) -> None:
    _ = posthog_configured
    httpx_mock.add_response(json={"featureFlags": {"selector_cta_copy": payload_variant}})
    flags = FeatureFlags()
    assert await flags.variant("selector_cta_copy", "user-1", default="control") == expected
