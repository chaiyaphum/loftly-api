"""POST /v1/internal/cache-warm tests."""

from __future__ import annotations

from httpx import AsyncClient

from loftly.core.settings import get_settings


async def test_cache_warm_short_circuits_without_key(seeded_client: AsyncClient) -> None:
    """Without ANTHROPIC_API_KEY → warmed:false, no network."""
    settings = get_settings()
    resp = await seeded_client.post(
        "/v1/internal/cache-warm",
        headers={"X-API-Key": settings.jwt_signing_key},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["warmed"] is False
    assert body["reason"] == "anthropic_key_not_configured"


async def test_cache_warm_rejects_missing_api_key(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post("/v1/internal/cache-warm")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"
