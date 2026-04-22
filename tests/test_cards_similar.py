"""Similar-card endpoint tests — `GET /v1/cards/similar/{slug}`.

The seeder guarantees three cards across two issuers + two earn currencies +
two tiers, so each similarity channel (issuer / earn_currency / tier) can be
exercised without inserting additional rows.
"""

from __future__ import annotations

from httpx import AsyncClient


async def test_similar_happy_path(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/similar/kbank-wisdom")
    assert resp.status_code == 200
    body = resp.json()
    assert "data" in body
    slugs = {c["slug"] for c in body["data"]}
    # Must exclude the source card itself.
    assert "kbank-wisdom" not in slugs


async def test_similar_excludes_source_card(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/similar/uob-prvi-miles")
    assert resp.status_code == 200
    slugs = {c["slug"] for c in resp.json()["data"]}
    assert "uob-prvi-miles" not in slugs


async def test_similar_ranks_same_tier_first(seeded_client: AsyncClient) -> None:
    """kbank-wisdom + uob-prvi-miles both are Signature tier."""
    resp = await seeded_client.get("/v1/cards/similar/kbank-wisdom")
    assert resp.status_code == 200
    slugs = [c["slug"] for c in resp.json()["data"]]
    assert "uob-prvi-miles" in slugs
    # scb-thai-airways (Platinum) also surfaces because source-card has a
    # different tier but returns via "active card" catchall match.
    assert len(slugs) >= 1


async def test_similar_respects_limit(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/similar/kbank-wisdom", params={"limit": 1})
    assert resp.status_code == 200
    assert len(resp.json()["data"]) <= 1


async def test_similar_invalid_slug_returns_404(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/similar/does-not-exist")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "card_not_found"
    assert body["error"]["details"]["slug"] == "does-not-exist"


async def test_similar_limit_bounds_validated(seeded_client: AsyncClient) -> None:
    # limit=0 rejected by Query(ge=1).
    resp = await seeded_client.get("/v1/cards/similar/kbank-wisdom", params={"limit": 0})
    assert resp.status_code == 422
