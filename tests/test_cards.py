"""Card catalog tests — hits the in-memory fixture from cards.py."""

from __future__ import annotations

from httpx import AsyncClient


async def test_list_cards_returns_fixture(client: AsyncClient) -> None:
    resp = await client.get("/v1/cards")
    assert resp.status_code == 200
    body = resp.json()
    assert "data" in body and "pagination" in body
    assert len(body["data"]) >= 2
    slugs = {c["slug"] for c in body["data"]}
    assert {"kbank-wisdom", "ktc-x-infinite"}.issubset(slugs)


async def test_list_cards_filters_by_issuer(client: AsyncClient) -> None:
    resp = await client.get("/v1/cards", params={"issuer": "kbank"})
    assert resp.status_code == 200
    body = resp.json()
    assert all(c["bank"]["slug"] == "kbank" for c in body["data"])
    assert any(c["slug"] == "kbank-wisdom" for c in body["data"])


async def test_card_detail_ok(client: AsyncClient) -> None:
    resp = await client.get("/v1/cards/kbank-wisdom")
    assert resp.status_code == 200
    body = resp.json()
    assert body["slug"] == "kbank-wisdom"
    assert body["network"] == "Visa"
    assert body["bank"]["display_name_th"] == "กสิกรไทย"


async def test_card_detail_404_has_error_envelope(client: AsyncClient) -> None:
    resp = await client.get("/v1/cards/does-not-exist")
    assert resp.status_code == 404
    body = resp.json()
    # openapi.yaml#Error shape
    assert "error" in body
    assert body["error"]["code"] == "card_not_found"
    assert body["error"]["message_en"]
    assert body["error"]["details"] == {"slug": "does-not-exist"}


async def test_selector_stub_returns_fallback_envelope(client: AsyncClient) -> None:
    payload = {
        "monthly_spend_thb": 80000,
        "spend_categories": {
            "dining": 15000,
            "online": 20000,
            "travel": 25000,
            "grocery": 10000,
            "other": 10000,
        },
        "current_cards": [],
        "goal": {"type": "miles", "currency_preference": "ROP", "horizon_months": 12},
        "locale": "th",
    }
    resp = await client.post("/v1/selector", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["fallback"] is True
    assert body["llm_model"] == "stub"
    assert len(body["stack"]) >= 1
