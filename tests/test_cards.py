"""Card catalog tests — DB-backed after Week 2.

The `seeded_client` fixture runs the idempotent catalog seeder (2 sample cards:
`kbank-wisdom`, `uob-prvi-miles`) against the in-memory SQLite test DB.
"""

from __future__ import annotations

from httpx import AsyncClient


async def test_list_cards_returns_seeded_rows(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards")
    assert resp.status_code == 200
    body = resp.json()
    assert "data" in body and "pagination" in body
    assert len(body["data"]) >= 2
    slugs = {c["slug"] for c in body["data"]}
    assert {"kbank-wisdom", "uob-prvi-miles"}.issubset(slugs)


async def test_list_cards_filters_by_issuer(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards", params={"issuer": "kbank"})
    assert resp.status_code == 200
    body = resp.json()
    assert all(c["bank"]["slug"] == "kbank" for c in body["data"])
    assert any(c["slug"] == "kbank-wisdom" for c in body["data"])


async def test_list_cards_filters_by_network(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards", params={"network": "Visa"})
    assert resp.status_code == 200
    body = resp.json()
    assert all(c["network"] == "Visa" for c in body["data"])


async def test_list_cards_cursor_pagination(seeded_client: AsyncClient) -> None:
    # Force a page size of 1 so cursor logic kicks in.
    resp = await seeded_client.get("/v1/cards", params={"limit": 1})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 1
    assert body["pagination"]["has_more"] is True
    cursor = body["pagination"]["cursor_next"]
    assert cursor

    resp2 = await seeded_client.get("/v1/cards", params={"limit": 1, "cursor": cursor})
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert len(body2["data"]) == 1
    # Second page card must differ from first.
    assert body["data"][0]["slug"] != body2["data"][0]["slug"]


async def test_list_cards_invalid_cursor_returns_400(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.get("/v1/cards", params={"cursor": "not-base64!!"})
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "invalid_cursor"


async def test_card_detail_ok(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/kbank-wisdom")
    assert resp.status_code == 200
    body = resp.json()
    assert body["slug"] == "kbank-wisdom"
    assert body["network"] == "Visa"
    assert body["bank"]["display_name_th"] == "กสิกรไทย"


async def test_card_detail_404_has_error_envelope(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/cards/does-not-exist")
    assert resp.status_code == 404
    body = resp.json()
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
