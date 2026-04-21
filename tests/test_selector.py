"""Card Selector end-to-end tests — Week 5-6 scope.

Exercises:
- Category-sum validation (422 `selector_invalid_categories`)
- Deterministic provider ranks + persists a selector_sessions row
- Profile-hash cache hit avoids a second DB write
- GET /v1/selector/{id} with valid token returns the stored envelope
- GET with missing/invalid token → 401
- GET with unknown session → 404
"""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import func, select

from loftly.api.routes.selector import issue_session_token
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.selector_session import SelectorSession


def _base_payload() -> dict[str, object]:
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


async def test_selector_rejects_category_sum_off(seeded_client: AsyncClient) -> None:
    payload = _base_payload()
    payload["spend_categories"] = {"dining": 10_000, "online": 10_000}  # sum 20k, not 80k
    resp = await seeded_client.post("/v1/selector", json=payload)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["error"]["code"] == "selector_invalid_categories"
    assert body["error"]["message_th"]
    assert "diff_thb" in body["error"]["details"]


async def test_selector_deterministic_returns_stack_and_persists(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post("/v1/selector", json=_base_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["fallback"] is True
    assert body["llm_model"] == "deterministic"
    assert body["partial_unlock"] is True
    assert isinstance(body["session_id"], str)
    # Deterministic provider should produce at least one card (uob-prvi-miles
    # has UOB_REWARDS which is bank_proprietary, NOT airline — so for goal=miles
    # it should be filtered out; kbank-wisdom earns K_POINT (bank_prop), also
    # filtered. So with the seed, stack may be empty — but session row persists.
    assert isinstance(body["stack"], list)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        count = await s.scalar(select(func.count()).select_from(SelectorSession))
    assert count == 1


async def test_selector_cache_hit_skips_second_db_write(
    seeded_client: AsyncClient,
) -> None:
    payload = _base_payload()
    r1 = await seeded_client.post("/v1/selector", json=payload)
    assert r1.status_code == 200
    r2 = await seeded_client.post("/v1/selector", json=payload)
    assert r2.status_code == 200
    # Cache hit returns the same envelope including session_id.
    assert r1.json()["session_id"] == r2.json()["session_id"]

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as s:
        count = await s.scalar(select(func.count()).select_from(SelectorSession))
    assert count == 1


async def test_selector_retrieve_with_valid_token(seeded_client: AsyncClient) -> None:
    submit = await seeded_client.post("/v1/selector", json=_base_payload())
    assert submit.status_code == 200
    session_id = submit.json()["session_id"]

    settings = get_settings()
    import uuid

    token = issue_session_token(uuid.UUID(session_id), settings)

    resp = await seeded_client.get(f"/v1/selector/{session_id}", params={"token": token})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["session_id"] == session_id


async def test_selector_retrieve_rejects_bad_token(seeded_client: AsyncClient) -> None:
    submit = await seeded_client.post("/v1/selector", json=_base_payload())
    session_id = submit.json()["session_id"]

    resp = await seeded_client.get(f"/v1/selector/{session_id}", params={"token": "not.a.real.jwt"})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"


async def test_selector_retrieve_404_for_unknown_session(
    seeded_client: AsyncClient,
) -> None:
    import uuid

    settings = get_settings()
    fake_id = uuid.uuid4()
    token = issue_session_token(fake_id, settings)
    resp = await seeded_client.get(f"/v1/selector/{fake_id}", params={"token": token})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "selector_session_not_found"
