"""Tests for `POST /v1/selector/{id}/archive` — POST_V1 §3 "ทำ Selector ใหม่" CTA.

Covers:
- Valid session_id with existing meta → 200 `{archived:true}` + key renamed
- Nonexistent session_id → 200 `{archived:false}` (idempotent, not 404)
- Rate limit (11th req / 60s) → 429
- Post-archive GET `/recent?session_id=…` → `{expired:true}`
- Direct `GET /v1/selector/{id}` (DB-backed) still works after archive —
  documents the §3 acceptance criterion: results page recoverable for 24h.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from httpx import AsyncClient

from loftly.api.routes.selector import issue_session_token
from loftly.core.cache import InMemoryCache, get_cache
from loftly.core.settings import get_settings
from loftly.selector.session_cache import SessionMeta, read_session_meta, write_session_meta


def _base_payload() -> dict[str, object]:
    """Same profile shape as test_selector.py; kept local to keep tests standalone."""
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


async def test_archive_existing_session_returns_true_and_renames_key(
    seeded_client: AsyncClient,
) -> None:
    session_id = str(uuid.uuid4())
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="KBank WISDOM",
            card_id="kbank-wisdom-id",
            profile_hash="hash-1",
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )

    resp = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"archived": True}

    # Source key gone.
    assert await read_session_meta(session_id) is None

    # Archived key exists under the archived namespace.
    cache = get_cache()
    assert isinstance(cache, InMemoryCache)
    archived_keys = [
        k for k in cache._store if k.startswith(f"selector:session:archived:{session_id}:")
    ]
    assert len(archived_keys) == 1, f"expected exactly one archived key, got {archived_keys}"


async def test_archive_nonexistent_session_returns_false(seeded_client: AsyncClient) -> None:
    """Idempotent: archiving a never-seen session is a no-op, not a 404.

    The frontend will call this blindly on "ทำ Selector ใหม่" — it shouldn't
    have to care whether archive already happened in a prior tab.
    """
    unknown_id = str(uuid.uuid4())
    resp = await seeded_client.post(f"/v1/selector/{unknown_id}/archive")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"archived": False}


async def test_archive_invalid_uuid_returns_422(seeded_client: AsyncClient) -> None:
    """FastAPI path-param UUID validation rejects a malformed id with 422."""
    resp = await seeded_client.post("/v1/selector/not-a-uuid/archive")
    assert resp.status_code == 422


async def test_archive_rate_limit_429_on_11th_request(seeded_client: AsyncClient) -> None:
    """10/min cap per IP. 11th request in the window → 429."""
    for i in range(10):
        sid = str(uuid.uuid4())
        resp = await seeded_client.post(f"/v1/selector/{sid}/archive")
        assert resp.status_code == 200, f"request {i} should not be rate-limited: {resp.text}"
    blocked = await seeded_client.post(f"/v1/selector/{uuid.uuid4()}/archive")
    assert blocked.status_code == 429, blocked.text
    assert blocked.json()["error"]["code"] == "rate_limited"


async def test_archive_then_recent_returns_expired(seeded_client: AsyncClient) -> None:
    """After archive, GET /recent returns `expired:true`.

    This is the happy path exercised by the §3 "ทำ Selector ใหม่" CTA:
    archive hides the session from the returning-user hero on next landing.
    """
    session_id = str(uuid.uuid4())
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="SCB PRIME",
            card_id="scb-prime-id",
            profile_hash="hash-2",
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )

    archive_resp = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert archive_resp.status_code == 200
    assert archive_resp.json()["archived"] is True

    recent_resp = await seeded_client.get(
        "/v1/selector/recent",
        params={"session_id": session_id},
    )
    assert recent_resp.status_code == 200
    assert recent_resp.json()["expired"] is True


async def test_archive_does_not_break_direct_results_link(
    seeded_client: AsyncClient,
) -> None:
    """Post-archive, `GET /v1/selector/{id}?token=…` still works via Postgres.

    §3 acceptance: "the prior Redis profile is archived (not deleted) —
    recoverable via `/selector/results/[id]` direct link for the full 24h".

    The Redis rename affects only the returning-user hero; the
    `selector_sessions` row lives in Postgres and is read independently.
    """
    # Create a real session via the full flow (writes DB row + meta).
    submit = await seeded_client.post("/v1/selector", json=_base_payload())
    assert submit.status_code == 200, submit.text
    session_id = submit.json()["session_id"]

    # Archive it.
    archive_resp = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert archive_resp.status_code == 200

    # Direct GET with valid token still works.
    settings = get_settings()
    token = issue_session_token(uuid.UUID(session_id), settings)
    resp = await seeded_client.get(f"/v1/selector/{session_id}", params={"token": token})
    assert resp.status_code == 200, resp.text
    assert resp.json()["session_id"] == session_id


async def test_archive_is_idempotent_second_call_returns_false(
    seeded_client: AsyncClient,
) -> None:
    """Calling archive twice: first=true, second=false. No error either way."""
    session_id = str(uuid.uuid4())
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="KTC X",
            card_id="ktc-x-id",
            profile_hash="hash-3",
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )

    r1 = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert r1.json() == {"archived": True}

    r2 = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert r2.status_code == 200
    assert r2.json() == {"archived": False}
