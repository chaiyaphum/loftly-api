"""Tests for `GET /v1/selector/recent` — POST_V1 §3 returning-user landing API.

Covers:
- No `session_id` → 200 `{expired:true, ...null}`
- Valid `session_id` + fresh meta → 200 with all four fields populated
- Valid `session_id` + expired (absent) meta → 200 `{expired:true, ...null}`
- Rate limit (31st req / 60s) → 429
- Malformed UUID → 400

Uses `seeded_client` because the app boot-path imports the cache + settings the
route reaches into. `InMemoryCache` is the test-default cache (no Redis).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient

from loftly.core.cache import set_cache
from loftly.selector.session_cache import SessionMeta, write_session_meta


def _fake_session_id() -> str:
    """Stable-enough UUID for test assertions."""
    return str(uuid.uuid4())


async def test_recent_no_session_id_returns_expired(seeded_client: AsyncClient) -> None:
    """No cookie → 200 `{expired:true, ...null}`. No 404 — fetch log stays clean."""
    resp = await seeded_client.get("/v1/selector/recent")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "card_name": None,
        "card_id": None,
        "hours_since_last_session": None,
        "expired": True,
    }


async def test_recent_with_fresh_meta_returns_all_fields(seeded_client: AsyncClient) -> None:
    """Happy path — written meta round-trips through the HTTP layer."""
    session_id = _fake_session_id()
    # Write meta with a timestamp that will yield a predictable hours delta.
    last_seen = (datetime.now(UTC) - timedelta(hours=12, minutes=18)).isoformat()
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="KBank WISDOM",
            card_id="kbank-wisdom-id",
            profile_hash="deadbeef",
            last_seen_at=last_seen,
        ),
    )

    resp = await seeded_client.get("/v1/selector/recent", params={"session_id": session_id})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["card_name"] == "KBank WISDOM"
    assert body["card_id"] == "kbank-wisdom-id"
    assert body["expired"] is False
    # hours_since_last_session computed from `last_seen_at`; allow slack for
    # test-runner wall clock drift.
    assert 12.0 <= body["hours_since_last_session"] <= 13.0


async def test_recent_with_expired_meta_returns_expired(seeded_client: AsyncClient) -> None:
    """Session_id provided but no meta in cache → 200 `{expired:true}`, not 404."""
    # Use a well-formed UUID the cache has never seen.
    resp = await seeded_client.get(
        "/v1/selector/recent",
        params={"session_id": _fake_session_id()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["expired"] is True
    assert body["card_name"] is None
    assert body["card_id"] is None
    assert body["hours_since_last_session"] is None


async def test_recent_invalid_uuid_returns_400(seeded_client: AsyncClient) -> None:
    """Malformed UUID is a frontend bug — surface it, don't swallow as expired."""
    resp = await seeded_client.get(
        "/v1/selector/recent",
        params={"session_id": "not-a-uuid"},
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "invalid_session_id"


async def test_recent_rate_limit_429_on_31st_request(seeded_client: AsyncClient) -> None:
    """30/min cap per IP — the 31st request in a window returns 429."""
    # 30 requests — all should succeed.
    for i in range(30):
        resp = await seeded_client.get("/v1/selector/recent")
        assert resp.status_code == 200, f"request {i} should not be rate-limited: {resp.text}"
    # 31st → 429.
    blocked = await seeded_client.get("/v1/selector/recent")
    assert blocked.status_code == 429, blocked.text
    assert blocked.json()["error"]["code"] == "rate_limited"


async def test_recent_does_not_leak_pii(seeded_client: AsyncClient) -> None:
    """Regression guard: response must not surface profile_hash or any PII field.

    The SessionMeta shape intentionally holds `profile_hash` for §1 chat
    invalidation; the /recent response must omit it so it can never accidentally
    end up in a client-side log.
    """
    session_id = _fake_session_id()
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="SCB PRIME",
            card_id="scb-prime-id",
            profile_hash="secret-not-for-client",
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )

    resp = await seeded_client.get("/v1/selector/recent", params={"session_id": session_id})
    body = resp.json()
    assert "profile_hash" not in body
    assert "secret-not-for-client" not in resp.text


async def test_recent_after_archive_returns_expired(seeded_client: AsyncClient) -> None:
    """Document the archive flow end-to-end: archive → GET /recent is `expired:true`."""
    session_id = _fake_session_id()
    await write_session_meta(
        session_id,
        SessionMeta(
            card_name="KTC Forever",
            card_id="ktc-forever-id",
            profile_hash="abc",
            last_seen_at=datetime.now(UTC).isoformat(),
        ),
    )

    archive_resp = await seeded_client.post(f"/v1/selector/{session_id}/archive")
    assert archive_resp.status_code == 200
    assert archive_resp.json() == {"archived": True}

    get_resp = await seeded_client.get("/v1/selector/recent", params={"session_id": session_id})
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["expired"] is True
    assert body["card_name"] is None


async def test_recent_empty_string_session_id_returns_expired(seeded_client: AsyncClient) -> None:
    """Empty-string `?session_id=` (some browsers omit `value` differently) → expired."""
    _ = set_cache  # keep the import used for future cache swaps in this module
    resp = await seeded_client.get("/v1/selector/recent", params={"session_id": ""})
    assert resp.status_code == 200, resp.text
    assert resp.json()["expired"] is True
