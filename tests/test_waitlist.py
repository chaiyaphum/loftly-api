"""Waitlist endpoints — W24 scope.

Covers:
- POST /v1/waitlist happy path → 201 + body
- Duplicate (email, source) → 204 (idempotent re-join)
- Invalid email → 422 via shared validation handler
- Rate-limit blocks the 11th call from the same IP → 429
- ip_hash / user_agent_hash are stored hashed, never raw (grep-check the DB row)
- Audit row is written with no raw email in the metadata
- GET /v1/admin/waitlist requires admin JWT; returns paginated rows
- Admin list `source` filter works
"""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from loftly.api.routes.waitlist import reset_limiter as reset_waitlist_limiter
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.db.models.waitlist import Waitlist


async def test_join_happy_path_returns_201_and_persists_row(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/waitlist",
        json={
            "email": "first@loftly.example",
            "variant": "B",
            "tier": "premium",
            "monthly_price_thb": 149,
            "source": "pricing",
            "meta": {"experiment": "price-test-1"},
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["source"] == "pricing"
    assert body["id"] >= 1
    assert body["created_at"]

    # Row in DB.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(Waitlist))).scalars().all())
    assert len(rows) == 1
    row = rows[0]
    assert row.email == "first@loftly.example"
    assert row.variant == "B"
    assert row.tier == "premium"
    assert row.monthly_price_thb == 149
    assert row.source == "pricing"
    assert row.meta == {"experiment": "price-test-1"}


async def test_join_duplicate_email_source_returns_204(
    seeded_client: AsyncClient,
) -> None:
    first = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "dup@loftly.example", "source": "pricing"},
    )
    assert first.status_code == 201

    second = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "dup@loftly.example", "source": "pricing"},
    )
    assert second.status_code == 204
    assert second.content == b""

    # Only one row persisted.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(Waitlist))).scalars().all())
    assert len(rows) == 1


async def test_join_same_email_different_source_is_not_duplicate(
    seeded_client: AsyncClient,
) -> None:
    r1 = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "both@loftly.example", "source": "pricing"},
    )
    assert r1.status_code == 201
    r2 = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "both@loftly.example", "source": "coming-soon"},
    )
    assert r2.status_code == 201

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(Waitlist))).scalars().all())
    assert len(rows) == 2


async def test_join_invalid_email_returns_422(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "definitely-not-an-email"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_join_rate_limit_blocks_11th_call(seeded_client: AsyncClient) -> None:
    reset_waitlist_limiter()
    # 10 distinct emails so the rate-limit triggers (not the dedupe path).
    for i in range(10):
        r = await seeded_client.post(
            "/v1/waitlist",
            json={"email": f"rl-{i}@loftly.example", "source": "pricing"},
        )
        assert r.status_code == 201, r.text

    over = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "rl-11@loftly.example", "source": "pricing"},
    )
    assert over.status_code == 429
    assert over.json()["error"]["code"] == "rate_limited"


async def test_join_hashes_ip_and_user_agent(seeded_client: AsyncClient) -> None:
    """Raw IP / UA must never land in the DB — dedupe/forensics on hashes only."""
    raw_ua = "Mozilla/5.0 (Test) PII-SHOULD-BE-HASHED"
    resp = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "pii@loftly.example", "source": "pricing"},
        headers={"User-Agent": raw_ua},
    )
    assert resp.status_code == 201

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = (
            await session.execute(
                select(Waitlist).where(Waitlist.email == "pii@loftly.example")
            )
        ).scalar_one()

    assert row.ip_hash is not None
    assert row.user_agent_hash is not None
    # 64-char lowercase hex — the SHA-256 shape, not the raw string.
    assert len(row.ip_hash) == 64
    assert len(row.user_agent_hash) == 64
    assert all(c in "0123456789abcdef" for c in row.ip_hash)
    assert all(c in "0123456789abcdef" for c in row.user_agent_hash)
    assert raw_ua not in row.user_agent_hash
    # And across the whole row serialization, the raw UA string is absent.
    serialized = " ".join(
        str(v) for v in (row.email, row.ip_hash or "", row.user_agent_hash or "")
    )
    assert "PII-SHOULD-BE-HASHED" not in serialized


async def test_join_writes_audit_row_without_raw_email(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/waitlist",
        json={
            "email": "audited@loftly.example",
            "variant": "A",
            "tier": "premium",
            "monthly_price_thb": 199,
            "source": "pricing",
        },
    )
    assert resp.status_code == 201

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        audit_rows = list(
            (
                await session.execute(
                    select(AuditLog).where(AuditLog.action == "waitlist.joined")
                )
            )
            .scalars()
            .all()
        )
    assert len(audit_rows) == 1
    row = audit_rows[0]
    assert row.subject_type == "waitlist"
    # Payload carries segmentation keys but NOT the email.
    assert row.meta["source"] == "pricing"
    assert row.meta["variant"] == "A"
    assert row.meta["tier"] == "premium"
    assert row.meta["monthly_price_thb"] == 199
    # Serialize the whole audit row and grep for the raw email — must be absent.
    blob = str(row.meta) + str(row.subject_type) + str(row.action)
    assert "audited@loftly.example" not in blob


# ---------------------------------------------------------------------------
# Admin list
# ---------------------------------------------------------------------------


async def test_admin_list_requires_admin_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/waitlist")
    assert resp.status_code == 401


async def test_admin_list_returns_paginated_rows(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    # Seed 3 entries across two sources.
    for i in range(2):
        r = await seeded_client.post(
            "/v1/waitlist",
            json={"email": f"p{i}@loftly.example", "source": "pricing"},
        )
        assert r.status_code == 201
    r3 = await seeded_client.post(
        "/v1/waitlist",
        json={"email": "cs@loftly.example", "source": "coming-soon"},
    )
    assert r3.status_code == 201

    # Unfiltered list returns all 3.
    resp = await seeded_client.get(
        "/v1/admin/waitlist",
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 3
    assert len(body["data"]) == 3
    assert body["limit"] == 100
    assert body["offset"] == 0
    assert body["has_more"] is False
    # Raw email is surfaced to admins (that's the point of the export).
    assert any(row["email"] == "p0@loftly.example" for row in body["data"])

    # Source filter narrows to 2.
    resp = await seeded_client.get(
        "/v1/admin/waitlist?source=pricing",
        headers=admin_headers,
    )
    body = resp.json()
    assert body["total"] == 2
    assert {row["email"] for row in body["data"]} == {
        "p0@loftly.example",
        "p1@loftly.example",
    }

    # Pagination via limit/offset.
    page1 = await seeded_client.get(
        "/v1/admin/waitlist?limit=1&offset=0",
        headers=admin_headers,
    )
    assert page1.status_code == 200
    p1_body = page1.json()
    assert len(p1_body["data"]) == 1
    assert p1_body["has_more"] is True
    assert p1_body["total"] == 3

    page2 = await seeded_client.get(
        "/v1/admin/waitlist?limit=1&offset=2",
        headers=admin_headers,
    )
    assert page2.status_code == 200
    p2_body = page2.json()
    assert len(p2_body["data"]) == 1
    assert p2_body["has_more"] is False


async def test_admin_list_rejects_non_admin(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get("/v1/admin/waitlist", headers=user_headers)
    assert resp.status_code == 403
