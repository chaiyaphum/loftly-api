"""Consent endpoint tests — SPEC.md §1 + §7."""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.consent import UserConsent


async def test_get_consent_empty_returns_all_false(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/consent")
    assert resp.status_code == 200
    body = resp.json()
    assert "policy_version" in body
    assert body["consents"] == {
        "optimization": False,
        "marketing": False,
        "analytics": False,
        "sharing": False,
    }


async def test_post_consent_writes_row_and_returns_state(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/consent",
        json={
            "purpose": "marketing",
            "granted": True,
            "policy_version": "2026-04-01",
            "source": "account_settings",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["consents"]["marketing"] is True
    assert body["policy_version"] == "2026-04-01"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(UserConsent))).scalars().all())
    assert len(rows) == 1
    assert rows[0].purpose == "marketing"
    assert rows[0].granted is True
    assert rows[0].policy_version == "2026-04-01"
    assert rows[0].source == "account_settings"
    assert rows[0].granted_at is not None


async def test_post_consent_is_append_only(seeded_client: AsyncClient) -> None:
    # First: grant marketing.
    r1 = await seeded_client.post(
        "/v1/consent",
        json={"purpose": "marketing", "granted": True, "policy_version": "v1"},
    )
    assert r1.status_code == 200
    # Then: revoke marketing. Should INSERT a new row, not UPDATE.
    r2 = await seeded_client.post(
        "/v1/consent",
        json={"purpose": "marketing", "granted": False, "policy_version": "v1"},
    )
    assert r2.status_code == 200
    assert r2.json()["consents"]["marketing"] is False

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(UserConsent).where(UserConsent.purpose == "marketing")))
            .scalars()
            .all()
        )
    # Two immutable rows proves append-only semantics.
    assert len(rows) == 2
    assert {r.granted for r in rows} == {True, False}


async def test_post_optimization_false_is_rejected(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/consent",
        json={"purpose": "optimization", "granted": False, "policy_version": "v1"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "consent_optimization_required"
    assert body["error"]["message_en"]
    assert body["error"]["details"] == {"purpose": "optimization"}


async def test_post_optimization_true_is_accepted(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/consent",
        json={
            "purpose": "optimization",
            "granted": True,
            "policy_version": "2026-04-01",
            "source": "onboarding",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["consents"]["optimization"] is True
