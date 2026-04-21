"""Affiliate postback webhook — HMAC-signed partner conversions."""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid

import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import (
    AffiliateClick,
    AffiliateConversion,
    AffiliateLink,
)
from loftly.db.models.card import Card as CardModel

_PARTNER = "test-partner"
_SECRET = "shhh-test-secret"  # matches conftest AFFILIATE_PARTNER_SECRETS


def _sign(body: bytes) -> str:
    digest = hmac.new(_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


async def _make_click() -> uuid.UUID:
    """Create a click row so the webhook has a real click_id to attach to."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one()
        link = AffiliateLink(
            card_id=card_id,
            partner_id=_PARTNER,
            url_template="https://partner.example.com/apply?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        await session.flush()

        click_id = uuid.uuid4()
        session.add(
            AffiliateClick(
                click_id=click_id,
                affiliate_link_id=link.id,
                card_id=card_id,
                partner_id=_PARTNER,
                placement="review",
            )
        )
        await session.commit()
    return click_id


@pytest_asyncio.fixture
async def click_id(seeded_db: object) -> uuid.UUID:
    _ = seeded_db
    return await _make_click()


async def test_webhook_valid_signature_creates_conversion(
    seeded_client: AsyncClient, click_id: uuid.UUID
) -> None:
    payload = {
        "click_id": str(click_id),
        "event": "application_submitted",
        "event_at": "2026-04-21T10:00:00Z",
        "commission_thb": 250.0,
    }
    body = json.dumps(payload).encode("utf-8")
    resp = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Loftly-Signature": _sign(body),
        },
    )
    assert resp.status_code == 204, resp.text

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(AffiliateConversion))).scalars().all())
    assert len(rows) == 1
    assert rows[0].conversion_type == "application_submitted"
    assert rows[0].status == "pending"


async def test_webhook_invalid_signature_returns_401(
    seeded_client: AsyncClient, click_id: uuid.UUID
) -> None:
    payload = {
        "click_id": str(click_id),
        "event": "application_submitted",
        "event_at": "2026-04-21T10:00:00Z",
    }
    body = json.dumps(payload).encode("utf-8")
    resp = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Loftly-Signature": "sha256=deadbeef",
        },
    )
    assert resp.status_code == 401
    body_json = resp.json()
    assert body_json["error"]["code"] == "webhook_signature_invalid"
    assert body_json["error"]["message_th"]


async def test_webhook_unknown_partner_returns_401(
    seeded_client: AsyncClient, click_id: uuid.UUID
) -> None:
    payload = {
        "click_id": str(click_id),
        "event": "application_submitted",
        "event_at": "2026-04-21T10:00:00Z",
    }
    body = json.dumps(payload).encode("utf-8")
    resp = await seeded_client.post(
        "/v1/webhooks/affiliate/unknown-partner",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Loftly-Signature": _sign(body),  # "correct" sig but partner unknown
        },
    )
    assert resp.status_code == 401


async def test_webhook_is_idempotent(seeded_client: AsyncClient, click_id: uuid.UUID) -> None:
    payload = {
        "click_id": str(click_id),
        "event": "application_approved",
        "event_at": "2026-04-21T10:00:00Z",
        "commission_thb": 400.0,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-Loftly-Signature": _sign(body),
    }

    # Replay twice.
    r1 = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}", content=body, headers=headers
    )
    r2 = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}", content=body, headers=headers
    )
    assert r1.status_code == 204
    assert r2.status_code == 204

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list((await session.execute(select(AffiliateConversion))).scalars().all())
    assert len(rows) == 1  # idempotent


async def test_webhook_missing_signature_returns_401(
    seeded_client: AsyncClient, click_id: uuid.UUID
) -> None:
    payload = {
        "click_id": str(click_id),
        "event": "application_submitted",
        "event_at": "2026-04-21T10:00:00Z",
    }
    body = json.dumps(payload).encode("utf-8")
    resp = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}",
        content=body,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 401
