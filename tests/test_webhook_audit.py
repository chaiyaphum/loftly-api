"""Webhook signature-rejection audit trail uses the deterministic system user."""

from __future__ import annotations

import json

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from tests.conftest import SYSTEM_USER_ID


async def test_webhook_bad_signature_audits_under_system_user(
    seeded_client: AsyncClient,
) -> None:
    body = json.dumps({"click_id": "x", "event": "x", "event_at": "x"}).encode("utf-8")
    resp = await seeded_client.post(
        "/v1/webhooks/affiliate/test-partner",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Loftly-Signature": "sha256=garbage",
        },
    )
    assert resp.status_code == 401

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (
                await session.execute(
                    select(AuditLog).where(AuditLog.action == "webhook.signature_rejected")
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].actor_id == SYSTEM_USER_ID
    assert rows[0].meta["partner_id"] == "test-partner"
    assert rows[0].meta["reason"] == "signature_mismatch"


async def test_webhook_unknown_partner_also_audits(seeded_client: AsyncClient) -> None:
    body = b"{}"
    resp = await seeded_client.post(
        "/v1/webhooks/affiliate/nobody",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Loftly-Signature": "sha256=x",
        },
    )
    assert resp.status_code == 401
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (
                await session.execute(
                    select(AuditLog).where(AuditLog.action == "webhook.signature_rejected")
                )
            )
            .scalars()
            .all()
        )
    assert any(r.meta["partner_id"] == "nobody" for r in rows)
