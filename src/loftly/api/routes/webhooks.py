"""Webhook endpoints — partner conversion postbacks (HMAC-signed).

Signature format: `X-Loftly-Signature: sha256=<hex>` where the MAC is over the
raw request body (bytes). Per-partner secrets come from the
`AFFILIATE_PARTNER_SECRETS` env var (JSON map `partner_id -> secret`).

Idempotency: unique(click_id, partner_id, conversion_type) at the DB level
means the same postback can be replayed safely — the second insert is
swallowed and we still return 204.

Security note: on signature mismatch we audit-log the rejection (actor is a
synthetic system UUID so the audit_log FK holds) to aid forensics.
"""

from __future__ import annotations

import hmac
import json
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Header, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.core.settings import Settings, get_settings
from loftly.db.audit import log_action
from loftly.db.engine import get_session
from loftly.db.models.affiliate import AffiliateClick, AffiliateConversion

router = APIRouter(prefix="/v1/webhooks", tags=["webhook"])

# Seeded by migration 012. Stable UUID so audit rows remain attributable.
SYSTEM_USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def _audit_signature_rejection(
    session: AsyncSession, *, partner_id: str, reason: str
) -> None:
    """Log every rejected webhook under the system user so forensics survive."""
    await log_action(
        session,
        actor_id=SYSTEM_USER_ID,
        action="webhook.signature_rejected",
        subject_type="affiliate_partner",
        subject_id=None,
        metadata={"partner_id": partner_id, "reason": reason},
    )
    await session.commit()


_CONVERSION_TYPE_TO_STATUS = {
    "application_submitted": "pending",
    "application_approved": "confirmed",
    "application_rejected": "rejected",
}


def _expected_signature(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, "sha256").hexdigest()
    return f"sha256={digest}"


@router.post(
    "/affiliate/{partner_id}",
    summary="Partner conversion postback",
    response_class=Response,
)
async def affiliate_postback(
    partner_id: str,
    request: Request,
    x_loftly_signature: str | None = Header(default=None, alias="X-Loftly-Signature"),
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Verify HMAC, upsert a conversion row, return 204."""
    secrets = settings.affiliate_partner_secrets or {}
    secret = secrets.get(partner_id)
    body = await request.body()

    # Unknown partner OR missing signature OR bad signature => 401 identically.
    if secret is None or not x_loftly_signature:
        reason = "unknown_partner" if secret is None else "missing_signature"
        await _audit_signature_rejection(session, partner_id=partner_id, reason=reason)
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="webhook_signature_invalid",
            message_en="Invalid or missing webhook signature.",
            message_th="ลายเซ็น Webhook ไม่ถูกต้อง",
        )

    expected = _expected_signature(secret, body)
    if not hmac.compare_digest(expected, x_loftly_signature):
        await _audit_signature_rejection(
            session, partner_id=partner_id, reason="signature_mismatch"
        )
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="webhook_signature_invalid",
            message_en="Invalid or missing webhook signature.",
            message_th="ลายเซ็น Webhook ไม่ถูกต้อง",
        )

    # Parse body AFTER HMAC so we never act on tampered payloads.
    try:
        payload: dict[str, Any] = json.loads(body.decode("utf-8") or "{}")
    except (ValueError, UnicodeDecodeError) as exc:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_payload",
            message_en="Webhook body is not valid JSON.",
            message_th="ข้อมูล Webhook ไม่ใช่ JSON ที่ถูกต้อง",
        ) from exc

    missing = [f for f in ("click_id", "event", "event_at") if f not in payload]
    if missing:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_payload",
            message_en=f"Missing required fields: {', '.join(missing)}.",
            message_th="ข้อมูลไม่ครบถ้วน",
            details={"missing": missing},
        )

    try:
        click_id = uuid.UUID(str(payload["click_id"]))
    except ValueError as exc:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_payload",
            message_en="`click_id` is not a valid UUID.",
            message_th="`click_id` ไม่ถูกต้อง",
        ) from exc

    event = str(payload["event"])
    status_value = _CONVERSION_TYPE_TO_STATUS.get(event)
    if status_value is None:
        raise LoftlyError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_event",
            message_en=f"Unknown event {event!r}.",
            message_th="รหัสเหตุการณ์ไม่ถูกต้อง",
            details={"event": event},
        )

    # Click must exist and belong to this partner.
    click = (
        (await session.execute(select(AffiliateClick).where(AffiliateClick.click_id == click_id)))
        .scalars()
        .one_or_none()
    )
    if click is None or click.partner_id != partner_id:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="click_not_found",
            message_en="click_id does not match any known click for this partner.",
            message_th="ไม่พบการคลิกที่ระบุ",
        )

    # Idempotency: insert-on-not-exists keyed on (click_id, partner_id, conversion_type).
    existing = (
        (
            await session.execute(
                select(AffiliateConversion).where(
                    AffiliateConversion.click_id == click_id,
                    AffiliateConversion.partner_id == partner_id,
                    AffiliateConversion.conversion_type == event,
                )
            )
        )
        .scalars()
        .one_or_none()
    )

    if existing is None:
        commission = payload.get("commission_thb")
        row = AffiliateConversion(
            click_id=click_id,
            partner_id=partner_id,
            conversion_type=event,
            status=status_value,
            commission_thb=commission,
            raw_payload=payload,
        )
        # `event_at` comes from the partner; we store it in raw_payload so we
        # don't have to widen the schema. `received_at` is our ingest time.
        _ = payload.get("event_at")  # validated above
        session.add(row)
        await session.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


__all__ = ["router"]
