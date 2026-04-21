"""Consent endpoints — GET/POST `/v1/consent`.

Implements SPEC.md §1 + §7:
- POST appends a new `user_consents` row (NEVER update).
- `optimization=false` is blocked — the product requires optimization consent.
- GET returns the latest-row state per purpose.

The route currently looks up `policy_version` from the most-recent written row
or falls back to the default constant; Week 3 will pull this from a CMS table.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_user_id
from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
from loftly.db.models.consent import UserConsent
from loftly.observability.prometheus import consent_observer
from loftly.schemas.consent import ConsentFlags, ConsentState, ConsentUpdate

router = APIRouter(prefix="/v1/consent", tags=["consent"])

# Default policy version echoed when the user has no consent rows yet.
# Keep in sync with the policy doc shipped to web; bump when terms change.
DEFAULT_POLICY_VERSION = "2026-04-01"


async def _latest_state(session: AsyncSession, user_id: uuid.UUID) -> tuple[ConsentFlags, str]:
    """Fold the append-only log to the current per-purpose grant state."""
    stmt = (
        select(UserConsent)
        .where(UserConsent.user_id == user_id)
        .order_by(UserConsent.granted_at.asc())
    )
    rows = list((await session.execute(stmt)).scalars().all())

    flags = ConsentFlags()
    policy_version = DEFAULT_POLICY_VERSION
    for row in rows:
        policy_version = row.policy_version  # latest wins
        setattr(flags, row.purpose, bool(row.granted))
    return flags, policy_version


@router.get(
    "",
    response_model=ConsentState,
    summary="Current consent state (4 purposes)",
)
async def get_consent(
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> ConsentState:
    flags, policy_version = await _latest_state(session, user_id)
    return ConsentState(policy_version=policy_version, consents=flags)


@router.post(
    "",
    response_model=ConsentState,
    summary="Update consent (append-only)",
)
async def update_consent(
    payload: ConsentUpdate,
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> ConsentState:
    # SPEC.md §1 / §7 — optimization consent is required for the core product.
    if payload.purpose == "optimization" and payload.granted is False:
        raise LoftlyError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="consent_optimization_required",
            message_en=(
                "Optimization consent is required to use Loftly's core features. "
                "To revoke, delete your account from Settings."
            ),
            message_th=(
                "ต้องยินยอมให้ใช้ข้อมูลเพื่อการแนะนำ จึงจะใช้งาน Loftly ได้ "
                "หากต้องการถอนการยินยอม โปรดลบบัญชีจากการตั้งค่า"
            ),
            details={"purpose": payload.purpose},
        )

    # Append-only: always INSERT, never UPDATE. PDPA log integrity.
    row = UserConsent(
        user_id=user_id,
        purpose=payload.purpose,
        granted=payload.granted,
        policy_version=payload.policy_version,
        source=payload.source,
    )
    session.add(row)
    await session.commit()

    # Metrics — counter per grant/withdraw event, labelled by purpose. Kept
    # after commit so failed writes don't inflate the counter.
    consent_observer(
        purpose=payload.purpose,
        action="granted" if payload.granted else "withdrawn",
    )

    flags, policy_version = await _latest_state(session, user_id)
    return ConsentState(policy_version=policy_version, consents=flags)


__all__ = ["router"]
