"""Consent-gated email tracking pixel — POST_V1 §2.

Serves a 1x1 transparent GIF to every valid token; only emits a PostHog
`welcome_email_opened` event when the user has granted **Analytics** consent.
Invalid / tampered tokens → 400. This endpoint must never 500 — a broken
tracking pixel would render as a broken-image icon in some email clients and
undermine trust in the email.

Contract choices:
- Token is an HMAC-signed `{user_id_hash, email_type}` payload built by
  `notifications.welcome_email.build_tracking_token` so the endpoint never
  sees (and therefore cannot leak) raw user IDs.
- We look up consent by **hashed** user_id only when a user_id_hash can be
  mapped back to a user — for POST_V1 §2 the pixel is sent at email-capture
  time when the user is still anonymous, so the common path is "no user row
  yet → no Analytics consent on file → skip the PostHog emit". That matches
  the spec ("If user has not granted Analytics consent, pixel is omitted
  from the email in the first place; as a belt-and-suspenders, the endpoint
  also skips emit on missing consent").

See `POST_V1.md §2 Instrumentation` + PDPA touchpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_session
from loftly.db.models.consent import UserConsent
from loftly.db.models.user import User
from loftly.notifications.welcome_email import verify_tracking_token
from loftly.observability.posthog import capture as posthog_capture
from loftly.observability.posthog import hash_distinct_id

router = APIRouter(prefix="/v1/tracking", tags=["tracking"])
log = get_logger(__name__)

# 1x1 transparent GIF89a — 43 bytes, universally accepted by email clients.
_PIXEL_GIF = bytes.fromhex(
    "47494638396101000100800000ffffff00000021f90401000000002c000000000100010000020144003b"
)
_GIF_HEADERS = {
    "Content-Type": "image/gif",
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
}


async def _has_analytics_consent(user_id_hash: str, session: AsyncSession) -> bool:
    """Return True if any user's latest `analytics` consent row is `granted=true`.

    We don't have a reverse index from user_id_hash to user row in Phase 1 —
    the hash is one-way. So we scan user rows (bounded by the small MVP user
    base) and compare hashes. If no match, consent is presumed not granted
    (which is the correct safe default under PDPA: opt-in, not opt-out).

    For higher-traffic deployments, add a `user_id_hash` column alongside
    users.id and index it. Tracking will do.
    """
    users = (await session.execute(select(User.id))).scalars().all()
    target_user_id = None
    for uid in users:
        if hash_distinct_id(str(uid)) == user_id_hash:
            target_user_id = uid
            break
    if target_user_id is None:
        return False

    # Latest-row-wins fold — same semantics as `/v1/consent`.
    consents = (
        (
            await session.execute(
                select(UserConsent)
                .where(UserConsent.user_id == target_user_id)
                .where(UserConsent.purpose == "analytics")
                .order_by(UserConsent.granted_at.asc())
            )
        )
        .scalars()
        .all()
    )
    if not consents:
        return False
    return bool(consents[-1].granted)


def _gif_response() -> Response:
    return Response(
        content=_PIXEL_GIF,
        status_code=status.HTTP_200_OK,
        media_type="image/gif",
        headers=_GIF_HEADERS,
    )


@router.get(
    "/email/{token}/open",
    summary="1x1 tracking pixel for email open events (Analytics consent only)",
)
async def email_open_pixel(
    token: str,
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Verify token → optionally emit PostHog → always return the GIF.

    Error handling is deliberately permissive: the GIF must render for any
    email client that fetched the URL. Only a *structurally* invalid token
    gets 400 (because returning a pixel on garbage paths would mask real
    bugs and waste budget on bot noise).
    """
    # Wrap entire body in a try/except because the spec explicitly says
    # "Never 500 on any path". The only acceptable non-200 is a 400 for
    # signature failure (detected below).
    try:
        verified = verify_tracking_token(token)
        if verified is None:
            return Response(
                content=b"",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        user_id_hash = verified["u"]
        email_type = verified["t"]

        # Look up consent. If not granted (or user not resolvable), skip the
        # emit and return the pixel anyway. Tracking noise must not drive
        # PDPA liability.
        try:
            granted = await _has_analytics_consent(user_id_hash, session)
        except Exception as exc:
            # DB error during consent lookup is not a reason to break the
            # email render — log + skip emit.
            log.warning("tracking_pixel_consent_lookup_failed", error=str(exc)[:200])
            granted = False

        if granted:
            await posthog_capture(
                event="welcome_email_opened",
                distinct_id=user_id_hash,
                properties={
                    "user_id_hash": user_id_hash,
                    "email_type": email_type,
                },
            )
        return _gif_response()
    except Exception as exc:
        # Belt-and-suspenders — if anything above raises we still serve a pixel.
        log.warning("tracking_pixel_unexpected_error", error=str(exc)[:200])
        return _gif_response()


__all__ = ["router"]
