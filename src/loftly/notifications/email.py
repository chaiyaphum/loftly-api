"""Resend-backed transactional email.

Stub mode: when `settings.resend_api_key` is missing we structlog the event so
dev + CI work without a real account. Real mode: the `resend` SDK ships the
email. Both paths accept the same `locale` and `magic_url` so the call-site
doesn't care which one is active.

Templates kept inline (no external templating engine) — two emails today
(magic-link TH / EN); if we grow past five, we'll move to `jinja2` templates
under `notifications/templates/`.
"""

from __future__ import annotations

import asyncio
from typing import Any

from loftly.core.logging import get_logger
from loftly.core.settings import get_settings

log = get_logger(__name__)

# POST_V1 §2 AC: Resend bounce/error → retry once after 30s; if still failing,
# alert Sentry. Wall-clock delay is fine (we're in an asyncio task off the
# request path), and 30s matches the user-facing "ไม่ได้รับ email?" copy.
_RESEND_RETRY_DELAY_SEC = 30.0

_SUBJECT_TH = "ลิงก์เข้าสู่ระบบ Loftly"
_SUBJECT_EN = "Your Loftly sign-in link"

_BODY_TH = """\
สวัสดีค่ะ

กดลิงก์ด้านล่างเพื่อเข้าสู่ระบบ Loftly — ลิงก์นี้จะหมดอายุใน 15 นาที

{magic_url}

หากคุณไม่ได้ขอลิงก์นี้ ไม่ต้องทำอะไรค่ะ

ขอบคุณที่ใช้ Loftly
"""

_BODY_EN = """\
Hi there,

Click the link below to sign in to Loftly — it expires in 15 minutes.

{magic_url}

If you didn't request this, you can safely ignore this email.

— Loftly
"""


async def send_magic_link(email: str, magic_url: str, locale: str = "th") -> None:
    """Send magic-link email via Resend (or log-only when no key configured)."""
    settings = get_settings()

    if locale == "th":
        subject = _SUBJECT_TH
        body = _BODY_TH.format(magic_url=magic_url)
    else:
        subject = _SUBJECT_EN
        body = _BODY_EN.format(magic_url=magic_url)

    if not settings.resend_api_key:
        log.info(
            "magic_link_email_stub",
            email=email,
            subject=subject,
            magic_url=magic_url,
            locale=locale,
        )
        return

    # Lazy import — keeps `resend` optional when stubbed.
    import resend

    resend.api_key = settings.resend_api_key
    # Resend's `Emails.send` is sync; offload if we need to in future.
    # SendParams is a TypedDict; we satisfy the subset we need.
    payload: Any = {
        "from": settings.resend_from_address,
        "to": [email],
        "subject": subject,
        "text": body,
    }
    result = resend.Emails.send(payload)
    log.info(
        "magic_link_email_sent",
        email=email,
        locale=locale,
        resend_id=result.get("id") if isinstance(result, dict) else None,
    )


async def send_email(
    *,
    to: str,
    subject: str,
    text: str,
    html: str | None = None,
) -> str | None:
    """Generic transactional send. Plain-text body required; HTML optional.

    Used by operator-facing notifications (e.g. weekly content-stale digest)
    and by the POST_V1 §2 personalized welcome email which supplies an HTML
    body alongside the plaintext fallback. Returns the Resend message id when
    a send happened, or `None` in stub mode (no RESEND_API_KEY) so callers
    can branch on "did a real email leave?".

    Stubbing mirrors `send_magic_link`: when the API key is unset we structlog
    the event and return `None`. This keeps dev + CI working without a real
    Resend account.
    """
    settings = get_settings()

    if not settings.resend_api_key:
        log.info(
            "transactional_email_stub",
            to=to,
            subject=subject,
            has_html=html is not None,
        )
        return None

    import resend

    resend.api_key = settings.resend_api_key
    # Resend's `SendParams` is a TypedDict; satisfy it structurally via Any
    # (matches the pattern in `send_magic_link`). Shape validated at runtime.
    payload: Any = {
        "from": settings.resend_from_address,
        "to": [to],
        "subject": subject,
        "text": text,
    }
    if html is not None:
        payload["html"] = html
    result = resend.Emails.send(payload)
    message_id = result.get("id") if isinstance(result, dict) else None
    log.info(
        "transactional_email_sent",
        to=to,
        subject=subject,
        resend_id=message_id,
    )
    return message_id if isinstance(message_id, str) else None


async def send_email_with_retry(
    *,
    to: str,
    subject: str,
    text: str,
    html: str | None = None,
    email_type: str = "transactional",
) -> str | None:
    """Send once, wait 30s, retry once. Sentry-alert + raise on double-fail.

    POST_V1 §2 AC: "Given email-delivery service bounces or errors, when
    detected, then retry once after 30s; if still failing, alert Sentry".

    This wraps ``send_email`` — stub mode (no RESEND_API_KEY) skips retry
    because there's nothing to fail. The 30s wait is deliberate real time, not
    exponential backoff; we're off the request path and the user-facing copy
    on the results page says "resend link" if the email never arrives.

    On double-failure we capture via ``sentry_sdk.capture_exception`` (no-op
    if Sentry isn't configured — matches the pattern elsewhere) and re-raise
    so the caller's structlog line records the failure shape.
    """
    settings = get_settings()

    # Stub mode: no retry ceremony — nothing can actually fail over the wire.
    if not settings.resend_api_key:
        return await send_email(to=to, subject=subject, text=text, html=html)

    try:
        return await send_email(to=to, subject=subject, text=text, html=html)
    except Exception as first_exc:
        log.warning(
            "email_send_attempt_failed",
            email_type=email_type,
            attempt=1,
            to=to,
            error=str(first_exc)[:200],
        )

    # Fixed 30s wait — spec says "30s" not "30s with jitter".
    await asyncio.sleep(_RESEND_RETRY_DELAY_SEC)

    try:
        return await send_email(to=to, subject=subject, text=text, html=html)
    except Exception as second_exc:
        log.error(
            "email_send_attempt_failed",
            email_type=email_type,
            attempt=2,
            to=to,
            error=str(second_exc)[:200],
        )
        # Route through Sentry. `capture_exception` is a no-op if Sentry isn't
        # initialised, so the guard is just to keep the import lazy.
        try:
            import sentry_sdk

            sentry_sdk.capture_exception(second_exc)
        except Exception:  # pragma: no cover — defensive; never let Sentry throw
            pass
        raise


__all__ = ["send_email", "send_email_with_retry", "send_magic_link"]
