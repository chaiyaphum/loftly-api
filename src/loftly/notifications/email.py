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

from typing import Any

from loftly.core.logging import get_logger
from loftly.core.settings import get_settings

log = get_logger(__name__)

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
) -> str | None:
    """Generic plain-text transactional send.

    Used by operator-facing notifications (e.g. weekly content-stale digest)
    where we don't want the magic-link template. Returns the Resend message id
    when a send happened, or `None` in stub mode (no RESEND_API_KEY) so callers
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
        )
        return None

    import resend

    resend.api_key = settings.resend_api_key
    payload: Any = {
        "from": settings.resend_from_address,
        "to": [to],
        "subject": subject,
        "text": text,
    }
    result = resend.Emails.send(payload)
    message_id = result.get("id") if isinstance(result, dict) else None
    log.info(
        "transactional_email_sent",
        to=to,
        subject=subject,
        resend_id=message_id,
    )
    return message_id if isinstance(message_id, str) else None


__all__ = ["send_email", "send_magic_link"]
