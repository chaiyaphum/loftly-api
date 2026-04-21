"""Weekly content-stale digest.

Scans `articles` for rows with `state='published'` and `updated_at` older than
``STALE_THRESHOLD_DAYS`` (default 90) and, if any exist, emails the founder a
plain-text summary with the top 10 oldest. Mirrors the logic behind
`GET /v1/admin/articles/stale` but extracts the core query into a standalone,
testable callable so it can be driven from cron without going through admin
auth.

Contract
--------
- No stale articles → return ``DigestResult`` with ``count=0`` and no email.
- ≥1 stale articles + Resend configured → send email, emit PostHog event,
  append an `audit_log` row (`content.stale_digest.sent`).
- ≥1 stale articles + Resend NOT configured → same audit + PostHog path minus
  the email; result carries ``email_sent=False, skip_reason="resend_disabled"``
  so the caller (internal route) can return HTTP 202 instead of 200.

Language
--------
Email body is **Thai-primary, English-secondary** — the founder reads both,
but Loftly's editorial voice is Thai-first (BRAND.md §4), so the ops emails
should match. No HTML template — plain text is the explicit Phase 1 call per
the task.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.audit import log_action
from loftly.db.models.article import Article

log = get_logger(__name__)

#: Matches the `/v1/admin/articles/stale` default (`_STALE_DEFAULT_DAYS`).
STALE_THRESHOLD_DAYS: int = 90

#: Number of articles included in the email body. The full list remains in
#: the admin dashboard; this is a pruning hint, not a work order.
EMAIL_TOP_N: int = 10

#: Stable actor for the audit row. Mirrors `routes/webhooks.py::SYSTEM_USER_ID`
#: so all unattended writes converge on the same system user (seeded by
#: migration 012).
SYSTEM_USER_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")

#: Canonical audit action string. Kept alongside `content.*` rather than
#: `article.*` because the event is about the content pipeline as a whole,
#: not an edit to a specific article.
AUDIT_ACTION_DIGEST_SENT: str = "content.stale_digest.sent"

#: PostHog event name. Namespaced under `content_` to keep ops analytics
#: grouped separately from user-facing product events.
POSTHOG_EVENT_DIGEST_EMAILED: str = "content_stale_digest_emailed"


@dataclass(frozen=True)
class StaleArticleRow:
    """Minimal projection used for the email body."""

    id: uuid.UUID
    slug: str
    title_th: str
    updated_at: datetime

    @property
    def days_old(self) -> int:
        now = datetime.now(UTC)
        ua = self.updated_at
        if ua.tzinfo is None:
            ua = ua.replace(tzinfo=UTC)
        return max(0, (now - ua).days)


@dataclass(frozen=True)
class DigestResult:
    """Return shape for `run_digest` — structured so the route can map to HTTP
    status codes without re-inspecting strings."""

    count: int
    oldest_days: int
    email_sent: bool
    message_id: str | None
    skip_reason: str | None
    duration_ms: float

    def to_log_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "oldest_days": self.oldest_days,
            "email_sent": self.email_sent,
            "message_id": self.message_id,
            "skip_reason": self.skip_reason,
            "duration_ms": round(self.duration_ms, 2),
        }


async def _count_stale_published(session: AsyncSession, *, cutoff: datetime) -> int:
    from sqlalchemy import func

    stmt = (
        select(func.count(Article.id))
        .where(Article.state == "published")
        .where(Article.updated_at < cutoff)
    )
    return int((await session.execute(stmt)).scalar_one())


async def _load_oldest_stale(
    session: AsyncSession, *, cutoff: datetime, limit: int
) -> list[StaleArticleRow]:
    stmt = (
        select(Article.id, Article.slug, Article.title_th, Article.updated_at)
        .where(Article.state == "published")
        .where(Article.updated_at < cutoff)
        .order_by(Article.updated_at.asc())
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()
    return [
        StaleArticleRow(
            id=uuid.UUID(str(r.id)),
            slug=r.slug,
            title_th=r.title_th,
            updated_at=r.updated_at,
        )
        for r in rows
    ]


def _subject_line(count: int) -> str:
    # Thai-primary subject. Short + scannable in the inbox.
    return f"[Loftly] บทความค้างรีวิว {count} บทความ / {count} articles overdue for review"


def _render_email_body(
    rows: list[StaleArticleRow], *, total_count: int, threshold_days: int
) -> str:
    """Plain-text body. Thai first, English second.

    The list is the `EMAIL_TOP_N` oldest entries; `total_count` is the
    unpaginated total so the founder sees the real backlog size when it's
    bigger than the shown slice.
    """
    lines: list[str] = []
    # --- Thai block ---
    lines.append("สวัสดีค่ะ")
    lines.append("")
    lines.append(
        f"พบบทความที่เผยแพร่แล้วและไม่ได้อัปเดตเกิน {threshold_days} วันจำนวน "
        f"{total_count} บทความ แสดง {len(rows)} บทความที่เก่าที่สุดด้านล่าง:"
    )
    lines.append("")
    for idx, row in enumerate(rows, start=1):
        lines.append(f"{idx}. {row.title_th} ({row.slug}) — {row.days_old} วัน")
    lines.append("")
    lines.append(
        "เข้าไปที่หน้าแอดมิน /admin/articles/stale เพื่อยืนยันการรีวิว (กด mark-reviewed หลังตรวจเนื้อหาแล้ว)"
    )
    lines.append("")
    # --- English block ---
    lines.append("---")
    lines.append("")
    lines.append(
        f"Found {total_count} published article(s) whose updated_at is older "
        f"than {threshold_days} days. Top {len(rows)} oldest shown above."
    )
    lines.append("")
    lines.append(
        "Open /admin/articles/stale in the CMS to mark each one reviewed "
        "once you've verified the content is still accurate."
    )
    lines.append("")
    lines.append("— Loftly content ops")
    return "\n".join(lines)


async def _emit_posthog(count: int, oldest_days: int) -> None:
    """Fire the analytics event. Never raises."""
    # Lazy import to keep the job module importable without httpx eagerly
    # pulling settings on test paths that don't care about PostHog.
    from loftly.observability.posthog import capture, hash_distinct_id

    try:
        await capture(
            event=POSTHOG_EVENT_DIGEST_EMAILED,
            distinct_id=hash_distinct_id("system:content-ops"),
            properties={"count": count, "oldest_days": oldest_days},
        )
    except Exception as exc:  # pragma: no cover — capture itself swallows errors
        log.warning("content_stale_digest_posthog_failed", error=str(exc)[:200])


async def run_digest(
    session: AsyncSession,
    *,
    threshold_days: int = STALE_THRESHOLD_DAYS,
    top_n: int = EMAIL_TOP_N,
    now: datetime | None = None,
) -> DigestResult:
    """Entry point for the internal route and future CLI callers.

    The session is passed in (not opened here) so the internal route can share
    its request-scoped session + transaction — the audit row and the count
    land atomically.
    """
    from loftly.core.settings import get_settings
    from loftly.notifications.email import send_email

    t0 = time.perf_counter()
    reference = now if now is not None else datetime.now(UTC)
    cutoff = reference - timedelta(days=threshold_days)

    count = await _count_stale_published(session, cutoff=cutoff)

    if count == 0:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        log.info(
            "content_stale_digest_noop",
            threshold_days=threshold_days,
            cutoff=cutoff.isoformat(),
        )
        return DigestResult(
            count=0,
            oldest_days=0,
            email_sent=False,
            message_id=None,
            skip_reason="no_stale_articles",
            duration_ms=duration_ms,
        )

    rows = await _load_oldest_stale(session, cutoff=cutoff, limit=top_n)
    oldest_days = rows[0].days_old if rows else 0

    settings = get_settings()
    email_sent = False
    message_id: str | None = None
    skip_reason: str | None = None

    if not settings.resend_api_key:
        skip_reason = "resend_disabled"
        log.info(
            "content_stale_digest_skipped",
            reason=skip_reason,
            count=count,
            oldest_days=oldest_days,
        )
    else:
        subject = _subject_line(count)
        body = _render_email_body(rows, total_count=count, threshold_days=threshold_days)
        message_id = await send_email(
            to=settings.founder_notify_email,
            subject=subject,
            text=body,
        )
        email_sent = message_id is not None
        if not email_sent:
            # send_email() returned None despite the key being set — this can
            # happen if Resend returned a non-dict response. We still fire the
            # audit/PostHog events so ops visibility survives.
            skip_reason = "resend_returned_no_id"

    # Audit row — always, so "we ran the sweep" is recorded even when email
    # skipped.
    await log_action(
        session,
        actor_id=SYSTEM_USER_ID,
        action=AUDIT_ACTION_DIGEST_SENT,
        subject_type="content_ops",
        subject_id=None,
        metadata={
            "count": count,
            "oldest_days": oldest_days,
            "message_id": message_id,
            "email_sent": email_sent,
            "skip_reason": skip_reason,
            "threshold_days": threshold_days,
        },
    )
    await session.commit()

    # PostHog event — only when an email actually left. Per task spec the
    # event represents "a digest was emailed", not "we checked for staleness".
    if email_sent:
        await _emit_posthog(count=count, oldest_days=oldest_days)

    duration_ms = (time.perf_counter() - t0) * 1000.0
    result = DigestResult(
        count=count,
        oldest_days=oldest_days,
        email_sent=email_sent,
        message_id=message_id,
        skip_reason=skip_reason,
        duration_ms=duration_ms,
    )
    log.info("content_stale_digest_run", **result.to_log_dict())
    return result


__all__ = [
    "AUDIT_ACTION_DIGEST_SENT",
    "EMAIL_TOP_N",
    "POSTHOG_EVENT_DIGEST_EMAILED",
    "STALE_THRESHOLD_DAYS",
    "SYSTEM_USER_ID",
    "DigestResult",
    "StaleArticleRow",
    "run_digest",
]
