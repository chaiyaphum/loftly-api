"""Account-delete purge executor.

Runs over any `jobs` row where `job_type='account_delete_scheduled'`,
`status='queued'`, and `expires_at < now()`. Purges PII while preserving
`user_consents` (7-year legal hold per PDPA Q11 decision) and `audit_log`
(accountability).

Triggered manually by the founder during Phase 1 via `scripts/run_purges.py`.
Phase 2 wires it to cron.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateClick, AffiliateConversion
from loftly.db.models.job import Job
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.db.models.user_card import UserCard
from loftly.observability.prometheus import dsar_observer

log = get_logger(__name__)


async def purge_user(session: AsyncSession, user_id: uuid.UUID) -> dict[str, int]:
    """Purge PII for `user_id`. Returns counters for observability."""
    counters: dict[str, int] = {
        "user_cards_deleted": 0,
        "selector_sessions_scrubbed": 0,
        "affiliate_clicks_unlinked": 0,
        "affiliate_conversions_unlinked": 0,
    }

    # user_cards: delete outright.
    cards = list(
        (await session.execute(select(UserCard).where(UserCard.user_id == user_id))).scalars().all()
    )
    for row in cards:
        await session.delete(row)
    counters["user_cards_deleted"] = len(cards)

    # selector_sessions: keep row but scrub input/output so no PII survives.
    sessions = list(
        (await session.execute(select(SelectorSession).where(SelectorSession.user_id == user_id)))
        .scalars()
        .all()
    )
    for sess in sessions:
        sess.input = {"purged": True}
        sess.output = {"purged": True}
    counters["selector_sessions_scrubbed"] = len(sessions)

    # affiliate_clicks: detach user_id so the click row lives on for analytics.
    click_rows = list(
        (await session.execute(select(AffiliateClick).where(AffiliateClick.user_id == user_id)))
        .scalars()
        .all()
    )
    click_ids = [c.click_id for c in click_rows]
    if click_ids:
        await session.execute(
            update(AffiliateClick)
            .where(AffiliateClick.user_id == user_id)
            .values(user_id=None, ip_hash=None, user_agent=None)
        )
    counters["affiliate_clicks_unlinked"] = len(click_ids)

    if click_ids:
        # Conversions FK to clicks but don't reference users directly; scrub
        # raw_payload so any bundled PII disappears.
        conv_rows = list(
            (
                await session.execute(
                    select(AffiliateConversion).where(AffiliateConversion.click_id.in_(click_ids))
                )
            )
            .scalars()
            .all()
        )
        for conv in conv_rows:
            conv.raw_payload = {"purged": True}
        counters["affiliate_conversions_unlinked"] = len(conv_rows)

    # users: scrub identifying columns but keep the row (FK integrity for
    # audit_log, affiliate_conversions.click -> affiliate_clicks.user).
    user = (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
    if user is not None:
        user.email = f"purged+{user.id}@deleted.loftly.co.th"
        user.phone = None
        user.oauth_subject = f"purged:{user.id}"
        user.deleted_at = datetime.now(UTC)

    return counters


async def run_due_purges() -> list[dict[str, object]]:
    """Finalize every scheduled delete whose grace period has elapsed."""
    sessionmaker = get_sessionmaker()
    now = datetime.now(UTC)
    results: list[dict[str, object]] = []
    async with sessionmaker() as session:
        # SQLite may return naive datetimes — filter in Python post-fetch to
        # keep the comparison dialect-agnostic.
        candidates = list(
            (
                await session.execute(
                    select(Job).where(
                        Job.job_type == "account_delete_scheduled",
                        Job.status == "queued",
                    )
                )
            )
            .scalars()
            .all()
        )
        due_jobs = []
        for j in candidates:
            expires = j.expires_at
            if expires is None:
                continue
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires < now:
                due_jobs.append(j)
        for job in due_jobs:
            counters = await purge_user(session, job.user_id)
            job.status = "done"
            job.finished_at = now
            # DSAR metric: record days from request creation to final purge.
            created = job.created_at
            if created is not None:
                created_aware = (
                    created if created.tzinfo is not None else created.replace(tzinfo=UTC)
                )
                days = max(0.0, (now - created_aware).total_seconds() / 86_400.0)
                dsar_observer("delete", "closed", resolution_days=days)
            else:
                dsar_observer("delete", "closed")
            results.append(
                {
                    "job_id": str(job.id),
                    "user_id": str(job.user_id),
                    **counters,
                }
            )
            log.info(
                "account_purge_complete",
                job_id=str(job.id),
                user_id=str(job.user_id),
                **counters,
            )
        await session.commit()
    return results


__all__ = ["purge_user", "run_due_purges"]
