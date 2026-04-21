"""audit_log retention cron — policy correctness + batched delete.

Covers:
- Consent-bucket rows survive past the 18-month default cutoff.
- Default-bucket rows older than 18 months are deleted.
- Recent rows (any bucket) are left alone.
- Consent rows older than 7 years ARE deleted.
- Dry-run never writes.
- Preview endpoint (admin) and internal run endpoint (X-API-Key) work.

The seed uses 1 000 rows split across buckets + ages so the batch-loop exit
condition is also exercised (> 1 batch per bucket).
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.jobs.audit_log_retention import (
    CONSENT_RETENTION_DAYS,
    DEFAULT_RETENTION_DAYS,
    run_retention,
)
from tests.conftest import TEST_ADMIN_ID


async def _seed_rows(
    *,
    default_old: int,
    default_recent: int,
    consent_old_18m: int,
    consent_very_old_8y: int,
    other_recent: int,
) -> None:
    """Insert a mix of aged audit_log rows. All use `TEST_ADMIN_ID` as actor."""
    now = datetime.now(UTC)
    very_old_default = now - timedelta(days=DEFAULT_RETENTION_DAYS + 30)
    recent_default = now - timedelta(days=30)
    old_consent_18m = now - timedelta(days=DEFAULT_RETENTION_DAYS + 45)  # past default cutoff
    very_old_consent_8y = now - timedelta(days=CONSENT_RETENTION_DAYS + 60)  # past consent cutoff

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Default-bucket, old — should be deleted.
        for _ in range(default_old):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="card.updated",
                    subject_type="card",
                    subject_id=uuid.uuid4(),
                    meta={},
                    created_at=very_old_default,
                )
            )
        # Default-bucket, recent — should survive.
        for _ in range(default_recent):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="article.published",
                    subject_type="article",
                    subject_id=uuid.uuid4(),
                    meta={},
                    created_at=recent_default,
                )
            )
        # Consent-bucket aged 18m — should SURVIVE (consent retention is 7y).
        for _ in range(consent_old_18m):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="consent.revoked",
                    subject_type="user",
                    subject_id=TEST_ADMIN_ID,
                    meta={"purpose": "optimization"},
                    created_at=old_consent_18m,
                )
            )
        # Consent-bucket aged 8y — should be deleted.
        for _ in range(consent_very_old_8y):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="consent.granted",
                    subject_type="user",
                    subject_id=TEST_ADMIN_ID,
                    meta={"purpose": "optimization"},
                    created_at=very_old_consent_8y,
                )
            )
        # Mixed recent — webhook reject + account.delete.requested (consent bucket, recent).
        for _ in range(other_recent):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="account.delete.requested",
                    subject_type="user",
                    subject_id=TEST_ADMIN_ID,
                    meta={},
                    created_at=recent_default,
                )
            )
        await session.commit()


async def _count_by(action_prefix: str) -> int:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(AuditLog)
                    .where(AuditLog.action.like(f"{action_prefix}%"))
                )
            ).scalar_one()
        )


async def _total_rows() -> int:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return int(
            (await session.execute(select(func.count()).select_from(AuditLog))).scalar_one()
        )


@pytest.mark.asyncio
async def test_dry_run_counts_without_deleting(seeded_db: object) -> None:
    _ = seeded_db
    await _seed_rows(
        default_old=50,
        default_recent=10,
        consent_old_18m=20,
        consent_very_old_8y=5,
        other_recent=3,
    )
    before = await _total_rows()
    assert before == 88

    result = await run_retention(dry_run=True)
    assert result.dry_run is True

    after = await _total_rows()
    assert after == before, "dry-run must not delete rows"

    buckets = {b.name: b for b in result.buckets}
    # Default bucket matches 50 old rows (card.updated, article.published, etc.
    # — article.published recent rows NOT past cutoff).
    assert buckets["default"].rows_matched == 50
    # Consent bucket matches only the 8-year-old consent rows — 18m consent
    # rows are still within the 7y retention window.
    assert buckets["consent"].rows_matched == 5


@pytest.mark.asyncio
async def test_execute_deletes_only_aged_rows(seeded_db: object) -> None:
    _ = seeded_db
    await _seed_rows(
        default_old=1200,  # spans > 1 batch (BATCH_SIZE=1000) — proves loop exits
        default_recent=50,
        consent_old_18m=40,
        consent_very_old_8y=8,
        other_recent=10,
    )
    before = await _total_rows()
    assert before == 1308

    result = await run_retention(dry_run=False)
    assert result.dry_run is False
    assert result.total_rows_deleted == 1200 + 8

    # Recent rows + consent-at-18m must survive.
    after = await _total_rows()
    assert after == 50 + 40 + 10 == 100

    # Specific invariants.
    remaining_consent = await _count_by("consent.")
    assert remaining_consent == 40, "consent rows < 7y old must survive"

    remaining_card = await _count_by("card.")
    assert remaining_card == 0, "aged card.* rows must be deleted"

    remaining_account = await _count_by("account.delete.")
    assert remaining_account == 10, "recent account.delete.* rows must survive"


@pytest.mark.asyncio
async def test_consent_rows_survive_default_cutoff(seeded_db: object) -> None:
    """The load-bearing PDPA invariant: an 18-month-old consent.granted row
    must NOT be swept by the default 18-month cutoff."""
    _ = seeded_db
    await _seed_rows(
        default_old=0,
        default_recent=0,
        consent_old_18m=25,
        consent_very_old_8y=0,
        other_recent=0,
    )
    await run_retention(dry_run=False)
    remaining = await _count_by("consent.")
    assert remaining == 25


@pytest.mark.asyncio
async def test_env_override_retention_days(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Overriding AUDIT_RETENTION_DEFAULT_DAYS shortens the sweep window."""
    _ = seeded_db
    # Seed 5 rows from 100 days ago — normally inside the 540-day retention.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        for _ in range(5):
            session.add(
                AuditLog(
                    id=uuid.uuid4(),
                    actor_id=TEST_ADMIN_ID,
                    action="card.created",
                    subject_type="card",
                    subject_id=uuid.uuid4(),
                    meta={},
                    created_at=datetime.now(UTC) - timedelta(days=100),
                )
            )
        await session.commit()

    monkeypatch.setenv("AUDIT_RETENTION_DEFAULT_DAYS", "30")
    preview = await run_retention(dry_run=True)
    default_bucket = next(b for b in preview.buckets if b.name == "default")
    assert default_bucket.rows_matched == 5


@pytest.mark.asyncio
async def test_admin_preview_endpoint(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    await _seed_rows(
        default_old=30,
        default_recent=5,
        consent_old_18m=10,
        consent_very_old_8y=2,
        other_recent=0,
    )
    resp = await seeded_client.get(
        "/v1/admin/jobs/audit-retention/preview", headers=admin_headers
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is True
    by_name = {b["name"]: b for b in body["buckets"]}
    assert by_name["default"]["rows_matched"] == 30
    assert by_name["consent"]["rows_matched"] == 2
    # Preview must NOT have deleted anything.
    assert await _total_rows() == 47


@pytest.mark.asyncio
async def test_internal_run_endpoint_executes(
    seeded_client: AsyncClient,
) -> None:
    await _seed_rows(
        default_old=20,
        default_recent=3,
        consent_old_18m=7,
        consent_very_old_8y=4,
        other_recent=0,
    )
    # JWT_SIGNING_KEY is the X-API-Key in Phase 1 (see internal.py).
    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.post(
        "/v1/internal/audit-retention/run",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is False
    assert body["total_rows_deleted"] == 20 + 4

    # Unauthorized without the header.
    bad = await seeded_client.post("/v1/internal/audit-retention/run")
    assert bad.status_code == 401


@pytest.mark.asyncio
async def test_empty_table_is_noop(seeded_db: object) -> None:
    _ = seeded_db
    result = await run_retention(dry_run=False)
    assert result.total_rows_deleted == 0
    for bucket in result.buckets:
        assert bucket.rows_matched == 0
        assert bucket.rows_deleted == 0
