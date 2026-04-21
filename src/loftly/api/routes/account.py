"""Account / PDPA self-service endpoints.

Implements SPEC.md §7 + PDPA_COMPLIANCE.md §5 in Phase 1 shape:

- `POST /v1/account/data-export/request` — queue a data-export job (2/day
  rate limit per user). Background task builds the JSON bundle and persists
  a HMAC-signed local download URL valid 48h.
- `GET  /v1/account/data-export/{job_id}` — poll status.
- `GET  /v1/account/data-export/{job_id}/download?token=...` — stream the
  bundle. Access is via the signed token so the user doesn't need a session
  cookie.
- `POST /v1/account/delete/request` — start the 14-day grace period. Sets
  `users.deleted_at`; on cancel before grace ends we clear it.
- `POST /v1/account/delete/cancel` — revoke a pending delete.
- `GET  /v1/account/delete/status` — inspect current state.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_user_id
from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import FixedWindowLimiter
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.job import Job
from loftly.db.models.user import User
from loftly.jobs.data_export import (
    DOWNLOAD_TTL_SECONDS,
    export_file_path,
    run_export,
    verify_download_token,
)
from loftly.observability.prometheus import dsar_observer

router = APIRouter(prefix="/v1/account", tags=["account"])

# 2/day per user — PDPA exports are manual-review-worthy so we cap tight.
# Window = 24h. Key = user_id string so we don't leak across accounts.
DATA_EXPORT_LIMITER = FixedWindowLimiter(max_calls=2, window_sec=24 * 60 * 60)

GRACE_PERIOD_DAYS = 14


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _as_aware(dt: datetime | None) -> datetime | None:
    """SQLite strips tz on roundtrip — re-attach UTC for comparisons."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Data export
# ---------------------------------------------------------------------------


@router.post(
    "/data-export/request",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue data export job",
)
async def request_export(
    background_tasks: BackgroundTasks,
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    """Create a `jobs` row, schedule `run_export`, return a `JobHandle`."""
    if not DATA_EXPORT_LIMITER.allow(str(user_id)):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Data-export quota reached; try again tomorrow.",
            message_th="ขอส่งออกข้อมูลเกินจำนวนในวันนี้ กรุณาลองใหม่พรุ่งนี้",
        )

    job = Job(
        user_id=user_id,
        job_type="data_export",
        status="queued",
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    # DSAR tracking: export request opened.
    dsar_observer("export", "opened")

    background_tasks.add_task(run_export, job.id)
    return {"job_id": str(job.id), "status": "queued"}


@router.get(
    "/data-export/{job_id}",
    summary="Export job status",
)
async def export_status(
    job_id: uuid.UUID,
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    """Return `ExportJob`. 404 if the job belongs to a different user."""
    job = (
        (
            await session.execute(
                select(Job).where(
                    Job.id == job_id,
                    Job.job_type == "data_export",
                )
            )
        )
        .scalars()
        .one_or_none()
    )
    if job is None or job.user_id != user_id:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="job_not_found",
            message_en="Export job not found.",
            message_th="ไม่พบงานส่งออกข้อมูล",
        )
    return {
        "job_id": str(job.id),
        "status": job.status,
        "download_url": job.result_url,
        "expires_at": job.expires_at.isoformat() if job.expires_at else None,
    }


@router.get(
    "/data-export/{job_id}/download",
    summary="Download signed export bundle",
)
async def download_export(
    job_id: uuid.UUID,
    token: str = Query(...),
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
) -> FileResponse:
    """Verify HMAC + stream the JSON file. No auth header needed — token-scoped.

    The token is non-transferable to a different job_id (signed over the
    job_id) and carries its own expiry so stolen URLs stop working after 48h.
    """
    if not verify_download_token(job_id, token, secret=settings.jwt_signing_key):
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
            message_en="Download link is invalid or has expired.",
            message_th="ลิงก์ดาวน์โหลดหมดอายุหรือไม่ถูกต้อง",
        )

    job = (
        (
            await session.execute(
                select(Job).where(
                    Job.id == job_id,
                    Job.job_type == "data_export",
                )
            )
        )
        .scalars()
        .one_or_none()
    )
    if job is None or job.status != "done":
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="job_not_found",
            message_en="Export job not found.",
            message_th="ไม่พบงานส่งออกข้อมูล",
        )

    path: Path = export_file_path(job_id)
    if not path.exists():
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="export_missing",
            message_en="Export file is no longer available.",
            message_th="ไฟล์ส่งออกหมดอายุแล้ว",
        )
    _ = DOWNLOAD_TTL_SECONDS  # kept here so linters see the import is used downstream
    return FileResponse(
        path=path,
        media_type="application/json",
        filename=f"loftly_export_{job_id}.json",
    )


# ---------------------------------------------------------------------------
# Account deletion
# ---------------------------------------------------------------------------


async def _pending_delete_job(session: AsyncSession, user_id: uuid.UUID) -> Job | None:
    """Return the active `account_delete_scheduled` job for the user, if any."""
    stmt = (
        select(Job)
        .where(
            Job.user_id == user_id,
            Job.job_type == "account_delete_scheduled",
            Job.status == "queued",
        )
        .order_by(Job.created_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().one_or_none()


def _delete_status_payload(job: Job | None) -> dict[str, object]:
    if job is None:
        return {
            "status": "not_requested",
            "requested_at": None,
            "grace_ends_at": None,
        }
    if job.status == "cancelled":
        return {
            "status": "cancelled",
            "requested_at": job.created_at.isoformat(),
            "grace_ends_at": None,
        }
    if job.status == "done":
        return {
            "status": "completed",
            "requested_at": job.created_at.isoformat(),
            "grace_ends_at": (job.expires_at.isoformat() if job.expires_at is not None else None),
        }
    return {
        "status": "pending",
        "requested_at": job.created_at.isoformat(),
        "grace_ends_at": (job.expires_at.isoformat() if job.expires_at is not None else None),
    }


@router.post(
    "/delete/request",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Begin 14-day deletion grace period",
)
async def request_delete(
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    """Start the grace period. 409 if one is already pending."""
    existing = await _pending_delete_job(session, user_id)
    if existing is not None:
        raise LoftlyError(
            status_code=status.HTTP_409_CONFLICT,
            code="delete_already_pending",
            message_en="Account deletion is already scheduled.",
            message_th="มีคำขอลบบัญชีค้างอยู่แล้ว",
        )

    user = (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
    if user is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="user_not_found",
            message_en="User not found.",
            message_th="ไม่พบบัญชีผู้ใช้",
        )

    now = _utcnow()
    user.deleted_at = now
    job = Job(
        user_id=user_id,
        job_type="account_delete_scheduled",
        status="queued",
        expires_at=now + timedelta(days=GRACE_PERIOD_DAYS),
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    # DSAR tracking: delete request opened.
    dsar_observer("delete", "opened")

    return _delete_status_payload(job)


@router.post(
    "/delete/cancel",
    summary="Cancel pending deletion (before grace ends)",
)
async def cancel_delete(
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    """Only works while grace_ends_at > now. Clears `deleted_at` too."""
    job = await _pending_delete_job(session, user_id)
    if job is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="no_pending_delete",
            message_en="No pending deletion to cancel.",
            message_th="ไม่พบคำขอลบบัญชีที่ค้างอยู่",
        )
    now = _utcnow()
    job_expires_at = _as_aware(job.expires_at)
    if job_expires_at is not None and job_expires_at <= now:
        raise LoftlyError(
            status_code=status.HTTP_409_CONFLICT,
            code="grace_period_expired",
            message_en="Grace period already elapsed; cancellation no longer possible.",
            message_th="ระยะเวลาผ่อนผันสิ้นสุดแล้ว",
        )

    job.status = "cancelled"
    job.finished_at = now
    user = (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
    if user is not None:
        user.deleted_at = None
    await session.commit()
    await session.refresh(job)

    # DSAR tracking: delete request closed (cancelled) — count resolution time
    # so the PDPA dashboard shows short turnarounds as well as long ones.
    created = _as_aware(job.created_at)
    resolution_days = (
        max(0.0, (now - created).total_seconds() / 86_400.0) if created is not None else None
    )
    dsar_observer("delete", "closed", resolution_days=resolution_days)

    return _delete_status_payload(job)


@router.get(
    "/delete/status",
    summary="Inspect deletion request state",
)
async def delete_status(
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    job = await _pending_delete_job(session, user_id)
    if job is None:
        # Also surface the most recent terminal job (done / cancelled).
        stmt = (
            select(Job)
            .where(
                Job.user_id == user_id,
                Job.job_type == "account_delete_scheduled",
            )
            .order_by(Job.created_at.desc())
            .limit(1)
        )
        job = (await session.execute(stmt)).scalars().one_or_none()
    return _delete_status_payload(job)


__all__ = ["DATA_EXPORT_LIMITER", "router"]
