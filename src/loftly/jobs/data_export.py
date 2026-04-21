"""PDPA data-export job.

Bundles every row referencing a user_id into a JSON file on local tmp, mints a
HMAC-signed download URL valid 48h, and marks the `jobs` row done.

Phase 1 storage is a local tmp directory (`/tmp/loftly_exports/<job_id>.json`).
This works on a single Fly instance; when we scale horizontally the file needs
to move to R2/S3. See the TODO below.

<!-- TODO: replace local tmp with R2 signed URL in prod -->
"""

from __future__ import annotations

import hmac
import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import select

from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateClick, AffiliateConversion
from loftly.db.models.consent import UserConsent
from loftly.db.models.job import Job
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.db.models.user_card import UserCard
from loftly.observability.prometheus import dsar_observer

log = get_logger(__name__)

EXPORT_DIR = Path("/tmp/loftly_exports")
EXPORT_VERSION = "1.0"
DOWNLOAD_TTL_SECONDS = 48 * 60 * 60


def _row_as_dict(row: object) -> dict[str, Any]:
    """Best-effort ORM row → plain dict, dropping SA internals."""
    out: dict[str, Any] = {}
    for column in row.__class__.__table__.columns:  # type: ignore[attr-defined]
        val = getattr(row, column.name if column.name != "metadata" else "meta", None)
        if isinstance(val, uuid.UUID):
            out[column.name] = str(val)
        elif isinstance(val, datetime):
            out[column.name] = val.isoformat()
        elif isinstance(val, bytes):
            out[column.name] = val.hex()
        else:
            out[column.name] = val
    return out


def sign_download_token(job_id: uuid.UUID, *, expires_at: datetime, secret: str) -> str:
    """HMAC(job_id || expires_unix) — hex digest, used as the `token` query param."""
    payload = f"{job_id}.{int(expires_at.timestamp())}"
    mac = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), sha256).hexdigest()
    return f"{int(expires_at.timestamp())}.{mac}"


def verify_download_token(job_id: uuid.UUID, token: str, *, secret: str) -> bool:
    """Constant-time verification of a download token, incl. expiry check."""
    try:
        exp_str, mac = token.split(".", 1)
        exp = int(exp_str)
    except (ValueError, AttributeError):
        return False
    if exp < int(time.time()):
        return False
    payload = f"{job_id}.{exp}"
    expected = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), sha256).hexdigest()
    return hmac.compare_digest(expected, mac)


async def _gather_bundle(user_id: uuid.UUID) -> dict[str, Any]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (
            (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
        )
        if user is None:
            raise RuntimeError(f"user {user_id} missing at export time")

        consents = list(
            (await session.execute(select(UserConsent).where(UserConsent.user_id == user_id)))
            .scalars()
            .all()
        )
        user_cards = list(
            (await session.execute(select(UserCard).where(UserCard.user_id == user_id)))
            .scalars()
            .all()
        )
        selector_sessions = list(
            (
                await session.execute(
                    select(SelectorSession).where(SelectorSession.user_id == user_id)
                )
            )
            .scalars()
            .all()
        )
        clicks = list(
            (await session.execute(select(AffiliateClick).where(AffiliateClick.user_id == user_id)))
            .scalars()
            .all()
        )
        click_ids = [c.click_id for c in clicks]
        conversions: list[AffiliateConversion] = []
        if click_ids:
            conversions = list(
                (
                    await session.execute(
                        select(AffiliateConversion).where(
                            AffiliateConversion.click_id.in_(click_ids)
                        )
                    )
                )
                .scalars()
                .all()
            )

    return {
        "export_version": EXPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "user": _row_as_dict(user),
        "consents": [_row_as_dict(r) for r in consents],
        "user_cards": [_row_as_dict(r) for r in user_cards],
        "selector_sessions": [_row_as_dict(r) for r in selector_sessions],
        "affiliate_clicks": [_row_as_dict(r) for r in clicks],
        "affiliate_conversions": [_row_as_dict(r) for r in conversions],
    }


async def run_export(job_id: uuid.UUID) -> None:
    """Build the bundle, write to tmp, mint a signed URL, mark done.

    Called from FastAPI `BackgroundTasks`. Any exception is caught and
    persisted onto the job row as `error_message`.
    """
    settings = get_settings()
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalars().one_or_none()
        if job is None:
            log.warning("data_export_job_missing", job_id=str(job_id))
            return
        user_id = job.user_id
        job.status = "running"
        job.started_at = datetime.now(UTC)
        await session.commit()

    try:
        bundle = await _gather_bundle(user_id)
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        file_path = EXPORT_DIR / f"{job_id}.json"
        file_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

        expires_at = datetime.now(UTC) + timedelta(seconds=DOWNLOAD_TTL_SECONDS)
        token = sign_download_token(job_id, expires_at=expires_at, secret=settings.jwt_signing_key)
        download_url = f"/v1/account/data-export/{job_id}/download?token={token}"

        finished_at = datetime.now(UTC)
        async with sessionmaker() as session:
            job = (await session.execute(select(Job).where(Job.id == job_id))).scalars().one()
            job.status = "done"
            job.result_url = download_url
            job.expires_at = expires_at
            job.finished_at = finished_at
            created_at = job.created_at
            await session.commit()

        # DSAR metric close — use wall-clock days between request + finish.
        if created_at is not None:
            created_aware = (
                created_at if created_at.tzinfo is not None else created_at.replace(tzinfo=UTC)
            )
            days = max(0.0, (finished_at - created_aware).total_seconds() / 86_400.0)
            dsar_observer("export", "closed", resolution_days=days)
        else:
            dsar_observer("export", "closed")

        log.info(
            "data_export_complete",
            job_id=str(job_id),
            user_id=str(user_id),
            bytes=file_path.stat().st_size,
        )
    except Exception as exc:
        log.exception("data_export_failed", job_id=str(job_id))
        async with sessionmaker() as session:
            job = (
                (await session.execute(select(Job).where(Job.id == job_id))).scalars().one_or_none()
            )
            if job is not None:
                job.status = "failed"
                job.error_message = str(exc)[:500]
                job.finished_at = datetime.now(UTC)
                await session.commit()


def export_file_path(job_id: uuid.UUID) -> Path:
    return EXPORT_DIR / f"{job_id}.json"


__all__ = [
    "DOWNLOAD_TTL_SECONDS",
    "EXPORT_VERSION",
    "export_file_path",
    "run_export",
    "sign_download_token",
    "verify_download_token",
]
