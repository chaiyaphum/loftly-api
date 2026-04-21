"""Internal service endpoints.

Protected via `X-API-Key` header (see `require_internal_api_key_or_test`). The
key is the `jwt_signing_key` reused to avoid proliferation — the fly secret
already carries it.

- `POST /v1/internal/sync/deal-harvester` — kick off a background sync.
- `GET  /v1/internal/sync/deal-harvester/last` — peek latest SyncRun.
- `POST /v1/internal/valuation/run` — scaffold stub kept for parity.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, Header, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.audit import SyncRun
from loftly.jobs.deal_harvester_sync import run_sync

router = APIRouter(prefix="/v1/internal", tags=["internal"])


async def require_internal_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    settings: Settings = Depends(get_settings),
) -> None:
    """Thin internal-service guard. Phase 1 reuses JWT_SIGNING_KEY as the API key.

    Ship a dedicated secret in prod once rotation is set up.
    """
    if not x_api_key or x_api_key != settings.jwt_signing_key:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
            message_en="Missing or invalid X-API-Key.",
            message_th="ไม่พบคีย์ภายในระบบ",
        )


@router.post(
    "/sync/deal-harvester",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger deal-harvester sync",
)
async def trigger_sync(
    background_tasks: BackgroundTasks,
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, str]:
    """Queue the sync. Returns a `JobHandle` immediately — poll `/last` later."""
    background_tasks.add_task(run_sync)
    return {"job_id": "deal-harvester", "status": "queued"}


@router.get("/sync/deal-harvester/last", summary="Last sync run status")
async def last_sync(
    _auth: None = Depends(require_internal_api_key),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                select(SyncRun)
                .where(SyncRun.source == "deal_harvester")
                .order_by(SyncRun.started_at.desc())
                .limit(1)
            )
        )
        .scalars()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="no_sync_runs",
            message_en="No deal-harvester sync run recorded yet.",
            message_th="ยังไม่มีการซิงก์ข้อมูล",
        )
    return {
        "id": str(row.id),
        "source": row.source,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "status": row.status,
        "upstream_count": row.upstream_count,
        "inserted_count": row.inserted_count,
        "updated_count": row.updated_count,
        "deactivated_count": row.deactivated_count,
        "mapping_queue_added": row.mapping_queue_added,
        "error_message": row.error_message,
    }


@router.post(
    "/valuation/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger weekly valuation job",
)
async def run_valuation(
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, str]:
    """Scaffold — Phase 2 will hook into `jobs.valuation.run_all`."""
    return {"job_id": "valuation", "status": "queued"}
