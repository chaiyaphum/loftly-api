"""Health probes — `GET /healthz`, `GET /readyz`.

See API_CONTRACT.md §Health.
"""

from __future__ import annotations

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from loftly.core.logging import get_logger
from loftly.db.engine import get_engine
from loftly.schemas.common import Health

router = APIRouter(tags=["health"])
log = get_logger(__name__)


@router.get("/healthz", response_model=Health, summary="Liveness")
async def healthz() -> Health:
    """Always 200 while the process is running. No downstream checks."""
    return Health(status="ok", checks={"process": "ok"})


@router.get(
    "/readyz",
    summary="Readiness (DB + downstream probes)",
    responses={
        200: {"model": Health, "description": "Ready"},
        503: {"model": Health, "description": "Not ready"},
    },
)
async def readyz() -> JSONResponse:
    """200 when DB reachable. Redis/Anthropic probes added Week 3+."""
    checks: dict[str, str] = {}
    ok = True

    # DB probe
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        log.warning("readyz_db_failed", error=str(e))
        checks["database"] = "degraded"
        ok = False

    payload = Health(status="ok" if ok else "degraded", checks=checks)
    return JSONResponse(
        status_code=status.HTTP_200_OK if ok else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=payload.model_dump(),
    )
