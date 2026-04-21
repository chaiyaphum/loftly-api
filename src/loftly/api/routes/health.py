"""Health probes — `GET /healthz`, `GET /readyz`.

See API_CONTRACT.md §Health.

Graceful-shutdown behavior: the lifespan teardown flips `_shutting_down` to
True *before* closing DB / background tasks. `/readyz` then returns 503 with
`database=shutting_down` immediately — that is the signal DO App Platform's
load balancer waits on before pulling the pod out of rotation for a rolling
deploy. See DRILL-003.
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

# Module-level flag flipped by `lifespan` teardown in `loftly.api.app`. Kept
# here (not in app.py) so `/readyz` can read it without importing the app and
# tests can flip it directly.
_shutting_down: bool = False


def set_shutting_down(value: bool) -> None:
    """Set the shutdown flag. Called by the lifespan teardown hook."""
    global _shutting_down
    _shutting_down = value


def is_shutting_down() -> bool:
    """Return the current shutdown flag (exported for tests)."""
    return _shutting_down


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
    """200 when DB reachable. Redis/Anthropic probes added Week 3+.

    Short-circuits to 503 `{"database":"shutting_down"}` during graceful
    shutdown so DO's load balancer drains this pod before we close the DB
    pool.
    """
    if _shutting_down:
        payload = Health(status="degraded", checks={"database": "shutting_down"})
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=payload.model_dump(),
        )

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
