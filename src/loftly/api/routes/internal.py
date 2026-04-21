"""Internal service endpoints — scaffold stubs. See API_CONTRACT.md §Internal."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/internal", tags=["internal"])


@router.post("/sync/deal-harvester", summary="Trigger deal-harvester sync")
async def trigger_sync() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/sync/deal-harvester/last", summary="Last sync run status")
async def last_sync() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/valuation/run", summary="Trigger weekly valuation job")
async def run_valuation() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
