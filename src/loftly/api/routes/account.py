"""Account/PDPA self-service — scaffold stubs."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/account", tags=["account"])


@router.post("/data-export/request", summary="Queue data export job")
async def request_export() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/data-export/{job_id}", summary="Export job status")
async def export_status(job_id: str) -> None:
    _ = job_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/delete/request", summary="Begin 14-day deletion grace period")
async def request_delete() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/delete/cancel", summary="Cancel pending deletion")
async def cancel_delete() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
