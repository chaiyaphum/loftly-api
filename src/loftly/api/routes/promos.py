"""Promo endpoints — scaffold stubs. See API_CONTRACT.md §Promos."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/promos", tags=["promos"])


@router.get("", summary="Active promotions (filterable)")
async def list_promos() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
