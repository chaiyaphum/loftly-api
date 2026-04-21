"""Affiliate endpoints — scaffold stubs. See API_CONTRACT.md §Affiliate."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/affiliate", tags=["affiliate"])


@router.post("/click/{card_id}", summary="Record click; redirect to partner URL")
async def record_click(card_id: str) -> None:
    _ = card_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
