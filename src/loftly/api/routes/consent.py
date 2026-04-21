"""Consent endpoints — scaffold stubs. See API_CONTRACT.md §Consent & account."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/consent", tags=["consent"])


@router.get("", summary="Current consent state (4 purposes)")
async def get_consent() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("", summary="Update consent (append-only)")
async def update_consent() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
