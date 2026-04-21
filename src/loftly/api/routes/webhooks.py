"""Webhook endpoints — scaffold stubs. See API_CONTRACT.md §Webhook contracts."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/webhooks", tags=["webhook"])


@router.post("/affiliate/{partner_id}", summary="Partner conversion postback")
async def affiliate_postback(partner_id: str) -> None:
    _ = partner_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
