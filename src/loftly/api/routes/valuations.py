"""Valuations endpoints — scaffold stubs. See API_CONTRACT.md §Valuations."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/valuations", tags=["valuations"])


@router.get("", summary="All current valuations")
async def list_valuations() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/{currency_code}", summary="Valuation + methodology for one currency")
async def get_valuation(currency_code: str) -> None:
    _ = currency_code
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
