"""Auth & session endpoints — scaffold stubs. See API_CONTRACT.md §Auth & session."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/auth", tags=["auth"])


@router.post("/oauth/callback", summary="Complete OAuth and mint JWT pair")
async def oauth_callback() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/magic-link/request", summary="Send magic link for email-only signup")
async def magic_link_request() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/magic-link/consume", summary="Redeem magic link token")
async def magic_link_consume() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/refresh", summary="Rotate access token")
async def refresh() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/logout", summary="Invalidate refresh token")
async def logout() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
