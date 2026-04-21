"""Auth & session endpoints.

OAuth + magic-link flows are Week 4+ manual items (OAuth app creation,
Resend template wiring). Only the test-only token-issuer is live today so
integration tests can mint real JWTs without a provider round-trip.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from loftly.api.errors import LoftlyError
from loftly.api.jwt_util import Locale, Role, TokenPair, issue_token_pair
from loftly.core.settings import Settings, get_settings

router = APIRouter(prefix="/v1/auth", tags=["auth"])


class _TestIssueRequest(BaseModel):
    user_id: uuid.UUID
    role: Role = "user"
    locale: Locale = "th"


class _TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int


def _not_implemented() -> LoftlyError:
    return LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Endpoint not yet implemented.",
        message_th="ยังไม่เปิดใช้งานจุดให้บริการนี้",
    )


@router.post("/oauth/callback", summary="Complete OAuth and mint JWT pair")
async def oauth_callback() -> None:
    raise _not_implemented()


@router.post("/magic-link/request", summary="Send magic link for email-only signup")
async def magic_link_request() -> None:
    raise _not_implemented()


@router.post("/magic-link/consume", summary="Redeem magic link token")
async def magic_link_consume() -> None:
    raise _not_implemented()


@router.post("/refresh", summary="Rotate access token")
async def refresh() -> None:
    raise _not_implemented()


@router.post("/logout", summary="Invalidate refresh token")
async def logout() -> None:
    raise _not_implemented()


@router.post(
    "/_test/issue",
    summary="(test-only) Mint a JWT pair for an arbitrary user_id + role",
    response_model=_TokenPairResponse,
)
async def _test_issue(
    payload: _TestIssueRequest,
    settings: Settings = Depends(get_settings),
) -> _TokenPairResponse:
    """Only enabled in `LOFTLY_ENV=test`. Lets integration tests mint real JWTs.

    Returning `404` in non-test envs means this endpoint effectively doesn't
    exist in prod, matching "keep test-only surface off in production".
    """
    if not settings.is_test:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
            message_en="Route not found.",
            message_th="ไม่พบเส้นทาง",
        )
    pair: TokenPair = issue_token_pair(
        user_id=payload.user_id,
        role=payload.role,
        locale=payload.locale,
        settings=settings,
    )
    return _TokenPairResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
    )
