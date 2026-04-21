"""Account identity echo — `GET /v1/me`.

Powers the loftly-web account settings page (#16). The page currently shows
an amber fallback banner because this endpoint didn't exist; once shipped,
the banner is suppressed when the call resolves 200.

Scope discipline:
- Single-table read against `users`. No consent fan-out, no balances, no
  selector history — those have their own endpoints.
- 401 on missing/invalid JWT. 404 if the `sub` UUID doesn't correspond to
  a live user (shouldn't happen in practice, but defensive against token
  replay after account deletion).
"""

from __future__ import annotations

import uuid
from typing import cast

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_user_id
from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
from loftly.db.models.user import User
from loftly.schemas.me import Locale, MeResponse

router = APIRouter(prefix="/v1/me", tags=["me"])

# Providers whose email claim we trust as pre-verified. Magic-link counts too:
# the user had to click the link in their inbox to land a row in `users` with
# `oauth_provider='email_magic'`, so their email is demonstrably theirs.
_VERIFIED_PROVIDERS = {"google", "apple", "line", "email_magic"}


@router.get(
    "",
    response_model=MeResponse,
    summary="Current user profile (account settings echo)",
)
async def get_me(
    user_id: uuid.UUID = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> MeResponse:
    """Return the authenticated user's profile row.

    Reads `users` once. The JWT has already been verified upstream by
    `get_current_user_id`, so a missing row here means the user was deleted
    after the token was issued — treat as 404 (client should drop tokens).
    """
    user = (
        (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
    )
    if user is None or user.deleted_at is not None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="user_not_found",
            message_en="User not found.",
            message_th="ไม่พบบัญชีผู้ใช้",
        )

    return MeResponse(
        id=user.id,
        email=user.email,
        email_verified=user.oauth_provider in _VERIFIED_PROVIDERS,
        created_at=user.created_at,
        last_login_at=user.last_login_at,
        locale=cast(Locale, user.preferred_locale),
        auth_provider=user.oauth_provider or None,
    )


__all__ = ["router"]
