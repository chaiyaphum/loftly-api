"""Auth dependencies — real JWT verification + role guards.

`get_current_user_id` is the canonical extractor: reads `Authorization: Bearer
<token>`, verifies signature + expiry, and returns the user UUID. Unauthenticated
or expired requests receive a `LoftlyError` rendering the contract `Error`
envelope (both `message_en` and `message_th`).

`get_current_admin_id` builds on that by additionally requiring `role=admin` in
the claim set. Non-admin users get a `403` with the same envelope.

Tests can still bypass these via `app.dependency_overrides[...]`.
"""

from __future__ import annotations

import uuid

from fastapi import Depends, Header, status
from jose import JWTError

from loftly.api.errors import LoftlyError
from loftly.api.jwt_util import decode_access_token
from loftly.core.settings import Settings, get_settings


def _unauthorized(message_en: str, message_th: str) -> LoftlyError:
    return LoftlyError(
        status_code=status.HTTP_401_UNAUTHORIZED,
        code="unauthorized",
        message_en=message_en,
        message_th=message_th,
    )


def _forbidden(message_en: str, message_th: str) -> LoftlyError:
    return LoftlyError(
        status_code=status.HTTP_403_FORBIDDEN,
        code="forbidden",
        message_en=message_en,
        message_th=message_th,
    )


async def get_current_user_id(
    authorization: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> uuid.UUID:
    """Extract `sub` from a bearer JWT, returning the user UUID.

    Tests override via `app.dependency_overrides[get_current_user_id]`.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise _unauthorized(
            "Missing bearer token.",
            "ไม่พบโทเคนยืนยันตัวตน",
        )
    token = authorization.split(" ", 1)[1].strip()
    try:
        claims = decode_access_token(token, settings=settings)
    except JWTError as exc:
        raise _unauthorized(
            "Invalid or expired token.",
            "โทเคนหมดอายุหรือไม่ถูกต้อง",
        ) from exc

    sub = claims.get("sub")
    if not sub:
        raise _unauthorized(
            "Token missing `sub` claim.",
            "โทเคนไม่มีข้อมูลผู้ใช้",
        )
    try:
        return uuid.UUID(str(sub))
    except ValueError as exc:
        raise _unauthorized(
            "Token `sub` is not a UUID.",
            "รหัสผู้ใช้ในโทเคนไม่ถูกต้อง",
        ) from exc


async def get_current_admin_id(
    authorization: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> uuid.UUID:
    """Like `get_current_user_id` but requires `role=admin`.

    Missing/expired token -> 401. Authenticated-but-not-admin -> 403.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise _unauthorized(
            "Missing bearer token.",
            "ไม่พบโทเคนยืนยันตัวตน",
        )
    token = authorization.split(" ", 1)[1].strip()
    try:
        claims = decode_access_token(token, settings=settings)
    except JWTError as exc:
        raise _unauthorized(
            "Invalid or expired token.",
            "โทเคนหมดอายุหรือไม่ถูกต้อง",
        ) from exc

    sub = claims.get("sub")
    role = claims.get("role")
    if not sub:
        raise _unauthorized(
            "Token missing `sub` claim.",
            "โทเคนไม่มีข้อมูลผู้ใช้",
        )
    if role != "admin":
        raise _forbidden(
            "Admin role required.",
            "ต้องเป็นผู้ดูแลระบบเท่านั้น",
        )
    try:
        return uuid.UUID(str(sub))
    except ValueError as exc:
        raise _unauthorized(
            "Token `sub` is not a UUID.",
            "รหัสผู้ใช้ในโทเคนไม่ถูกต้อง",
        ) from exc


async def require_user_jwt(
    _: uuid.UUID = Depends(get_current_user_id),
) -> None:
    """Dependency for routes tagged `userJWT` in openapi.yaml."""
    return None


async def require_admin_jwt(
    _: uuid.UUID = Depends(get_current_admin_id),
) -> None:
    """Dependency for routes tagged `adminJWT`. Requires `role=admin` claim."""
    return None


async def require_internal_api_key(
    x_api_key: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    """Dependency for routes tagged `internalApiKey`. Static key per service.

    Phase 1 wiring still pending — keep 501 so ops don't think it's hardened.
    """
    _ = x_api_key, settings
    raise LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Internal API key verification not yet implemented.",
        message_th="ยังไม่ได้เปิดใช้งานการยืนยันคีย์ภายในระบบ",
    )


async def verify_webhook_signature(
    x_loftly_signature: str | None = Header(default=None),
) -> None:
    """Dependency for `webhookHMAC`-secured routes.

    Left as a no-op placeholder — concrete verification lives inside the
    webhook route itself because it needs the raw request body.
    """
    _ = x_loftly_signature
    return None


__all__ = [
    "get_current_admin_id",
    "get_current_user_id",
    "require_admin_jwt",
    "require_internal_api_key",
    "require_user_jwt",
    "verify_webhook_signature",
]
