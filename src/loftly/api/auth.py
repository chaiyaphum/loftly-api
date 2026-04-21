"""Auth dependencies — Phase 1 scaffold with a usable current-user hook.

Real JWT issuance + refresh rotation still lands later; right now we expose a
single `get_current_user_id` dependency that decodes a bearer token using
`settings.jwt_signing_key`. Tests bypass it via `app.dependency_overrides`.
"""

from __future__ import annotations

import uuid

from fastapi import Depends, Header, HTTPException, status
from jose import JWTError, jwt

from loftly.core.settings import Settings, get_settings


async def get_current_user_id(
    authorization: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> uuid.UUID:
    """Extract `sub` from a bearer JWT, returning the user UUID.

    Tests override this via `app.dependency_overrides[get_current_user_id]`
    so routes can be exercised without a real token. In prod this will be
    the authoritative gate.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token.",
        )
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(
            token,
            settings.jwt_signing_key,
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        ) from exc

    sub = payload.get("sub")
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing `sub` claim.",
        )
    try:
        return uuid.UUID(str(sub))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token `sub` is not a UUID.",
        ) from exc


async def require_user_jwt(
    _: uuid.UUID = Depends(get_current_user_id),
) -> None:
    """Dependency for routes tagged `userJWT` in openapi.yaml."""
    # The resolver above does the work; presence = ok.
    return None


async def require_admin_jwt(
    authorization: str | None = Header(default=None),
) -> None:
    """Dependency for routes tagged `adminJWT`. Requires `role=admin` claim."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Admin JWT authentication not yet implemented",
    )


async def require_internal_api_key(
    x_api_key: str | None = Header(default=None),
) -> None:
    """Dependency for routes tagged `internalApiKey`. Static key per service."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Internal API key verification not yet implemented",
    )


async def verify_webhook_signature(
    x_loftly_signature: str | None = Header(default=None),
) -> None:
    """Dependency for `webhookHMAC`-secured routes. Constant-time HMAC compare."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Webhook HMAC verification not yet implemented",
    )
