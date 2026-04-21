"""Auth dependencies — stubs for Phase 1 scaffold.

Real JWT + API-key verification lands in Week 2 per DEV_PLAN.md. Until then
protected routes raise `HTTPException(501)` so contracts remain honest.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status


async def require_user_jwt(
    authorization: str | None = Header(default=None),
) -> None:
    """Dependency for routes tagged `userJWT` in openapi.yaml.

    TODO(week-2): verify HS256 JWT with `settings.jwt_signing_key`, decode
    `sub`/`role`/`consents`, attach a `CurrentUser` object to request.state.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="JWT authentication not yet implemented",
    )


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
