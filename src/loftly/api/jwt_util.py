"""JWT issuance + verification helpers.

Thin wrapper around `python-jose` that:
- Issues an access + refresh pair keyed off `user_id` with `role` + `locale` claims.
- Decodes access tokens back into the claim dict, raising on expiry / tamper.

The signing key + algorithm come from `Settings` so prod can rotate the HMAC
secret without code changes. Refresh tokens are opaque-looking JWTs here
(Phase 1); a future revocation table will let us invalidate them server-side.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from jose import jwt

from loftly.core.settings import Settings, get_settings

Role = Literal["user", "admin"]
Locale = Literal["th", "en"]

_TOKEN_TYPE_ACCESS = "access"
_TOKEN_TYPE_REFRESH = "refresh"


@dataclass(frozen=True)
class TokenPair:
    """Access + refresh pair returned by auth flows. Matches `openapi.yaml#TokenPair`."""

    access_token: str
    refresh_token: str
    expires_in: int  # seconds until access_token expires


def _now() -> datetime:
    return datetime.now(UTC)


def _encode(
    payload: dict[str, Any],
    *,
    settings: Settings,
) -> str:
    return jwt.encode(
        payload,
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )


def issue_token_pair(
    *,
    user_id: uuid.UUID,
    role: Role = "user",
    locale: Locale = "th",
    settings: Settings | None = None,
) -> TokenPair:
    """Mint a fresh `(access, refresh)` pair for `user_id`."""
    s = settings or get_settings()
    now = _now()
    access_exp = now + timedelta(seconds=s.jwt_access_ttl_sec)
    refresh_exp = now + timedelta(seconds=s.jwt_refresh_ttl_sec)

    base = {
        "sub": str(user_id),
        "role": role,
        "locale": locale,
        "iat": int(now.timestamp()),
    }
    access_payload: dict[str, Any] = {
        **base,
        "exp": int(access_exp.timestamp()),
        "type": _TOKEN_TYPE_ACCESS,
    }
    refresh_payload: dict[str, Any] = {
        **base,
        "exp": int(refresh_exp.timestamp()),
        "type": _TOKEN_TYPE_REFRESH,
    }

    return TokenPair(
        access_token=_encode(access_payload, settings=s),
        refresh_token=_encode(refresh_payload, settings=s),
        expires_in=s.jwt_access_ttl_sec,
    )


def decode_access_token(token: str, *, settings: Settings | None = None) -> dict[str, Any]:
    """Decode + verify an access token. Raises `jose.JWTError` on failure."""
    s = settings or get_settings()
    claims: dict[str, Any] = jwt.decode(
        token,
        s.jwt_signing_key,
        algorithms=[s.jwt_algorithm],
    )
    if claims.get("type") != _TOKEN_TYPE_ACCESS:
        # jose.JWTError is what the caller already catches; keep the surface consistent.
        from jose import JWTError

        raise JWTError("token is not of type=access")
    return claims


__all__ = [
    "Locale",
    "Role",
    "TokenPair",
    "decode_access_token",
    "issue_token_pair",
]
