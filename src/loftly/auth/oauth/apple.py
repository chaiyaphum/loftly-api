"""Apple OAuth — verifies the upstream `id_token` JWT against Apple's JWKS.

Apple's Sign in with Apple flow POSTs an `id_token` + `code` to our callback.
We verify the JWT signature using Apple's rotating public keys
(https://appleid.apple.com/auth/keys), then trust its `sub` + `email` claims.

Real mode: needs `LOFTLY_OAUTH_APPLE_CLIENT_ID` (the bundle identifier / service ID).
Team/Key/PrivateKey env vars are needed only if we ever switch from the
"assertion-less" flow to one where we mint a client_secret JWT — keep them in
settings so ops can add them later without a deploy.

Stub mode: `LOFTLY_OAUTH_APPLE_CLIENT_ID` unset → `OAuthNotConfigured("apple")`.
"""

from __future__ import annotations

from typing import Any

import httpx
from jose import jwt

from loftly.auth.oauth import OAuthExchangeFailed, OAuthNotConfigured, OAuthUserInfo
from loftly.core.settings import get_settings

_APPLE_JWKS_URL = "https://appleid.apple.com/auth/keys"
_APPLE_ISSUER = "https://appleid.apple.com"


async def _fetch_apple_jwks() -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as http:
        resp = await http.get(_APPLE_JWKS_URL)
    if resp.status_code != 200:
        raise OAuthExchangeFailed(f"Apple JWKS fetch failed: {resp.status_code}")
    data: dict[str, Any] = resp.json()
    return data


async def exchange_code(code: str, redirect_uri: str) -> OAuthUserInfo:
    # Apple's client shape doesn't actually need `redirect_uri` for id_token
    # verification, but we keep the signature uniform across providers.
    _ = redirect_uri
    settings = get_settings()
    client_id = settings.loftly_oauth_apple_client_id
    if not client_id:
        raise OAuthNotConfigured("apple")

    # Apple's web flow passes the id_token as `code` from our frontend. For the
    # full `grant_type=authorization_code` flow we'd POST to /auth/token with a
    # signed client_secret JWT; shipping the simpler id_token-verify flow now
    # and leaving room for upgrade via settings.
    jwks = await _fetch_apple_jwks()
    try:
        claims: dict[str, Any] = jwt.decode(
            code,
            jwks,
            algorithms=["RS256"],
            audience=client_id,
            issuer=_APPLE_ISSUER,
        )
    except Exception as exc:
        raise OAuthExchangeFailed(f"Apple id_token verification failed: {exc}") from exc

    sub = claims.get("sub")
    email = claims.get("email")
    if not sub:
        raise OAuthExchangeFailed("Apple id_token missing subject.")
    return OAuthUserInfo(provider="apple", subject=str(sub), email=email)


__all__ = ["exchange_code"]
