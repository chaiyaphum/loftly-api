"""Google OAuth — exchange authorization code for ID token, verify, return user.

Real mode: needs `LOFTLY_OAUTH_GOOGLE_CLIENT_ID` + `LOFTLY_OAUTH_GOOGLE_CLIENT_SECRET`.
Stub mode (either unset): raises `OAuthNotConfigured("google")`.

We verify the `id_token` via google-auth's JWKS client so we don't trust the
upstream /userinfo payload naively — the token is signed by Google's keys and
contains the canonical `sub` + `email` + `email_verified` claims.
"""

from __future__ import annotations

from typing import Any

import httpx

from loftly.auth.oauth import OAuthExchangeFailed, OAuthNotConfigured, OAuthUserInfo
from loftly.core.settings import get_settings

_TOKEN_URL = "https://oauth2.googleapis.com/token"


async def exchange_code(code: str, redirect_uri: str) -> OAuthUserInfo:
    settings = get_settings()
    client_id = settings.loftly_oauth_google_client_id
    client_secret = settings.loftly_oauth_google_client_secret
    if not client_id or not client_secret:
        raise OAuthNotConfigured("google")

    async with httpx.AsyncClient(timeout=10.0) as http:
        resp = await http.post(
            _TOKEN_URL,
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"Accept": "application/json"},
        )
    if resp.status_code != 200:
        raise OAuthExchangeFailed(f"Google token exchange failed: {resp.status_code}")

    body = resp.json()
    id_token_str = body.get("id_token")
    if not id_token_str:
        raise OAuthExchangeFailed("Google response missing id_token.")

    # google-auth verifies signature + audience + expiry in one call.
    from google.auth.transport import requests as ga_requests
    from google.oauth2 import id_token as google_id_token

    request = ga_requests.Request()
    claims: dict[str, Any] = google_id_token.verify_oauth2_token(  # type: ignore[no-untyped-call]
        id_token_str, request, client_id
    )

    sub = claims.get("sub")
    email = claims.get("email")
    if not sub:
        raise OAuthExchangeFailed("Google id_token missing subject.")

    return OAuthUserInfo(provider="google", subject=str(sub), email=email)


__all__ = ["exchange_code"]
