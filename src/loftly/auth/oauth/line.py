"""LINE OAuth — exchange authorization code for access + id token, return user.

Most Thai mobile sign-in traffic comes via LINE; keeping this wired is a
Phase-2 commitment per SPEC.md §1. We POST the code to LINE's token endpoint
then fetch the userinfo endpoint to get `userId` + `email` (optional scope).

Real mode: needs `LOFTLY_OAUTH_LINE_CLIENT_ID` + `LOFTLY_OAUTH_LINE_CLIENT_SECRET`.
Stub mode: either unset → `OAuthNotConfigured("line")`.
"""

from __future__ import annotations

import httpx

from loftly.auth.oauth import OAuthExchangeFailed, OAuthNotConfigured, OAuthUserInfo
from loftly.core.settings import get_settings

_TOKEN_URL = "https://api.line.me/oauth2/v2.1/token"
_PROFILE_URL = "https://api.line.me/v2/profile"


async def exchange_code(code: str, redirect_uri: str) -> OAuthUserInfo:
    settings = get_settings()
    client_id = settings.loftly_oauth_line_client_id
    client_secret = settings.loftly_oauth_line_client_secret
    if not client_id or not client_secret:
        raise OAuthNotConfigured("line")

    async with httpx.AsyncClient(timeout=10.0) as http:
        token_resp = await http.post(
            _TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if token_resp.status_code != 200:
            raise OAuthExchangeFailed(f"LINE token exchange failed: {token_resp.status_code}")
        access_token = token_resp.json().get("access_token")
        if not access_token:
            raise OAuthExchangeFailed("LINE response missing access_token.")

        profile_resp = await http.get(
            _PROFILE_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if profile_resp.status_code != 200:
        raise OAuthExchangeFailed(f"LINE profile fetch failed: {profile_resp.status_code}")
    profile = profile_resp.json()
    subject = profile.get("userId")
    if not subject:
        raise OAuthExchangeFailed("LINE profile missing userId.")
    # Email scope is optional; LINE only returns it when the user approved it.
    email = profile.get("email")
    return OAuthUserInfo(provider="line", subject=str(subject), email=email)


__all__ = ["exchange_code"]
