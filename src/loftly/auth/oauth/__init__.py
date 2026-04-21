"""OAuth provider registry.

Each provider module exposes `exchange_code(code, redirect_uri)` which returns
an `OAuthUserInfo` with the upstream subject identifier + email. Providers
raise `OAuthNotConfigured` when the required env vars are missing — the route
surfaces that as a 503 so the frontend can degrade gracefully.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

Provider = Literal["google", "apple", "line"]


@dataclass(frozen=True)
class OAuthUserInfo:
    """Normalized identity returned by every provider.

    `subject` is the provider-specific stable identifier (Google `sub`, Apple
    `sub`, LINE `userId`). Email may be None for LINE (OIDC scope not granted)
    or Apple (user hid their real address).
    """

    provider: Provider
    subject: str
    email: str | None


class OAuthNotConfigured(Exception):
    """Raised when an OAuth provider is called but its env vars are unset."""

    def __init__(self, provider: Provider) -> None:
        super().__init__(f"OAuth provider not configured: {provider}")
        self.provider = provider


class OAuthExchangeFailed(Exception):
    """Raised on upstream exchange error (network, invalid code, bad state)."""


class ProviderModule(Protocol):
    """Structural type every oauth/{provider}.py module implements."""

    async def exchange_code(self, code: str, redirect_uri: str) -> OAuthUserInfo: ...


__all__ = [
    "OAuthExchangeFailed",
    "OAuthNotConfigured",
    "OAuthUserInfo",
    "Provider",
    "ProviderModule",
]
