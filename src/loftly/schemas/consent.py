"""Consent schemas — `openapi.yaml#ConsentState`, `#ConsentUpdate`.

PDPA-aligned. See SPEC.md §1 + §7 for the append-only log + optimization-
required invariant.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

Purpose = Literal["optimization", "marketing", "analytics", "sharing"]
Source = Literal["onboarding", "account_settings", "selector", "admin"]


class ConsentFlags(BaseModel):
    """Latest-row state per purpose. False when no row has been written yet."""

    optimization: bool = False
    marketing: bool = False
    analytics: bool = False
    sharing: bool = False


class ConsentState(BaseModel):
    """openapi.yaml#ConsentState."""

    policy_version: str
    consents: ConsentFlags = Field(default_factory=ConsentFlags)


class ConsentUpdate(BaseModel):
    """openapi.yaml#ConsentUpdate.

    `source` defaults to `account_settings` when omitted, matching how the
    web settings screen will post. `onboarding`, `selector`, `admin` are
    explicit.
    """

    purpose: Purpose
    granted: bool
    policy_version: str
    source: Source = "account_settings"
