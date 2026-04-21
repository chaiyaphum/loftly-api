"""Pydantic schemas for `GET /v1/me` — account settings surface.

loftly-web #16 ships a settings page that needs a lightweight identity echo
so the header and form defaults populate before the user touches anything.
The shape intentionally stays small — no balances, no points, no consent log
— because this endpoint is on the critical path of every settings-page load
and we don't want it fanning out to secondary tables.

See `docs/reference/` in the strategy repo for the broader PDPA envelope.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

Locale = Literal["th", "en"]


class MeResponse(BaseModel):
    """Body for `GET /v1/me`.

    Only the fields the loftly-web account settings page actually renders.
    Intentionally NOT returning `role` — admin status is orthogonal to the
    settings UI and leaking it widens the attack surface if the token is
    replayed into a logged surface.

    `email` is typed as plain `str` rather than `EmailStr` because OAuth
    flows can mint placeholder addresses like `google-<subject>@loftly.local`
    (when the provider doesn't return a verified email) and reserved-TLD
    test fixtures like `test@loftly.test` need to echo back cleanly too.
    Input-side validation is enforced at signup, not here on the read path.
    """

    id: uuid.UUID
    email: str
    email_verified: bool
    created_at: datetime
    last_login_at: datetime | None
    locale: Locale
    auth_provider: str | None
