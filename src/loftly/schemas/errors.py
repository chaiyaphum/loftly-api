"""Error envelope — `openapi.yaml#Error`.

Voice rules from BRAND.md §4 apply to user-facing `message_th` — direct,
warm, specific; no translation-smell.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ErrorBody(BaseModel):
    code: str
    message_en: str
    message_th: str | None = None
    details: dict[str, Any] | None = None


class Error(BaseModel):
    error: ErrorBody
