"""Valuation schemas — `openapi.yaml#Valuation`, `#ValuationDetail`.

Public API shapes consumed by `loftly-web` (`src/lib/api/types.ts` §Valuation).
Keep field names + types 1:1 with the TS interface — the frontend mirrors this
without translation.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from loftly.schemas.cards import Currency


class Valuation(BaseModel):
    """One current THB-per-point valuation — `openapi.yaml#Valuation`."""

    model_config = ConfigDict(from_attributes=True)

    currency: Currency
    thb_per_point: float
    methodology: str
    percentile: int
    sample_size: int
    confidence: float = Field(ge=0.0, le=1.0)
    top_redemption_example: str | None = None
    computed_at: datetime


class ValuationList(BaseModel):
    """List response for `GET /v1/valuations`."""

    data: list[Valuation]


class ValuationHistoryPoint(BaseModel):
    """One historical valuation observation — shown on the `/valuations/[code]` page."""

    thb_per_point: float
    computed_at: datetime


class ValuationDetail(Valuation):
    """Single-currency valuation with methodology extras — `openapi.yaml#ValuationDetail`."""

    distribution_summary: dict[str, float] | None = None
    history: list[ValuationHistoryPoint] = Field(default_factory=list)


__all__ = [
    "Valuation",
    "ValuationDetail",
    "ValuationHistoryPoint",
    "ValuationList",
]
