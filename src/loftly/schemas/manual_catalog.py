"""Pydantic schemas for the manual-catalog ingest path.

Fixtures under `src/loftly/data/manual_catalogs/{bank}.json` and admin CSV
uploads are both validated into `ManualPromo` before being diffed against the
`promos` table. Keep field names aligned with the W18 DEV_PLAN contract:
`{title, bank, card_types[], category, start_date, end_date, discount_pct,
min_spend_thb, cashback_thb, cap_thb, source_url, notes}`.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Reasonable bounds — anything outside is almost certainly a fat-finger in the
# fixture rather than a real bank promo. Fail loud rather than silently ingest.
_MAX_PCT = Decimal("100")
_MAX_THB = Decimal("10000000")


class ManualPromo(BaseModel):
    """One row of a manual-catalog fixture.

    The natural key for diffing against existing `promos` rows is
    `(bank, title, start_date)` — the ingest job looks up existing rows on
    that tuple, updates in place, or inserts.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    title: Annotated[str, Field(min_length=1, max_length=500)]
    bank: Annotated[str, Field(min_length=1, max_length=64)]
    card_types: list[str] = Field(default_factory=list)
    category: str | None = Field(default=None, max_length=64)
    start_date: date
    end_date: date
    discount_pct: Decimal | None = Field(default=None)
    min_spend_thb: Decimal | None = Field(default=None)
    cashback_thb: Decimal | None = Field(default=None)
    cap_thb: Decimal | None = Field(default=None)
    source_url: Annotated[str, Field(min_length=1, max_length=1024)]
    notes: str | None = None

    @field_validator("discount_pct")
    @classmethod
    def _check_pct(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v < 0 or v > _MAX_PCT:
            raise ValueError("discount_pct must be within [0, 100]")
        return v

    @field_validator("min_spend_thb", "cashback_thb", "cap_thb")
    @classmethod
    def _check_thb(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v < 0 or v > _MAX_THB:
            raise ValueError("THB amount out of reasonable range")
        return v

    @field_validator("end_date")
    @classmethod
    def _check_dates(cls, v: date, info: object) -> date:
        # `info` is `ValidationInfo` in pydantic-v2; we access its `data` dict.
        values = getattr(info, "data", None) or {}
        start = values.get("start_date")
        if start is not None and v < start:
            raise ValueError("end_date must be on or after start_date")
        return v


class ManualCatalogFile(BaseModel):
    """Schema for the whole JSON fixture file."""

    model_config = ConfigDict(extra="ignore")

    bank: str
    source: str = "manual_catalog"
    notes: str | None = None
    promos: list[ManualPromo]


class IngestResult(BaseModel):
    """Response payload for ingest endpoints + CLI output."""

    bank_slug: str
    dry_run: bool
    upstream_count: int
    inserted: int
    updated: int
    archived: int
    unchanged: int
    errors: list[str] = Field(default_factory=list)


__all__ = [
    "IngestResult",
    "ManualCatalogFile",
    "ManualPromo",
]
