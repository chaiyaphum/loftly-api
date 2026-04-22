"""Response schemas for `/v1/promos` — see mvp/API_CONTRACT.md §Promos.

Deliberately flat: loftly-web consumes these directly, and we don't want to
force clients through a second fetch for bank/merchant details on the list
surface. Fields `raw_data`, `external_checksum`, `last_synced_at`, and other
upstream/bookkeeping columns are intentionally stripped — see SCHEMA.md §9
for the full DB shape.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

PromoType = Literal[
    "category_bonus",
    "cashback",
    "transfer_bonus",
    "signup",
    "statement_credit",
    "dining_program",
]


class BankRef(BaseModel):
    """Compact bank reference embedded in a promo row.

    Matches `banks` columns the UI needs without leaking admin-only fields.
    """

    id: str
    slug: str
    name_th: str | None = None
    name_en: str | None = None


class MerchantCanonicalRef(BaseModel):
    """Canonical merchant pointer — null until the canonicalizer has run."""

    slug: str
    name_th: str | None = None
    name_en: str | None = None


class PromoListItem(BaseModel):
    id: str
    bank: BankRef
    merchant_name: str | None = None
    merchant_canonical: MerchantCanonicalRef | None = None
    title_th: str
    title_en: str | None = None
    description_th: str | None = None
    image_url: str | None = None
    category: str | None = None
    promo_type: PromoType | None = None
    discount_type: str | None = None
    discount_value: str | None = None
    discount_amount: Decimal | None = None
    discount_unit: str | None = None
    minimum_spend: Decimal | None = None
    valid_from: date | None = None
    valid_until: date | None = None
    source_url: str
    card_ids: list[str] = Field(default_factory=list)


class PromoListResponse(BaseModel):
    items: list[PromoListItem]
    total: int
    page: int
    page_size: int
    pages: int


__all__ = [
    "BankRef",
    "MerchantCanonicalRef",
    "PromoListItem",
    "PromoListResponse",
    "PromoType",
]
