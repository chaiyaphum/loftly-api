"""Card catalog schemas — `openapi.yaml#Card`, `#CardList`, `#Currency`."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from loftly.schemas.common import Pagination

Network = Literal["Visa", "Mastercard", "Amex", "JCB", "UnionPay"]
CardStatus = Literal["active", "inactive", "archived"]
CurrencyType = Literal["bank_proprietary", "airline", "hotel"]


class Currency(BaseModel):
    """Loyalty currency — `openapi.yaml#Currency`."""

    code: str
    display_name_en: str
    display_name_th: str
    currency_type: CurrencyType
    issuing_entity: str | None = None


class BankMini(BaseModel):
    """Minimal bank embedded in a Card response — matches the inline object in openapi.yaml#Card."""

    slug: str
    display_name_en: str
    display_name_th: str


class SignupBonus(BaseModel):
    bonus_points: int
    spend_required: float
    timeframe_days: int


class Card(BaseModel):
    """Card catalog row — `openapi.yaml#Card`."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    slug: str
    display_name: str
    bank: BankMini
    tier: str | None = None
    network: Network
    annual_fee_thb: float | None = None
    annual_fee_waiver: str | None = None
    min_income_thb: float | None = None
    min_age: int | None = None
    earn_currency: Currency
    earn_rate_local: dict[str, float] = Field(default_factory=dict)
    earn_rate_foreign: dict[str, float] | None = None
    benefits: dict[str, Any] = Field(default_factory=dict)
    signup_bonus: SignupBonus | None = None
    description_th: str | None = None
    description_en: str | None = None
    status: CardStatus = "active"


class CardList(BaseModel):
    """Paginated card collection — `openapi.yaml#CardList`."""

    data: list[Card]
    pagination: Pagination


class TransferPartner(BaseModel):
    """One `transfer_ratios` row projected for the compare widget."""

    destination_code: str
    destination_display_name_en: str
    destination_display_name_th: str
    ratio_source: float
    ratio_destination: float
    bonus_percentage: float


class CardValuationSnapshot(BaseModel):
    """Most-recent `point_valuations` row for the card's earn currency."""

    thb_per_point: float
    methodology: str
    confidence: float
    sample_size: int


class CardComparison(BaseModel):
    """Enriched card payload used by `/v1/cards/compare` — superset of `Card`.

    Adds transfer partners, a THB-per-point valuation snapshot, and a Loftly
    score (computed server-side from confidence + earn-rate + fee heuristics
    until the dedicated scoring pipeline lands post-MVP).
    """

    card: Card
    transfer_partners: list[TransferPartner] = Field(default_factory=list)
    valuation: CardValuationSnapshot | None = None
    loftly_score: float | None = None


class CardComparisonList(BaseModel):
    """Response envelope for `/v1/cards/compare`."""

    data: list[CardComparison]


class CardSimilarList(BaseModel):
    """Response envelope for `/v1/cards/similar/{slug}`."""

    data: list[Card]
