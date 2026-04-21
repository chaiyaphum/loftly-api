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
