"""Card Selector schemas — `openapi.yaml#SelectorInput`, `#SelectorResult`."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

GoalType = Literal["miles", "cashback", "benefits"]
Role = Literal["primary", "secondary", "tertiary"]
Locale = Literal["th", "en"]


class SelectorGoal(BaseModel):
    type: GoalType
    currency_preference: str | None = None
    horizon_months: int | None = None
    target_points: int | None = None


class SelectorInput(BaseModel):
    """POST /v1/selector body — `openapi.yaml#SelectorInput`."""

    monthly_spend_thb: int = Field(ge=5_000, le=2_000_000)
    spend_categories: dict[str, int] = Field(
        description="Keys in {dining, online, travel, grocery, petrol, other}",
    )
    current_cards: list[str] = Field(default_factory=list)
    goal: SelectorGoal
    locale: Locale


class SelectorStackItem(BaseModel):
    card_id: str
    slug: str
    role: Role
    monthly_earning_points: int
    monthly_earning_thb_equivalent: int
    annual_fee_thb: float | None = None
    reason_th: str
    reason_en: str | None = None


class SelectorResult(BaseModel):
    """Selector response envelope — `openapi.yaml#SelectorResult`."""

    session_id: str
    stack: list[SelectorStackItem]
    total_monthly_earning_points: int
    total_monthly_earning_thb_equivalent: int
    months_to_goal: int | None = None
    with_signup_bonus_months: int | None = None
    valuation_confidence: float
    rationale_th: str
    rationale_en: str | None = None
    warnings: list[str] = Field(default_factory=list)
    llm_model: str
    fallback: bool = False
    partial_unlock: bool = False
