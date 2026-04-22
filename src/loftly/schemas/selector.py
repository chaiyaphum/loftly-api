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
    # POST_V1 §3 Tier A (2026-04-22): promos the LLM cited for this stack item.
    # Populated only when `LOFTLY_FF_SELECTOR_PROMO_CONTEXT` is ON. Server
    # strips any id not in the current promo snapshot before returning.
    cited_promo_ids: list[str] = Field(default_factory=list)


class PromoChipPayload(BaseModel):
    """Minimal promo details embedded in SelectorResult so the frontend can
    render a chip without a second API round-trip. Mirrors the shape of
    PromoChipProps in `loftly-web/src/components/loftly/PromoChip.tsx`.
    """

    promo_id: str
    merchant: str | None = None
    discount_value: str | None = None
    discount_type: str | None = None
    valid_until: str | None = None  # ISO date
    min_spend: float | None = None
    source_url: str | None = None


PromoContextStatus = Literal["ok", "degraded", "stale"]


FallbackReason = Literal[
    "upstream_503",
    "timeout",
    "rate_limit",
    "both_failed",
    "cost_cap",
]


class SelectorResult(BaseModel):
    """Selector response envelope — `openapi.yaml#SelectorResult`.

    Fallback fields:
    - `fallback` — legacy flag, True whenever we dropped below the Sonnet
      primary path. Kept for backward compat with existing clients. New
      callers should prefer the more specific pair below.
    - `used_fallback` — True whenever we dropped below Sonnet (Haiku OR
      deterministic). Mirrors the legacy `fallback` flag semantically.
    - `fallback_reason` — classified cause of the Sonnet departure.
    - `used_deterministic` — True only when we landed on the rule-based
      provider (i.e. both Sonnet + Haiku failed, or cost cap skipped Haiku).
    """

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
    used_fallback: bool = False
    fallback_reason: FallbackReason | None = None
    used_deterministic: bool = False
    partial_unlock: bool = False
    # POST_V1 §3 Tier A (2026-04-22) — Promo-Aware Card Selector.
    # Union of every stack item's `cited_promo_ids`, computed server-side after
    # validation against the live snapshot. Empty when flag is OFF or no
    # promos were cited.
    cited_promo_ids: list[str] = Field(default_factory=list)
    # "ok" — snapshot built cleanly and at least attempted
    # "degraded" — query failed / timed out / feature flag forced degrade
    # "stale" — snapshot older than the 72h freshness window
    promo_context_status: PromoContextStatus = "ok"
    # sha256(first16) of the promo snapshot's (id,checksum) material. Surfaced
    # for Langfuse trace correlation + cache-key debugging. None when flag OFF.
    promo_snapshot_digest: str | None = None
    # Full chip payloads for the ids in `cited_promo_ids`, denormalized from
    # the snapshot so the frontend doesn't need a second fetch.
    promo_chips: list[PromoChipPayload] = Field(default_factory=list)
