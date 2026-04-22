"""Merchant Reverse Lookup schemas — `/v1/merchants/*` surface.

Mirrors `mvp/API_CONTRACT.md §merchants` + `mvp/SCHEMA.md §15/§16`. The
`/merchants/[slug]` page is the Risk 1 (AI Overviews) mitigation surface:
a proprietary, numerical answer to "which card is best at <merchant>?"

These models describe the API contract — rendered by the Next.js SSR page
under `loftly-web/src/app/merchants/[slug]/page.tsx`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

MerchantType = Literal["retail", "fnb", "ecommerce", "travel", "service"]
MerchantStatus = Literal["active", "pending_review", "merged", "disabled"]
CanonicalizerMethod = Literal["exact", "fuzzy", "llm", "manual"]


class MerchantCanonical(BaseModel):
    """Canonical merchant record — SCHEMA.md §15."""

    id: str
    slug: str
    display_name_th: str
    display_name_en: str
    category_default: str | None = None
    alt_names: list[str] = Field(default_factory=list)
    logo_url: str | None = None
    description_th: str | None = None
    description_en: str | None = None
    merchant_type: MerchantType
    status: MerchantStatus = "active"


class MerchantSearchResult(BaseModel):
    """Row returned by `GET /v1/merchants/search`. Compact for autocomplete."""

    slug: str
    display_name: str
    logo_url: str | None = None
    active_promo_count: int = 0
    category_default: str | None = None


class PromoSummary(BaseModel):
    """Compact promo descriptor embedded in `MerchantRankedCard`.

    Deliberately thin — the full promo payload is on `/v1/promos/{id}`;
    this is the "chip" shown next to a ranked card on the merchant page.
    """

    id: str
    title_th: str
    title_en: str | None = None
    discount_value: str | None = None
    valid_until: str | None = None  # ISO date string


class MerchantRankedCard(BaseModel):
    """One row in the merchant page's ranked-card list.

    `est_value_per_1000_thb` is the core headline: "spend THB 1,000 at
    this merchant → earn roughly ~X THB back" once base-earn × valuation
    and applicable promo uplift are combined. Pure function of the
    ranking service (no LLM) — see `services/merchant_ranking.py`.
    """

    card_slug: str
    display_name: str
    bank_display_name_th: str | None = None
    base_earn_rate: float
    applicable_promos: list[PromoSummary] = Field(default_factory=list)
    est_value_per_1000_thb: float
    confidence: float = Field(ge=0.0, le=1.0)
    applied_rules: list[str] = Field(default_factory=list)
    affiliate_apply_url: str | None = None
    user_owns: bool = False


class HreflangAlternate(BaseModel):
    locale: str  # e.g. "th-TH", "en-US", "x-default"
    href: str


class MerchantPageData(BaseModel):
    """Full payload for `GET /v1/merchants/{slug}`."""

    merchant: MerchantCanonical
    ranked_cards: list[MerchantRankedCard]
    generated_at: datetime
    valuation_snapshot_id: str | None = None
    canonical_url: str
    hreflang_alternates: list[HreflangAlternate] = Field(default_factory=list)


class MerchantListItem(BaseModel):
    """Row in the `GET /v1/merchants` browse hub."""

    slug: str
    display_name_th: str
    display_name_en: str
    category_default: str | None = None
    merchant_type: MerchantType
    active_promo_count: int = 0


class MerchantListResponse(BaseModel):
    """Envelope for `GET /v1/merchants` (browse hub)."""

    data: list[MerchantListItem]
    total: int
    category: str | None = None
    letter: str | None = None


# ---------------------------------------------------------------------------
# Admin: merge + split (skeleton — POST endpoints return 501 for v1).
# Keeping the schemas in-tree so the admin UI PR can consume them when the
# endpoints land in the follow-up release per API_CONTRACT.md §merchants.
# ---------------------------------------------------------------------------


class AdminMergeRequest(BaseModel):
    """Absorb `source_id` into this canonical. Source becomes `status='merged'`.

    Effects: rewrites `promos_merchant_canonical_map.merchant_canonical_id` for
    every source-mapped promo, sets `source.merged_into_id = target.id`,
    invalidates CDN for both slugs, pings GSC reindex, writes `audit_log`.
    """

    source_id: str
    reason: str | None = None


class AdminSplitRequest(BaseModel):
    """Split the current canonical by admin-picked per-promo target.

    Each `reassignments` entry moves one promo's map row to the new canonical.
    If `new_merchant` is provided, a brand-new canonical row is created first
    and the reassignments target its id.
    """

    new_merchant: MerchantCanonical | None = None
    new_merchant_id: str | None = None
    reassignments: list[dict[str, str]] = Field(default_factory=list)
    reason: str | None = None


# ---------------------------------------------------------------------------
# Canonicalizer LLM output — used by `jobs/canonicalize_merchants.py`.
# Mirrors `mvp/AI_PROMPTS.md §Prompt 8`.
# ---------------------------------------------------------------------------


class ProposedMerchant(BaseModel):
    display_name_th: str
    display_name_en: str
    slug: str
    merchant_type: MerchantType
    alt_names: list[str] = Field(default_factory=list)


class UncertainCandidate(BaseModel):
    merchant_id: str
    confidence: float = Field(ge=0.0, le=1.0)


class CanonicalizerResult(BaseModel):
    """One per input candidate — mirrors Prompt 8 output schema."""

    promo_id: str
    action: Literal["match", "new", "uncertain"]
    merchant_id: str | None = None
    proposed: ProposedMerchant | None = None
    top_candidates: list[UncertainCandidate] | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning_th: str


class MerchantCanonicalizerOutput(BaseModel):
    """Top-level Haiku response for Prompt 8."""

    results: list[CanonicalizerResult] = Field(default_factory=list)


class CandidatePromo(BaseModel):
    """Input shape for the canonicalizer prompt — one per candidate."""

    promo_id: str
    raw_merchant_name: str
    promo_category: str | None = None
    promo_title_th: str


class MerchantCanonicalizerInput(BaseModel):
    """Batch envelope (≤20 per call)."""

    candidates: list[CandidatePromo]

    def model_post_init(self, __context: Any) -> None:  # noqa: D401
        if len(self.candidates) > 20:
            raise ValueError("Prompt 8 batches cap at 20 candidates.")


__all__ = [
    "AdminMergeRequest",
    "AdminSplitRequest",
    "CandidatePromo",
    "CanonicalizerResult",
    "HreflangAlternate",
    "MerchantCanonical",
    "MerchantCanonicalizerInput",
    "MerchantCanonicalizerOutput",
    "MerchantListItem",
    "MerchantListResponse",
    "MerchantPageData",
    "MerchantRankedCard",
    "MerchantSearchResult",
    "MerchantStatus",
    "MerchantType",
    "ProposedMerchant",
    "PromoSummary",
    "UncertainCandidate",
]
