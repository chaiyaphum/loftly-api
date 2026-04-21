"""Free-text spend-NLU schemas — `typhoon_nlu_spend` prompt (AI_PROMPTS.md §Prompt 3).

The Typhoon Selector path parses Thai free-text ("ผมใช้จ่ายเดือนละ 80k ส่วนใหญ่กินข้าวข้างนอก อยากเก็บไมล์")
into the structured shape the canonical Selector endpoint consumes. Unlike the
JSON Selector contract (`SelectorInput.spend_categories: dict[str, int]` with THB
amounts), Typhoon returns **fractional allocations** summing to 1.0 — the client
layer multiplies by `monthly_spend_thb` before handing to `POST /v1/selector`.

Spec gap: `mvp/AI_PROMPTS.md §Prompt 3` shows `dict[str, int]` (THB amounts).
DEV_PLAN W19 instructs fractional allocation to avoid double-representing the
total, which is the more robust shape for a free-text parser that may miss an
amount. We keep the fraction form here; the Thai prompt asks the model for
fractions explicitly. See AI_PROMPTS.md commit to reconcile.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Fixed category set — mirrors `SelectorInput.spend_categories` keys with
# `default` in place of `other` because Typhoon is more consistent at
# "residual = default bucket" than "residual = other".
SpendCategory = Literal["dining", "online", "grocery", "travel", "petrol", "default"]

Goal = Literal["miles", "cashback", "flexible"]

# The validator tolerates this much floating-point drift from 1.0 before
# rejecting. Tighter than the Selector's 100-THB tolerance because fractions
# should round-trip exactly.
_FRACTION_SUM_TOLERANCE = 0.01


class SpendProfile(BaseModel):
    """Structured spend profile parsed from free-text Thai.

    `spend_categories` values are **fractions** in [0, 1] summing to ~1.0.
    """

    monthly_spend_thb: int = Field(ge=5_000, le=2_000_000)
    spend_categories: dict[SpendCategory, float] = Field(
        description=(
            "Fractional allocation per category. Keys: "
            "dining, online, grocery, travel, petrol, default. Must sum to ~1.0."
        ),
    )
    goal: Goal

    @field_validator("spend_categories")
    @classmethod
    def _fractions_sum_to_one(
        cls,
        v: dict[SpendCategory, float],
    ) -> dict[SpendCategory, float]:
        if not v:
            raise ValueError("spend_categories must not be empty.")
        for key, value in v.items():
            if value < 0.0 or value > 1.0:
                raise ValueError(
                    f"spend_categories[{key}] must be in [0.0, 1.0]; got {value}."
                )
        total = sum(v.values())
        if abs(total - 1.0) > _FRACTION_SUM_TOLERANCE:
            raise ValueError(
                f"spend_categories must sum to 1.0 (±{_FRACTION_SUM_TOLERANCE}); got {total:.4f}."
            )
        return v


class SpendNLURequest(BaseModel):
    """Request body for `POST /v1/selector/parse-nlu`."""

    text_th: str = Field(
        min_length=4,
        max_length=2_000,
        description="Free-text Thai description of monthly spend habits and goal.",
    )


class SpendNLUResponse(BaseModel):
    """Envelope returned by `POST /v1/selector/parse-nlu`."""

    profile: SpendProfile
    confidence: float = Field(ge=0.0, le=1.0)
    model: str
    duration_ms: int


__all__ = [
    "Goal",
    "SpendCategory",
    "SpendNLURequest",
    "SpendNLUResponse",
    "SpendProfile",
]
