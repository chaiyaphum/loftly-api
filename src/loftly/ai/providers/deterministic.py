"""Rule-based `LLMProvider` — no LLM call, always available.

Used as:
- Default provider in dev + tests (no Anthropic key required)
- Fallback whenever the Anthropic path errors (Week 7+, post-wiring)

Algorithm (Selector):
  1. For each candidate card, compute a blended earn rate across the user's
     spend_categories. `earn_rate_local["default"]` covers unspecified categories.
  2. Convert points-earned to THB via `point_valuations.thb_per_point` for the
     card's `earn_currency_id`; unknown currency → THB 0 (excluded from ranking).
  3. Filter / weight by `goal.type`:
       - `miles`     → only `airline` currencies; prefer `goal.currency_preference`
       - `cashback`  → bias toward `bank_proprietary` (cashback via statement credit)
       - `benefits`  → rank by deepest `benefits` JSON (lounge / insurance / etc.)
  4. Issue warning if `min_income_thb > user.monthly_spend x 12 x 0.3`
     (a rough eligibility heuristic — conservative but documented in rationale).
  5. Return top 3 cards with role labels (`primary`/`secondary`/`tertiary`).

Algorithm (Valuation):
  Placeholder — real numeric pass lives in `jobs/valuation.py`. This provider's
  `valuation()` method exists only to satisfy the Protocol; the weekly job
  calls the numeric routine directly, not through the LLM provider layer.
"""

from __future__ import annotations

import math

from loftly.ai import (
    LLMProvider,
    SelectorContext,
    ValuationInput,
    ValuationOutput,
)
from loftly.db.models.card import Card as CardModel
from loftly.schemas.selector import (
    SelectorInput,
    SelectorResult,
    SelectorStackItem,
)
from loftly.services.valuation_fallback import (
    FallbackValuation,
    resolve_earn_rate_key,
)

# Categories the client may send; keep in sync with openapi.yaml#SelectorInput.
KNOWN_CATEGORIES = {"dining", "online", "travel", "grocery", "petrol", "other"}


def _blended_earn_rate(card: CardModel, categories: dict[str, int]) -> float:
    """Weighted earn rate for a card across the user's category mix.

    Looks up each user-supplied category in `earn_rate_local` with alias
    fallback (e.g. selector "grocery" → card "supermarket", selector "petrol"
    → card "fuel"). Unmapped categories fall through to `default`. Returns
    0.0 if the card has no rates at all.
    """
    rates: dict[str, float] = {k: float(v) for k, v in (card.earn_rate_local or {}).items()}
    if not rates:
        return 0.0
    default_rate = rates.get("default", 0.0)
    total_spend = sum(categories.values()) or 1
    weighted_sum = 0.0
    for cat, amount in categories.items():
        key = resolve_earn_rate_key(cat, rates)
        rate = rates[key] if key is not None else default_rate
        weighted_sum += rate * amount
    return weighted_sum / total_spend


def _benefits_depth(card: CardModel) -> int:
    """Shallow count of unique benefit keys — proxy for benefit richness."""
    benefits = card.benefits or {}
    return len(benefits)


class DeterministicProvider:
    """Rule-based provider. Returns `fallback=true` on `SelectorResult`."""

    name = "deterministic"

    async def card_selector(
        self,
        input: SelectorInput,
        context: SelectorContext,
    ) -> SelectorResult:
        categories = dict(input.spend_categories)
        goal_type = input.goal.type
        goal_currency = input.goal.currency_preference

        income_floor = input.monthly_spend_thb * 12 * 0.3

        # Score every candidate card, tagging reasons to explain the rank.
        scored: list[tuple[float, CardModel, int, float]] = []
        eligibility_warnings: list[str] = []
        # Track currencies whose valuation came from the starter fallback table
        # (vs. a real `point_valuations` row). Surfaced once per unique code in
        # the top-level `warnings` array so the UI can render an "estimated,
        # not from weekly compute" badge. Matches the `fallback_valuation`
        # rule convention from `services/merchant_ranking.py` (PR #38).
        fallback_codes_used: set[str] = set()
        for card in context.cards:
            if card.status != "active":
                continue
            # Goal-type filter (hard).
            cur = card.earn_currency
            if goal_type == "miles" and cur.currency_type != "airline":
                continue
            if goal_type == "cashback" and cur.currency_type not in {
                "bank_proprietary",
            }:
                continue
            # `benefits` goal keeps all cards in play; ranked by depth below.

            blended = _blended_earn_rate(card, categories)
            monthly_points = int(blended * input.monthly_spend_thb)

            valuation = context.valuations_by_currency_code.get(cur.code)
            thb_per_point = float(valuation.thb_per_point) if valuation else 0.0
            if isinstance(valuation, FallbackValuation):
                fallback_codes_used.add(cur.code)
            monthly_thb_equivalent = int(monthly_points * thb_per_point)

            # Soft score: combine THB-equivalent + benefit depth + currency preference.
            score = float(monthly_thb_equivalent)
            if goal_currency and cur.code == goal_currency:
                score *= 1.25  # nudge toward requested currency
            if goal_type == "benefits":
                score = float(_benefits_depth(card)) * 100.0 + score / 10.0

            # Eligibility warning (soft; still include the card).
            min_income = float(card.min_income_thb or 0)
            if min_income > income_floor:
                eligibility_warnings.append(f"income_gate:{card.slug}:{int(min_income)}")

            scored.append((score, card, monthly_points, thb_per_point))

        scored.sort(key=lambda t: t[0], reverse=True)
        top = scored[:3]

        # Build stack items with role labels.
        roles = ["primary", "secondary", "tertiary"]
        stack: list[SelectorStackItem] = []
        total_points = 0
        total_thb = 0
        primary_name = None
        for (_score, card, monthly_points, thb_per_point), role in zip(top, roles, strict=False):
            monthly_thb = int(monthly_points * thb_per_point)
            total_points += monthly_points
            total_thb += monthly_thb
            if role == "primary":
                primary_name = card.display_name
            stack.append(
                SelectorStackItem(
                    card_id=str(card.id),
                    slug=card.slug,
                    role=role,
                    monthly_earning_points=monthly_points,
                    monthly_earning_thb_equivalent=monthly_thb,
                    annual_fee_thb=(
                        float(card.annual_fee_thb) if card.annual_fee_thb is not None else None
                    ),
                    reason_th=_reason_th(card, monthly_points, thb_per_point, role),
                    reason_en=_reason_en(card, monthly_points, thb_per_point, role),
                )
            )

        # months_to_goal if target_points is set. Deterministic: ceil.
        months_to_goal: int | None = None
        if input.goal.target_points and total_points > 0:
            months_to_goal = math.ceil(input.goal.target_points / total_points)

        rationale_th = _rationale_th(input.monthly_spend_thb, primary_name)
        rationale_en = _rationale_en(input.monthly_spend_thb, primary_name)

        # Confidence: average valuation confidence across stacked currencies,
        # defaulting to 0.5 for the rule-based path.
        stacked_fallback_codes: set[str] = set()
        if stack:
            confidences: list[float] = []
            for item in stack:
                slug = item.slug
                card = next(c for c in context.cards if c.slug == slug)
                val = context.valuations_by_currency_code.get(card.earn_currency.code)
                if val is not None:
                    confidences.append(float(val.confidence))
                # Only surface fallback notes for codes on the final stack —
                # filtering keeps `warnings` compact vs. flooding every seeded
                # card's currency.
                if card.earn_currency.code in fallback_codes_used and isinstance(
                    val, FallbackValuation
                ):
                    stacked_fallback_codes.add(card.earn_currency.code)
            valuation_confidence = sum(confidences) / len(confidences) if confidences else 0.5
        else:
            valuation_confidence = 0.0

        # Emit `fallback_valuation:<code>` once per currency actually used in
        # the returned stack. Matches `services/merchant_ranking.py` naming so
        # the UI can reuse the same rule parser.
        for code in sorted(stacked_fallback_codes):
            eligibility_warnings.append(f"fallback_valuation:{code}")

        return SelectorResult(
            session_id="deterministic",  # overwritten by route handler
            stack=stack,
            total_monthly_earning_points=total_points,
            total_monthly_earning_thb_equivalent=total_thb,
            months_to_goal=months_to_goal,
            with_signup_bonus_months=None,
            valuation_confidence=round(valuation_confidence, 2),
            rationale_th=rationale_th,
            rationale_en=rationale_en,
            warnings=eligibility_warnings,
            llm_model="deterministic",
            fallback=True,
            partial_unlock=False,
        )

    async def valuation(self, input: ValuationInput) -> ValuationOutput:
        """Protocol stub — the weekly valuation runs the numeric routine directly.

        See `jobs/valuation.py`. We keep this method so the Protocol stays
        uniform, but Selector is the only caller of providers today.
        """
        raise NotImplementedError(
            "DeterministicProvider.valuation is not used; call jobs.valuation.compute() directly."
        )


def _reason_th(
    card: CardModel,
    monthly_points: int,
    thb_per_point: float,
    role: str,
) -> str:
    role_label = {"primary": "หลัก", "secondary": "เสริม", "tertiary": "สำรอง"}[role]
    thb_eq = int(monthly_points * thb_per_point)
    return (
        f"บัตร{role_label}: สะสม ~{monthly_points:,} คะแนน/เดือน "
        f"เทียบเท่า ~THB {thb_eq:,}/เดือน "
        f"({card.earn_currency.display_name_th})"
    )


def _reason_en(
    card: CardModel,
    monthly_points: int,
    thb_per_point: float,
    role: str,
) -> str:
    thb_eq = int(monthly_points * thb_per_point)
    return (
        f"{role.capitalize()} pick: ~{monthly_points:,} pts/mo "
        f"(≈ THB {thb_eq:,}) on {card.earn_currency.display_name_en}"
    )


def _rationale_th(monthly_spend: int, primary_name: str | None) -> str:
    primary = primary_name or "บัตรแนะนำ"
    return (
        f"ด้วยการใช้จ่าย THB {monthly_spend:,}/เดือนบนบัตร {primary} "
        f"ระบบคำนวณแบบ rule-based (ไม่มีการเรียก LLM) โดยให้น้ำหนักตามหมวดที่คุณระบุ "
        "และมูลค่าแต้มจากตาราง valuation ล่าสุด"
    )


def _rationale_en(monthly_spend: int, primary_name: str | None) -> str:
    primary = primary_name or "the recommended card"
    return (
        f"With monthly spend of THB {monthly_spend:,} on {primary}, "
        "this is a rule-based ranking (no LLM call) that weights your category "
        "allocation against the latest point valuations."
    )


__all__ = ["DeterministicProvider", "LLMProvider"]
