"""Admin feature-flag inspector.

Lists the flags Loftly owns in PostHog with their current evaluation for a
synthetic probe user. Useful for ops to confirm a flag is live + the rollout
is taking effect without leaving the PostHog UI.

The list of `KNOWN_FLAGS` is hand-maintained — add a row when you introduce a
new flag so admins don't have to cross-reference PostHog to know what exists.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends

from loftly.api.auth import get_current_admin_id
from loftly.core.feature_flags import FeatureFlags

router = APIRouter(prefix="/v1/admin", tags=["admin"])


# Synthetic probe distinct_id — never collides with a real user UUID. Using a
# fixed value here means the response is stable between polls for quick diffs.
_PROBE_DISTINCT_ID = "admin-flag-probe"


# Hand-rolled registry of flags the app knows about. The `rollout_pct` field
# is a human-readable expectation, NOT the live value — PostHog is the source
# of truth for the real rollout. Update alongside any PostHog dashboard change.
KNOWN_FLAGS: list[dict[str, Any]] = [
    {
        "key": "selector_cta_copy",
        "description": (
            "A/B test on the Selector results Apply CTA copy (control vs variant_a). Owner: growth."
        ),
        "type": "multivariate",
        "expected_variants": ["control", "variant_a"],
        "expected_rollout_pct": 100,
    },
    {
        "key": "typhoon_nlu_spend",
        "description": (
            "Free-text Thai spend parser (Typhoon / SambaNova) gating "
            "POST /v1/selector/parse-nlu. Env override: LOFTLY_TYPHOON_NLU_ENABLED."
        ),
        "type": "boolean",
        "expected_variants": [True, False],
        "expected_rollout_pct": 0,
    },
    {
        "key": "landing_hero_cta",
        "description": (
            "Landing-page hero copy A/B. variant_benefit_led tests benefit framing; "
            "variant_urgency tests loss-aversion framing. Owner: founder."
        ),
        "type": "multivariate",
        "expected_variants": ["control", "variant_benefit_led", "variant_urgency"],
        "expected_rollout_pct": 100,
    },
    {
        "key": "post_v1_selector_chat",
        "description": (
            "POST_V1 §1 selector results chat panel + backend endpoint. "
            "Owner: founder. Default OFF."
        ),
        "type": "boolean",
        "expected_variants": None,
        "expected_rollout_pct": 0,
    },
    {
        "key": "post_v1_returning_landing",
        "description": (
            "POST_V1 §3 returning-user personalized landing (client-hydrated). "
            "Owner: founder. Default OFF."
        ),
        "type": "boolean",
        "expected_variants": None,
        "expected_rollout_pct": 0,
    },
    {
        "key": "selector_promo_context",
        "description": (
            "Promo-Aware Card Selector: inject deal-harvester active promos into "
            "Sonnet cached context + render promo chips per stack card. "
            "POST_V1 Tier A fast-follow ratified 2026-04-22. "
            "Env override: LOFTLY_FF_SELECTOR_PROMO_CONTEXT. "
            "Owner: founder. Default OFF; rollout = shadow-mode -> founder-only -> all staging."
        ),
        "type": "boolean",
        "expected_variants": None,
        "expected_rollout_pct": 0,
    },
    {
        "key": "merchants_reverse_lookup",
        "description": (
            "POST_V1 §9 Merchant Reverse Lookup (/merchants/[slug]) — ranked cards "
            "with active promos per merchant. Tier B ratified early via Q18 2026-04-22 "
            "(lower PDPA surface than §6–§8). Seed 50 curated brands; hidden routes "
            "behind flag until canonicalization precision ≥ 0.9. "
            "Owner: founder. Default OFF."
        ),
        "type": "boolean",
        "expected_variants": None,
        "expected_rollout_pct": 0,
    },
]


@router.get(
    "/feature-flags",
    summary="List known feature flags + current probe evaluation",
)
async def list_feature_flags(
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
) -> dict[str, Any]:
    """Return `KNOWN_FLAGS` augmented with a live PostHog evaluation.

    `probe_value` is the variant (or boolean) PostHog returns for the static
    `_PROBE_DISTINCT_ID`. When PostHog is unconfigured, `probe_value` will be
    the `default` ("control" / False) and `posthog_configured` is False —
    admins see at a glance that the backend is in fallback mode.
    """
    flags = FeatureFlags()
    configured = flags._api_key is not None

    data: list[dict[str, Any]] = []
    for entry in KNOWN_FLAGS:
        key = str(entry["key"])
        probe: Any
        if entry.get("type") == "multivariate":
            probe = await flags.variant(key, _PROBE_DISTINCT_ID, default="control")
        else:
            probe = await flags.is_enabled(key, _PROBE_DISTINCT_ID, default=False)
        data.append(
            {
                **entry,
                "probe_value": probe,
                "probe_distinct_id": _PROBE_DISTINCT_ID,
            }
        )

    return {
        "posthog_configured": configured,
        "flags": data,
    }
