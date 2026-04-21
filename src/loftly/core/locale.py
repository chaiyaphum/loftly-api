"""Thai/English locale detection from Accept-Language + override.

Used by POST_V1 §2 personalized welcome email composer and §3 returning-user
landing hero to pick the right user-facing language. Thai is the default on
ambiguous input per POST_V1.md §2 AC-4 — the target audience is Thai first,
English copy is a secondary accommodation.

Resolution order:
  1. Explicit caller override (e.g., user's persisted locale preference)
  2. Accept-Language header — Thai wins on any presence (even mixed) per AC-4
  3. English wins only when Accept-Language signals English AND not Thai
  4. Fallback → Thai
"""

from __future__ import annotations

from typing import Literal

Locale = Literal["th", "en"]


def detect_locale(
    accept_language: str | None,
    override: Locale | str | None = None,
) -> Locale:
    """Resolve user locale for email + UI rendering.

    - Explicit override wins (caller already picked a locale)
    - Thai default on ambiguous / missing / mixed per POST_V1.md §2 AC-4
    - Only returns 'en' when Accept-Language signals English clearly
      AND Thai is not also present
    """
    if override in ("th", "en"):
        return override  # type: ignore[return-value]
    if not accept_language:
        return "th"
    tokens = [t.split(";")[0].strip().lower() for t in accept_language.split(",")]
    has_thai = any(t.startswith("th") for t in tokens)
    has_english = any(t.startswith("en") for t in tokens)
    if has_thai:
        return "th"
    if has_english:
        return "en"
    return "th"


__all__ = ["Locale", "detect_locale"]
