"""Pure-regex classifier for POST_V1 §1 `selector_chat_followup` routing.

Per `POST_V1.md §1 Acceptance criteria` — what-if questions (amount + category
keyword) require a deterministic re-rank with a modified profile; explain
questions just echo rationale; `other` falls through to a generic LLM answer.

Design principles:
- **No LLM.** This is the cheap pre-flight that decides *whether* to call Haiku
  at all. Running an LLM to classify an LLM question invites cascade failures.
- **Mixed Thai-English.** Users routinely type "เพิ่ม dining 20000" or
  "what if online เพิ่ม 30k". Both category keywords and trigger phrases
  include both scripts.
- **Category-first precedence.** `amount + category` dominates. If the input
  has both a THB amount and a category keyword, it is what-if — even if it
  *also* includes an "explain" trigger like "ทำไม". Rationale: a real what-if
  question ("ถ้าเพิ่ม dining 20,000 ทำไมผลเปลี่ยน?") carries both signals;
  the re-rank is the more useful answer.

The extractor returns a stable `{category, amount_thb_delta}` tuple when the
question is unambiguous, and `None` when multiple categories or multiple
amounts appear (we'd rather decline than guess which delta applies).
"""

from __future__ import annotations

import re
from typing import Literal

Category = Literal["explain", "what-if", "other"]

# Match THB amounts in the forms users actually write:
#   80000, 80,000, 80k, 80K, ฿20000, THB 20000, 20,000 บาท, 20k บาท
# We intentionally do NOT match bare 1-3 digit numbers without a unit or
# thousands-separator ("answer 2" should not be read as "2 THB"). The regex
# requires either a thousands separator (`\d{1,3}(?:,\d{3})+`), a 4+ digit run,
# OR a short number paired with a k/K/พัน/หมื่น/แสน multiplier.
_AMOUNT_PATTERN = re.compile(
    r"""
    (?:THB|thb|฿)?\s*              # optional leading currency
    (?P<num>
        \d{1,3}(?:,\d{3})+          # 80,000
      | \d{4,}                       # 80000, 100000
      | \d{1,3}(?=\s*(?:k|K|พัน|หมื่น|แสน))  # 80k, 80 k, 5หมื่น
    )
    \s*
    (?P<mult>k|K|พัน|หมื่น|แสน)?  # optional multiplier suffix
    \s*
    (?:THB|thb|บาท|฿)?             # optional trailing currency
    """,
    re.VERBOSE,
)

# Canonical category slug → recognized keywords (Thai + English).
# Keep in sync with `SpendCategory` slugs used in schemas/spend_nlu.py and
# with the POST_V1 §1 classifier contract in the plan.
_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "dining": ("dining", "กิน", "ทาน", "อาหาร", "ร้านอาหาร", "คาเฟ่"),
    "online": ("online", "ออนไลน์", "ช้อปออนไลน์", "shopee", "lazada"),
    "grocery": ("grocery", "ซูเปอร์", "ซุปเปอร์", "ตลาด", "lotus", "tops", "makro", "big c"),
    "travel": ("travel", "เดินทาง", "ท่องเที่ยว", "โรงแรม", "เที่ยวบิน", "grab", "taxi"),
    "petrol": ("petrol", "น้ำมัน", "ปั๊ม", "ptt", "shell", "caltex"),
}

# "Explain" triggers — questions that ask WHY or COMPARE without a new amount.
_EXPLAIN_TRIGGERS: tuple[str, ...] = (
    "ทำไม",
    "why",
    "อันดับ",
    "rank",
    "เปรียบเทียบ",
    "compare",
    " vs ",
    "vs.",
    "อธิบาย",
    "explain",
    "เหตุผล",
)

# Thai / English numeric multipliers.
_MULTIPLIER: dict[str, int] = {
    "k": 1_000,
    "K": 1_000,
    "พัน": 1_000,
    "หมื่น": 10_000,
    "แสน": 100_000,
}

# Explicit "decrease" markers that flip the delta sign.
_NEGATIVE_TRIGGERS: tuple[str, ...] = ("ลด", "น้อยลง", "-", "ลบ", "decrease", "less")
# Positive markers — kept for completeness; default is positive if none match.
_POSITIVE_TRIGGERS: tuple[str, ...] = ("เพิ่ม", "มากขึ้น", "+", "บวก", "increase", "more")


def _find_categories(question: str) -> list[str]:
    """Return every canonical category slug whose keyword appears in `question`.

    Matching is case-insensitive. Order-preserving + dedup — the first-appearing
    category wins when `extract_whatif_params` needs a single answer.
    """
    lowered = question.lower()
    hits: list[str] = []
    for slug, keywords in _CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in lowered:
                hits.append(slug)
                break
    return hits


def _has_amount(question: str) -> bool:
    return _AMOUNT_PATTERN.search(question) is not None


def _has_explain_trigger(question: str) -> bool:
    lowered = question.lower()
    return any(t.lower() in lowered for t in _EXPLAIN_TRIGGERS)


def classify(question: str) -> Category:
    """Return `"what-if"`, `"explain"`, or `"other"` for a follow-up question.

    Precedence: (amount + category) → what-if; else explain-trigger → explain;
    else other. A what-if classification does NOT require an explicit
    "ถ้า/what if" phrase — the amount+category combo is sufficient signal.
    """
    if not question or not question.strip():
        return "other"
    has_amount = _has_amount(question)
    categories = _find_categories(question)
    if has_amount and categories:
        return "what-if"
    if _has_explain_trigger(question):
        return "explain"
    return "other"


def _parse_amount(match: re.Match[str]) -> int | None:
    """Convert a single regex match (num + optional multiplier) to THB integer."""
    raw_num = match.group("num").replace(",", "")
    try:
        base = int(raw_num)
    except ValueError:
        return None
    mult_str = match.group("mult")
    if mult_str:
        base *= _MULTIPLIER.get(mult_str, 1)
    return base


def _delta_sign(question: str, amount_match_span: tuple[int, int]) -> int:
    """Return -1 for a detected decrease, +1 otherwise.

    Looks only at the window immediately before the amount (up to 20 chars) so
    a generic "ลด" elsewhere in the question doesn't flip an unrelated delta.
    """
    start = max(0, amount_match_span[0] - 20)
    window = question[start : amount_match_span[0]].lower()
    for marker in _NEGATIVE_TRIGGERS:
        if marker.lower() in window:
            return -1
    for marker in _POSITIVE_TRIGGERS:
        if marker.lower() in window:
            return 1
    return 1


def extract_whatif_params(question: str) -> dict[str, int | str] | None:
    """For a what-if question, return `{category, amount_thb_delta}` or `None`.

    Returns None when:
    - no amount was found
    - more than one distinct category is mentioned (ambiguous target)
    - more than one amount is found (ambiguous delta)
    - the question would not classify as what-if in the first place

    Negative deltas (e.g., "ลด dining 10000") return a negative
    `amount_thb_delta`, preserving direction for downstream profile
    modification.
    """
    if classify(question) != "what-if":
        return None

    categories = _find_categories(question)
    if len(categories) != 1:
        # Zero categories shouldn't happen post-classify, but multi-category
        # ("เพิ่ม dining กับ travel 20000") is genuinely ambiguous.
        return None

    amount_matches = list(_AMOUNT_PATTERN.finditer(question))
    if not amount_matches:
        return None
    # If multiple distinct amounts appear, bail — "ลด dining 10000 เพิ่ม 30000"
    # is the user doing two things at once, not a single delta.
    numeric_values: list[int] = []
    for m in amount_matches:
        parsed = _parse_amount(m)
        if parsed is not None:
            numeric_values.append(parsed)
    if len(numeric_values) != 1:
        return None

    amount = numeric_values[0]
    match = amount_matches[0]
    sign = _delta_sign(question, match.span())

    return {"category": categories[0], "amount_thb_delta": sign * amount}


__all__ = ["Category", "classify", "extract_whatif_params"]
