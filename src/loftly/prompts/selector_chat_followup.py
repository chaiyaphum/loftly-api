"""Versioned prompt loader for `selector_chat_followup` (POST_V1 §1).

Per `mvp/AI_PROMPTS.md §Prompt 5` — handle follow-up questions on
`/selector/results/[session_id]` (explain ranking, what-if scenarios) via
Haiku 4.5, reusing the 50k Selector cache + cached SelectorResult.

Loading contract (mirrors the pattern adopted for Prompt 6
`personalized_welcome_email` in the adjacent PR-3 — once that lands the two
loaders share the same shape):

- `load() -> ChatFollowupPrompt` returns a frozen dataclass holding the prompt
  text, version, banned-phrase list, and required placeholder names.
- `PROMPT_NAME` + `PROMPT_VERSION` constants are stamped into every PostHog
  event + Langfuse trace via `prompt_slug()`.
- The markdown source lives in `selector_chat_followup.md`; we load it once
  at import time so tests can assert content without repeatedly hitting disk.

Route / provider wiring arrives in PR-9. Nothing here is live on main.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

PROMPT_NAME = "selector_chat_followup"
PROMPT_VERSION = "v1"

# Placeholders the route MUST substitute before the prompt hits Haiku. CI
# linter (see `AI_PROMPTS.md §Prompt versioning`) will eventually fail builds
# when a placeholder is added to the markdown but not registered here.
REQUIRED_PLACEHOLDERS: tuple[str, ...] = (
    "{question_th}",
    "{classifier_category}",
    "{selector_context}",
    "{cached_result}",
)

# Mirrors the "Banned phrases" block at the bottom of the markdown. The route
# output linter scans `answer_th` + `answer_en` against this list and retries
# once on hit; persistent hit → static fallback.
BANNED_PHRASES: tuple[str, ...] = (
    "revolutionary",
    "cutting-edge",
    "synergy",
    "game-changer",
    "ปฏิวัติวงการ",
    "ล้ำสมัย",
    "เปลี่ยนโลก",
)

# Classifier hint values the route may pass in. Values outside this set → the
# route coerces to "other" before the prompt is built. Kept here so the route
# and prompt agree on the enum.
CLASSIFIER_CATEGORIES: tuple[str, ...] = ("explain", "what-if", "other")

# Per `mvp/AI_PROMPTS.md §Prompt 5 — Latency + cost budget` — enforced
# pre-flight by the route (PR-9).
MAX_QUESTION_CHARS: int = 500
COST_CAP_THB: float = 0.10
HAIKU_TIMEOUT_S: float = 5.0
P95_BUDGET_S: float = 3.0
RATE_LIMIT_PER_SESSION: int = 10

_PROMPT_MD_PATH = Path(__file__).parent / "selector_chat_followup.md"


@dataclass(frozen=True)
class ChatFollowupPrompt:
    """Loaded prompt artifact. Route treats this as opaque + read-only."""

    text: str
    version: str
    banned_phrases: tuple[str, ...]
    placeholders: tuple[str, ...]


@lru_cache(maxsize=1)
def load() -> ChatFollowupPrompt:
    """Read the markdown source and return a cached `ChatFollowupPrompt`.

    Importers should call `load()` at request time; the `lru_cache` makes it
    O(1) after first call. Tests that want a fresh read (e.g., after editing
    the md fixture) can `load.cache_clear()`.
    """
    text = _PROMPT_MD_PATH.read_text(encoding="utf-8")
    if not text.strip():
        raise RuntimeError(f"selector_chat_followup.md is empty at {_PROMPT_MD_PATH}")
    return ChatFollowupPrompt(
        text=text,
        version=PROMPT_VERSION,
        banned_phrases=BANNED_PHRASES,
        placeholders=REQUIRED_PLACEHOLDERS,
    )


def prompt_slug() -> str:
    """PostHog/Langfuse identifier: `selector_chat_followup@v1`."""
    return f"{PROMPT_NAME}@{PROMPT_VERSION}"


__all__ = [
    "BANNED_PHRASES",
    "CLASSIFIER_CATEGORIES",
    "COST_CAP_THB",
    "HAIKU_TIMEOUT_S",
    "MAX_QUESTION_CHARS",
    "P95_BUDGET_S",
    "PROMPT_NAME",
    "PROMPT_VERSION",
    "RATE_LIMIT_PER_SESSION",
    "REQUIRED_PLACEHOLDERS",
    "ChatFollowupPrompt",
    "load",
    "prompt_slug",
]
