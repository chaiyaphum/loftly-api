"""Loader stub for `selector_chat_followup` prompt.

PR-8 will replace this stub with the real text loaded from
`src/loftly/prompts/selector_chat_followup.md`. This PR (§1 chat backend)
only needs a stable `load()` signature so the endpoint can build a Haiku
message from a placeholder map. Keeping it as a module-level Python file
(not a `.md` file yet) avoids racing PR-8's content; when PR-8 lands, the
loader here is updated to read the `.md` and the endpoint is unchanged.

Design:
- `load()` returns the fully-substituted system + user prompt pair.
- The caller provides a `variables` dict (rationale, stack, question, category).
- Missing placeholders raise `KeyError` immediately — catching a silent
  templating hole in tests beats discovering it in a billable Haiku call.
"""

from __future__ import annotations

from typing import Any

PROMPT_NAME = "selector_chat_followup"
PROMPT_VERSION = "v0-stub"

# Bare-bones system prompt. PR-8 supplies the real brand-voice-aligned text.
# The placeholders below are the stable contract between this loader and the
# chat route — PR-8 may add *more* placeholders, but must not remove these.
_SYSTEM_TEMPLATE = (
    "You are Loftly's Card Selector follow-up assistant. "
    "A Thai user just saw their top-ranked card stack and is asking a "
    "follow-up question. Respond in the user's locale ({locale}). "
    "Keep answers under 400 characters. Cite specific cards from the "
    "provided stack only — do not invent card names. If the question is "
    "beyond the data, say so plainly."
)

_USER_TEMPLATE = (
    "### Selector rationale\n{rationale_th}\n\n"
    "### Ranked stack\n{stack_json}\n\n"
    "### User question ({category})\n{question}\n"
)


def load(variables: dict[str, Any]) -> dict[str, str]:
    """Return `{"system": ..., "user": ...}` with placeholders substituted.

    Raises `KeyError` if a required placeholder is missing from `variables`.
    """
    return {
        "system": _SYSTEM_TEMPLATE.format(**variables),
        "user": _USER_TEMPLATE.format(**variables),
    }


def prompt_slug() -> str:
    """PostHog/Langfuse identifier — bumped to real version when PR-8 lands."""
    return f"{PROMPT_NAME}@{PROMPT_VERSION}"


__all__ = ["PROMPT_NAME", "PROMPT_VERSION", "load", "prompt_slug"]
